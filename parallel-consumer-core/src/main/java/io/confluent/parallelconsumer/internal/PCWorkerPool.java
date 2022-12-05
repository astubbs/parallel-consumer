package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.Range;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.state.ProcessingShard;
import io.confluent.parallelconsumer.state.QueuedWorkManager;
import io.confluent.parallelconsumer.state.WorkContainer;
import io.confluent.parallelconsumer.state.WorkManager;
import io.micrometer.core.instrument.Timer;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @author Antony Stubbs
 * @see PCWorker
 */
@Slf4j
@Value
@NonFinal
// todo rename WorkerPool
public class PCWorkerPool<K, V, R> implements Closeable {

    /**
     * The pool which is used for running the users' supplied function
     */
    ThreadPoolExecutor executorPool;

    ParallelConsumerOptions<K, V> options;

    FunctionRunner<K, V, R> runner;

    //    BlockingQueue<PCWorker<K, V, R>> workers;
    NavigableSet<PCWorker<K, V, R>> workers;

//    Map<ProcessingShard<?, ?>, PCWorker<K, V, ?>> shardWorkerMap = new HashMap<>();

    private final PCModule<K, V> module;

    @NonFinal
    PCWorker<K, V, R> lastWorker;

//    Batcher<?> batcher = new Batcher();

    @SneakyThrows
    public PCWorkerPool(int poolSize, FunctionRunner<K, V, R> functionRunner, PCModule<K, V> module) {
        runner = functionRunner;
        this.options = module.options();
        this.module = module;
        executorPool = createExecutorPool(options.getMaxConcurrency(), module.workManager());
        var qwm = new QueuedWorkManager<K, V>(module.queuedShardManager());
        var pcWorkerStream = Range.range(poolSize).toStream().boxed()
                .map(ignore -> new PCWorker<>(this, qwm))
                .collect(Collectors.toList());
        workers = new TreeSet<PCWorker<K, V, R>>(Comparator.comparing(PCWorker::toString));
        workers.addAll(pcWorkerStream);

        // start
        for (PCWorker<K, V, R> worker : workers) {
            // todo throws
            executorPool.submit(worker::loop);
        }
    }

    protected ThreadPoolExecutor createExecutorPool(int poolSize, WorkManager<K, V> wm) {
        ThreadFactory defaultFactory;
        try {
            defaultFactory = InitialContext.doLookup(options.getManagedThreadFactory());
        } catch (NamingException e) {
            log.debug("Using Java SE Thread", e);
            defaultFactory = Executors.defaultThreadFactory();
        }
        ThreadFactory finalDefaultFactory = defaultFactory;
        ThreadFactory namingThreadFactory = r -> {
            Thread thread = finalDefaultFactory.newThread(r);
            String name = thread.getName();
            thread.setName("pc-" + name);
            return thread;
        };
        ThreadPoolExecutor.AbortPolicy rejectionHandler = new ThreadPoolExecutor.AbortPolicy();

        LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();

        return new ThreadPoolExecutor(poolSize, poolSize, 0L, MILLISECONDS, workQueue,
                namingThreadFactory, rejectionHandler);
    }

//    public void onWorkAdded(@NonNull ProcessingShard<K, V> processingShard) throws InterruptedException {
//        PCWorker<K, V, ?> worker = maybeDistributeShards(processingShard);
//        worker.onWorkAdded(processingShard);
//    }

    public int getCapacity(Timer workRetrievalTimer) {
        return workers.stream().map(worker -> worker.getQueueCapacity(workRetrievalTimer)).reduce(Integer::sum).orElse(0);
    }

//    /**
//     * Distribute the work in this list fairly across the workers
//     * <p>
//     * Single threaded.
//     */
//    public void distributeToWorkerShards(Map<ProcessingShard<K, V>, List<WorkContainer<K, V>>> workMap) {
//        for (var toProcess : workMap.entrySet()) {
//            var shardKey = toProcess.getKey();
//            PCWorker<K, V, ?> worker = shardWorkerMap.get(shardKey);
//            worker.newWorkMessage(shardKey, toProcess.getValue());
//        }
//    }

//    private void distributeToThreadPool(ArrayDeque<List<WorkContainer<K, V>>> queue) {
//        while (!queue.isEmpty()) {
//            var work = queue.pop();
//            executorPool.submit(() -> log.debug("{}", work));
//        }
//
//    }

//    private void distributeToWorkers(ArrayDeque<List<WorkContainer<K, V>>> queue) {
//        for (var worker : workers) {
//            var poll = ofNullable(queue.poll());
//            poll.ifPresent(worker::enqueue); // todo object allocation warning
//        }
//    }

    @Override
    public void close() throws IOException {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public boolean awaitTermination(long toSeconds, TimeUnit seconds) throws InterruptedException {
        return executorPool.awaitTermination(toSeconds, seconds);
    }

    /**
     * @return aim to never have the pool queue drop below this
     */
    private int getPoolLoadTarget() {
        return options.getTargetAmountOfRecordsInFlight();
    }

    private int getNumberOfUserFunctionsQueued() {
        return executorPool.getQueue().size();
    }

    /**
     * Distribute round robbin to the workers, resuming from the last worker
     */
    public void distributeToWorkers(Map<ProcessingShard<K, V>, List<WorkContainer<K, V>>> workMap) {
        if (workMap.isEmpty()) {
            log.debug("No work to distribute");
        }

        //
        var startPoint = lastWorker == null ? workers.first() : lastWorker;
        var workerTail = workers.tailSet(startPoint);
        if (workerTail.isEmpty()) {
            // loop to start
            workerTail = workers;
        }
        var workerIterator = workerTail.iterator();

        // loops forever over the workers, resuming from last point, until all work is distributed
        for (var work : workMap.entrySet()) {
            // if exhausted, loop back to start
            if (!workerIterator.hasNext()) {
                workerIterator = workers.iterator();
            }
            var workUnits = work.getValue();
            var worker = workerIterator.next();
            lastWorker = worker;
            worker.newWorkMessage(work.getKey(), workUnits);
        }

//        // flatten
//        var queue = new ArrayDeque<List<WorkContainer<K, V>>>();
//        for (var toProcess : workMap.entrySet()) {
//            queue.add(toProcess.getValue());
//        }
//
//        distributeToWorkers(queue);
    }

//    private void distributeToWorkers(Deque<List<WorkContainer<K, V>>> queue) {
//        // round robin
//        // todo can batch?
//
//        while (!queue.isEmpty()) {
//            for (var worker : workers) {
//                var poll = ofNullable(queue.poll());
//                poll.ifPresent(p -> worker.newWorkMessage(p)); // todo object allocation warning
//            }
//        }
//    }

//    /**
//     * Round robyn using queue pop, offer
//     *
//     * @return the worker for the shard
//     */
//    private PCWorker<K, V, ?> maybeDistributeShards(@NonNull ProcessingShard<K, V> shard) throws InterruptedException {
//        var workerOpt = Optional.ofNullable(shardWorkerMap.get(shard));
//        if (workerOpt.isPresent()) {
//            return workerOpt.get();
//        } else {
//            var poll = getWorkers().take();
//            poll.addShardIfMissing(shard);
//            shardWorkerMap.put(shard, poll);
//            getWorkers().offer(poll); // move to back
//            return poll;
//        }
//    }

//    public void addRecord(long epochOfInboundRecords, ConsumerRecord<K, V> aRecord) {
//
//    }
}
