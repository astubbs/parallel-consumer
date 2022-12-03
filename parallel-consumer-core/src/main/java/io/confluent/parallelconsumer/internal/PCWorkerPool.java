package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.csid.utils.Range;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.state.QueuedWorkManager;
import io.confluent.parallelconsumer.state.WorkContainer;
import io.confluent.parallelconsumer.state.WorkManager;
import io.micrometer.core.instrument.Timer;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Optional.ofNullable;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @author Antony Stubbs
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

    List<PCWorker<K, V, R>> workers;

    public PCWorkerPool(int poolSize, FunctionRunner<K, V, R> functionRunner, PCModule<K, V> module) {
        runner = functionRunner;
        this.options = module.options();
        executorPool = createExecutorPool(options.getMaxConcurrency(), module.workManager());
        workers = Range.range(poolSize).toStream().boxed()
                .map(ignore -> new PCWorker<>(this))
                .collect(Collectors.toList());
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

        //LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
        var workQueue = new QueuedWorkManager<K, V>(wm);

        return new ThreadPoolExecutor(poolSize, poolSize, 0L, MILLISECONDS, workQueue,
                namingThreadFactory, rejectionHandler);
    }

    public int getCapacity(Timer workRetrievalTimer) {
        return workers.stream().map(worker -> worker.getQueueCapacity(workRetrievalTimer)).reduce(Integer::sum).orElse(0);
    }

    /**
     * Distribute the work in this list fairly across the workers
     */
    public void distribute(List<WorkContainer<K, V>> workToProcess) {
        var batches = makeBatches(workToProcess);

        var queue = new ArrayDeque<>(batches);
        distributeToThreadPool(queue);
//        distributeToWorkers(queue);
    }

    private void distributeToThreadPool(ArrayDeque<List<WorkContainer<K, V>>> queue) {
        while (!queue.isEmpty()) {
            var work = queue.pop();
            executorPool.submit(() -> log.debug("{}", work));
        }

    }

    private void distributeToWorkers(ArrayDeque<List<WorkContainer<K, V>>> queue) {
        for (var worker : workers) {
            var poll = ofNullable(queue.poll());
            poll.ifPresent(worker::enqueue); // todo object allocation warning
        }
    }

    private List<List<WorkContainer<K, V>>> makeBatches(List<WorkContainer<K, V>> workToProcess) {
        int maxBatchSize = options.getBatchSize();
        var batches = partition(workToProcess, maxBatchSize);

        // debugging
        if (log.isDebugEnabled()) {
            var sizes = batches.stream().map(List::size).sorted().collect(Collectors.toList());
            log.debug("Number batches: {}, smallest {}, sizes {}", batches.size(), sizes.stream().findFirst().get(), sizes);
            List<Integer> integerStream = sizes.stream().filter(x -> x < (int) options.getBatchSize()).collect(Collectors.toList());
            if (integerStream.size() > 1) {
                log.warn("More than one batch isn't target size: {}. Input number of batches: {}", integerStream, batches.size());
            }
        }

        return batches;
    }

    private static <T> List<List<T>> partition(Collection<T> sourceCollection, int maxBatchSize) {
        List<List<T>> listOfBatches = new ArrayList<>();
        List<T> batchInConstruction = new ArrayList<>();

        //
        for (T item : sourceCollection) {
            batchInConstruction.add(item);

            //
            if (batchInConstruction.size() == maxBatchSize) {
                listOfBatches.add(batchInConstruction);
                batchInConstruction = new ArrayList<>(); // todo object allocation warning
            }
        }

        // add partial tail
        if (!batchInConstruction.isEmpty()) {
            listOfBatches.add(batchInConstruction);
        }

        log.debug("sourceCollection.size() {}, batches: {}, batch sizes {}",
                sourceCollection.size(),
                listOfBatches.size(),
                listOfBatches.stream().map(List::size).collect(Collectors.toList()));
        return listOfBatches;
    }

    @Override
    public void close() throws IOException {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public boolean awaitTermination(long toSeconds, TimeUnit seconds) throws InterruptedException {
        throw new UnsupportedOperationException("Not implemented yet");
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
}
