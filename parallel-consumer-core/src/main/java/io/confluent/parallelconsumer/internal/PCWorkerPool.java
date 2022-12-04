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
import lombok.SneakyThrows;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static java.util.Optional.ofNullable;
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

    List<PCWorker<K, V, R>> workers;

    Batcher batcher = new Batcher();

    @SneakyThrows
    public PCWorkerPool(int poolSize, FunctionRunner<K, V, R> functionRunner, PCModule<K, V> module) {
        runner = functionRunner;
        this.options = module.options();
        executorPool = createExecutorPool(options.getMaxConcurrency(), module.workManager());
        var qwm = new QueuedWorkManager<K, V>(module.queuedShardManager());
        workers = Range.range(poolSize).toStream().boxed()
                .map(ignore -> new PCWorker<>(this, qwm))
                .collect(Collectors.toList());

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

    public int getCapacity(Timer workRetrievalTimer) {
        return workers.stream().map(worker -> worker.getQueueCapacity(workRetrievalTimer)).reduce(Integer::sum).orElse(0);
    }

    /**
     * Distribute the work in this list fairly across the workers
     */
    public void distribute(List<WorkContainer<K, V>> workToProcess) {
        var batches = batcher.makeBatches(workToProcess);

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
}
