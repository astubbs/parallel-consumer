package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.state.Batch;
import io.confluent.parallelconsumer.state.ProcessingShard;
import io.confluent.parallelconsumer.state.QueuedWorkManager;
import io.confluent.parallelconsumer.state.WorkContainer;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * @author Antony Stubbs
 */
@Slf4j
@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
// todo rename worker
public class PCWorker<K, V, R> {

    SimpleMeterRegistry metricsRegistry = new SimpleMeterRegistry();

    Timer userFunctionTimer = metricsRegistry.timer("user.function");

    WorkQueue<K, V> workQueue = new WorkQueue<>();

    PCWorkerPool<K, V, R> parentPool;

    QueuedWorkManager<K, V> wm;

    Batcher<WorkContainer<K, V>> batcher = new Batcher<>();

    public void loop() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
//                Batch<K, V> work = acquireFromWm();
                var work = aquireFromQueue();
                var batches = batcher.makeBatches(work);
//                var work = acquireFromWmBatch();
//                var batches = batcher.makeBatches(work.getValues());
                process(batches);
            } catch (Exception e) {
                log.error("Error acquiring work from work manager", e);
            }
        }
    }

    private Batch<WorkContainer<K, V>> acquireFromWmBatch() throws InterruptedException {
        return wm.take();
    }

//    private Batch<K, V> acquireFromWm() throws InterruptedException {
//        return wm.take();
//    }

    private void process(List<List<WorkContainer<K, V>>> listOfBatches) {
        userFunctionTimer.record(() -> {
                    var functionRunner = parentPool.getRunner();
//                    if (functionRunner.isPresent()) {
//                        functionRunner.get().run(work);
                    for (var batch : listOfBatches) {
                        functionRunner.run(batch);
                    }
//                    } else {
//                        throw new IllegalStateException("Function runner not set");
//                    }
                }
        );
    }

    public int getQueueCapacity(Timer workRetrievalTimer) {
        return calculateQuantityToGet(workRetrievalTimer) - workQueue.size();
    }

    private int calculateQuantityToGet(Timer workRetrievalTimer) {
        var retrieval = workRetrievalTimer.mean(NANOSECONDS);
        var processing = userFunctionTimer.mean(NANOSECONDS);
        var quantity = retrieval / processing;
        return (int) quantity * 2;
    }

    private List<WorkContainer<K, V>> aquireFromQueue() throws InterruptedException {
        return workQueue.poll();
    }

    public void enqueue(List<WorkContainer<K, V>> work) {
        workQueue.add(work);
    }

    public void addShard(ProcessingShard<K, V> shard) {
        workQueue.add(shard);
    }
}

