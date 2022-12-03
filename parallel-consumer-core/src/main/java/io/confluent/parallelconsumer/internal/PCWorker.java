package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.state.Batch;
import io.confluent.parallelconsumer.state.QueuedWorkManager;
import io.confluent.parallelconsumer.state.WorkContainer;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * @author Antony Stubbs
 */
@Slf4j
@Value
// todo rename worker
public class PCWorker<K, V, R> {

    SimpleMeterRegistry metricsRegistry = new SimpleMeterRegistry();

    Timer userFunctionTimer = metricsRegistry.timer("user.function");

    WorkQueue<K, V> workQueue = new WorkQueue<>();

    PCWorkerPool<K, V, R> parentPool;

    QueuedWorkManager<K, V> wm;

    public void loop() throws InterruptedException {
        while (true) {
            var work = aquireFromWm();
            process(work);
        }
    }

    private Batch<K, V> aquireFromWm() throws InterruptedException {
        return wm.take();
    }

    private void process(Batch<K, V> batch) {
        userFunctionTimer.record(() -> {
                    var functionRunner = parentPool.getRunner();
//                    if (functionRunner.isPresent()) {
//                        functionRunner.get().run(work);
                    functionRunner.run(batch.getValues());
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

    private List<WorkContainer<K, V>> aquireFromQueue() {
        return workQueue.poll();
    }

    public void enqueue(List<WorkContainer<K, V>> work) {
        workQueue.add(work);
    }
}

