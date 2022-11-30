package io.confluent.parallelconsumer.internal;

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
public class PCWorker<K, V, R> {

    public static final int DEFUALT_QUEUE_SIZE = 5;
    SimpleMeterRegistry metricsRegistry = new SimpleMeterRegistry();

    Timer userFunctionTimer = metricsRegistry.timer("user.function");

    WorkQueue<K, V> workQueue = new WorkQueue<>();

    PCWorkerPool<K, V, R> parentPool;

    // todo supervise this thread
    public void loop() {
        // todo close thread on shutdown
        while (true) {
            try {
                var poll = workQueue.take();
                process(poll);
            } catch (Exception e) {
                log.error("Error processing work", e);
            }
        }
    }


    /**
     * @return number of {@link WorkContainer} units that should be enqueued
     */
    public int getQueueCapacity(Timer workRetrievalTimer) {
        var target = calculateQuantityToGet(workRetrievalTimer);
        return target - workQueue.size();
    }

    private int calculateQuantityToGet(Timer workRetrievalTimer) {
        var count = workRetrievalTimer.count();
        if (count < DEFUALT_QUEUE_SIZE) {
            return DEFUALT_QUEUE_SIZE;
        }

        var retrieval = workRetrievalTimer.mean(NANOSECONDS);
        var processing = userFunctionTimer.mean(NANOSECONDS);
        var quantity = retrieval / processing;
        return (int) quantity * 2;
    }

    private void process(List<WorkContainer<K, V>> batch) {
        userFunctionTimer.record(() -> {
                    var functionRunner = parentPool.getRunner();
//                    if (functionRunner.isPresent()) {
//                        functionRunner.get().run(work);
                    functionRunner.run(batch);
//                    } else {
//                        throw new IllegalStateException("Function runner not set");
//                    }
                }
        );
    }

    public void enqueue(List<WorkContainer<K, V>> work) {
        workQueue.add(work);
    }

}

