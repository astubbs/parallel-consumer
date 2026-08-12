package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.PollContextInternal;
import io.confluent.parallelconsumer.state.WorkContainer;
import io.confluent.parallelconsumer.state.WorkManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Provides a set of methods for testing internal and configuration based interfaces of
 * {@link AbstractParallelEoSStreamProcessor}.
 */
public class TestParallelEoSStreamProcessor<K, V> extends AbstractParallelEoSStreamProcessor<K, V> {
    public TestParallelEoSStreamProcessor(final ParallelConsumerOptions<K, V> newOptions) {
        super(newOptions);
    }

    /**
     * Lets a test supply its own {@link PCModule}, so components like the {@link DynamicLoadFactor} can be wired
     * through the DI system rather than poked in afterwards.
     */
    public TestParallelEoSStreamProcessor(final ParallelConsumerOptions<K, V> newOptions, final PCModule<K, V> module) {
        super(newOptions, module);
    }

    public int getTargetLoad() { return getQueueTargetLoaded(); }

    /**
     * Runs a single control-loop pressure check - the pass which decides whether to step the loading factor up, and
     * what to report when it cannot.
     */
    public void runPipelinePressureCheck() {
        checkPipelinePressure();
    }

    /**
     * The pressure check only acts when the last work request was fulfilled; the control loop sets that as it
     * distributes work, so a test driving {@link #runPipelinePressureCheck()} directly must say so itself.
     */
    public void markLastWorkRequestFulfilled() {
        setLastWorkRequestWasFulfilled(true);
    }

    public  <R> List<Tuple<ConsumerRecord<K, V>, R>> runUserFunc(
            Function<PollContextInternal<K, V>, List<R>> dummyFunction,
            Consumer<R> callback,
            final List<WorkContainer<K, V>> activeWorkContainers) {

        return super.runUserFunction(dummyFunction, callback , activeWorkContainers);
    }

    public void setWm(WorkManager wm) {
        super.wm = wm;
    }

    public long getMailBoxSuccessCnt() {
        return super.getWorkMailBox().stream()
                .filter(kvControllerEventMessage -> {
                    WorkContainer<K, V> wc = kvControllerEventMessage.getWorkContainer();
                    return (wc != null && wc.isUserFunctionSucceeded());
                })
                .count();
    }

    public long getMailBoxFailedCnt() {
        return super.getWorkMailBox().stream()
                .filter(kvControllerEventMessage -> {
                    WorkContainer<K, V> wc = kvControllerEventMessage.getWorkContainer();
                    return (wc != null && !wc.isUserFunctionSucceeded());
                })
                .count();
    }
}
