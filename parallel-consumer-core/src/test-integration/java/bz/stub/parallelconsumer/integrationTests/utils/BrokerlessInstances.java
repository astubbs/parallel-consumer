package bz.stub.parallelconsumer.integrationTests.utils;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;

/**
 * Instances for the broker-free lifecycle tests: a real {@link ManagedPCInstance} with a null
 * {@link KafkaClientUtils}, so every guard is the production one while nothing can reach a broker.
 * <p>
 * The null is load-bearing rather than lazy - a {@code run()} that wrongly proceeds past its guards
 * NPEs on the first broker call, which is how those tests tell "aborted correctly" apart from
 * "ran anyway" (see {@link RecordingExecutor#runAll()}, which surfaces that throwable instead of
 * letting the executor swallow it).
 */
public final class BrokerlessInstances {

    private BrokerlessInstances() {
    }

    public static ManagedPCInstance newInstance(String inputTopic) {
        ManagedPCInstance.Config config = ManagedPCInstance.Config.builder()
                .commitMode(CommitMode.PERIODIC_CONSUMER_SYNC)
                .order(ProcessingOrder.UNORDERED)
                .inputTopic(inputTopic)
                .build();
        return new ManagedPCInstance(config, null, key -> {
        });
    }
}
