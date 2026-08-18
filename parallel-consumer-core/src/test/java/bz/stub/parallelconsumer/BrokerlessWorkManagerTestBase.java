package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.state.ModelUtils;
import bz.stub.parallelconsumer.state.PartitionStateManager;
import bz.stub.parallelconsumer.state.ShardManager;
import bz.stub.parallelconsumer.state.WorkManager;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;

/**
 * A {@link WorkManager} and its collaborators, wired without a broker.
 * <p>
 * Extracted because two tests had built this same fixture independently - identical fields and an
 * identical {@code setup()} differing only in whether partitions were assigned - and the
 * duplicate-code check had been reporting it on every push since 2026-08-13. Sharing it is also what
 * the repo asks for: extend the shared helpers rather than growing a parallel copy, because a
 * drifted copy of setup logic has already been a flaky-CI source here.
 * <p>
 * <b>Assignment is deliberately not done for you.</b> Whether partitions are assigned before the
 * test body is the one thing these fixtures disagreed about, and it is load-bearing in both
 * directions: {@code ShardManagerStaleContainerTest} needs an epoch-0 assignment to make work stale
 * against, while {@code EpochAndRecordsMapRaceTest} must NOT assign, because the race it reproduces
 * is precisely a poll arriving before {@code onPartitionsAssigned}. Override
 * {@link #assignPartitionsIfWanted()} to opt in.
 */
public abstract class BrokerlessWorkManagerTestBase {

    protected final ModelUtils mu = new ModelUtils();

    protected WorkManager<String, String> wm;
    protected ShardManager<String, String> sm;
    protected PartitionStateManager<String, String> pm;

    protected final String topic = "topic";
    protected final TopicPartition tp = new TopicPartition(topic, 0);

    @BeforeEach
    void buildWorkManagerFixture() {
        PCModuleTestEnv module = mu.getModule();
        wm = module.workManager();
        sm = wm.getSm();
        pm = wm.getPm();
        assignPartitionsIfWanted();
    }

    /**
     * Called at the end of fixture setup. Does nothing by default: a subclass that needs an
     * assignment says so, rather than every subclass inheriting one it may be trying to avoid.
     */
    protected void assignPartitionsIfWanted() {
        // no-op by default - see the class javadoc for why this is not the common case
    }
}
