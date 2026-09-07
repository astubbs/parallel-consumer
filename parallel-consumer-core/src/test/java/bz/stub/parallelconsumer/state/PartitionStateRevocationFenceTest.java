package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.EpochAndRecordsMap;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import org.apache.kafka.common.TopicPartition;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The revocation fence as a mechanism: once set, every container of the partition reads as stale at the current
 * epoch, so nothing starts or produces for it, while the partition's own commit data is untouched. The composition
 * - the control thread setting it inside the producer write lock between a revocation commit's drain and its commit,
 * and a worker meeting it after the produce lock - is exercised at broker level by {@code RebalanceEoSDeadlockTest},
 * which measured the duplicates the fence exists to stop. {@link PartitionState#fenceForRevocation} owns the why.
 */
class PartitionStateRevocationFenceTest {

    @Test
    void aFencedPartitionsWorkIsStaleAtTheCurrentEpochAndItsCommitDataIsNot() {
        var module = new PCModuleTestEnv();
        var mu = new ModelUtils(module);
        var wm = module.workManager();
        wm.onPartitionsAssigned(mu.getPartitions());
        var pm = wm.getPm();
        var partitionState = pm.getPartitionState(mu.getPartition());

        // two records of the one assignment, taken together: the first completed and drained, so the partition is
        // dirty; the second still fresh and in hand, as a worker parked on the produce lock would hold it
        var tp = mu.getPartition();
        wm.registerWork(new EpochAndRecordsMap<>(ModelUtils.pollOf(tp, 0, 1), pm));
        var taken = wm.getWorkIfAvailable();
        assertWithMessage("fixture: both registered containers must be selectable").that(taken).hasSize(2);
        var completed = taken.get(0);
        completed.onUserFunctionSuccess();
        wm.handleFutureResult(completed);
        var work = taken.get(1);

        assertWithMessage("precondition: at the current epoch the work is fresh, or the fence proves nothing")
                .that(wm.checkIfWorkIsStale(work))
                .isFalse();
        assertWithMessage("precondition: the completed offset made the partition dirty")
                .that(wm.isDirty())
                .isTrue();

        wm.fenceForRevocation(epochsNow(wm, mu.getPartitions()));

        assertWithMessage("fenced: the same container at the same epoch is stale - the epoch did not move, the "
                + "fence is what says so")
                .that(wm.checkIfWorkIsStale(work))
                .isTrue();
        assertWithMessage("fenced: the shard will not hand the container out as work")
                .that(partitionState.couldBeTakenAsWork(work))
                .isFalse();
        assertWithMessage("fenced: the completed work drained before the fence still commits - the fence stops "
                + "new work, not the commit it precedes")
                .that(wm.collectCommitDataForDirtyPartitions())
                .containsKey(mu.getPartition());
    }

    /**
     * A revocation can name a partition that has no state - one whose assignment failed after its epoch was recorded
     * (astubbs#451) - and the served pass runs on the control thread, where a throw ends the instance. Nothing was
     * ever dispatched for such a partition, so there is nothing to fence and the call is a no-op, like the sweep.
     */
    @Test
    void fencingAPartitionWithNoStateIsANoOp() {
        var module = new PCModuleTestEnv();
        var mu = new ModelUtils(module);
        var wm = module.workManager();
        var neverAssigned = new TopicPartition(mu.getTopic(), 7);

        wm.fenceForRevocation(UniMaps.of(neverAssigned, 0L));

        assertWithMessage("the fence tolerated the missing state rather than throwing on the control thread")
                .that(wm.getPm().getPartitionState(neverAssigned))
                .isNull();
    }

    /**
     * The served pass can run after its waiter timed out, truncated, and the partition came back to this instance
     * under a new epoch. A fence posted for the old generation must not touch the new one - or the fresh assignment
     * reads as stale until the next rebalance. Found by the independent cross-model review of the fix.
     */
    @Test
    void aFencePostedForAnOlderEpochLeavesTheReassignedPartitionAlone() {
        var module = new PCModuleTestEnv();
        var mu = new ModelUtils(module);
        var wm = module.workManager();
        var tp = mu.getPartition();
        wm.onPartitionsAssigned(mu.getPartitions());
        var epochsWhenRevoked = epochsNow(wm, mu.getPartitions());

        // the waiter gave up: truncation, then the same partition assigned again to this instance
        wm.onPartitionsRevoked(mu.getPartitions());
        wm.onPartitionsAssigned(mu.getPartitions());
        wm.registerWork(new EpochAndRecordsMap<>(ModelUtils.pollOf(tp, 0), wm.getPm()));
        var freshWork = wm.getWorkIfAvailable();
        assertWithMessage("fixture: the re-assignment accepted new work").that(freshWork).hasSize(1);

        // the late pass serves the OLD request
        wm.fenceForRevocation(epochsWhenRevoked);

        assertWithMessage("the fresh assignment must not be fenced by a revocation of the previous generation")
                .that(wm.checkIfWorkIsStale(freshWork.get(0)))
                .isFalse();
    }

    /**
     * A fence reaching an already-truncated partition finds the shared removed-state singleton, which reads as
     * stale already and must never be mutated - it is one object for every removed partition in the process.
     */
    @Test
    void aFenceOnAnAlreadyTruncatedPartitionLeavesTheRemovedSingletonAlone() {
        var module = new PCModuleTestEnv();
        var mu = new ModelUtils(module);
        var wm = module.workManager();
        wm.onPartitionsAssigned(mu.getPartitions());
        var epochsWhenRevoked = epochsNow(wm, mu.getPartitions());
        wm.onPartitionsRevoked(mu.getPartitions());
        var removed = wm.getPm().getPartitionState(mu.getPartition());
        assertWithMessage("fixture: truncation installed the removed state").that(removed.isRemoved()).isTrue();

        wm.fenceForRevocation(epochsWhenRevoked);

        assertWithMessage("the removed singleton is untouched - it is shared by every removed partition")
                .that(removed)
                .isSameInstanceAs(RemovedPartitionState.getSingleton());
    }

    /** The epochs a revocation callback would capture when it posts its request. */
    private static Map<TopicPartition, Long> epochsNow(WorkManager<String, String> wm, Collection<TopicPartition> partitions) {
        Map<TopicPartition, Long> epochs = new HashMap<>(partitions.size());
        for (TopicPartition partition : partitions) {
            epochs.put(partition, wm.getPm().getEpochOfPartition(partition));
        }
        return epochs;
    }
}
