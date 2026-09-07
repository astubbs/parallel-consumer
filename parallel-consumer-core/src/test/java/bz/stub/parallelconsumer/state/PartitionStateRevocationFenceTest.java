package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.EpochAndRecordsMap;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;
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
        var records = new ConsumerRecords<>(UniMaps.of(tp, UniLists.of(
                new ConsumerRecord<>(tp.topic(), tp.partition(), 0, "key-0", "value"),
                new ConsumerRecord<>(tp.topic(), tp.partition(), 1, "key-1", "value"))));
        wm.registerWork(new EpochAndRecordsMap<>(records, pm));
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

        wm.fenceForRevocation(mu.getPartitions());

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

        wm.fenceForRevocation(UniLists.of(neverAssigned));

        assertWithMessage("the fence tolerated the missing state rather than throwing on the control thread")
                .that(wm.getPm().getPartitionState(neverAssigned))
                .isNull();
    }
}
