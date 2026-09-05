package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.offsets.CorruptOffsetMetadataException;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.Map;
import java.util.Set;

import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Reproduces the partial-assignment error path that {@code PartitionStateManager.resetOffsetMapAndRemoveWork} could
 * not survive: {@code onPartitionsAssigned} records a partition's epoch <em>before</em> it loads the partition's
 * state, so anything thrown by {@code loadPartitionStateForAssignment} leaves the partition with an epoch and no
 * state. Kafka keeps the partition assigned regardless - the classic coordinator assigns in {@code SubscriptionState}
 * before invoking the listener, and a throwing listener is logged and rethrown out of {@code poll()} - so the next
 * rebalance, or {@code close()}, revokes it. The revoke sweep then read {@code partitionStates.get(tp)} as
 * {@code null} and dereferenced it.
 * <p>
 * The hazard is where that throws from: the revoke listener runs on the broker-poll thread inside
 * {@code consumer.poll}, so an escape is the poller-death shape, on top of an assignment failure that has already
 * been logged at error level. There is nothing to sweep for a partition whose state was never installed - no work
 * was registered against it and no shard references it - and its epoch is advanced by the same revoke before the
 * sweep runs, so anything that somehow carried the old epoch is already fenced. The sweep therefore marks the
 * partition removed, says why, and moves on, exactly as it would have if the state had been there and empty.
 * <p>
 * <b>Two production routes into the partial state, both driven here:</b>
 * <ul>
 *     <li>{@code consumer.committed()} throwing - a broker or authorisation failure while reading committed offsets,
 *     simulated by a {@link MockConsumer} whose {@code committed(Set)} throws;</li>
 *     <li>{@link InvalidOffsetMetadataHandlingPolicy#FAIL} rejecting unreadable metadata - the opt-in route
 *     {@code docs/inflight/core-fail-policy-escapes-the-rebalance-callback.md} records, with real committed metadata
 *     that is not base64.</li>
 * </ul>
 * <b>Before the fix, both were RED</b> with {@code NullPointerException} out of {@code onPartitionsRevoked}: the
 * assignment threw as expected, and the revoke that followed threw as well. The healthy-assignment control is green
 * either way, so a failure above is the sweep's and not the fixture's.
 * <p>
 * <b>What astubbs/parallel-consumer#345 did and did not cover.</b> That fix is the single-read {@code getShard(key)}
 * idiom in {@code ShardManager.removeWorkFromShardFor}, one call deeper on this same sweep, against a shard removed
 * by the control thread between two reads. This null is a different one: a state that was never installed, on a
 * single thread, with no race - the two only share the symptom family. Its PR body dismissed
 * {@code partitionStates} from the check-then-get sweep because nothing removes from that map, which is true and is
 * not this path.
 *
 * @author Antony Stubbs
 */
class PartitionStateManagerRevokeAfterFailedAssignmentTest {

    static final TopicPartition TP = new TopicPartition("myTopic", 3);

    /** Metadata no decoder can read: under {@code FAIL} it throws; under the default {@code IGNORE} it is discarded. */
    static final OffsetAndMetadata UNREADABLE_COMMIT = new OffsetAndMetadata(100L, "%% not base64 %%");

    /**
     * A consumer whose committed-offset lookup fails the way a broker or an authorisation problem would, part way
     * through the assignment callback.
     */
    static class CommittedLookupFails extends MockConsumer<String, String> {
        CommittedLookupFails() {
            super(OffsetResetStrategy.EARLIEST);
        }

        @Override
        public synchronized Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) {
            throw new KafkaException("simulated: committed() failed while loading the assignment");
        }
    }

    @Test
    void revokeAfterCommittedLookupThrewDuringAssignmentDoesNotThrow() {
        WorkManager<String, String> wm = workManagerWith(new CommittedLookupFails(), InvalidOffsetMetadataHandlingPolicy.IGNORE);

        assertThrows(KafkaException.class, () -> wm.onPartitionsAssigned(UniLists.of(TP)),
                "the assignment must fail - this is the error path under test");

        assertRevokeSurvivesThePartialState(wm);
    }

    @Test
    void revokeAfterFailPolicyRejectedTheMetadataDuringAssignmentDoesNotThrow() {
        MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.assign(UniLists.of(TP));
        consumer.commitSync(UniMaps.of(TP, UNREADABLE_COMMIT));
        WorkManager<String, String> wm = workManagerWith(consumer, InvalidOffsetMetadataHandlingPolicy.FAIL);

        assertThrows(CorruptOffsetMetadataException.class, () -> wm.onPartitionsAssigned(UniLists.of(TP)),
                "FAIL must reject the metadata and escape the assignment - this is the error path under test");

        assertRevokeSurvivesThePartialState(wm);
    }

    /**
     * Control: the same assignment and revoke with nothing failing. Green before and after the fix, so the tests
     * above fail because of the partial state and not because the fixture cannot revoke at all.
     */
    @Test
    void revokeAfterAHealthyAssignmentSweepsTheState() {
        WorkManager<String, String> wm = workManagerWith(new MockConsumer<>(OffsetResetStrategy.EARLIEST), InvalidOffsetMetadataHandlingPolicy.IGNORE);
        PartitionStateManager<String, String> pm = wm.getPm();

        wm.onPartitionsAssigned(UniLists.of(TP));
        assertWithMessage("a healthy assignment installs the state").that(pm.getPartitionState(TP)).isNotNull();

        assertDoesNotThrow(() -> wm.onPartitionsRevoked(UniLists.of(TP)));
        assertWithMessage("the revoke replaces the state with the removed marker").that(pm.getPartitionState(TP).isRemoved()).isTrue();
        assertWithMessage("the revoke advances the epoch").that(pm.getEpochOfPartition(TP)).isEqualTo(1L);
    }

    private static void assertRevokeSurvivesThePartialState(WorkManager<String, String> wm) {
        PartitionStateManager<String, String> pm = wm.getPm();
        assertWithMessage("the partial state: the epoch was recorded before the load failed")
                .that(pm.getEpochOfPartition(TP)).isEqualTo(0L);
        assertWithMessage("the partial state: no PartitionState was installed")
                .that(pm.getPartitionState(TP)).isNull();

        assertDoesNotThrow(() -> wm.onPartitionsRevoked(UniLists.of(TP)),
                "revoking a partition whose assignment failed must not escape the rebalance listener");

        assertWithMessage("the revoke still marks the partition removed, so a reassignment reads as a reassignment")
                .that(pm.getPartitionState(TP).isRemoved()).isTrue();
        assertWithMessage("the revoke still advances the epoch, fencing anything that carried the old one")
                .that(pm.getEpochOfPartition(TP)).isEqualTo(1L);
    }

    private static WorkManager<String, String> workManagerWith(MockConsumer<String, String> consumer,
                                                               InvalidOffsetMetadataHandlingPolicy policy) {
        var module = new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder()
                .consumer(consumer)
                .invalidOffsetMetadataPolicy(policy)
                .build());
        return module.workManager();
    }
}
