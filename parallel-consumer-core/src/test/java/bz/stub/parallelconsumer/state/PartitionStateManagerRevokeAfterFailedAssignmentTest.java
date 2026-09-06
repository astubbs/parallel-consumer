package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy;
import bz.stub.parallelconsumer.internal.EpochAndRecordsMap;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.offsets.CorruptOffsetMetadataException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Reproduces the partial-assignment error path that {@code PartitionStateManager.resetOffsetMapAndRemoveWork} could
 * not survive: {@code onPartitionsAssigned} records a partition's epoch <em>before</em> it loads the partition's
 * state, so anything thrown by {@code loadPartitionStateForAssignment} leaves the partition with an epoch and no
 * state. Kafka keeps the partition assigned regardless: the classic coordinator applies the assignment in
 * {@code SubscriptionState} before invoking the listener, and {@code ConsumerCoordinator.onJoinComplete} rethrows the
 * listener's exception out of {@code poll()} with the member STABLE and its heartbeat thread running. The revoke sweep
 * then read {@code partitionStates.get(tp)} as {@code null} and dereferenced it.
 * <p>
 * <b>How the sweep is reached after that - traced in September 2026, and it is not "the next rebalance".</b> The
 * exception does not stop at the callback: {@code PartitionStateManager.onPartitionsAssigned} logs and rethrows,
 * {@code AbstractParallelEoSStreamProcessor.onPartitionsAssigned} has no catch, {@code ConsumerManager.poll} catches
 * only {@code SaslAuthenticationException} and {@code WakeupException}, and {@code BrokerPollSystem.controlLoop}'s
 * catch notifies the committer and rethrows. The broker-poll thread ends there, so there is no next poll for a
 * rebalance to arrive through. What remains is the close sequence, on the <em>control</em> thread:
 * {@code brokerPollSubsystem.supervise()} surfaces the dead poller's future, {@code supervisorLoop} runs
 * {@code doClose}, and {@code maybeCloseConsumer} closes the consumer - whose {@code AbstractCoordinator.close} runs
 * {@code onLeavePrepare}, which invokes {@code onPartitionsRevoked} (or {@code onPartitionsLost}; both reach
 * {@code onPartitionsRemoved} and this sweep) and only then {@code maybeLeaveGroup}. That step is gated on
 * {@code committer instanceof ProducerManager}, so the sweep is a live path only in
 * {@code PERIODIC_TRANSACTIONAL_PRODUCER} mode. In the default consumer-commit modes the thread that would have
 * closed the consumer is the poll thread, and it is gone - nothing reaches the sweep, and the guard under test is
 * insurance there rather than a live path. That gap is its own defect, recorded in
 * {@code docs/inflight/bug-poller-death-leaves-the-consumer-open-in-consumer-commit-modes.md}.
 * <p>
 * <b>What the old throw cost, on the route that is live.</b> {@code AbstractCoordinator.close} throws out of
 * {@code onLeavePrepare} <em>before</em> it reaches {@code maybeLeaveGroup}, and {@code ClassicKafkaConsumer.close}
 * swallows that to carry on closing the network client. So the NPE did not just add a stack trace to a close that
 * was already reporting the assignment failure: it skipped the LeaveGroup, leaving this member's partitions assigned
 * to it until the coordinator's session timeout expired. Fail-open is still the right shape for the guard: there is
 * nothing to sweep for a partition whose state was never installed - no work was registered against it and no shard
 * references it - and its epoch is advanced by the same revoke before the sweep runs, so anything that somehow
 * carried the old epoch is already fenced. The sweep therefore marks the partition removed, says why, and moves on,
 * exactly as it would have if the state had been there and empty.
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
 * <b>The mixed-collection case is what the fix changed for the partitions that DO have state.</b> Kafka hands the
 * listener the whole revoked set in one call, and the old throw aborted the {@code for} loop at the first stateless
 * partition, leaving every partition after it unswept - state live, work still in its shard. The test below revokes
 * a stateless and a stateful partition together, stateless first, and asserts the stateful one is swept. Before the
 * fix it was RED on three counts at once: the NPE escaped, the stateful partition's state was never replaced, and its
 * record was still queued in its shard.
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

    /** The partition whose assignment fails, leaving it with an epoch and no state. */
    static final TopicPartition TP = new TopicPartition("myTopic", 3);

    /** A partition whose assignment succeeds, so the revoke sweep has real state and real work to remove for it. */
    static final TopicPartition HEALTHY_TP = new TopicPartition("myTopic", 4);

    /** Metadata no decoder can read: under {@code FAIL} it throws; under the default {@code IGNORE} it is discarded. */
    static final OffsetAndMetadata UNREADABLE_COMMIT = new OffsetAndMetadata(100L, "%% not base64 %%");

    /**
     * A consumer whose committed-offset lookup fails the way a broker or an authorisation problem would, part way
     * through the assignment callback - but only when the lookup includes the doomed partition, so the same consumer
     * can assign a healthy partition first and fail the next one.
     */
    static class CommittedLookupFailsFor extends MockConsumer<String, String> {
        private final TopicPartition doomed;

        CommittedLookupFailsFor(TopicPartition doomed) {
            super(OffsetResetStrategy.EARLIEST);
            this.doomed = doomed;
        }

        @Override
        public synchronized Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) {
            if (partitions.contains(doomed)) {
                throw new KafkaException("simulated: committed() failed while loading the assignment of " + doomed);
            }
            return super.committed(partitions);
        }
    }

    @Test
    void revokeAfterCommittedLookupThrewDuringAssignmentDoesNotThrow() {
        WorkManager<String, String> wm = workManagerWith(new CommittedLookupFailsFor(TP),
                InvalidOffsetMetadataHandlingPolicy.IGNORE);

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
     * One revoke call carrying a stateless partition and a stateful one, stateless FIRST. The order is what the
     * assertion turns on, and it is guaranteed rather than hoped for: {@code UniLists.of} returns an immutable
     * {@link List} that iterates in argument order, {@code onPartitionsRemoved} passes that same collection straight
     * through, and {@code resetOffsetMapAndRemoveWork} walks it with a plain for-each. So the sweep meets {@link #TP}
     * before {@link #HEALTHY_TP}, which is where the old code threw - and everything after it went unswept.
     * <p>
     * The three post-revoke checks are gathered with {@code assertAll} on purpose, so a red run reports all of them:
     * it is the combination - the escape AND the live state AND the still-queued record - that shows the loop was
     * aborted rather than merely noisy.
     */
    @Test
    void revokingAStatelessPartitionAlongsideAStatefulOneStillSweepsTheStatefulOne() {
        WorkManager<String, String> wm = workManagerWith(new CommittedLookupFailsFor(TP),
                InvalidOffsetMetadataHandlingPolicy.IGNORE);
        PartitionStateManager<String, String> pm = wm.getPm();

        wm.onPartitionsAssigned(UniLists.of(HEALTHY_TP));
        registerOneRecordOn(wm, HEALTHY_TP);
        assertWithMessage("fixture: the healthy partition has state and one record queued in its shard")
                .that(wm.getNumberOfWorkQueuedInShardsAwaitingSelection()).isEqualTo(1);

        assertThrows(KafkaException.class, () -> wm.onPartitionsAssigned(UniLists.of(TP)),
                "the second assignment must fail - this is the error path under test");
        assertWithMessage("fixture: the partial state - an epoch and no PartitionState")
                .that(pm.getPartitionState(TP)).isNull();

        List<TopicPartition> statelessFirst = UniLists.of(TP, HEALTHY_TP);
        Throwable escaped = revokeCapturingWhatEscapes(wm, statelessFirst);

        assertAll("one revoke, the stateless partition iterated first",
                () -> assertWithMessage("nothing escapes the rebalance listener").that(escaped).isNull(),
                () -> assertWithMessage("the stateless partition is still marked removed")
                        .that(pm.getPartitionState(TP).isRemoved()).isTrue(),
                () -> assertWithMessage("the stateful partition, iterated second, is swept - its state replaced")
                        .that(pm.getPartitionState(HEALTHY_TP).isRemoved()).isTrue(),
                () -> assertWithMessage("the stateful partition's queued record is gone from its shard")
                        .that(wm.getNumberOfWorkQueuedInShardsAwaitingSelection()).isEqualTo(0),
                () -> assertWithMessage("both epochs advanced, fencing anything that carried the old ones")
                        .that(UniLists.of(pm.getEpochOfPartition(TP), pm.getEpochOfPartition(HEALTHY_TP)))
                        .containsExactly(1L, 1L));
    }

    /**
     * Control: the same assignment and revoke with nothing failing. Green before and after the fix, so the tests
     * above fail because of the partial state and not because the fixture cannot revoke at all.
     */
    @Test
    void revokeAfterAHealthyAssignmentSweepsTheState() {
        WorkManager<String, String> wm = workManagerWith(new MockConsumer<>(OffsetResetStrategy.EARLIEST),
                InvalidOffsetMetadataHandlingPolicy.IGNORE);
        PartitionStateManager<String, String> pm = wm.getPm();

        wm.onPartitionsAssigned(UniLists.of(TP));
        assertWithMessage("a healthy assignment installs the state").that(pm.getPartitionState(TP)).isNotNull();

        assertDoesNotThrow(() -> wm.onPartitionsRevoked(UniLists.of(TP)));
        assertWithMessage("the revoke replaces the state with the removed marker")
                .that(pm.getPartitionState(TP).isRemoved()).isTrue();
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

    /** Registers one record for the partition: its state then holds an incomplete offset, its shard a container. */
    private static void registerOneRecordOn(WorkManager<String, String> wm, TopicPartition tp) {
        var record = new ConsumerRecord<>(tp.topic(), tp.partition(), 0, "key-0", "value");
        var records = new ConsumerRecords<>(UniMaps.of(tp, UniLists.of(record)));
        wm.registerWork(new EpochAndRecordsMap<>(records, wm.getPm()));
    }

    /**
     * Runs the revoke and hands back whatever escaped, or null. A plain {@code assertDoesNotThrow} would stop the
     * test at the escape; the mixed-collection test needs to go on and report the state the escape left behind.
     */
    private static Throwable revokeCapturingWhatEscapes(WorkManager<String, String> wm,
                                                        List<TopicPartition> partitions) {
        try {
            wm.onPartitionsRevoked(partitions);
            return null;
        } catch (RuntimeException escaped) {
            return escaped;
        }
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
