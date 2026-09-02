package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.KafkaUtils;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.EpochAndRecordsMap;
import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Optional;
import java.util.SortedSet;

/**
 * No op version of {@link PartitionState} used for when partition assignments are removed, to avoid managing null
 * references or {@link Optional}s. By replacing with a no op implementation, we protect for stale messages still in
 * queues which reference it, among other things.
 * <p>
 * The alternative to this implementation, is having {@link PartitionStateManager#getPartitionState(TopicPartition)}
 * return {@link Optional}, which forces the implicit null check everywhere partition state is retrieved. This was
 * drafted to a degree, but found to be extremely invasive, where this solution with decent separation of concerns and
 * encapsulation, is sufficient and potentially more useful as is non-destructive. Potential issue is that of memory
 * leak as the collection will forever expand. However, even massive partition counts to a single consumer would be in
 * the hundreds of thousands, this would only result in hundreds of thousands of {@link TopicPartition} object keys all
 * pointing to the same instance of {@link RemovedPartitionState}.
 *
 * @author Antony Stubbs
 */
@Slf4j
public class RemovedPartitionState<K, V> extends PartitionState<K, V> {

    /**
     * What a <b>removed</b> partition answers when asked which of its offsets are still incomplete: nothing, because
     * the partition is gone and PC no longer tracks work for it.
     * <p>
     * This class is the null-object stand-in installed when a partition is revoked or lost, so every getter here
     * answers the "there is no state" version of its question rather than making callers null-check. Returning a
     * shared constant rather than a fresh set per call matters because the offset encoders ask this on the commit
     * path, per partition.
     * <p>
     * <b>Immutable, and that is load-bearing.</b> Shared by every partition and every PC in the JVM, so the name is
     * a promise something has to keep. It was a {@code new TreeSet<>()} - a shared, mutable static behind a name
     * promising the opposite. One caller mutating what it received would have corrupted what every
     * other caller sees, for the life of the JVM, with the field name saying that could not happen. Every caller
     * that reaches it - through {@link #getIncompleteOffsetsBelow(long)}, or through the unbounded form that
     * delegates to it - reads only, so making it genuinely immutable changes nothing today and turns that silent
     * corruption into an immediate {@code UnsupportedOperationException} if a future one does not.
     */
    private static final SortedSet<Long> READ_ONLY_EMPTY_SET = Collections.emptySortedSet();

    private static final PartitionState singleton = new RemovedPartitionState<>();

    public static final String NO_OP = "no-op";
    public static final int NO_EPOCH = -1;

    public RemovedPartitionState() {
        super(NO_EPOCH, new PCModule<>(ParallelConsumerOptions.<K, V>builder().build()), null, OffsetMapCodecManager.HighestOffsetAndIncompletes.of());
    }

    public static PartitionState getSingleton() {
        return RemovedPartitionState.singleton;
    }

    @Override
    public boolean isRemoved() {
        // by definition true in this implementation
        return true;
    }

    @Override
    public TopicPartition getTp() {
        return null;
    }

    @Override
    public void maybeRegisterNewPollBatchAsWork(@NonNull EpochAndRecordsMap<K, V>.RecordsAndEpoch recordsAndEpoch) {
        // no-op
        log.warn("Dropping polled record batch for partition no longer assigned. WC: {}", recordsAndEpoch);
    }

    /**
     * Don't allow more records to be processed for this partition. Eventually these records triggering this check will
     * be cleaned out.
     *
     * @return always returns false
     */
    @Override
    boolean isAllowedMoreRecords() {
        log.debug(NO_OP);
        return true;
    }

    /**
     * Guards both entry points: {@link #getIncompleteOffsetsBelowHighestSucceeded()} delegates through this bounded
     * overload in the base class, so overriding here keeps the no-op contract whichever one a caller reaches for.
     */
    @Override
    public SortedSet<Long> getIncompleteOffsetsBelow(long highestSucceededBound) {
        log.debug(NO_OP);
        return READ_ONLY_EMPTY_SET;
    }

    @Override
    public long getOffsetHighestSeen() {
        log.debug(NO_OP);
        return PartitionState.KAFKA_OFFSET_ABSENCE;
    }

    @Override
    public long getOffsetHighestSucceeded() {
        log.debug(NO_OP);
        return PartitionState.KAFKA_OFFSET_ABSENCE;
    }

    @Override
    public boolean isRecordPreviouslyCompleted(final ConsumerRecord<K, V> rec) {
        log.debug("Ignoring previously completed request for partition no longer assigned. Partition: {}", KafkaUtils.toTopicPartition(rec));
        return false;
    }

    @Override
    public boolean hasIncompleteOffsets() {
        return false;
    }

    @Override
    public int getNumberOfIncompleteOffsets() {
        return 0;
    }

    @Override
    public void onSuccess(long offset) {
        log.debug("Dropping completed work container for partition no longer assigned. WC: {}, partition: {}", offset, getTp());
    }

    @Override
    public boolean isPartitionRemovedOrNeverAssigned() {
        return true;
    }

    /**
     * The one inherited mutator that was never overridden, and the only one a <b>different</b> thread can reach
     * after removal.
     * <p>
     * A commit already in flight when its partitions are revoked completes afterwards and calls
     * {@code PartitionStateManager#onOffsetCommitSuccess}, which resolves the now-removed partition to this
     * <b>shared singleton</b>. Without this override that lands in {@link PartitionState#onOffsetCommitSuccess},
     * writing {@code lastCommittedOffset} and {@code stateChangedSinceCommitStart} - plain, non-volatile fields -
     * on the one instance every removed partition in the JVM points at, from the control thread, unsynchronised.
     * <p>
     * Nothing has been observed to break, because {@code getAssignedPartitions()} filters on
     * {@link #isPartitionRemovedOrNeverAssigned()} and so nothing reads those fields back off the singleton. That
     * is an accidental safety net in another class, not a property of this one - and
     * astubbs/parallel-consumer#44 leaned on it, because declining the revoke commit means truncation no longer
     * waits for an in-flight commit to finish. Made explicit rather than left incidental.
     * <p>
     * <b>Cleared 2026-09-02.</b> The remaining inherited mutators were walked: {@code onFailure} has an empty
     * body, and {@code addNewIncompleteRecord}'s only caller {@code maybeRegisterNewPollBatchAsWork} is itself
     * overridden here - so this was the one reachable write. <b>What would reopen it:</b> a new mutator added to
     * {@link PartitionState} without a no-op override here, or any reader of {@code lastCommittedOffset} /
     * {@code stateChangedSinceCommitStart} that stops filtering on
     * {@link #isPartitionRemovedOrNeverAssigned()}. Nothing enforces either - the ArchUnit rules in this repo do
     * not check override completeness against a base class.
     */
    @Override
    public void onOffsetCommitSuccess(OffsetAndMetadata committed) {
        log.debug("Ignoring commit success for partition no longer assigned. Committed: {}, partition: {}", committed, getTp());
    }
}
