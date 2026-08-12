package io.confluent.parallelconsumer.dashboard.snapshot;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Builder;
import lombok.Value;
import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * One row of per-topic-partition state, as it was at the instant the enclosing {@link PcSnapshot} was captured.
 * <p>
 * <strong>Absent is not zero.</strong> Every quantity here is a boxed type and {@code null} means "the meter that
 * supplies this was not present in the registry". A partition that has processed no records reports {@code 0}; a
 * partition whose counter has never been created reports {@code null}. The page draws those differently - see
 * {@link PcSnapshot} for the rationale.
 * <p>
 * Experimental: the dashboard module is opt-in and its API may change without notice.
 */
@InterfaceStability.Unstable
@Value
public class PartitionSnapshot {

    /**
     * Topic name, from the {@code topic} meter tag. Never null.
     */
    String topic;

    /**
     * Partition number, from the {@code partition} meter tag.
     */
    int partition;

    /**
     * {@code pc.partition.highest.seen.offset} - the furthest offset PC has taken delivery of.
     */
    Long highestSeenOffset;

    /**
     * {@code pc.partition.highest.completed.offset} - the highest offset that has succeeded, which may sit above a
     * gap of incomplete offsets below it. This is the marker that makes PC's "running ahead of the commit" visible.
     */
    Long highestCompletedOffset;

    /**
     * {@code pc.partition.highest.sequential.succeeded.offset} - the highest offset below which nothing is
     * incomplete, i.e. the highest offset that would be safe to commit.
     */
    Long highestSequentialSucceededOffset;

    /**
     * {@code pc.partition.latest.committed.offset} - what has actually been committed to the broker.
     * <p>
     * <strong>This is the NEXT offset to consume, not the last one done.</strong> Kafka's commit is exclusive, and
     * {@code PartitionState.getOffsetToCommit()} publishes {@code highestSequentialSucceeded + 1}, so on a partition
     * that is completely caught up this reads one <em>above</em> every other marker here. Carried on the wire exactly
     * as the gauge reports it - it is the number the broker holds and the offset a restart resumes from - and
     * converted to its inclusive form only where it is compared against the other three. See
     * {@link #isOffsetOrderingConsistent()}.
     */
    Long lastCommittedOffset;

    /**
     * {@code pc.partition.incomplete.offsets} - count of offsets seen but not yet succeeded.
     */
    Long incompleteOffsets;

    /**
     * {@code pc.partition.assignment.epoch} - increments each time this partition is re-assigned to this instance.
     */
    Long assignmentEpoch;

    /**
     * {@code pc.processed.records} counter for this partition. Micrometer counters are doubles; kept as a double so
     * a very long-running counter is not silently truncated.
     */
    Double processedRecords;

    /**
     * {@code pc.failed.records} counter for this partition.
     */
    Double failedRecords;

    /**
     * {@code pc.slow.records} counter for this partition - records that waited longer than the configured threshold.
     */
    Double slowRecords;

    @Builder(toBuilder = true)
    PartitionSnapshot(String topic,
                      int partition,
                      Long highestSeenOffset,
                      Long highestCompletedOffset,
                      Long highestSequentialSucceededOffset,
                      Long lastCommittedOffset,
                      Long incompleteOffsets,
                      Long assignmentEpoch,
                      Double processedRecords,
                      Double failedRecords,
                      Double slowRecords) {
        this.topic = topic;
        this.partition = partition;
        this.highestSeenOffset = highestSeenOffset;
        this.highestCompletedOffset = highestCompletedOffset;
        this.highestSequentialSucceededOffset = highestSequentialSucceededOffset;
        this.lastCommittedOffset = lastCommittedOffset;
        this.incompleteOffsets = incompleteOffsets;
        this.assignmentEpoch = assignmentEpoch;
        this.processedRecords = processedRecords;
        this.failedRecords = failedRecords;
        this.slowRecords = slowRecords;
    }

    /**
     * Stable identity for this row, e.g. {@code my-topic-3}. Used as the key a later unit's table and ribbon render
     * against, so a partition keeps its row across snapshots.
     */
    public String getKey() {
        return topic + "-" + partition;
    }

    /**
     * Whether the four offset markers are in the only order they can legally be in:
     * committed &lt;= sequential-succeeded &lt;= completed &lt;= seen.
     * <p>
     * This is the invariant the offset ribbon is drawn from - if it can be violated the graphic is meaningless, so
     * it is asserted rather than assumed. Markers that are absent are skipped rather than treated as zero: a missing
     * meter must not be able to report a false violation.
     * <p>
     * The committed marker is normalised to {@link #committedInclusive()} first. The other three are inclusive
     * positions - the highest offset that is seen, succeeded, sequentially succeeded - while the committed gauge is
     * exclusive, so comparing them raw reports every <em>caught-up</em> partition as violating the invariant, which is
     * the one partition state that is unambiguously healthy.
     */
    public boolean isOffsetOrderingConsistent() {
        Long committed = committedInclusive();
        return notAbove(committed, highestSequentialSucceededOffset)
                && notAbove(highestSequentialSucceededOffset, highestCompletedOffset)
                && notAbove(highestCompletedOffset, highestSeenOffset)
                // also check the pairs that skip an absent middle marker, so one missing meter does not hide a
                // genuine inversion between the markers either side of it
                && notAbove(committed, highestCompletedOffset)
                && notAbove(committed, highestSeenOffset)
                && notAbove(highestSequentialSucceededOffset, highestSeenOffset);
    }

    /**
     * {@link #lastCommittedOffset} as an inclusive position - the highest offset actually committed - so it is
     * comparable with the other three markers. Null in, null out: an absent meter stays absent.
     */
    private Long committedInclusive() {
        return lastCommittedOffset == null ? null : lastCommittedOffset - 1;
    }

    private static boolean notAbove(Long lower, Long upper) {
        if (lower == null || upper == null) {
            return true;
        }
        return lower <= upper;
    }
}
