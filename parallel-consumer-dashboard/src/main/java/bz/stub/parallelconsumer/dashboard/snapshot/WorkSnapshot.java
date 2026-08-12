package bz.stub.parallelconsumer.dashboard.snapshot;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Builder;
import lombok.Value;
import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * Instance-wide work state - what PC is holding, running and waiting on right now.
 * <p>
 * <strong>Absent is not zero.</strong> Every field is boxed and {@code null} means the meter was not present. See
 * {@link PcSnapshot} for why that distinction is kept all the way to the page.
 * <p>
 * Experimental: the dashboard module is opt-in and its API may change without notice.
 */
@InterfaceStability.Unstable
@Value
public class WorkSnapshot {

    /**
     * {@code pc.inflight.records} - records currently being processed or waiting for a retry.
     */
    Long inflightRecords;

    /**
     * {@code pc.waiting.records} - records downloaded and waiting to be selected for processing.
     */
    Long waitingRecords;

    /**
     * {@code pc.shards} - number of shards (the ordering-key buckets work is queued into).
     */
    Long shards;

    /**
     * {@code pc.shards.size} - records queued across all shards.
     */
    Long shardsSize;

    /**
     * {@code pc.incomplete.offsets.total} - incomplete offsets across every assigned partition.
     */
    Long incompleteOffsetsTotal;

    /**
     * {@code pc.partitions.paused} - partitions the broker poller has paused for back-pressure.
     */
    Long pausedPartitions;

    /**
     * {@code pc.partitions.number} - partitions currently assigned.
     */
    Long numberOfPartitions;

    /**
     * {@code pc.dynamic.load.factor} - the multiple of max concurrency PC is currently buffering to. A ratio rather
     * than a count, so it stays a double.
     */
    Double dynamicLoadFactor;

    @Builder(toBuilder = true)
    WorkSnapshot(Long inflightRecords,
                 Long waitingRecords,
                 Long shards,
                 Long shardsSize,
                 Long incompleteOffsetsTotal,
                 Long pausedPartitions,
                 Long numberOfPartitions,
                 Double dynamicLoadFactor) {
        this.inflightRecords = inflightRecords;
        this.waitingRecords = waitingRecords;
        this.shards = shards;
        this.shardsSize = shardsSize;
        this.incompleteOffsetsTotal = incompleteOffsetsTotal;
        this.pausedPartitions = pausedPartitions;
        this.numberOfPartitions = numberOfPartitions;
        this.dynamicLoadFactor = dynamicLoadFactor;
    }
}
