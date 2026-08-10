package io.confluent.parallelconsumer.connect;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.confluent.parallelconsumer.state.ShardKey;
import io.confluent.parallelconsumer.streams.PcTaskDispatcher;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Routes each record to the one lane that owns its key, so several {@link org.apache.kafka.connect.sink.SinkTask}
 * instances can share a partition without ever running concurrently for the same key.
 *
 * <p>This is the piece the whole direction turns on. Connect gives a task whole partitions, which caps
 * useful concurrency at the partition count; sharding by key instead is what buys concurrency above it,
 * and it is only sound because each key still lands in exactly one lane, in offset order.
 *
 * <p>Lane selection uses Parallel Consumer's own {@link ShardKey} under {@link ProcessingOrder#KEY} rather
 * than a second key implementation. That is not tidiness: {@code ShardKey} decides deep array equality,
 * null-key handling and topic-partition participation, and a private copy of those rules would drift from
 * the ones {@code WorkManager} actually enforces - so two records PC considers one shard could reach two
 * lanes and run concurrently, which is precisely the thing the lock exists to prevent.
 *
 * <p>Null keys are therefore bound to their topic-partition, because that is what {@code ShardKey} does
 * with them; they keep source order within a partition and gain no concurrency, which is correct - a null
 * key carries no identity to order by.
 *
 * <p><b>Out of scope.</b> This router does not construct, start or stop tasks, does not convert with
 * Connect's real {@code Converter}s, and never calls {@code preCommit}, {@code flush}, or an offset
 * committer. Completion means the callback returned. Composing task watermarks with PC's frontier is the
 * next design step and deliberately absent here.
 */
@Slf4j
public class PcSinkTaskLaneRouter implements PcTaskDispatcher.WorkPreparer {

    private final List<PcSinkTaskLane> lanes;
    private final RecordProjection projection;

    /** Turns a raw record into the {@link SinkRecord} the task sees. */
    public interface RecordProjection {
        SinkRecord project(ConsumerRecord<byte[], byte[]> record);
    }

    public PcSinkTaskLaneRouter(final List<PcSinkTaskLane> lanes, final RecordProjection projection) {
        if (lanes == null || lanes.isEmpty()) {
            throw new IllegalArgumentException("at least one lane is required");
        }
        // Java 8 API surface: this module compiles at --release 8, so List.copyOf is unavailable.
        this.lanes = Collections.unmodifiableList(new ArrayList<>(lanes));
        this.projection = Objects.requireNonNull(projection, "projection");
    }

    /**
     * Runs on the dispatcher's owner thread. Chooses the lane and projects the record here, so the worker
     * is handed nothing but the call - keeping every shared-state read on the single-threaded side.
     */
    @Override
    public Runnable prepare(final ConsumerRecord<byte[], byte[]> record) {
        final PcSinkTaskLane lane = laneFor(record);
        final SinkRecord projected = projection.project(record);
        return () -> lane.put(Collections.singletonList(projected));
    }

    /**
     * The lane owning this record's key.
     *
     * <p>{@code floorMod} rather than {@code %}: a hash can be negative, and {@code %} would then yield a
     * negative index. Also note the mapping is by hash, so distinct keys can share a lane - they are then
     * serialised against each other, which costs throughput but never correctness.
     */
    PcSinkTaskLane laneFor(final ConsumerRecord<byte[], byte[]> record) {
        final ShardKey shard = ShardKey.of(record, ProcessingOrder.KEY);
        return lanes.get(Math.floorMod(shard.hashCode(), lanes.size()));
    }

    public int laneCount() {
        return lanes.size();
    }
}
