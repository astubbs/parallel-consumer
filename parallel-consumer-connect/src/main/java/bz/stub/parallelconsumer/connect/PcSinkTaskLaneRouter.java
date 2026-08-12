package bz.stub.parallelconsumer.connect;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.connect.PcSinkTaskDurabilityBarrier.ConfirmationRule;
import bz.stub.parallelconsumer.state.ShardKey;
import bz.stub.parallelconsumer.streams.PcTaskDispatcher;
import bz.stub.parallelconsumer.streams.PcTaskDispatcher.CompletionHandle;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
public class PcSinkTaskLaneRouter implements PcTaskDispatcher.DeferringWorkPreparer {

    private final List<PcSinkTaskLane> lanes;
    private final List<PcSinkTaskDurabilityBarrier> barriers;
    private final RecordProjection projection;
    private final ConfirmationRule rule;

    /** Turns a raw record into the {@link SinkRecord} the task sees. */
    public interface RecordProjection {
        SinkRecord project(ConsumerRecord<byte[], byte[]> record);
    }

    public PcSinkTaskLaneRouter(final List<PcSinkTaskLane> lanes, final RecordProjection projection) {
        this(lanes, projection, ConfirmationRule.OWNING_LANE);
    }

    /**
     * @param rule which watermark confirms a record. Anything but
     *             {@link ConfirmationRule#OWNING_LANE} is a deliberately-broken control arm for the probe.
     */
    public PcSinkTaskLaneRouter(final List<PcSinkTaskLane> lanes, final RecordProjection projection,
                                final ConfirmationRule rule) {
        if (lanes == null || lanes.isEmpty()) {
            throw new IllegalArgumentException("at least one lane is required");
        }
        // Java 8 API surface: this module compiles at --release 8, so List.copyOf is unavailable.
        this.lanes = Collections.unmodifiableList(new ArrayList<>(lanes));
        final List<PcSinkTaskDurabilityBarrier> built = new ArrayList<>(this.lanes.size());
        for (final PcSinkTaskLane lane : this.lanes) {
            built.add(new PcSinkTaskDurabilityBarrier(lane));
        }
        this.barriers = Collections.unmodifiableList(built);
        this.projection = Objects.requireNonNull(projection, "projection");
        this.rule = Objects.requireNonNull(rule, "rule");
    }

    /**
     * Runs on the dispatcher's owner thread. Chooses the lane and projects the record here, so the worker
     * is handed nothing but the call - keeping every shared-state read on the single-threaded side.
     *
     * <p>The record is <b>not</b> completed when this {@link Runnable} returns: {@code put} returning means
     * the sink has the record, not that it wrote it. The record is staged against its lane's barrier and
     * completed later, when that lane's own {@code preCommit} declares a watermark covering it.
     */
    @Override
    public Runnable prepare(final ConsumerRecord<byte[], byte[]> record, final CompletionHandle handle) {
        final int index = laneIndexFor(record);
        final PcSinkTaskLane lane = lanes.get(index);
        final PcSinkTaskDurabilityBarrier barrier = barriers.get(index);
        final SinkRecord projected = projection.project(record);
        final TopicPartition partition = new TopicPartition(record.topic(), record.partition());

        barrier.staged(partition, record.offset(), handle);
        return () -> {
            try {
                lane.put(Collections.singletonList(projected));
            } catch (RuntimeException | Error e) {
                // Without this the throw reaches the dispatcher, which fails the WorkContainer directly and
                // never tells the barrier - leaving the record staged with a live handle nothing will ever
                // report. Routing it through the barrier releases that handle exactly once; the dispatcher's
                // own catch then finds the completion already reported and adds nothing.
                barrier.failed(partition, record.offset(), e);
                throw e;
            }
            // Promoted only now, exactly as WorkerSinkTask promotes origOffsets into currentOffsets only
            // after task.put returns (WorkerSinkTask.java:616). A throw skips this, so the offset never
            // reaches preCommit and the record cannot be confirmed durable by a later cycle.
            barrier.delivered(partition, record.offset());
        };
    }

    /**
     * Asks every lane what it has durably written, completing whatever the answers cover.
     *
     * <p>Call this off the dispatcher's owner thread - it holds each lane's lock in turn and a connector
     * flushes inside {@code preCommit}.
     *
     * <p><b>One lane's failure is that lane's alone.</b> {@code preCommit} is where a sink reports that its
     * store is unreachable, and a connector throwing there used to abort the whole cycle: lanes later in the
     * list were never asked, and because confirmation runs only after every watermark is gathered, no lane
     * completed anything. That is the head-of-line stall this module exists to remove, reappearing one level
     * up. A throwing lane now contributes an empty answer and the cycle carries on without it.
     *
     * <p><b>Retried, not quarantined.</b> A thrown {@code preCommit} is a connector's transient-failure
     * signal, and {@code WorkerSinkTask} itself logs the failed commit and tries again on the next interval
     * rather than retiring the task; diverging from the runtime being patched would need a reason, and one
     * network blip is not it. Retrying is safe because a throw confirms nothing: an empty answer covers no
     * offset, so the lane's records stay in the barrier's {@code deliverable} map, PC's frontier holds at the
     * lowest incomplete offset, and a later successful cycle completes the whole backlog. Quarantining would
     * also need an un-quarantine path, which nothing here has.
     *
     * <p>Only {@link RuntimeException} is caught. An {@link Error} means the JVM is in trouble rather than
     * the connector, and continuing the cycle around it would hide that.
     *
     * @return the number of records confirmed durable, and any lane that failed this cycle
     */
    public DurabilityCycleResult runDurabilityCycle() {
        // Two phases on purpose. Gather every lane's watermark BEFORE confirming any of them, so the
        // cross-lane ceiling describes this cycle rather than the previous one. Built from the previous
        // cycle it starts empty, the inverted rule degenerates into the sound one, and the probe's negative
        // control silently cannot fire - which is how it first shipped, and what the probe caught.
        final List<Map<TopicPartition, OffsetAndMetadata>> watermarks = new ArrayList<>(barriers.size());
        final Map<TopicPartition, Long> ceiling = new HashMap<>();
        final Map<Integer, RuntimeException> failures = new LinkedHashMap<>();
        for (int index = 0; index < barriers.size(); index++) {
            Map<TopicPartition, OffsetAndMetadata> returned;
            try {
                returned = barriers.get(index).pollWatermarks();
            } catch (RuntimeException e) {
                failures.put(index, e);
                // Empty, not partial: this lane says nothing this cycle, so it neither confirms its own
                // records nor raises the cross-lane ceiling the inverted control rule reads.
                returned = Collections.emptyMap();
                log.warn("Lane {} preCommit failed; its records stay incomplete and it will be asked again "
                        + "next cycle. The other {} lane(s) are unaffected.", index, lanes.size() - 1, e);
            }
            watermarks.add(returned);
            returned.forEach((partition, offset) -> ceiling.merge(partition, offset.offset(), Math::max));
        }

        int confirmed = 0;
        for (int index = 0; index < barriers.size(); index++) {
            confirmed += barriers.get(index).confirm(rule, watermarks.get(index), ceiling);
        }
        return new DurabilityCycleResult(confirmed, failures);
    }

    /**
     * What one durability cycle did: how much it confirmed, and which lanes could not answer.
     *
     * <p>The failures are returned rather than logged and dropped because a lane that never answers never
     * completes a record, and the only symptom otherwise is a frontier that quietly stops advancing. A caller
     * driving the cycle is the one placed to escalate that - retire the lane, alert, or stop the worker.
     */
    public static final class DurabilityCycleResult {

        private final int confirmed;
        private final Map<Integer, RuntimeException> failures;

        private DurabilityCycleResult(final int confirmed, final Map<Integer, RuntimeException> failures) {
            this.confirmed = confirmed;
            this.failures = Collections.unmodifiableMap(failures);
        }

        /** How many records were confirmed durable across every lane that answered. */
        public int confirmed() {
            return confirmed;
        }

        /** Lane index to the exception it threw, in lane order. Empty when every lane answered. */
        public Map<Integer, RuntimeException> failures() {
            return failures;
        }

        public boolean hasFailures() {
            return !failures.isEmpty();
        }
    }

    /**
     * The lane owning this record's key.
     *
     * <p>{@code floorMod} rather than {@code %}: a hash can be negative, and {@code %} would then yield a
     * negative index. Also note the mapping is by hash, so distinct keys can share a lane - they are then
     * serialised against each other, which costs throughput but never correctness.
     */
    PcSinkTaskLane laneFor(final ConsumerRecord<byte[], byte[]> record) {
        return lanes.get(laneIndexFor(record));
    }

    private int laneIndexFor(final ConsumerRecord<byte[], byte[]> record) {
        final ShardKey shard = ShardKey.of(record, ProcessingOrder.KEY);
        return Math.floorMod(shard.hashCode(), lanes.size());
    }

    public int laneCount() {
        return lanes.size();
    }
}
