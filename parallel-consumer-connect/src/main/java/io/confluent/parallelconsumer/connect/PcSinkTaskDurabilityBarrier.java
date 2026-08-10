package io.confluent.parallelconsumer.connect;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.streams.PcTaskDispatcher.CompletionHandle;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Turns one lane's {@code preCommit} watermark into per-record durability facts about <b>that lane's own
 * record stream</b> - and nothing else.
 *
 * <p>This is candidate C3 of the offset-composition investigation, and its trick is that it never composes
 * watermarks at all. Composing them is impossible: {@code preCommit} takes and returns
 * {@code Map<TopicPartition, OffsetAndMetadata>}, one scalar per partition meaning a contiguous prefix, so a
 * lane holding a hash-sharded subset of a partition cannot express what it durably wrote. Instead each
 * lane's watermark is read against the records that lane itself received, converted into completions, and
 * Parallel Consumer's existing frontier machinery - which already encodes out-of-order completion - does the
 * composing.
 *
 * <p><b>The invariant.</b> A record at offset {@code o} in partition {@code P}, routed to lane {@code L}, is
 * completed <em>iff</em> {@code L}'s most recent {@code preCommit} return for {@code P} is strictly greater
 * than {@code o}, where the map {@code L} was given contained only offsets {@code L} itself received. No
 * record is ever completed on the strength of another lane's watermark, and no returned watermark is ever
 * routed into a consumer commit.
 *
 * <p>That last clause is load-bearing rather than tidy. {@code WorkerSinkTask} puts no floor under a
 * returned offset - a value below the last committed one is accepted silently and walks the group's
 * committed offset backwards. Because nothing here reaches {@code doCommit}, that hazard cannot be reached.
 *
 * <p><b>Staged, then promoted</b>, mirroring the runtime being patched: an offset enters {@link #staged}
 * when the record is prepared, and only reaches the map handed to {@code preCommit} once that record's
 * {@code put} has returned normally. {@code WorkerSinkTask} does the same thing by promoting
 * {@code origOffsets} into {@code currentOffsets} only after {@code task.put} returns, which is what makes
 * its redelivery non-lossy.
 */
@Slf4j
public class PcSinkTaskDurabilityBarrier {

    /**
     * How a returned watermark is turned into completions. The inverted rule exists so the probe has a
     * negative control: a detector that never fires is indistinguishable from one that cannot.
     */
    public enum ConfirmationRule {
        /** The sound rule: a lane's watermark confirms only records that lane received. */
        OWNING_LANE,
        /**
         * Deliberately wrong - confirms against the highest watermark any lane has returned. Over-commits
         * exactly when one lane runs ahead of another, which is the failure the sound rule exists to avoid.
         * Never select this outside a test.
         */
        HIGHEST_ACROSS_LANES
    }

    private final PcSinkTaskLane lane;

    /**
     * Routed to this lane but not yet handed to the sink. <b>Not confirmable.</b> A watermark can only speak
     * for records the sink actually received, so a record whose {@code put} never ran - or threw - must not
     * be reachable by one, however high it climbs. Keeping these in the same map as the deliverable ones is
     * an over-commit the probe caught: offset 0's {@code put} threw, and a later watermark covering offset 1
     * marked 0 durable too.
     */
    private final Map<TopicPartition, NavigableMap<Long, CompletionHandle>> staged = new HashMap<>();

    /** Handed to the sink and returned from {@code put}. Only these may be confirmed by a watermark. */
    private final Map<TopicPartition, NavigableMap<Long, CompletionHandle>> deliverable = new HashMap<>();

    /** Next-offset per partition over records whose {@code put} has returned. What {@code preCommit} sees. */
    private final Map<TopicPartition, Long> delivered = new HashMap<>();

    /** The highest watermark this lane has returned per partition. Read by the probe's negative control. */
    private final Map<TopicPartition, Long> lastReturned = new HashMap<>();

    public PcSinkTaskDurabilityBarrier(final PcSinkTaskLane lane) {
        this.lane = lane;
    }

    /** Registers a record routed to this lane. Its completion is now owned by this barrier. */
    public synchronized void staged(final TopicPartition partition, final long offset,
                                    final CompletionHandle handle) {
        staged.computeIfAbsent(partition, key -> new TreeMap<>()).put(offset, handle);
    }

    /**
     * Promotes a record whose {@code put} returned: it becomes confirmable, and its offset may now appear in
     * the map handed to {@code preCommit}.
     *
     * <p>Mirrors {@code WorkerSinkTask} promoting {@code origOffsets} into {@code currentOffsets} only after
     * {@code task.put} returns. A record that never reaches here can never be confirmed durable, which is
     * what keeps a failed {@code put} from being swept up by a later watermark.
     */
    public synchronized void delivered(final TopicPartition partition, final long offset) {
        final NavigableMap<Long, CompletionHandle> stagedForPartition = staged.get(partition);
        if (stagedForPartition == null) {
            return;
        }
        final CompletionHandle handle = stagedForPartition.remove(offset);
        if (handle == null) {
            return;
        }
        deliverable.computeIfAbsent(partition, key -> new TreeMap<>()).put(offset, handle);
        delivered.merge(partition, offset + 1, Math::max);
    }

    /** Fails one record outright, releasing it to PC's own failure handling. */
    public synchronized void failed(final TopicPartition partition, final long offset, final Throwable cause) {
        CompletionHandle handle = remove(staged, partition, offset);
        if (handle == null) {
            handle = remove(deliverable, partition, offset);
        }
        if (handle != null) {
            handle.failed(cause);
        }
    }

    private static CompletionHandle remove(final Map<TopicPartition, NavigableMap<Long, CompletionHandle>> from,
                                           final TopicPartition partition, final long offset) {
        final NavigableMap<Long, CompletionHandle> byOffset = from.get(partition);
        return byOffset == null ? null : byOffset.remove(offset);
    }

    /**
     * Asks this lane what it has durably written, and completes whatever that answer covers.
     *
     * <p>Runs off the dispatcher's owner thread on purpose. It holds the lane's lock for the duration -
     * {@code preCommit} is a {@code SinkTask} method and must not interleave with an in-flight {@code put} -
     * and a real connector flushes inside it, so running it on the owner thread would stall the dispatch
     * pump for every lane at once.
     *
     * @param rule which watermark confirms a record; see {@link ConfirmationRule}
     * @param ceilingAcrossLanes the highest watermark any lane has returned per partition, used only by
     *                           {@link ConfirmationRule#HIGHEST_ACROSS_LANES}
     * @return the records confirmed durable by this cycle
     */
    public Map<TopicPartition, OffsetAndMetadata> pollWatermarks() {
        final Map<TopicPartition, OffsetAndMetadata> toCommit = snapshotDelivered();
        if (toCommit.isEmpty()) {
            // Nothing has been put yet. Connect skips the commit entirely in this case rather than calling
            // preCommit with an empty map (WorkerSinkTask.java:417-418), so we do too.
            return Collections.emptyMap();
        }

        final Map<TopicPartition, OffsetAndMetadata> returned = lane.preCommit(toCommit);
        if (returned == null || returned.isEmpty()) {
            // The task opted out. Connect treats this as a successful no-op commit; here it simply means
            // nothing became durable, so nothing completes and the frontier stays where it is.
            return Collections.emptyMap();
        }
        synchronized (this) {
            returned.forEach((partition, offset) -> lastReturned.merge(partition, offset.offset(), Math::max));
        }
        return returned;
    }

    /**
     * Completes whatever {@code watermarks} covers, under {@code rule}.
     *
     * <p>Separated from {@link #pollWatermarks()} so that a cycle gathers <em>every</em> lane's answer before
     * confirming any of them. That ordering is what makes the negative control a control: with the ceiling
     * built from a previous cycle it starts empty, the inverted rule degenerates to the sound one, and the
     * control silently cannot fire.
     */
    public int confirm(final ConfirmationRule rule,
                       final Map<TopicPartition, OffsetAndMetadata> watermarks,
                       final Map<TopicPartition, Long> ceilingAcrossLanes) {
        return applyWatermarks(watermarks, rule, ceilingAcrossLanes);
    }

    private synchronized Map<TopicPartition, OffsetAndMetadata> snapshotDelivered() {
        final Map<TopicPartition, OffsetAndMetadata> snapshot = new HashMap<>();
        delivered.forEach((partition, next) -> snapshot.put(partition, new OffsetAndMetadata(next)));
        return snapshot;
    }

    private synchronized int applyWatermarks(final Map<TopicPartition, OffsetAndMetadata> returned,
                                             final ConfirmationRule rule,
                                             final Map<TopicPartition, Long> ceilingAcrossLanes) {
        int confirmed = 0;
        for (final Map.Entry<TopicPartition, OffsetAndMetadata> entry : returned.entrySet()) {
            final TopicPartition partition = entry.getKey();

            final long watermark = rule == ConfirmationRule.HIGHEST_ACROSS_LANES
                    ? Math.max(entry.getValue().offset(), ceilingAcrossLanes.getOrDefault(partition, 0L))
                    : entry.getValue().offset();

            // deliverable, not staged: a watermark speaks only for records the sink actually received.
            final NavigableMap<Long, CompletionHandle> byOffset = deliverable.get(partition);
            if (byOffset == null) {
                continue;
            }
            // headMap exclusive: a watermark of N means "everything BELOW N", so offset N itself is not
            // covered. Connect's watermarks are last-consumed-plus-one, so an off-by-one here would complete
            // a record the sink has not written - exactly the over-commit this whole barrier prevents.
            final NavigableMap<Long, CompletionHandle> covered = byOffset.headMap(watermark, false);
            for (final CompletionHandle handle : new ArrayList<>(covered.values())) {
                handle.succeeded();
                confirmed++;
            }
            covered.clear();
        }
        return confirmed;
    }

    /** Visible for tests: records routed here but not yet confirmed durable, delivered or not. */
    synchronized int pendingCount() {
        return staged.values().stream().mapToInt(Map::size).sum()
                + deliverable.values().stream().mapToInt(Map::size).sum();
    }
}
