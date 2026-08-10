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
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

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
     *
     * <p>It is also the gap list {@link #advanceDeliveredThrough} reads: an offset still sitting here is one
     * this lane has <em>not</em> received, so nothing above it may be claimed to {@code preCommit}.
     */
    private final Map<TopicPartition, NavigableMap<Long, CompletionHandle>> staged = new HashMap<>();

    /** Handed to the sink and returned from {@code put}. Only these may be confirmed by a watermark. */
    private final Map<TopicPartition, NavigableMap<Long, CompletionHandle>> deliverable = new HashMap<>();

    /**
     * Offsets whose {@code put} returned but which are not yet covered by {@link #deliveredThrough}. Kept
     * because a lane's records are a <b>sparse, out-of-order</b> subset of its partition: distinct keys
     * share a lane and the lane's lock orders nothing, so offset 15 can return from {@code put} before
     * offset 10. Folding each one into the watermark as it lands would claim 16 while 10 is still in flight.
     */
    private final Map<TopicPartition, NavigableSet<Long>> deliveredOffsets = new HashMap<>();

    /**
     * Next-offset per partition over the <b>contiguous prefix</b> of this lane's own received records. What
     * {@code preCommit} sees, and the only number this barrier ever tells the sink about itself.
     *
     * <p>Contiguous over <em>this lane's</em> offsets, not the partition's: a lane holding {0, 5, 9} and
     * having received all three claims 10, because Connect's scalar means "everything I was given, up to
     * here". A gap at an offset this lane never received is not a gap in its own stream.
     */
    private final Map<TopicPartition, Long> deliveredThrough = new HashMap<>();

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
        deliveredOffsets.computeIfAbsent(partition, key -> new TreeSet<>()).add(offset);
        advanceDeliveredThrough(partition);
    }

    /**
     * Moves the watermark to the top of the contiguous prefix, and no further.
     *
     * <p>Deliberately <b>not</b> {@code max(delivered) + 1}. That was a real over-report: two keys sharing a
     * lane are serialised but not ordered, so offsets 10 and 15 can return from {@code put} in either order,
     * and taking the max after 15 lands tells the sink it received through 16 while 10 is still in flight.
     * For a connector that overrides {@code flush} - and {@code SinkTask}'s base {@code preCommit} is
     * {@code flush(offsets); return offsets;} - that is an instruction to flush offsets it was never given.
     *
     * <p>The stop line is the lowest offset still {@link #staged}: everything below it that this lane
     * received is genuinely in the sink's hands. Offsets at or below the new watermark are dropped from
     * {@link #deliveredOffsets}, which is what keeps it bounded.
     */
    private void advanceDeliveredThrough(final TopicPartition partition) {
        final NavigableSet<Long> received = deliveredOffsets.get(partition);
        if (received == null || received.isEmpty()) {
            return;
        }
        final NavigableMap<Long, CompletionHandle> outstanding = staged.get(partition);
        final Long lowestOutstanding = outstanding == null || outstanding.isEmpty() ? null : outstanding.firstKey();
        // Strictly below the gap: `lower`, not `floor`. The outstanding offset itself is the thing not received.
        final Long prefixTop = lowestOutstanding == null ? received.last() : received.lower(lowestOutstanding);
        if (prefixTop == null) {
            return;
        }
        deliveredThrough.merge(partition, prefixTop + 1, Math::max);
        received.headSet(prefixTop, true).clear();
    }

    /**
     * Fails one record outright, releasing it to PC's own failure handling.
     *
     * <p>A record that failed <em>before</em> reaching the sink keeps its place in {@link #staged} rather
     * than being removed. That is the same thing {@code WorkerSinkTask} does by not promoting
     * {@code origOffsets} when {@code task.put} throws: the lane did not receive it, so the lane may not
     * claim past it. Removing it would let the watermark step over an offset the sink never took, which is
     * the over-claim this class exists to prevent. A retry re-stages the offset and replaces the handle;
     * this dispatcher disables retries, so in practice such a record pins this lane's watermark for good -
     * exactly as it pins PC's own frontier.
     */
    public synchronized void failed(final TopicPartition partition, final long offset, final Throwable cause) {
        final NavigableMap<Long, CompletionHandle> stagedForPartition = staged.get(partition);
        final CompletionHandle stagedHandle = stagedForPartition == null ? null : stagedForPartition.get(offset);
        if (stagedHandle != null) {
            stagedHandle.failed(cause);
            return;
        }
        // Delivered but not yet confirmed: the sink did receive it, so the watermark stays honest. Drop it
        // so no later watermark can confirm a record PC has already been told failed.
        final NavigableMap<Long, CompletionHandle> deliverableForPartition = deliverable.get(partition);
        final CompletionHandle handle =
                deliverableForPartition == null ? null : deliverableForPartition.remove(offset);
        if (handle != null) {
            handle.failed(cause);
        }
    }

    /**
     * Asks this lane what it has durably written, and returns its answer verbatim. Confirms nothing - that
     * is {@link #confirm}'s job, and the split between them is what makes the negative control a control.
     *
     * <p>Runs off the dispatcher's owner thread on purpose. It holds the lane's lock for the duration -
     * {@code preCommit} is a {@code SinkTask} method and must not interleave with an in-flight {@code put} -
     * and a real connector flushes inside it, so running it on the owner thread would stall the dispatch
     * pump for every lane at once.
     *
     * @return whatever this lane's {@code preCommit} returned, or an empty map if it was not asked or
     *         declined to answer
     */
    public Map<TopicPartition, OffsetAndMetadata> pollWatermarks() {
        final Map<TopicPartition, OffsetAndMetadata> toCommit = snapshotDeliveredThrough();
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
        return returned;
    }

    /**
     * Completes whatever {@code watermarks} covers, under {@code rule}.
     *
     * <p>Separated from {@link #pollWatermarks()} so that a cycle gathers <em>every</em> lane's answer before
     * confirming any of them. That ordering is what makes the negative control a control: with the ceiling
     * built from a previous cycle it starts empty, the inverted rule degenerates to the sound one, and the
     * control silently cannot fire.
     *
     * @param rule which watermark confirms a record; see {@link ConfirmationRule}
     * @param watermarks this lane's own answer, as returned by {@link #pollWatermarks()}
     * @param ceilingAcrossLanes the highest watermark any lane returned this cycle, per partition. Read only
     *                           by {@link ConfirmationRule#HIGHEST_ACROSS_LANES}
     * @return how many of this lane's records the answer confirmed durable
     */
    public synchronized int confirm(final ConfirmationRule rule,
                                    final Map<TopicPartition, OffsetAndMetadata> watermarks,
                                    final Map<TopicPartition, Long> ceilingAcrossLanes) {
        int confirmed = 0;
        for (final Map.Entry<TopicPartition, OffsetAndMetadata> entry : watermarks.entrySet()) {
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

    /**
     * What this lane would hand {@code preCommit} right now: its own contiguous received prefix, per
     * partition. Package-private so the probe can assert on the claim itself rather than only on what a
     * scripted task chose to return for it.
     */
    synchronized Map<TopicPartition, OffsetAndMetadata> snapshotDeliveredThrough() {
        final Map<TopicPartition, OffsetAndMetadata> snapshot = new HashMap<>();
        deliveredThrough.forEach((partition, next) -> snapshot.put(partition, new OffsetAndMetadata(next)));
        return snapshot;
    }
}
