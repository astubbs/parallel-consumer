package bz.stub.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.PollContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * The per-key ORDERING half of the chaos correctness ledger - the guarantee the library exists for
 * ("key concurrency without losing per-key order"), asserted under the same churn the loss/duplicate
 * half of the ledger ({@link ProgressProbe#ledger}) runs under.
 *
 * <h2>The guarantee this asserts, and the window it holds in</h2>
 * <p>
 * <b>Within one PC incarnation, one partition, one partition-assignment epoch and one record key, the
 * records of that key are executed strictly one at a time, in ascending offset order.</b> Two
 * violations follow from that, and this class reports exactly those two:
 * <ul>
 *   <li>{@code LEDGER_KEY_ORDER} - a later-started delivery has a LOWER offset than one already started
 *   in the same window (an out-of-order regression).</li>
 *   <li>{@code LEDGER_KEY_CONCURRENCY} - two deliveries of the same window overlapped in flight (the
 *   serialisation half of the same promise; an ordering bug can show up as either).</li>
 * </ul>
 * <p>
 * The window is not decoration - it is the whole difficulty, because a NAIVE "offsets per key must
 * increase" fires on correct behaviour. Each of the four components excludes one class of legitimate
 * re-processing that at-least-once delivery under churn produces on purpose:
 * <ul>
 *   <li><b>key</b> - PC orders per key ONLY; two different keys share no order (that is the
 *   concurrency). {@code ShardKey.KeyOrderedKey} is the shard identity in
 *   {@link bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder#KEY} mode.</li>
 *   <li><b>partition</b> - {@code ShardKey.KeyOrderedKey} carries the record's {@code TopicPartition},
 *   not just the topic, so the shard (and therefore the order) is per key PER PARTITION.</li>
 *   <li><b>epoch</b> - the assignment generation, incremented per partition on BOTH revoke and assign
 *   ({@code PartitionStateManager.incrementPartitionAssignmentEpoch}). When an assignment moves,
 *   uncommitted work is redelivered from the last commit, so an earlier offset is legitimately
 *   processed after a later one. That opens a NEW window; it does not violate the old one. The epoch
 *   is read off the record's own {@code WorkContainer}, so a heavy record still in flight when its
 *   partition is revoked keeps the OLD epoch - it cannot be mistaken for the new window's work.</li>
 *   <li><b>incarnation</b> - a chaos restart builds a fresh {@code ParallelEoSStreamProcessor}, whose
 *   epoch counter starts again at zero. Scoping to the (stable) chaos instance id would collide epoch
 *   0 of two unrelated PC lifetimes and report the second one's legitimate redelivery as a regression.</li>
 * </ul>
 * <p>
 * Everything the window excludes is excluded in the SAFE direction: a delivery that cannot be placed in
 * a window (or whose end is unknown) is skipped, so a mis-scoped observation loses a detection rather
 * than inventing one. A detector that fires on correct behaviour would be disabled within a week, and
 * the suite's whole value is that a RED means something.
 *
 * <h2>Why this cannot silently become vacuous</h2>
 * A window holding one delivery asserts nothing, and a workload with a unique key per record (which is
 * what the other chaos scenarios produce) is nothing BUT such windows. {@link #check} therefore reports
 * {@code LEDGER_ORDER_VACUOUS} when no window held two deliveries - the check going quiet is itself a
 * failure. Scenarios that make no ordering claim (UNORDERED processing) must not record at all rather
 * than record a history this would call vacuous.
 *
 * <h2>Prior art, and why it is not extended</h2>
 * The one existing per-key ordering assertion in the repo is
 * {@code KafkaTestUtils.checkExactOrdering}, called only by
 * {@code ParallelEoSStreamProcessorTest.lessKeysThanThreads}. It has NO window, because it cannot need
 * one: a {@code MockConsumer}, one instance, no rebalance, drained before the check - so no record can
 * be redelivered. In that world it can assert something strictly stronger than this class does (a
 * gapless {@code +1} value sequence of exactly the produced size), which doubles as a loss/duplicate
 * check. Scoping it to a window would weaken it for its own caller, and it fails on the first duplicate
 * under churn, so the two are kept separate and cross-referenced rather than merged. The unit-level
 * assertions in {@code WorkManagerTest} ({@code orderedByKeyParallel},
 * {@code testOrderedInFlightShouldBlockQueue}) check the same guarantee one layer down - that a shard
 * hands out one record at a time in offset order - also with no epochs in play.
 *
 * @see ProgressProbe#ledger the loss / bounded-duplicate half of the same end-of-run ledger
 */
@Slf4j
public final class KeyOrderLedger {

    /** Reported problems are capped: a systemic break produces thousands of identical lines. */
    static final int MAX_REPORTED_PER_KIND = 5;

    private KeyOrderLedger() {
    }

    /**
     * One execution of the user function, bracketed. {@code startSeq}/{@code endSeq} come from a single
     * shared counter, so they order events across every worker thread in the fleet - the observation
     * order the check replays. {@code endSeq} stays {@link #UNFINISHED} for a delivery still running when
     * the run was torn down.
     */
    @Getter
    public static class Delivery {

        /** {@code endSeq} of a delivery that never completed - its overlap window is unknowable. */
        public static final long UNFINISHED = -1;

        private final String incarnationId;
        private final int partition;
        private final long epoch;
        private final String key;
        private final long offset;
        private final long startSeq;
        private volatile long endSeq;

        public Delivery(String incarnationId, int partition, long epoch, String key, long offset,
                        long startSeq, long endSeq) {
            this.incarnationId = incarnationId;
            this.partition = partition;
            this.epoch = epoch;
            this.key = key;
            this.offset = offset;
            this.startSeq = startSeq;
            this.endSeq = endSeq;
        }

        /** The ordering window this delivery belongs to - see the class javadoc for why each part is in it. */
        String window() {
            return incarnationId + "|p" + partition + "|e" + epoch + "|" + key;
        }

        @Override
        public String toString() {
            return "offset " + offset + " of key '" + key + "' [" + window() + "]";
        }
    }

    /**
     * Live recorder handed to a scenario's user function. Allocation is one {@link Delivery} per record
     * (not per event), so a 60k-record run holds 60k objects - the history has to survive to the end of
     * the run because {@link #check} is a pure replay, which is what makes it unit-testable against
     * constructed histories instead of only against live chaos.
     */
    public static class Recorder {

        private final AtomicLong sequence = new AtomicLong();
        @Getter
        private final Queue<Delivery> history = new ConcurrentLinkedQueue<>();

        /**
         * Call as the FIRST thing the user function does, and pass the returned handle to
         * {@link #finished} in a {@code finally}.
         */
        public Delivery started(String incarnationId, PollContext<String, String> context) {
            var record = context.getSingleConsumerRecord();
            long epoch = epochOf(context);
            var delivery = new Delivery(incarnationId, record.partition(), epoch, record.key(),
                    record.offset(), sequence.incrementAndGet(), Delivery.UNFINISHED);
            history.add(delivery);
            return delivery;
        }

        public void finished(Delivery delivery) {
            delivery.endSeq = sequence.incrementAndGet();
        }

        /**
         * PC's own per-partition assignment epoch, carried ON the record rather than read from a
         * counter at execution time. That is what makes the window race-free: a record taken as work
         * in epoch N and started on a worker thread after a rebalance bumped the partition to N+1
         * still reports N, so it lands in the window it was actually taken from.
         * <p>
         * {@code PollContext#streamInternal} and {@code RecordContextInternal#getWorkContainer} are both
         * public API, so this needs no production change and no same-package accessor.
         */
        private static long epochOf(PollContext<String, String> context) {
            return context.streamInternal()
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("poll context held no record: " + context))
                    .getWorkContainer()
                    .getEpoch();
        }
    }

    /**
     * Replays a delivery history and returns the ordering violations (empty = the ordering ledger
     * balances). Pure function - the history may be live-recorded or constructed, which is how the
     * boundary is pinned deterministically in {@code KeyOrderLedgerIT} rather than only by chaos runs.
     * <p>
     * The history is sorted by {@code startSeq} rather than trusted in iteration order: the sequence is
     * claimed atomically but the append that follows it is not, so two worker threads can enqueue in
     * the opposite order to the one they started in.
     *
     * @param history every {@link Delivery} of the run, in any order
     * @return list of ledger violations (empty = balanced)
     */
    public static List<String> check(Collection<Delivery> history) {
        List<String> orderProblems = new ArrayList<>();
        List<String> overlapProblems = new ArrayList<>();
        long orderCount = 0;
        long overlapCount = 0;
        long comparedCount = 0;

        var byStartSeq = history.stream()
                .sorted((a, b) -> Long.compare(a.getStartSeq(), b.getStartSeq()))
                .collect(Collectors.toList());

        Map<String, Long> highestOffsetStarted = new HashMap<>();
        Map<String, Long> latestEndSeq = new HashMap<>();
        Set<String> assertingWindows = new HashSet<>();
        Set<String> keysSeen = new HashSet<>();

        for (Delivery delivery : byStartSeq) {
            String window = delivery.window();
            keysSeen.add(delivery.getKey());

            Long previousOffset = highestOffsetStarted.get(window);
            if (previousOffset == null) {
                highestOffsetStarted.put(window, delivery.getOffset());
            } else {
                assertingWindows.add(window);
                comparedCount++;
                if (delivery.getOffset() < previousOffset) {
                    // strictly LOWER only: an equal offset is a redelivery of the same record, which is
                    // the duplicate ledger's business, not an ordering regression
                    orderCount++;
                    if (orderProblems.size() < MAX_REPORTED_PER_KIND) {
                        orderProblems.add(delivery + " was started after offset " + previousOffset
                                + " of the same key, in the SAME assignment epoch on the SAME instance");
                    }
                } else {
                    highestOffsetStarted.put(window, delivery.getOffset());
                }

                Long previousEnd = latestEndSeq.get(window);
                if (previousEnd != null && previousEnd > delivery.getStartSeq()) {
                    overlapCount++;
                    if (overlapProblems.size() < MAX_REPORTED_PER_KIND) {
                        overlapProblems.add(delivery + " started at seq " + delivery.getStartSeq()
                                + " while an earlier delivery of the same key was still in flight (it ended at seq "
                                + previousEnd + ")");
                    }
                }
            }

            if (delivery.getEndSeq() != Delivery.UNFINISHED) {
                // an UNFINISHED delivery leaves the window's end unknown, so the next delivery's overlap
                // check is skipped rather than guessed - the safe direction
                latestEndSeq.merge(window, delivery.getEndSeq(), Math::max);
            } else {
                latestEndSeq.remove(window);
            }
        }

        List<String> problems = new ArrayList<>();
        if (!orderProblems.isEmpty()) {
            problems.add("LEDGER_KEY_ORDER: " + orderCount + " per-key ordering regression(s) - "
                    + "records of one key ran out of offset order within one instance+partition+epoch, which "
                    + "is the guarantee PC exists to keep (sample: " + orderProblems + ")");
        }
        if (!overlapProblems.isEmpty()) {
            problems.add("LEDGER_KEY_CONCURRENCY: " + overlapCount + " overlapping delivery pair(s) - "
                    + "two records of one key were in flight at once within one instance+partition+epoch, so "
                    + "per-key order was not merely reordered but abandoned (sample: " + overlapProblems + ")");
        }
        if (assertingWindows.isEmpty()) {
            problems.add("LEDGER_ORDER_VACUOUS: no key was delivered twice inside one instance+partition+epoch, "
                    + "so the ordering ledger asserted NOTHING over " + byStartSeq.size() + " deliveries across "
                    + keysSeen.size() + " keys - the workload must repeat keys (and process them in KEY order) "
                    + "for this check to mean anything");
        }

        // comparedDeliveries is the "how much did this assert" number: a delivery only counts once it has
        // a predecessor in its own window, so heavy churn (short windows) shows up here as a smaller
        // number rather than as a quietly weaker check
        log.info("[chaos-ledger] ordering: deliveries={} comparedDeliveries={} keys={} windows={} "
                        + "assertingWindows={} orderRegressions={} overlaps={}",
                byStartSeq.size(), comparedCount, keysSeen.size(), highestOffsetStarted.size(),
                assertingWindows.size(), orderCount, overlapCount);
        return problems;
    }

    /** Null-tolerant convenience for scenarios that make no ordering claim - see {@code ChaosScenarioBase}. */
    public static List<String> checkIfRecording(Recorder recorder) {
        return recorder == null ? new ArrayList<>() : check(recorder.getHistory());
    }
}
