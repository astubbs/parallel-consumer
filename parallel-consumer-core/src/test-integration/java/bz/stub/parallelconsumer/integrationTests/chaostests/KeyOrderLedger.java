package bz.stub.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.PollContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
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
 * <h2>The guarantee this asserts, and the windows it holds in</h2>
 * <p>
 * <b>Within one PC incarnation, one partition and one record key, at most one delivery of that key is
 * executing at any instant - across assignment epochs; and within one assignment epoch, the deliveries
 * of that key start in ascending offset order.</b> Two violations follow from that, and this class
 * reports exactly those two, each in its own window:
 * <ul>
 *   <li>{@code LEDGER_KEY_ORDER} - a later-started delivery has a LOWER offset than one already started
 *   in the same incarnation, partition, EPOCH and key (an out-of-order regression). Scoped to the epoch
 *   because a revoke re-delivers uncommitted work from the last commit, so an earlier offset legitimately
 *   follows a later one across an epoch boundary.</li>
 *   <li>{@code LEDGER_KEY_CONCURRENCY} - two deliveries of the same incarnation, partition and key
 *   overlapped in flight, <b>whatever their epochs</b> (the serialisation half of the same promise; an
 *   ordering bug can show up as either). Not scoped to the epoch, since astubbs#178: a revoke does not
 *   interrupt a running worker, and the same instance getting the partition back and starting the
 *   re-delivered record while that worker is still inside the user function is the one route in the
 *   engine that puts one key on two threads at once. The engine now makes the re-delivery wait
 *   ({@code ProcessingShard#flightsOwed}), so the bound on "how long may an old-epoch delivery still be
 *   running once the new epoch starts the same key" is ZERO within an incarnation - which is why this
 *   check needs no calibrated allowance, and why an earlier version of this javadoc, which said picking
 *   that number was "the whole job", no longer stands.</li>
 * </ul>
 * <p>
 * The windows are not decoration - they are the whole difficulty, because a NAIVE "offsets per key must
 * increase" fires on correct behaviour. Each of the components excludes one class of legitimate
 * re-processing that at-least-once delivery under churn produces on purpose:
 * <ul>
 *   <li><b>key</b> - PC orders per key ONLY; two different keys share no order (that is the
 *   concurrency). {@code ShardKey.KeyOrderedKey} is the shard identity in
 *   {@link bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder#KEY} mode.</li>
 *   <li><b>partition</b> - {@code ShardKey.KeyOrderedKey} carries the record's {@code TopicPartition},
 *   not just the topic, so the shard (and therefore the order) is per key PER PARTITION.</li>
 *   <li><b>epoch</b> (the ORDER window only) - the assignment generation, incremented per partition on
 *   BOTH revoke and assign ({@code PartitionStateManager.incrementPartitionAssignmentEpoch}). When an
 *   assignment moves, uncommitted work is redelivered from the last commit, so an earlier offset is
 *   legitimately processed after a later one. That opens a NEW order window; it does not violate the
 *   old one. The epoch is read off the record's own {@code WorkContainer}, so a heavy record still in
 *   flight when its partition is revoked keeps the OLD epoch and lands in the order window it was taken
 *   from. The serialisation window deliberately leaves the epoch out - see above.</li>
 *   <li><b>incarnation</b> - a chaos restart builds a fresh {@code ParallelEoSStreamProcessor}, whose
 *   epoch counter starts again at zero. Scoping to the (stable) chaos instance id would collide epoch
 *   0 of two unrelated PC lifetimes and report the second one's legitimate redelivery as a regression.</li>
 * </ul>
 * <p>
 * Everything the window excludes is excluded in the SAFE direction: a delivery that cannot be placed in
 * a window is skipped, so a mis-scoped observation loses a detection rather than inventing one. (A
 * delivery with no recorded end is NOT such a case: {@code finished} is finally-guaranteed, so a missing
 * end means still-running, and {@link #check} treats it as an open interval that any later same-window
 * start certainly overlaps.) A detector that fires on correct behaviour would be disabled within a week, and
 * the suite's whole value is that a RED means something.
 *
 * <h2>Why this cannot silently become vacuous</h2>
 * A window holding one delivery asserts nothing, and a workload with a unique key per record (which is
 * what the other chaos scenarios produce) is nothing BUT such windows. {@link #check} therefore reports
 * {@code LEDGER_ORDER_VACUOUS} when no window held two deliveries - the check going quiet is itself a
 * failure. Scenarios that make no ordering claim (UNORDERED processing) must not record at all rather
 * than record a history this would call vacuous. The same failure shape - an assertion silently losing
 * its precondition and going green - is the subject of
 * {@code docs/solutions/test-flakiness/vacuous-counting-assertion-loop-changed-its-own-precondition-2026-08-18.md}.
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
 * <h2>What this does NOT assert</h2>
 *
 * "Asserts ordering under churn" is easy to read as "no two owners ever concurrently touch a key", and
 * it does not mean that. The serialisation window is scoped to one INCARNATION, so a straggler on a
 * stopped-but-not-drained owner still executing while a <em>different</em> instance (or a restarted
 * one) processes the same key is not raised. That is deliberate and not a gap this ledger could
 * close: PC promises order within one consumer, and no client-side mechanism can see a worker in
 * another JVM - that is at-least-once delivery, and the fence for it is the application's. The
 * within-incarnation case used to be excluded on the same footing, and is not any more: astubbs#178
 * ruled it a violation and the engine now prevents it, so the check above asserts it (it was a
 * function nobody had written, not data nobody had - every {@link Delivery} carries its epoch, the
 * history is retained, and the window is a grouping the ANALYSIS chooses).
 *
 * <p>The cross-instance case is worth stating because it is the shape of a product bug this repo
 * has already found once - the drain-path zombie in
 * {@code docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md}, fixed in
 * astubbs#80 - so a reader could reasonably assume this ledger covers it. It covers the half that
 * lives inside one instance; {@code AbstractRevokeUnderWorkScenario} names the other half from the
 * scenario side ("a drain opens the Class 1 drain-zombie window, which can mask the Class 2 mechanism
 * it isolates"). Detecting cross-instance overlap needs an instrument that outlives an instance, which
 * is a different tool from this one.
 *
 * <h2>It is an event register: record facts, decide meaning at the end</h2>
 *
 * The recorder writes down what happened - key, offset, start, end - and nothing else. Interpretation
 * belongs to {@link #check}, which runs once, with the whole run in front of it. Keeping those two
 * jobs apart is not tidiness; it is the reason the ledger can be trusted, because a fact discarded
 * while the run is still going cannot be reconsidered once the run is over.
 *
 * <p><b>The analysis pass must not throw information away either, and that is where this went
 * wrong.</b> A delivery with no end time is a FACT - the worker never finished. The first version
 * read that fact, concluded "end unknown", and deleted the window's running end, so the next
 * delivery had nothing to compare against. It had turned a recorded fact into a forgotten one, and
 * the detector reported green because it had stopped looking rather than because nothing was wrong.
 * The information was always there; only the analysis discarded it.
 *
 * <p>So the rule for anything added here: <b>never delete, only interpret</b>. A missing end means
 * still running, which makes every later start in that window a certain overlap - no guess, and no
 * data thrown away to reach it.
 *
 * <p><b>Which is why absence is {@code null} here and never a sentinel.</b> A sentinel is
 * type-compatible with the arithmetic: {@code Long.MAX_VALUE} slides into {@code Math::max}, into a
 * {@code >} comparison, into a subtraction, and the calculation simply happens - silently, on a
 * number the run never produced. A {@code null Long} cannot. It will not compile into arithmetic,
 * and at runtime it throws rather than returning a plausible answer, so the compiler is what forces
 * every site touching the value to decide what "we do not know" means there. That is the difference
 * between a rule written down and a rule enforced.
 *
 * <p>This is not hypothetical: the first fix for the discarded-end bug used {@code Long.MAX_VALUE}
 * as the open-interval marker and reintroduced the same class of error one layer down - a max
 * computed over a fabricated value, which then leaked into the overlap report as "ended at seq
 * 9223372036854775807". <b>Do not take shortcuts in capturing reality</b>: record what happened, and
 * let the type system carry the fact that sometimes nothing did.
 *
 * <p><b>And the reason the rule is absolute rather than a preference: a check that does not hold all
 * the information its decision needs cannot be testing what it claims to test.</b> Not testing it
 * weakly - not testing it. Once the end time was discarded, no amount of care in the comparison that
 * followed could recover the answer, because the input to that comparison was gone. That is why this
 * failed silently instead of loudly: a check starved of its inputs does not error, it returns
 * "nothing found", which is indistinguishable from a healthy run. Any future change here should be
 * read against that test - after it, does the assessment still hold everything it needs to decide?
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
     * order the check replays. {@code endSeq} stays {@code null} for a delivery still running when the
     * run was torn down - the absence of an end, never a value standing in for one.
     */
    @Getter
    public static class Delivery {

        private final String incarnationId;
        private final int partition;
        private final long epoch;
        private final String key;
        private final long offset;
        private final long startSeq;
        /** {@code null} until {@link #finished} runs - absence of an end, not a value standing in for one. */
        private volatile Long endSeq;

        public Delivery(String incarnationId, int partition, long epoch, String key, long offset,
                        long startSeq, Long endSeq) {
            this.incarnationId = incarnationId;
            this.partition = partition;
            this.epoch = epoch;
            this.key = key;
            this.offset = offset;
            this.startSeq = startSeq;
            this.endSeq = endSeq;
        }

        /** The ORDER window this delivery belongs to - see the class javadoc for why each part is in it. */
        String window() {
            return incarnationId + "|p" + partition + "|e" + epoch + "|" + key;
        }

        /**
         * The SERIALISATION window: the order window without the epoch, because a revoke does not interrupt a
         * running worker and the same instance may start the re-delivered key before that worker returns
         * (astubbs#178). The epoch stays in {@link #toString()} through {@link #window()}, so an overlap
         * report shows whether the two deliveries crossed one.
         */
        String serialisationWindow() {
            return incarnationId + "|p" + partition + "|" + key;
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
                    record.offset(), sequence.incrementAndGet(), null);
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
                .sorted(Comparator.comparingLong(Delivery::getStartSeq))
                .collect(Collectors.toList());

        Map<String, Long> highestOffsetStarted = new HashMap<>();
        // The serialisation half is keyed by the SERIALISATION window (no epoch) - see Delivery#serialisationWindow.
        // The latest-ending delivery is kept rather than its end alone, so the report can name the delivery that
        // was still running, epoch and all.
        Map<String, Delivery> latestEnded = new HashMap<>();
        // A serialisation window with a delivery that never ended. Kept as its own FACT rather than folded
        // into latestEnded as a Long.MAX_VALUE end: that sentinel is a real, valid end value standing in for
        // "no end", so it fabricates information the run never produced - and it leaked, printing
        // "ended at seq 9223372036854775807" in the very report meant to explain the overlap.
        Map<String, Delivery> openDelivery = new HashMap<>();
        Set<String> assertingWindows = new HashSet<>();
        Set<String> keysSeen = new HashSet<>();
        long crossEpochOverlapCount = 0;

        for (Delivery delivery : byStartSeq) {
            String window = delivery.window();
            String lane = delivery.serialisationWindow();
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
            }

            // The overlap half, across epochs: whichever earlier delivery of this key on this instance is
            // still running - never finished, or finished after this one started - this one overlapped it.
            Delivery stillOpen = openDelivery.get(lane);
            Delivery lastEnded = latestEnded.get(lane);
            Delivery overlapped = stillOpen != null ? stillOpen
                    : (lastEnded != null && lastEnded.getEndSeq() > delivery.getStartSeq()) ? lastEnded
                    : null;
            if (overlapped != null) {
                overlapCount++;
                boolean crossedAnEpoch = overlapped.getEpoch() != delivery.getEpoch();
                if (crossedAnEpoch) {
                    crossEpochOverlapCount++;
                }
                if (overlapProblems.size() < MAX_REPORTED_PER_KIND) {
                    String howItWasStillRunning = stillOpen != null
                            ? "that delivery never finished"
                            : "it ended at seq " + overlapped.getEndSeq();
                    overlapProblems.add(delivery + " started at seq " + delivery.getStartSeq() + " while "
                            + overlapped + " was still in flight (" + howItWasStillRunning
                            + (crossedAnEpoch ? "; ACROSS an assignment epoch - the old epoch's worker was still "
                                    + "running when the same instance started the re-delivered key, astubbs#178" : "")
                            + ")");
                }
            }

            if (delivery.getEndSeq() != null) {
                latestEnded.merge(lane, delivery, (a, b) -> a.getEndSeq() >= b.getEndSeq() ? a : b);
            } else {
                // No end recorded is a FACT, and it is recorded as one. finished() is
                // finally-guaranteed ({@code ChaosScenarioBase#newInstance}), so no end means the
                // delivery is genuinely still running - which makes any later start in the same
                // serialisation window a CERTAIN overlap, with no guess and no invented end value
                // involved. Legitimate redelivery of a wedged record to ANOTHER incarnation opens a
                // new serialisation window, so this cannot fire on correct behaviour; the same
                // incarnation starting it is exactly astubbs#178. Deleting the window's end here
                // instead was the original bug: it silently disabled the overlap half for exactly the
                // confluentinc#857 wedge shape this detector exists to catch.
                openDelivery.putIfAbsent(lane, delivery);
            }
        }

        List<String> problems = new ArrayList<>();
        if (!orderProblems.isEmpty()) {
            problems.add("LEDGER_KEY_ORDER: " + orderCount + " per-key ordering regression(s) - "
                    + "records of one key ran out of offset order within one instance+partition+epoch, which "
                    + "is the guarantee PC exists to keep (sample: " + orderProblems + ")");
        }
        if (!overlapProblems.isEmpty()) {
            problems.add("LEDGER_KEY_CONCURRENCY: " + overlapCount + " overlapping delivery pair(s), "
                    + crossEpochOverlapCount + " of them across an assignment epoch - "
                    + "two records of one key were in flight at once within one instance+partition, so "
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
                        + "assertingWindows={} orderRegressions={} overlaps={} crossEpochOverlaps={}",
                byStartSeq.size(), comparedCount, keysSeen.size(), highestOffsetStarted.size(),
                assertingWindows.size(), orderCount, overlapCount, crossEpochOverlapCount);
        return problems;
    }

    /** Null-tolerant convenience for scenarios that make no ordering claim - see {@code ChaosScenarioBase}. */
    public static List<String> checkIfRecording(Recorder recorder) {
        return recorder == null ? new ArrayList<>() : check(recorder.getHistory());
    }
}
