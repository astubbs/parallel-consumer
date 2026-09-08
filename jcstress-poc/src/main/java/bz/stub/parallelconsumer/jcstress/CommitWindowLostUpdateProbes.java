package bz.stub.parallelconsumer.jcstress;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Description;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.ZZ_Result;

import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;
import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE_INTERESTING;
import static org.openjdk.jcstress.annotations.Expect.FORBIDDEN;

/**
 * {@code PartitionState.stateChangedSinceCommitStart} - the flag that stops a commit marking a partition
 * clean over a completion that landed <i>inside</i> the commit window. <b>Both</b> threads write it, which
 * is why {@code volatile} alone is not its fix and why these arms exist:
 * {@link VolatileStateChangedFlagAcrossTheCommitWindow} is the arm that says so.
 *
 * <h2>Correspondence to production code (check this if the source drifts)</h2>
 *
 * Control thread, on a record completing - {@code PartitionState.onSuccess}:
 * <ol>
 *   <li>{@code incompleteOffsets.remove(offset)} - a {@link ConcurrentSkipListMap};</li>
 *   <li>{@code updateHighestSucceededOffsetSoFar(offset)} - plain write to {@code offsetHighestSucceeded};</li>
 *   <li>{@code setDirty()} - plain write {@code stateChangedSinceCommitStart = true}, then the
 *       {@code volatile} write {@code dirty = true} (volatile since astubbs/parallel-consumer#349).</li>
 * </ol>
 *
 * Broker-poll thread, one whole commit cycle - {@code AbstractOffsetCommitter.retrieveOffsetsAndCommit}
 * runs collect, commit and success callback as one sequential unit on the committer thread:
 * <ol>
 *   <li>{@code getCommitDataIfDirty()}: {@code isDirty()}, then {@code stateChangedSinceCommitStart = false}
 *       - <b>a plain write of the same field the control thread writes</b>;</li>
 *   <li>{@code createOffsetAndMetadata()} → {@code tryToEncodeOffsets()} → the map, then a plain read of
 *       {@code offsetHighestSucceeded} through {@code getOffsetHighestSequentialSucceeded()}: the offset
 *       this cycle will commit;</li>
 *   <li>{@code commitOffsets(...)} to the broker;</li>
 *   <li>{@code onOffsetCommitSuccess} → {@code setClean()}:
 *       {@code if (!stateChangedSinceCommitStart) setDirty(false);} - <b>a read of the flag and a separate
 *       write of {@code dirty}</b>.</li>
 * </ol>
 *
 * <h2>The invariant, and the harm when it breaks</h2>
 *
 * <b>A completion that lands anywhere inside the commit window must leave the partition dirty, unless the
 * commit covered it.</b> Break it and the partition is flagged clean while an offset it completed is
 * uncommitted: nothing re-dirties the partition until the <i>next</i> completion, and on a partition that
 * then goes idle the committed offset waits for the next rebalance. That is the burnt-commit-cycle stall
 * astubbs/parallel-consumer#349 fenced {@code dirty} against; this is its second half, argued there and
 * measured here for the first time.
 *
 * <h2>Three ways the invariant breaks, and only one of them is a memory-model effect</h2>
 *
 * <ol>
 *   <li><b>Check-then-act.</b> {@code setClean()} reads the flag and then writes {@code dirty} as two
 *       steps. A completion landing between them is a plain interleaving - <b>sequentially consistent
 *       hardware reaches it</b>, so it is not a fence problem and no modifier fixes it.</li>
 *   <li><b>Lost update.</b> The poll thread's {@code false} at commit start and the control thread's
 *       {@code true} inside the window are racing writes with no read-modify-write discipline. Ordering
 *       them (which is all {@code volatile} does) does not decide which wins.</li>
 *   <li><b>Staleness.</b> The control thread's {@code true} may simply not be visible to
 *       {@code setClean()}'s read. This is the only one {@code volatile} addresses.</li>
 * </ol>
 *
 * The arbiter below reports the invariant, not a mechanism, so a firing arm does not say which of the
 * three produced it. What separates them is the <i>arms</i>: (3) alone would be closed by
 * {@link VolatileStateChangedFlagAcrossTheCommitWindow}, and it is not.
 *
 * <h2>Nothing binds these arms to the real code</h2>
 *
 * This module imports no {@code bz.stub.parallelconsumer} class - every probe is a hand-copied replica,
 * bound to production by nothing but a human having copied it. If {@code PartitionState}'s write order
 * changes, nothing goes red. Recorded, with the shape a check would take, in
 * {@code docs/inflight/test-jcstress-probe-module-open-items.md}.
 *
 * <h2>What these arms do NOT model</h2>
 *
 * The broker commit itself, and the {@code ConcurrentLinkedQueue} handoff that gets the poll thread into
 * the cycle in the first place - so, as with {@code CommitPathVisibilityProbes}, the window modelled is
 * one commit cycle wide and the rates are <b>per raced pair, not a production incidence rate</b>.
 *
 * <h2>Calibration status</h2>
 *
 * Run 2026-09-07, {@code -m quick}, macOS 26.5.2 / Apple M2 Pro (arm64, 12 CPUs), JDK
 * Temurin-17.0.18+8. {@link CalibrationProbes.PlainFieldStoreLoadReordering}'s positive control fired at
 * 50.82% in the same run, so the zeros below are interpretable rather than vacuous.
 * <p>
 * <b>The headline: the plain arm and the volatile arm fire at the SAME rate</b> - 1.6e-3 per raced pair,
 * 142,177 in 91,295,031 and 148,418 in 94,683,660. Making the flag {@code volatile}, which is exactly
 * what astubbs/parallel-consumer#349 did to {@code dirty} and what the note recorded as the obvious next
 * step, moves the anomaly by nothing at all. That is the measurement that rejects "one more volatile" as
 * the fix, and it is why this field got a protocol instead. The protocol arm is FORBIDDEN at 0 in
 * 121,707,028 samples.
 *
 * <h2>Why the plain and volatile arms below duplicate each other</h2>
 *
 * Each arm pair below is deliberately near-identical, differing only in the modifier under test - a
 * jcstress arm must be a copy of its neighbour with that one term varied, or the comparison between
 * arms stops isolating the thing being measured. Do not refactor the duplication away.
 */
public class CommitWindowLostUpdateProbes {

    /**
     * The commit window <b>as shipped</b>: {@code dirty} volatile (astubbs/parallel-consumer#349),
     * {@code stateChangedSinceCommitStart} plain and written from both threads.
     * <p>
     * <b>Result, 2026-09-07: 142,177 anomalies in 91,295,031 samples - 1.6e-3 per raced pair.</b> Three
     * orders of magnitude above the {@code dirty} pair's ~1.4e-7, because most of this is not a
     * memory-model residual at all: the check-then-act in {@code setClean()} is reachable on sequentially
     * consistent hardware, so it fires at interleaving rates rather than store-buffer rates.
     */
    @JCStressTest
    @Description("Commit window as shipped: volatile dirty, plain stateChangedSinceCommitStart")
    @Outcome(id = "false, false", expect = ACCEPTABLE_INTERESTING,
            desc = "ANOMALY: partition marked clean while the commit did not cover the completion - burnt cycle")
    @Outcome(id = "true, false", expect = ACCEPTABLE, desc = "Completion not covered, partition still dirty - re-commits")
    @Outcome(id = "true, true", expect = ACCEPTABLE, desc = "Covered and still dirty - one pessimistic extra commit")
    @Outcome(id = "false, true", expect = ACCEPTABLE, desc = "Covered and clean - the intended case")
    @State
    public static class PlainStateChangedFlagAcrossTheCommitWindow {

        final ConcurrentSkipListMap<Long, Optional<Object>> incompleteOffsets = new ConcurrentSkipListMap<>();

        long offsetHighestSucceeded;
        volatile boolean dirty = true;
        boolean stateChangedSinceCommitStart = true;

        /**
         * Instrumentation - the offset this commit cycle captured, so the arbiter can say whether the
         * commit covered the completion. Written and read by the poll actor only.
         */
        long offsetThisCycleCommitted;

        public PlainStateChangedFlagAcrossTheCommitWindow() {
            incompleteOffsets.put(1L, Optional.empty());
        }

        /**
         * {@code PartitionState.onSuccess(1)} - a completion landing inside the commit window.
         */
        @Actor
        public void controlThread() {
            incompleteOffsets.remove(1L);
            offsetHighestSucceeded = 1;
            stateChangedSinceCommitStart = true;
            dirty = true;
        }

        /**
         * One whole commit cycle: {@code getCommitDataIfDirty()}, the commit, {@code setClean()}.
         */
        @Actor
        public void brokerPollThread() {
            if (dirty) {                                        // getCommitDataIfDirty
                stateChangedSinceCommitStart = false;
                boolean ignoredEmpty = incompleteOffsets.isEmpty();  // tryToEncodeOffsets
                // branch kept, not simplified to a single value: tryToEncodeOffsets() branches on
                // isEmpty() too, but offsetOfNextExpectedMessage is computed before that branch and
                // returned unchanged by both its paths - this arm performs the same read production does
                offsetThisCycleCommitted = ignoredEmpty ? offsetHighestSucceeded : offsetHighestSucceeded;
            }
            // ... commitOffsets() to the broker ...
            if (!stateChangedSinceCommitStart) {                // setClean
                dirty = false;
            }
        }

        @Arbiter
        public void arbiter(ZZ_Result r) {
            r.r1 = dirty;                             // is the partition still going to be committed?
            r.r2 = offsetThisCycleCommitted >= 1;     // did this cycle's commit cover the completion?
        }
    }

    /**
     * <b>The arm that decides the design.</b> Same window, with {@code stateChangedSinceCommitStart} made
     * {@code volatile} - the fix that would mirror what astubbs/parallel-consumer#349 did to
     * {@code dirty}. If the anomaly survives here, "one more volatile" is not the fix and the field needs
     * a protocol.
     * <p>
     * <b>Result, 2026-09-07: 148,418 anomalies in 94,683,660 samples - 1.6e-3 per raced pair, statistically
     * indistinguishable from the plain arm.</b> The volatile buys nothing here. It closes (3) and leaves
     * (1) and (2) untouched, and (1) alone accounts for the rate. <b>This arm is the reason the fix is a
     * protocol change and not a modifier</b>, and it is the arm to re-run if anyone proposes going back.
     */
    @JCStressTest
    @Description("Naive fix: stateChangedSinceCommitStart made volatile, nothing else changed")
    @Outcome(id = "false, false", expect = ACCEPTABLE_INTERESTING,
            desc = "ANOMALY SURVIVES THE VOLATILE: check-then-act and lost update are not visibility problems")
    @Outcome(id = {"true, false", "true, true", "false, true"}, expect = ACCEPTABLE, desc = "Invariant held")
    @State
    public static class VolatileStateChangedFlagAcrossTheCommitWindow {

        final ConcurrentSkipListMap<Long, Optional<Object>> incompleteOffsets = new ConcurrentSkipListMap<>();

        long offsetHighestSucceeded;
        volatile boolean dirty = true;
        volatile boolean stateChangedSinceCommitStart = true;

        long offsetThisCycleCommitted;

        public VolatileStateChangedFlagAcrossTheCommitWindow() {
            incompleteOffsets.put(1L, Optional.empty());
        }

        @Actor
        public void controlThread() {
            incompleteOffsets.remove(1L);
            offsetHighestSucceeded = 1;
            stateChangedSinceCommitStart = true;
            dirty = true;
        }

        @Actor
        public void brokerPollThread() {
            if (dirty) {
                stateChangedSinceCommitStart = false;
                boolean ignoredEmpty = incompleteOffsets.isEmpty();
                // branch kept, not simplified to a single value: tryToEncodeOffsets() branches on
                // isEmpty() too, but offsetOfNextExpectedMessage is computed before that branch and
                // returned unchanged by both its paths - this arm performs the same read production does
                offsetThisCycleCommitted = ignoredEmpty ? offsetHighestSucceeded : offsetHighestSucceeded;
            }
            if (!stateChangedSinceCommitStart) {
                dirty = false;
            }
        }

        @Arbiter
        public void arbiter(ZZ_Result r) {
            r.r1 = dirty;
            r.r2 = offsetThisCycleCommitted >= 1;
        }
    }

    /**
     * <b>The fix this PR ships</b>: the two flags collapse into one monotone completion count plus the
     * count the last successful commit covered. The control thread only ever moves the count forward; the
     * poll thread only ever publishes the count its own cycle collected at. Neither thread writes a value
     * that can lose to the other, and "is it dirty" becomes a comparison of two values rather than a flag
     * anyone clears - so there is no check-then-act and no racing write left to order.
     * <p>
     * The count is sampled <b>before</b> the offsets are captured, deliberately: a completion between the
     * sample and the capture is then committed <i>and</i> still counted as uncovered, which costs one
     * extra commit cycle. Sampling after would mark it covered when it was not, which loses it. The
     * pessimistic direction is the safe one, and it is the same pessimism the field's original javadoc
     * already described.
     * <p>
     * <b>Result, 2026-09-07: 0 anomalies in 121,707,028 samples, outcome FORBIDDEN.</b> Note the
     * distribution: 79.92% of pairs end "dirty, not covered" - the pessimistic direction, one extra commit
     * cycle - and 20.08% "clean, covered". Nothing lands in the lossy corner.
     */
    @JCStressTest
    @Description("Fixed: monotone completion count, and the count the commit covered - no flag to clear")
    @Outcome(id = "false, false", expect = FORBIDDEN,
            desc = "Partition clean over an uncovered completion - would invalidate the protocol")
    @Outcome(id = {"true, false", "true, true", "false, true"}, expect = ACCEPTABLE, desc = "Invariant held")
    @State
    public static class GenerationCountedCommitWindow {

        final ConcurrentSkipListMap<Long, Optional<Object>> incompleteOffsets = new ConcurrentSkipListMap<>();

        long offsetHighestSucceeded;

        /** {@code PartitionState.completionCount} - only the completing thread touches it, and only upwards. */
        final AtomicLong completionCount = new AtomicLong();

        /** {@code PartitionState.completionCountCommitted} - published by the committer thread. */
        volatile long completionCountCommitted;

        /** {@code PartitionState.completionCountBeingCommitted} - confined to the committer thread. */
        long completionCountBeingCommitted;

        long offsetThisCycleCommitted;

        public GenerationCountedCommitWindow() {
            incompleteOffsets.put(1L, Optional.empty());
        }

        @Actor
        public void controlThread() {
            incompleteOffsets.remove(1L);
            offsetHighestSucceeded = 1;
            completionCount.incrementAndGet();          // recordCompletion()
        }

        @Actor
        public void brokerPollThread() {
            long collected = completionCount.get();
            if (collected != completionCountCommitted) {            // isDirty()
                completionCountBeingCommitted = collected;
                boolean ignoredEmpty = incompleteOffsets.isEmpty();
                // branch kept, not simplified to a single value: tryToEncodeOffsets() branches on
                // isEmpty() too, but offsetOfNextExpectedMessage is computed before that branch and
                // returned unchanged by both its paths - this arm performs the same read production does
                offsetThisCycleCommitted = ignoredEmpty ? offsetHighestSucceeded : offsetHighestSucceeded;
                // ... commitOffsets() to the broker ...
                completionCountCommitted = completionCountBeingCommitted;   // setClean()
            }
        }

        @Arbiter
        public void arbiter(ZZ_Result r) {
            r.r1 = completionCount.get() != completionCountCommitted;
            r.r2 = offsetThisCycleCommitted >= 1;
        }
    }
}
