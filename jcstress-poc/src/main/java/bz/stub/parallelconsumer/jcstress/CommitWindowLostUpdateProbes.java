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
 * 121,707,028 samples - <b>and that zero is not evidence the protocol is correct</b>, because its
 * forbidden corner is unreachable by construction.
 * {@link GenerationCountedCommitWindow} <b>owns</b> that caveat: why the corner cannot be entered, what
 * the arm does show, and why no negative control was built. Read it before quoting the zero anywhere.
 * The headline above stands on its own - it is the plain-versus-volatile pair, which the caveat does not
 * touch.
 * <p>
 * Those figures were measured before {@link CommitWindowState} hoisted the shared scaffolding out of the
 * arms. The re-run after that refactor confirmed the <b>outcome kinds</b> are unchanged - both flag arms
 * still fire, the protocol arm still reads FORBIDDEN at zero - which is what the numbers above are cited
 * for. They are not re-recorded per run: a rate is machine- and load-specific, and re-running is the way
 * to get today's.
 *
 * <h2>What these arms share, and what they deliberately do not - read before deduplicating further</h2>
 *
 * The duplicate-code check flagged the arms as clones of each other, and it was half right. The split:
 *
 * <ul>
 *   <li><b>Shared</b>, in {@link CommitWindowState}: every field that is <i>not</i> under test (the
 *       {@link ConcurrentSkipListMap}, {@code offsetHighestSucceeded}, the {@code offsetThisCycleCommitted}
 *       instrumentation), the setup that seeds the map, the "real surrounding accesses" scaffolding -
 *       {@link CommitWindowState#applyCompletionToOffsetState()} and
 *       {@link CommitWindowState#encodeAndCaptureCommittedOffset()} - and the arbiter's result mapping.
 *       None of it is the thing being measured, and three copies of it were three places to drift.</li>
 *   <li><b>Deliberately duplicated</b>, and it must stay that way: <b>the measured field keeps its own
 *       declaration in its own arm</b>, because plain-versus-{@code volatile} on that declaration is the
 *       entire term under test - hoisting it would erase the difference between the arms. And <b>each
 *       actor's sequence of accesses stays literal in the arm</b>, so the JIT compiles the same shape
 *       production has, in the same order, rather than a shape assembled from calls that vary per arm.</li>
 * </ul>
 *
 * The scaffolding helpers are {@code final} and tiny, so they inline; they contain <i>no</i> access to any
 * arm's measured field, which is the rule that decides what may move into them. The arbiter may use a
 * shared helper where an actor may not: it runs <i>after</i> both actors finish, so nothing it does is
 * inside the raced window.
 * <p>
 * jcstress permits this - its annotation processor requires a {@code @State} class to be public and
 * non-final, and bans inheritance only on {@code @Result} classes. The actors and the arbiter stay
 * declared on each {@code @State} arm regardless, which is also what the literal-actor rule above
 * requires.
 */
public class CommitWindowLostUpdateProbes {

    /**
     * The offset the control thread completes inside the commit window, and the one the arbiter asks
     * whether the commit covered.
     */
    static final long COMPLETED_OFFSET = 1L;

    /**
     * Scaffolding shared by all three arms: the state that is not under test, and the surrounding real
     * accesses that make the arms faithful rather than reduced.
     * <p>
     * Nothing here touches a measured field - see the class javadoc for the rule this follows. Extending
     * it costs the arms nothing at jcstress level: only {@code @Result} classes are barred from
     * inheriting, and each arm still declares its own actors and its own arbiter.
     */
    abstract static class CommitWindowState {

        final ConcurrentSkipListMap<Long, Optional<Object>> incompleteOffsets = new ConcurrentSkipListMap<>();

        long offsetHighestSucceeded;

        /**
         * Instrumentation - the offset this commit cycle captured, so the arbiter can say whether the
         * commit covered the completion. Written and read by the poll actor only.
         */
        long offsetThisCycleCommitted;

        CommitWindowState() {
            incompleteOffsets.put(COMPLETED_OFFSET, Optional.empty());
        }

        /**
         * The first two steps of {@code PartitionState.onSuccess} - the map removal and the plain write of
         * the highest succeeded offset. What follows them is the commit-protocol write, which differs per
         * arm and therefore stays in the arm.
         */
        final void applyCompletionToOffsetState() {
            incompleteOffsets.remove(COMPLETED_OFFSET);
            offsetHighestSucceeded = COMPLETED_OFFSET;
        }

        /**
         * {@code createOffsetAndMetadata()} → {@code tryToEncodeOffsets()}: the map touch, then the plain
         * read of {@code offsetHighestSucceeded} that yields the offset this cycle commits.
         * <p>
         * The branch is kept rather than simplified to a single value: {@code tryToEncodeOffsets()}
         * branches on {@code isEmpty()} too, but {@code offsetOfNextExpectedMessage} is computed before
         * that branch and returned unchanged by both its paths - so an arm that models the branch
         * faithfully lands on the same value either way, and performs the same reads production performs.
         */
        final long encodeAndCaptureCommittedOffset() {
            boolean ignoredEmpty = incompleteOffsets.isEmpty();
            return ignoredEmpty ? offsetHighestSucceeded : offsetHighestSucceeded;
        }

        /**
         * The arbiter's result mapping, identical across the arms because the invariant is. Safe to share
         * where an actor's body is not: the arbiter runs after both actors, outside the raced window.
         *
         * @param stillDirty the arm's own answer to "is the partition still going to be committed?"
         */
        final void reportInvariant(ZZ_Result r, boolean stillDirty) {
            r.r1 = stillDirty;
            r.r2 = offsetThisCycleCommitted >= COMPLETED_OFFSET;   // did this cycle's commit cover it?
        }
    }

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
    public static class PlainStateChangedFlagAcrossTheCommitWindow extends CommitWindowState {

        // THE TERM UNDER TEST - the pair of flags this arm's protocol is made of, declared here and not
        // hoisted: the modifiers on these two lines are the only thing that separates this arm from
        // VolatileStateChangedFlagAcrossTheCommitWindow.
        volatile boolean dirty = true;
        boolean stateChangedSinceCommitStart = true;

        /**
         * {@code PartitionState.onSuccess(1)} - a completion landing inside the commit window.
         */
        @Actor
        public void controlThread() {
            applyCompletionToOffsetState();
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
                offsetThisCycleCommitted = encodeAndCaptureCommittedOffset();
            }
            // ... commitOffsets() to the broker ...
            if (!stateChangedSinceCommitStart) {                // setClean
                dirty = false;
            }
        }

        @Arbiter
        public void arbiter(ZZ_Result r) {
            reportInvariant(r, dirty);
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
    public static class VolatileStateChangedFlagAcrossTheCommitWindow extends CommitWindowState {

        // THE TERM UNDER TEST - identical to the plain arm's pair but for the modifier on the second
        // line. That one keyword is the whole experiment; the arm exists to hold it.
        volatile boolean dirty = true;
        volatile boolean stateChangedSinceCommitStart = true;

        @Actor
        public void controlThread() {
            applyCompletionToOffsetState();
            stateChangedSinceCommitStart = true;
            dirty = true;
        }

        @Actor
        public void brokerPollThread() {
            if (dirty) {
                stateChangedSinceCommitStart = false;
                offsetThisCycleCommitted = encodeAndCaptureCommittedOffset();
            }
            if (!stateChangedSinceCommitStart) {
                dirty = false;
            }
        }

        @Arbiter
        public void arbiter(ZZ_Result r) {
            reportInvariant(r, dirty);
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
     * <p>
     * <b>READ THIS BEFORE QUOTING THAT ZERO - the forbidden corner is unreachable by construction, so this
     * arm running green is not evidence that the protocol is correct.</b> Unlike the two flag arms above,
     * this arm is <b>not seeded dirty</b>: both counters start at zero, so the poll actor enters the commit
     * window only when it has already observed the completion. If it samples zero it skips the window
     * entirely; if it samples the increment, that {@link AtomicLong} acquire also publishes the map removal
     * and the offset write that preceded the release, so the commit necessarily covers the completion.
     * Neither path can reach "clean over an uncovered completion", and <b>neither path depends on the
     * protocol below it being right</b> - break the protocol and the corner stays just as unreachable. The
     * zero therefore reports that the corner was never entered, not that it was entered and held.
     * <p>
     * <b>What this arm does show, and what the flag arms show that it cannot.</b> In the recorded run of
     * 2026-09-07 above, the two flag arms each reached all four outcomes, including "covered and still
     * dirty". This arm reached only two - "dirty, not covered" and "clean, covered" - and never produced
     * "covered and still dirty" at all, which is the signature of a window that only ever opens over a
     * completion it has already observed. So the arm shows that the protocol admits no lost update
     * <i>along the interleavings it reaches</i>, and that those interleavings split between the pessimistic
     * outcome and the covered one; it says nothing about the interleaving its FORBIDDEN outcome names. The
     * arms are not comparing like with like, and this one measures something narrower than the plain arm.
     * <p>
     * <b>Seeding this arm dirty is not the fix</b>, which is why it has not been done. Seeding makes the
     * window open reliably, but the corner stays unreachable for the same release/acquire reason: any
     * sample that observes the increment also observes everything published before it. That is a
     * restatement of the protocol being correct, not a demonstration that the probe could detect it being
     * wrong.
     * <p>
     * <b>A real power check would need a deliberately broken negative control</b> - a protocol variant with
     * the defect put back, which this arm would then have to catch, the way
     * {@code PartitionStateCommitWindowSeamTest} keeps a control arm asserting the old defect and stays
     * runnable after the fix removed it. Raised by a Codex review on astubbs/parallel-consumer#469, where
     * the maintainer ruled that the control would <b>not</b> be built and the weakness would be recorded
     * instead. So this is a standing limitation of this arm, not an open item somebody is about to close:
     * cite it as "no anomaly observed", never as "the window was measured shut", and do not let a future
     * green run here be read as the protocol having been verified. The pair the argument for the shipped
     * fix actually rests on is the plain-versus-volatile comparison above, which none of this affects.
     */
    @JCStressTest
    @Description("Fixed: monotone completion count, and the count the commit covered - no flag to clear")
    @Outcome(id = "false, false", expect = FORBIDDEN,
            desc = "Partition clean over an uncovered completion - would invalidate the protocol")
    @Outcome(id = {"true, false", "true, true", "false, true"}, expect = ACCEPTABLE, desc = "Invariant held")
    @State
    public static class GenerationCountedCommitWindow extends CommitWindowState {

        // THE TERM UNDER TEST - this arm's protocol replaces the flag pair above outright, so its three
        // fields are what differs from the other arms and they stay declared here.

        /** {@code PartitionState.completionCount} - only the completing thread touches it, and only upwards. */
        final AtomicLong completionCount = new AtomicLong();

        /** {@code PartitionState.completionCountCommitted} - published by the committer thread. */
        volatile long completionCountCommitted;

        /** {@code PartitionState.completionCountBeingCommitted} - confined to the committer thread. */
        long completionCountBeingCommitted;

        @Actor
        public void controlThread() {
            applyCompletionToOffsetState();
            completionCount.incrementAndGet();          // recordCompletion()
        }

        @Actor
        public void brokerPollThread() {
            long collected = completionCount.get();
            if (collected != completionCountCommitted) {            // isDirty()
                completionCountBeingCommitted = collected;
                offsetThisCycleCommitted = encodeAndCaptureCommittedOffset();
                // ... commitOffsets() to the broker ...
                completionCountCommitted = completionCountBeingCommitted;   // setClean()
            }
        }

        @Arbiter
        public void arbiter(ZZ_Result r) {
            reportInvariant(r, completionCount.get() != completionCountCommitted);
        }
    }
}
