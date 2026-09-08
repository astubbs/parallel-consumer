package bz.stub.parallelconsumer.jcstress;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Description;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.ZZ_Result;

import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;
import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE_INTERESTING;
import static org.openjdk.jcstress.annotations.Expect.FORBIDDEN;

/**
 * {@code PartitionState.allowedMoreRecords} - the offset-encoding back-pressure flag - written on the
 * <b>broker-poll</b> thread inside the commit path's encode, read on the <b>control</b> thread when work
 * is taken. The direction is the opposite of {@code dirty}'s, which is why
 * astubbs/parallel-consumer#349's fence does not cover it.
 *
 * <h2>Correspondence to production code (check this if the source drifts)</h2>
 *
 * Writer, broker-poll thread - {@code PartitionState.getCommitDataIfDirty} →
 * {@code createOffsetAndMetadata} → {@code tryToEncodeOffsets} → {@code updateBlockFromEncodingResult},
 * in this order:
 * <ol>
 *   <li>{@code stateChangedSinceCommitStart = false} - plain write;</li>
 *   <li>{@code incompleteOffsets.isEmpty()} - a {@link ConcurrentSkipListMap} access;</li>
 *   <li>{@code setAllowedMoreRecords(false)} - plain write, through the Lombok {@code @Setter(PRIVATE)},
 *       when the encoded payload exceeded the pressure threshold.</li>
 * </ol>
 *
 * Reader, control thread - {@code ProcessingShard.getWorkIfAvailable} →
 * {@code PartitionStateManager.couldBeTakenAsWork} → {@code PartitionState.couldBeTakenAsWork}:
 * <ol>
 *   <li>{@code checkIfWorkIsStale(wc)} - reads a {@code final long} epoch captured at construction, and
 *       {@code isPartitionRemovedOrNeverAssigned()};</li>
 *   <li>{@code isAllowedMoreRecords()} - plain read.</li>
 * </ol>
 * Nothing on the reader's path is an acquire load that the writer thread later releases, so there is no
 * happens-before edge between the two.
 *
 * <h2>WHAT THESE ARMS CAN AND CANNOT SHOW - read before quoting a number</h2>
 *
 * <b>The harm recorded in
 * {@code docs/solutions/logic-errors/volatile-is-the-fix-for-a-one-writer-field-not-a-shared-one-2026-09-07.md}
 * is NOT measurable here, and no bounded stress run can measure it.</b> That harm is <i>unbounded
 * staleness</i>: a control thread holding a stale {@code false} admits nothing, so nothing succeeds,
 * nothing re-dirties the partition, and {@code tryToEncodeOffsets} never runs again - there is no later
 * write to rescue it. "Never" is not an outcome jcstress can observe; every value the reader sees in a
 * bounded run is a value the writer wrote at <i>some</i> point, and a run that stops has not shown that
 * the flag would have stayed stale.
 * <p>
 * <b>There is also no message-passing pair in the real code.</b> The flag is written last and read
 * alone: it carries its whole meaning in one bit and publishes no payload, so the classic
 * flag-then-payload anomaly has nothing to be the payload. The arms below therefore supply one -
 * {@code blockedAtPayloadLength}, declared as instrumentation - which makes them measure the
 * <b>fence</b> (does the flag's write publish what preceded it?) rather than the <b>stall</b> (does the
 * flag ever become stale forever?).
 * <p>
 * So the honest reading is: a firing {@link PlainBackPressureFlagPublishesNothing} shows the flag is
 * published with no release on this machine, and a clean {@link VolatileBackPressureFlagPublishesEncode}
 * shows one keyword closes that. Neither is the justification for fencing the field. That justification
 * is the JMM argument in the note plus SpotBugs' {@code AT_STALE_THREAD_WRITE_OF_PRIMITIVE} on this
 * exact field, and it is stated that way in the field's own javadoc.
 *
 * <h2>Nothing binds these arms to the real code</h2>
 *
 * This module imports no {@code bz.stub.parallelconsumer} class - every probe is a hand-copied replica,
 * bound to production by nothing but a human having copied it. If {@code PartitionState}'s write order
 * changes, nothing goes red. Recorded, with the shape a check would take, in
 * {@code docs/inflight/test-jcstress-probe-module-open-items.md}.
 *
 * <h2>Calibration status</h2>
 *
 * Run 2026-09-07, {@code -m quick}, macOS 15 / arm64 (Apple silicon), JDK 17.0.18-tem, 10 CPUs.
 * {@link CalibrationProbes.PlainFieldStoreLoadReordering}'s positive control fired in the same run, so
 * the zeros below are interpretable rather than vacuous. Results are recorded on each arm.
 * <p>
 * Those figures were measured before {@link SurroundedBackPressureState} hoisted the shared scaffolding
 * out of the two faithful arms. The re-run after that refactor confirmed the <b>outcome kinds</b> are
 * unchanged - the plain arm still fires, the volatile arm still reads FORBIDDEN at zero - which is what
 * the numbers above are cited for. They are not re-recorded per run: a rate is machine- and
 * load-specific, and re-running is the way to get today's.
 *
 * <h2>What these arms share, and what they deliberately do not - read before deduplicating further</h2>
 *
 * The duplicate-code check flagged the two faithful arms as clones of each other, and it was half right.
 * The split:
 *
 * <ul>
 *   <li><b>Shared</b>, in {@link SurroundedBackPressureState}: every field that is <i>not</i> under test
 *       (the {@link ConcurrentSkipListMap}, the neighbouring {@code stateChangedSinceCommitStart} write,
 *       the {@code blockedAtPayloadLength} instrumentation payload), the setup that seeds the map, and
 *       the "real surrounding accesses" scaffolding
 *       {@link SurroundedBackPressureState#encodeOverThreshold()}. None of it is the thing being
 *       measured, and two copies of it were two places to drift.</li>
 *   <li><b>Deliberately duplicated</b>, and it must stay that way: <b>the measured field keeps its own
 *       declaration in its own arm</b>, because plain-versus-{@code volatile} on
 *       {@code allowedMoreRecords} is the entire term under test - hoisting it would erase the
 *       difference between the arms. And <b>each actor's sequence of accesses stays literal in the
 *       arm</b>, so the JIT compiles the same shape production has, in the same order. That is why
 *       {@code controlThread} keeps both of its lines rather than calling a shared mapper: it is an
 *       actor, its two reads happen <i>inside</i> the raced window, and their order is the
 *       measurement.</li>
 *   <li><b>{@link ReducedPlainBackPressureFlag} shares nothing on purpose</b> and does not extend the
 *       base. Its whole point is the <i>absence</i> of the surrounding accesses - giving it the
 *       scaffolding would make it a third copy of the faithful arm and destroy the 51x comparison it
 *       exists to provide.</li>
 * </ul>
 *
 * The scaffolding helper is {@code final} and tiny, so it inlines; it contains <i>no</i> access to any
 * arm's measured field, which is the rule that decides what may move into it.
 * <p>
 * jcstress permits this - its annotation processor requires a {@code @State} class to be public and
 * non-final, and bans inheritance only on {@code @Result} classes. The actors stay declared on each
 * {@code @State} arm regardless, which is also what the literal-actor rule above requires.
 */
public class BackPressureFlagVisibilityProbes {

    /** The single offset the modelled encode runs against. */
    static final long ENCODED_OFFSET = 1L;

    /**
     * Scaffolding shared by the two <i>faithful</i> arms: the state that is not under test, and the real
     * surrounding accesses that make them faithful rather than reduced.
     * <p>
     * {@link ReducedPlainBackPressureFlag} deliberately does not extend this - see the class javadoc.
     * Nothing here touches a measured field.
     */
    abstract static class SurroundedBackPressureState {

        final ConcurrentSkipListMap<Long, Optional<Object>> incompleteOffsets = new ConcurrentSkipListMap<>();

        boolean stateChangedSinceCommitStart;

        /**
         * INSTRUMENTATION ONLY - nothing in {@code PartitionState} corresponds to it. The real flag
         * publishes no payload (see the class javadoc), so an arm that measures publication has to
         * invent one. It stands for the encode result the block decision was taken from.
         */
        boolean blockedAtPayloadLength;

        SurroundedBackPressureState() {
            incompleteOffsets.put(ENCODED_OFFSET, Optional.empty());
        }

        /**
         * {@code getCommitDataIfDirty()} → {@code tryToEncodeOffsets()} →
         * {@code updateBlockFromEncodingResult()}, taking the over-threshold branch - everything the poll
         * thread does <i>before</i> it writes the flag under test.
         * <p>
         * The branch is kept rather than simplified to a single value: this models the over-threshold
         * path, which production takes regardless of {@code isEmpty()}, so the arm performs the same read
         * production does and lands on the same value either way.
         */
        final void encodeOverThreshold() {
            stateChangedSinceCommitStart = false;
            boolean ignoredEmpty = incompleteOffsets.isEmpty(); // tryToEncodeOffsets' first branch
            blockedAtPayloadLength = ignoredEmpty || true;      // instrumentation payload
        }
    }

    /**
     * The back-pressure flag <b>as shipped</b> before this change: plain, written last by the poll
     * thread's encode, read alone by the control thread.
     * <p>
     * <b>Result, 2026-09-07: 1,643 anomalies in 179,391,234 samples - 9.2e-6 per raced pair.</b> The plain
     * flag really is published with no release on this hardware: the control thread can see the partition
     * blocked while the state the block was computed from is still stale.
     * <p>
     * Read it against {@link ReducedPlainBackPressureFlag}, which is the same pair with the surrounding
     * accesses stripped and fires at 4.7e-4 - <b>51x higher</b>. The {@link ConcurrentSkipListMap} touch
     * one statement earlier suppresses the anomaly by a factor of fifty and does not close it, which is
     * the same incidental-fencing shape {@code CommitPathVisibilityProbes}' faithful arm measured at
     * ~130x for the {@code dirty} pair. <b>Incidental is not guaranteed</b>: that map access sits in a
     * branch ({@code incompleteOffsets.isEmpty()} short-circuits the whole encode) and nothing in the
     * source says the fence is load-bearing.
     */
    @JCStressTest
    @Description("Pre-fix back-pressure flag: plain allowedMoreRecords, publishing a plain encode result")
    @Outcome(id = "false, false", expect = ACCEPTABLE_INTERESTING,
            desc = "ANOMALY: control thread saw the partition blocked but a stale encode result behind it")
    @Outcome(id = {"true, false", "true, true", "false, true"}, expect = ACCEPTABLE,
            desc = "Non-anomalous orderings - either the block is not visible yet, or its cause is")
    @State
    public static class PlainBackPressureFlagPublishesNothing extends SurroundedBackPressureState {

        // THE TERM UNDER TEST - the absence of a modifier on this line is the whole experiment, which is
        // why it is declared here and not in the shared base.
        boolean allowedMoreRecords = true;

        /**
         * {@code getCommitDataIfDirty()} → {@code tryToEncodeOffsets()} →
         * {@code updateBlockFromEncodingResult()}, taking the over-threshold branch.
         */
        @Actor
        public void brokerPollThread() {
            encodeOverThreshold();                              // the real surrounding accesses
            allowedMoreRecords = false;                         // setAllowedMoreRecords(false)
        }

        /**
         * {@code couldBeTakenAsWork(wc)} - the flag read first, its cause read second. Kept literal: both
         * reads happen inside the raced window and their order is the measurement.
         */
        @Actor
        public void controlThread(ZZ_Result r) {
            r.r1 = allowedMoreRecords;      // isAllowedMoreRecords()
            r.r2 = blockedAtPayloadLength;  // the reason behind the block
        }
    }

    /**
     * The same pair with the flag {@code volatile} - the fix this PR ships, and the control that says
     * one keyword is enough. A volatile store releases everything written before it and a volatile load
     * acquires everything read after it, so the payload can stay plain.
     * <p>
     * <b>Result, 2026-09-07: 0 anomalies in 201,927,188 samples, outcome FORBIDDEN.</b> Read with the
     * class javadoc: this says the fence works, not that the fence was needed. The reason to fence the
     * field is the JMM argument and the SpotBugs finding, not this table.
     */
    @JCStressTest
    @Description("Fixed back-pressure flag: volatile allowedMoreRecords releases the plain encode result")
    @Outcome(id = "false, false", expect = FORBIDDEN,
            desc = "Publication through the volatile flag failed - would invalidate the fix")
    @Outcome(id = {"true, false", "true, true", "false, true"}, expect = ACCEPTABLE,
            desc = "Orderings the release/acquire edge permits")
    @State
    public static class VolatileBackPressureFlagPublishesEncode extends SurroundedBackPressureState {

        // THE TERM UNDER TEST - identical to the plain arm's declaration but for this one keyword.
        volatile boolean allowedMoreRecords = true;

        @Actor
        public void brokerPollThread() {
            encodeOverThreshold();
            allowedMoreRecords = false;
        }

        @Actor
        public void controlThread(ZZ_Result r) {
            r.r1 = allowedMoreRecords;
            r.r2 = blockedAtPayloadLength;
        }
    }

    /**
     * The reduced arm: the flag and its payload with <b>none</b> of the surrounding accesses - no map
     * touch, no neighbouring plain write. It exists to separate "the plain field is unpublished" from
     * "the plain field is unpublished but the code around it happens to fence it", which is the only way
     * to read {@link PlainBackPressureFlagPublishesNothing}'s zero.
     * <p>
     * It does not extend {@link SurroundedBackPressureState}, and must not: inheriting that scaffolding
     * is precisely the thing this arm is defined by not having.
     * <p>
     * <b>Result, 2026-09-07: 213,452 anomalies in 455,930,388 samples - 4.7e-4 per raced pair, 51x the
     * faithful arm.</b> So the faithful arm's much lower rate is explained by the surrounding accesses,
     * not by the anomaly being unreachable: strip them and it is common. That gap is the measurement -
     * the neighbouring concurrent code narrows the window by a factor of fifty and leaves it open.
     */
    @JCStressTest
    @Description("Reduced control: two adjacent plain writes, no surrounding concurrent accesses")
    @Outcome(id = "false, false", expect = ACCEPTABLE_INTERESTING,
            desc = "ANOMALY: reproduced with nothing around the pair - the reduction, not the neighbours")
    @Outcome(id = {"true, false", "true, true", "false, true"}, expect = ACCEPTABLE,
            desc = "Non-anomalous orderings")
    @State
    public static class ReducedPlainBackPressureFlag {

        boolean blockedAtPayloadLength;

        boolean allowedMoreRecords = true;

        @Actor
        public void brokerPollThread() {
            blockedAtPayloadLength = true;
            allowedMoreRecords = false;
        }

        @Actor
        public void controlThread(ZZ_Result r) {
            r.r1 = allowedMoreRecords;
            r.r2 = blockedAtPayloadLength;
        }
    }
}
