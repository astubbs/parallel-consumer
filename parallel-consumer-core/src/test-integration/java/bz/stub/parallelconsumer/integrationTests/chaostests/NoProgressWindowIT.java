package bz.stub.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The contract behind {@link ProgressProbe#withNoProgressWindow(Duration)}, and the RED CONTROL that
 * a widened window is not a disabled one.
 * <p>
 * <b>Why this needed a test rather than a replay.</b> {@code ChaosChurnStormIT} widened its window
 * from {@link ProgressProbe#NO_PROGRESS_WINDOW} to {@link ProgressProbe#CHURN_NO_PROGRESS_WINDOW}
 * because its own churn crosses the narrower one while the fleet is merely slow - the seeds, the
 * drain trajectories and the arithmetic are in
 * {@code docs/inflight/test-no-progress-window-may-not-transfer-to-w1.md}. The obvious way to check
 * a widening is to replay a firing seed and watch it go green, and that check is WORTHLESS here for
 * the reason {@link RebalanceDwellToggleIT} states about the dwell bound: the crossing is
 * probabilistic even on a fixed seed, so a green run may simply never have reached the condition. A
 * green there is an absence, the weakest evidence this repo recognises - and a window widened to
 * infinity would produce exactly the same green.
 * <p>
 * So the two halves are asserted directly. The half that matters is
 * {@link #theWidenedWindowStillFiresOnAFleetThatGenuinelyStops()}: a fleet that really does stop
 * still fails the run at the wider bound. Without it, "we widened the window" and "we deleted the
 * detector" are indistinguishable from the outside, which is the failure the whole
 * {@code docs/solutions/best-practices/a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md}
 * write-up is about.
 * <p>
 * Untagged deliberately, so it gates every default integration build - the
 * {@link RebalanceDwellToggleIT} / {@link ProgressProbeLedgerIT} pattern, broker-free and with no
 * wall clock spent waiting on a bound.
 */
class NoProgressWindowIT {

    private static final long EXPECTED_TOTAL = 100_000;
    /**
     * Comfortably outside {@link ProgressProbe#TAIL_SLACK} - the shape every recorded firing had,
     * and derived from the slack rather than written as a literal so that WIDENING the slack moves
     * this with it. A hard-coded outstanding count would keep the firing tests green through a slack
     * bump that had quietly disarmed the detector for the case they exist to cover.
     */
    private static final long THOUSANDS_OUTSTANDING = EXPECTED_TOTAL - (4 * ProgressProbe.TAIL_SLACK);

    private static ProgressProbe probe() {
        return ProgressProbe.forSeamTest("no-progress-group", "no-progress-topic", EXPECTED_TOTAL);
    }

    private static Duration over(Duration window) {
        return window.plusSeconds(1);
    }

    private static Duration under(Duration window) {
        return window.minusSeconds(1);
    }

    /**
     * <b>The calibration itself, pinned as a value.</b> Every other test here derives its durations
     * from these constants, so all of them stay green if a constant changes - they prove the
     * mechanism, never the number. The number is the whole argument: 60s is ~1.9x the longest
     * no-completion stretch measured on the churn storm while the detector was armed (32.1s over
     * thirteen replays, {@code docs/inflight/test-no-progress-window-may-not-transfer-to-w1.md}),
     * the same ratio {@link ProgressProbe#REBALANCE_DWELL_BOUND} was calibrated at.
     * <p>
     * <b>Going red here is not a failure, it is the request</b>: re-measure the pause distribution
     * with {@code -Dchaos.diagnoseStallRecovery=true} and record what it says, then move this line.
     * A gate quietly made less sensitive is the failure this whole test class exists to prevent, and
     * a symbolic assertion cannot tell a re-calibration from a bump to make CI stop complaining.
     */
    @Test
    void theCalibratedValuesAreWhatWasMeasured() {
        assertWithMessage("the widened window is a measured value, not a convenience - see this "
                + "method's javadoc before changing it")
                .that(ProgressProbe.CHURN_NO_PROGRESS_WINDOW).isEqualTo(Duration.ofSeconds(60));
        assertWithMessage("the default window every other scenario still holds")
                .that(ProgressProbe.NO_PROGRESS_WINDOW).isEqualTo(Duration.ofSeconds(30));
        assertWithMessage("the OTHER term - the recorded firings sat 1196-6513 records short, so a "
                + "slack wide enough to excuse them would blind the detector to the stall with "
                + "thousands remaining it exists to catch")
                .that(ProgressProbe.TAIL_SLACK).isEqualTo(500);
    }

    @Test
    void armedIsTheControl_theDefaultWindowFiresOnAFlatFleet() {
        ProgressProbe probe = probe();

        boolean violated = probe.recordFleetProgress(THOUSANDS_OUTSTANDING, over(ProgressProbe.NO_PROGRESS_WINDOW));

        assertWithMessage("without this arm the widened cases below prove nothing - a detector that "
                + "never fires either way would pass them")
                .that(violated).isTrue();
        assertThat(probe.getViolations()).hasSize(1);
        assertThat(probe.getViolations().get(0)).contains("NO_PROGRESS: fleet consumed count stuck");
    }

    /**
     * <b>The red control for the widening.</b> A fleet that genuinely stops still fails the run at
     * the wider bound, so {@code CHURN_NO_PROGRESS_WINDOW} is a re-calibration and not a deletion.
     */
    @Test
    void theWidenedWindowStillFiresOnAFleetThatGenuinelyStops() {
        ProgressProbe probe = probe().withNoProgressWindow(ProgressProbe.CHURN_NO_PROGRESS_WINDOW);

        boolean violated = probe.recordFleetProgress(THOUSANDS_OUTSTANDING, over(ProgressProbe.CHURN_NO_PROGRESS_WINDOW));

        assertWithMessage("a widened window that could not fire would be a disabled one, and nothing "
                + "would go red to say so")
                .that(violated).isTrue();
        assertThat(probe.getViolations()).hasSize(1);
    }

    /**
     * The calibration change itself: the crossings W1's own churn produces - measured at the 30s
     * bound, never at 60s - no longer gate.
     */
    @Test
    void theWidenedWindowIgnoresACrossingOfTheDefaultOne() {
        ProgressProbe probe = probe().withNoProgressWindow(ProgressProbe.CHURN_NO_PROGRESS_WINDOW);

        boolean violated = probe.recordFleetProgress(THOUSANDS_OUTSTANDING, over(ProgressProbe.NO_PROGRESS_WINDOW));

        assertThat(violated).isFalse();
        assertWithMessage("this is the whole point of the widening").that(probe.getViolations()).isEmpty();
    }

    /**
     * The violation text must name the window that was actually configured. A scenario's log is the
     * only place a reader learns which bound this run held itself to, and every recorded sighting in
     * the ledgers was read off that line.
     */
    @Test
    void theViolationNamesTheConfiguredWindowNotTheDefault() {
        ProgressProbe probe = probe().withNoProgressWindow(ProgressProbe.CHURN_NO_PROGRESS_WINDOW);

        probe.recordFleetProgress(THOUSANDS_OUTSTANDING, over(ProgressProbe.CHURN_NO_PROGRESS_WINDOW));

        assertThat(probe.getViolations().get(0))
                .contains("bound " + ProgressProbe.CHURN_NO_PROGRESS_WINDOW.getSeconds() + "s");
    }

    /**
     * The boundary itself, which every other case here steps a whole second clear of. The guard is
     * strictly-greater-than, so a stall of exactly the window does NOT fire - deliberate, and worth
     * pinning because the sampler compares a wall-clock difference where equality is reachable.
     */
    /**
     * <b>The sampler actually consults the decision, and re-arms after it fires.</b> Every other case
     * here calls {@link ProgressProbe#recordFleetProgress} directly, so deleting the sampler's call
     * to it would leave them all green while the running detector reported nothing - the hole an
     * independent cross-model review and this repo's own testing review found at the same time on
     * astubbs/parallel-consumer#499. Driven through a stubbed clock, so it costs no wall time.
     */
    @Test
    void theSamplerReachesTheDecisionAndReArmsAfterFiring() {
        Instant t0 = Instant.parse("2026-09-09T00:00:00Z");
        AtomicReference<Instant> now = new AtomicReference<>(t0);
        AtomicLong consumed = new AtomicLong(THOUSANDS_OUTSTANDING);
        ProgressProbe probe = ProgressProbe.forSeamTest("sampler-group", "sampler-topic", EXPECTED_TOTAL,
                consumed::get).withClock(now::get);

        probe.sampleProgress();                                   // first sample: records the count
        now.set(t0.plus(under(ProgressProbe.NO_PROGRESS_WINDOW))); // still inside the window
        probe.sampleProgress();
        assertWithMessage("inside the window the sampler must stay silent")
                .that(probe.getViolations()).isEmpty();

        now.set(t0.plus(over(ProgressProbe.NO_PROGRESS_WINDOW)));  // now past it, count unchanged
        probe.sampleProgress();
        assertWithMessage("if the sampler ever stops calling recordFleetProgress, every other test "
                + "in this class still passes and the running detector reports nothing")
                .that(probe.getViolations()).hasSize(1);

        now.set(t0.plus(over(ProgressProbe.NO_PROGRESS_WINDOW)).plusSeconds(1));
        probe.sampleProgress();
        assertWithMessage("re-armed: a genuine stall reports once per window, not once per sample")
                .that(probe.getViolations()).hasSize(1);
    }

    /**
     * <b>The churn storm wires the bound it means to.</b> Asserting the constant on a probe this test
     * configured itself proves nothing about the scenario - the second half of the same gap. This
     * calls the scenario's own configuration helper, so deleting the widening from
     * {@code ChaosChurnStormIT} turns this red in milliseconds rather than silently changing what a
     * five-minute chaos run gates on.
     */
    @Test
    void theChurnStormConfiguresItsProbeWithTheWidenedWindow() {
        ProgressProbe probe = ChaosChurnStormIT.configureProbe(probe());

        assertThat(probe.noProgressWindow()).isEqualTo(ProgressProbe.CHURN_NO_PROGRESS_WINDOW);
    }

    @Test
    void aStallOfExactlyTheWindowDoesNotFire() {
        ProgressProbe probe = probe();

        boolean violated = probe.recordFleetProgress(THOUSANDS_OUTSTANDING, ProgressProbe.NO_PROGRESS_WINDOW);

        assertThat(violated).isFalse();
        assertThat(probe.getViolations()).isEmpty();
    }

    @Test
    void aStallShorterThanTheWindowNeverFires() {
        ProgressProbe probe = probe();

        boolean violated = probe.recordFleetProgress(THOUSANDS_OUTSTANDING, under(ProgressProbe.NO_PROGRESS_WINDOW));

        assertThat(violated).isFalse();
        assertThat(probe.getViolations()).isEmpty();
    }

    /**
     * The OTHER term in the guard, pinned so a future re-calibration reaches for the window rather
     * than for this one. Inside the tail slack the detector is silent however long the stall - which
     * is why raising {@code TAIL_SLACK} to cover the recorded firings would have blinded it to
     * exactly the "stall with THOUSANDS remaining" its own javadoc names as the defect signature.
     */
    @Test
    void insideTheTailSlackNothingFiresHoweverLongTheStall() {
        ProgressProbe probe = probe();
        long insideTheSlack = EXPECTED_TOTAL - ProgressProbe.TAIL_SLACK;

        boolean violated = probe.recordFleetProgress(insideTheSlack, Duration.ofMinutes(10));

        assertThat(violated).isFalse();
        assertThat(probe.getViolations()).isEmpty();
    }
}
