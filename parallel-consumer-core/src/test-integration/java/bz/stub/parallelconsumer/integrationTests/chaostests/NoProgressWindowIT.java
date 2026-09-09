package bz.stub.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.time.Duration;

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
    /** Comfortably outside {@link ProgressProbe#TAIL_SLACK}: the shape every recorded firing had. */
    private static final long THOUSANDS_OUTSTANDING = EXPECTED_TOTAL - 3_000;

    private static ProgressProbe probe() {
        return ProgressProbe.forSeamTest("no-progress-group", "no-progress-topic", EXPECTED_TOTAL);
    }

    private static Duration over(Duration window) {
        return window.plusSeconds(1);
    }

    private static Duration under(Duration window) {
        return window.minusSeconds(1);
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
