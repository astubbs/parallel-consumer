package bz.stub.parallelconsumer.dashboard.ui;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The page's tween timing - {@code tween.js} - executed as the browser executes it.
 *
 * <h2>What this suite is for</h2>
 * <p>
 * This is the arithmetic that decides how long each tween runs for, and getting it wrong is what made the page look
 * "jumpy" rather than broken: every value stayed correct, every document arrived, and the motion stuttered. That is
 * a defect no compile step catches, no screenshot catches, and no assertion about the state document catches -
 * which is exactly why the logic was pulled out of {@code app.js}, where it sat behind a DOM lookup and could only
 * be checked by opening a browser and watching.
 *
 * <h2>The failure it exists to pin</h2>
 * <p>
 * Sizing a tween from the last observed gap alone freezes the picture whenever the next document is late, and
 * documents are late roughly half the time because arrival gaps are not even. The asymmetry asserted below - rise
 * instantly, fall slowly - is the whole fix, so it is asserted in both directions rather than by one round trip
 * that a symmetric average would also pass.
 *
 * <h2>One thread, deliberately</h2>
 * <p>
 * A GraalVM context may not be entered from a second thread, and this repository runs JUnit methods concurrently by
 * default. See {@code OffsetModelTest} for the same constraint.
 */
@Execution(ExecutionMode.SAME_THREAD)
class TweenTimingTest {

    private static PageModules page;

    private static Value tween;

    @BeforeAll
    static void open() {
        page = PageModules.open();
        tween = page.module(PageModules.TWEEN);
    }

    @AfterAll
    static void close() {
        page.close();
    }

    private static double estimate(Double previous, double observedGap) {
        Value result = tween.getMember("nextGapEstimate")
                .execute(previous == null ? null : previous, observedGap);
        return result.asDouble();
    }

    private static boolean interruption(Double gap, double staleThreshold) {
        return tween.getMember("isInterruption")
                .execute(gap == null ? null : gap, staleThreshold)
                .asBoolean();
    }

    private static double fraction(double elapsed, double duration) {
        return tween.getMember("tweenFraction").execute(elapsed, duration).asDouble();
    }

    @Test
    void theFirstObservedGapIsTheEstimateOutright() {
        assertThat(estimate(null, 250)).isEqualTo(250);
    }

    /**
     * The half of the asymmetry that removes the freeze. A tween sized shorter than the next gap runs out with
     * nothing left to draw, so one late document has to be enough - waiting for an average to catch up would let
     * the same pause happen again on the very next tween.
     */
    @Test
    void aLongerGapIsAdoptedImmediatelyAndInFull() {
        assertThat(estimate(100.0, 900)).as("no easing at all in this direction").isEqualTo(900);
    }

    /**
     * The other half, which removes the step. A tween sized longer than the next gap is still mid-way when the new
     * document retargets it, so one early document must not be allowed to shorten the estimate to match.
     */
    @Test
    void aShorterGapIsOnlyEasedIntoRatherThanAdopted() {
        double eased = estimate(1000.0, 100);

        assertThat(eased).as("moved towards the shorter gap").isLessThan(1000);
        assertThat(eased).as("but nowhere near it after one observation").isGreaterThan(500);
    }

    /**
     * A single anomalous gap must not still be setting the tween duration many documents later. Rising instantly is
     * only safe because the fall is monotonic and gets there.
     */
    @Test
    void aRunOfShorterGapsConvergesBackDownTowardsThem() {
        double current = 2000;
        for (int i = 0; i < 30; i++) {
            current = estimate(current, 100);
        }

        assertThat(current).isCloseTo(100, org.assertj.core.data.Offset.offset(1.0));
    }

    /**
     * A gap of zero or less carries no information about cadence - two documents delivered in the same millisecond,
     * or a clock that did not move. Folding it in would collapse the estimate and freeze the page solid.
     */
    @Test
    void anUnusableGapLeavesTheEstimateAlone() {
        assertThat(estimate(400.0, 0)).isEqualTo(400);
        assertThat(estimate(400.0, -5)).isEqualTo(400);
    }

    @Test
    void aGapPastTheStaleThresholdIsAnInterruptionRatherThanACadence() {
        assertThat(interruption(9000.0, 8000)).isTrue();
        assertThat(interruption(7999.0, 8000)).isFalse();
        assertThat(interruption(null, 8000)).as("the very first document has no gap behind it").isFalse();
    }

    @Test
    void theTweenRunsFromZeroToOneAcrossItsDuration() {
        assertThat(fraction(0, 200)).isZero();
        assertThat(fraction(100, 200)).isEqualTo(0.5);
        assertThat(fraction(200, 200)).isEqualTo(1);
    }

    /**
     * Clamped, never extrapolated. Running on past the newest sample at the last known velocity would draw a number
     * the instance never reported - and on a page about somebody's consumer, a plausible invented value is worse
     * than a picture that has visibly stopped.
     */
    @Test
    void aTweenWhoseNextDocumentIsLateHoldsAtTheNewestSampleRatherThanRunningPastIt() {
        assertThat(fraction(10_000, 200)).isEqualTo(1);
    }

    @Test
    void aDurationOfZeroLeavesNothingToTweenSoTheNewestSampleIsDrawnOutright() {
        assertThat(fraction(50, 0)).isEqualTo(1);
    }
}
