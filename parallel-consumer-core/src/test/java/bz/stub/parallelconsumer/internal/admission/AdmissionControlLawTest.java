package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static bz.stub.parallelconsumer.internal.admission.AdmissionControlLaw.AIMD_BACKOFF_RATIO;
import static bz.stub.parallelconsumer.internal.admission.AdmissionControlLaw.DEFAULT_PROBE_DOWN_CADENCE_WINDOWS;
import static bz.stub.parallelconsumer.internal.admission.AdmissionControlLaw.DEFAULT_PROBE_DOWN_RATIO;
import static bz.stub.parallelconsumer.internal.admission.AdmissionControlLaw.DEFAULT_QUEUE_HEADROOM_SLOTS;
import static bz.stub.parallelconsumer.internal.admission.AdmissionControlLaw.DEFAULT_SMOOTHING;
import static bz.stub.parallelconsumer.internal.admission.AdmissionControlLaw.GRADIENT_FLOOR;
import static bz.stub.parallelconsumer.internal.admission.AdmissionControlLaw.LIMIT_FLOOR_SLOTS;
import static bz.stub.parallelconsumer.internal.admission.AdmissionControlLaw.PROBE_UP_STEP_SLOTS;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.ADAPTING;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.APP_LIMITED;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.AT_CAP;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.AT_FLOOR;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.BACKOFF;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.FAILURE_LIMITED;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.PROBING;
import static bz.stub.parallelconsumer.internal.admission.TestWindows.saturated;
import static bz.stub.parallelconsumer.internal.admission.TestWindows.withDrops;
import static bz.stub.parallelconsumer.internal.admission.TestWindows.withIgnores;
import static bz.stub.parallelconsumer.internal.admission.TestWindows.withInFlight;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Deterministic exact-sequence tests of the control-law arms, in the style of upstream's {@code VegasLimitTest}:
 * every window is an explicit call, no clock, no sleeps. When the gradient pins at 1.0 with flat latency the
 * per-window growth is EXACTLY {@code smoothing * queueHeadroom} slots, which many tests pin to kill mutations of
 * the smoothing and headroom terms.
 */
class AdmissionControlLawTest {

    private static final double MS = 1_000_000.0; // nanos per millisecond
    private static final double EXACT = 1e-6;

    /**
     * Per-window growth when the gradient pins at 1.0: smoothing * headroom = 0.8 slots with defaults.
     */
    private static final double STEADY_GROWTH_PER_WINDOW = DEFAULT_SMOOTHING * DEFAULT_QUEUE_HEADROOM_SLOTS;

    private static final int SAMPLES = 50;

    /**
     * Feeds {@code windows} healthy saturated windows at the given service time; in-flight median tracks the
     * law's own limit, as a saturated closed-loop system would.
     */
    private static void feedSaturated(AdmissionControlLaw law, int windows, double serviceTimeNanos) {
        for (int i = 0; i < windows; i++) {
            law.onWindowClosed(saturated(SAMPLES, serviceTimeNanos, law.getLimit()));
        }
    }

    // ------------------------------------------------------------------
    // Step response
    // ------------------------------------------------------------------

    @Test
    void latencyStepContractsWithinBoundedWindows_perWindowCutBoundedByGradientFloorTimesSmoothing() {
        AdmissionControlLaw law = AdmissionControlLaw.newBuilder().initialLimit(50).ceiling(200).build();

        feedSaturated(law, 20, 10 * MS);
        // steady low latency: gradient pinned at 1.0, growth exactly smoothing * headroom per window
        double preStep = law.getEstimatedLimit();
        assertThat(preStep).isWithin(EXACT).of(50 + 20 * STEADY_GROWTH_PER_WINDOW);

        // 4x latency step
        double worstCaseCutFactor = 1 - DEFAULT_SMOOTHING * (1 - GRADIENT_FLOOR); // = 0.9 with defaults
        for (int i = 0; i < 12; i++) {
            double before = law.getEstimatedLimit();
            AdmissionDecision decision = law.onWindowClosed(saturated(SAMPLES, 40 * MS, law.getLimit()));

            assertThat(decision.getReason()).isEqualTo(ADAPTING);
            // the cut per window is bounded below by the gradient floor combined with smoothing
            assertWithMessage("window %s cut more than gradient floor x smoothing allows", i)
                    .that(law.getEstimatedLimit())
                    .isAtLeast(before * worstCaseCutFactor - EXACT);
            // and with the gradient pinned at its floor the recurrence is exact
            assertThat(law.getEstimatedLimit())
                    .isWithin(EXACT)
                    .of(before * worstCaseCutFactor + DEFAULT_SMOOTHING * DEFAULT_QUEUE_HEADROOM_SLOTS);
        }

        assertWithMessage("limit should contract to under 55 percent of pre-step within 12 windows")
                .that(law.getEstimatedLimit()).isLessThan(0.55 * preStep);
    }

    // ------------------------------------------------------------------
    // Recovery and the anti-drift decay
    // ------------------------------------------------------------------

    @Test
    void recoveryRegrowsViaHeadroom_andDecayUnsticksStaleHighBaseline() {
        AdmissionControlLaw law = AdmissionControlLaw.newBuilder().initialLimit(30).ceiling(200).build();

        feedSaturated(law, 15, 80 * MS);
        double staleBaseline = law.getServiceTimeBaselineNanos();
        assertThat(staleBaseline).isWithin(EXACT).of(80 * MS);

        // latency returns to baseline: growth resumes via the additive headroom term, exactly, every window
        for (int i = 0; i < 30; i++) {
            double before = law.getEstimatedLimit();
            law.onWindowClosed(saturated(SAMPLES, 10 * MS, law.getLimit()));
            assertWithMessage("window %s regrowth should be exactly smoothing * headroom", i)
                    .that(law.getEstimatedLimit())
                    .isWithin(EXACT)
                    .of(before + STEADY_GROWTH_PER_WINDOW);
        }

        // The 0.95 long-EWMA decay unsticks the stale-high baseline: without it, EWMA alone (span 600) would
        // still read ~72ms here; with it the baseline has collapsed towards the true 10ms.
        assertThat(law.getServiceTimeBaselineNanos()).isLessThan(25 * MS);
        assertThat(law.getServiceTimeBaselineNanos()).isAtLeast(10 * MS);
    }

    // ------------------------------------------------------------------
    // App-limited hold
    // ------------------------------------------------------------------

    @Test
    void appLimitedWindowsHoldTheLimitBitIdentical() {
        AdmissionControlLaw law = AdmissionControlLaw.newBuilder().initialLimit(40).ceiling(200).build();
        double held = law.getEstimatedLimit();

        double[] latenciesMs = {10, 30, 5, 90, 10};
        for (double latencyMs : latenciesMs) {
            // in-flight median 10 = limit / 4: below the app-limited threshold (half the limit) but not "far
            // below" it (starvation needs median < limit / 4)
            AdmissionDecision decision = law.onWindowClosed(withInFlight(SAMPLES, latencyMs * MS, 10, 0));

            assertThat(decision.getReason()).isEqualTo(APP_LIMITED);
            // bit-identical: the estimate is HELD, not merely re-derived close by
            assertThat(law.getEstimatedLimit()).isEqualTo(held);
            assertThat(decision.getTargetConcurrency()).isEqualTo(40);
        }
    }

    // ------------------------------------------------------------------
    // Clamps and numeric guards
    // ------------------------------------------------------------------

    @Test
    void backoffNeverCutsBelowOneSlotFloor() {
        AdmissionControlLaw law = AdmissionControlLaw.newBuilder().initialLimit(5).ceiling(100).build();

        for (int i = 0; i < 40; i++) {
            AdmissionDecision decision = law.onWindowClosed(withDrops(SAMPLES, 10 * MS, law.getLimit(), 3));
            assertThat(decision.getReason()).isEqualTo(BACKOFF);
            assertWithMessage("window %s went below the one-slot floor", i)
                    .that(law.getEstimatedLimit()).isAtLeast((double) LIMIT_FLOOR_SLOTS);
        }
        assertThat(law.getEstimatedLimit()).isEqualTo((double) LIMIT_FLOOR_SLOTS);
    }

    @Test
    void gradientContractionClampsAtFloorWithReasonAtFloor() {
        // headroom 0 removes the additive growth term so pure contraction can reach the floor
        AdmissionControlLaw law = AdmissionControlLaw.newBuilder()
                .initialLimit(50).ceiling(100).queueHeadroomSlots(0).build();

        feedSaturated(law, 10, 10 * MS);
        assertThat(law.getEstimatedLimit()).isWithin(EXACT).of(50.0); // headroom 0: no growth at steady state

        AdmissionDecision last = null;
        for (int i = 0; i < 60; i++) {
            last = law.onWindowClosed(saturated(SAMPLES, 100 * MS, law.getLimit()));
            assertThat(law.getEstimatedLimit()).isAtLeast((double) LIMIT_FLOOR_SLOTS);
        }
        assertThat(law.getEstimatedLimit()).isEqualTo((double) LIMIT_FLOOR_SLOTS);
        assertThat(last.getReason()).isEqualTo(AT_FLOOR);
    }

    @Test
    void growthClampsAtCeilingWithReasonAtCap() {
        AdmissionControlLaw law = AdmissionControlLaw.newBuilder()
                .initialLimit(20).ceiling(30)
                .probeDownCadenceWindows(0) // isolate the clamp from the re-measure probe
                .build();

        AdmissionDecision last = null;
        for (int i = 0; i < 25; i++) {
            last = law.onWindowClosed(saturated(SAMPLES, 10 * MS, law.getLimit()));
            assertWithMessage("window %s exceeded the ceiling", i)
                    .that(law.getEstimatedLimit()).isAtMost(30.0);
        }
        assertThat(law.getEstimatedLimit()).isEqualTo(30.0);
        assertThat(last.getReason()).isEqualTo(AT_CAP);
    }

    @Test
    void zeroLatencyWindowIsGuardedAgainstNaN() {
        AdmissionControlLaw law = AdmissionControlLaw.newBuilder().initialLimit(20).ceiling(100).build();

        for (int i = 0; i < 3; i++) {
            law.onWindowClosed(saturated(SAMPLES, 0.0, law.getLimit()));
            // without the MIN_SERVICE_TIME_NANOS guard the gradient divides by zero, the estimate goes NaN and
            // the integer limit collapses to 0
            assertThat(Double.isFinite(law.getEstimatedLimit())).isTrue();
            assertThat(law.getLimit()).isAtLeast(LIMIT_FLOOR_SLOTS);
            assertThat(law.getLimit()).isAtMost(100);
        }
    }

    @Test
    void extremeLatenciesNeitherOverflowNorEscapeTheClamps() {
        AdmissionControlLaw law = AdmissionControlLaw.newBuilder().initialLimit(20).ceiling(100).build();

        for (int i = 0; i < 5; i++) {
            law.onWindowClosed(saturated(SAMPLES, (double) Long.MAX_VALUE, law.getLimit()));
            assertThat(Double.isFinite(law.getEstimatedLimit())).isTrue();
            assertThat(law.getLimit()).isAtLeast(LIMIT_FLOOR_SLOTS);
            assertThat(law.getLimit()).isAtMost(100);
        }
        // whiplash back to a tiny latency: the drift decay path must also stay finite
        law.onWindowClosed(saturated(SAMPLES, 1.0, law.getLimit()));
        assertThat(Double.isFinite(law.getEstimatedLimit())).isTrue();
        assertThat(law.getLimit()).isAtLeast(LIMIT_FLOOR_SLOTS);
    }

    // ------------------------------------------------------------------
    // Oscillation band
    // ------------------------------------------------------------------

    @Test
    void constantLatencyKeepsTheLimitInABoundedBand() {
        AdmissionControlLaw law = AdmissionControlLaw.newBuilder().initialLimit(20).ceiling(60).build();

        List<Double> lastEstimates = new ArrayList<>();
        boolean sawProbe = false;
        for (int i = 0; i < 150; i++) {
            AdmissionDecision decision = law.onWindowClosed(saturated(SAMPLES, 10 * MS, law.getLimit()));
            if (i >= 120) {
                lastEstimates.add(law.getEstimatedLimit());
                sawProbe |= decision.getReason() == PROBING;
            }
        }

        double band = Collections.max(lastEstimates) - Collections.min(lastEstimates);
        // the only movement at the cap is the periodic re-measure probe and its headroom regrowth
        double allowedBand = 60 * (1 - DEFAULT_PROBE_DOWN_RATIO)
                + DEFAULT_PROBE_DOWN_CADENCE_WINDOWS * STEADY_GROWTH_PER_WINDOW * 0.25 + 0.5;
        assertWithMessage("limit oscillation band over the last 30 windows").that(band).isAtMost(allowedBand);
        // and the band sits at the top of the range - no descent spiral under constant latency
        assertThat(Collections.min(lastEstimates)).isAtLeast(60 * DEFAULT_PROBE_DOWN_RATIO - 1);
        assertThat(sawProbe).isTrue();
    }

    // ------------------------------------------------------------------
    // Outcome arms: drops, ignores, failure fraction
    // ------------------------------------------------------------------

    @Test
    void overloadDropsFireTheAimdArmOncePerWindow_regardlessOfDropCount() {
        AdmissionControlLaw one = AdmissionControlLaw.newBuilder().initialLimit(20).ceiling(100).build();
        AdmissionControlLaw many = AdmissionControlLaw.newBuilder().initialLimit(20).ceiling(100).build();
        feedSaturated(one, 10, 10 * MS);
        feedSaturated(many, 10, 10 * MS);
        double warm = one.getEstimatedLimit();
        assertThat(many.getEstimatedLimit()).isEqualTo(warm);

        AdmissionDecision d1 = one.onWindowClosed(withDrops(SAMPLES, 10 * MS, one.getLimit(), 1));
        AdmissionDecision d5 = many.onWindowClosed(withDrops(SAMPLES, 10 * MS, many.getLimit(), 5));

        assertThat(d1.getReason()).isEqualTo(BACKOFF);
        assertThat(d5.getReason()).isEqualTo(BACKOFF);
        // one multiplicative cut for the window, however many drops it carried
        assertThat(one.getEstimatedLimit()).isWithin(EXACT).of(warm * AIMD_BACKOFF_RATIO);
        assertThat(many.getEstimatedLimit()).isEqualTo(one.getEstimatedLimit());
    }

    @Test
    void ignoreOutcomesBelowTheFailureThresholdLeaveTheLimitUntouched() {
        AdmissionControlLaw withoutIgnores = AdmissionControlLaw.newBuilder().initialLimit(20).ceiling(100).build();
        AdmissionControlLaw withSomeIgnores = AdmissionControlLaw.newBuilder().initialLimit(20).ceiling(100).build();
        feedSaturated(withoutIgnores, 10, 10 * MS);
        feedSaturated(withSomeIgnores, 10, 10 * MS);

        AdmissionDecision clean = withoutIgnores.onWindowClosed(
                withIgnores(SAMPLES, 10 * MS, withoutIgnores.getLimit(), 100, 0));
        // 10% ignores: under the freeze threshold, and ignores never enter the latency math
        AdmissionDecision ignoring = withSomeIgnores.onWindowClosed(
                withIgnores(SAMPLES, 10 * MS, withSomeIgnores.getLimit(), 90, 10));

        assertThat(ignoring.getReason()).isEqualTo(clean.getReason());
        assertThat(withSomeIgnores.getEstimatedLimit()).isEqualTo(withoutIgnores.getEstimatedLimit());
    }

    @Test
    void risingFailureFractionWithFallingLatencyMustNotGrowTheLimit() {
        AdmissionControlLaw law = AdmissionControlLaw.newBuilder().initialLimit(20).ceiling(100).build();
        feedSaturated(law, 12, 40 * MS);
        double before = law.getEstimatedLimit();

        // Falling latency (fast-rejecting overloaded downstream) - the gradient alone would read this as
        // headroom and grow. Half the outcomes are non-successes.
        AdmissionDecision decision = law.onWindowClosed(
                withIgnores(SAMPLES, 20 * MS, law.getLimit(), 25, 25));

        assertThat(decision.getReason()).isEqualTo(FAILURE_LIMITED);
        assertWithMessage("growth must be frozen while failures dominate")
                .that(law.getEstimatedLimit()).isAtMost(before);

        // control arm: the SAME latency with clean outcomes does grow - failures were the only inhibitor
        AdmissionDecision healthy = law.onWindowClosed(saturated(SAMPLES, 20 * MS, law.getLimit()));
        assertThat(healthy.getReason()).isEqualTo(ADAPTING);
        assertThat(law.getEstimatedLimit()).isGreaterThan(before);
    }

    @Test
    void failureLimitedWindowsMayStillContractViaTheGradient() {
        AdmissionControlLaw law = AdmissionControlLaw.newBuilder().initialLimit(20).ceiling(100).build();
        feedSaturated(law, 12, 10 * MS);
        double before = law.getEstimatedLimit();

        // rising latency AND failures: FAILURE_LIMITED must not block the contraction
        AdmissionDecision decision = law.onWindowClosed(
                withIgnores(SAMPLES, 100 * MS, law.getLimit(), 25, 25));

        assertThat(decision.getReason()).isEqualTo(FAILURE_LIMITED);
        assertThat(law.getEstimatedLimit()).isLessThan(before);
    }

    // ------------------------------------------------------------------
    // Slow workloads (time-bound closes with almost no samples)
    // ------------------------------------------------------------------

    @Test
    void windowsBelowTheSampleMinimumAreHeld_thenTheLimitRecoversOnceSamplesSuffice() {
        AdmissionControlLaw law = AdmissionControlLaw.newBuilder().initialLimit(20).ceiling(100).build();
        feedSaturated(law, 12, 10 * MS);
        double held = law.getEstimatedLimit();
        double baseline = law.getServiceTimeBaselineNanos();

        // the caller closes on the time bound with only 1-2 samples; latency in them is wild
        for (int i = 0; i < 5; i++) {
            AdmissionDecision decision = law.onWindowClosed(
                    TestWindows.window(2, 500 * MS, 2, 2, 0, 2, 0, 0));
            assertThat(decision.getReason()).isEqualTo(APP_LIMITED);
            assertThat(law.getEstimatedLimit()).isEqualTo(held);
        }
        // a signal-free window must leave the baseline untouched too
        assertThat(law.getServiceTimeBaselineNanos()).isEqualTo(baseline);

        // once samples suffice, growth resumes at the steady rate - recovery within a bounded window count
        int recoveryWindows = 10;
        feedSaturated(law, recoveryWindows, 10 * MS);
        assertThat(law.getEstimatedLimit())
                .isWithin(EXACT)
                .of(held + recoveryWindows * STEADY_GROWTH_PER_WINDOW);
    }

    // ------------------------------------------------------------------
    // Starvation probe
    // ------------------------------------------------------------------

    @Test
    void starvedWindowProbesUpOneBoundedStep_notAPersistentFreeze() {
        AdmissionControlLaw law = AdmissionControlLaw.newBuilder().initialLimit(40).ceiling(100).build();
        feedSaturated(law, 12, 10 * MS);
        double before = law.getEstimatedLimit(); // 49.6

        // starvation signature: median far below the limit, tight unimodal spread, flat latency
        AdmissionDecision first = law.onWindowClosed(withInFlight(SAMPLES, 10 * MS, 5, 5));
        assertThat(first.getReason()).isEqualTo(PROBING);
        assertThat(law.getEstimatedLimit()).isWithin(EXACT).of(before + PROBE_UP_STEP_SLOTS);

        // the ratchet must not lock: a second starved window probes again rather than freezing
        AdmissionDecision second = law.onWindowClosed(withInFlight(SAMPLES, 10 * MS, 5, 5));
        assertThat(second.getReason()).isEqualTo(PROBING);
        assertThat(law.getEstimatedLimit()).isWithin(EXACT).of(before + 2 * PROBE_UP_STEP_SLOTS);
    }

    @Test
    void bimodalInFlightSpreadDoesNotClassifyAsStarved() {
        AdmissionControlLaw law = AdmissionControlLaw.newBuilder().initialLimit(40).ceiling(100).build();
        feedSaturated(law, 12, 10 * MS);
        double before = law.getEstimatedLimit();

        // same low median, but the spread says half the samples sat near the limit: bimodal, not starved
        AdmissionDecision decision = law.onWindowClosed(withInFlight(SAMPLES, 10 * MS, 5, 30));

        assertThat(decision.getReason()).isEqualTo(APP_LIMITED);
        assertThat(law.getEstimatedLimit()).isEqualTo(before);
    }

    @Test
    void starvationRequiresFlatLatency() {
        AdmissionControlLaw law = AdmissionControlLaw.newBuilder().initialLimit(40).ceiling(100).build();
        feedSaturated(law, 12, 10 * MS);
        double before = law.getEstimatedLimit();

        // low median and tight spread, but latency is 4x the baseline - not a starvation signature
        AdmissionDecision decision = law.onWindowClosed(withInFlight(SAMPLES, 40 * MS, 5, 5));

        assertThat(decision.getReason()).isEqualTo(APP_LIMITED);
        assertThat(law.getEstimatedLimit()).isEqualTo(before);
    }

    @Test
    void starvedProbeNeverStepsPastTheCeiling() {
        AdmissionControlLaw law = AdmissionControlLaw.newBuilder().initialLimit(40).ceiling(41).build();
        // flat baseline first
        law.onWindowClosed(saturated(SAMPLES, 10 * MS, law.getLimit()));
        // that window grew the limit to 40.8; a starved window may only step to the ceiling, never past it
        AdmissionDecision decision = law.onWindowClosed(withInFlight(SAMPLES, 10 * MS, 5, 5));

        assertThat(law.getEstimatedLimit()).isAtMost(41.0);
        assertThat(decision.getTargetConcurrency()).isAtMost(41);
    }
}
