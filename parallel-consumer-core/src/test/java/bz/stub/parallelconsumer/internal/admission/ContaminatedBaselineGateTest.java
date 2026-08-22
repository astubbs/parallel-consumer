package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.AT_CAP;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.PROBING;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * THE GATE: can the law descend from a contaminated baseline?
 * <p>
 * The simulation starts already saturated - the very first window carries degraded latency and high in-flight, so
 * the long EWMA never sees a clean baseline. It warms up on degraded values, the gradient reads flat-degraded
 * latency as healthy (gradient pinned at 1.0), the additive headroom grows the limit to the ceiling, and it pins
 * there.
 * <p>
 * VERDICT (encoded by {@link #portedLawAloneCannotDescendFromAContaminatedBaseline()}): the ported Gradient2 math
 * CANNOT descend - which is why {@link AdmissionControlLaw} adds the bounded periodic probe-DOWN (see the arm-6
 * javadoc), whose descent {@link #probeDownRemeasuresAndDescendsToNearCapacity()} asserts.
 * <p>
 * Deterministic closed-loop latency model (no randomness): service time is flat up to a modeled capacity and rises
 * linearly with every slot of concurrency beyond it.
 */
class ContaminatedBaselineGateTest {

    private static final double MS = 1_000_000.0;

    private static final double BASE_SERVICE_TIME_NANOS = 10 * MS;
    private static final int MODELED_CAPACITY_SLOTS = 40;
    private static final double OVERLOAD_LATENCY_SLOPE_PER_SLOT = 0.1;

    private static final int CEILING = 100;
    private static final int SAMPLES = 50;

    /**
     * Latency flat at base up to capacity, then +10% of base per excess slot.
     */
    private static double modeledLatency(int limit) {
        int excess = Math.max(0, limit - MODELED_CAPACITY_SLOTS);
        return BASE_SERVICE_TIME_NANOS * (1 + OVERLOAD_LATENCY_SLOPE_PER_SLOT * excess);
    }

    /**
     * One saturated closed-loop window at the law's current limit: latency follows the model, in-flight sits at
     * the limit.
     */
    private static AdmissionDecision runWindow(AdmissionControlLaw law) {
        int limit = law.getLimit();
        return law.onWindowClosed(new ClosedAdmissionWindow(
                SAMPLES, modeledLatency(limit), SAMPLES, limit, 2, SAMPLES, 0, 0));
    }

    @Test
    void portedLawAloneCannotDescendFromAContaminatedBaseline() {
        // probe-down disabled: this IS the ported Gradient2 law with the fork's outcome/starvation arms idle
        AdmissionControlLaw law = AdmissionControlLaw.newBuilder()
                .initialLimit(CEILING).ceiling(CEILING)
                .probeDownCadenceWindows(0)
                .build();

        for (int window = 0; window < 100; window++) {
            AdmissionDecision decision = runWindow(law);
            assertWithMessage("window %s: with no clean baseline the gradient reads saturation as health", window)
                    .that(law.getLimit()).isEqualTo(CEILING);
            assertThat(decision.getReason()).isEqualTo(AT_CAP);
        }
    }

    @Test
    void probeDownRemeasuresAndDescendsToNearCapacity() {
        AdmissionControlLaw law = AdmissionControlLaw.newBuilder()
                .initialLimit(CEILING).ceiling(CEILING)
                .build(); // probe-down at its default cadence

        boolean sawDownwardProbe = false;
        for (int window = 0; window < 150; window++) {
            int before = law.getLimit();
            AdmissionDecision decision = runWindow(law);
            if (decision.getReason() == PROBING && decision.getTargetConcurrency() < before) {
                sawDownwardProbe = true;
            }
        }
        assertWithMessage("descent must happen through the re-measure probe").that(sawDownwardProbe).isTrue();

        // after descent, the law must stay near the modeled capacity: never re-pinning at the cap, never
        // collapsing to the floor
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        for (int window = 0; window < 20; window++) {
            runWindow(law);
            min = Math.min(min, law.getLimit());
            max = Math.max(max, law.getLimit());
        }
        assertWithMessage("limit should have descended from the contaminated cap")
                .that(max).isAtMost((int) (1.5 * MODELED_CAPACITY_SLOTS));
        assertWithMessage("descent must not collapse the limit")
                .that(min).isAtLeast(MODELED_CAPACITY_SLOTS / 2);

        // and the baseline itself has been re-measured down to sane values (it started at 70ms - fully degraded)
        assertThat(law.getServiceTimeBaselineNanos()).isLessThan(3 * BASE_SERVICE_TIME_NANOS);
    }
}
