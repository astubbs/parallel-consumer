package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.AdaptiveConcurrencyMode;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.internal.admission.AdmissionController.Outcome;
import org.junit.jupiter.api.Test;
import org.threeten.extra.MutableClock;

import java.time.Duration;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.DEFAULT_MAX_CONCURRENCY;
import static bz.stub.parallelconsumer.internal.admission.AdmissionController.ADAPTIVE_DEFAULT_CEILING;
import static bz.stub.parallelconsumer.internal.admission.AdmissionController.SAMPLE_WINDOW_DURATION;
import static bz.stub.parallelconsumer.internal.admission.AdmissionControlLaw.DEFAULT_MIN_SAMPLES_PER_WINDOW;
import static bz.stub.parallelconsumer.internal.admission.AdmissionControlLaw.LIMIT_FLOOR_SLOTS;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.ADAPTING;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.AT_CAP;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.AT_FLOOR;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The controller's OWN responsibilities - KTD4 ceiling resolution, R4 start value, mode-gated publication, clamps,
 * and the tick cadence. All time comes from an injected {@link MutableClock} (no wall clock anywhere); all law
 * behaviour beyond the clamp boundaries is {@link AdmissionControlLawTest}'s job.
 */
class AdmissionControllerTest {

    private static final long MS = 1_000_000L; // nanos per millisecond

    /** Comfortably above the law's per-window minimum so every fed window is acted on. */
    private static final int SAMPLES = DEFAULT_MIN_SAMPLES_PER_WINDOW + 2;

    private final MutableClock clock = MutableClock.epochUTC();

    private ParallelConsumerOptions<?, ?> options(AdaptiveConcurrencyMode mode, int maxConcurrency, int seed) {
        return ParallelConsumerOptions.builder()
                .adaptiveConcurrencyMode(mode)
                .maxConcurrency(maxConcurrency)
                .adaptiveConcurrencyInitialTarget(seed)
                .build();
    }

    private AdmissionController controller(AdaptiveConcurrencyMode mode, int maxConcurrency) {
        return new AdmissionController(options(mode, maxConcurrency, 0), clock);
    }

    /**
     * Feeds one healthy, saturated window (every outcome a success, in-flight pinned at {@code inFlightMedian})
     * and ticks past the time bound - the controller-level analogue of {@link TestWindows#saturated}.
     */
    private void feedWindowAndTick(AdmissionController controller, long meanServiceTimeNanos, int inFlightMedian) {
        for (int i = 0; i < SAMPLES; i++) {
            controller.recordServiceTime(meanServiceTimeNanos);
            controller.recordInFlight(inFlightMedian);
            controller.recordOutcome(Outcome.SUCCESS);
        }
        clock.add(SAMPLE_WINDOW_DURATION);
        controller.tick();
    }

    // ------------------------------------------------------------------
    // KTD4: ceiling resolution
    // ------------------------------------------------------------------

    @Test
    void userSetMaxConcurrencyWinsUnderEnforce() {
        var controller = controller(AdaptiveConcurrencyMode.ENFORCE, 24);

        assertThat(controller.effectiveMaximum()).isEqualTo(24);
        assertThat(controller.wouldBeEnforceCeiling()).isEqualTo(24);
    }

    @Test
    void libraryDefaultUnderEnforceSubstitutesTheAdaptiveDefaultCeiling() {
        var controller = controller(AdaptiveConcurrencyMode.ENFORCE, DEFAULT_MAX_CONCURRENCY);

        assertThat(controller.effectiveMaximum()).isEqualTo(ADAPTIVE_DEFAULT_CEILING);
        assertThat(controller.wouldBeEnforceCeiling()).isEqualTo(ADAPTIVE_DEFAULT_CEILING);
    }

    @Test
    void libraryDefaultUnderObserveKeepsTheDefaultWhileReportingTheEnforceCeiling() {
        var controller = controller(AdaptiveConcurrencyMode.OBSERVE, DEFAULT_MAX_CONCURRENCY);

        // A non-acting mode resizes nothing - the effective maximum stays what static config would use...
        assertThat(controller.effectiveMaximum()).isEqualTo(DEFAULT_MAX_CONCURRENCY);
        // ...while the would-be computation runs against the ceiling ENFORCE would substitute.
        assertThat(controller.wouldBeEnforceCeiling()).isEqualTo(ADAPTIVE_DEFAULT_CEILING);
    }

    // ------------------------------------------------------------------
    // R4: start value
    // ------------------------------------------------------------------

    @Test
    void unseededStartIsTheStaticDerivedTargetNeverTheSubstitutedCeiling() {
        var controller = controller(AdaptiveConcurrencyMode.ENFORCE, DEFAULT_MAX_CONCURRENCY);

        // t=0 admission never exceeds today's static behaviour, even though the ceiling substituted to 64.
        assertThat(controller.currentTarget()).isEqualTo(DEFAULT_MAX_CONCURRENCY);
        assertThat(controller.wouldBeTarget()).isEqualTo(DEFAULT_MAX_CONCURRENCY);
        assertThat(controller.effectiveMaximum()).isEqualTo(ADAPTIVE_DEFAULT_CEILING);
    }

    @Test
    void seededStartPublishesTheSeedUnderEnforce() {
        var controller = new AdmissionController(options(AdaptiveConcurrencyMode.ENFORCE, 24, 8), clock);

        assertThat(controller.currentTarget()).isEqualTo(8);
    }

    @Test
    void seedUnderObserveSeedsOnlyTheWouldBeTarget() {
        var controller = new AdmissionController(options(AdaptiveConcurrencyMode.OBSERVE, 24, 8), clock);

        // The option's contract: in OBSERVE the seed seeds the reported (hypothetical) target only.
        assertThat(controller.currentTarget()).isEqualTo(24);
        assertThat(controller.wouldBeTarget()).isEqualTo(8);
    }

    // ------------------------------------------------------------------
    // Clamps
    // ------------------------------------------------------------------

    @Test
    void lawEstimateAboveTheCeilingHoldsAtTheCeiling() {
        // User-set ceiling 4; healthy saturated windows make the gradient want 4.8 - the clamp must bind.
        var controller = controller(AdaptiveConcurrencyMode.ENFORCE, 4);

        // Stay under the law's probe-down cadence: this pins the clamp, not the re-measure probe.
        for (int window = 0; window < 4; window++) {
            feedWindowAndTick(controller, 10 * MS, controller.currentTarget());
            assertWithMessage("window %s escaped the ceiling", window)
                    .that(controller.currentTarget()).isEqualTo(4);
        }
        assertThat(controller.lastDecisionReason()).hasValue(AT_CAP);
    }

    @Test
    void estimateBelowOneSlotClampsToTheFloorWithReasonAtFloor() {
        // Zero queue headroom (test seam) removes the additive growth term so pure contraction can reach the
        // floor - the same setup the law's own floor test uses, driven through the controller.
        var controller = new AdmissionController(options(AdaptiveConcurrencyMode.ENFORCE, 100, 50), clock,
                AdmissionControlLaw.newBuilder().queueHeadroomSlots(0));

        // Establish a fast baseline, then degrade latency 10x until contraction bottoms out.
        for (int window = 0; window < 10; window++) {
            feedWindowAndTick(controller, 10 * MS, controller.currentTarget());
        }
        for (int window = 0; window < 60; window++) {
            feedWindowAndTick(controller, 100 * MS, controller.currentTarget());
            assertWithMessage("window %s went below the one-slot floor", window)
                    .that(controller.currentTarget()).isAtLeast(LIMIT_FLOOR_SLOTS);
        }

        assertThat(controller.currentTarget()).isEqualTo(LIMIT_FLOOR_SLOTS);
        assertThat(controller.lastDecisionReason()).hasValue(AT_FLOOR);
    }

    // ------------------------------------------------------------------
    // OBSERVE: computes and records without publishing
    // ------------------------------------------------------------------

    @Test
    void observeMovesOnlyTheWouldBeTarget() {
        var controller = controller(AdaptiveConcurrencyMode.OBSERVE, DEFAULT_MAX_CONCURRENCY);

        // Healthy saturated windows grow the law's estimate by smoothing * headroom slots per window.
        for (int window = 0; window < 5; window++) {
            feedWindowAndTick(controller, 10 * MS, controller.wouldBeTarget());
            assertWithMessage("window %s published an OBSERVE-mode movement", window)
                    .that(controller.currentTarget()).isEqualTo(DEFAULT_MAX_CONCURRENCY);
        }

        assertThat(controller.wouldBeTarget()).isGreaterThan(DEFAULT_MAX_CONCURRENCY);
        assertThat(controller.lastDecisionReason()).hasValue(ADAPTING);
        // The would-be movement is recorded as a movement - it is the mode's whole product.
        assertThat(controller.lastMovementAt()).isPresent();
    }

    // ------------------------------------------------------------------
    // Tick cadence
    // ------------------------------------------------------------------

    @Test
    void noWindowClosesBeforeTheTimeBoundElapses() {
        var controller = controller(AdaptiveConcurrencyMode.ENFORCE, 24);
        for (int i = 0; i < SAMPLES; i++) {
            controller.recordServiceTime(10 * MS);
            controller.recordInFlight(24);
            controller.recordOutcome(Outcome.SUCCESS);
        }

        clock.add(SAMPLE_WINDOW_DURATION.minusMillis(1));
        controller.tick();
        assertThat(controller.lastDecisionReason()).isEmpty();

        clock.add(Duration.ofMillis(1));
        controller.tick();
        assertThat(controller.lastDecisionReason()).isPresent();
    }

    @Test
    void movementTimestampComesFromTheInjectedClock() {
        var controller = controller(AdaptiveConcurrencyMode.ENFORCE, DEFAULT_MAX_CONCURRENCY);

        feedWindowAndTick(controller, 10 * MS, controller.currentTarget());

        // One healthy window at the default start grows the estimate (16 -> 16.8 -> published 16); movement in
        // whole slots needs a second window - what matters here is WHEN it is stamped, and by which clock.
        feedWindowAndTick(controller, 10 * MS, controller.currentTarget());
        assertThat(controller.lastMovementAt()).hasValue(clock.instant());
    }

    // ------------------------------------------------------------------
    // DISABLED: inert, defensively
    // ------------------------------------------------------------------

    @Test
    void disabledModeIsInert() {
        var controller = controller(AdaptiveConcurrencyMode.DISABLED, 24);

        controller.recordServiceTime(10 * MS);
        controller.recordInFlight(24);
        controller.recordOutcome(Outcome.SUCCESS);
        clock.add(SAMPLE_WINDOW_DURATION.multipliedBy(3));
        controller.tick();

        assertThat(controller.currentTarget()).isEqualTo(24);
        assertThat(controller.wouldBeTarget()).isEqualTo(24);
        assertThat(controller.effectiveMaximum()).isEqualTo(24);
        assertThat(controller.lastDecisionReason()).isEmpty();
        assertThat(controller.lastMovementAt()).isEmpty();
    }

    // ------------------------------------------------------------------
    // PCModule wiring
    // ------------------------------------------------------------------

    @Test
    void moduleConstructsOneControllerLazilyAndAlways() {
        var module = new PCModuleTestEnv();

        var controller = module.admissionController();

        // Always-construct policy: DISABLED still yields a (inert) controller, so downstream reads never null-check.
        assertThat(controller).isNotNull();
        assertThat(controller.mode()).isEqualTo(AdaptiveConcurrencyMode.DISABLED);
        assertThat(controller.currentTarget()).isEqualTo(DEFAULT_MAX_CONCURRENCY);
        assertThat(module.admissionController()).isSameInstanceAs(controller);
    }
}
