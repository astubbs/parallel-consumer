package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.AdaptiveConcurrencyMode;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.internal.admission.AdmissionController.Outcome;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.threeten.extra.MutableClock;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.DEFAULT_MAX_CONCURRENCY;
import static bz.stub.parallelconsumer.internal.admission.AdmissionController.ADAPTIVE_DEFAULT_CEILING;
import static bz.stub.parallelconsumer.internal.admission.AdmissionController.REBALANCE_TARGET_FREEZE_COOLDOWN;
import static bz.stub.parallelconsumer.internal.admission.AdmissionController.SAMPLE_WINDOW_DURATION;
import static bz.stub.parallelconsumer.internal.admission.AdmissionControlLaw.DEFAULT_MIN_SAMPLES_PER_WINDOW;
import static bz.stub.parallelconsumer.internal.admission.AdmissionControlLaw.LIMIT_FLOOR_SLOTS;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.AT_CAP;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.BACKOFF;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.COOLDOWN;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.INSUFFICIENT_SIGNAL;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.WARMUP;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.WARMUP_EXHAUSTED;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The controller's OWN responsibilities - KTD4 ceiling resolution, R4 start value, mode-gated publication, clamps,
 * and the tick cadence. All time comes from an injected {@link MutableClock} (no wall clock anywhere); all law
 * behaviour beyond the clamp boundaries is {@link AdmissionControlLawTest}'s job.
 * <p>
 * MIGRATED with the U5 band-machine rewrite: growth now needs LIMIT-BOUND boundary signals (the binding gate,
 * R5), so the growth-driving helper closes windows through {@link AdmissionController#tick(java.util.function.Supplier)}
 * with {@link TestWindows#boundAt} signals; the no-arg {@link AdmissionController#tick()} closes UNSAMPLED
 * (never-bound) windows and is used where binding must not matter. Every migrated expectation is mapped in
 * {@code docs/test-hardening/admission-law-rewrite-test-migration-2026-08-25.md}.
 */
class AdmissionControllerTest {

    private static final long MS = 1_000_000L; // nanos per millisecond

    /** Comfortably above the law's per-window minimum so every fed window is adjudicated. */
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
     * Feeds one healthy window's worth of samples and ticks past the time bound with LIMIT-BOUND boundary
     * signals at {@code boundSlots} - the shape the band machine may grow on (binding gate, R5).
     */
    private void feedBoundWindowAndTick(AdmissionController controller, long meanServiceTimeNanos, int boundSlots) {
        feedSamples(controller, meanServiceTimeNanos, boundSlots);
        clock.add(SAMPLE_WINDOW_DURATION);
        controller.tick(() -> TestWindows.boundAt(boundSlots));
    }

    /** The feeding half alone - for scenarios where the samples' fate (discarded vs acted-on) IS the assertion. */
    private void feedSamples(AdmissionController controller, long meanServiceTimeNanos, int inFlightMedian) {
        for (int i = 0; i < SAMPLES; i++) {
            controller.recordServiceTime(meanServiceTimeNanos);
            controller.recordInFlight(inFlightMedian);
            controller.recordOutcome(Outcome.SUCCESS);
        }
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
    void warmupGrowthAboveTheCeilingHoldsAtTheCeilingWithReasonAtCap() {
        // User-set ceiling 4, started AT it; bound windows make the warmup band want 4 + sqrt(4) = 6 - the
        // clamp must bind, and keep binding until the warmup allowance is spent.
        var controller = controller(AdaptiveConcurrencyMode.ENFORCE, 4);

        feedBoundWindowAndTick(controller, 10 * MS, controller.currentTarget());
        assertThat(controller.lastDecisionReason()).hasValue(AT_CAP);

        for (int window = 0; window < 3; window++) {
            feedBoundWindowAndTick(controller, 10 * MS, controller.currentTarget());
            assertWithMessage("window %s escaped the ceiling", window)
                    .that(controller.currentTarget()).isEqualTo(4);
        }
        // Allowance (4 slots) spent in two clipped sqrt(4)=2 grants; blind growth then degrades to preserve.
        assertThat(controller.lastDecisionReason()).hasValue(WARMUP_EXHAUSTED);
    }

    @Test
    void overloadBackoffClampsAtTheOneSlotFloor() {
        // The floor clamp driven through the controller: repeated overload-drop windows cut 0.9x per window
        // (the BACKOFF brake fires before the binding gate, so UNSAMPLED windows suffice) and must bottom out
        // at the one-slot floor, never below. The AT_FLOOR reason for a FALL-band contraction clipping at the
        // floor is pinned at law level (AdmissionControlLawTest).
        var controller = new AdmissionController(options(AdaptiveConcurrencyMode.ENFORCE, 100, 50), clock);

        for (int window = 0; window < 60; window++) {
            feedSamples(controller, 10 * MS, controller.currentTarget());
            controller.recordOutcome(Outcome.OVERLOAD_DROP);
            clock.add(SAMPLE_WINDOW_DURATION);
            controller.tick();
            assertWithMessage("window %s went below the one-slot floor", window)
                    .that(controller.currentTarget()).isAtLeast(LIMIT_FLOOR_SLOTS);
        }

        assertThat(controller.currentTarget()).isEqualTo(LIMIT_FLOOR_SLOTS);
        assertThat(controller.lastDecisionReason()).hasValue(BACKOFF);
    }

    // ------------------------------------------------------------------
    // OBSERVE: computes and records without publishing
    // ------------------------------------------------------------------

    @Test
    void observeMovesOnlyTheWouldBeTarget() {
        var controller = controller(AdaptiveConcurrencyMode.OBSERVE, DEFAULT_MAX_CONCURRENCY);

        // Bound windows: the warmup band grows the would-be target by sqrt(16) = 4 - the whole allowance in
        // one grant - then preserves.
        for (int window = 0; window < 5; window++) {
            feedBoundWindowAndTick(controller, 10 * MS, controller.wouldBeTarget());
            assertWithMessage("window %s published an OBSERVE-mode movement", window)
                    .that(controller.currentTarget()).isEqualTo(DEFAULT_MAX_CONCURRENCY);
        }

        assertThat(controller.wouldBeTarget()).isEqualTo(DEFAULT_MAX_CONCURRENCY + 4);
        assertThat(controller.lastDecisionReason()).hasValue(WARMUP_EXHAUSTED);
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

        // One bound healthy window moves the target (the warmup grant 16 -> 20); what matters here is WHEN the
        // movement is stamped, and by which clock.
        feedBoundWindowAndTick(controller, 10 * MS, controller.currentTarget());

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

    // ------------------------------------------------------------------
    // Lifecycle (the plan's U7/R13): the rebalance delta gate, the cooldown, and the window discard.
    // Callbacks are driven here directly the way the engine drives them - revoke/assign pairs on one thread -
    // asserting converged decisions, never tick paths.
    // ------------------------------------------------------------------

    static final TopicPartition TP_0 = new TopicPartition("lifecycle-topic", 0);
    static final TopicPartition TP_1 = new TopicPartition("lifecycle-topic", 1);

    /** ENFORCE under a user ceiling of 24 with a contracted seed of 8 - room to move in both directions. */
    private AdmissionController seededEnforceController() {
        return new AdmissionController(options(AdaptiveConcurrencyMode.ENFORCE, 24, 8), clock);
    }

    /**
     * Warms the law: bound healthy windows, so there is real history (elasticity entries, spent warmup
     * allowance) to protect or discard. Three windows spend the whole allowance: 8 -> 10.83 -> 12, then
     * preserve.
     */
    private void warm(AdmissionController controller) {
        controller.onPartitionsAssigned(UniLists.of(TP_0));
        for (int window = 0; window < 3; window++) {
            feedBoundWindowAndTick(controller, 10 * MS, controller.currentTarget());
        }
        assertWithMessage("fixture: warming must have given the law's estimator history to protect")
                .that(controller.law().estimatorHistorySize()).isGreaterThan(0);
    }

    /**
     * The startup arm of the delta gate: the FIRST assignment establishes the baseline without a cooldown - there
     * is no history to protect yet, and freezing at t=0 would only delay warmup.
     */
    @Test
    void firstAssignmentEstablishesTheBaselineWithoutACooldown() {
        var controller = seededEnforceController();

        controller.onPartitionsAssigned(UniLists.of(TP_0));
        controller.tick();

        assertThat(controller.lastDecisionReason()).isEmpty();
        feedBoundWindowAndTick(controller, 10 * MS, controller.currentTarget());
        assertWithMessage("the warmup band must act on the very first window after the initial assignment")
                .that(controller.lastDecisionReason()).hasValue(WARMUP);
    }

    /**
     * A REAL assignment delta (a partition swapped): the in-progress window and the law's whole history are
     * discarded - the law is reconstructed, its owned estimator empty - while the target itself carries over as
     * the new law's prior (R13), frozen under reason COOLDOWN. The reset lands on the first tick after the
     * cycle, before any window boundary.
     */
    @Test
    void aRealAssignmentDeltaDiscardsHistoryAndFreezesTheTarget() {
        var controller = seededEnforceController();
        warm(controller);
        var lawBefore = controller.law();
        int targetBefore = controller.currentTarget();
        feedSamples(controller, 10 * MS, targetBefore); // mid-window samples that must die with the old assignment

        controller.onPartitionsRevoked(UniLists.of(TP_0));
        controller.onPartitionsAssigned(UniLists.of(TP_1));
        controller.tick();

        assertThat(controller.lastDecisionReason()).hasValue(COOLDOWN);
        assertWithMessage("the target carries over as the best available prior - a rebalance is not a verdict")
                .that(controller.currentTarget()).isEqualTo(targetBefore);
        assertWithMessage("the law must be reconstructed, not retained")
                .that(controller.law()).isNotSameInstanceAs(lawBefore);
        assertWithMessage("a reconstructed law owns a fresh estimator - the old assignment's history is gone")
                .that(controller.law().estimatorHistorySize()).isEqualTo(0);
        assertWithMessage("the carried-over target seeds the new law")
                .that(controller.law().getEstimatedLimit()).isEqualTo(targetBefore);
    }

    /**
     * The freeze holds for the whole cooldown on the injected clock - growth-inducing windows landing inside it
     * are discarded, reason COOLDOWN - and adaptation (a fresh law's warmup band) resumes once it lapses.
     */
    @Test
    void theCooldownFreezesTheTargetUntilItLapsesThenAdaptationResumes() {
        var controller = seededEnforceController();
        warm(controller);
        controller.onPartitionsRevoked(UniLists.of(TP_0));
        controller.onPartitionsAssigned(UniLists.of(TP_1));
        controller.tick();
        int frozen = controller.currentTarget();

        // Every window that CLOSES before the cooldown's end is discarded, however healthy and bound.
        long windowsInsideCooldown = REBALANCE_TARGET_FREEZE_COOLDOWN.toMillis() / SAMPLE_WINDOW_DURATION.toMillis() - 1;
        for (long window = 0; window < windowsInsideCooldown; window++) {
            feedBoundWindowAndTick(controller, 10 * MS, frozen);
            assertWithMessage("window %s inside the cooldown moved the frozen target", window)
                    .that(controller.currentTarget()).isEqualTo(frozen);
            assertThat(controller.lastDecisionReason()).hasValue(COOLDOWN);
        }

        // The cooldown has lapsed on the injected clock: the same bound windows now grow the target again -
        // the reconstructed law's fresh warmup episode.
        feedBoundWindowAndTick(controller, 10 * MS, controller.currentTarget());
        assertThat(controller.lastDecisionReason()).hasValue(WARMUP);
        assertWithMessage("adaptation must resume after the cooldown lapses")
                .that(controller.currentTarget()).isGreaterThan(frozen);
    }

    /**
     * THE DELTA GATE: an eager-protocol rebalance revokes everything and hands the SAME set back - and a
     * cooperative no-op fires empty callbacks. Neither moved anything for this instance, so the law, its
     * estimator history and the in-progress window's samples must all survive; group churn must not starve the
     * controller of history. (Sabotage-proofed: with the equals-gate removed from checkAssignmentDelta, this
     * fails on COOLDOWN-instead-of-WARMUP_EXHAUSTED.)
     */
    @Test
    void aNoDeltaRebalanceLeavesTheLawAndWindowIntact() {
        var controller = seededEnforceController();
        warm(controller);
        var lawBefore = controller.law();
        int historyBefore = lawBefore.estimatorHistorySize();
        feedSamples(controller, 10 * MS, controller.currentTarget()); // mid-window samples that must SURVIVE

        // eager identical re-assignment
        controller.onPartitionsRevoked(UniLists.of(TP_0));
        controller.onPartitionsAssigned(UniLists.of(TP_0));
        // cooperative no-op cycle
        controller.onPartitionsRevoked(UniLists.of());
        controller.onPartitionsAssigned(UniLists.of());

        clock.add(SAMPLE_WINDOW_DURATION);
        controller.tick(() -> TestWindows.boundAt(controller.currentTarget()));

        assertWithMessage("the surviving window's samples must be adjudicated - not discarded, not COOLDOWN "
                + "(the warmed law's allowance is spent, so a bound window preserves under WARMUP_EXHAUSTED)")
                .that(controller.lastDecisionReason()).hasValue(WARMUP_EXHAUSTED);
        assertWithMessage("no delta, no reconstruction")
                .that(controller.law()).isSameInstanceAs(lawBefore);
        assertWithMessage("...and the surviving window was offered to the SAME estimator history")
                .that(controller.law().estimatorHistorySize()).isEqualTo(historyBefore + 1);
    }

    /**
     * Partitions LOST (fenced) end a cycle with no assignment half - the delta gate must compare there too.
     */
    @Test
    void aLossIsACycleEndAndARealDelta() {
        var controller = seededEnforceController();
        controller.onPartitionsAssigned(UniLists.of(TP_0, TP_1));
        feedBoundWindowAndTick(controller, 10 * MS, controller.currentTarget());

        controller.onPartitionsLost(UniLists.of(TP_1));
        controller.tick();

        assertThat(controller.lastDecisionReason()).hasValue(COOLDOWN);
    }

    /**
     * {@link AdmissionController#discardWindow()} - the engine's pause-poison lever: the in-progress window's
     * samples never reach the law, but the law itself (and its elasticity history) survives, because a pause
     * says nothing about the downstream.
     */
    @Test
    void discardWindowDropsTheSamplesButKeepsTheLaw() {
        var controller = seededEnforceController();
        warm(controller);
        var lawBefore = controller.law();
        int historyBefore = lawBefore.estimatorHistorySize();
        feedSamples(controller, 10 * MS, controller.currentTarget());

        controller.discardWindow();
        clock.add(SAMPLE_WINDOW_DURATION);
        controller.tick();

        assertWithMessage("the first window after the discard must close EMPTY - a hold, never a decision on "
                + "discarded samples")
                .that(controller.lastDecisionReason()).hasValue(INSUFFICIENT_SIGNAL);
        assertThat(controller.law()).isSameInstanceAs(lawBefore);
        assertWithMessage("an empty window leaves the estimator untouched (the adjudication gate holds all state)")
                .that(controller.law().estimatorHistorySize()).isEqualTo(historyBefore);
    }

    /**
     * DISABLED stays inert through every lifecycle input - no NPE on the null window/law, no state movement.
     */
    @Test
    void disabledIgnoresLifecycleInputs() {
        var controller = controller(AdaptiveConcurrencyMode.DISABLED, 24);

        controller.onPartitionsAssigned(UniLists.of(TP_0));
        controller.onPartitionsRevoked(UniLists.of(TP_0));
        controller.onPartitionsAssigned(UniLists.of(TP_1));
        controller.onPartitionsLost(UniLists.of(TP_1));
        controller.discardWindow();
        clock.add(SAMPLE_WINDOW_DURATION.multipliedBy(2));
        controller.tick();

        assertThat(controller.currentTarget()).isEqualTo(24);
        assertThat(controller.lastDecisionReason()).isEmpty();
    }
}
