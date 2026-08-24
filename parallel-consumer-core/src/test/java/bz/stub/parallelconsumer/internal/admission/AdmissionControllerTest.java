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
import java.util.HashSet;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.DEFAULT_MAX_CONCURRENCY;
import static bz.stub.parallelconsumer.internal.admission.AdmissionController.ADAPTIVE_DEFAULT_CEILING;
import static bz.stub.parallelconsumer.internal.admission.AdmissionController.REBALANCE_TARGET_FREEZE_COOLDOWN;
import static bz.stub.parallelconsumer.internal.admission.AdmissionController.SAMPLE_WINDOW_DURATION;
import static bz.stub.parallelconsumer.internal.admission.AdmissionControlLaw.DEFAULT_MIN_SAMPLES_PER_WINDOW;
import static bz.stub.parallelconsumer.internal.admission.AdmissionControlLaw.LIMIT_FLOOR_SLOTS;
import static bz.stub.parallelconsumer.internal.admission.AdmissionController.FLOOR_ESCAPE_WINDOWS;
import static bz.stub.parallelconsumer.internal.admission.AdmissionController.PROBE_DURATION_WINDOWS;
import static bz.stub.parallelconsumer.internal.admission.AdmissionControlLaw.DEFAULT_WARMUP_ALLOWANCE_SLOTS;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.AT_CAP;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.BACKOFF;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.COOLDOWN;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.DESCENT_PROBE;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.ESCAPE_PROBE;
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
     * {@link AdmissionController#notifyPauseResumed()} - the engine's pause-poison lever, U6 semantics (KTD3):
     * the in-progress window's samples never reach the law, AND the pause stamps an invalidation boundary
     * through the law's estimator - pre-pause entries describe a plant an unknown span in the past, so history
     * and verdict die while the law INSTANCE (and, per KTD2, its warmup episode) survives. This deliberately
     * supersedes the U5 lever's keep-the-history contract, which its own javadoc carried as "until U6 refines
     * it".
     */
    @Test
    void pauseResumedDropsTheSamplesAndStampsAnInvalidationBoundary() {
        var controller = seededEnforceController();
        warm(controller);
        var lawBefore = controller.law();
        double allowanceBefore = lawBefore.warmupAllowanceRemaining();
        assertWithMessage("fixture: warming must have left estimator history for the boundary to kill")
                .that(lawBefore.estimatorHistorySize()).isGreaterThan(0);
        feedSamples(controller, 10 * MS, controller.currentTarget());

        controller.notifyPauseResumed();
        clock.add(SAMPLE_WINDOW_DURATION);
        controller.tick();

        assertWithMessage("the first window after the discard must close EMPTY - a hold, never a decision on "
                + "discarded samples")
                .that(controller.lastDecisionReason()).hasValue(INSUFFICIENT_SIGNAL);
        assertWithMessage("the law survives a pause - only its estimator's history dies")
                .that(controller.law()).isSameInstanceAs(lawBefore);
        assertWithMessage("KTD3: the pause boundary kills every pre-pause estimator entry")
                .that(controller.law().estimatorHistorySize()).isEqualTo(0);
        assertWithMessage("KTD2: the warmup EPISODE spans the boundary - pause must not refill the allowance")
                .that(controller.law().warmupAllowanceRemaining()).isEqualTo(allowanceBefore);
    }

    // ------------------------------------------------------------------
    // U6: the escape hatch, the descent probe, and the lifecycle edges (R6, KTD2-KTD4).
    // ------------------------------------------------------------------

    /** A fixed jitter seed so probe timing is deterministic; its value is arbitrary. */
    private static final long JITTER_SEED = 42L;

    /** The largest start delay the jitter can add - one window at N=5 with the 15% fraction. */
    private static final int MAX_JITTER_WINDOWS =
            (int) Math.round(FLOOR_ESCAPE_WINDOWS * AdmissionController.ESCAPE_JITTER_FRACTION);

    private AdmissionController seededController(int maxConcurrency, int seedSlots) {
        return new AdmissionController(options(AdaptiveConcurrencyMode.ENFORCE, maxConcurrency, seedSlots), clock,
                AdmissionControlLaw.newBuilder(), null, JITTER_SEED);
    }

    /** Feeds {@code samples} successes and ticks across the boundary with LIMIT-BOUND signals at {@code slots}. */
    private void feedCountedBoundWindowAndTick(AdmissionController controller, int samples, int slots) {
        for (int i = 0; i < samples; i++) {
            controller.recordServiceTime(10 * MS);
            controller.recordInFlight(slots);
            controller.recordOutcome(Outcome.SUCCESS);
        }
        clock.add(SAMPLE_WINDOW_DURATION);
        controller.tick(() -> TestWindows.boundAt(slots));
    }

    /**
     * Closes empty floor windows (every gated signal reading empty) until the escape fires, asserting it fires
     * inside the N..N+jitter arming band, and returns the window index it fired at.
     */
    private int driveFloorWindowsUntilEscapeFires(AdmissionController controller,
                                                  Runnable oneFloorWindow) {
        for (int window = 1; window <= FLOOR_ESCAPE_WINDOWS + MAX_JITTER_WINDOWS; window++) {
            oneFloorWindow.run();
            if (controller.lastDecisionReason().orElse(null) == ESCAPE_PROBE) {
                assertWithMessage("the escape fired before N consecutive floor windows elapsed")
                        .that(window).isAtLeast(FLOOR_ESCAPE_WINDOWS);
                return window;
            }
        }
        throw new AssertionError("the escape never fired within N + jitter floor windows");
    }

    /**
     * The ungated arming path (R6): empty UNSAMPLED windows at the floor - zero samples (the adjudication gate
     * reads nothing), never limit-bound (the binding gate reads nothing) - must still arm and fire the escape
     * within N + jitter windows. Sabotage signature: make the counter respect either gated signal and this
     * (and the floor-pin falsifier) times out at the floor forever.
     */
    @Test
    void escapeFiresAfterConsecutiveFloorWindowsDespiteEmptyGatedSignals() {
        var controller = seededController(24, 1);

        driveFloorWindowsUntilEscapeFires(controller, () -> {
            clock.add(SAMPLE_WINDOW_DURATION);
            controller.tick();
        });

        assertThat(controller.probeInFlight()).isTrue();
        assertWithMessage("the deferred restore value at the floor IS the floor")
                .that(controller.probeDeferredRestoreTarget()).isEqualTo(LIMIT_FLOOR_SLOTS);
        assertWithMessage("the probe pins the published target to the floor")
                .that(controller.currentTarget()).isEqualTo(LIMIT_FLOOR_SLOTS);
    }

    /**
     * A genuinely idle floor (probe windows never limit-bound) concludes by restoring the floor unchanged with
     * a FRESH warmup allowance - idleness must not fund growth, and the fresh allowance is what keeps the
     * escape's liveness alive under the KTD2 cap once bound work returns.
     */
    @Test
    void idleEscapeProbeConcludesByRestoringTheFloorUnchanged() {
        var controller = seededController(24, 1);
        driveFloorWindowsUntilEscapeFires(controller, () -> {
            clock.add(SAMPLE_WINDOW_DURATION);
            controller.tick();
        });

        for (int probeWindow = 0; probeWindow < PROBE_DURATION_WINDOWS; probeWindow++) {
            assertWithMessage("probe window %s must still be in flight", probeWindow)
                    .that(controller.probeInFlight()).isTrue();
            assertThat(controller.lastDecisionReason()).hasValue(ESCAPE_PROBE);
            clock.add(SAMPLE_WINDOW_DURATION);
            controller.tick();
        }

        assertThat(controller.probeInFlight()).isFalse();
        assertWithMessage("un-bound probe windows restore the floor - no step on idleness")
                .that(controller.currentTarget()).isEqualTo(LIMIT_FLOOR_SLOTS);
        assertWithMessage("a concluded probe opens a fresh allowance (KTD2)")
                .that(controller.law().warmupAllowanceRemaining()).isEqualTo(DEFAULT_WARMUP_ALLOWANCE_SLOTS);
    }

    /**
     * The escape's liveness half: probe windows that stay LIMIT-BOUND (even a trickle saturates the floor's one
     * slot) conclude with ONE accelerator re-entry step - provisional growth the next verdict adjudicates like
     * any warmup grant. Sample-starved throughout, so no gated band could ever have moved the target.
     */
    @Test
    void boundEscapeProbeConcludesWithOneReEntryStep() {
        var controller = seededController(24, 1);
        Runnable boundStarvedWindow = () -> {
            clock.add(SAMPLE_WINDOW_DURATION);
            controller.tick(() -> TestWindows.boundAt(1));
        };
        driveFloorWindowsUntilEscapeFires(controller, boundStarvedWindow);

        for (int probeWindow = 0; probeWindow < PROBE_DURATION_WINDOWS; probeWindow++) {
            boundStarvedWindow.run();
        }

        assertThat(controller.probeInFlight()).isFalse();
        assertWithMessage("floor + one accelerator step (sqrt(1) = 1)")
                .that(controller.currentTarget()).isEqualTo(2);
        assertThat(controller.law().getEstimatedLimit()).isEqualTo(2.0);
    }

    /**
     * Jitter determinism under an injected seed: two controllers with the same seed fire at the identical
     * window index - the property the fleet-level jitter must NOT have across instances, provided here for
     * tests by the seeded constructor.
     */
    @Test
    void escapeJitterIsDeterministicUnderAnInjectedSeed() {
        var first = seededController(24, 1);
        int firstFiredAt = driveFloorWindowsUntilEscapeFires(first, () -> {
            clock.add(SAMPLE_WINDOW_DURATION);
            first.tick();
        });

        var second = seededController(24, 1);
        int secondFiredAt = driveFloorWindowsUntilEscapeFires(second, () -> {
            clock.add(SAMPLE_WINDOW_DURATION);
            second.tick();
        });

        assertThat(secondFiredAt).isEqualTo(firstFiredAt);
    }

    /**
     * KTD3: cooldown-discarded windows advance neither the floor counter nor a probe - after the cooldown
     * lapses the escape needs its full N floor windows again, however many floor windows the cooldown ate.
     */
    @Test
    void cooldownWindowsDoNotAdvanceTheEscapeCounter() {
        var controller = seededController(24, 1);
        controller.onPartitionsAssigned(UniLists.of(TP_0));
        controller.onPartitionsRevoked(UniLists.of(TP_0));
        controller.onPartitionsAssigned(UniLists.of(TP_1));
        controller.tick();
        assertThat(controller.lastDecisionReason()).hasValue(COOLDOWN);

        // A cooldown's worth of floor windows: discarded, and the counter must not move.
        long windowsInsideCooldown =
                REBALANCE_TARGET_FREEZE_COOLDOWN.toMillis() / SAMPLE_WINDOW_DURATION.toMillis() - 1;
        for (long window = 0; window < windowsInsideCooldown; window++) {
            clock.add(SAMPLE_WINDOW_DURATION);
            controller.tick();
            assertThat(controller.lastDecisionReason()).hasValue(COOLDOWN);
            assertThat(controller.probeInFlight()).isFalse();
        }

        // Post-cooldown the arming starts from zero: the helper asserts the full N windows are needed again -
        // had the discarded windows counted, the first evaluated window would have fired below N.
        driveFloorWindowsUntilEscapeFires(controller, () -> {
            clock.add(SAMPLE_WINDOW_DURATION);
            controller.tick();
        });
    }

    /**
     * A floor held by an ABSOLUTE BRAKE is braked, not stranded: the brake is a live verdict that only fires
     * when there IS signal, so braked windows must not arm the escape - an overloaded floor keeps cutting under
     * {@code BACKOFF} instead of oscillating through pointless re-measurements.
     */
    @Test
    void brakedFloorWindowsDoNotArmTheEscape() {
        var controller = seededController(24, 1);

        for (int window = 0; window < FLOOR_ESCAPE_WINDOWS + MAX_JITTER_WINDOWS + 2; window++) {
            feedSamples(controller, 10 * MS, 1);
            controller.recordOutcome(Outcome.OVERLOAD_DROP);
            clock.add(SAMPLE_WINDOW_DURATION);
            controller.tick();
            assertThat(controller.lastDecisionReason()).hasValue(BACKOFF);
            assertThat(controller.probeInFlight()).isFalse();
        }
    }

    // ------------------------------------------------------------------
    // U6: the descent probe (R14's sweep-from-above) and the probe lifecycle edges.
    // ------------------------------------------------------------------

    /**
     * Drives a fresh ENFORCE controller (ceiling 100, seed 16) to its first descent probe: the warmup grant
     * 16 -&gt; 20, the first verdict's retraction back to 16, then three plateau-held windows arm the probe,
     * which pins to 16 - accelerator step = 12. Asserts the fixture landed where documented.
     */
    private AdmissionController driveToDescentProbe(int partitionCount) {
        var controller = seededController(100, 16);
        if (partitionCount > 0) {
            var partitions = new HashSet<TopicPartition>();
            for (int p = 0; p < partitionCount; p++) {
                partitions.add(new TopicPartition("lifecycle-topic", p));
            }
            controller.onPartitionsAssigned(partitions);
        }
        for (int window = 0; window < 30; window++) {
            feedBoundWindowAndTick(controller, 10 * MS, controller.currentTarget());
            if (controller.lastDecisionReason().orElse(null) == DESCENT_PROBE) {
                assertWithMessage("fixture: the probe pins one accelerator step down")
                        .that(controller.currentTarget()).isEqualTo(12);
                assertWithMessage("fixture: the deferred restore value is the plateau level")
                        .that(controller.probeDeferredRestoreTarget()).isEqualTo(16);
                return controller;
            }
        }
        throw new AssertionError("fixture: no descent probe fired within 30 plateau-shaped windows");
    }

    /**
     * Descent KEEP: probe windows matching the reference throughput prove the lower target PAID - it is kept,
     * and the walk repeats at the same cadence (the next probe steps down again after another plateau streak).
     */
    @Test
    void descentProbeKeepsTheLowerTargetWhenThroughputHolds() {
        var controller = driveToDescentProbe(0);

        for (int probeWindow = 0; probeWindow < PROBE_DURATION_WINDOWS; probeWindow++) {
            feedBoundWindowAndTick(controller, 10 * MS, controller.currentTarget()); // same 12/s throughput
        }

        assertThat(controller.probeInFlight()).isFalse();
        assertWithMessage("unchanged throughput at the lower target: the step down paid, keep it")
                .that(controller.currentTarget()).isEqualTo(12);

        // The walk repeats: within the un-backed-off cadence another probe steps down again.
        for (int window = 0; window < AdmissionController.DESCENT_PLATEAU_WINDOWS; window++) {
            feedBoundWindowAndTick(controller, 10 * MS, controller.currentTarget());
        }
        assertWithMessage("a kept probe leaves the cadence alone - the walk continues toward the knee")
                .that(controller.lastDecisionReason()).hasValue(DESCENT_PROBE);
        assertThat(controller.currentTarget()).isEqualTo(9); // 12 - sqrt(12), rounded
    }

    /**
     * Descent RESTORE: probe windows whose throughput FELL beyond the tolerance restore the deferred target and
     * back the cadence off (doubled) - the plant answered, so re-asking gets rarer. Throughput criterion only.
     */
    @Test
    void descentProbeRestoresAndBacksOffWhenThroughputFalls() {
        var controller = driveToDescentProbe(0);

        for (int probeWindow = 0; probeWindow < PROBE_DURATION_WINDOWS; probeWindow++) {
            feedCountedBoundWindowAndTick(controller, 10, 12); // 10/s against the 12/s reference: a 17% fall
        }

        assertThat(controller.probeInFlight()).isFalse();
        assertWithMessage("fallen throughput restores the remembered target")
                .that(controller.currentTarget()).isEqualTo(16);
        assertThat(controller.law().getEstimatedLimit()).isEqualTo(16.0);

        // The cadence doubled: the old cadence's worth of plateau windows must NOT re-fire...
        for (int window = 0; window < AdmissionController.DESCENT_PLATEAU_WINDOWS; window++) {
            feedBoundWindowAndTick(controller, 10 * MS, controller.currentTarget());
            assertWithMessage("window %s re-fired inside the backed-off cadence", window)
                    .that(controller.lastDecisionReason().orElse(null)).isNotEqualTo(DESCENT_PROBE);
        }
        // ...while the doubled cadence does.
        for (int window = 0; window < AdmissionController.DESCENT_PLATEAU_WINDOWS; window++) {
            feedBoundWindowAndTick(controller, 10 * MS, controller.currentTarget());
        }
        assertThat(controller.lastDecisionReason()).hasValue(DESCENT_PROBE);
    }

    /**
     * The pause edge (KTD3): a pause mid-probe aborts it - deferred target restored immediately, the law
     * instance retained, the history boundary stamped - and the first bound post-resume window lands in the
     * warmup band (never an absorbing hold), on the allowance the pause deliberately did NOT refill (KTD2).
     */
    @Test
    void pauseMidProbeAbortsRestoresAndStampsTheBoundary() {
        var controller = driveToDescentProbe(0);
        var lawBefore = controller.law();

        controller.notifyPauseResumed();

        assertThat(controller.probeInFlight()).isFalse();
        assertWithMessage("the deferred value is restored immediately")
                .that(controller.currentTarget()).isEqualTo(16);
        assertThat(controller.law()).isSameInstanceAs(lawBefore);
        assertWithMessage("KTD3: the pause kills the estimator history")
                .that(controller.law().estimatorHistorySize()).isEqualTo(0);

        feedBoundWindowAndTick(controller, 10 * MS, controller.currentTarget());
        assertWithMessage("resume with binding work lands in the warmup band - never an absorbing hold")
                .that(controller.lastDecisionReason()).hasValue(WARMUP);
        assertThat(controller.currentTarget()).isEqualTo(20);
    }

    /**
     * The rebalance edge (KTD4): a rebalance mid-probe seeds the reconstructed law from the DEFERRED restore
     * value, never the pinned probe value - otherwise the reset launders the probe's reduced target into the
     * frozen post-rebalance prior and group churn ratchets the target down.
     */
    @Test
    void rebalanceMidProbeSeedsFromTheDeferredValueNotTheProbePin() {
        var controller = driveToDescentProbe(1);
        var lawBefore = controller.law();

        controller.onPartitionsRevoked(UniLists.of(TP_0));
        controller.onPartitionsAssigned(UniLists.of(TP_1));
        controller.tick();

        assertThat(controller.lastDecisionReason()).hasValue(COOLDOWN);
        assertThat(controller.probeInFlight()).isFalse();
        assertWithMessage("KTD4: the seed is the deferred restore value, not the probe's pinned 12")
                .that(controller.currentTarget()).isEqualTo(16);
        assertThat(controller.law()).isNotSameInstanceAs(lawBefore);
        assertThat(controller.law().getEstimatedLimit()).isEqualTo(16.0);
    }

    /**
     * KTD4's shrink scaling, mid-probe: the assignment halves while a descent probe is pinned at 12 with a
     * deferred 16 - the seed is the DEFERRED value scaled by the partition ratio, floor-clamped.
     */
    @Test
    void shrinkMidProbeScalesTheDeferredValueByThePartitionRatio() {
        var controller = driveToDescentProbe(2);

        controller.onPartitionsLost(UniLists.of(new TopicPartition("lifecycle-topic", 1)));
        controller.tick();

        assertThat(controller.lastDecisionReason()).hasValue(COOLDOWN);
        assertWithMessage("deferred 16 scaled by the 1/2 partition ratio")
                .that(controller.currentTarget()).isEqualTo(8);
        assertThat(controller.law().getEstimatedLimit()).isEqualTo(8.0);
    }

    /**
     * Shrink scaling without a probe: the carried-over target itself scales down by the partition ratio - and
     * the protection is one-directional, so a GROWN assignment leaves the seed alone (growth is re-earned).
     */
    @Test
    void assignmentShrinkScalesTheCarriedOverSeedGrowthDoesNot() {
        var shrunk = new AdmissionController(options(AdaptiveConcurrencyMode.ENFORCE, 100, 12), clock,
                AdmissionControlLaw.newBuilder(), null, JITTER_SEED);
        shrunk.onPartitionsAssigned(UniLists.of(TP_0, TP_1));
        shrunk.onPartitionsLost(UniLists.of(TP_1));
        shrunk.tick();
        assertWithMessage("half the partitions, half the seed")
                .that(shrunk.currentTarget()).isEqualTo(6);

        var grown = new AdmissionController(options(AdaptiveConcurrencyMode.ENFORCE, 100, 12), clock,
                AdmissionControlLaw.newBuilder(), null, JITTER_SEED);
        grown.onPartitionsAssigned(UniLists.of(TP_0));
        grown.onPartitionsAssigned(UniLists.of(TP_1));
        grown.tick();
        assertWithMessage("a grown assignment must not inflate the seed - one-directional protection only")
                .that(grown.currentTarget()).isEqualTo(12);
    }

    /**
     * ESCAPE safeguard 2, observable end to end: the probe's conclusion measures from a CLEARED history only -
     * pre-probe entries die at probe entry, and post-conclusion the history holds exactly the probe's own
     * qualifying windows.
     */
    @Test
    void escapeProbeConcludesMeasuringFromAClearedHistoryOnly() {
        // Allowance zero: sample-rich bound floor windows adjudicate and feed the estimator, but cannot grow -
        // the WARMUP_EXHAUSTED floor pin whose escape cadence is KTD2's named steady state.
        var controller = new AdmissionController(options(AdaptiveConcurrencyMode.ENFORCE, 24, 1), clock,
                AdmissionControlLaw.newBuilder().warmupAllowanceSlots(0), null, JITTER_SEED);
        driveFloorWindowsUntilEscapeFires(controller,
                () -> feedCountedBoundWindowAndTick(controller, SAMPLES, 1));
        assertWithMessage("probe entry clears the pre-probe history (ESCAPE safeguard 2)")
                .that(controller.law().estimatorHistorySize()).isEqualTo(0);

        for (int probeWindow = 0; probeWindow < PROBE_DURATION_WINDOWS; probeWindow++) {
            feedCountedBoundWindowAndTick(controller, SAMPLES, 1);
        }

        assertWithMessage("the concluded probe's history is its own windows and nothing else")
                .that(controller.law().estimatorHistorySize()).isEqualTo(PROBE_DURATION_WINDOWS);
        assertWithMessage("bound throughout: the re-entry step fires")
                .that(controller.currentTarget()).isEqualTo(2);
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
        controller.notifyPauseResumed();
        clock.add(SAMPLE_WINDOW_DURATION.multipliedBy(2));
        controller.tick();

        assertThat(controller.currentTarget()).isEqualTo(24);
        assertThat(controller.lastDecisionReason()).isEmpty();
    }
}
