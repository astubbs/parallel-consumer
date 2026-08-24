package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static bz.stub.parallelconsumer.internal.admission.AdmissionControlLaw.AIMD_BACKOFF_RATIO;
import static bz.stub.parallelconsumer.internal.admission.AdmissionControlLaw.DEFAULT_WARMUP_ALLOWANCE_SLOTS;
import static bz.stub.parallelconsumer.internal.admission.AdmissionControlLaw.LIMIT_FLOOR_SLOTS;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.ADAPTING;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.AT_CAP;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.AT_FLOOR;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.BACKOFF;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.FAILURE_LIMITED;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.INSUFFICIENT_SIGNAL;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.NO_WORK;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.OFFSET_BACK_PRESSURE;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.ORDERING_STARVED;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.PLATEAU;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.SELF_THROTTLED;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.WARMUP;
import static bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason.WARMUP_EXHAUSTED;
import static bz.stub.parallelconsumer.internal.admission.TestWindows.bound;
import static bz.stub.parallelconsumer.internal.admission.TestWindows.boundWithOffsetBackPressure;
import static bz.stub.parallelconsumer.internal.admission.TestWindows.unboundNoWork;
import static bz.stub.parallelconsumer.internal.admission.TestWindows.unboundOrderingStarved;
import static bz.stub.parallelconsumer.internal.admission.TestWindows.unboundSelfThrottled;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Deterministic exact-sequence tests of the band machine's gates (the 2026-08-24-003 design's R1/R3/R5/R7/R8,
 * KTD2/KTD6): every window is an explicit call, no clock, no sleeps. This file was REWRITTEN with the U5 law
 * rewrite (KTD8): the deleted Gradient2-port cases are each named, with their successor, in
 * {@code docs/test-hardening/admission-law-rewrite-test-migration-2026-08-25.md}.
 * <p>
 * Convention throughout: windows close at the nominal one second, so a {@link TestWindows#bound} window's
 * success count IS its throughput per second, and crafted (x = active slots, y = throughput) series read
 * directly as the elasticity evidence the estimator regresses over. Closed-loop trajectory behaviour - where
 * the law's own target feeds back into the windows - is {@link AdmissionLawFalsifierTest}'s job against the
 * deterministic plant; here each gate is pinned in isolation.
 */
class AdmissionControlLawTest {

    private static final double EXACT = 1e-9;

    /** A law with warmup disabled, so band behaviour is testable without blind-growth movements in the way. */
    private static AdmissionControlLaw bandOnlyLaw(int initialLimit, int ceiling) {
        return AdmissionControlLaw.newBuilder()
                .initialLimit(initialLimit).ceiling(ceiling)
                .warmupAllowanceSlots(0)
                .build();
    }

    /**
     * Feeds a crafted (x, y) elasticity series: 8 bound windows - exactly the estimator's minimum - walking
     * active slots from {@code xStart} in steps of one, with throughput from {@code yStart} in steps of
     * {@code yStep}. The 8th offer computes the first verdict, and with no prior movement the law acts on it in
     * the same window - whose decision is returned.
     */
    private static AdmissionDecision feedSeries(AdmissionControlLaw law, int xStart, int yStart, int yStep) {
        AdmissionDecision last = null;
        for (int i = 0; i < 8; i++) {
            last = law.onWindowClosed(bound(yStart + i * yStep, xStart + i));
        }
        return last;
    }

    // ------------------------------------------------------------------
    // Gate 1: adjudication - INSUFFICIENT_SIGNAL shadows everything below it
    // ------------------------------------------------------------------

    @Test
    void thinWindowsHoldWithAllStateUntouched_evenWhenTheyCarryDrops() {
        AdmissionControlLaw law = AdmissionControlLaw.newBuilder().initialLimit(20).ceiling(100).build();
        double held = law.getEstimatedLimit();

        // 5 samples < the minimum of 10, AND overload drops: the adjudication gate must shadow the brake -
        // a signal-free window is not evidence of overload either.
        ClosedAdmissionWindow thinWithDrops = TestWindows.window(5, 10_000_000.0, 5, 20, 0, 3, 0, 2);
        AdmissionDecision decision = law.onWindowClosed(thinWithDrops);

        assertThat(decision.getReason()).isEqualTo(INSUFFICIENT_SIGNAL);
        assertWithMessage("bit-identical hold - not re-derived close by")
                .that(law.getEstimatedLimit()).isEqualTo(held);
        assertWithMessage("a thin window must never teach the estimator (KTD3)")
                .that(law.estimatorHistorySize()).isEqualTo(0);
        assertWithMessage("nor spend warmup allowance")
                .that(law.warmupAllowanceRemaining()).isEqualTo(DEFAULT_WARMUP_ALLOWANCE_SLOTS);
    }

    // ------------------------------------------------------------------
    // Gate 2a: overload drops - BACKOFF shadows the failure freeze and the bands
    // ------------------------------------------------------------------

    @Test
    void overloadDropsFireOneAimdCutPerWindow_regardlessOfDropCount() {
        AdmissionControlLaw one = AdmissionControlLaw.newBuilder().initialLimit(20).ceiling(100).build();
        AdmissionControlLaw many = AdmissionControlLaw.newBuilder().initialLimit(20).ceiling(100).build();

        AdmissionDecision d1 = one.onWindowClosed(TestWindows.withDrops(50, 10_000_000.0, 20, 1));
        AdmissionDecision d5 = many.onWindowClosed(TestWindows.withDrops(50, 10_000_000.0, 20, 5));

        assertThat(d1.getReason()).isEqualTo(BACKOFF);
        assertThat(d5.getReason()).isEqualTo(BACKOFF);
        assertThat(one.getEstimatedLimit()).isWithin(EXACT).of(20 * AIMD_BACKOFF_RATIO);
        assertWithMessage("one multiplicative cut for the window, however many drops it carried")
                .that(many.getEstimatedLimit()).isEqualTo(one.getEstimatedLimit());
        assertWithMessage("a braked window's evidence is polluted - never offered")
                .that(one.estimatorHistorySize()).isEqualTo(0);
    }

    @Test
    void backoffNeverCutsBelowTheOneSlotFloor() {
        AdmissionControlLaw law = AdmissionControlLaw.newBuilder().initialLimit(5).ceiling(100).build();

        for (int i = 0; i < 40; i++) {
            AdmissionDecision decision = law.onWindowClosed(TestWindows.withDrops(50, 10_000_000.0, 5, 3));
            assertThat(decision.getReason()).isEqualTo(BACKOFF);
            assertWithMessage("window %s went below the one-slot floor", i)
                    .that(law.getEstimatedLimit()).isAtLeast((double) LIMIT_FLOOR_SLOTS);
        }
        assertThat(law.getEstimatedLimit()).isEqualTo((double) LIMIT_FLOOR_SLOTS);
    }

    // ------------------------------------------------------------------
    // Gate 2b: failure fraction - growth frozen, and it shadows the binding gate and bands
    // ------------------------------------------------------------------

    @Test
    void failureFractionAboveThresholdFreezesGrowthBitIdentical() {
        AdmissionControlLaw law = AdmissionControlLaw.newBuilder().initialLimit(20).ceiling(100).build();
        double held = law.getEstimatedLimit();

        // Half the outcomes are non-successes - far above the 0.2 threshold. Bound and sample-rich, so
        // without this gate the warmup band would have granted growth on it.
        AdmissionDecision decision = law.onWindowClosed(TestWindows.withIgnores(50, 10_000_000.0, 20, 25, 25));

        assertThat(decision.getReason()).isEqualTo(FAILURE_LIMITED);
        assertThat(law.getEstimatedLimit()).isEqualTo(held);
        assertWithMessage("a failure-poisoned window must not teach the estimator")
                .that(law.estimatorHistorySize()).isEqualTo(0);

        // Control arm: the SAME window shape with clean outcomes grows via warmup - failures were the inhibitor.
        AdmissionDecision healthy = law.onWindowClosed(bound(50, 20));
        assertThat(healthy.getReason()).isEqualTo(WARMUP);
        assertThat(law.getEstimatedLimit()).isGreaterThan(held);
    }

    @Test
    void ignoresBelowTheThresholdDoNotFreeze() {
        AdmissionControlLaw law = AdmissionControlLaw.newBuilder().initialLimit(20).ceiling(100).build();

        // 10% ignores: under the freeze threshold - the window proceeds to the bands (warmup here).
        AdmissionDecision decision = law.onWindowClosed(TestWindows.withIgnores(50, 10_000_000.0, 20, 90, 10));

        assertThat(decision.getReason()).isEqualTo(WARMUP);
    }

    // ------------------------------------------------------------------
    // Gate 2c: offset-encoding back-pressure (R8) - hold, never grow, shadows the binding gate
    // ------------------------------------------------------------------

    @Test
    void offsetBackPressureHoldsAndNeverGrows() {
        AdmissionControlLaw law = AdmissionControlLaw.newBuilder().initialLimit(20).ceiling(100).build();
        double held = law.getEstimatedLimit();

        for (int i = 0; i < 5; i++) {
            // Bound and healthy - without the R8 brake this is exactly the shape warmup grows on.
            AdmissionDecision decision = law.onWindowClosed(boundWithOffsetBackPressure(50, 20));
            assertThat(decision.getReason()).isEqualTo(OFFSET_BACK_PRESSURE);
            assertThat(law.getEstimatedLimit()).isEqualTo(held);
        }
        assertWithMessage("a partition refusing records makes the window's throughput unrepresentative - "
                + "never offered")
                .that(law.estimatorHistorySize()).isEqualTo(0);
    }

    // ------------------------------------------------------------------
    // Gate 3: binding (R5) - preserve bit-identical, named for the separated starvation cause
    // ------------------------------------------------------------------

    @Test
    void unboundWindowsPreserveBitIdentical_namedForTheirStarvationCause() {
        AdmissionControlLaw law = AdmissionControlLaw.newBuilder().initialLimit(40).ceiling(100).build();
        double held = law.getEstimatedLimit();

        assertThat(law.onWindowClosed(unboundNoWork(50)).getReason()).isEqualTo(NO_WORK);
        assertThat(law.onWindowClosed(unboundOrderingStarved(50)).getReason()).isEqualTo(ORDERING_STARVED);
        assertThat(law.onWindowClosed(unboundSelfThrottled(50)).getReason()).isEqualTo(SELF_THROTTLED);

        assertWithMessage("preserve means bit-identical (R5) - absence of data yields no decision, not a "
                + "conservative one")
                .that(law.getEstimatedLimit()).isEqualTo(held);
        assertWithMessage("unbound windows never enter the estimator's history (R2)")
                .that(law.estimatorHistorySize()).isEqualTo(0);
        assertWithMessage("and never spend warmup allowance - growth is gated on binding (R3)")
                .that(law.warmupAllowanceRemaining()).isEqualTo(DEFAULT_WARMUP_ALLOWANCE_SLOTS);
    }

    // ------------------------------------------------------------------
    // WARMUP band (R3/KTD2): +q on binding alone, capped per episode, adjudicated by the first verdict
    // ------------------------------------------------------------------

    @Nested
    class WarmupBand {

        @Test
        void bindingAloneGrowsByTheAcceleratorStep_untilTheAllowanceIsSpent() {
            AdmissionControlLaw law = AdmissionControlLaw.newBuilder().initialLimit(16).ceiling(100).build();

            // q = sqrt(16) = 4 = the whole default allowance: one grant, exactly.
            AdmissionDecision first = law.onWindowClosed(bound(400, 16));
            assertThat(first.getReason()).isEqualTo(WARMUP);
            assertThat(law.getEstimatedLimit()).isWithin(EXACT).of(20.0);
            assertThat(law.warmupAllowanceRemaining()).isWithin(EXACT).of(0.0);

            // The cap: still bound, still no verdict - preserved, not grown (KTD2's named steady state).
            AdmissionDecision capped = law.onWindowClosed(bound(400, 20));
            assertThat(capped.getReason()).isEqualTo(WARMUP_EXHAUSTED);
            assertThat(law.getEstimatedLimit()).isWithin(EXACT).of(20.0);
        }

        @Test
        void grantsAreClippedToTheRemainingAllowance() {
            // q = sqrt(9) = 3, then sqrt(12) = 3.46 clipped to the 1 slot left of the 4-slot allowance.
            AdmissionControlLaw law = AdmissionControlLaw.newBuilder().initialLimit(9).ceiling(100).build();

            law.onWindowClosed(bound(400, 9));
            assertThat(law.getEstimatedLimit()).isWithin(EXACT).of(12.0);
            law.onWindowClosed(bound(400, 12));
            assertThat(law.getEstimatedLimit()).isWithin(EXACT).of(13.0);
            assertThat(law.onWindowClosed(bound(400, 13)).getReason()).isEqualTo(WARMUP_EXHAUSTED);
        }

        @Test
        void theFloorIsNotAbsorbing_oneStepFromTheFloorIsAWholeSlot() {
            // R7's point: accelerator(floor) >= 1 slot, so a law at the floor can still move.
            AdmissionControlLaw law = AdmissionControlLaw.newBuilder().initialLimit(1).ceiling(100).build();

            AdmissionDecision decision = law.onWindowClosed(bound(20, 1));

            assertThat(decision.getReason()).isEqualTo(WARMUP);
            assertThat(law.getEstimatedLimit()).isWithin(EXACT).of(2.0);
        }

        @Test
        void actingOnAVerdictResetsTheEpisodeAllowance() {
            AdmissionControlLaw law = AdmissionControlLaw.newBuilder().initialLimit(16).ceiling(100).build();
            law.onWindowClosed(bound(400, 16)); // warmup grant spends the whole allowance -> 20
            assertThat(law.warmupAllowanceRemaining()).isWithin(EXACT).of(0.0);

            // Flat evidence at the two levels until the verdict computes and, post-settle, ACTS (retracting).
            AdmissionDecision acted = null;
            for (int i = 0; i < 12 && (acted == null || acted.getReason() != ADAPTING); i++) {
                acted = law.onWindowClosed(bound(400, 20));
            }
            assertWithMessage("fixture: the flat series must have produced an acted (retracting) verdict")
                    .that(acted.getReason()).isEqualTo(ADAPTING);

            assertWithMessage("a new acted verdict opens a fresh warmup episode (KTD2)")
                    .that(law.warmupAllowanceRemaining()).isWithin(EXACT).of(DEFAULT_WARMUP_ALLOWANCE_SLOTS);
        }
    }

    // ------------------------------------------------------------------
    // The bands proper: RISE / PLATEAU / FALL from crafted series (R1)
    // ------------------------------------------------------------------

    @Nested
    class ElasticityBands {

        @Test
        void risingSeriesTakesOneAcceleratorStep() {
            AdmissionControlLaw law = bandOnlyLaw(10, 100);

            // y proportional to x: elasticity 1, far above the RISE threshold of 0.25.
            AdmissionDecision decision = feedSeries(law, 10, 100, 10);

            assertThat(decision.getReason()).isEqualTo(ADAPTING);
            assertThat(law.getEstimatedLimit()).isWithin(EXACT).of(10 + Math.sqrt(10));
        }

        @Test
        void afterAStepTheLawSettlesBeforeSteppingAgain() {
            AdmissionControlLaw law = bandOnlyLaw(10, 100);
            feedSeries(law, 10, 100, 10); // first step taken at the 8th offer
            double afterFirstStep = law.getEstimatedLimit();

            // 7 more rising offers: the verdict stays RISE, but the settle cadence parks the law - the
            // history is still answering the pre-step question (the KTD6 dynamics note).
            for (int i = 0; i < 7; i++) {
                AdmissionDecision settling = law.onWindowClosed(bound(180 + i * 10, 18 + i));
                assertThat(settling.getReason()).isEqualTo(PLATEAU);
                assertThat(law.getEstimatedLimit()).isEqualTo(afterFirstStep);
            }

            // The 8th post-step offer completes the settle: still rising, so the next step is taken.
            AdmissionDecision second = law.onWindowClosed(bound(250, 25));
            assertThat(second.getReason()).isEqualTo(ADAPTING);
            assertThat(law.getEstimatedLimit()).isWithin(EXACT).of(afterFirstStep + Math.sqrt(afterFirstStep));
        }

        @Test
        void flatSeriesHoldsAtThePlateau() {
            AdmissionControlLaw law = bandOnlyLaw(14, 100);

            // Two operating levels, identical throughput: elasticity exactly 0 - the HOLD band, the knee.
            for (int i = 0; i < 4; i++) {
                law.onWindowClosed(bound(400, 10));
            }
            AdmissionDecision decision = null;
            for (int i = 0; i < 4; i++) {
                decision = law.onWindowClosed(bound(400, 14));
            }

            assertThat(decision.getReason()).isEqualTo(PLATEAU);
            assertWithMessage("the plateau brake: flat throughput never licenses growth, however healthy the "
                    + "windows look otherwise (the old law's ratchet)")
                    .that(law.getEstimatedLimit()).isEqualTo(14.0);
        }

        @Test
        void fallingSeriesContractsMultiplicatively() {
            AdmissionControlLaw law = bandOnlyLaw(10, 100);

            // y falling as x rises: elasticity below zero - more concurrency bought less work.
            AdmissionDecision decision = feedSeries(law, 10, 170, -10);

            assertThat(decision.getReason()).isEqualTo(ADAPTING);
            assertThat(law.getEstimatedLimit()).isWithin(EXACT).of(10 * AIMD_BACKOFF_RATIO);
        }

        @Test
        void fallCutsOncePerSettle_notOncePerWindow() {
            AdmissionControlLaw law = bandOnlyLaw(10, 100);
            feedSeries(law, 10, 170, -10);
            double afterCut = law.getEstimatedLimit();

            // Further falling windows inside the settle: the verdict is still FALL, but a fresh cut needs
            // post-cut evidence - otherwise one bad episode compounds 0.9 per window into a collapse.
            AdmissionDecision next = law.onWindowClosed(bound(80, 19));
            assertThat(next.getReason()).isEqualTo(PLATEAU);
            assertThat(law.getEstimatedLimit()).isEqualTo(afterCut);
        }
    }

    // ------------------------------------------------------------------
    // Retraction: growth is provisional until the next verdict adjudicates it
    // ------------------------------------------------------------------

    @Test
    void warmupGrowthThatBoughtNothingIsRetractedToTheEpisodeBaseline() {
        AdmissionControlLaw law = AdmissionControlLaw.newBuilder().initialLimit(20).ceiling(100).build();

        // The blind step: 20 -> 24 on binding alone.
        assertThat(law.onWindowClosed(bound(400, 20)).getReason()).isEqualTo(WARMUP);
        assertThat(law.getEstimatedLimit()).isWithin(EXACT).of(24.0);

        // Flat evidence at 24: the step bought nothing. The first acted verdict must take it back - the law
        // converges to the last level that PAID, not one overshoot step above it.
        AdmissionDecision acted = null;
        for (int i = 0; i < 12 && (acted == null || acted.getReason() != ADAPTING); i++) {
            acted = law.onWindowClosed(bound(400, 24));
        }
        assertThat(acted.getReason()).isEqualTo(ADAPTING);
        assertWithMessage("retracted to exactly the pre-episode baseline")
                .that(law.getEstimatedLimit()).isWithin(EXACT).of(20.0);

        // And it stays there: the verdict is HOLD with nothing pending - a plain plateau park.
        for (int i = 0; i < 10; i++) {
            law.onWindowClosed(bound(400, 20));
            assertThat(law.getEstimatedLimit()).isWithin(EXACT).of(20.0);
        }
    }

    @Test
    void burstThenIdleCycleEndsWhereItBegan() {
        AdmissionControlLaw law = AdmissionControlLaw.newBuilder().initialLimit(20).ceiling(100).build();
        double start = law.getEstimatedLimit();

        // Burst: bound, flat-throughput windows - one warmup step out, retracted when the verdict lands.
        for (int i = 0; i < 12; i++) {
            law.onWindowClosed(bound(400, law.getLimit()));
        }
        // Idle: the work dries up - preserve, never decay (R5).
        for (int i = 0; i < 20; i++) {
            law.onWindowClosed(unboundNoWork(50));
        }

        assertWithMessage("the round trip must end bit-identically where it began - a cycle that leaks target "
                + "in either direction compounds across bursts")
                .that(law.getEstimatedLimit()).isEqualTo(start);
    }

    // ------------------------------------------------------------------
    // Clamps: AT_CAP / AT_FLOOR keep their gauge semantics (the law wanted to move past the bound)
    // ------------------------------------------------------------------

    @Test
    void growthClippedByTheCeilingReportsAtCap() {
        AdmissionControlLaw law = AdmissionControlLaw.newBuilder().initialLimit(30).ceiling(32).build();

        // Warmup wants 30 + sqrt(30) = 35.48; the cap binds at 32.
        AdmissionDecision decision = law.onWindowClosed(bound(400, 30));

        assertThat(decision.getReason()).isEqualTo(AT_CAP);
        assertThat(law.getEstimatedLimit()).isEqualTo(32.0);
        assertThat(decision.getTargetConcurrency()).isEqualTo(32);
    }

    @Test
    void contractionClippedByTheFloorReportsAtFloor() {
        AdmissionControlLaw law = bandOnlyLaw(1, 100);

        // FALL at the floor: the cut wants 0.9, the floor binds at 1.
        AdmissionDecision decision = feedSeries(law, 10, 170, -10);

        assertThat(decision.getReason()).isEqualTo(AT_FLOOR);
        assertThat(law.getEstimatedLimit()).isEqualTo((double) LIMIT_FLOOR_SLOTS);
    }

    // ------------------------------------------------------------------
    // Construction invariants
    // ------------------------------------------------------------------

    @Test
    void constructionRejectsOutOfBoundsCalibration() {
        assertThrows(IllegalArgumentException.class,
                () -> AdmissionControlLaw.newBuilder().initialLimit(0).ceiling(10).build());
        assertThrows(IllegalArgumentException.class,
                () -> AdmissionControlLaw.newBuilder().initialLimit(11).ceiling(10).build());
        assertThrows(IllegalArgumentException.class,
                () -> AdmissionControlLaw.newBuilder().ceiling(0).build());
    }

    @Test
    void aLawAtTheFloorIsConstructibleAndCanAccelerate() {
        // The R7 invariant is asserted inside construction; this is its observable consequence - constructing
        // AT the floor works, and the first bound window moves the law a whole slot off it.
        AdmissionControlLaw law = AdmissionControlLaw.newBuilder().initialLimit(LIMIT_FLOOR_SLOTS).ceiling(8).build();

        law.onWindowClosed(bound(20, 1));

        assertThat(law.getEstimatedLimit()).isAtLeast(LIMIT_FLOOR_SLOTS + 1.0);
    }
}
