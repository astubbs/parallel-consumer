package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.admission.ScenarioRunner.Trajectory;
import bz.stub.parallelconsumer.internal.admission.ScenarioRunner.WindowRecord;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.List;

import static bz.stub.parallelconsumer.internal.admission.ScenarioMatrix.CEILING;
import static bz.stub.parallelconsumer.internal.admission.ScenarioMatrix.KNEE;
import static bz.stub.parallelconsumer.internal.admission.ScenarioMatrix.MU;
import static bz.stub.parallelconsumer.internal.admission.ScenarioMatrix.SATURATING_ARRIVAL;
import static bz.stub.parallelconsumer.internal.admission.ScenarioMatrix.W0;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The torture set (soak/torture plan U4): the adversarial scenarios, each trying to break the adaptive law
 * where a control law characteristically breaks - resonance with its own cadences, curves that punish
 * over-drive, knees in degenerate places, outcome noise riding its thresholds, and the local-minimum
 * question the owner raised (the second-wind plant). Every scenario carries a mutant negative control
 * proving it CAN go red, per the falsifier suite's discipline: a torture scenario satisfiable by inaction
 * tortures nothing.
 * <p>
 * A red here on the REAL policy is the product, not a nuisance: report it faithfully, shrink it to a
 * falsifier, then fix the law - never loosen the scenario (the plan's ledger convention).
 */
@Slf4j
class AdmissionTortureTest {

    private static final int SETTLE = 100;

    private static Trajectory runController(DeterministicPlant plant, int seed, List<ScenarioRunner.Phase> phases) {
        // Capacity overrides here model the DOWNSTREAM moving, never partitions - no rebalance mapping.
        return ScenarioRunner.run(new ControllerAdmissionPolicy(seed, 2, false), plant, seed, phases);
    }

    private static Trajectory runLaw(DeterministicPlant plant, int seed, List<ScenarioRunner.Phase> phases) {
        return ScenarioRunner.run(new LawAdmissionPolicy(seed, CEILING), plant, seed, phases);
    }

    /** The band-top allowance the horizon lane derived: knee + one probe step + one step at the excursion. */
    private static double pumpBound(double knee) {
        double step = AdmissionControlLaw.acceleratorStep(knee);
        return knee + Math.ceil(step)
                + Math.ceil(AdmissionControlLaw.acceleratorStep(knee + step));
    }

    // ------------------------------------------------------------------
    // Resonance: the plant moves at the law's own rhythm.
    // ------------------------------------------------------------------

    /**
     * Square-wave capacity at half-periods swept ACROSS the law's cadence neighbourhood (4, 8, 16 windows -
     * around the settle cadence and the short probe cadences), 200 cycles each. The pump hypothesis: edges
     * phase-locked to the adjudication cadence teach the estimator only the favourable half and ratchet the
     * target without bound.
     * <p>
     * CHARACTERIZED before being pinned (2026-08-25, the scratch probe's numbers): the pump does NOT exist
     * as a ratchet - max commanded after settle is IDENTICAL at 40, 200 and 800 cycles (hp4: 12, hp8: 39,
     * hp16: 22), and the LAST-QUARTER envelope is TIGHTER than the early one (hp8: 39 early vs 27 steady) -
     * so the sole excursion above the derived band top is a bounded, once-per-run transient in the first
     * re-climb (confounded early evidence plus the fresh warmup allowance), not resonance feeding on itself.
     * At hp4 the law lands BELOW the knee (~7 slots vs the ~15 time-average optimum): capacity flicker
     * faster than the settle cadence starves adjudication and degrades CONSERVATIVELY - throughput
     * sacrificed, downstream protected.
     * <p>
     * The pins, all derived: safety everywhere; the steady state (last quarter of 200 cycles) inside the
     * HIGH capacity's band top - no phase of the plant ever supports more; and the envelope must not grow -
     * the last-quarter max never exceeds the after-settle max, which is the ratchet detector that needs no
     * bound at all. The early transient is deliberately reported (CSV), not bounded: no honest
     * construction-derived cap for it exists yet, and inventing one would be fitting.
     */
    @Test
    void cadenceSweptResonanceNeverPumpsTheTargetAboveTheHighKnee() {
        for (int halfPeriod : new int[]{4, 8, 16}) {
            DeterministicPlant plant = new DeterministicPlant(MU, W0, 1);
            Trajectory trajectory = runController(plant, KNEE,
                    CapacitySchedules.oscillation(200, halfPeriod, SATURATING_ARRIVAL, MU, MU / 2));
            TrajectoryInvariants.assertCeilingRespected(trajectory, CEILING);
            TrajectoryInvariants.assertFloorRespected(trajectory);
            int windows = trajectory.getRecords().size();
            double bound = pumpBound(KNEE);
            int maxAfterSettle = 0;
            int maxLastQuarter = 0;
            for (WindowRecord record : trajectory.getRecords()) {
                if (record.getWindowIndex() >= SETTLE) {
                    maxAfterSettle = Math.max(maxAfterSettle, record.getCommandedTarget());
                }
                if (record.getWindowIndex() >= windows * 3 / 4) {
                    maxLastQuarter = Math.max(maxLastQuarter, record.getCommandedTarget());
                    assertWithMessage("half-period %s window %s: steady-state commanded %s above the "
                                    + "high-capacity band top %s - resonance is pumping the target",
                            halfPeriod, record.getWindowIndex(), record.getCommandedTarget(), bound)
                            .that((double) record.getCommandedTarget()).isAtMost(bound);
                }
            }
            assertWithMessage("half-period %s: the last-quarter envelope (%s) exceeds the after-settle "
                            + "envelope (%s) - the excursion is GROWING with time, which is the ratchet",
                    halfPeriod, maxLastQuarter, maxAfterSettle)
                    .that(maxLastQuarter).isAtMost(maxAfterSettle);
            TrajectoryInvariants.writeCsv("torture-resonance-hp" + halfPeriod, trajectory);
            log.info("{}", TrajectoryInvariants.summarize("torture-resonance-hp" + halfPeriod, trajectory));
        }
    }

    /** Negative control: a creeper mutant under the same resonance MUST breach the pump bound. */
    @Test
    void resonancePumpBoundFailsACreepingMutant() {
        AdmissionPolicy creeper = (previousTarget, window) -> previousTarget + 1;
        Trajectory trajectory = ScenarioRunner.run(creeper, new DeterministicPlant(MU, W0, 1), KNEE,
                CapacitySchedules.oscillation(40, 8, SATURATING_ARRIVAL, MU, MU / 2));
        assertThrows(AssertionError.class, () -> {
            double bound = pumpBound(KNEE);
            for (WindowRecord record : trajectory.getRecords()) {
                if (record.getWindowIndex() >= SETTLE && record.getCommandedTarget() > bound) {
                    throw new AssertionError("pumped to " + record.getCommandedTarget());
                }
            }
        }, "the pump bound must be breachable, or the resonance scenario asserts nothing");
    }

    // ------------------------------------------------------------------
    // The thrash curve: past the knee, more concurrency buys LESS throughput.
    // ------------------------------------------------------------------

    /**
     * Congestion collapse from a deliberately too-high seed: the law starts parked at 3x the knee on a curve
     * where over-drive measurably destroys throughput, and must walk DOWN into the knee's band - the
     * far-side park the plan names as a live failure mode. Deadline derived: the estimator's minimum entry
     * count plus the multiplicative walk from 60 into the band.
     */
    @Test
    void thrashCurveFromAHighSeedIsWalkedDownNotParkedOn() {
        DeterministicPlant plant = new DeterministicPlant(MU, W0, 1);
        plant.enableCongestionCollapse();
        Trajectory trajectory = runLaw(plant, 3 * KNEE,
                CapacitySchedules.constant(200, SATURATING_ARRIVAL));
        TrajectoryInvariants.assertSettledBand(trajectory, KNEE);
        TrajectoryInvariants.assertSettledThroughputAtLeast(trajectory, 0.7 * MU);
        TrajectoryInvariants.writeCsv("torture-thrash-high-seed", trajectory);
        log.info("{}", TrajectoryInvariants.summarize("torture-thrash-high-seed", trajectory));
    }

    /** Negative control: pinned at the high seed, the same scenario MUST fail both settled invariants. */
    @Test
    void thrashScenarioFailsAFrozenHighMutant() {
        DeterministicPlant plant = new DeterministicPlant(MU, W0, 1);
        plant.enableCongestionCollapse();
        Trajectory trajectory = ScenarioRunner.run(new MutantPolicies.FrozenLimit(), plant, 3 * KNEE,
                CapacitySchedules.constant(200, SATURATING_ARRIVAL));
        assertThrows(AssertionError.class,
                () -> TrajectoryInvariants.assertSettledThroughputAtLeast(trajectory, 0.7 * MU),
                "a mutant frozen at 3x the knee on a collapse curve must miss the throughput floor");
    }

    // ------------------------------------------------------------------
    // Degenerate knees.
    // ------------------------------------------------------------------

    /**
     * The knee far below the seed, static from the first window (no collapse event to react to - the world
     * simply IS smaller than the configuration claims): seed 48 against a knee of 8. Driven through the
     * CONTROLLER deliberately: descending from a too-high start on a flat plateau is the descent probes' job
     * by design - the bare law documents that it cannot do this alone (its javadoc says so), and this
     * scenario's first cut proved it by freezing at 48 for the whole run when law-driven.
     */
    @Test
    void kneeFarBelowTheSeedIsFoundAndHeld() {
        Trajectory trajectory = runController(new DeterministicPlant(160, W0, 1), 48,
                CapacitySchedules.constant(200, 240));
        TrajectoryInvariants.assertSettledBand(trajectory, 8);
        TrajectoryInvariants.assertSettledThroughputAtLeast(trajectory, 0.7 * 160);
        log.info("{}", TrajectoryInvariants.summarize("torture-knee-below-seed", trajectory));
    }

    /**
     * The knee AT the floor: capacity 20/s at 50ms is one slot's worth of work. The controller must park at
     * the floor without oscillating away from it - the escape probe fires on its cadence forever (designed),
     * so the bound allows its excursion and nothing more.
     */
    @Test
    void kneeAtTheFloorParksWithoutOscillation() {
        Trajectory trajectory = runController(new DeterministicPlant(20, W0, 1), 2,
                CapacitySchedules.constant(300, 40));
        TrajectoryInvariants.assertFloorRespected(trajectory);
        // The designed post-escape envelope, derived from KTD2: an escape probe that concludes limit-bound
        // takes its re-entry step AND opens a fresh warmup allowance, whose blind growth is bounded at
        // SPARSE_GROWTH_ALLOWANCE_SLOTS before adjudication retracts it - the first cut bounded only the
        // probe excursion (4) and went red on a designed transient of 5.
        double bound = AdmissionControlLaw.LIMIT_FLOOR_SLOTS + 1
                + FalsifierScenarios.SPARSE_GROWTH_ALLOWANCE_SLOTS;
        for (WindowRecord record : trajectory.getRecords()) {
            if (record.getWindowIndex() >= SETTLE) {
                assertWithMessage("window %s: commanded %s on a one-slot plant - outside the designed "
                                + "post-escape envelope %s (floor + re-entry + warmup allowance)",
                        record.getWindowIndex(), record.getCommandedTarget(), bound)
                        .that((double) record.getCommandedTarget()).isAtMost(bound);
            }
        }
        log.info("{}", TrajectoryInvariants.summarize("torture-knee-at-floor", trajectory));
    }

    // ------------------------------------------------------------------
    // Outcome noise riding the law's own threshold.
    // ------------------------------------------------------------------

    /**
     * The non-success fraction alternates just below and just above the growth-freeze threshold (0.18/0.22
     * against 0.2), ten windows a side, for the whole run. Growth may only happen in the below-threshold
     * stretches, so the trajectory must stay inside the clean plant's own band - threshold flapping must not
     * unlock MORE growth than a clean run, and must not walk the target anywhere over time.
     */
    @Test
    void failureFractionRidingTheFreezeThresholdNeitherRatchetsNorEscapes() {
        java.util.List<ScenarioRunner.Phase> riding = new java.util.ArrayList<>();
        for (int i = 0; i < 15; i++) {
            riding.add(ScenarioRunner.Phase.withOutcomes(10, SATURATING_ARRIVAL, 0.18, -1));
            riding.add(ScenarioRunner.Phase.withOutcomes(10, SATURATING_ARRIVAL, 0.22, -1));
        }
        Trajectory trajectory = runLaw(new DeterministicPlant(MU, W0, 1), 2, riding);
        TrajectoryInvariants.assertNoRatchetAfterSettle(trajectory, SETTLE);
        TrajectoryInvariants.assertSettledBand(trajectory, KNEE);
        log.info("{}", TrajectoryInvariants.summarize("torture-threshold-riding", trajectory));
    }

    // ------------------------------------------------------------------
    // The pacing regression pin: the moving world that refuted the doubling experiment.
    // ------------------------------------------------------------------

    /**
     * The moving-downstream shape (healthy knee 20, degraded knee 5, recovered), with the amplitude bounds
     * that the 2026-08-25 pacing experiment's doubled accelerator step could not hold - that scratch run
     * swung the target floor-to-ceiling (52 -> 96 -> 1 -> 93) and finished pinned at the ceiling. Anyone
     * re-scaling the shared step constant re-runs that experiment here, red.
     */
    @Test
    void movingWorldPinsThePacingDiscipline() {
        Trajectory trajectory = runController(new DeterministicPlant(MU, W0, 1), KNEE,
                CapacitySchedules.step(60, 80, 120, SATURATING_ARRIVAL, MU, MU / 4));
        TrajectoryInvariants.assertCeilingRespected(trajectory, CEILING);
        TrajectoryInvariants.assertFloorRespected(trajectory);
        // No window of any phase supports more than the HEALTHY knee's band - the doubling experiment
        // breached this within its first oscillation.
        double bound = pumpBound(KNEE);
        for (WindowRecord record : trajectory.getRecords()) {
            if (record.getWindowIndex() >= 40) {
                assertWithMessage("window %s: commanded %s above the healthy band top %s - the pacing "
                                + "discipline is broken (this is the doubled-step experiment's signature)",
                        record.getWindowIndex(), record.getCommandedTarget(), bound)
                        .that((double) record.getCommandedTarget()).isAtMost(bound);
            }
        }
        // And the degraded phase must actually be entered: by its end the target sits inside the degraded
        // band (knee 5), the walk the demo measured at 57 -> 8.
        List<WindowRecord> records = trajectory.getRecords();
        int degradedEnd = 60 + 80 - 1;
        int atDegradedEnd = records.get(degradedEnd).getCommandedTarget();
        assertWithMessage("target %s at the end of the degraded phase - never walked down into the degraded "
                + "band (%s)", atDegradedEnd, pumpBound(5))
                .that((double) atDegradedEnd).isAtMost(pumpBound(5));
        TrajectoryInvariants.writeCsv("torture-moving-world-pin", trajectory);
        log.info("{}", TrajectoryInvariants.summarize("torture-moving-world-pin", trajectory));
    }

    // ------------------------------------------------------------------
    // The local-minimum question (owner, 2026-08-25): a downstream with a SECOND WIND.
    // ------------------------------------------------------------------

    /**
     * The second-wind plant: 400/s at the first knee (20 slots), a valley where latency worsens and
     * throughput plateaus, then a REBOUND at 60 slots where the downstream gets genuinely better (1,200/s) -
     * batch amortization, a cache regime, a pool tier. The law is first-knee-seeking by construction: its
     * one-step probes land in the valley, see no gain, and restore - so it parks at the first knee and never
     * crosses unaided. This scenario pins that as DOCUMENTED, DELIBERATE behaviour (protective: crossing the
     * valley means deliberately over-driving a downstream through a measurably-worse region on a hypothesis),
     * and logs the cost - the second plateau's throughput left on the table. A future exploration feature
     * must consciously flip this pin, and THAT is the point of it.
     */
    @Test
    void secondWindBeyondTheValleyIsNotCrossedUnaided() {
        DeterministicPlant plant = new DeterministicPlant(MU, W0, 1);
        plant.enableSecondWind(60, 1200);
        Trajectory trajectory = runLaw(plant, 2, CapacitySchedules.constant(300, 2000));
        TrajectoryInvariants.assertSettledBand(trajectory, KNEE);
        for (WindowRecord record : trajectory.getRecords()) {
            if (record.getWindowIndex() >= SETTLE) {
                assertWithMessage("window %s: commanded %s slots - the law crossed the valley to the second "
                                + "wind unaided, which first-knee-seeking should not do; if a law change made "
                                + "this deliberate, flip this pin consciously and record the exploration design",
                        record.getWindowIndex(), record.getCommandedTarget())
                        .that(record.getCommandedTarget()).isLessThan(60);
            }
        }
        double settled = trajectory.settledMeanThroughput(100);
        log.info("second-wind pin: settled at {}/s on the first plateau; the un-crossed second plateau "
                + "offers 1,200/s - the documented cost of protective first-knee-seeking (seed or ceiling "
                + "past the valley is the operator's override today)", String.format("%.0f", settled));
        TrajectoryInvariants.writeCsv("torture-second-wind", trajectory);
    }

    /** Negative control: a mutant pinned past the valley PASSES the crossing check's negation - i.e. the
     * check can detect a crossing. */
    @Test
    void secondWindCrossingIsDetectable() {
        DeterministicPlant plant = new DeterministicPlant(MU, W0, 1);
        plant.enableSecondWind(60, 1200);
        Trajectory trajectory = ScenarioRunner.run(new MutantPolicies.AlwaysMaxLimit(CEILING), plant, 2,
                CapacitySchedules.constant(300, 2000));
        boolean crossed = false;
        for (WindowRecord record : trajectory.getRecords()) {
            if (record.getWindowIndex() >= SETTLE && record.getCommandedTarget() >= 60) {
                crossed = true;
                break;
            }
        }
        assertWithMessage("an always-max mutant must register as crossing the valley - otherwise the "
                + "second-wind pin cannot detect the behaviour it pins against")
                .that(crossed).isTrue();
    }
}
