package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.admission.ScenarioRunner.Phase;
import bz.stub.parallelconsumer.internal.admission.ScenarioRunner.Trajectory;

import java.util.Arrays;
import java.util.List;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The falsifier scenarios of the design's R14, each a function of (plant config schedule, assertions) over any
 * {@link AdmissionPolicy}. A scenario THROWS {@link AssertionError} when the policy fails it - which is what
 * lets the harness meta-tests assert that each mutant fails ({@code assertThrows} on the scenario), the
 * negative-control layer that keeps the suite from being satisfiable by inaction.
 * <p>
 * Working constants, deliberately loose for the harness landing (the U5 rewrite tightens them with the new
 * law): the band tolerance is {@link #BAND_TOLERANCE_FRACTION} of the oracle; deadlines are named per scenario.
 * The oracle for every scenario is {@link DeterministicPlant#optimalTargetSlots()}, derived from plant
 * parameters, never hand-fitted.
 */
final class FalsifierScenarios {

    // ------------------------------------------------------------------
    // Shared plant working constants: mu_max 400 records/s, W0 50ms => L* = 20 in-flight records,
    // so L*_slots = 20 at batchSize 1 and 5 at batchSize 4 (the units-seam arm).
    // ------------------------------------------------------------------

    static final double MU_MAX_RECORDS_PER_SECOND = 400.0;
    static final double W0_SECONDS = 0.05;
    static final int CEILING_SLOTS = 100;

    /** Band tolerance around the oracle: +/-35% of L*_slots (working constant per the plan's U8 packet). */
    static final double BAND_TOLERANCE_FRACTION = 0.35;

    /** Sweep length and its convergence deadline: in band for every window from the deadline on. */
    static final int SWEEP_WINDOWS = 200;
    static final int SWEEP_CONVERGENCE_DEADLINE_WINDOW = 160;

    /** KTD2's working warmup allowance: bounded growth under sparse adjudication, in slots. */
    static final int SPARSE_GROWTH_ALLOWANCE_SLOTS = 8;

    private FalsifierScenarios() {
    }

    static DeterministicPlant standardPlant(int batchSize) {
        return new DeterministicPlant(MU_MAX_RECORDS_PER_SECOND, W0_SECONDS, batchSize);
    }

    // ------------------------------------------------------------------
    // The scenarios asserted today (mutant matrix, and the old-law control where noted)
    // ------------------------------------------------------------------

    /**
     * The initial-condition sweep - the region-of-attraction liveness test. The plant is fixed (saturating
     * arrival, constant L*); the policy starts from {@code initialTarget} and must be inside the band of the
     * slots oracle for every window from the convergence deadline on. A frozen controller passes exactly one
     * arm (the oracle start) and fails all others.
     */
    static void initialConditionSweep(AdmissionPolicy policy, int initialTarget, int batchSize) {
        DeterministicPlant plant = standardPlant(batchSize);
        double oracle = plant.optimalTargetSlots();
        Trajectory trajectory = ScenarioRunner.run(policy, plant, initialTarget,
                Arrays.asList(Phase.of(SWEEP_WINDOWS, 1.5 * MU_MAX_RECORDS_PER_SECOND)));
        assertTargetInBandFrom(trajectory, SWEEP_CONVERGENCE_DEADLINE_WINDOW, oracle,
                "sweep from initial target " + initialTarget + " at batchSize " + batchSize);
    }

    /**
     * The arrival-burst dual: L* is held constant while the arrival rate settles, bursts, gaps and returns.
     * After the settle deadline the target must stay within the band of L* THROUGHOUT - a controller that
     * chases load (in-flight and throughput moving together, the estimator's central confound) walks out of
     * the band during the burst or the gap.
     */
    static void arrivalBurstDual(AdmissionPolicy policy, int initialTarget) {
        DeterministicPlant plant = standardPlant(1);
        double oracle = plant.optimalTargetSlots();
        int settleWindows = 60;
        Trajectory trajectory = ScenarioRunner.run(policy, plant, initialTarget, Arrays.asList(
                Phase.of(settleWindows, 380),   // settle just below capacity
                Phase.of(20, 700),              // burst above capacity
                Phase.of(60, 100),              // drain, then a genuine arrival gap
                Phase.of(40, 380)));            // return
        assertTargetInBandFrom(trajectory, settleWindows, oracle,
                "arrival-burst dual (no chasing) from initial target " + initialTarget);
    }

    /**
     * The app-limited lull - HOLD's falsifier (the RFC 7661 decay-on-idle regression test): arrival drops to
     * near zero mid-run and returns. The target must have converged before the lull, be PRESERVED through it,
     * and useful throughput must recover after it.
     */
    static void appLimitedLull(AdmissionPolicy policy, int initialTarget) {
        DeterministicPlant plant = standardPlant(1);
        double oracle = plant.optimalTargetSlots();
        int settleWindows = 100;
        int lullWindows = 40;
        double workingArrival = 380;
        Trajectory trajectory = ScenarioRunner.run(policy, plant, initialTarget, Arrays.asList(
                Phase.of(settleWindows, workingArrival),
                Phase.of(lullWindows, 2),        // near-zero arrival: windows starve of samples
                Phase.of(60, workingArrival)));

        assertTargetInBand(trajectory.commandedTargetAt(settleWindows - 1), oracle,
                "app-limited lull: converged before the lull");
        for (int i = settleWindows; i < settleWindows + lullWindows; i++) {
            assertTargetInBand(trajectory.commandedTargetAt(i), oracle,
                    "app-limited lull: target preserved through the lull at window " + i);
        }
        int recoveryTail = 30;
        assertWithMessage("app-limited lull: throughput recovered after the lull")
                .that(trajectory.settledMeanThroughput(recoveryTail))
                .isAtLeast(0.9 * workingArrival);
    }

    /**
     * The graceful-saturation plateau - the ratchet made visible: arrival is above capacity for the whole run,
     * so throughput is flat at mu_max while the queue grows, and NOTHING about a higher target buys more
     * throughput. The target must not walk: settled throughput at capacity, final target within the band of
     * the knee, and settled service time bracketed (a too-high target shows up as queueing, W &gt; W0).
     */
    static void gracefulSaturationPlateau(AdmissionPolicy policy, int initialTarget) {
        DeterministicPlant plant = standardPlant(1);
        double oracle = plant.optimalTargetSlots();
        int windows = 250;
        Trajectory trajectory = ScenarioRunner.run(policy, plant, initialTarget,
                Arrays.asList(Phase.of(windows, 1.5 * MU_MAX_RECORDS_PER_SECOND)));

        int tail = 50;
        assertWithMessage("plateau: settled useful throughput at capacity (a too-low target fails this)")
                .that(trajectory.settledMeanThroughput(tail))
                .isAtLeast(0.9 * MU_MAX_RECORDS_PER_SECOND);
        assertTargetInBand(trajectory.getFinalTarget(), oracle,
                "plateau: the target must not walk off the knee");
        assertWithMessage("plateau: settled service time bracketed (a too-high target queues, W > W0)")
                .that(trajectory.settledMeanServiceTimeNanos(tail))
                .isAtMost(1.5 * plant.uncongestedServiceTimeNanos());
    }

    /**
     * Sparse adjudication: most windows carry too few samples to adjudicate (KTD2's warmup-allowance
     * scenario). Blind growth must stay within the bounded allowance - a controller that grows on every
     * qualifying scrap of signal without a cap walks away.
     */
    static void sparseAdjudication(AdmissionPolicy policy, int initialTarget) {
        DeterministicPlant plant = standardPlant(1);
        // 8 cycles of 9 near-empty windows (4 records/s: below any sane per-window minimum sample count)
        // and one busy-but-uncongested window (150 records/s, fully served: no backlog leaks across cycles).
        Phase quiet = Phase.of(9, 4);
        Phase busy = Phase.of(1, 150);
        List<Phase> schedule = Arrays.asList(
                quiet, busy, quiet, busy, quiet, busy, quiet, busy,
                quiet, busy, quiet, busy, quiet, busy, quiet, busy);
        Trajectory trajectory = ScenarioRunner.run(policy, plant, initialTarget, schedule);
        assertWithMessage("sparse adjudication: growth bounded by the warmup allowance of "
                + SPARSE_GROWTH_ALLOWANCE_SLOTS + " slots from initial target " + initialTarget)
                .that(trajectory.maxCommandedTarget() - initialTarget)
                .isAtMost(SPARSE_GROWTH_ALLOWANCE_SLOTS);
    }

    // ------------------------------------------------------------------
    // Scenarios built now, asserted from U6 - they exercise controller machinery
    // (pause invalidation boundaries, the escape hatch, rebalance restore) that the
    // U5 law does not carry, so AdmissionLawFalsifierTest does not drive them yet.
    // TODO(refactor): wire pauseCycling into the falsifier suite in U6 (plan 2026-08-24-003, R14) - it
    //  needs the controller's pause invalidation boundaries, which U6 builds.
    // TODO(refactor): wire rebalanceShrink into the falsifier suite in U6 (plan 2026-08-24-003, KTD4) - it
    //  needs the controller's rebalance restore path; not asserted against the old law by design.
    // TODO(refactor): wire floorPin into the falsifier suite in U6 (plan 2026-08-24-003, the escape's
    //  falsifier) - its green is owned by U6's verification, which builds the ungated escape it exercises.
    // ------------------------------------------------------------------

    /**
     * Pause-cycling: periodic pause/resume (arrival gaps) against a saturated plant - PC's public throttling
     * idiom. The target must not walk across cycles: N cycles share one warmup allowance (KTD2).
     */
    static void pauseCycling(AdmissionPolicy policy, int initialTarget) {
        DeterministicPlant plant = standardPlant(1);
        double oracle = plant.optimalTargetSlots();
        Phase running = Phase.of(20, 1.5 * MU_MAX_RECORDS_PER_SECOND);
        Phase paused = Phase.of(10, 0);
        Trajectory trajectory = ScenarioRunner.run(policy, plant, initialTarget, Arrays.asList(
                running, paused, running, paused, running, paused, running, paused, running, paused, running));
        assertTargetInBandFrom(trajectory, 60, oracle,
                "pause-cycling: the target must not walk across pause/resume cycles");
    }

    /**
     * Rebalance-shrink: capacity (and with it the oracle) halves mid-run - the per-instance share after an
     * assignment shrink. The restored trajectory must respect the NEW assignment's L*.
     */
    static void rebalanceShrink(AdmissionPolicy policy, int initialTarget) {
        DeterministicPlant plant = standardPlant(1);
        Trajectory trajectory = ScenarioRunner.run(policy, plant, initialTarget, Arrays.asList(
                Phase.of(100, 1.5 * MU_MAX_RECORDS_PER_SECOND),
                Phase.withCapacity(120, 300, MU_MAX_RECORDS_PER_SECOND / 2)));
        double newOracle = plant.optimalTargetSlots(); // halved capacity => halved L*
        assertTargetInBand(trajectory.getFinalTarget(), newOracle,
                "rebalance-shrink: the trajectory must respect the NEW assignment's L*");
    }

    /**
     * The floor pin - ESCAPE's falsifier: pinned at the floor with the gated signals reading empty, the
     * re-measurement must fire within a deadline anyway (the hatch is on a path no gated signal can suppress).
     */
    static void floorPin(AdmissionPolicy policy) {
        DeterministicPlant plant = standardPlant(1);
        int escapeDeadlineWindows = 60;
        Trajectory trajectory = ScenarioRunner.run(policy, plant, 1,
                Arrays.asList(Phase.of(escapeDeadlineWindows, 0.5)));
        assertWithMessage("floor pin: the escape must lift the target off the floor within "
                + escapeDeadlineWindows + " windows even with all gated signals empty")
                .that(trajectory.maxCommandedTarget())
                .isAtLeast(2);
    }

    // ------------------------------------------------------------------
    // Band arithmetic (KTD1/KTD2 semantics: the band is denominated in slots, around the slots oracle)
    // ------------------------------------------------------------------

    static double bandFloor(double oracleSlots) {
        return oracleSlots * (1 - BAND_TOLERANCE_FRACTION);
    }

    static double bandCeiling(double oracleSlots) {
        return oracleSlots * (1 + BAND_TOLERANCE_FRACTION);
    }

    private static void assertTargetInBand(int target, double oracleSlots, String context) {
        assertWithMessage(context + ": target " + target + " below band of oracle " + oracleSlots)
                .that((double) target).isAtLeast(bandFloor(oracleSlots));
        assertWithMessage(context + ": target " + target + " above band of oracle " + oracleSlots)
                .that((double) target).isAtMost(bandCeiling(oracleSlots));
    }

    private static void assertTargetInBandFrom(Trajectory trajectory, int fromWindow, double oracleSlots,
                                               String context) {
        for (int i = fromWindow; i < trajectory.getRecords().size(); i++) {
            assertTargetInBand(trajectory.commandedTargetAt(i), oracleSlots, context + " at window " + i);
        }
        assertTargetInBand(trajectory.getFinalTarget(), oracleSlots, context + " at the final decision");
    }
}
