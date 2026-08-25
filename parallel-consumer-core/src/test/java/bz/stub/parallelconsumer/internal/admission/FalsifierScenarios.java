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

    /**
     * The broker-fidelity scenario's honest-boundary cadence: only one boundary instant in four catches the
     * saturated slots - the 2026-08-25 comparison IT's observed ratio (45x {@code SELF_THROTTLED} to 16x bound
     * verdicts on a fully saturated arm). At a 1s window cadence that leaves at most ~4 bound entries inside
     * the law's 12s estimator horizon when binding is decided by the boundary instant alone - below the
     * estimator's minimum of 8, which is the evidence starvation that froze the broker run.
     */
    static final int FLICKER_HONEST_BOUNDARY_PERIOD = 4;

    /** Capacity collapse: the healthy plant's knee in slots - the seed, and the pre-collapse operating level. */
    static final int COLLAPSE_HEALTHY_KNEE_SLOTS = 40;

    /** Capacity collapse: windows of healthy running (sub-capacity arrival, unbound) before mu_max halves. */
    static final int COLLAPSE_HEALTHY_WINDOWS = 20;

    /** Capacity collapse: length of the halved-capacity phase. */
    static final int COLLAPSE_PHASE_WINDOWS = 60;

    /**
     * Capacity collapse: windows after the collapse by which the target must be inside the new knee's band.
     * Derived on {@link #capacityCollapse}: the first FALL verdict needs the estimator's 8 entries (all
     * post-collapse - the healthy phase is unbound and offers nothing), and the walk from seed-plus-warmup
     * (44) into the new knee's band (&le; 27) is the retraction to 40 plus four 0.9 cuts
     * ({@code 40 * 0.9^4 = 26.2}). Contraction paying the settle cadence between cuts therefore needs
     * {@code 8 + 4 * DEFAULT_SETTLE_WINDOWS = 40} windows and misses this deadline BY CONSTRUCTION;
     * per-window contraction needs {@code 8 + 4 = 12}, inside it with margin.
     */
    static final int COLLAPSE_IN_BAND_DEADLINE_WINDOWS = 16;

    /** Capacity recovery: length of the recovered phase (knee tripled from the degraded level). */
    static final int RECOVERY_PHASE_WINDOWS = 90;

    /**
     * Capacity recovery: windows after the recovery by which the target must be inside the recovered knee's
     * band. Calibrated from the recovery re-ask's own worst case (law-U13): a recovery landing just after a
     * failed re-ask waits at most the backed-off cadence cap (32) for the next ask, plus the probe (4), plus
     * the RISE ladder from the kept half-step level (~21) into the band floor (39) - one immediate confirmed
     * step then one per settle period, {@code 21 -> 26 -> 31 -> 36 -> 42}, ~25 windows - totalling ~61; 64
     * leaves determinism a small remainder. The pre-U13 controller failed at ANY deadline (its park was
     * absorbing - see the red-proof record on {@link #capacityRecovery}).
     */
    static final int RECOVERY_IN_BAND_DEADLINE_WINDOWS = 64;

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
     * The broker-freeze reproduction (the 2026-08-25 comparison IT's finding, made deterministic): seed far
     * below the knee, arrival saturating throughout, and BROKER-FIDELITY boundaries - the boundary-instant
     * active-task sample flickers empty on 3 of 4 windows and every saturated window closes under a
     * self-paused poller (healthy back-pressure: the buffer is full precisely because the plant is
     * saturated). The slots themselves stay saturated the whole run, which the per-pass active-task samples
     * carry.
     * <p>
     * The controller must still converge into the knee's band by the sweep deadline and stay there. Against
     * the pre-fix law this scenario is RED with the IT's exact freeze signature: the boundary-instant binding
     * verdict classifies most windows {@code SELF_THROTTLED}, bound windows arrive too sparsely for the warmup
     * climb levels to coexist in the estimator horizon, the verdict is structurally unreachable, and
     * {@code WARMUP_EXHAUSTED} absorbs the trajectory a truncated slot below seed-plus-allowance.
     */
    static Trajectory saturatedFlickerConvergesToTheKnee(AdmissionPolicy policy, int initialTarget) {
        DeterministicPlant plant = standardPlant(1);
        plant.enableBoundaryFidelity(FLICKER_HONEST_BOUNDARY_PERIOD, true);
        double oracle = plant.optimalTargetSlots();
        Trajectory trajectory = ScenarioRunner.run(policy, plant, initialTarget,
                Arrays.asList(Phase.of(SWEEP_WINDOWS, 1.5 * MU_MAX_RECORDS_PER_SECOND)));
        assertTargetInBandFrom(trajectory, SWEEP_CONVERGENCE_DEADLINE_WINDOW, oracle,
                "saturated flicker (broker fidelity) from initial target " + initialTarget);
        return trajectory;
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
    // The U6 lifecycle scenarios: they exercise controller machinery (pause invalidation boundaries, the
    // ungated escape, rebalance restore, the descent probe) the law alone does not carry, so
    // AdmissionLawFalsifierTest drives them through ControllerAdmissionPolicy, never LawAdmissionPolicy.
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
     * Capacity collapse - the FALL band's PACE falsifier (the 2026-08-25 comparison IT's phase-2 finding, made
     * deterministic): a consumer seeded at its healthy knee (the hand-tuned-operator initial condition, held
     * exactly by R5's preserve through a sub-capacity healthy phase), on a CONGESTION-COLLAPSE plant
     * (quadratic curve - the IT's own degradation shape, where over-driving buys measurably less work), whose
     * mu_max HALVES mid-run WITHOUT a rebalance (one partition assigned, so the capacity override is a silent
     * downstream slowdown - the rebalance-shrink scenario owns the assignment-delta path). The target must be
     * inside the NEW knee's band within {@link #COLLAPSE_IN_BAND_DEADLINE_WINDOWS} windows of the collapse and
     * stay there for the rest of the phase.
     * <p>
     * <b>The deadline is derived from the settle-cadence arithmetic (see the constant), and a law that makes
     * contraction pay the settle cadence misses it by construction</b> - the broker measurement behind it: the
     * comparison IT's adaptive arm walked 15 -&gt; 5 at one 0.9 cut per 8-window settle, ~57s of a 60s phase
     * spent over-driving the halved downstream, erasing its whole phase-2 p95 claim. Contraction acting on
     * every offered window (AIMD / RFC 7661: fast down, slow up) does the walk in ~4 windows.
     * <p>
     * <b>The stay-in-band tail is the tail-chasing check</b>: each cut's resulting window feeds the estimator
     * before the next cut is adjudicated, and the walk must stop when the freshest cross-level evidence says
     * the knee was crossed - measured without that stop, the walk over-contracted to HALF the new knee on
     * stale FALL evidence and limit-cycled below the band. The scale (knee 40 -&gt; 20) is chosen so the
     * band also brackets the controller's honest exploration around the settled level - descent-probe dips
     * (one accelerator step down, 4 windows) and one-step RISE excursions stay inside it, so the from-deadline
     * assertion needs no probe exemptions.
     */
    static Trajectory capacityCollapse(AdmissionPolicy policy) {
        DeterministicPlant plant =
                new DeterministicPlant(2 * MU_MAX_RECORDS_PER_SECOND, W0_SECONDS, 1); // knee 40 slots
        plant.enableCongestionCollapse();
        double healthyArrival = 0.75 * 2 * MU_MAX_RECORDS_PER_SECOND; // sub-capacity: healthy phase unbound
        Trajectory trajectory = ScenarioRunner.run(policy, plant, COLLAPSE_HEALTHY_KNEE_SLOTS, Arrays.asList(
                Phase.of(COLLAPSE_HEALTHY_WINDOWS, healthyArrival),
                Phase.withCapacity(COLLAPSE_PHASE_WINDOWS, healthyArrival, MU_MAX_RECORDS_PER_SECOND)));
        double newOracle = plant.optimalTargetSlots(); // halved capacity => halved L* = 20 slots
        assertWithMessage("capacity collapse: the healthy phase is unbound, so R5's preserve must hold the "
                + "seeded knee EXACTLY until the collapse - absence of data yields no decision")
                .that(trajectory.commandedTargetAt(COLLAPSE_HEALTHY_WINDOWS - 1))
                .isEqualTo(COLLAPSE_HEALTHY_KNEE_SLOTS);
        assertTargetInBandFrom(trajectory, COLLAPSE_HEALTHY_WINDOWS + COLLAPSE_IN_BAND_DEADLINE_WINDOWS,
                newOracle, "capacity collapse: contraction must reach the NEW knee's band inside the deadline");
        return trajectory;
    }

    /**
     * Capacity recovery - the absorbing-park falsifier (law-U13; the 2026-08-25 comparison IT's phase-3
     * finding, made deterministic): the {@link #capacityCollapse} shape, then capacity RECOVERS past the old
     * knee (halves, then triples relative to the degraded level - the IT's 8 -&gt; 4 -&gt; 12 shape at this
     * scenario's scale: knee 40 -&gt; 20 -&gt; 60). Work stays saturating throughout, so from the recovery on,
     * every window at the parked level is limit-bound with a growing backlog while two-thirds of the
     * downstream's capacity sits unused. The controller must re-expand into the recovered knee's band within
     * {@link #RECOVERY_IN_BAND_DEADLINE_WINDOWS} windows of the recovery and hold it to the end.
     * <p>
     * <b>RED against the pre-U13 controller, and the red is the diagnosed absorbing park</b> (run recorded
     * 2026-08-25, temporary harness): the collapse walk parks at 19 by window 34 - one accelerator cut BELOW
     * the degraded knee of 20, where the marginal-pair stop always lands, since the first below-knee window is
     * what reveals the crossing - and the recovery phase (windows 80-139) is 19 throughout with 4-window
     * descent-probe dips to 15 at the backed-off cadence (windows 102-105, 130-133), failing the deadline at
     * window 120 with target 19 against a band floor of 39: the stale HOLD verdict persists (KTD1, its
     * supporting spread evicted), RISE cannot fire on it, the descent probe keeps re-asking the only question
     * it owns (down; the dip completes 300/s against the park's 380/s and restores, evidence dropped by
     * design), stagnation cannot arm (the verdict is live), and the escape is floor-only. No mechanism asks
     * UP, so the park is absorbing at any run length.
     * <p>
     * <b>What the same run proves about an own-level drift trigger</b> (own-level throughput above the
     * verdict-era reference - the up-probe trigger law-U13 first specified): at the below-knee park the
     * plant's windows are BIT-IDENTICAL before and after the recovery - windows 78-85 all read
     * {@code target=19 throughput=380.0/s service=50ms bound=true} across the boundary at window 80, because
     * below the knee throughput is {@code slots/W0} with no capacity term. Recovery is unobservable at every
     * level the controller visits (19 and 15 both sit below the degraded and recovered knees); only probing
     * above the parked level can reveal it. The broker's phase-3 park (5 slots on a degraded knee of 4,
     * throughput drifting 76 -&gt; 115/s at the same level) is the special case where the park sits ABOVE the
     * degraded knee; the general park does not.
     * <p>
     * Landed green by law-U13's recovery re-ask probe (the bounded periodic up-ask from a live-verdict park,
     * drift-accelerated where drift is observable) - driven in {@link AdmissionLawFalsifierTest}, with its
     * mutant arms in {@code AdmissionFalsifierHarnessTest}. The red-proof record above is retained as the
     * probe's justification. Green trajectory (2026-08-25): re-asks 19 -&gt; 21 during the degraded phase are
     * answered "still the knee" and restored (windows 57-60, cadence doubling); the first post-recovery
     * re-ask (window 81) finds 420/s against the park's 380/s, keeps 21, and the re-opened full-step RISE
     * ladder walks 26 -&gt; 31 -&gt; 36 -&gt; 42 - inside the recovered band at recovery+30 against the
     * worst-case deadline of 64 - settling at 63 against the recovered knee of 60.
     */
    static Trajectory capacityRecovery(AdmissionPolicy policy) {
        DeterministicPlant plant =
                new DeterministicPlant(2 * MU_MAX_RECORDS_PER_SECOND, W0_SECONDS, 1); // knee 40 slots
        plant.enableCongestionCollapse();
        double arrival = 0.75 * 2 * MU_MAX_RECORDS_PER_SECOND;
        Trajectory trajectory = ScenarioRunner.run(policy, plant, COLLAPSE_HEALTHY_KNEE_SLOTS, Arrays.asList(
                Phase.of(COLLAPSE_HEALTHY_WINDOWS, arrival),
                Phase.withCapacity(COLLAPSE_PHASE_WINDOWS, arrival, MU_MAX_RECORDS_PER_SECOND),
                Phase.withCapacity(RECOVERY_PHASE_WINDOWS, arrival, 3 * MU_MAX_RECORDS_PER_SECOND)));
        double recoveredOracle = plant.optimalTargetSlots(); // tripled from degraded => 60 slots
        double degradedOracle = recoveredOracle / 3;
        int recoveryStartWindow = COLLAPSE_HEALTHY_WINDOWS + COLLAPSE_PHASE_WINDOWS;
        // The full round trip is the claim: contraction INTO the degraded band first (the same deadline the
        // collapse scenario derives), then re-expansion. Without the contraction half, a controller frozen at
        // the healthy knee (40) would sit inside the recovered band [39, 81] by geometric accident and the
        // scenario would be satisfiable by inaction.
        for (int i = COLLAPSE_HEALTHY_WINDOWS + COLLAPSE_IN_BAND_DEADLINE_WINDOWS; i < recoveryStartWindow; i++) {
            assertTargetInBand(trajectory.commandedTargetAt(i), degradedOracle,
                    "capacity recovery: contracted into the DEGRADED knee's band at window " + i);
        }
        assertTargetInBandFrom(trajectory, recoveryStartWindow + RECOVERY_IN_BAND_DEADLINE_WINDOWS,
                recoveredOracle,
                "capacity recovery: the controller must re-expand into the recovered knee's band");
        return trajectory;
    }

    /**
     * The floor pin - ESCAPE's falsifier: pinned at the floor with the gated signals reading empty, the
     * re-measurement must fire within a deadline anyway (the hatch is on a path no gated signal can suppress).
     */
    static Trajectory floorPin(AdmissionPolicy policy) {
        DeterministicPlant plant = standardPlant(1);
        int escapeDeadlineWindows = 60;
        Trajectory trajectory = ScenarioRunner.run(policy, plant, 1,
                Arrays.asList(Phase.of(escapeDeadlineWindows, 0.5)));
        assertWithMessage("floor pin: the escape must lift the target off the floor within "
                + escapeDeadlineWindows + " windows even with all gated signals empty")
                .that(trajectory.maxCommandedTarget())
                .isAtLeast(2);
        return trajectory;
    }

    /**
     * Descent from above - the R14 sweep-from-above arms the U5 suite documented as its gap: started ABOVE the
     * knee on a saturating plant, flat throughput gives the elasticity bands nothing to descend on (both levels
     * complete the same records/s), so only the controller's descent probe can walk the target down. The arm
     * must be inside the knee's band for EVERY window from the deadline on - probe dips included, which is what
     * keeps the probe's amplitude honest - and the walk must not cost throughput at the tail.
     */
    static Trajectory descentFromAbove(AdmissionPolicy policy, int initialTarget) {
        DeterministicPlant plant = standardPlant(1);
        double oracle = plant.optimalTargetSlots();
        int convergenceDeadlineWindow = 120;
        Trajectory trajectory = ScenarioRunner.run(policy, plant, initialTarget,
                Arrays.asList(Phase.of(200, 1.5 * MU_MAX_RECORDS_PER_SECOND)));
        assertTargetInBandFrom(trajectory, convergenceDeadlineWindow, oracle,
                "descent from above: the sweep must converge down to the knee band from " + initialTarget);
        assertWithMessage("descent from above: the walk must not cost settled throughput")
                .that(trajectory.settledMeanThroughput(30))
                .isAtLeast(0.9 * MU_MAX_RECORDS_PER_SECOND);
        return trajectory;
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
