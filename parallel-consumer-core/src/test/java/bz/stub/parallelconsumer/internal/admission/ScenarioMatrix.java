package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.admission.ScenarioRunner.Phase;
import bz.stub.parallelconsumer.internal.admission.ScenarioRunner.Trajectory;
import lombok.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * The simulated half of the soak/torture plan's dimensional matrix (U3): a checked-in catalog of scenarios,
 * each one a data point in plant-shape x capacity-dynamics x arrival x outcome-mix x driver x scale, executed
 * against the {@link DeterministicPlant} through the falsifier suite's own {@link ScenarioRunner} seam. The
 * engine-attached dimensions (ordering, real rebalance storms, commit modes) belong to the IT-side matrix and
 * are OUT of this class's scope - stated here so the coverage check's silence about them reads as scoping,
 * not as coverage.
 * <p>
 * Coverage teeth live in {@code ScenarioMatrixTest}: every dimension value must appear in at least one
 * catalog scenario (red on silent drops), and each named high-risk dimension pair must be exercised beyond
 * marginal coverage, with the exercised value-pairs emitted as a summary - because eight scenarios can cover
 * six dimensions marginally while exercising zero interactions.
 */
final class ScenarioMatrix {

    private ScenarioMatrix() {
    }

    // Dimension vocabularies as STRING constants rather than enums, deliberately: the Truth assertion
    // generator auto-scans source enums and generates entry-point imports that cannot see a package-private
    // holder (the same trap that forced AdmissionController.ProbeKind public). The canonical value lists are
    // what the coverage check derives "every value must appear" from - adding a value here without a
    // scenario goes red exactly as an enum would.
    static final List<String> PLANT_SHAPES =
            java.util.Arrays.asList("HARD_KNEE", "CONGESTION_COLLAPSE", "NO_KNEE_BELOW_CEILING", "KNEE_AT_FLOOR");
    static final List<String> CAPACITY_DYNAMICS = java.util.Arrays.asList("STATIC", "STEP", "DRIFT", "OSCILLATION");
    static final List<String> ARRIVALS = java.util.Arrays.asList("SATURATING", "BELOW_CAPACITY", "BURST");
    static final List<String> OUTCOME_MIXES = java.util.Arrays.asList("CLEAN", "NON_SUCCESS_RIDING", "OVERLOAD_DROPS");
    /** LAW = the band machine alone; CONTROLLER = the real controller with probe/pause/rebalance machinery. */
    static final List<String> DRIVERS = java.util.Arrays.asList("LAW", "CONTROLLER");
    static final List<String> SCALES = java.util.Arrays.asList("FLOOR_HUGGING", "MID", "WIDE");

    /**
     * One catalog entry. The dimension tags are what the coverage check reads; {@code plant}/{@code phases}/
     * {@code policy} are how the scenario runs; {@code kneeSlots} carries the construction-derived oracle;
     * {@code settleAssertable} marks the static-plant scenarios the full settle/liveness invariants apply to
     * (dynamic schedules get safety-only, per the plan's open question).
     */
    @Value
    static class Scenario {
        String name;
        String shape;
        String dynamics;
        String arrival;
        String mix;
        String driver;
        String scale;
        Supplier<DeterministicPlant> plant;
        List<Phase> phases;
        Supplier<AdmissionPolicy> policy;
        int initialTarget;
        int ceilingSlots;
        double kneeSlots;
        boolean settleAssertable;
    }

    // Shared working constants: the falsifier plant (mu 400, W0 50ms => knee 20 slots at batch 1).
    static final double MU = FalsifierScenarios.MU_MAX_RECORDS_PER_SECOND;
    static final double W0 = FalsifierScenarios.W0_SECONDS;
    static final int KNEE = 20;
    static final int CEILING = FalsifierScenarios.CEILING_SLOTS;
    static final double SATURATING_ARRIVAL = MU * 1.5;

    private static DeterministicPlant standardPlant() {
        return new DeterministicPlant(MU, W0, 1);
    }

    private static Scenario scenario(String name, String shape, String dynamics, String arrival,
                                     String mix, String driver, String scale,
                                     Supplier<DeterministicPlant> plant, List<Phase> phases, int initialTarget,
                                     int ceiling, double knee, boolean settleAssertable) {
        Supplier<AdmissionPolicy> policy = driver.equals("LAW")
                ? () -> new LawAdmissionPolicy(initialTarget, ceiling)
                : () -> new ControllerAdmissionPolicy(initialTarget, 2);
        return new Scenario(name, shape, dynamics, arrival, mix, driver, scale, plant, phases, policy,
                initialTarget, ceiling, knee, settleAssertable);
    }

    /**
     * The checked-in sample. Windows are sized for a fast suite (~200 per scenario); the horizon lane (U7)
     * re-runs the settle-assertable subset at hour-scale window counts.
     */
    static List<Scenario> catalog() {
        List<Scenario> catalog = new ArrayList<>();

        catalog.add(scenario("hardKnee-static-saturating-clean-law",
                "HARD_KNEE", "STATIC", "SATURATING", "CLEAN",
                "LAW", "MID",
                ScenarioMatrix::standardPlant, CapacitySchedules.constant(200, SATURATING_ARRIVAL),
                2, CEILING, KNEE, true));

        catalog.add(scenario("hardKnee-static-saturating-clean-controller",
                "HARD_KNEE", "STATIC", "SATURATING", "CLEAN",
                "CONTROLLER", "MID",
                ScenarioMatrix::standardPlant, CapacitySchedules.constant(200, SATURATING_ARRIVAL),
                2, CEILING, KNEE, true));

        // The demo's outage shape, simulated: healthy knee 20 -> degraded knee 5 -> recovered.
        catalog.add(scenario("hardKnee-step-saturating-clean-controller",
                "HARD_KNEE", "STEP", "SATURATING", "CLEAN",
                "CONTROLLER", "MID",
                ScenarioMatrix::standardPlant,
                CapacitySchedules.step(60, 80, 100, SATURATING_ARRIVAL, MU, MU / 4),
                2, CEILING, KNEE, false));

        // Drift up then hold: capacity ramps 200->400 over 100 windows, then holds 100 - the settled band is
        // asserted against the FINAL knee, a derived form that works for a drift with a static tail.
        catalog.add(scenario("hardKnee-drift-saturating-clean-law",
                "HARD_KNEE", "DRIFT", "SATURATING", "CLEAN",
                "LAW", "MID",
                () -> new DeterministicPlant(MU / 2, W0, 1),
                CapacitySchedules.concat(
                        CapacitySchedules.drift(100, SATURATING_ARRIVAL, MU / 2, MU),
                        CapacitySchedules.constant(100, SATURATING_ARRIVAL)),
                2, CEILING, KNEE, true));

        // Cadence-adjacent oscillation - the resonance shape at matrix depth (U4 carries the deep version).
        catalog.add(scenario("hardKnee-oscillation-saturating-clean-controller",
                "HARD_KNEE", "OSCILLATION", "SATURATING", "CLEAN",
                "CONTROLLER", "MID",
                ScenarioMatrix::standardPlant,
                CapacitySchedules.oscillation(12, 8, SATURATING_ARRIVAL, MU, MU / 2),
                KNEE, CEILING, KNEE, false));

        // The thrash curve: throughput FALLS past the knee, so parking above it is a live failure mode.
        catalog.add(scenario("collapse-static-saturating-clean-law",
                "CONGESTION_COLLAPSE", "STATIC", "SATURATING", "CLEAN",
                "LAW", "MID",
                () -> {
                    DeterministicPlant plant = standardPlant();
                    plant.enableCongestionCollapse();
                    return plant;
                },
                CapacitySchedules.constant(200, SATURATING_ARRIVAL),
                2, CEILING, KNEE, true));

        // No knee below the ceiling: riding to the cap and sitting there IS correct behaviour.
        catalog.add(scenario("noKnee-static-saturating-clean-law",
                "NO_KNEE_BELOW_CEILING", "STATIC", "SATURATING", "CLEAN",
                "LAW", "MID",
                // knee_slots = mu * W0 = 6000 * 0.05 = 300, three times the 100 ceiling: no knee reachable.
                () -> new DeterministicPlant(6000, W0, 1),
                CapacitySchedules.constant(200, 8000),
                2, CEILING, CEILING, false));

        // Knee at the floor: mu * W0 = 1 slot - the escape probe's home turf.
        catalog.add(scenario("kneeAtFloor-static-saturating-clean-controller",
                "KNEE_AT_FLOOR", "STATIC", "SATURATING", "CLEAN",
                "CONTROLLER", "FLOOR_HUGGING",
                () -> new DeterministicPlant(20, W0, 1),
                CapacitySchedules.constant(200, 40),
                2, CEILING, 1, false));

        // App-limited: arrival below capacity - the target must not be ratcheted by idle headroom.
        catalog.add(scenario("hardKnee-static-belowCapacity-clean-law",
                "HARD_KNEE", "STATIC", "BELOW_CAPACITY", "CLEAN",
                "LAW", "MID",
                ScenarioMatrix::standardPlant, CapacitySchedules.constant(200, MU / 4),
                KNEE, CEILING, KNEE, false));

        // Bursts: alternating saturating and quiet - burst arrival must not be chased upward.
        catalog.add(scenario("hardKnee-burst-clean-law",
                "HARD_KNEE", "STATIC", "BURST", "CLEAN",
                "LAW", "MID",
                ScenarioMatrix::standardPlant,
                CapacitySchedules.concat(
                        CapacitySchedules.constant(20, SATURATING_ARRIVAL),
                        CapacitySchedules.constant(20, MU / 8),
                        CapacitySchedules.constant(20, SATURATING_ARRIVAL),
                        CapacitySchedules.constant(20, MU / 8),
                        CapacitySchedules.constant(80, SATURATING_ARRIVAL)),
                2, CEILING, KNEE, false));

        // Failure fraction riding the growth-freeze threshold (0.2): alternating just-below and just-above.
        List<Phase> riding = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            riding.add(Phase.withOutcomes(10, SATURATING_ARRIVAL, 0.18, -1));
            riding.add(Phase.withOutcomes(10, SATURATING_ARRIVAL, 0.22, -1));
        }
        catalog.add(scenario("hardKnee-static-saturating-nonSuccessRiding-law",
                "HARD_KNEE", "STATIC", "SATURATING",
                "NON_SUCCESS_RIDING", "LAW", "MID",
                ScenarioMatrix::standardPlant, riding, 2, CEILING, KNEE, false));

        // Overload drops every window: the BACKOFF arm cuts multiplicatively, forever - floor-park expected.
        List<Phase> dropping = new ArrayList<>();
        dropping.add(Phase.of(40, SATURATING_ARRIVAL));
        dropping.add(Phase.withOutcomes(160, SATURATING_ARRIVAL, 0.0, 5));
        catalog.add(scenario("hardKnee-static-saturating-overloadDrops-controller",
                "HARD_KNEE", "STATIC", "SATURATING",
                "OVERLOAD_DROPS", "CONTROLLER", "MID",
                ScenarioMatrix::standardPlant, dropping, 2, CEILING, KNEE, false));

        // Wide scale: knee 200 under a 400 ceiling - the accelerator's sqrt step at real width. This
        // scenario's first runs found the settled-band bound's second-order term: the steady-state cycle
        // [196..225] is identical at 300 and 600 windows (a limit cycle, not runway), and its 29-slot span
        // is two accelerator steps taken at the BAND TOP, which 2*step(knee) under-counts at this width -
        // the invariant kit's band bound now evaluates the step at knee + step. 600 windows keeps the final
        // third fully post-settle so the assertion reads pure steady state.
        catalog.add(scenario("hardKnee-static-saturating-clean-law-wide",
                "HARD_KNEE", "STATIC", "SATURATING", "CLEAN",
                "LAW", "WIDE",
                () -> new DeterministicPlant(4000, W0, 1),
                CapacitySchedules.constant(600, 6000),
                2, 400, 200, true));

        // Oscillation under the LAW driver - the driver x dynamics pair the controller arm alone leaves dark.
        catalog.add(scenario("hardKnee-oscillation-saturating-clean-law",
                "HARD_KNEE", "OSCILLATION", "SATURATING", "CLEAN",
                "LAW", "MID",
                ScenarioMatrix::standardPlant,
                CapacitySchedules.oscillation(12, 8, SATURATING_ARRIVAL, MU, MU / 2),
                KNEE, CEILING, KNEE, false));

        return catalog;
    }

    /**
     * Executes one scenario: run, apply the universally-applicable safety invariants, apply the settle band
     * on settle-assertable (static, clean) scenarios, write the CSV artifact, return the verdict line.
     */
    static String execute(Scenario scenario) {
        DeterministicPlant plant = scenario.getPlant().get();
        Trajectory trajectory = ScenarioRunner.run(scenario.getPolicy().get(), plant,
                scenario.getInitialTarget(), scenario.getPhases());
        TrajectoryInvariants.assertCeilingRespected(trajectory, scenario.getCeilingSlots());
        TrajectoryInvariants.assertFloorRespected(trajectory);
        if (scenario.isSettleAssertable()) {
            TrajectoryInvariants.assertSettledBand(trajectory, scenario.getKneeSlots());
        }
        TrajectoryInvariants.writeCsv(scenario.getName(), trajectory);
        return TrajectoryInvariants.summarize(scenario.getName(), trajectory);
    }
}
