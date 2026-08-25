package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.admission.ScenarioMatrix.Scenario;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Runs the checked-in {@link ScenarioMatrix} sample (one dynamic test per scenario, so a red names its
 * scenario) and enforces the coverage obligations the soak/torture plan gives U3 teeth with:
 * <ol>
 * <li><b>Marginal coverage:</b> every value of every simulated dimension's canonical vocabulary appears in at
 * least one scenario - derived FROM the vocabulary lists, so adding a value without a scenario goes red
 * rather than silently thin.</li>
 * <li><b>Interaction coverage:</b> each named high-risk dimension pair is exercised beyond the marginal
 * minimum - every plant shape under at least one capacity dynamics, the workhorse shape under ALL of them,
 * every driver against at least two dynamics, every outcome mix under at least one arrival - and the
 * exercised value-pairs are emitted as a summary, because marginal coverage of six dimensions is satisfiable
 * by scenarios that jointly exercise zero interactions.</li>
 * </ol>
 * The engine-attached dimensions (ordering, real engine events, commit modes) are the IT-side matrix's to
 * cover; their absence here is scoping, stated in {@link ScenarioMatrix}'s javadoc.
 */
@Slf4j
class ScenarioMatrixTest {

    @TestFactory
    List<DynamicTest> catalogScenariosHoldTheirInvariants() {
        List<DynamicTest> tests = new ArrayList<>();
        for (Scenario scenario : ScenarioMatrix.catalog()) {
            tests.add(DynamicTest.dynamicTest(scenario.getName(), () -> {
                String verdict = ScenarioMatrix.execute(scenario);
                log.info("{}", verdict);
            }));
        }
        return tests;
    }

    @Test
    void everyDimensionValueAppearsInTheCatalog() {
        List<Scenario> catalog = ScenarioMatrix.catalog();
        assertDimensionCovered(catalog, "plant shape", ScenarioMatrix.PLANT_SHAPES, Scenario::getShape);
        assertDimensionCovered(catalog, "capacity dynamics", ScenarioMatrix.CAPACITY_DYNAMICS,
                Scenario::getDynamics);
        assertDimensionCovered(catalog, "arrival", ScenarioMatrix.ARRIVALS, Scenario::getArrival);
        assertDimensionCovered(catalog, "outcome mix", ScenarioMatrix.OUTCOME_MIXES, Scenario::getMix);
        assertDimensionCovered(catalog, "driver", ScenarioMatrix.DRIVERS, Scenario::getDriver);
        assertDimensionCovered(catalog, "scale", ScenarioMatrix.SCALES, Scenario::getScale);
    }

    private static void assertDimensionCovered(List<Scenario> catalog, String dimension,
                                               List<String> vocabulary, Function<Scenario, String> extractor) {
        Set<String> covered = catalog.stream().map(extractor).collect(Collectors.toSet());
        List<String> missing = new ArrayList<>(vocabulary);
        missing.removeAll(covered);
        assertWithMessage("dimension '%s' has values no checked-in scenario exercises: %s - a dimension "
                + "value without a scenario is silent thinning, add one or remove the value", dimension, missing)
                .that(missing).isEmpty();
        List<String> unknown = new ArrayList<>(covered);
        unknown.removeAll(vocabulary);
        assertWithMessage("dimension '%s' has scenario tags outside the canonical vocabulary: %s - a typo "
                + "here silently exits every coverage count", dimension, unknown)
                .that(unknown).isEmpty();
    }

    /**
     * The named high-risk pairs, each held above marginal coverage; the exercised value-pairs are logged as
     * the coverage summary the plan requires the runner to emit.
     */
    @Test
    void namedDimensionPairsAreExercisedBeyondMarginalCoverage() {
        List<Scenario> catalog = ScenarioMatrix.catalog();

        logPairSummary(catalog, "shape x dynamics", s -> s.getShape() + " x " + s.getDynamics());
        logPairSummary(catalog, "mix x arrival", s -> s.getMix() + " x " + s.getArrival());
        logPairSummary(catalog, "driver x dynamics", s -> s.getDriver() + " x " + s.getDynamics());

        // Every plant shape under at least the static baseline; the workhorse shape under ALL dynamics.
        for (String shape : ScenarioMatrix.PLANT_SHAPES) {
            Set<String> dynamics = catalog.stream()
                    .filter(s -> s.getShape().equals(shape)).map(Scenario::getDynamics)
                    .collect(Collectors.toSet());
            assertWithMessage("plant shape %s appears under no capacity dynamics", shape)
                    .that(dynamics).isNotEmpty();
        }
        Set<String> hardKneeDynamics = catalog.stream()
                .filter(s -> s.getShape().equals("HARD_KNEE"))
                .map(Scenario::getDynamics).collect(Collectors.toSet());
        assertWithMessage("the workhorse HARD_KNEE shape must be exercised under every capacity dynamics - "
                + "movement is where interactions live")
                .that(hardKneeDynamics).containsAtLeastElementsIn(ScenarioMatrix.CAPACITY_DYNAMICS);

        // Every driver against at least two dynamics: probe machinery x movement is a named risk pair.
        for (String driver : ScenarioMatrix.DRIVERS) {
            Set<String> dynamics = catalog.stream()
                    .filter(s -> s.getDriver().equals(driver)).map(Scenario::getDynamics)
                    .collect(Collectors.toSet());
            assertWithMessage("driver %s is exercised under fewer than two capacity dynamics - the "
                    + "driver x movement interaction is a named high-risk pair", driver)
                    .that(dynamics.size()).isAtLeast(2);
        }

        // Every outcome mix under at least one arrival pattern (outcome effects bind under saturation).
        for (String mix : ScenarioMatrix.OUTCOME_MIXES) {
            Set<String> arrivals = catalog.stream()
                    .filter(s -> s.getMix().equals(mix)).map(Scenario::getArrival)
                    .collect(Collectors.toSet());
            assertWithMessage("outcome mix %s appears under no arrival pattern", mix)
                    .that(arrivals).isNotEmpty();
        }
    }

    private static void logPairSummary(List<Scenario> catalog, String pairName,
                                       Function<Scenario, String> pair) {
        Set<String> pairs = new HashSet<>();
        for (Scenario scenario : catalog) {
            pairs.add(pair.apply(scenario));
        }
        log.info("coverage summary [{}]: {} value-pair(s) exercised: {}", pairName, pairs.size(),
                pairs.stream().sorted().collect(Collectors.joining(", ")));
    }

    // ------------------------------------------------------------------
    // Sabotage controls (testing-at-write-time): each invariant is proven ABLE to fail by driving a mutant
    // policy through a catalog-shaped run - the negative-control layer that keeps the kit from being
    // satisfiable by inaction.
    // ------------------------------------------------------------------

    /** A policy pinned at the ceiling must fail the settled band on the thrash plant - parked above the knee. */
    @Test
    void settledBandInvariantFailsAnAlwaysMaxMutant() {
        DeterministicPlant plant = new DeterministicPlant(ScenarioMatrix.MU, ScenarioMatrix.W0, 1);
        plant.enableCongestionCollapse();
        ScenarioRunner.Trajectory trajectory = ScenarioRunner.run(
                new MutantPolicies.AlwaysMaxLimit(ScenarioMatrix.CEILING), plant, 2,
                CapacitySchedules.constant(200, ScenarioMatrix.SATURATING_ARRIVAL));
        org.junit.jupiter.api.Assertions.assertThrows(AssertionError.class,
                () -> TrajectoryInvariants.assertSettledBand(trajectory, ScenarioMatrix.KNEE),
                "a mutant parked at the ceiling must fail the settled band - if it passes, the band asserts nothing");
    }

    /** A policy frozen at the seed must fail the derived throughput floor - it never climbs to capacity. */
    @Test
    void throughputFloorInvariantFailsAFrozenMutant() {
        ScenarioRunner.Trajectory trajectory = ScenarioRunner.run(
                new MutantPolicies.FrozenLimit(),
                new DeterministicPlant(ScenarioMatrix.MU, ScenarioMatrix.W0, 1), 2,
                CapacitySchedules.constant(200, ScenarioMatrix.SATURATING_ARRIVAL));
        org.junit.jupiter.api.Assertions.assertThrows(AssertionError.class,
                () -> TrajectoryInvariants.assertSettledThroughputAtLeast(trajectory,
                        0.8 * ScenarioMatrix.MU),
                "a mutant frozen at 2 slots must fail the derived throughput floor");
    }

    /** A ratcheting mutant - one slot up every window, forever - must fail the no-ratchet invariant. */
    @Test
    void noRatchetInvariantFailsACreepingMutant() {
        AdmissionPolicy creeper = (previousTarget, window) -> previousTarget + 1;
        ScenarioRunner.Trajectory trajectory = ScenarioRunner.run(creeper,
                new DeterministicPlant(ScenarioMatrix.MU, ScenarioMatrix.W0, 1), 2,
                CapacitySchedules.constant(400, ScenarioMatrix.SATURATING_ARRIVAL));
        org.junit.jupiter.api.Assertions.assertThrows(AssertionError.class,
                () -> TrajectoryInvariants.assertNoRatchetAfterSettle(trajectory, 100),
                "a mutant that adds one slot per window is the ratchet itself - the invariant must catch it");
    }

    /** Scenario names are the CSV artifact names, so a collision would silently overwrite a trajectory. */
    @Test
    void scenarioNamesAreUnique() {
        List<String> names = ScenarioMatrix.catalog().stream().map(Scenario::getName)
                .collect(Collectors.toList());
        assertWithMessage("catalog scenario names must be unique")
                .that(names).containsNoDuplicates();
    }
}
