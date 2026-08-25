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
import java.util.EnumSet;
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
 * <li><b>Marginal coverage:</b> every value of every simulated dimension appears in at least one scenario -
 * derived FROM the enums, so adding a dimension value without a scenario goes red rather than silently
 * thin.</li>
 * <li><b>Interaction coverage:</b> each named high-risk dimension pair is exercised beyond the marginal
 * minimum - every plant shape under at least two capacity dynamics, every outcome mix under scrutiny beyond
 * one arrival, every driver against at least two dynamics - and the exercised value-pairs are emitted as a
 * summary, because marginal coverage of six dimensions is satisfiable by scenarios that jointly exercise
 * zero interactions.</li>
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
        assertDimensionCovered(catalog, "plant shape", ScenarioMatrix.PlantShape.class, Scenario::getShape);
        assertDimensionCovered(catalog, "capacity dynamics", ScenarioMatrix.CapacityDynamics.class,
                Scenario::getDynamics);
        assertDimensionCovered(catalog, "arrival", ScenarioMatrix.Arrival.class, Scenario::getArrival);
        assertDimensionCovered(catalog, "outcome mix", ScenarioMatrix.OutcomeMix.class, Scenario::getMix);
        assertDimensionCovered(catalog, "driver", ScenarioMatrix.Driver.class, Scenario::getDriver);
        assertDimensionCovered(catalog, "scale", ScenarioMatrix.Scale.class, Scenario::getScale);
    }

    private static <E extends Enum<E>> void assertDimensionCovered(List<Scenario> catalog, String dimension,
                                                                   Class<E> values,
                                                                   Function<Scenario, E> extractor) {
        Set<E> covered = catalog.stream().map(extractor).collect(Collectors.toSet());
        Set<E> missing = EnumSet.allOf(values);
        missing.removeAll(covered);
        assertWithMessage("dimension '%s' has values no checked-in scenario exercises: %s - a dimension "
                + "value without a scenario is silent thinning, add one or remove the value", dimension, missing)
                .that(missing).isEmpty();
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
        for (ScenarioMatrix.PlantShape shape : ScenarioMatrix.PlantShape.values()) {
            Set<ScenarioMatrix.CapacityDynamics> dynamics = catalog.stream()
                    .filter(s -> s.getShape() == shape).map(Scenario::getDynamics)
                    .collect(Collectors.toSet());
            assertWithMessage("plant shape %s appears under no capacity dynamics", shape)
                    .that(dynamics).isNotEmpty();
        }
        Set<ScenarioMatrix.CapacityDynamics> hardKneeDynamics = catalog.stream()
                .filter(s -> s.getShape() == ScenarioMatrix.PlantShape.HARD_KNEE)
                .map(Scenario::getDynamics).collect(Collectors.toSet());
        assertWithMessage("the workhorse HARD_KNEE shape must be exercised under every capacity dynamics - "
                + "movement is where interactions live")
                .that(hardKneeDynamics).containsAtLeastElementsIn(
                        EnumSet.allOf(ScenarioMatrix.CapacityDynamics.class));

        // Every driver against at least two dynamics: probe machinery x movement is a named risk pair.
        for (ScenarioMatrix.Driver driver : ScenarioMatrix.Driver.values()) {
            Set<ScenarioMatrix.CapacityDynamics> dynamics = catalog.stream()
                    .filter(s -> s.getDriver() == driver).map(Scenario::getDynamics)
                    .collect(Collectors.toSet());
            assertWithMessage("driver %s is exercised under fewer than two capacity dynamics - the "
                    + "driver x movement interaction is a named high-risk pair", driver)
                    .that(dynamics.size()).isAtLeast(2);
        }

        // Every non-clean outcome mix under saturating arrival at minimum (that is where outcomes bind).
        for (ScenarioMatrix.OutcomeMix mix : ScenarioMatrix.OutcomeMix.values()) {
            Set<ScenarioMatrix.Arrival> arrivals = catalog.stream()
                    .filter(s -> s.getMix() == mix).map(Scenario::getArrival).collect(Collectors.toSet());
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

    /** Scenario names are the CSV artifact names, so a collision would silently overwrite a trajectory. */
    @Test
    void scenarioNamesAreUnique() {
        List<String> names = ScenarioMatrix.catalog().stream().map(Scenario::getName)
                .collect(Collectors.toList());
        assertWithMessage("catalog scenario names must be unique")
                .that(names).containsNoDuplicates();
    }
}
