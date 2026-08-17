package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.proxy.harness.HarnessScenario;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The scenario names are one vocabulary, shared by the engine-side harness, this suite and every language's
 * tests - so this holds the two lists that exist in Java against each other.
 * <p>
 * <b>A scenario the harness can serve but nothing drives is the quiet failure here.</b> It reads as
 * covered - the name is in the guide, the harness answers to it - while no client has ever been asked to
 * do it. Growing the harness therefore fails this test until either a runner behaviour is wired for the
 * new scenario or it is listed below as deliberately not driven yet, which is a decision someone made
 * rather than one nobody noticed.
 *
 * @author Antony Stubbs
 */
class ScenarioCoverageTest {

    /**
     * Harness scenarios this suite deliberately does not drive yet. Empty today: all four of the harness's
     * live scenarios have a runner behaviour. The named-but-not-yet-served scenarios in
     * {@code parallel-consumer-proxy/docs/client-authoring-guide.md} are NOT here, because the harness does
     * not offer them yet either - they arrive together with the engine units that make them answerable.
     */
    private static final List<String> DELIBERATELY_NOT_DRIVEN = List.of();

    @Test
    void everyHarnessScenarioIsDrivenOrDeliberatelyNot() {
        var driven = ConformanceScenarios.all().stream().map(ConformanceScenario::name).toList();
        var undriven = HarnessScenario.conformanceScenarios().stream()
                .map(HarnessScenario::name)
                .filter(name -> !driven.contains(name))
                .filter(name -> !DELIBERATELY_NOT_DRIVEN.contains(name))
                .toList();

        assertWithMessage("harness scenarios no client is ever asked to run. Wire one into "
                + "ConformanceScenarios, or list it in DELIBERATELY_NOT_DRIVEN with a reason - an "
                + "undriven scenario reads as covered and is not")
                .that(undriven).isEmpty();
    }

    @Test
    void everyDrivenScenarioIsOneTheHarnessKnows() {
        var known = HarnessScenario.conformanceScenarios().stream().map(HarnessScenario::name).toList();
        for (var scenario : ConformanceScenarios.all()) {
            assertWithMessage("the scenario name is its identity in the harness CLI, in the guide and in "
                    + "every language's tests, so it cannot be invented here")
                    .that(known).contains(scenario.name());
        }
    }

    @Test
    void everyScenarioPrescribesABehaviourAndAtLeastOneDelivery() {
        for (var scenario : ConformanceScenarios.all()) {
            assertWithMessage("%s prescribes a behaviour", scenario.name())
                    .that(scenario.behaviour()).isNotNull();
            // A scenario expecting zero deliveries would pass without the client doing anything at all -
            // the vacuous-assertion shape this suite exists to avoid.
            assertWithMessage("%s expects at least one delivery", scenario.name())
                    .that(scenario.expectedDispatches()).isAtLeast(1);
            assertWithMessage("%s configures an in-flight ceiling the proxy would accept", scenario.name())
                    .that(scenario.maxConcurrency()).isAtLeast(1);
        }
    }

    /**
     * A ceiling at or above a scenario's own dispatch count is one the scenario can never reach, so a
     * behaviour whose whole instrument is FILLING the ceiling would hold a group that never fills - and the
     * runner would exit 1 for a reason that is nothing to do with the client.
     * <p>
     * Written as a property of the pair rather than as a comment on one scenario, because the trap is
     * invisible at the call site: the ceiling's default is the dispatch count, so a ceiling scenario written
     * without an explicit one looks identical to the four that want the default.
     */
    @Test
    void aScenarioThatFillsTheCeilingSetsOneItCanReach() {
        for (var scenario : ConformanceScenarios.all()) {
            if (scenario.behaviour() != RunnerBehaviour.HOLD_UNTIL_CEILING_FULL) {
                continue;
            }
            assertWithMessage("%s prescribes hold-until-ceiling-full, so its ceiling must be below the number "
                    + "of records it seeds - otherwise the group never fills and the runner times out "
                    + "against a client that did nothing wrong", scenario.name())
                    .that(scenario.maxConcurrency()).isLessThan(scenario.expectedDispatches());
        }
    }
}
