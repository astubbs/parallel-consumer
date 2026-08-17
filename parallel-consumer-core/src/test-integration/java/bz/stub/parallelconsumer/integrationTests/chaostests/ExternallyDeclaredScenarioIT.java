package bz.stub.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.integrationTests.chaostests.scenario.Scenario;
import bz.stub.parallelconsumer.integrationTests.chaostests.scenario.ScenarioAction;
import bz.stub.parallelconsumer.integrationTests.chaostests.scenario.ScenarioContext;
import bz.stub.parallelconsumer.integrationTests.chaostests.scenario.ScenarioPhase;
import bz.stub.parallelconsumer.integrationTests.chaostests.scenario.ScenarioRunner;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * R30: the scenario framework is generic - a scenario, its actions, and its control surface can all be
 * declared OUTSIDE the framework package and run unmodified. Deliberately lives in {@code chaostests}
 * rather than {@code chaostests.scenario} so that "outside the package" is literally true here, rather
 * than only asserted; nothing below imports anything package-private, and the framework needed no change
 * to accommodate it.
 * <p>
 * It also stands as the worked example of how to declare a scenario: an action set of your own, a context
 * of your own, phases with postconditions, and a {@link ScenarioRunner} in ONCE mode as the test verdict.
 * <p>
 * No broker: the "system" under scenario here is a counter.
 */
class ExternallyDeclaredScenarioIT {

    /** A domain of its own, with no relationship to consumer groups or Kafka. */
    private static class Reservoir implements ScenarioContext {
        private int level;
        private final List<String> timeline = new ArrayList<>();

        @Override
        public void record(String what, int instanceId) {
            timeline.add(what);
        }
    }

    /** An action set of its own, declared here and not registered anywhere in the framework. */
    private static ScenarioAction adjust(String name, int delta) {
        return new ScenarioAction() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public boolean apply(ScenarioContext context, int targetRoll) {
                ((Reservoir) context).level += delta;
                context.record(name, -1);
                return false;
            }
        };
    }

    private static final ScenarioAction FILL = adjust("FILL", 1);
    private static final ScenarioAction DRAIN = adjust("DRAIN", -1);

    private static Map<ScenarioAction, Integer> weights(int fill, int drain) {
        Map<ScenarioAction, Integer> w = new LinkedHashMap<>();
        w.put(FILL, fill);
        w.put(DRAIN, drain);
        return w;
    }

    @Test
    void aScenarioDeclaredOutsideTheFrameworkRunsUnmodified() {
        Reservoir reservoir = new Reservoir();

        Scenario scenario = Scenario.of("reservoir",
                ScenarioPhase.builder()
                        .description("fill the reservoir")
                        .duration(Duration.ofMillis(150))
                        .minTick(Duration.ofMillis(1))
                        .maxTick(Duration.ofMillis(1))
                        .weights(weights(1, 0))
                        .postcondition(ctx -> ((Reservoir) ctx).level > 0
                                ? Collections.emptyList()
                                : Collections.singletonList("the reservoir did not fill"))
                        .build(),
                ScenarioPhase.builder()
                        .description("drain it again")
                        .duration(Duration.ofMillis(150))
                        .minTick(Duration.ofMillis(1))
                        .maxTick(Duration.ofMillis(1))
                        .weights(weights(0, 1))
                        .build());

        List<String> failures = ScenarioRunner.builder()
                .scenario(scenario)
                .seed(42L)
                .mode(ScenarioRunner.Mode.ONCE)
                .context(reservoir)
                .replayCommand("./mvnw ... -Dreservoir.seed=42")
                .build()
                .run();

        assertThat(failures).isEmpty();
        assertWithMessage("both phases must have run, in order")
                .that(reservoir.timeline.stream().filter(e -> e.startsWith("PHASE START")).count()).isEqualTo(2);
    }

    /** A postcondition that cannot hold makes ONCE report a failure that names the phase. */
    @Test
    void anUnreachablePostconditionMakesOnceFail() {
        Reservoir reservoir = new Reservoir();

        List<String> failures = ScenarioRunner.builder()
                .scenario(Scenario.of("impossible", ScenarioPhase.builder()
                        .description("drain below empty")
                        .duration(Duration.ofMillis(50))
                        .minTick(Duration.ofMillis(1))
                        .maxTick(Duration.ofMillis(1))
                        .weights(weights(0, 1))
                        .postcondition(ctx -> ((Reservoir) ctx).level > 0
                                ? Collections.emptyList()
                                : Collections.singletonList("level never rose above zero"))
                        .build()))
                .seed(42L)
                .mode(ScenarioRunner.Mode.ONCE)
                .context(reservoir)
                .build()
                .run();

        assertThat(failures).hasSize(1);
        assertThat(failures.get(0)).contains("drain below empty");
        assertThat(failures.get(0)).contains("level never rose above zero");
    }
}
