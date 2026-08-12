package io.confluent.parallelconsumer.integrationTests.chaostests.scenario;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Behaviour of the scenario runner itself - phase ordering, both modes, postconditions, the follow-on
 * bias, and the seed contract at the run level (same seed = same action log; different seed = same phase
 * sequence, different action log).
 * <p>
 * No broker and no fleet: the runner is exercised through a recording {@link ScenarioContext}, so what is
 * under test is the driver, not Kafka. The draw stream it consumes is guarded separately and much more
 * strictly by {@link PlanSourceSeedStabilityTest}.
 */
class ScenarioRunnerTest {

    private static final Duration FIXED_TICK = Duration.ofMillis(1);

    /**
     * How many ticks a 400ms phase must have produced before its log is worth comparing. Deliberately far
     * below the ~400 a 1ms tick would give on an idle machine: failsafe runs test methods in parallel, so
     * these phases compete for CPU. With three equally weighted actions, coincidence over this many
     * entries is about one in fifty thousand.
     */
    private static final int MIN_COMPARABLE_TICKS = 10;

    /**
     * Test action set: each one just names itself into the context's log. Deliberately NOT an enum - the
     * Truth subject generator picks enums up out of the test tree on the following build and cannot see
     * ones nested in a package-private test class.
     */
    private static ScenarioAction logging(String name) {
        return new ScenarioAction() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public boolean apply(ScenarioContext context, int targetRoll) {
                context.record(name + "@" + targetRoll, -1);
                return false;
            }

            @Override
            public String toString() {
                return name;
            }
        };
    }

    private static final ScenarioAction ALPHA = logging("ALPHA");
    private static final ScenarioAction BRAVO = logging("BRAVO");
    private static final ScenarioAction CHARLIE = logging("CHARLIE");

    /** Arms the follow-on bias, the way STOP_DRAIN does. */
    private static final ScenarioAction ARM = new ScenarioAction() {
        @Override
        public String name() {
            return "ARM";
        }

        @Override
        public boolean apply(ScenarioContext context, int targetRoll) {
            context.record("ARM", -1);
            return true;
        }

        @Override
        public String toString() {
            return "ARM";
        }
    };

    /** Captures everything the runner and its actions record. */
    private static class RecordingContext implements ScenarioContext {
        private final List<String> entries = new CopyOnWriteArrayList<>();

        @Override
        public void record(String what, int instanceId) {
            entries.add(what);
        }

        List<String> actionLog() {
            return entries.stream()
                    .filter(e -> !e.startsWith("PHASE ") && !e.startsWith("SCENARIO "))
                    .collect(Collectors.toList());
        }

        List<String> phaseStarts() {
            return entries.stream().filter(e -> e.startsWith("PHASE START")).collect(Collectors.toList());
        }
    }

    private static Map<ScenarioAction, Integer> evenWeights() {
        Map<ScenarioAction, Integer> weights = new LinkedHashMap<>();
        weights.put(ALPHA, 1);
        weights.put(BRAVO, 1);
        weights.put(CHARLIE, 1);
        return weights;
    }

    private static ScenarioPhase phase(String description, Duration duration, ScenarioPhase.Postcondition post) {
        return ScenarioPhase.builder()
                .description(description)
                .duration(duration)
                .minTick(FIXED_TICK)
                .maxTick(FIXED_TICK)
                .weights(evenWeights())
                .postcondition(post)
                .build();
    }

    @Test
    void onceRunsEveryPhaseInOrderAndReportsNoFailures() {
        RecordingContext context = new RecordingContext();
        Scenario scenario = Scenario.of("two phase",
                phase("first", Duration.ofMillis(60), null),
                phase("second", Duration.ofMillis(60), null));

        List<String> failures = runner(scenario, 42L, ScenarioRunner.Mode.ONCE, context).run();

        assertThat(failures).isEmpty();
        assertThat(context.phaseStarts()).hasSize(2);
        assertThat(context.phaseStarts().get(0)).contains("first");
        assertThat(context.phaseStarts().get(1)).contains("second");
        assertWithMessage("a phase that ran for 60ms at a 1ms tick must actually have acted")
                .that(context.actionLog()).isNotEmpty();
    }

    @Test
    void onceReportsAFailedPostconditionAndNamesItsPhase() {
        RecordingContext context = new RecordingContext();
        Scenario scenario = Scenario.of("one good one bad",
                phase("the good phase", Duration.ofMillis(30), ctx -> Collections.emptyList()),
                phase("the bad phase", Duration.ofMillis(30),
                        ctx -> Collections.singletonList("no work was stranded behind the failing offset")));

        List<String> failures = runner(scenario, 42L, ScenarioRunner.Mode.ONCE, context).run();

        assertThat(failures).hasSize(1);
        assertThat(failures.get(0)).contains("the bad phase");
        assertThat(failures.get(0)).contains("no work was stranded behind the failing offset");
        assertWithMessage("a passing phase must not be blamed").that(failures.get(0)).doesNotContain("the good");
    }

    /** A postcondition that blows up is a failure, not a silently-skipped check. */
    @Test
    void aThrowingPostconditionIsAFailureNotASkip() {
        RecordingContext context = new RecordingContext();
        Scenario scenario = Scenario.of("throwing check",
                phase("explodes", Duration.ofMillis(20), ctx -> {
                    throw new IllegalStateException("probe unavailable");
                }));

        List<String> failures = runner(scenario, 1L, ScenarioRunner.Mode.ONCE, context).run();

        assertThat(failures).hasSize(1);
        assertThat(failures.get(0)).contains("probe unavailable");
    }

    @Test
    void onceRefusesAnUnboundedPhaseRatherThanHanging() {
        Scenario unbounded = Scenario.of("never ends", phase("forever", null, null));
        assertThatThrownBy(() -> runner(unbounded, 1L, ScenarioRunner.Mode.ONCE, new RecordingContext()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ONCE mode would never finish");
    }

    @Test
    void loopRepeatsThePhaseListAndStopsCleanlyOnInterrupt() throws Exception {
        RecordingContext context = new RecordingContext();
        Scenario scenario = Scenario.of("looping",
                phase("first", Duration.ofMillis(30), null),
                phase("second", Duration.ofMillis(30), null));
        ScenarioRunner runner = runner(scenario, 42L, ScenarioRunner.Mode.LOOP, context);

        Thread thread = new Thread(runner::run, "scenario-runner-test");
        thread.start();
        Thread.sleep(400);
        runner.stop();
        thread.join(5_000);

        assertWithMessage("the runner thread must terminate on stop(), leaving nothing orphaned")
                .that(thread.isAlive()).isFalse();
        assertThat(runner.isRunning()).isFalse();
        assertWithMessage("400ms of 60ms passes must have repeated the phase list more than once")
                .that(context.phaseStarts().size()).isAtLeast(4);
    }

    @Test
    void sameScenarioAndSeedProduceAnIdenticalActionLog() {
        Scenario scenario = Scenario.of("determinism", phase("only", Duration.ofMillis(400), null));

        List<String> first = actionLogOf(scenario, 42L);
        List<String> second = actionLogOf(scenario, 42L);

        assertThat(comparablePrefix(first, second)).isAtLeast(MIN_COMPARABLE_TICKS);
        assertThat(prefix(first, comparablePrefix(first, second)))
                .isEqualTo(prefix(second, comparablePrefix(first, second)));
    }

    @Test
    void aDifferentSeedKeepsThePhaseSequenceAndChangesTheActionLog() {
        Scenario scenario = Scenario.of("determinism",
                phase("alpha phase", Duration.ofMillis(400), null),
                phase("beta phase", Duration.ofMillis(400), null));

        RecordingContext a = new RecordingContext();
        RecordingContext b = new RecordingContext();
        runner(scenario, 42L, ScenarioRunner.Mode.ONCE, a).run();
        runner(scenario, 43L, ScenarioRunner.Mode.ONCE, b).run();

        assertWithMessage("the phase list is the script - it must not vary with the seed")
                .that(descriptions(a)).isEqualTo(descriptions(b));
        int common = comparablePrefix(a.actionLog(), b.actionLog());
        assertThat(common).isAtLeast(MIN_COMPARABLE_TICKS);
        assertWithMessage("the draws within a phase come from the seed - a different seed must draw differently")
                .that(prefix(a.actionLog(), common)).isNotEqualTo(prefix(b.actionLog(), common));
    }

    /**
     * The generalised join-after-drain bias: when the previous action armed it and the tick's bias roll
     * falls under the probability, the drawn action is replaced by the phase's follow-on action.
     */
    @Test
    void anArmedFollowOnBiasReplacesTheDrawnAction() {
        RecordingContext context = new RecordingContext();
        // tick 1 draws ARM (arms the bias); tick 2 draws ALPHA with a bias roll of 0, so the follow-on wins
        ScenarioTick arming = new ScenarioTick(1, 0.5, ARM, 0);
        ScenarioTick drawn = new ScenarioTick(1, 0.0, ALPHA, 7);
        ScenarioPhase phase = ScenarioPhase.builder()
                .description("bias")
                .duration(Duration.ofMillis(60))
                .weights(evenWeights())
                .followOnAction(CHARLIE)
                .followOnProbability(1.0)
                .planSourceFactory(random -> ScriptedPlanSource.cycling(arming, drawn))
                .build();

        runner(Scenario.of("bias", phase), 1L, ScenarioRunner.Mode.ONCE, context).run();

        List<String> log = context.actionLog();
        assertThat(log.size()).isAtLeast(2);
        assertThat(log.get(0)).isEqualTo("ARM");
        assertWithMessage("with the bias armed and the roll under the probability, CHARLIE must replace ALPHA")
                .that(log.get(1)).startsWith("CHARLIE");
    }

    /** The bias only fires when armed - an unarmed tick keeps its drawn action. */
    @Test
    void anUnarmedFollowOnBiasLeavesTheDrawnActionAlone() {
        RecordingContext context = new RecordingContext();
        ScenarioTick drawn = new ScenarioTick(1, 0.0, ALPHA, 7);
        ScenarioPhase phase = ScenarioPhase.builder()
                .description("no bias")
                .duration(Duration.ofMillis(40))
                .weights(evenWeights())
                .followOnAction(CHARLIE)
                .followOnProbability(1.0)
                .planSourceFactory(random -> ScriptedPlanSource.cycling(drawn))
                .build();

        runner(Scenario.of("no bias", phase), 1L, ScenarioRunner.Mode.ONCE, context).run();

        assertThat(context.actionLog()).isNotEmpty();
        assertThat(context.actionLog().stream().anyMatch(e -> e.startsWith("CHARLIE"))).isFalse();
    }

    /** An action failing must not kill the run - it is recorded and the next tick proceeds. */
    @Test
    void anActionThatThrowsIsRecordedAndTheRunContinues() {
        RecordingContext context = new RecordingContext();
        ScenarioAction explosive = new ScenarioAction() {
            @Override
            public String name() {
                return "EXPLODE";
            }

            @Override
            public boolean apply(ScenarioContext ctx, int targetRoll) {
                throw new IllegalStateException("boom");
            }
        };
        Map<ScenarioAction, Integer> weights = new LinkedHashMap<>();
        weights.put(explosive, 1);
        weights.put(ALPHA, 1);
        ScenarioPhase phase = ScenarioPhase.builder()
                .description("survives failures")
                .duration(Duration.ofMillis(120))
                .minTick(FIXED_TICK)
                .maxTick(FIXED_TICK)
                .weights(weights)
                .build();

        List<String> failures = runner(Scenario.of("resilient", phase), 42L, ScenarioRunner.Mode.ONCE, context).run();

        assertWithMessage("an action failure is not a postcondition failure").that(failures).isEmpty();
        assertThat(context.entries.stream().anyMatch(e -> e.contains("SCENARIO ERROR") && e.contains("boom"))).isTrue();
        assertWithMessage("ticks after the failure must still act")
                .that(context.actionLog().stream().anyMatch(e -> e.startsWith("ALPHA"))).isTrue();
    }

    /**
     * A malformed phase must be rejected where it is declared, not discovered as an
     * {@code IllegalArgumentException} out of {@code Random.nextInt(0)} an hour into a demo.
     */
    @Test
    void aMalformedPhaseIsRejectedAtDeclaration() {
        Map<ScenarioAction, Integer> allZero = new LinkedHashMap<>();
        allZero.put(ALPHA, 0);
        assertThatThrownBy(() -> ScenarioPhase.builder().description("zero weights").weights(allZero).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("total weight 0");

        assertThatThrownBy(() -> ScenarioPhase.builder().description("no actions").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no weighted actions");

        assertThatThrownBy(() -> ScenarioPhase.builder().description("").weights(evenWeights()).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("description");

        assertThatThrownBy(() -> ScenarioPhase.builder().description("inverted ticks")
                .minTick(Duration.ofSeconds(5)).maxTick(Duration.ofSeconds(1))
                .weights(evenWeights()).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("minTick <= maxTick");

        assertThatThrownBy(() -> ScenarioPhase.builder().description("impossible bias")
                .followOnProbability(1.5).weights(evenWeights()).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("follow-on probability");

        assertThatThrownBy(() -> Scenario.builder().name("no phases").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no phases");
    }

    /** A context with no workload wired must name the problem, not silently do nothing. */
    @Test
    void aWorkloadActionAgainstAContextWithNoWorkloadFailsByName() {
        RecordingContext context = new RecordingContext();
        assertThatThrownBy(() -> WorkloadActions.publishAt(500).apply(context, 0))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("no workload control");
    }

    // --- helpers ---

    private static ScenarioRunner runner(Scenario scenario, long seed, ScenarioRunner.Mode mode,
                                         ScenarioContext context) {
        return ScenarioRunner.builder()
                .scenario(scenario)
                .seed(seed)
                .mode(mode)
                .context(context)
                .build();
    }

    private static List<String> actionLogOf(Scenario scenario, long seed) {
        RecordingContext context = new RecordingContext();
        runner(scenario, seed, ScenarioRunner.Mode.ONCE, context).run();
        return context.actionLog();
    }

    private static List<String> descriptions(RecordingContext context) {
        return context.phaseStarts();
    }

    /**
     * Two runs of a duration-bounded phase produce slightly different tick COUNTS (wall clock decides
     * when the phase ends), so determinism is asserted over the common prefix - which is where the draw
     * stream lives.
     */
    private static int comparablePrefix(List<String> a, List<String> b) {
        return Math.min(a.size(), b.size());
    }

    private static List<String> prefix(List<String> list, int n) {
        return new ArrayList<>(list.subList(0, Math.min(n, list.size())));
    }
}
