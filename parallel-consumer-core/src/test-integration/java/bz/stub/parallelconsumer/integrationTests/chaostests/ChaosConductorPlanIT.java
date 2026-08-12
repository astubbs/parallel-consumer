package io.confluent.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.integrationTests.chaostests.scenario.ChaosScenarios;
import io.confluent.parallelconsumer.integrationTests.chaostests.scenario.MembershipAction;
import io.confluent.parallelconsumer.integrationTests.chaostests.scenario.ScenarioAction;
import io.confluent.parallelconsumer.integrationTests.chaostests.scenario.ScenarioTick;
import io.confluent.parallelconsumer.integrationTests.chaostests.scenario.SeededPlanSource;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.google.common.truth.Truth.assertThat;

/**
 * Seed-determinism regression for the conductor's plan: same seed must produce the same COMPLETE draw
 * sequence - tick, bias roll, action AND target roll - through the real draw path
 * ({@link SeededPlanSource#drawTick}, the exact function the live loop consumes each tick), which is the
 * replayability contract of {@code -Dchaos.seed}. Different seeds must diverge.
 * <p>
 * Self-consistency only: this test says the stream is stable WITHIN a build. That the stream is the same
 * one recorded seeds were drawn from ACROSS builds is guarded by the golden values in
 * {@code scenario.PlanSourceSeedStabilityTest}, captured from the pre-generalisation implementation.
 * <p>
 * Pure function, no broker - and deliberately NOT tagged {@code chaos}: this test gates every default
 * integration build, guarding the contract the on-demand chaos runs rely on.
 */
class ChaosConductorPlanIT {

    private static final java.time.Duration MIN_TICK = java.time.Duration.ofMillis(500);
    private static final java.time.Duration MAX_TICK = java.time.Duration.ofMillis(1500);

    @Test
    void sameSeedDrawsIdenticalSequence() {
        var weights = ChaosScenarios.w1Weights();
        List<ScenarioTick> planA = SeededPlanSource.plan(42L, 200, MIN_TICK, MAX_TICK, weights);
        List<ScenarioTick> planB = SeededPlanSource.plan(42L, 200, MIN_TICK, MAX_TICK, weights);
        assertThat(planA).isEqualTo(planB);
    }

    @Test
    void differentSeedsDiverge() {
        var weights = ChaosScenarios.w1Weights();
        List<ScenarioTick> planA = SeededPlanSource.plan(42L, 200, MIN_TICK, MAX_TICK, weights);
        List<ScenarioTick> planB = SeededPlanSource.plan(43L, 200, MIN_TICK, MAX_TICK, weights);
        assertThat(planA).isNotEqualTo(planB);
    }

    @Test
    void w4SameSeedDrawsIdenticalSequence() {
        var weights = ChaosScenarios.w4Weights();
        List<ScenarioTick> planA = SeededPlanSource.plan(42L, 200, MIN_TICK, MAX_TICK, weights);
        List<ScenarioTick> planB = SeededPlanSource.plan(42L, 200, MIN_TICK, MAX_TICK, weights);
        assertThat(planA).isEqualTo(planB);
    }

    /**
     * W4's design invariant: no drains, ever. A STOP_DRAIN would open the Class 1 drain-zombie window
     * and mask the Class 2 revoke-under-work stall mechanism the scenario exists to isolate.
     */
    @Test
    void w4NeverPlansStopDrain() {
        var weights = ChaosScenarios.w4Weights();
        for (long seed = 0; seed < 10; seed++) {
            List<ScenarioAction> actions = SeededPlanSource.plan(seed, 500, MIN_TICK, MAX_TICK, weights)
                    .stream().map(ScenarioTick::getAction).collect(java.util.stream.Collectors.toList());
            assertThat(actions).doesNotContain(MembershipAction.STOP_DRAIN);
        }
    }
}
