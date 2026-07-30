package io.confluent.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2020-2026 Confluent, Inc. and contributors
 */

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.google.common.truth.Truth.assertThat;

/**
 * Seed-determinism regression for {@link ChaosConductor}: same seed must plan the same action sequence
 * (the replayability contract of {@code -Dchaos.seed}); different seeds must diverge. Pure function -
 * no broker needed.
 */
@Tag("chaos")
class ChaosConductorPlanIT {

    @Test
    void sameSeedPlansIdenticalActionSequence() {
        var weights = ChaosConductor.defaultW1Weights();
        List<ChaosConductor.ChaosAction> planA = ChaosConductor.planActions(42L, 200, weights);
        List<ChaosConductor.ChaosAction> planB = ChaosConductor.planActions(42L, 200, weights);
        assertThat(planA).isEqualTo(planB);
    }

    @Test
    void differentSeedsDiverge() {
        var weights = ChaosConductor.defaultW1Weights();
        List<ChaosConductor.ChaosAction> planA = ChaosConductor.planActions(42L, 200, weights);
        List<ChaosConductor.ChaosAction> planB = ChaosConductor.planActions(43L, 200, weights);
        assertThat(planA).isNotEqualTo(planB);
    }

    @Test
    void w4SameSeedPlansIdenticalActionSequence() {
        var weights = ChaosConductor.defaultW4Weights();
        List<ChaosConductor.ChaosAction> planA = ChaosConductor.planActions(42L, 200, weights);
        List<ChaosConductor.ChaosAction> planB = ChaosConductor.planActions(42L, 200, weights);
        assertThat(planA).isEqualTo(planB);
    }

    /**
     * W4's design invariant: no drains, ever. A STOP_DRAIN would open the Class 1 drain-zombie window
     * and mask the Class 2 revoke-under-work stall mechanism the scenario exists to isolate.
     */
    @Test
    void w4NeverPlansStopDrain() {
        var weights = ChaosConductor.defaultW4Weights();
        for (long seed = 0; seed < 10; seed++) {
            List<ChaosConductor.ChaosAction> plan = ChaosConductor.planActions(seed, 500, weights);
            assertThat(plan).doesNotContain(ChaosConductor.ChaosAction.STOP_DRAIN);
        }
    }
}
