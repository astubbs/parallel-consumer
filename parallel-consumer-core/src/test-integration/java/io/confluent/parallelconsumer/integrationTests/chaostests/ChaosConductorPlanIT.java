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
}
