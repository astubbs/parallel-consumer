package io.confluent.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.util.List;

import static com.google.common.truth.Truth.assertThat;

/**
 * Seed-determinism regression for {@link ChaosConductor}: same seed must produce the same COMPLETE
 * draw sequence - tick, bias roll, action AND target roll - through the real draw path
 * ({@code ChaosConductor.drawTick}, the exact function the live loop consumes each tick), which is the
 * replayability contract of {@code -Dchaos.seed}. Different seeds must diverge.
 * <p>
 * Pure function, no broker - and deliberately NOT tagged {@code chaos}: this test gates every default
 * integration build, guarding the contract the on-demand chaos runs rely on.
 */
class ChaosConductorPlanIT {

    private static final java.time.Duration MIN_TICK = java.time.Duration.ofMillis(500);
    private static final java.time.Duration MAX_TICK = java.time.Duration.ofMillis(1500);

    @Test
    void sameSeedDrawsIdenticalSequence() {
        var weights = ChaosConductor.defaultW1Weights();
        List<ChaosConductor.TickDraws> planA = ChaosConductor.planTicks(42L, 200, MIN_TICK, MAX_TICK, weights);
        List<ChaosConductor.TickDraws> planB = ChaosConductor.planTicks(42L, 200, MIN_TICK, MAX_TICK, weights);
        assertThat(planA).isEqualTo(planB);
    }

    @Test
    void differentSeedsDiverge() {
        var weights = ChaosConductor.defaultW1Weights();
        List<ChaosConductor.TickDraws> planA = ChaosConductor.planTicks(42L, 200, MIN_TICK, MAX_TICK, weights);
        List<ChaosConductor.TickDraws> planB = ChaosConductor.planTicks(43L, 200, MIN_TICK, MAX_TICK, weights);
        assertThat(planA).isNotEqualTo(planB);
    }
}
