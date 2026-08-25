package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.admission.ScenarioRunner.Phase;

import java.util.ArrayList;
import java.util.List;

/**
 * Capacity movement as DATA (soak/torture plan U1): factories that render a downstream's capacity-over-time
 * shape into the {@link Phase} lists {@link ScenarioRunner} already consumes, so a drift or an oscillation is a
 * parameter set in a scenario catalog rather than bespoke loop code in each test. Every generator emits
 * one-window phases where capacity moves and coarse phases where it holds - the plant applies capacity at phase
 * start, so a per-window phase list IS a continuous schedule at the plant's own resolution.
 * <p>
 * The four shapes are the plan's capacity-dynamics dimension: static, step (the outage/recovery of the demos),
 * drift (the slow ramp the old law's baseline contamination fed on - now the law must track it), and
 * oscillation - including at the law's own cadences, the resonance torture, because a plant that moves at the
 * controller's rhythm is the adversarial case for any control law.
 */
final class CapacitySchedules {

    private CapacitySchedules() {
    }

    /** A single held phase: the static plant every settle/no-ratchet invariant is derived against. */
    static List<Phase> constant(int windows, double arrivalPerSecond) {
        List<Phase> phases = new ArrayList<>();
        phases.add(Phase.of(windows, arrivalPerSecond));
        return phases;
    }

    /**
     * The demos' outage shape: healthy, a capacity step to {@code degradedMu}, recovery back to
     * {@code healthyMu}. Arrival is held; the capacity moves under it.
     */
    static List<Phase> step(int healthyWindows, int degradedWindows, int recoveredWindows,
                            double arrivalPerSecond, double healthyMu, double degradedMu) {
        List<Phase> phases = new ArrayList<>();
        phases.add(Phase.withCapacity(healthyWindows, arrivalPerSecond, healthyMu));
        phases.add(Phase.withCapacity(degradedWindows, arrivalPerSecond, degradedMu));
        phases.add(Phase.withCapacity(recoveredWindows, arrivalPerSecond, healthyMu));
        return phases;
    }

    /**
     * A linear capacity ramp from {@code fromMu} to {@code toMu} across {@code windows}, one window per phase -
     * the slow-drift shape. Endpoints inclusive.
     */
    static List<Phase> drift(int windows, double arrivalPerSecond, double fromMu, double toMu) {
        if (windows < 2) {
            throw new IllegalArgumentException("a drift needs at least two windows");
        }
        List<Phase> phases = new ArrayList<>();
        for (int w = 0; w < windows; w++) {
            double mu = fromMu + (toMu - fromMu) * w / (windows - 1);
            phases.add(Phase.withCapacity(1, arrivalPerSecond, mu));
        }
        return phases;
    }

    /**
     * A square-wave capacity oscillation: {@code halfPeriodWindows} at {@code highMu}, the same at
     * {@code lowMu}, repeated for {@code cycles}. Drive {@code halfPeriodWindows} at or near the law's own
     * settle/probe cadences for the resonance torture - the square wave is deliberately the harshest phase
     * relationship (instantaneous capacity edges, no slew for the law to average over).
     */
    static List<Phase> oscillation(int cycles, int halfPeriodWindows, double arrivalPerSecond,
                                   double highMu, double lowMu) {
        List<Phase> phases = new ArrayList<>();
        for (int cycle = 0; cycle < cycles; cycle++) {
            phases.add(Phase.withCapacity(halfPeriodWindows, arrivalPerSecond, highMu));
            phases.add(Phase.withCapacity(halfPeriodWindows, arrivalPerSecond, lowMu));
        }
        return phases;
    }

    /** Concatenation, so composite scenarios (drift into oscillation, outage inside a drift) stay data. */
    @SafeVarargs
    static List<Phase> concat(List<Phase>... parts) {
        List<Phase> phases = new ArrayList<>();
        for (List<Phase> part : parts) {
            phases.addAll(part);
        }
        return phases;
    }
}
