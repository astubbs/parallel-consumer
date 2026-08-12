package bz.stub.parallelconsumer.integrationTests.chaostests.scenario;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * The seeded, replayable plan source - the Chaos Pain Suite's original draw stream, lifted out of the
 * conductor's loop unchanged.
 * <p>
 * <b>THE SEED-STABILITY INVARIANT.</b> {@link #drawTick} consumes exactly four draws per tick, always in
 * this order:
 * <ol>
 *   <li>{@code nextInt(1000)} - the tick length</li>
 *   <li>{@code nextDouble()} - the follow-on bias roll</li>
 *   <li>{@code nextInt(totalWeight)} - the weighted action pick (inside {@link #weightedPick})</li>
 *   <li>{@code nextInt(Integer.MAX_VALUE)} - the target roll</li>
 * </ol>
 * Every chaos run logs its seed and a replay command, and the suite's probes are calibrated against a
 * real historical defect with thresholds sitting in measured gaps. Change the ORDER, the COUNT, or the
 * bounds of these draws - or the total weight or iteration order of a weight map - and every previously
 * recorded seed stops reproducing the schedule it used to, silently: no test goes red for it. That is
 * what {@code PlanSourceSeedStabilityTest} exists to prevent, with golden values captured from the
 * pre-generalisation implementation.
 * <p>
 * The single draw path is used by BOTH the live run and {@link #plan}, so the determinism regression
 * test exercises the exact production draw sequence, bias and target rolls included.
 */
public class SeededPlanSource implements PlanSource {

    private final Random random;
    private final Duration minTick;
    private final Duration maxTick;
    private final Map<? extends ScenarioAction, Integer> weights;

    /**
     * @param random  the run's RNG. Phases of one scenario SHARE it, so the whole run is one continuous
     *                stream from the seed rather than a per-phase restart.
     * @param weights must iterate deterministically (an {@link java.util.EnumMap} or
     *                {@link java.util.LinkedHashMap}) - a {@link java.util.HashMap} would iterate in
     *                identity-hash order and break replay
     */
    public SeededPlanSource(Random random, Duration minTick, Duration maxTick,
                            Map<? extends ScenarioAction, Integer> weights) {
        this.random = random;
        this.minTick = minTick;
        this.maxTick = maxTick;
        this.weights = weights;
    }

    @Override
    public ScenarioTick nextTick() {
        return drawTick(random, minTick, maxTick, weights);
    }

    @Override
    public String describe() {
        return "seeded tick=" + minTick + ".." + maxTick + " weights=" + weights;
    }

    /** The one draw path. See the class javadoc before touching a single line of it. */
    public static ScenarioTick drawTick(Random random, Duration minTick, Duration maxTick,
                                        Map<? extends ScenarioAction, Integer> weights) {
        long tickMs = minTick.toMillis()
                + (long) (random.nextInt(1000) / 1000.0 * (maxTick.toMillis() - minTick.toMillis()));
        double biasRoll = random.nextDouble();
        ScenarioAction action = weightedPick(random, weights);
        int targetRoll = random.nextInt(Integer.MAX_VALUE);
        return new ScenarioTick(tickMs, biasRoll, action, targetRoll);
    }

    /** Pure function of the seed: the exact draw sequence a run with this seed consumes. */
    public static List<ScenarioTick> plan(long seed, int steps, Duration minTick, Duration maxTick,
                                          Map<? extends ScenarioAction, Integer> weights) {
        Random r = new Random(seed);
        List<ScenarioTick> plan = new ArrayList<>();
        for (int i = 0; i < steps; i++) {
            plan.add(drawTick(r, minTick, maxTick, weights));
        }
        return plan;
    }

    /**
     * One {@code nextInt(total)} draw, resolved by walking the map's iteration order and accumulating -
     * so both the total weight AND the iteration order are part of the seed contract.
     */
    static ScenarioAction weightedPick(Random r, Map<? extends ScenarioAction, Integer> weights) {
        int total = weights.values().stream().mapToInt(Integer::intValue).sum();
        int pick = r.nextInt(total);
        int acc = 0;
        ScenarioAction last = null;
        for (Map.Entry<? extends ScenarioAction, Integer> e : weights.entrySet()) {
            acc += e.getValue();
            last = e.getKey();
            if (pick < acc) return e.getKey();
        }
        return last; // unreachable: acc ends at total and pick < total
    }
}
