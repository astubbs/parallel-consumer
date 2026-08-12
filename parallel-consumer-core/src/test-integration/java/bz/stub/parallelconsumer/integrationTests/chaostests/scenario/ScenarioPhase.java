package io.confluent.parallelconsumer.integrationTests.chaostests.scenario;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * One stage of a {@link Scenario}: how long it lasts, what it is meant to look like, which actions can
 * fire in it and how often, and - optionally - what must be true by the time it ends.
 * <p>
 * <b>The phase list is the script; the draws within a phase come from the seed.</b> That hybrid is the
 * whole point. Scripting alone makes every run identical and gives a test nothing to explore; seeding
 * alone cannot guarantee a demo reaches the interesting condition before the person watching gives up.
 */
@Getter
@ToString(exclude = {"postcondition", "onEnter", "planSourceFactory"})
public class ScenarioPhase {

    /**
     * What this phase is meant to show, in words a watcher can read. The runner logs it on entry, so a
     * viewer knows what they are being shown, and a failing postcondition names it.
     */
    private final String description;

    /**
     * How long the phase runs, or null for unbounded (run until the runner is stopped). Unbounded phases
     * are how the chaos suite works - the test stops the driver when its own await completes - and are
     * rejected by {@link ScenarioRunner.Mode#ONCE}, which would otherwise never terminate.
     */
    private final Duration duration;

    private final Duration minTick;
    private final Duration maxTick;

    /**
     * Weighted action set. Stored as a {@link LinkedHashMap} built in the SUPPLIED map's encounter order,
     * so an {@link java.util.EnumMap} argument keeps enum order exactly - and with it the seed contract
     * (see {@link SeededPlanSource#weightedPick}). Passing a {@link java.util.HashMap} here is a
     * replayability bug: it iterates in identity-hash order.
     */
    private final Map<ScenarioAction, Integer> weights;

    /**
     * If the previous tick's action armed the bias (returned true from
     * {@link ScenarioAction#apply}) and the tick's bias roll falls under
     * {@link #followOnProbability}, the drawn action is replaced by this one. The chaos suite's
     * join-after-stopDrain bias is exactly this: the zombie-drain defect class bites hardest when a
     * member joins while another is mid-drain.
     */
    private final ScenarioAction followOnAction;
    private final double followOnProbability;

    /** Run once on entering the phase - typically to set the workload up for what the phase shows. */
    private final Consumer<ScenarioContext> onEnter;

    /** What must be true by the phase's end. Null means the phase asserts nothing. */
    private final Postcondition postcondition;

    /**
     * Override where this phase's ticks come from. Given the run's shared {@link Random}, returns a
     * {@link PlanSource}. Null uses a {@link SeededPlanSource} over {@link #weights}.
     */
    private final Function<Random, PlanSource> planSourceFactory;

    /**
     * What a phase intends to produce, checked when it ends. A phase whose postcondition does not hold is
     * a run failure, so a scenario cannot silently stop demonstrating the thing it exists to demonstrate
     * (R33).
     */
    @FunctionalInterface
    public interface Postcondition {
        /** @return empty if the phase produced what it intended; otherwise every reason it did not. */
        List<String> check(ScenarioContext context);
    }

    @Builder(toBuilder = true)
    public ScenarioPhase(String description,
                         Duration duration,
                         Duration minTick,
                         Duration maxTick,
                         Map<? extends ScenarioAction, Integer> weights,
                         ScenarioAction followOnAction,
                         double followOnProbability,
                         Consumer<ScenarioContext> onEnter,
                         Postcondition postcondition,
                         Function<Random, PlanSource> planSourceFactory) {
        if (description == null || description.trim().isEmpty()) {
            throw new IllegalArgumentException("every phase needs a description - the runner logs it, and a "
                    + "failing postcondition names it");
        }
        this.description = description;
        this.duration = duration;
        this.minTick = minTick == null ? Duration.ofSeconds(1) : minTick;
        this.maxTick = maxTick == null ? Duration.ofSeconds(3) : maxTick;
        if (this.minTick.isNegative() || this.maxTick.compareTo(this.minTick) < 0) {
            throw new IllegalArgumentException("phase '" + description + "' needs 0 <= minTick <= maxTick, got "
                    + this.minTick + ".." + this.maxTick);
        }
        if (duration != null && (duration.isNegative() || duration.isZero())) {
            throw new IllegalArgumentException("phase '" + description + "' has a non-positive duration ("
                    + duration + ") - use null for a phase that runs until stopped");
        }
        if (followOnProbability < 0 || followOnProbability > 1) {
            throw new IllegalArgumentException("phase '" + description + "' needs a follow-on probability in "
                    + "[0,1], got " + followOnProbability);
        }
        // defensive ordered copy: deterministic iteration order is what makes the weighted pick replayable
        Map<ScenarioAction, Integer> ordered = new LinkedHashMap<>();
        int totalWeight = 0;
        if (weights != null) {
            for (Map.Entry<? extends ScenarioAction, Integer> e : weights.entrySet()) {
                if (e.getValue() == null || e.getValue() < 0) {
                    throw new IllegalArgumentException("phase '" + description + "' gives action "
                            + e.getKey().name() + " a negative or null weight");
                }
                ordered.put(e.getKey(), e.getValue());
                totalWeight += e.getValue();
            }
        }
        if (planSourceFactory == null && totalWeight <= 0) {
            throw new IllegalArgumentException("phase '" + description + "' has no weighted actions (total "
                    + "weight " + totalWeight + ") and no plan source - it would tick forever doing nothing");
        }
        this.weights = Collections.unmodifiableMap(ordered);
        this.followOnAction = followOnAction;
        this.followOnProbability = followOnProbability;
        this.onEnter = onEnter;
        this.postcondition = postcondition;
        this.planSourceFactory = planSourceFactory;
    }

    /** The plan source for one pass of this phase, drawing from the run's shared RNG. */
    public PlanSource newPlanSource(Random random) {
        return planSourceFactory != null
                ? planSourceFactory.apply(random)
                : new SeededPlanSource(random, minTick, maxTick, weights);
    }
}
