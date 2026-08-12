package io.confluent.parallelconsumer.integrationTests.chaostests.scenario;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.time.Duration;

/**
 * The workload action set - the genuinely new capability beside the membership actions the chaos suite
 * already had. These are what put a running instance into the conditions worth looking at: a throughput
 * ramp, a proportion of records failing and retrying, one key failing repeatedly so completed work
 * strands behind it (head-of-line blocking), and a user function slow enough to fill the worker pool.
 * <p>
 * Actions are PARAMETERISED, which is why these are factory methods rather than enum constants: "publish
 * at 500/s" and "publish at 5000/s" are different actions in a weight map. Each produced action carries a
 * name that includes its parameter, so a timeline entry says what actually happened, and each registers
 * itself with {@link ScenarioActions} under that name.
 */
public final class WorkloadActions {

    private WorkloadActions() {
    }

    /** Set an absolute publish rate. Zero pauses the producer - a deliberate quiet period. */
    public static ScenarioAction publishAt(int recordsPerSecond) {
        return named("PUBLISH_AT(" + recordsPerSecond + "/s)", (context, targetRoll) ->
                context.workload().setPublishRatePerSecond(recordsPerSecond));
    }

    /**
     * Scale the current publish rate. Repeated draws of this in a phase produce a ramp rather than a
     * step, which is what makes a throughput chart worth watching.
     *
     * @param factor multiplier, e.g. 1.5 to ramp up, 0.5 to back off
     */
    public static ScenarioAction scalePublishRate(double factor, int minRate, int maxRate) {
        if (factor <= 0) throw new IllegalArgumentException("scale factor must be positive, was " + factor);
        if (minRate < 0 || maxRate < minRate) {
            throw new IllegalArgumentException("need 0 <= minRate <= maxRate, got " + minRate + ".." + maxRate);
        }
        return named("SCALE_PUBLISH_RATE(x" + factor + " clamp " + minRate + ".." + maxRate + ")", (context, targetRoll) -> {
            WorkloadControl workload = context.workload();
            int scaled = (int) Math.round(workload.getPublishRatePerSecond() * factor);
            workload.setPublishRatePerSecond(Math.max(minRate, Math.min(maxRate, scaled)));
        });
    }

    /** Fail this proportion of records, chosen by key so the same keys fail on every delivery. */
    public static ScenarioAction failProportion(double proportion) {
        return named("FAIL_PROPORTION(" + proportion + ")", (context, targetRoll) ->
                context.workload().setFailureProportion(proportion));
    }

    /**
     * Fail one key on every delivery - the head-of-line-blocking lever. Its shard stalls behind retry
     * backoff while the rest of the partition completes past it, which is precisely the condition the
     * offset view exists to show.
     */
    public static ScenarioAction failKeyRepeatedly(String key) {
        if (key == null) throw new IllegalArgumentException("failKeyRepeatedly needs a key");
        return named("FAIL_KEY(" + key + ")", (context, targetRoll) ->
                context.workload().setFailingKey(key));
    }

    /** Stop all induced failures. The stranded backlog then retries through and the commit lurches forward. */
    public static ScenarioAction clearInducedFailures() {
        return named("CLEAR_INDUCED_FAILURES", (context, targetRoll) -> {
            context.workload().clearFailingKey();
            context.workload().setFailureProportion(0);
        });
    }

    /** Dwell this long per record - backs the worker pool up and drives in-flight counts. */
    public static ScenarioAction functionDelay(Duration delay) {
        if (delay == null || delay.isNegative()) {
            throw new IllegalArgumentException("functionDelay needs a non-negative duration, was " + delay);
        }
        return named("FUNCTION_DELAY(" + delay + ")", (context, targetRoll) ->
                context.workload().setFunctionDelay(delay));
    }

    /** Adapts a side-effect lambda into a named, registered action that never arms the follow-on bias. */
    private static ScenarioAction named(String name, WorkloadEffect effect) {
        ScenarioAction action = new ScenarioAction() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public boolean apply(ScenarioContext context, int targetRoll) {
                effect.apply(context, targetRoll);
                context.record(name, -1);
                return false;
            }

            @Override
            public String toString() {
                return name;
            }
        };
        return ScenarioActions.byName(name).orElseGet(() -> ScenarioActions.register(action));
    }

    @FunctionalInterface
    private interface WorkloadEffect {
        void apply(ScenarioContext context, int targetRoll);
    }
}
