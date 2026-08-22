package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Getter;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Why {@link AdmissionControlLaw} chose the target concurrency it did for a window.
 * <p>
 * The reasons are listed in the precedence order the law evaluates them - the first arm whose condition holds wins
 * the window.
 * <p>
 * Each carries a hand-assigned {@link #getValue() metrics value} so the
 * {@code PCMetricsDef#ADMISSION_CONSTRAINT} gauge can publish "which constraint is binding" as a number - the same
 * device {@link bz.stub.parallelconsumer.internal.State} uses for {@code pc.status}. The values are deliberately NOT
 * ordinals: adding or removing a reason must never silently renumber the ones a dashboard is already keyed on.
 */
public enum AdmissionDecisionReason {

    /**
     * The window carried too few samples to judge, or in-flight concurrency shows the application (not the
     * downstream) is the bottleneck - the limit is held, neither growing nor shrinking.
     */
    APP_LIMITED(1),

    /**
     * At least one admission was dropped for overload this window - multiplicative AIMD backoff was applied.
     */
    BACKOFF(2),

    /**
     * The non-success fraction of outcomes exceeded the threshold - growth is frozen for the window (the gradient
     * may still contract the limit). A fast-failing overloaded downstream LOWERS measured service time; without
     * this arm the gradient would read overload as headroom and grow without bound.
     */
    FAILURE_LIMITED(3),

    /**
     * A bounded probe was taken: one small step up out of a starvation signature, or one small step down to
     * re-measure a possibly contaminated baseline while pinned at the ceiling.
     */
    PROBING(4),

    /**
     * Normal Gradient2 adaptation - the gradient of short-term vs long-term service time set the new limit.
     */
    ADAPTING(5),

    /**
     * The gradient update wanted to go higher but the ceiling clamp bound.
     */
    AT_CAP(6),

    /**
     * The gradient update wanted to go lower but the one-slot floor clamp bound.
     */
    AT_FLOOR(7),

    /**
     * A real change to this instance's partition assignment discarded the sample window and the law's history, and
     * froze the target at its pre-rebalance value (carried over as the best available prior) for a cooldown - the
     * old assignment's measurements say nothing about the new workload, and adapting on settle-time noise would
     * move the target on evidence about a system that no longer exists. Set by
     * {@link AdmissionController}, not the law - the law never sees a rebalance.
     */
    COOLDOWN(8);

    /**
     * What the constraint gauge publishes before the first window has closed - no arm has decided anything yet, so
     * no reason is binding. Zero is reserved for it, which is why the reasons themselves start at one.
     */
    public static final int NO_DECISION_YET_VALUE = 0;

    /**
     * The metrics value published for this reason - hand-assigned, never the ordinal (see the class javadoc).
     */
    @Getter
    private final int value;

    AdmissionDecisionReason(int value) {
        this.value = value;
    }

    /**
     * The value-to-reason mapping, rendered for a meter description so the gauge's numbers are readable without
     * this source file - the {@code State}-to-value listing's counterpart for
     * {@code PCMetricsDef#ADMISSION_CONSTRAINT}.
     */
    public static String getReasonToValueListing() {
        return NO_DECISION_YET_VALUE + ":NONE, " + Arrays.stream(values())
                .map(reason -> reason.value + ":" + reason)
                .collect(Collectors.joining(", "));
    }
}
