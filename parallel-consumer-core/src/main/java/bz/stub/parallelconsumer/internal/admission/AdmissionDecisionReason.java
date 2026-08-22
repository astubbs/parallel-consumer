package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * Why {@link AdmissionControlLaw} chose the target concurrency it did for a window.
 * <p>
 * The reasons are listed in the precedence order the law evaluates them - the first arm whose condition holds wins
 * the window.
 */
public enum AdmissionDecisionReason {

    /**
     * The window carried too few samples to judge, or in-flight concurrency shows the application (not the
     * downstream) is the bottleneck - the limit is held, neither growing nor shrinking.
     */
    APP_LIMITED,

    /**
     * At least one admission was dropped for overload this window - multiplicative AIMD backoff was applied.
     */
    BACKOFF,

    /**
     * The non-success fraction of outcomes exceeded the threshold - growth is frozen for the window (the gradient
     * may still contract the limit). A fast-failing overloaded downstream LOWERS measured service time; without
     * this arm the gradient would read overload as headroom and grow without bound.
     */
    FAILURE_LIMITED,

    /**
     * A bounded probe was taken: one small step up out of a starvation signature, or one small step down to
     * re-measure a possibly contaminated baseline while pinned at the ceiling.
     */
    PROBING,

    /**
     * Normal Gradient2 adaptation - the gradient of short-term vs long-term service time set the new limit.
     */
    ADAPTING,

    /**
     * The gradient update wanted to go higher but the ceiling clamp bound.
     */
    AT_CAP,

    /**
     * The gradient update wanted to go lower but the one-slot floor clamp bound.
     */
    AT_FLOOR,
}
