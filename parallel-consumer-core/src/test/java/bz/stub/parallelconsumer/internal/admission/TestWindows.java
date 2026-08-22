package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * Fabricates {@link ClosedAdmissionWindow}s for control-law tests - the decision layer only ever sees closed
 * aggregates, so tests drive it with exact values instead of running the accumulator.
 */
class TestWindows {

    private TestWindows() {
    }

    /**
     * A healthy saturated window: in-flight median pinned at the current limit (spread zero), every outcome a
     * success - the shape that lets the gradient arm act.
     */
    static ClosedAdmissionWindow saturated(int samples, double meanServiceTimeNanos, int inFlightMedian) {
        return new ClosedAdmissionWindow(samples, meanServiceTimeNanos, samples, inFlightMedian, 0, samples, 0, 0);
    }

    /**
     * A saturated window carrying overload drops.
     */
    static ClosedAdmissionWindow withDrops(int samples, double meanServiceTimeNanos, int inFlightMedian,
                                           long drops) {
        return new ClosedAdmissionWindow(samples, meanServiceTimeNanos, samples, inFlightMedian, 0,
                samples - drops, 0, drops);
    }

    /**
     * A saturated window with an explicit success/ignore outcome split.
     */
    static ClosedAdmissionWindow withIgnores(int samples, double meanServiceTimeNanos, int inFlightMedian,
                                             long successes, long ignores) {
        return new ClosedAdmissionWindow(samples, meanServiceTimeNanos, samples, inFlightMedian, 0,
                successes, ignores, 0);
    }

    /**
     * A window with full control of the in-flight distribution (median and spread), all successes.
     */
    static ClosedAdmissionWindow withInFlight(int samples, double meanServiceTimeNanos, int inFlightMedian,
                                              int inFlightSpread) {
        return new ClosedAdmissionWindow(samples, meanServiceTimeNanos, samples, inFlightMedian, inFlightSpread,
                samples, 0, 0);
    }
}
