package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * Fabricates {@link ClosedAdmissionWindow}s for control-law tests - the decision layer only ever sees closed
 * aggregates, so tests drive it with exact values instead of running the accumulator.
 * <p>
 * Every factory closes at the NOMINAL one-second elapsed time with boundary signals bound at the in-flight
 * median - the committed law reads neither, so tests that care about elapsed time or classification construct
 * their windows directly ({@code AdmissionSampleWindowTest}).
 */
class TestWindows {

    /** The nominal window length these fabrications claim to have measured. */
    static final long NOMINAL_ELAPSED_NANOS = 1_000_000_000L;

    private TestWindows() {
    }

    /**
     * A healthy saturated window: in-flight median pinned at the current limit (spread zero), every outcome a
     * success - the shape that lets the gradient arm act.
     */
    static ClosedAdmissionWindow saturated(int samples, double meanServiceTimeNanos, int inFlightMedian) {
        return window(samples, meanServiceTimeNanos, samples, inFlightMedian, 0, samples, 0, 0);
    }

    /**
     * A saturated window carrying overload drops.
     */
    static ClosedAdmissionWindow withDrops(int samples, double meanServiceTimeNanos, int inFlightMedian,
                                           long drops) {
        return window(samples, meanServiceTimeNanos, samples, inFlightMedian, 0, samples - drops, 0, drops);
    }

    /**
     * A saturated window with an explicit success/ignore outcome split.
     */
    static ClosedAdmissionWindow withIgnores(int samples, double meanServiceTimeNanos, int inFlightMedian,
                                             long successes, long ignores) {
        return window(samples, meanServiceTimeNanos, samples, inFlightMedian, 0, successes, ignores, 0);
    }

    /**
     * A window with full control of the in-flight distribution (median and spread), all successes.
     */
    static ClosedAdmissionWindow withInFlight(int samples, double meanServiceTimeNanos, int inFlightMedian,
                                              int inFlightSpread) {
        return window(samples, meanServiceTimeNanos, samples, inFlightMedian, inFlightSpread, samples, 0, 0);
    }

    /**
     * The pre-U3 aggregate shape, defaulted onto the new constructor: nominal elapsed time, boundary signals
     * bound at the in-flight median - so law tests written against the old eight-value window keep their exact
     * meaning.
     */
    static ClosedAdmissionWindow window(int samples, double meanServiceTimeNanos, int inFlightSampleCount,
                                        int inFlightMedian, int inFlightSpread,
                                        long successes, long ignores, long drops) {
        return new ClosedAdmissionWindow(samples, meanServiceTimeNanos, inFlightSampleCount, inFlightMedian,
                inFlightSpread, successes, ignores, drops,
                NOMINAL_ELAPSED_NANOS, boundAt(inFlightMedian));
    }

    /**
     * Boundary signals reading LIMIT-BOUND at {@code slots}: active tasks at the commanded target. For a median
     * of zero the signals read unbound-with-no-work instead - zero slots cannot saturate anything.
     */
    static AdmissionBoundarySignals boundAt(int slots) {
        return new AdmissionBoundarySignals(slots, Math.max(1, slots), false, 0, 0, false, false);
    }
}
