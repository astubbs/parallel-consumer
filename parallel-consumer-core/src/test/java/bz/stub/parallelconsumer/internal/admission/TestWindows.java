package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * Fabricates {@link ClosedAdmissionWindow}s for control-law tests - the decision layer only ever sees closed
 * aggregates, so tests drive it with exact values instead of running the accumulator.
 * <p>
 * Every factory closes at the NOMINAL one-second elapsed time, so a window's success count IS its success
 * throughput per second - which is what makes elasticity series readable in the tests: {@code boundAt} windows
 * carry the boundary signals the band machine's binding gate and the estimator actually read (active slots =
 * the commanded target), and the unbound trio fabricate each of the three separated starvation causes.
 */
class TestWindows {

    /** The nominal window length these fabrications claim to have measured. */
    static final long NOMINAL_ELAPSED_NANOS = 1_000_000_000L;

    private TestWindows() {
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
     * The pre-U3 aggregate shape, defaulted onto the new constructor: nominal elapsed time, boundary signals
     * bound at the in-flight median - so law tests written against the old eight-value window keep their exact
     * meaning.
     */
    static ClosedAdmissionWindow window(int samples, double meanServiceTimeNanos, int inFlightSampleCount,
                                        int inFlightMedian, int inFlightSpread,
                                        long successes, long ignores, long drops) {
        return new ClosedAdmissionWindow(samples, meanServiceTimeNanos, inFlightSampleCount, inFlightMedian,
                inFlightSpread, 0, 0, successes, ignores, drops,
                NOMINAL_ELAPSED_NANOS, boundAt(inFlightMedian));
    }

    /**
     * Boundary signals reading LIMIT-BOUND at {@code slots}: active tasks at the commanded target. For a median
     * of zero the signals read unbound-with-no-work instead - zero slots cannot saturate anything.
     */
    static AdmissionBoundarySignals boundAt(int slots) {
        return new AdmissionBoundarySignals(slots, Math.max(1, slots), false, 0, 0, false, false);
    }

    // ------------------------------------------------------------------
    // Band-machine factories (the U5 law): explicit throughput and binding classification.
    // ------------------------------------------------------------------

    /**
     * A limit-bound, all-success window: {@code successes} completions over the nominal second (so the success
     * throughput IS {@code successes}/s) with active slots at the commanded target {@code activeSlots} - the
     * shape the estimator learns from (R2).
     */
    static ClosedAdmissionWindow bound(int successes, int activeSlots) {
        return new ClosedAdmissionWindow(successes, 10_000_000.0, successes, activeSlots, 0,
                0, 0, successes, 0, 0, NOMINAL_ELAPSED_NANOS, boundAt(activeSlots));
    }

    /** An UNBOUND window whose cause is the app running out of work - {@code NO_WORK}. */
    static ClosedAdmissionWindow unboundNoWork(int samples) {
        return new ClosedAdmissionWindow(samples, 10_000_000.0, samples, 0, 0,
                0, 0, samples, 0, 0, NOMINAL_ELAPSED_NANOS,
                new AdmissionBoundarySignals(0, 8, false, 0, 0, false, false));
    }

    /** An UNBOUND window with buffered work the shards could not yield - {@code ORDERING_STARVED}. */
    static ClosedAdmissionWindow unboundOrderingStarved(int samples) {
        return new ClosedAdmissionWindow(samples, 10_000_000.0, samples, 0, 0,
                0, 0, samples, 0, 0, NOMINAL_ELAPSED_NANOS,
                new AdmissionBoundarySignals(0, 8, true, 0, 50, false, false));
    }

    /** An UNBOUND window closed under a self-throttled poller - {@code SELF_THROTTLED}. */
    static ClosedAdmissionWindow unboundSelfThrottled(int samples) {
        return new ClosedAdmissionWindow(samples, 10_000_000.0, samples, 0, 0,
                0, 0, samples, 0, 0, NOMINAL_ELAPSED_NANOS,
                new AdmissionBoundarySignals(0, 8, false, 0, 50, true, false));
    }

    /** A limit-bound window flagged with offset-encoding back-pressure at the boundary (the R8 brake). */
    static ClosedAdmissionWindow boundWithOffsetBackPressure(int successes, int activeSlots) {
        return new ClosedAdmissionWindow(successes, 10_000_000.0, successes, activeSlots, 0,
                0, 0, successes, 0, 0, NOMINAL_ELAPSED_NANOS,
                new AdmissionBoundarySignals(activeSlots, Math.max(1, activeSlots), false, 0, 0, false, true));
    }
}
