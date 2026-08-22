package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Value;

/**
 * Immutable aggregates of one closed sample window, as handed to {@link AdmissionControlLaw#onWindowClosed}.
 * <p>
 * Produced by {@link AdmissionSampleWindow#close()}; the caller (the engine) decides when the window's time bound
 * has elapsed and closes it with whatever it has - possibly zero samples.
 */
@Value
public class ClosedAdmissionWindow {

    /**
     * Number of service-time samples - one per user-function invocation. The decision layer holds (no limit change)
     * when this is below its per-window minimum.
     */
    int sampleCount;

    /**
     * Arithmetic mean of the window's service-time samples, in nanoseconds. Zero when {@link #sampleCount} is zero.
     * Samples are already fill-normalized by the caller (batch normalization is the CALLER's job).
     */
    double meanServiceTimeNanos;

    /**
     * Number of in-flight snapshots observed this window.
     */
    int inFlightSampleCount;

    /**
     * Median (p50) of the in-flight snapshots. Zero when none were observed.
     */
    int inFlightMedian;

    /**
     * Dispersion of the in-flight snapshots, measured as the p90 - p10 distance (see
     * {@link AdmissionSampleWindow} for the percentile convention). A LARGE spread relative to the limit is the
     * bimodal signature: samples split between near-idle and near-limit, which must NOT classify as starvation.
     */
    int inFlightSpread;

    /**
     * Invocations that completed successfully this window.
     */
    long successCount;

    /**
     * Invocations whose outcome should not influence the limit (e.g. skipped/filtered work). Ignores only matter
     * through {@link #nonSuccessFraction()} - they never enter the latency math.
     */
    long ignoreCount;

    /**
     * Admissions dropped because the system was overloaded. Any non-zero count fires the AIMD backoff arm exactly
     * once for the window.
     */
    long overloadDropCount;

    public long totalOutcomeCount() {
        return successCount + ignoreCount + overloadDropCount;
    }

    /**
     * Fraction of outcomes that were not successes; zero when no outcomes were recorded.
     */
    public double nonSuccessFraction() {
        long total = totalOutcomeCount();
        if (total == 0) {
            return 0.0;
        }
        return (double) (total - successCount) / total;
    }
}
