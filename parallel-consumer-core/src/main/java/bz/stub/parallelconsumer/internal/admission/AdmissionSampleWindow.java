package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Single accumulation owner for one sampling window of admission telemetry - the engine holds exactly one instance
 * and closes it on the window's TIME BOUND (the caller decides when; this class holds no clock).
 * <p>
 * Accepts three kinds of input:
 * <ul>
 * <li>service-time samples - one per user-function invocation, already fill-normalized by the caller (batch
 * normalization is the CALLER's job; this window never sees fill counts);</li>
 * <li>in-flight snapshots (ints) - reduced at close to their MEDIAN and SPREAD, where spread is the p90 - p10
 * distance (chosen over IQR for a stronger bimodality signature: a distribution split between near-idle and
 * near-limit shows a p90 - p10 close to the full range, which the decision layer must not read as starvation);</li>
 * <li>outcome counts - success / ignore / overloadDrop.</li>
 * </ul>
 * {@link #close(long, AdmissionBoundarySignals)} returns the immutable aggregates with whatever has accumulated -
 * possibly nothing - and resets this instance for the next window, so the engine can keep one long-lived instance;
 * {@link #discard()} resets without producing aggregates.
 * <p>
 * Percentile convention: nearest-rank on the sorted snapshots, index {@code round(q * (n - 1))}.
 * <p>
 * Semantics informed by {@code com.netflix.concurrency.limits.limit.WindowedLimit} and its sample windows, but this
 * is a fresh implementation: it deliberately aggregates the in-flight DISTRIBUTION (median and spread) rather than
 * upstream's max-in-flight, because a max reads healthy when the system is starved.
 * <p>
 * Single-threaded by design; the engine owns synchronization if it needs any.
 */
public class AdmissionSampleWindow {

    private static final double P10 = 0.10;
    private static final double P50 = 0.50;
    private static final double P90 = 0.90;

    /**
     * Kept as a double sum: extreme (near {@code Long.MAX_VALUE}) samples would overflow a long sum, and the
     * decision layer only ever consumes the mean, where double precision suffices. Guarded by the
     * extreme-latency scenario in the test suite.
     */
    private double serviceTimeSumNanos = 0.0;

    private int serviceTimeSampleCount = 0;

    private final List<Integer> inFlightSamples = new ArrayList<>();

    private long successCount = 0;
    private long ignoreCount = 0;
    private long overloadDropCount = 0;

    /**
     * Records one user-function invocation's service time. The value must already be fill-normalized (per-record)
     * by the caller if the invocation carried a batch.
     */
    public void addServiceTimeSample(long serviceTimeNanos) {
        serviceTimeSumNanos += serviceTimeNanos;
        serviceTimeSampleCount++;
    }

    /**
     * Records a snapshot of how many invocations were in flight.
     */
    public void addInFlightSample(int inFlight) {
        inFlightSamples.add(inFlight);
    }

    public void recordSuccess() {
        successCount++;
    }

    public void recordIgnore() {
        ignoreCount++;
    }

    public void recordOverloadDrop() {
        overloadDropCount++;
    }

    /**
     * Number of service-time samples accumulated so far - exposed so the decision layer (via the engine) can hold
     * below a minimum.
     */
    public int getSampleCount() {
        return serviceTimeSampleCount;
    }

    /**
     * Closes the window on its time bound with whatever it has, and resets this instance for the next window.
     *
     * @param elapsedNanos    the window's MEASURED length on the caller's clock - never the nominal duration,
     *                        because windows drift (the design's R4: throughput needs the actual denominator)
     * @param boundarySignals the engine signals the caller sampled ONCE at this boundary
     */
    public ClosedAdmissionWindow close(long elapsedNanos, AdmissionBoundarySignals boundarySignals) {
        double mean = serviceTimeSampleCount == 0 ? 0.0 : serviceTimeSumNanos / serviceTimeSampleCount;

        int median = 0;
        int spread = 0;
        int inFlightCount = inFlightSamples.size();
        if (inFlightCount > 0) {
            Collections.sort(inFlightSamples);
            median = percentile(P50);
            spread = percentile(P90) - percentile(P10);
        }

        ClosedAdmissionWindow closed = new ClosedAdmissionWindow(serviceTimeSampleCount, mean,
                inFlightCount, median, spread,
                successCount, ignoreCount, overloadDropCount,
                elapsedNanos, boundarySignals);
        reset();
        return closed;
    }

    /**
     * Drops everything accumulated and resets for the next window WITHOUT producing aggregates - the discard the
     * engine's pause-poison and rebalance paths need, where the samples describe a window that must not be read.
     * Discarding produces no {@link ClosedAdmissionWindow}, so it needs no elapsed time and no boundary sample.
     */
    public void discard() {
        reset();
    }

    /**
     * Nearest-rank percentile over the (already sorted) in-flight snapshots: index {@code round(q * (n - 1))}.
     */
    private int percentile(double quantile) {
        int index = (int) Math.round(quantile * (inFlightSamples.size() - 1));
        return inFlightSamples.get(index);
    }

    private void reset() {
        serviceTimeSumNanos = 0.0;
        serviceTimeSampleCount = 0;
        inFlightSamples.clear();
        successCount = 0;
        ignoreCount = 0;
        overloadDropCount = 0;
    }
}
