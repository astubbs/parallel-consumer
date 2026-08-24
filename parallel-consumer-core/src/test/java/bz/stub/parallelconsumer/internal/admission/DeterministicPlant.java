package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * A deterministic downstream-plus-queue model for the falsifier suite (the design's R14): given a commanded
 * target in slots for one fixed one-second window, it produces a fully consistent
 * {@link ClosedAdmissionWindow} - throughput, HONEST mean service time from the load model (the old law reads
 * latency, so a dishonest latency would void the control run), in-flight median, and boundary signals whose
 * limit-bound verdict follows the real {@link ClosedAdmissionWindow#bindingClassification()} arithmetic.
 * <p>
 * <b>The load model</b> (M/M/1-flavoured, deterministic - documented here because every scenario's oracle is
 * derived from it): let {@code inFlightRecords = targetSlots * batchSize} and let the knee be
 * {@code kneeRecords = mu_max * W0} (Little's Law: the in-flight level at which the downstream is exactly
 * saturated). Per-record service time is
 * <pre>
 *     S(target) = W0 * max(1, inFlightRecords / kneeRecords)
 * </pre>
 * i.e. flat at {@code W0} below the knee, growing linearly with in-flight above it - so achievable throughput
 * {@code inFlightRecords / S} rises linearly below the knee and saturates exactly at {@code mu_max} above it
 * (the graceful-saturation plateau: pushing the target higher buys queueing, never throughput).
 * <p>
 * <b>Arrivals and backlog:</b> arrival is a rate in records/s (settable per phase); un-served records
 * accumulate in a backlog. A window is <b>limit-bound</b> when more work was offered than the commanded target
 * could serve (backlog remains) - active slots then equal the target and service time follows the congested
 * curve. Otherwise the window is <b>work-limited</b>: concurrency never exceeds demand, service time is the
 * uncongested {@code W0}, and occupancy (the in-flight median) follows Little's Law,
 * {@code completed * W0 / batchSize} slots.
 * <p>
 * <b>THE ORACLE</b>: the correct target, derived from plant parameters alone and known before any run -
 * {@link #optimalTargetSlots()} {@code = mu_max * W0 / batchSize}. Dimensionally the design's
 * {@code L* = mu_max * W0} is in-flight RECORDS; the target is in SLOTS of one batch each, so the batchSize
 * division lives here at the seam and nowhere else (the design's "oracle's units" note - at batchSize 1 the
 * mismatch is invisible, which is exactly how it would ship wrong, so the sweep runs one arm above 1).
 */
final class DeterministicPlant {

    /** Fixed nominal window length: the plant closes exact one-second windows. */
    static final long WINDOW_NANOS = 1_000_000_000L;

    private static final double BACKLOG_EPSILON_RECORDS = 1e-9;

    private final int batchSize;
    private final double w0Seconds;
    private double muMaxRecordsPerSecond;
    private double arrivalRatePerSecond;
    private double backlogRecords = 0.0;

    DeterministicPlant(double muMaxRecordsPerSecond, double w0Seconds, int batchSize) {
        if (muMaxRecordsPerSecond <= 0 || w0Seconds <= 0 || batchSize < 1) {
            throw new IllegalArgumentException("plant parameters must be positive");
        }
        this.muMaxRecordsPerSecond = muMaxRecordsPerSecond;
        this.w0Seconds = w0Seconds;
        this.batchSize = batchSize;
    }

    /**
     * The oracle: the correct target in SLOTS, {@code L*_slots = mu_max * W0 / batchSize}. See the class
     * javadoc for the units seam.
     */
    double optimalTargetSlots() {
        return muMaxRecordsPerSecond * w0Seconds / batchSize;
    }

    double getMuMaxRecordsPerSecond() {
        return muMaxRecordsPerSecond;
    }

    double uncongestedServiceTimeNanos() {
        return w0Seconds * WINDOW_NANOS;
    }

    double getBacklogRecords() {
        return backlogRecords;
    }

    void setArrivalRatePerSecond(double arrivalRatePerSecond) {
        this.arrivalRatePerSecond = arrivalRatePerSecond;
    }

    /** Capacity change mid-run (rebalance-shrink / metamorphic scenarios); moves the oracle with it. */
    void setMuMaxRecordsPerSecond(double muMaxRecordsPerSecond) {
        this.muMaxRecordsPerSecond = muMaxRecordsPerSecond;
    }

    /**
     * Runs one fixed one-second window under {@code targetSlots} and closes it. Advances the backlog.
     */
    ClosedAdmissionWindow produceWindow(int targetSlots) {
        double offeredRecords = backlogRecords + arrivalRatePerSecond; // one second of arrivals
        double inFlightRecordsAtTarget = (double) targetSlots * batchSize;
        double kneeRecords = muMaxRecordsPerSecond * w0Seconds;
        double serviceSecondsAtTarget = w0Seconds * Math.max(1.0, inFlightRecordsAtTarget / kneeRecords);
        double achievableThroughput = targetSlots == 0 ? 0.0 : inFlightRecordsAtTarget / serviceSecondsAtTarget;

        double completedRecords = Math.min(offeredRecords, achievableThroughput);
        backlogRecords = offeredRecords - completedRecords;
        boolean limitBound = backlogRecords > BACKLOG_EPSILON_RECORDS;

        final int activeSlots;
        final double serviceSeconds;
        if (limitBound) {
            // Slot saturation: every commanded slot busy, service time from the congested curve.
            activeSlots = targetSlots;
            serviceSeconds = serviceSecondsAtTarget;
        } else {
            // Work-limited: completed <= mu_max, so occupancy sits at or below the knee - uncongested.
            serviceSeconds = w0Seconds;
            activeSlots = Math.min(targetSlots, (int) Math.ceil(completedRecords * w0Seconds / batchSize));
        }

        int invocations = (int) Math.round(completedRecords / batchSize);
        long successes = Math.round(completedRecords);
        double meanServiceTimeNanos = invocations == 0 ? 0.0 : serviceSeconds * WINDOW_NANOS;

        AdmissionBoundarySignals signals = limitBound
                ? new AdmissionBoundarySignals(activeSlots, targetSlots, false,
                Math.round(backlogRecords), Math.round(backlogRecords), false, false)
                : new AdmissionBoundarySignals(activeSlots, targetSlots, false, 0, 0, false, false);

        return new ClosedAdmissionWindow(invocations, meanServiceTimeNanos, invocations, activeSlots, 0,
                successes, 0, 0, WINDOW_NANOS, signals);
    }
}
