package io.confluent.parallelconsumer.examples.streams.pc;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * What one arm produced: how long its records took, and the dispatch counters proving which path they took.
 * <p>
 * The two travel together on purpose. Latencies alone cannot tell a reader whether Parallel Consumer was
 * involved, and a run where the seam quietly did nothing produces a perfectly reasonable-looking
 * distribution. Keeping the evidence attached to the measurement means the report cannot print one without
 * the other.
 *
 * @author Antony Stubbs
 */
final class ArmResult {

    private final Latencies latencies;

    private final long offeredToWorkManager;

    private final long acceptedByWorkManager;

    private final long dispatchedToPool;

    private final long completedSuccessfully;

    private final long failed;

    private final long totalDrainMillis;

    ArmResult(final Latencies latencies,
              final long totalDrainMillis,
              final long offeredToWorkManager,
              final long acceptedByWorkManager,
              final long dispatchedToPool,
              final long completedSuccessfully,
              final long failed) {
        this.latencies = latencies;
        this.totalDrainMillis = totalDrainMillis;
        this.offeredToWorkManager = offeredToWorkManager;
        this.acceptedByWorkManager = acceptedByWorkManager;
        this.dispatchedToPool = dispatchedToPool;
        this.completedSuccessfully = completedSuccessfully;
        this.failed = failed;
    }

    Latencies latencies() {
        return latencies;
    }

    /**
     * How long the whole batch took to drain, blocker included. Reported so the demo cannot be accused of
     * showing only the records that win: if PC were quicker per fast record but slower overall, this is the
     * number that would say so.
     */
    long totalDrainMillis() {
        return totalDrainMillis;
    }

    long offeredToWorkManager() {
        return offeredToWorkManager;
    }

    long acceptedByWorkManager() {
        return acceptedByWorkManager;
    }

    /**
     * <b>The dispatch marker.</b> Incremented at exactly one place in the whole codebase, the submission of
     * a work container to the worker pool, so a non-zero reading cannot be produced by any other route.
     */
    long dispatchedToPool() {
        return dispatchedToPool;
    }

    long completedSuccessfully() {
        return completedSuccessfully;
    }

    long failed() {
        return failed;
    }

    /**
     * A gap here is the silent-drop bug: records handed to the work manager but discarded by the epoch
     * filter register as zero work and look exactly like an idle topology.
     */
    boolean hasWorkManagerDrop() {
        return offeredToWorkManager != acceptedByWorkManager;
    }
}
