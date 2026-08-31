package bz.stub.parallelconsumer.examples.streams.pc;
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

    private final long splitPollWaits;

    private final long wakesOnWork;

    ArmResult(final Latencies latencies,
              final long totalDrainMillis,
              final long offeredToWorkManager,
              final long acceptedByWorkManager,
              final long dispatchedToPool,
              final long completedSuccessfully,
              final long failed,
              final long splitPollWaits,
              final long wakesOnWork) {
        this.latencies = latencies;
        this.totalDrainMillis = totalDrainMillis;
        this.offeredToWorkManager = offeredToWorkManager;
        this.acceptedByWorkManager = acceptedByWorkManager;
        this.dispatchedToPool = dispatchedToPool;
        this.completedSuccessfully = completedSuccessfully;
        this.failed = failed;
        this.splitPollWaits = splitPollWaits;
        this.wakesOnWork = wakesOnWork;
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
     * <b>The wake-on-work marker.</b> How many times the StreamThread took the split poll wait - a short
     * poll, then a wait our own worker completions can end - instead of blocking in {@code poll()} for the
     * whole {@code poll.ms}.
     * <p>
     * Reported per arm because it separates two mechanisms that arrive together and would otherwise be
     * credited to each other. Under PC dispatch alone, completions cannot be drained until poll returns, so
     * the wait becomes latency charged per record; the split wait is what removes that. A PC arm reading
     * zero here is a PC arm with wake-on-work absent, and the negative control's result would then be
     * measuring something else.
     */
    long splitPollWaits() {
        return splitPollWaits;
    }

    /**
     * How many of those waits ended on a worker completion rather than on the budget running out - "did it
     * help", where {@link #splitPollWaits()} is "did it run". Many waits and no wakes is the mechanism
     * firing and never paying, which is a finding rather than a null result.
     */
    long wakesOnWork() {
        return wakesOnWork;
    }

    /**
     * A gap here is the silent-drop bug: records handed to the work manager but discarded by the epoch
     * filter register as zero work and look exactly like an idle topology.
     */
    boolean hasWorkManagerDrop() {
        return offeredToWorkManager != acceptedByWorkManager;
    }
}
