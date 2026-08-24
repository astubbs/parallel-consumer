package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Value;

/**
 * Immutable aggregates of one closed sample window, as handed to {@link AdmissionControlLaw#onWindowClosed}.
 * <p>
 * Produced by {@link AdmissionSampleWindow#close(long, AdmissionBoundarySignals)}; the caller (the engine) decides
 * when the window's time bound has elapsed and closes it with whatever it has - possibly zero samples - passing
 * the MEASURED elapsed time and the boundary-sampled engine signals.
 * <p>
 * Two derived reads matter to the decision layer (the design's R2/R4/KTD1):
 * <ul>
 * <li>{@link #successThroughputPerSecond()} - USEFUL throughput, successes over measured elapsed time;</li>
 * <li>{@link #bindingClassification()} - whether the admission limit actually bound this window, and when it did
 * not, which starvation cause did.</li>
 * </ul>
 */
@Value
public class ClosedAdmissionWindow {

    /**
     * The KTD1 binding verdict plus, when the limit did NOT bind, the cause. Exactly one value per window: the
     * verdict is {@link #LIMIT_BOUND} or one of the three unbound causes.
     */
    public enum BindingClassification {
        /**
         * Active tasks reached the commanded target at the boundary - SLOT saturation, whatever the batch fill.
         * The only windows the elasticity estimator may learn from (R2).
         */
        LIMIT_BOUND,
        /** Not bound, no selectable work and the shard buffer effectively empty: the app ran out of work. */
        NO_WORK,
        /**
         * Not bound while buffered work was present but the shards could not yield it (the ordering-aware upper
         * bound below what dispatch asked): ordering, not the limit or absence of work, held the slots empty.
         */
        ORDERING_STARVED,
        /**
         * Not bound while the poller had paused itself for throttling - self-inflicted, so the window must never
         * be read as evidence of anything.
         */
        SELF_THROTTLED,
    }

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

    /**
     * The window's MEASURED length in nanoseconds, as the caller's clock saw it - never the nominal window
     * duration, because windows drift (an idle consumer produces one multi-second window). The denominator of
     * {@link #successThroughputPerSecond()}.
     */
    long elapsedNanos;

    /**
     * The engine signals sampled once at the boundary that closed this window - the inputs to
     * {@link #bindingClassification()} and {@link #isOffsetBackPressure()}.
     */
    AdmissionBoundarySignals boundarySignals;

    public long totalOutcomeCount() {
        return successCount + ignoreCount + overloadDropCount;
    }

    /**
     * USEFUL throughput: successes per second of MEASURED elapsed time (the design's R4). {@link #ignoreCount}
     * and {@link #overloadDropCount} are completions but deliberately excluded from the numerator - rate-limit
     * rejections land in those counters, so a total-outcome rate stays high exactly when useful throughput
     * collapses, which is the one thing a throughput objective exists to see (Finding 2). Zero when no time was
     * measured.
     */
    public double successThroughputPerSecond() {
        if (elapsedNanos <= 0) {
            return 0.0;
        }
        return successCount * 1e9 / elapsedNanos;
    }

    /**
     * The KTD1 verdict from the boundary signals. Limit-bound iff active tasks reached the commanded target at
     * the boundary (slot saturation - achieved slots, not batch fill, so a thin-batch workload that fills every
     * slot reads BOUND, never app-limited). When not bound, the three engine signals name the cause, in
     * precedence order: a self-throttled poller wins (self-inflicted starvation masks the others), then work
     * presence separates {@link BindingClassification#NO_WORK} from
     * {@link BindingClassification#ORDERING_STARVED}.
     */
    public BindingClassification bindingClassification() {
        if (boundarySignals.getTargetSlots() > 0
                && boundarySignals.getActiveTasks() >= boundarySignals.getTargetSlots()) {
            return BindingClassification.LIMIT_BOUND;
        }
        if (boundarySignals.isPollerSelfThrottled()) {
            return BindingClassification.SELF_THROTTLED;
        }
        if (boundarySignals.getSelectableWorkUpperBound() == 0 && boundarySignals.getBufferedShardWork() == 0) {
            return BindingClassification.NO_WORK;
        }
        return BindingClassification.ORDERING_STARVED;
    }

    /** Convenience read of {@link #bindingClassification()}'s verdict half. */
    public boolean isLimitBound() {
        return bindingClassification() == BindingClassification.LIMIT_BOUND;
    }

    /**
     * Whether ANY assigned partition was blocked by offset-encoding back-pressure at the boundary - plumbed for
     * the law's absolute brake (R8); nothing consumes it in this unit.
     */
    public boolean isOffsetBackPressure() {
        return boundarySignals.isOffsetBackPressure();
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
