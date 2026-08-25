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

    /** Nominal control-loop passes per window - the active-task sample count every produced window carries. */
    static final int NOMINAL_PASSES_PER_WINDOW = 20;

    private static final double BACKLOG_EPSILON_RECORDS = 1e-9;

    private final int batchSize;
    private final double w0Seconds;
    private double muMaxRecordsPerSecond;
    private double arrivalRatePerSecond;
    private double backlogRecords = 0.0;

    // ------------------------------------------------------------------
    // Broker-fidelity mode (off by default). The default plant emits perfectly dense, perfectly bound windows:
    // boundary activeTasks == targetSlots and pollerSelfThrottled == false on every saturated window. A real
    // broker run does neither - the 2026-08-25 AdaptiveConcurrencyComparisonIT freeze (45x SELF_THROTTLED,
    // 16x WARMUP_EXHAUSTED, target frozen at 5 for 188s) was invisible to this suite precisely because of that
    // gap. Fidelity mode models the two real-engine behaviours that produced it:
    //  - boundary-instant flicker: the ONE-INSTANT active-task sample reads empty between completions and
    //    dispatches, on a deterministic cadence (only every honestBoundaryPeriod-th boundary catches the
    //    saturated instant);
    //  - self-throttle-under-saturation: the poller pauses itself because the buffer is FULL - healthy
    //    back-pressure that coexists with saturated slots, so it must never mask binding.
    // ------------------------------------------------------------------

    /** 0 = fidelity off (every bound boundary reads honestly). */
    private int honestBoundaryPeriod = 0;
    /** Whether bound windows carry a self-paused poller (the deep-backlog steady state on a real broker). */
    private boolean selfThrottledWhenBound = false;
    /** Whether over-driving collapses throughput (quadratic service curve) - see {@link #enableCongestionCollapse}. */
    private boolean congestionCollapse = false;
    private int windowsProduced = 0;

    // ------------------------------------------------------------------
    // Outcome-mix knobs (soak/torture plan U1). Both default to the plant's original all-success behaviour,
    // so every pre-existing falsifier runs byte-identical windows. Fractional records are carried exactly the
    // way the backlog is - as doubles rounded only at the window seam - so a 0.19 fraction is honest.
    // ------------------------------------------------------------------

    /** Fraction of completed outcomes that close as IGNORE rather than success (rides nonSuccessFraction). */
    private double nonSuccessFraction = 0.0;
    /** Overload drops recorded per window while set - any non-zero fires the law's BACKOFF arm each window. */
    private long overloadDropsPerWindow = 0;

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
     * Outcome-mix knob (torture plan U1): the given fraction of each window's completed outcomes closes as
     * IGNORE rather than success, riding {@link ClosedAdmissionWindow#nonSuccessFraction()} without touching
     * the latency math or firing the BACKOFF arm - the shape the failure-fraction growth-freeze threshold is
     * tested against. Zero restores the original all-success plant.
     */
    void setNonSuccessFraction(double fraction) {
        if (fraction < 0 || fraction >= 1) {
            throw new IllegalArgumentException("nonSuccessFraction must be in [0, 1)");
        }
        this.nonSuccessFraction = fraction;
    }

    /**
     * Outcome-mix knob (torture plan U1): every window closes carrying this many overload drops while set -
     * each non-zero window fires the law's multiplicative BACKOFF arm. Zero restores the original behaviour.
     */
    void setOverloadDropsPerWindow(long drops) {
        if (drops < 0) {
            throw new IllegalArgumentException("overloadDropsPerWindow must be >= 0");
        }
        this.overloadDropsPerWindow = drops;
    }

    /**
     * Switches on broker-fidelity boundaries (see the field block above): only every
     * {@code honestBoundaryPeriod}-th saturated window's boundary INSTANT catches the slots full (the others
     * read momentarily empty - the between-completions dip), and every saturated window closes under a
     * self-paused poller when {@code selfThrottledWhenBound}. At the comparison IT's observed ratio
     * (~3 flickered : 1 honest) a period of 4 reproduces the freeze's evidence starvation exactly.
     */
    void enableBoundaryFidelity(int honestBoundaryPeriod, boolean selfThrottledWhenBound) {
        if (honestBoundaryPeriod < 1) {
            throw new IllegalArgumentException("honestBoundaryPeriod must be >= 1");
        }
        this.honestBoundaryPeriod = honestBoundaryPeriod;
        this.selfThrottledWhenBound = selfThrottledWhenBound;
    }

    /** Second-wind mode (torture plan U4): 0 = off. See {@link #enableSecondWind}. */
    private double secondWindThresholdRecords = 0;
    private double secondWindMuMax = 0;

    /**
     * Switches on the SECOND-WIND curve (the owner's local-minimum question, 2026-08-25): above
     * {@code thresholdRecords} in flight, the downstream gets BETTER - a batch-amortization threshold, a cache
     * regime, a pool tier - behaving like a fresh plant with capacity {@code reboundMuMax}
     * ({@code S = W0 * max(1, inFlight / (reboundMuMax * W0))}). Between the first knee and the threshold lies
     * the valley: throughput plateaus at the FIRST capacity while latency worsens, so a one-step probe from the
     * first knee lands in the valley, sees no gain, and restores. The law is first-knee-seeking by
     * construction; this curve is what pins that as documented, deliberate behaviour.
     */
    void enableSecondWind(double thresholdRecords, double reboundMuMax) {
        if (thresholdRecords <= 0 || reboundMuMax <= muMaxRecordsPerSecond) {
            throw new IllegalArgumentException("second wind needs a positive threshold and a BETTER capacity");
        }
        this.secondWindThresholdRecords = thresholdRecords;
        this.secondWindMuMax = reboundMuMax;
    }

    /**
     * Switches on CONGESTION-COLLAPSE fidelity: service time grows QUADRATICALLY with over-drive above the
     * knee ({@code S = W0 * (inFlight/knee)^2}), so achievable throughput above the knee is
     * {@code mu_max * knee/inFlight} - more concurrency buys measurably LESS work, log-log elasticity exactly
     * -1. This is {@code SyntheticCongestionCurve.quadratic}, the curve the 2026-08-25 comparison IT's
     * degradation phases run against a real broker; the default plant's linear curve plateaus exactly flat
     * above the knee, which makes a genuinely negative elasticity verdict (the FALL band) unreachable by
     * construction - the very dynamics the capacity-collapse falsifier exists to exercise.
     */
    void enableCongestionCollapse() {
        this.congestionCollapse = true;
    }

    /**
     * Runs one fixed one-second window under {@code targetSlots} and closes it. Advances the backlog.
     */
    ClosedAdmissionWindow produceWindow(int targetSlots) {
        windowsProduced++;
        double offeredRecords = backlogRecords + arrivalRatePerSecond; // one second of arrivals
        double inFlightRecordsAtTarget = (double) targetSlots * batchSize;
        double kneeRecords = muMaxRecordsPerSecond * w0Seconds;
        double overDriveRatio = Math.max(1.0, inFlightRecordsAtTarget / kneeRecords);
        double serviceSecondsAtTarget =
                w0Seconds * (congestionCollapse ? overDriveRatio * overDriveRatio : overDriveRatio);
        if (secondWindThresholdRecords > 0 && inFlightRecordsAtTarget >= secondWindThresholdRecords) {
            // Past the threshold the downstream behaves like a fresh plant at the rebound capacity.
            double reboundKneeRecords = secondWindMuMax * w0Seconds;
            serviceSecondsAtTarget =
                    w0Seconds * Math.max(1.0, inFlightRecordsAtTarget / reboundKneeRecords);
        }
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
        long completedOutcomes = Math.round(completedRecords);
        long ignores = Math.round(completedOutcomes * nonSuccessFraction);
        long successes = completedOutcomes - ignores;
        long drops = completedOutcomes == 0 ? 0 : overloadDropsPerWindow;
        double meanServiceTimeNanos = invocations == 0 ? 0.0 : serviceSeconds * WINDOW_NANOS;

        final AdmissionBoundarySignals signals;
        if (limitBound) {
            // Fidelity mode: the boundary INSTANT misses the saturated slots on most windows (flicker), and the
            // poller is self-paused because the buffer is full - both while the slots genuinely stay saturated.
            boolean boundaryInstantFlickered =
                    honestBoundaryPeriod > 0 && windowsProduced % honestBoundaryPeriod != 0;
            int boundaryInstantActive = boundaryInstantFlickered ? 0 : activeSlots;
            signals = new AdmissionBoundarySignals(boundaryInstantActive, targetSlots, false,
                    Math.round(backlogRecords), Math.round(backlogRecords), selfThrottledWhenBound, false);
        } else {
            signals = new AdmissionBoundarySignals(activeSlots, targetSlots, false, 0, 0, false, false);
        }

        // The plant is honest about the per-pass active-task stream in EVERY mode: the engine samples active
        // tasks once per control-loop pass, and on this plant every pass of a window reads the same level, so
        // the p90 aggregate IS activeSlots (a nominal per-window pass count stands in for the loop cadence).
        // In fidelity mode this is exactly what keeps binding decidable while the boundary instant flickers.
        return new ClosedAdmissionWindow(invocations, meanServiceTimeNanos, invocations, activeSlots, 0,
                NOMINAL_PASSES_PER_WINDOW, activeSlots,
                successes, ignores, drops, WINDOW_NANOS, signals);
    }
}
