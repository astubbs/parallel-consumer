package bz.stub.parallelconsumer.internal.admission;

/*-
 * Portions copyright 2018 Netflix, Inc. - from Netflix/concurrency-limits (Apache-2.0), class Gradient2Limit
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 *
 * This file contains modified code derived from the Apache-2.0 licensed
 * Netflix/concurrency-limits project (https://github.com/Netflix/concurrency-limits).
 */

/**
 * The decision layer of the adaptive admission (target concurrency) control law: given one closed sample window,
 * produce a new admission limit and the {@link AdmissionDecisionReason reason} for it.
 * <p>
 * The core adaptation is the Gradient2 algorithm ported from
 * {@code com.netflix.concurrency.limits.limit.Gradient2Limit} (including the Netflix PR-88 anti-drift fixes: the
 * short-term signal is the raw, unsmoothed window mean, and the long-term EWMA baseline is decayed by
 * {@link #LONG_BASELINE_DECAY} whenever it exceeds {@link #LONG_BASELINE_DRIFT_RATIO} times the short signal):
 *
 * <pre>
 *     gradient = clamp(tolerance * longServiceTime / shortServiceTime, 0.5, 1.0)
 *     target   = limit * gradient + queueHeadroom
 *     limit    = limit * (1 - smoothing) + target * smoothing     // then clamp to [floor, ceiling]
 * </pre>
 *
 * The multiplicative backoff arm follows {@code AIMDLimit} semantics (ratio {@link #AIMD_BACKOFF_RATIO}), and the
 * minimum-samples hold follows {@code WindowedLimit}'s window-size guard - both from the same Netflix project.
 * <p>
 * Deliberate deviations from upstream, in the precedence order the arms are evaluated (first match wins):
 * <ol>
 * <li><b>APP_LIMITED (starving for samples)</b> - too few service-time samples to judge; hold.</li>
 * <li><b>BACKOFF</b> - any overload drop fires one multiplicative cut for the whole window.</li>
 * <li><b>FAILURE_LIMITED</b> - non-success fraction above {@link #FAILURE_FRACTION_GROWTH_FREEZE_THRESHOLD}
 * freezes growth (gradient may still contract). A fast-rejecting overloaded downstream LOWERS measured service
 * time; without this arm the gradient reads overload as headroom and grows without bound.</li>
 * <li><b>PROBING (up)</b> - a starvation signature (in-flight median far below the limit, spread NOT bimodal,
 * latency flat) while below the ceiling takes one bounded step up. A contraction narrows the pipeline buffer and
 * manufactures its own starvation evidence; this keeps the ratchet from locking. A bimodal in-flight spread does
 * NOT classify as starved.</li>
 * <li><b>APP_LIMITED (idle)</b> - in-flight MEDIAN below half the limit holds the limit. Upstream's
 * {@code WindowedLimit} feeds max-in-flight here; the median is a deliberate deviation, because a max reads
 * healthy when the system is starved.</li>
 * <li><b>PROBING (down)</b> - a bounded periodic re-measure probe: while pinned at the ceiling with flat latency
 * (or while a previous probe proved the baseline contaminated), step down every
 * {@link #DEFAULT_PROBE_DOWN_CADENCE_WINDOWS} windows and compare. A law that starts already saturated has no
 * clean baseline - the long EWMA warms up on degraded latency, the gradient reads flat-degraded as healthy, and
 * the limit pins at the cap forever (proven by the contaminated-baseline gate test). When a probe improves
 * latency by at least the {@link #PROBE_DOWN_IMPROVEMENT_RATIO} factor, the baseline is snapped down to the
 * measured value and probing continues on cadence - no longer gated on flat latency, since the regrowth between
 * probes lifts latency out of the flat band - until a probe stops helping.</li>
 * <li><b>ADAPTING / AT_CAP / AT_FLOOR</b> - the Gradient2 update above.</li>
 * </ol>
 * <p>
 * PURE MATH ONLY: no clock, no threads, no metrics - every update is an explicit {@link #onWindowClosed} call, and
 * all time values arrive as injected nanosecond quantities inside the window. Single-threaded by design.
 */
public final class AdmissionControlLaw {

    // ------------------------------------------------------------------
    // Named constants - package-visible so tests can assert against them.
    // ------------------------------------------------------------------

    /**
     * Hard floor: the limit never drops below one slot, or progress stops entirely.
     */
    static final int LIMIT_FLOOR_SLOTS = 1;

    /**
     * Minimum service-time samples a window must carry before the law will act on it (from
     * {@code WindowedLimit}'s default window size).
     */
    static final int DEFAULT_MIN_SAMPLES_PER_WINDOW = 10;

    /**
     * Non-success outcome fraction above which growth is frozen for the window.
     */
    static final double FAILURE_FRACTION_GROWTH_FREEZE_THRESHOLD = 0.2;

    /**
     * Multiplicative backoff applied when the window saw overload drops (from {@code AIMDLimit}'s default).
     */
    static final double AIMD_BACKOFF_RATIO = 0.9;

    /**
     * Size of the bounded starvation probe: one slot up per starved window.
     */
    static final int PROBE_UP_STEP_SLOTS = 1;

    /**
     * In-flight median below this fraction of the limit reads as "far below" for the starvation signature.
     */
    static final double STARVED_MEDIAN_FRACTION_OF_LIMIT = 0.25;

    /**
     * In-flight spread (p90 - p10) above this fraction of the limit is the bimodal signature and vetoes the
     * starvation classification.
     */
    static final double STARVED_SPREAD_MAX_FRACTION_OF_LIMIT = 0.25;

    /**
     * Upstream Gradient2's app-limited guard threshold: in-flight below half the limit means the application, not
     * the downstream, is the bottleneck - never grow on such a window.
     */
    static final double APP_LIMITED_MEDIAN_FRACTION_OF_LIMIT = 0.5;

    /**
     * The gradient is never allowed below this, to avoid aggressive shedding on outliers (upstream Gradient2).
     */
    static final double GRADIENT_FLOOR = 0.5;

    /**
     * The gradient is never allowed above this: growth comes only from the additive queue headroom (upstream
     * Gradient2).
     */
    static final double GRADIENT_CEILING = 1.0;

    /**
     * Smoothing factor on the limit update (upstream Gradient2 default).
     */
    static final double DEFAULT_SMOOTHING = 0.2;

    /**
     * Constant additive queue headroom - the only growth term in steady state (upstream Gradient2 default).
     */
    static final int DEFAULT_QUEUE_HEADROOM_SLOTS = 4;

    /**
     * Tolerated ratio of short to long service time before the gradient contracts (upstream Gradient2's
     * rttTolerance default).
     */
    static final double DEFAULT_SERVICE_TIME_TOLERANCE = 1.5;

    /**
     * Span of the long-term service-time EWMA, in windows (upstream Gradient2 default).
     */
    static final int DEFAULT_LONG_BASELINE_WINDOW = 600;

    /**
     * Arithmetic-mean warmup length of the long-term EWMA, in windows (upstream Gradient2 default).
     */
    static final int DEFAULT_LONG_BASELINE_WARMUP_WINDOW = 10;

    /**
     * PR-88 anti-drift: when the long baseline exceeds this multiple of the short signal, decay it.
     */
    static final double LONG_BASELINE_DRIFT_RATIO = 2.0;

    /**
     * PR-88 anti-drift: the per-window decay applied to a stale-high long baseline.
     */
    static final double LONG_BASELINE_DECAY = 0.95;

    /**
     * Latency counts as "flat" when the short signal is within this relative tolerance of the long baseline.
     */
    static final double LATENCY_FLAT_TOLERANCE = 0.1;

    /**
     * Size of the bounded probe-down step (multiplicative, like the AIMD arm but gentler in intent - it is a
     * re-measure, not a punishment).
     */
    static final double DEFAULT_PROBE_DOWN_RATIO = 0.9;

    /**
     * Cadence of the probe-down re-measure, in windows.
     */
    static final int DEFAULT_PROBE_DOWN_CADENCE_WINDOWS = 5;

    /**
     * A probe-down "improved" latency when the following window's short signal is at most this fraction of the
     * pre-probe short signal - evidence the baseline was contaminated by prior saturation.
     */
    static final double PROBE_DOWN_IMPROVEMENT_RATIO = 0.9;

    /**
     * Zero-latency guard: a window mean of zero (or negative, defensively) is clamped to this before entering the
     * gradient, whose division would otherwise produce NaN/Infinity. Added for the "clamps" scenario of the test
     * suite - upstream has no such guard because its RTTs come from a monotonic clock.
     */
    static final double MIN_SERVICE_TIME_NANOS = 1.0;

    // ------------------------------------------------------------------
    // Configuration (immutable)
    // ------------------------------------------------------------------

    private final int ceiling;
    private final double smoothing;
    private final int queueHeadroomSlots;
    private final double serviceTimeTolerance;
    private final int minSamplesPerWindow;
    private final double probeDownRatio;
    /** Zero disables the probe-down arm entirely (used by the gate test to demonstrate the ported law pins). */
    private final int probeDownCadenceWindows;

    // ------------------------------------------------------------------
    // State
    // ------------------------------------------------------------------

    /**
     * Estimated admission limit, kept fractional so sub-slot adjustments accumulate (as upstream does).
     */
    private double estimatedLimit;

    /**
     * Long-term, less volatile service-time baseline. Under load it trends higher; the drift decay and the
     * probe-down snap pull it back down.
     */
    private final ServiceTimeExpAvg longServiceTime;

    /**
     * The long-term baseline value that the LAST evaluated window's gradient was actually computed against, in
     * nanoseconds - i.e. {@code longTime} as {@link #onWindowClosed} sampled it, before any anti-drift decay or
     * probe-down snap applied to {@link #longServiceTime} for the NEXT window.
     * <p>
     * Purely observational: written and never read by the law itself, so it changes no computation. It exists
     * because the comparison the law makes - short-term against long-run - is the number that decides everything
     * and was invisible from outside, which made the baseline ratchet undiagnosable from the log alone.
     * {@link #getServiceTimeBaselineNanos()} answers a different question (where the baseline is NOW), and reading
     * it after the fact would misreport a window in which the decay fired.
     * <p>
     * Zero until a window has been evaluated far enough to compute it - a window held by the minimum-samples arm
     * never reaches the gradient, and deliberately leaves this untouched along with the rest of the state.
     */
    private double lastWindowBaselineNanos = 0.0;

    private int windowsSinceProbeDown = 0;
    private boolean awaitingProbeDownResult = false;
    private double preProbeShortTimeNanos = 0.0;

    /**
     * True while the last probe-down improved latency - the baseline was contaminated, so probing continues on
     * cadence even though the limit is no longer at the ceiling.
     */
    private boolean baselineRecoveryMode = false;

    private AdmissionControlLaw(Builder builder) {
        if (builder.ceiling < LIMIT_FLOOR_SLOTS) {
            throw new IllegalArgumentException("ceiling must be >= " + LIMIT_FLOOR_SLOTS);
        }
        if (builder.initialLimit < LIMIT_FLOOR_SLOTS || builder.initialLimit > builder.ceiling) {
            throw new IllegalArgumentException("initialLimit must be within [floor, ceiling]");
        }
        this.ceiling = builder.ceiling;
        this.smoothing = builder.smoothing;
        this.queueHeadroomSlots = builder.queueHeadroomSlots;
        this.serviceTimeTolerance = builder.serviceTimeTolerance;
        this.minSamplesPerWindow = builder.minSamplesPerWindow;
        this.probeDownRatio = builder.probeDownRatio;
        this.probeDownCadenceWindows = builder.probeDownCadenceWindows;
        this.estimatedLimit = builder.initialLimit;
        this.longServiceTime = new ServiceTimeExpAvg(builder.longBaselineWindow, builder.longBaselineWarmupWindow);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Evaluates one closed window and moves the limit. See the class javadoc for the arm precedence.
     */
    public AdmissionDecision onWindowClosed(ClosedAdmissionWindow window) {
        final double limit = estimatedLimit;

        // Arm 1: not enough samples to judge - hold, and leave ALL state (baseline, probe bookkeeping) untouched:
        // a signal-free window must not advance the probe cadence nor contaminate the baseline.
        if (window.getSampleCount() < minSamplesPerWindow) {
            return decide(limit, AdmissionDecisionReason.APP_LIMITED);
        }

        // Unsmoothed short signal (PR-88): the window mean feeds the gradient directly.
        // MIN_SERVICE_TIME_NANOS is the zero-latency guard (see its javadoc).
        final double shortTime = Math.max(MIN_SERVICE_TIME_NANOS, window.getMeanServiceTimeNanos());
        final double longTime = longServiceTime.add(shortTime);
        this.lastWindowBaselineNanos = longTime; // observation only - see the field's javadoc

        // PR-88 anti-drift: a long baseline left stale-high after a load episode is decayed toward reality.
        // As upstream, the decayed value takes effect from the NEXT window; this window uses longTime as sampled.
        if (longTime / shortTime > LONG_BASELINE_DRIFT_RATIO) {
            longServiceTime.update(current -> current * LONG_BASELINE_DECAY);
        }

        final boolean latencyFlat = shortTime >= longTime * (1 - LATENCY_FLAT_TOLERANCE)
                && shortTime <= longTime * (1 + LATENCY_FLAT_TOLERANCE);

        windowsSinceProbeDown++;

        // Evaluate the previous probe-down, if one is pending: did running lower actually run faster?
        if (awaitingProbeDownResult) {
            awaitingProbeDownResult = false;
            boolean improved = shortTime <= preProbeShortTimeNanos * PROBE_DOWN_IMPROVEMENT_RATIO;
            baselineRecoveryMode = improved;
            if (improved) {
                // The baseline was contaminated by saturation: snap it down to the measured value so the
                // gradient can see queueing again (cited scenario: contaminated-baseline gate test).
                final double measured = shortTime;
                longServiceTime.update(current -> Math.min(current, measured));
            }
        }

        // Arm 2: overload drops - one AIMD cut per window, however many drops it carried.
        if (window.getOverloadDropCount() > 0) {
            return decide(Math.max(LIMIT_FLOOR_SLOTS, limit * AIMD_BACKOFF_RATIO),
                    AdmissionDecisionReason.BACKOFF);
        }

        final double gradientTarget = gradientUpdate(limit, shortTime, longTime);

        // Arm 3: failure fraction freezes growth - contract-only gradient.
        if (window.nonSuccessFraction() > FAILURE_FRACTION_GROWTH_FREEZE_THRESHOLD) {
            double frozen = Math.min(limit, gradientTarget);
            return decide(Math.max(LIMIT_FLOOR_SLOTS, frozen), AdmissionDecisionReason.FAILURE_LIMITED);
        }

        // Arm 4: starvation signature - bounded one-window probe UP. A bimodal spread vetoes.
        boolean starved = window.getInFlightMedian() < limit * STARVED_MEDIAN_FRACTION_OF_LIMIT
                && window.getInFlightSpread() <= limit * STARVED_SPREAD_MAX_FRACTION_OF_LIMIT
                && latencyFlat
                && limit < ceiling;
        if (starved) {
            return decide(Math.min(ceiling, limit + PROBE_UP_STEP_SLOTS), AdmissionDecisionReason.PROBING);
        }

        // Arm 5: app-limited - the application is not filling half the limit, so the window says nothing about
        // the downstream's capacity. MEDIAN, not upstream's max: a max reads healthy when starved.
        if (window.getInFlightMedian() < limit * APP_LIMITED_MEDIAN_FRACTION_OF_LIMIT) {
            return decide(limit, AdmissionDecisionReason.APP_LIMITED);
        }

        // Arm 6: bounded periodic probe DOWN to re-measure a possibly contaminated baseline.
        if (probeDownDue(latencyFlat, limit)) {
            windowsSinceProbeDown = 0;
            awaitingProbeDownResult = true;
            preProbeShortTimeNanos = shortTime;
            return decide(Math.max(LIMIT_FLOOR_SLOTS, limit * probeDownRatio), AdmissionDecisionReason.PROBING);
        }

        // Arm 7: the Gradient2 update itself.
        double clamped = Math.max(LIMIT_FLOOR_SLOTS, Math.min(ceiling, gradientTarget));
        AdmissionDecisionReason reason = AdmissionDecisionReason.ADAPTING;
        if (gradientTarget > ceiling) {
            reason = AdmissionDecisionReason.AT_CAP;
        } else if (gradientTarget < LIMIT_FLOOR_SLOTS) {
            reason = AdmissionDecisionReason.AT_FLOOR;
        }
        return decide(clamped, reason);
    }

    /**
     * The ported Gradient2 core: gradient clamped to [{@link #GRADIENT_FLOOR}, {@link #GRADIENT_CEILING}],
     * additive queue headroom, then smoothing against the current limit. Unclamped to [floor, ceiling] - callers
     * clamp (or freeze) per arm.
     */
    private double gradientUpdate(double limit, double shortTime, double longTime) {
        double gradient = Math.max(GRADIENT_FLOOR,
                Math.min(GRADIENT_CEILING, serviceTimeTolerance * longTime / shortTime));
        double newLimit = limit * gradient + queueHeadroomSlots;
        return limit * (1 - smoothing) + newLimit * smoothing;
    }

    private boolean probeDownDue(boolean latencyFlat, double limit) {
        if (probeDownCadenceWindows <= 0) {
            return false; // probe-down disabled
        }
        if (windowsSinceProbeDown < probeDownCadenceWindows) {
            return false;
        }
        // Flat latency gates only the INITIAL at-cap trigger. Once a probe has proven the baseline contaminated
        // (recovery mode), each probe SNAPS the baseline down and the regrowth between probes lifts latency back
        // out of the flat band - gating recovery probes on flatness would end the descent after one step (this
        // exact stall was observed in the contaminated-baseline gate scenario). Recovery mode is bounded by its
        // own evidence instead: it ends the first time a probe fails to improve latency.
        boolean atCap = limit >= ceiling;
        return baselineRecoveryMode || (latencyFlat && atCap);
    }

    private AdmissionDecision decide(double newEstimatedLimit, AdmissionDecisionReason reason) {
        this.estimatedLimit = newEstimatedLimit;
        return new AdmissionDecision((int) newEstimatedLimit, reason);
    }

    /**
     * Current admission limit in whole slots.
     */
    public int getLimit() {
        return (int) estimatedLimit;
    }

    /**
     * The fractional estimated limit - exposed for tests and future metrics; sub-slot growth accumulates here.
     */
    public double getEstimatedLimit() {
        return estimatedLimit;
    }

    /**
     * The long-term service-time baseline in nanoseconds (upstream's "RTT no-load") - exposed for tests and
     * future metrics.
     */
    public double getServiceTimeBaselineNanos() {
        return longServiceTime.get();
    }

    /**
     * The long-run baseline the LAST evaluated window's short-term service time was compared against, in
     * nanoseconds - see {@link #lastWindowBaselineNanos}. Read-only; zero before any window has reached the
     * gradient.
     */
    public double getLastWindowServiceTimeBaselineNanos() {
        return lastWindowBaselineNanos;
    }

    /**
     * The tolerated short/long service-time ratio: the threshold the comparison is actually tested against.
     * <p>
     * The gradient is {@code clamp(tolerance * long / short, 0.5, 1.0)}, so it stops being 1.0 - and the limit
     * therefore stops growing and starts contracting - exactly when {@code short / long} rises above this value.
     * Reported alongside the ratio so a reader can see which side of the line a window fell on.
     */
    public double getServiceTimeTolerance() {
        return serviceTimeTolerance;
    }

    /**
     * Configuration for {@link AdmissionControlLaw}; defaults mirror upstream Gradient2Limit where a counterpart
     * exists. Ported and adapted from {@code Gradient2Limit.Builder}.
     */
    public static final class Builder {
        private int initialLimit = 20;
        private int ceiling = 100;
        private double smoothing = DEFAULT_SMOOTHING;
        private int queueHeadroomSlots = DEFAULT_QUEUE_HEADROOM_SLOTS;
        private double serviceTimeTolerance = DEFAULT_SERVICE_TIME_TOLERANCE;
        private int minSamplesPerWindow = DEFAULT_MIN_SAMPLES_PER_WINDOW;
        private int longBaselineWindow = DEFAULT_LONG_BASELINE_WINDOW;
        private int longBaselineWarmupWindow = DEFAULT_LONG_BASELINE_WARMUP_WINDOW;
        private double probeDownRatio = DEFAULT_PROBE_DOWN_RATIO;
        private int probeDownCadenceWindows = DEFAULT_PROBE_DOWN_CADENCE_WINDOWS;

        public Builder initialLimit(int initialLimit) {
            this.initialLimit = initialLimit;
            return this;
        }

        /**
         * Maximum allowable admission limit (upstream's maxConcurrency).
         */
        public Builder ceiling(int ceiling) {
            this.ceiling = ceiling;
            return this;
        }

        public Builder smoothing(double smoothing) {
            this.smoothing = smoothing;
            return this;
        }

        /**
         * Fixed amount the limit can grow while service times remain low (upstream's queueSize).
         */
        public Builder queueHeadroomSlots(int queueHeadroomSlots) {
            this.queueHeadroomSlots = queueHeadroomSlots;
            return this;
        }

        /**
         * Tolerated short/long service-time ratio before contraction (upstream's rttTolerance, must be >= 1.0).
         */
        public Builder serviceTimeTolerance(double serviceTimeTolerance) {
            if (serviceTimeTolerance < 1.0) {
                throw new IllegalArgumentException("serviceTimeTolerance must be >= 1.0");
            }
            this.serviceTimeTolerance = serviceTimeTolerance;
            return this;
        }

        public Builder minSamplesPerWindow(int minSamplesPerWindow) {
            this.minSamplesPerWindow = minSamplesPerWindow;
            return this;
        }

        public Builder longBaselineWindow(int longBaselineWindow) {
            this.longBaselineWindow = longBaselineWindow;
            return this;
        }

        public Builder longBaselineWarmupWindow(int longBaselineWarmupWindow) {
            this.longBaselineWarmupWindow = longBaselineWarmupWindow;
            return this;
        }

        public Builder probeDownRatio(double probeDownRatio) {
            if (probeDownRatio <= 0.0 || probeDownRatio >= 1.0) {
                throw new IllegalArgumentException("probeDownRatio must be in (0, 1)");
            }
            this.probeDownRatio = probeDownRatio;
            return this;
        }

        /**
         * How often the re-measure probe-down may fire, in windows; zero disables the probe-down arm.
         */
        public Builder probeDownCadenceWindows(int probeDownCadenceWindows) {
            this.probeDownCadenceWindows = probeDownCadenceWindows;
            return this;
        }

        public AdmissionControlLaw build() {
            return new AdmissionControlLaw(this);
        }
    }
}
