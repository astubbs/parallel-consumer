package bz.stub.parallelconsumer.internal.admission;

/*-
 * Portions copyright 2018 Netflix, Inc. - from Netflix/concurrency-limits (Apache-2.0): the AIMD backoff arm
 * follows AIMDLimit's semantics and the minimum-samples window guard follows WindowedLimit's.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;

/**
 * The decision layer of the adaptive admission (target concurrency) control law: given one closed sample window,
 * produce a new admission limit and the {@link AdmissionDecisionReason reason} for it.
 * <p>
 * <b>This is the band machine of the 2026-08-24-003 design</b> (R1, R3, R5, R7, R8, KTD2, KTD6), replacing the
 * six-arm Gradient2 port in one move. One statistic - the {@link AdmissionElasticityEstimator elasticity} of
 * useful throughput against concurrency - is read as three bands, and the law contains <b>no learned latency
 * reference of any kind</b> (R8): the long-latency EWMA baseline, the anti-drift decay, the probe-down arm, the
 * latency gradient, the starvation probe-up and the in-flight-median hold arm are all deleted with the port. The
 * window still carries service time for observability; this class never reads it.
 * <p>
 * Precedence per window close (first gate whose condition holds wins):
 * <ol>
 * <li><b>Adjudication gate</b> - fewer than {@link #DEFAULT_MIN_SAMPLES_PER_WINDOW} samples: hold, reason
 * {@code INSUFFICIENT_SIGNAL}, ALL state untouched, window never offered to the estimator.</li>
 * <li><b>Absolute brakes</b> - any overload drop: one multiplicative {@link #AIMD_BACKOFF_RATIO} cut, reason
 * {@code BACKOFF}; non-success fraction above {@link #FAILURE_FRACTION_GROWTH_FREEZE_THRESHOLD}: growth frozen
 * (the band machine takes no decision on a failure-poisoned window, so contract-only degenerates to hold), reason
 * {@code FAILURE_LIMITED}; offset-encoding back-pressure at the boundary: hold, never grow, reason
 * {@code OFFSET_BACK_PRESSURE} (R8 - a partition refusing records makes growth meaningless). Braked windows are
 * not offered to the estimator: their evidence is polluted by the very condition that fired the brake.</li>
 * <li><b>Binding gate</b> (R5) - the limit did not bind: PRESERVE, bit-identical, reason = the window's
 * {@link ClosedAdmissionWindow.BindingClassification starvation cause} ({@code NO_WORK} /
 * {@code ORDERING_STARVED} / {@code SELF_THROTTLED}). Never decayed (RFC 7661: absence of data yields no
 * decision), and never offered - the estimator refuses unbound windows anyway (R2), but the law does not lean on
 * that.</li>
 * <li><b>Offer</b> - a bound, adjudicated, un-braked window enters the estimator's history.</li>
 * <li><b>Bands</b> from the estimator's verdict - see below.</li>
 * </ol>
 * <p>
 * <b>The bands, and the two dynamics rules the falsifier suite forced</b> (both are the KTD6 re-derivation of the
 * accelerator's dynamics that the design deferred - the plan's Open Question 4 - carried out far enough to make
 * the R14 falsifiers green; the numbers cited are from the deterministic falsifier plant, knee at 20 slots):
 * <ul>
 * <li><b>WARMUP</b> (no verdict in force, R3/KTD2): binding alone licenses additive growth of
 * {@code q = sqrt(limit)} per window, cumulatively capped at {@link #DEFAULT_WARMUP_ALLOWANCE_SLOTS} slots per
 * episode; at the cap, {@code WARMUP_EXHAUSTED} preserves. The allowance is denominated in SLOTS, not q-steps
 * (deviation from KTD2's "8 steps of q", documented in the U5 audit): eight sqrt-steps from the knee is a 3x
 * blind overshoot that fails the graceful-saturation plateau falsifier outright, and the sparse-adjudication
 * falsifier's allowance constant was always stated in slots. Acting on a live verdict resets the episode.</li>
 * <li><b>RISE</b> (elasticity above the threshold): one accelerator step {@code +q}, but <b>only at the settle
 * cadence</b> - at least {@link #DEFAULT_SETTLE_WINDOWS} offered windows since the last movement. Between steps
 * the law holds (reason {@code PLATEAU}). Without the cadence, the whole-history regression is still dominated
 * by the below-knee climb when the knee is crossed, and sqrt-steps compound to the ceiling before the slope can
 * answer (measured: 9 -&gt; 100 in 14 windows against a knee of 20). The cadence, together with the law's
 * {@link #DEFAULT_ESTIMATOR_HORIZON short estimator horizon}, makes each verdict a comparison of the current
 * operating level against the previous one - marginal elasticity, which is the question RISE actually asks.</li>
 * <li><b>Growth is provisional until the next verdict adjudicates it.</b> The law remembers the pre-growth
 * baseline; when the first post-settle verdict says the step did not pay (HOLD band), the step is RETRACTED to
 * that baseline rather than kept - so the law converges to the LAST LEVEL THAT PAID (the falsifier plant's exact
 * knee) instead of parking one overshoot step above it. A RISE verdict confirms the growth and the baseline
 * advances. Warmup growth is adjudicated the same way by the episode's first verdict.</li>
 * <li><b>HOLD</b> (elasticity in [0, threshold]): hold, reason {@code PLATEAU}. This band is the plateau brake -
 * flat throughput with climbing in-flight never licenses growth (the ratchet the old law shipped).</li>
 * <li><b>FALL</b> (elasticity below zero): multiplicative contraction by {@link #AIMD_BACKOFF_RATIO}, floor
 * clamped, applied to the retracted baseline when growth was pending - more concurrency bought less work, so the
 * unconfirmed step is taken back before the cut.</li>
 * </ul>
 * <p>
 * <b>Floor invariant (R7), asserted at construction:</b> the floor never sits below one accelerator step -
 * {@code accelerator(floor) >= 1} slot - or the accelerator could not act at the floor and the floor would be
 * absorbing.
 * <p>
 * <b>What this law cannot do yet, deliberately:</b> descend from a too-high start on a flat plateau (no signal in
 * a throughput-steered law distinguishes 50 slots from 20 when both complete the same 400 records/s - the
 * operator-facing symptom is queueing latency, which R8 forbids this law to read), and escape a floor pin. Both
 * belong to U6's escape probe, which re-measures from a known-low operating point on a path no gated signal can
 * suppress.
 * <p>
 * PURE MATH ONLY: no clock, no threads, no metrics - every update is an explicit {@link #onWindowClosed} call.
 * The estimator's history horizon is driven by a synthetic instant cursor advanced by each window's own measured
 * {@link ClosedAdmissionWindow#getElapsedNanos() elapsed time}, so identical window sequences produce identical
 * trajectories. Single-threaded by design.
 */
@Slf4j
public final class AdmissionControlLaw {

    // ------------------------------------------------------------------
    // Named constants - package-visible so tests can assert against them.
    // ------------------------------------------------------------------

    /**
     * Hard floor: the limit never drops below one slot, or progress stops entirely. R7 requires it to also be at
     * least one accelerator step - asserted at construction, not left to coincidence.
     */
    static final int LIMIT_FLOOR_SLOTS = 1;

    /**
     * Minimum samples a window must carry before the law will act on it (from {@code WindowedLimit}'s default
     * window size). Below it the window is not adjudicated: held, and never offered to the estimator.
     */
    static final int DEFAULT_MIN_SAMPLES_PER_WINDOW = 10;

    /**
     * Non-success outcome fraction above which growth is frozen for the window.
     */
    static final double FAILURE_FRACTION_GROWTH_FREEZE_THRESHOLD = 0.2;

    /**
     * Multiplicative backoff applied on overload drops (from {@code AIMDLimit}'s default) and on the FALL band's
     * contraction - both are "more was destructive" verdicts, differing only in which signal said so.
     */
    static final double AIMD_BACKOFF_RATIO = 0.9;

    /**
     * KTD2's per-episode warmup allowance, in SLOTS of cumulative blind growth (see the class javadoc for why
     * slots, not q-steps). Working constant; U6's escape probe opens a fresh allowance when it concludes.
     */
    static final double DEFAULT_WARMUP_ALLOWANCE_SLOTS = 4.0;

    /**
     * How many offered (bound, adjudicated, un-braked) windows must pass after a movement before the law acts on
     * a live verdict again - the settle cadence that makes the estimator's verdict marginal rather than
     * whole-climb-averaged (class javadoc). Chosen equal to the estimator's minimum entry count so a post-settle
     * verdict is computable from post-movement evidence.
     */
    static final int DEFAULT_SETTLE_WINDOWS = 8;

    /**
     * The law's estimator horizon. Deliberately SHORTER than {@link AdmissionElasticityEstimator}'s own 60s
     * default (a documented U5 deviation from KTD1's default): at the one-second window cadence this holds about
     * twelve entries - the settle's eight post-movement windows plus a remnant of the previous operating level -
     * which is exactly the two-level comparison the settle cadence needs. A 60s horizon retains the whole
     * below-knee climb, whose averaged slope reads far above the RISE threshold long after growth has stopped
     * paying (measured: slope still 0.47 at 5x the knee).
     */
    static final Duration DEFAULT_ESTIMATOR_HORIZON = Duration.ofSeconds(12);

    /** KTD1: minimum adjudicated windows in the history before the estimator may act. */
    static final int DEFAULT_ESTIMATOR_MIN_ENTRIES = 8;

    /** KTD1: minimum in-flight spread (slots) across the history for a computable slope. */
    static final int DEFAULT_ESTIMATOR_MIN_SPREAD_SLOTS = 1;

    // ------------------------------------------------------------------
    // Configuration (immutable)
    // ------------------------------------------------------------------

    private final int ceiling;
    private final int minSamplesPerWindow;
    private final double warmupAllowanceSlots;
    private final int settleWindows;

    // ------------------------------------------------------------------
    // State
    // ------------------------------------------------------------------

    /**
     * Estimated admission limit, kept fractional so sub-slot adjustments accumulate; published truncated.
     */
    private double estimatedLimit;

    /**
     * The law OWNS its estimator (the U5 wiring decision): {@link AdmissionController#resetForAssignmentDelta}
     * reconstructs the law, which reconstructs the estimator - the rebalance invalidation, until U6 refines it.
     */
    private final AdmissionElasticityEstimator estimator;

    /**
     * Synthetic clock cursor for the estimator's wall-clock horizon: advanced by every closed window's measured
     * elapsed time, so the law stays clock-free and deterministic (class javadoc).
     */
    private Instant windowClockCursor = Instant.EPOCH;

    /** Cumulative blind growth granted since the episode began (construction, or the last acted verdict). KTD2. */
    private double warmupSlotsGranted = 0.0;

    /** Offered windows since the last movement - the settle cadence's counter. Starts open so a law may act. */
    private int offeredWindowsSinceMovement;

    /**
     * The limit BEFORE the growth currently awaiting adjudication (a warmup episode's start, or the level a RISE
     * step left). Null when no growth is pending. The first post-settle verdict resolves it: RISE confirms (the
     * baseline advances), HOLD retracts to it, FALL retracts to it and cuts. A BACKOFF supersedes it.
     */
    private Double pendingGrowthBaseline = null;

    private AdmissionControlLaw(Builder builder) {
        if (builder.ceiling < LIMIT_FLOOR_SLOTS) {
            throw new IllegalArgumentException("ceiling must be >= " + LIMIT_FLOOR_SLOTS);
        }
        if (builder.initialLimit < LIMIT_FLOOR_SLOTS || builder.initialLimit > builder.ceiling) {
            throw new IllegalArgumentException("initialLimit must be within [floor, ceiling]");
        }
        // R7, asserted rather than left to coincidence: at the floor the accelerator must still be able to act,
        // or the floor is absorbing. sqrt(1) = 1 >= 1 slot holds today; this guards the constants' future.
        if (acceleratorStep(LIMIT_FLOOR_SLOTS) < 1.0) {
            throw new IllegalStateException("floor invariant violated (R7): one accelerator step at the floor is "
                    + acceleratorStep(LIMIT_FLOOR_SLOTS) + " slot(s), below 1");
        }
        this.ceiling = builder.ceiling;
        this.minSamplesPerWindow = builder.minSamplesPerWindow;
        this.warmupAllowanceSlots = builder.warmupAllowanceSlots;
        this.settleWindows = builder.settleWindows;
        this.estimatedLimit = builder.initialLimit;
        this.estimator = new AdmissionElasticityEstimator(
                builder.estimatorHorizon, builder.estimatorMinEntries, builder.estimatorMinSpreadSlots);
        this.offeredWindowsSinceMovement = settleWindows; // the first live verdict may be acted on immediately
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * The accelerator: {@code q = sqrt(limit)}, floored at one whole slot (KTD6's working constant; the floor is
     * what makes the R7 invariant hold at limit 1).
     */
    private static double acceleratorStep(double limit) {
        return Math.max(1.0, Math.sqrt(limit));
    }

    /**
     * Evaluates one closed window and moves the limit. See the class javadoc for the gate precedence.
     */
    public AdmissionDecision onWindowClosed(ClosedAdmissionWindow window) {
        windowClockCursor = windowClockCursor.plusNanos(Math.max(0L, window.getElapsedNanos()));
        final double limit = estimatedLimit;

        // Gate 1: adjudication - too few samples to judge. Hold with ALL state untouched: a signal-free window
        // must not advance the settle cadence, spend warmup allowance, nor teach the estimator (KTD3).
        if (window.getSampleCount() < minSamplesPerWindow) {
            return hold(AdmissionDecisionReason.INSUFFICIENT_SIGNAL);
        }

        // Gate 2a: overload drops - one AIMD cut per window, however many drops it carried. Supersedes any
        // pending growth adjudication: the cut is the verdict on it. The reason stays BACKOFF even when the
        // floor clamp bites - the gauge fact that matters is "overload was observed", and dashboards key on it.
        if (window.getOverloadDropCount() > 0) {
            pendingGrowthBaseline = null;
            return move(limit * AIMD_BACKOFF_RATIO, AdmissionDecisionReason.BACKOFF, false);
        }

        // Gate 2b: failure fraction freezes growth. The band machine takes no decision on a failure-poisoned
        // window (it is not offered), so "contract-only" degenerates to a hold - min(current, no decision).
        if (window.nonSuccessFraction() > FAILURE_FRACTION_GROWTH_FREEZE_THRESHOLD) {
            return hold(AdmissionDecisionReason.FAILURE_LIMITED);
        }

        // Gate 2c: offset-encoding back-pressure (R8) - a partition refusing records makes growth meaningless.
        if (window.isOffsetBackPressure()) {
            return hold(AdmissionDecisionReason.OFFSET_BACK_PRESSURE);
        }

        // Gate 3: binding (R5) - an unbound window says nothing about the limit. PRESERVE bit-identical, named
        // for the separated starvation cause, and do not offer (the estimator would refuse it anyway - R2).
        if (!window.isLimitBound()) {
            return hold(reasonForUnbound(window.bindingClassification()));
        }

        // Step 4: bound + adjudicated + un-braked - the only windows the estimator may learn from.
        estimator.offer(windowClockCursor, window, true);
        offeredWindowsSinceMovement++;

        // Step 5: the bands.
        AdmissionElasticityEstimator.Verdict verdict = estimator.verdict();
        if (!verdict.isLive()) {
            return warmupBand(limit);
        }
        if (offeredWindowsSinceMovement < settleWindows) {
            // Between movements: the history is still dominated by the pre-movement operating level, so the
            // current verdict answers yesterday's question. Park until the settle completes (class javadoc).
            return hold(AdmissionDecisionReason.PLATEAU);
        }
        switch (verdict.getBand()) {
            case RISE:
                return riseStep(limit);
            case HOLD:
                return holdBand(limit);
            case FALL:
                return fallContraction(limit);
            default:
                // INSUFFICIENT_SIGNAL is excluded by isLive() above; fail loudly rather than guess.
                throw new IllegalStateException("unreachable band: " + verdict.getBand());
        }
    }

    /**
     * WARMUP band (R3/KTD2): no verdict in force - binding alone licenses one additive step per window, until the
     * episode's allowance is spent. The episode's first grant records the pre-episode baseline, so the episode's
     * eventual adjudicating verdict can retract blind growth that bought nothing.
     */
    private AdmissionDecision warmupBand(double limit) {
        double remaining = warmupAllowanceSlots - warmupSlotsGranted;
        if (remaining <= 0) {
            return hold(AdmissionDecisionReason.WARMUP_EXHAUSTED);
        }
        double grant = Math.min(acceleratorStep(limit), remaining);
        if (pendingGrowthBaseline == null) {
            pendingGrowthBaseline = limit;
        }
        warmupSlotsGranted += grant;
        return move(limit + grant, AdmissionDecisionReason.WARMUP);
    }

    /** RISE: the pending step (if any) is confirmed by this verdict; take the next one. Resets the episode. */
    private AdmissionDecision riseStep(double limit) {
        warmupSlotsGranted = 0.0; // acted verdict - fresh warmup episode (KTD2)
        pendingGrowthBaseline = limit;
        return move(limit + acceleratorStep(limit), AdmissionDecisionReason.ADAPTING);
    }

    /** HOLD: growth stopped paying. Retract a pending step that this verdict shows bought nothing; else park. */
    private AdmissionDecision holdBand(double limit) {
        warmupSlotsGranted = 0.0; // acted verdict - fresh warmup episode (KTD2)
        if (pendingGrowthBaseline != null && pendingGrowthBaseline < limit) {
            double baseline = pendingGrowthBaseline;
            pendingGrowthBaseline = null;
            return move(baseline, AdmissionDecisionReason.ADAPTING);
        }
        pendingGrowthBaseline = null;
        return hold(AdmissionDecisionReason.PLATEAU);
    }

    /** FALL: more bought less. Retract any unconfirmed growth, then cut multiplicatively (floor clamped). */
    private AdmissionDecision fallContraction(double limit) {
        warmupSlotsGranted = 0.0; // acted verdict - fresh warmup episode (KTD2)
        double base = pendingGrowthBaseline != null ? Math.min(limit, pendingGrowthBaseline) : limit;
        pendingGrowthBaseline = null;
        return move(base * AIMD_BACKOFF_RATIO, AdmissionDecisionReason.ADAPTING);
    }

    private static AdmissionDecisionReason reasonForUnbound(ClosedAdmissionWindow.BindingClassification cause) {
        switch (cause) {
            case NO_WORK:
                return AdmissionDecisionReason.NO_WORK;
            case ORDERING_STARVED:
                return AdmissionDecisionReason.ORDERING_STARVED;
            case SELF_THROTTLED:
                return AdmissionDecisionReason.SELF_THROTTLED;
            default:
                throw new IllegalStateException("limit-bound window reached the binding gate: " + cause);
        }
    }

    /**
     * A hold: the estimate is untouched - bit-identical, not re-derived - which is what R5's preserve means.
     */
    private AdmissionDecision hold(AdmissionDecisionReason reason) {
        return new AdmissionDecision((int) estimatedLimit, reason);
    }

    private AdmissionDecision move(double desired, AdmissionDecisionReason reason) {
        return move(desired, reason, true);
    }

    /**
     * A movement: clamp to [floor, ceiling] and, for band/warmup movements ({@code nameTheClamp}), name the
     * clamp when it bound ({@code AT_CAP} / {@code AT_FLOOR} keep their old-gauge semantics: the law wanted to
     * move further than the bounds allow). The BACKOFF brake keeps its own name at the clamps. Any movement
     * restarts the settle cadence.
     */
    private AdmissionDecision move(double desired, AdmissionDecisionReason reason, boolean nameTheClamp) {
        double clamped = Math.max(LIMIT_FLOOR_SLOTS, Math.min(ceiling, desired));
        if (nameTheClamp && desired > ceiling) {
            reason = AdmissionDecisionReason.AT_CAP;
        } else if (nameTheClamp && desired < LIMIT_FLOOR_SLOTS) {
            reason = AdmissionDecisionReason.AT_FLOOR;
        }
        this.estimatedLimit = clamped;
        this.offeredWindowsSinceMovement = 0;
        return new AdmissionDecision((int) clamped, reason);
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

    // ------------------------------------------------------------------
    // Reported state for the controller's movement log (U7) and the escape hatch (U6) - package-private.
    // ------------------------------------------------------------------

    /** The verdict currently in force - band, elasticity and freshness - for the movement log and U6. */
    AdmissionElasticityEstimator.Verdict currentVerdict() {
        return estimator.verdict();
    }

    /** Blind-growth allowance left in the current warmup episode, in slots (KTD2) - for tests and U6. */
    double warmupAllowanceRemaining() {
        return Math.max(0.0, warmupAllowanceSlots - warmupSlotsGranted);
    }

    /** Entries currently in the owned estimator's history - reconstruction is observable through this. */
    int estimatorHistorySize() {
        return estimator.historySize();
    }

    /**
     * Configuration for {@link AdmissionControlLaw}. The latency knobs of the deleted Gradient2 port (tolerance,
     * long-baseline, probe-down) are gone with it; what remains is the band machine's calibration plus the
     * estimator wiring (KTD1's minimums, and the law's own horizon - see {@link #DEFAULT_ESTIMATOR_HORIZON}).
     */
    public static final class Builder {
        private int initialLimit = 20;
        private int ceiling = 100;
        private int minSamplesPerWindow = DEFAULT_MIN_SAMPLES_PER_WINDOW;
        private double warmupAllowanceSlots = DEFAULT_WARMUP_ALLOWANCE_SLOTS;
        private int settleWindows = DEFAULT_SETTLE_WINDOWS;
        private Duration estimatorHorizon = DEFAULT_ESTIMATOR_HORIZON;
        private int estimatorMinEntries = DEFAULT_ESTIMATOR_MIN_ENTRIES;
        private int estimatorMinSpreadSlots = DEFAULT_ESTIMATOR_MIN_SPREAD_SLOTS;

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

        public Builder minSamplesPerWindow(int minSamplesPerWindow) {
            this.minSamplesPerWindow = minSamplesPerWindow;
            return this;
        }

        /** KTD2's per-episode blind-growth cap, in slots. Test seam; production uses the working constant. */
        public Builder warmupAllowanceSlots(double warmupAllowanceSlots) {
            if (warmupAllowanceSlots < 0) {
                throw new IllegalArgumentException("warmupAllowanceSlots must be >= 0");
            }
            this.warmupAllowanceSlots = warmupAllowanceSlots;
            return this;
        }

        /** The settle cadence in offered windows. Test seam; production uses the working constant. */
        public Builder settleWindows(int settleWindows) {
            if (settleWindows < 1) {
                throw new IllegalArgumentException("settleWindows must be >= 1");
            }
            this.settleWindows = settleWindows;
            return this;
        }

        /** The owned estimator's wall-clock horizon (see {@link #DEFAULT_ESTIMATOR_HORIZON} for why not 60s). */
        public Builder estimatorHorizon(Duration estimatorHorizon) {
            this.estimatorHorizon = estimatorHorizon;
            return this;
        }

        /** KTD1's minimum adjudicated windows before the estimator may act. */
        public Builder estimatorMinEntries(int estimatorMinEntries) {
            this.estimatorMinEntries = estimatorMinEntries;
            return this;
        }

        /** KTD1's minimum in-flight spread for a computable slope. */
        public Builder estimatorMinSpreadSlots(int estimatorMinSpreadSlots) {
            this.estimatorMinSpreadSlots = estimatorMinSpreadSlots;
            return this;
        }

        public AdmissionControlLaw build() {
            return new AdmissionControlLaw(this);
        }
    }
}
