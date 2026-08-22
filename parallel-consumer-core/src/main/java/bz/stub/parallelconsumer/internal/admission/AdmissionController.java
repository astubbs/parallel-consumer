package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.AdaptiveConcurrencyMode;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * Owns the LIVE admission target (in slots), its effective-maximum resolution, its mode, and its reported state -
 * the single component the engine consults for "how many invocations may be in flight right now".
 * <p>
 * It composes exactly one {@link AdmissionSampleWindow} (all accumulation) and one {@link AdmissionControlLaw} (all
 * decision logic), adding NO accumulation or control-law logic of its own: samples pass straight through to the
 * window, and the only arithmetic here is the KTD4 ceiling resolution and a defensive publish clamp.
 * <p>
 * <b>Effective-maximum resolution (the plan's KTD4).</b> {@code maxConcurrency} is the one knob - pool size and
 * effective maximum. When the user left it at the library default
 * ({@link ParallelConsumerOptions#DEFAULT_MAX_CONCURRENCY}) while {@link AdaptiveConcurrencyMode#ENFORCE} is on,
 * {@link #ADAPTIVE_DEFAULT_CEILING} substitutes - keeping the hand-tuning default while asking for adaptive
 * behaviour is a contradiction. The substitution applies in ENFORCE ONLY: OBSERVE keeps the configured (or
 * library-default) {@code maxConcurrency} as {@link #effectiveMaximum()} - a mode advertised as non-acting must not
 * resize anything - and computes its would-be target against {@link #wouldBeEnforceCeiling()}, the ceiling ENFORCE
 * would use, recorded as such.
 * <p>
 * <b>Start value (the plan's R4).</b> The target starts at the seed
 * ({@link ParallelConsumerOptions#getAdaptiveConcurrencyInitialTarget()}) when set, else at the target today's
 * static configuration derives - the user's {@code maxConcurrency}, NEVER the substituted ceiling: t=0 admission
 * must not exceed today's static behaviour. Clamped to {@code [1, effectiveMaximum]}.
 * <p>
 * <b>Modes.</b> In ENFORCE the law's decisions move {@link #currentTarget()}. In OBSERVE they move only
 * {@link #wouldBeTarget()}; the published target stays at the static value. In DISABLED the controller is inert -
 * it is still constructed (PCModule always constructs it, so downstream reads never null-check), but every input is
 * a no-op and {@link #currentTarget()} is the static {@code maxConcurrency}. Whether adaptive concurrency is ACTIVE
 * (mode requested AND the engine can serve it) is the processor's call -
 * {@code AbstractParallelEoSStreamProcessor#isAdaptiveConcurrencyActive()}; the DISABLED guard here is defensive
 * depth, not the decision.
 * <p>
 * No clock reads outside the injected {@link Clock} (the module's, so tests drive time), no threads of its own, no
 * metrics (Micrometer exposure is a later unit's job).
 * <p>
 * <b>Threading.</b> The control loop owns the decision surface - {@link #tick()} and every published-state read -
 * but service-time samples arrive from the WORKER threads (one per user-function invocation, recorded where the
 * invocation ran), so all {@link #window} access and the recorded-signal counters are guarded by one internal
 * lock. The lock is uncontended in DISABLED (records return before reaching it) and cheap everywhere else: each
 * record is a counter bump or list append.
 * <p>
 * Accessors deliberately have no {@code get} prefix, and this class stays OUT of the Truth-generator allowlist in
 * {@code parallel-consumer-core/pom.xml} - see {@code AbstractParallelEoSStreamProcessor#userFunctionTaskAccounting()}
 * for the constraint.
 */
public class AdmissionController {

    /**
     * The documented adaptive default ceiling (the plan's KTD4): substitutes for {@code maxConcurrency} when the
     * user left that at {@link ParallelConsumerOptions#DEFAULT_MAX_CONCURRENCY} with
     * {@link AdaptiveConcurrencyMode#ENFORCE} on.
     * <p>
     * <b>Value is calibration-pending.</b> 64 is a placeholder judged reasonable pending measurement, and the trade
     * is memory as much as threads: the worker pool holds ceiling threads permanently once loaded (core == max, no
     * timeout), and buffered records scale as {@code ceiling x batchSize x pinned-load-factor} - a ceiling of 64
     * with a batch size of 100 and a pinned factor of 2 is ~12,800 buffered records. Calibrate against both costs
     * before graduating the feature.
     * <p>
     * A user who wants exactly the old default as their cap sets {@code maxConcurrency} to it explicitly (the
     * sentinel edge, documented on {@link ParallelConsumerOptions#getAdaptiveConcurrencyMode()}).
     */
    public static final int ADAPTIVE_DEFAULT_CEILING = 64;

    /**
     * The sample window's time bound: {@link #tick()} closes the window once this much injected-clock time has
     * elapsed since it opened (the plan's KTD3b/KTD7 - "at least 1s", and the law holds via
     * {@code APP_LIMITED} when the closed window carries too few samples, so a short window never moves the
     * target). Package-visible so tests derive their clock steps from it.
     */
    static final Duration SAMPLE_WINDOW_DURATION = Duration.ofSeconds(1);

    /**
     * How a completed invocation's outcome classifies for admission purposes - the pass-through vocabulary for
     * {@link #recordOutcome(Outcome)}, mapping one-to-one onto {@link AdmissionSampleWindow}'s outcome counters.
     */
    public enum Outcome {
        /** Completed successfully - {@link AdmissionSampleWindow#recordSuccess()}. */
        SUCCESS,
        /**
         * Business-logic failure or skipped work; must not cut the limit -
         * {@link AdmissionSampleWindow#recordIgnore()}.
         */
        IGNORE,
        /** Dropped because the downstream is overloaded - {@link AdmissionSampleWindow#recordOverloadDrop()}. */
        OVERLOAD_DROP,
    }

    private final AdaptiveConcurrencyMode mode;
    private final Clock clock;

    /**
     * The static target today's configuration derives - the user's (or default) {@code maxConcurrency}. Published
     * as the target in OBSERVE and DISABLED.
     */
    private final int staticTarget;

    private final int effectiveMaximum;
    private final int enforceCeiling;

    /** Null in DISABLED - the inert mode accumulates nothing. */
    private final AdmissionSampleWindow window;
    /** Null in DISABLED. */
    private final AdmissionControlLaw law;

    /**
     * Guards {@link #window} and the recorded-signal counters below - see the class javadoc's threading note.
     * Never held while consulting the law or moving the target: those stay control-loop-owned.
     */
    private final Object windowLock = new Object();

    // Recorded-signal counters: how many of each signal reached the window since construction (windows reset on
    // close; these never do). Reported state for the signal-plumbing tests and the later metrics unit; guarded by
    // windowLock, and never incremented in DISABLED, whose records return before reaching the window.
    private long serviceTimeSamplesRecorded = 0;
    private long inFlightSamplesRecorded = 0;
    private long successOutcomesRecorded = 0;
    private long ignoreOutcomesRecorded = 0;
    private long overloadDropOutcomesRecorded = 0;

    /**
     * The law-driven target in slots: the LIVE target in ENFORCE, the would-be target in OBSERVE.
     */
    private int adaptiveTarget;

    private Instant windowOpenedAt;
    private AdmissionDecisionReason lastDecisionReason = null;
    private Instant lastMovementAt = null;

    public AdmissionController(ParallelConsumerOptions<?, ?> options, Clock clock) {
        this(options, clock, AdmissionControlLaw.newBuilder());
    }

    /**
     * Test seam: lets a test pre-tune the law's calibration (e.g. zero queue headroom so pure contraction can reach
     * the floor). The controller stamps {@code initialLimit} and {@code ceiling} onto the builder - those two are
     * ITS resolution to own, whatever the caller set.
     */
    AdmissionController(ParallelConsumerOptions<?, ?> options, Clock clock, AdmissionControlLaw.Builder lawBuilder) {
        this.mode = options.getAdaptiveConcurrencyMode();
        this.clock = clock;
        this.staticTarget = options.getMaxConcurrency();

        // KTD4 ceiling resolution - the sentinel test mirrors messageBufferSize's "left at default" convention.
        boolean leftAtLibraryDefault = staticTarget == ParallelConsumerOptions.DEFAULT_MAX_CONCURRENCY;
        this.enforceCeiling = leftAtLibraryDefault ? ADAPTIVE_DEFAULT_CEILING : staticTarget;
        this.effectiveMaximum = mode == AdaptiveConcurrencyMode.ENFORCE ? enforceCeiling : staticTarget;

        // R4 unseeded start: the static-configuration-derived target, never the substituted ceiling.
        int seed = options.getAdaptiveConcurrencyInitialTarget();
        int start = seed == 0 ? staticTarget : seed;
        this.adaptiveTarget = clamp(start, AdmissionControlLaw.LIMIT_FLOOR_SLOTS, enforceCeiling);

        if (mode == AdaptiveConcurrencyMode.DISABLED) {
            // Defensive inert construction - see class javadoc; the module constructs unconditionally.
            this.adaptiveTarget = staticTarget;
            this.window = null;
            this.law = null;
        } else {
            this.window = new AdmissionSampleWindow();
            // The law always adapts against the ceiling ENFORCE would use (KTD4): in ENFORCE that IS the
            // effective maximum; in OBSERVE it is what the would-be target is computed against, recorded as such.
            this.law = lawBuilder
                    .initialLimit(adaptiveTarget)
                    .ceiling(enforceCeiling)
                    .build();
        }
        this.windowOpenedAt = clock.instant();
    }

    // ------------------------------------------------------------------
    // Sample-feeding pass-throughs - straight delegation, no logic.
    // ------------------------------------------------------------------

    /**
     * Records one user-function invocation's service time - already fill-normalized by the caller (batch
     * normalization is the CALLER's job, per {@link AdmissionSampleWindow}'s contract). The one input that arrives
     * from worker threads, hence the lock.
     */
    public void recordServiceTime(long serviceTimeNanos) {
        if (window == null) {
            return;
        }
        synchronized (windowLock) {
            window.addServiceTimeSample(serviceTimeNanos);
            serviceTimeSamplesRecorded++;
        }
    }

    /**
     * Records a snapshot of how many invocations were in flight.
     */
    public void recordInFlight(int inFlight) {
        if (window == null) {
            return;
        }
        synchronized (windowLock) {
            window.addInFlightSample(inFlight);
            inFlightSamplesRecorded++;
        }
    }

    /**
     * Records one completed invocation's {@link Outcome}.
     */
    public void recordOutcome(Outcome outcome) {
        if (window == null) {
            return;
        }
        synchronized (windowLock) {
            switch (outcome) {
                case SUCCESS:
                    window.recordSuccess();
                    successOutcomesRecorded++;
                    break;
                case IGNORE:
                    window.recordIgnore();
                    ignoreOutcomesRecorded++;
                    break;
                case OVERLOAD_DROP:
                    window.recordOverloadDrop();
                    overloadDropOutcomesRecorded++;
                    break;
            }
        }
    }

    /**
     * Records one completed invocation by its raw completion facts, classifying the outcome here so the engine
     * never names outcomes itself: success maps to {@link Outcome#SUCCESS}; a failure goes through
     * {@link AdmissionOutcomeClassifier}, which in v1 answers {@link Outcome#IGNORE} for every cause and owns the
     * documented overload-drop socket.
     * <p>
     * Retry attempts route here like any other completion - their failures COUNT as failure signal, even though
     * their latency is excluded from {@link #recordServiceTime(long)} by the sampler.
     *
     * @param succeeded    the user function's verdict on this delivery
     * @param failureCause the failure's cause when {@code succeeded} is false; ignored (and expected {@code null})
     *                     on success
     */
    public void recordCompletion(boolean succeeded, Throwable failureCause) {
        recordOutcome(succeeded ? Outcome.SUCCESS : AdmissionOutcomeClassifier.classifyFailure(failureCause));
    }

    // ------------------------------------------------------------------
    // The tick
    // ------------------------------------------------------------------

    /**
     * The engine's periodic entry (rides the control loop): when {@link #SAMPLE_WINDOW_DURATION} has elapsed on the
     * injected clock since the window opened, closes the window through the law and moves the target - the live one
     * in ENFORCE, the would-be one in OBSERVE - recording the decision reason, and the timestamp when the target
     * actually moved. Below the time bound (and always in DISABLED) it does nothing, so it is safe to call every
     * control-loop pass.
     */
    public void tick() {
        if (mode == AdaptiveConcurrencyMode.DISABLED) {
            return;
        }
        Instant now = clock.instant();
        if (Duration.between(windowOpenedAt, now).compareTo(SAMPLE_WINDOW_DURATION) < 0) {
            return;
        }
        ClosedAdmissionWindow closed;
        synchronized (windowLock) {
            closed = window.close();
        }
        AdmissionDecision decision = law.onWindowClosed(closed);
        windowOpenedAt = now;
        lastDecisionReason = decision.getReason();

        // Defensive publish clamp - the law already clamps to [floor, ceiling], and its ceiling IS the enforce
        // ceiling, so this is min(effective maximum, estimate) restated (the plan's clamp expression, which KD6's
        // discovered-ceiling composition will extend here).
        int newTarget = clamp(decision.getTargetConcurrency(), AdmissionControlLaw.LIMIT_FLOOR_SLOTS, enforceCeiling);
        if (newTarget != adaptiveTarget) {
            adaptiveTarget = newTarget;
            lastMovementAt = now;
        }
    }

    // ------------------------------------------------------------------
    // Reported state - plain accessors, no `get` prefix (Truth-generator constraint, see class javadoc).
    // ------------------------------------------------------------------

    public AdaptiveConcurrencyMode mode() {
        return mode;
    }

    /**
     * The ceiling admission may never exceed in the CURRENT mode: the user's {@code maxConcurrency} when they set
     * it; under ENFORCE with the library default left in place, {@link #ADAPTIVE_DEFAULT_CEILING}; under
     * OBSERVE/DISABLED always the configured (or default) {@code maxConcurrency} - a non-acting mode resizes
     * nothing.
     */
    public int effectiveMaximum() {
        return effectiveMaximum;
    }

    /**
     * The ceiling ENFORCE would use - what OBSERVE's would-be target is computed against (KTD4). In ENFORCE this
     * equals {@link #effectiveMaximum()}.
     */
    public int wouldBeEnforceCeiling() {
        return enforceCeiling;
    }

    /**
     * The published admission target in slots - what dispatch may act on. Moves with the law only in ENFORCE; in
     * OBSERVE and DISABLED it is the static {@code maxConcurrency}-derived value, always.
     */
    public int currentTarget() {
        return mode == AdaptiveConcurrencyMode.ENFORCE ? adaptiveTarget : staticTarget;
    }

    /**
     * The law-driven target: what ENFORCE is publishing, or what OBSERVE reports it WOULD publish (computed against
     * {@link #wouldBeEnforceCeiling()}). In DISABLED it stays at the static value.
     */
    public int wouldBeTarget() {
        return adaptiveTarget;
    }

    /**
     * Which arm of the control law decided the most recent closed window; empty until the first window closes (and
     * always in DISABLED).
     */
    public Optional<AdmissionDecisionReason> lastDecisionReason() {
        return Optional.ofNullable(lastDecisionReason);
    }

    /**
     * When the (live or would-be) target last changed value; empty until it first moves. A window that closes
     * without moving the target (a hold, or a clamp that lands on the same slot count) does not advance this.
     */
    public Optional<Instant> lastMovementAt() {
        return Optional.ofNullable(lastMovementAt);
    }

    /**
     * How many service-time samples have been recorded since construction (windows reset on close; this never
     * does). Zero always in DISABLED, whose records are no-ops.
     */
    public long serviceTimeSamplesRecorded() {
        synchronized (windowLock) {
            return serviceTimeSamplesRecorded;
        }
    }

    /**
     * How many in-flight snapshots have been recorded since construction. Zero always in DISABLED.
     */
    public long inFlightSamplesRecorded() {
        synchronized (windowLock) {
            return inFlightSamplesRecorded;
        }
    }

    /**
     * How many completions have been recorded as the given {@link Outcome} since construction. Zero always in
     * DISABLED.
     */
    public long outcomesRecorded(Outcome outcome) {
        synchronized (windowLock) {
            switch (outcome) {
                case SUCCESS:
                    return successOutcomesRecorded;
                case IGNORE:
                    return ignoreOutcomesRecorded;
                case OVERLOAD_DROP:
                    return overloadDropOutcomesRecorded;
                default:
                    throw new IllegalArgumentException("Unknown outcome: " + outcome);
            }
        }
    }

    private static int clamp(int value, int floor, int ceiling) {
        return Math.max(floor, Math.min(ceiling, value));
    }
}
