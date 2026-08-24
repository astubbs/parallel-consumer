package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.AdaptiveConcurrencyMode;
import bz.stub.parallelconsumer.internal.RateLimiter;
import bz.stub.parallelconsumer.metrics.PCMetrics;
import bz.stub.parallelconsumer.metrics.PCMetricsDef;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

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
 * No clock reads outside the injected {@link Clock} (the module's, so tests drive time) and no threads of its own.
 * <p>
 * <b>Observability.</b> When a {@link PCMetrics} is supplied and the mode is not {@code DISABLED}, construction
 * registers the four {@code pc.admission.*} meters (live target, would-be target, binding constraint, movement
 * count) - see {@link #initMetrics(PCMetrics)}. Independently of any registry, {@link #tick()} narrates the
 * target on TWO channels, so an operator running the feature with no Micrometer registry configured (the
 * ordinary case for someone trying it out) still gets its product:
 * <ul>
 * <li>every window that MOVES the target logs one line - old and new target, the deciding gate, the window
 * aggregates that drove it, and the law's own reasoning: the elasticity verdict the band machine read
 * ({@link #reportMovement});</li>
 * <li>every window that HOLDS it behind a constraint logs one RATE-LIMITED line naming that constraint
 * ({@link #NO_MOVEMENT_CONSTRAINTS}, {@link #maybeReportBindingConstraint}) - the condition is a steady state,
 * not an event, so it is not repeated once per second.</li>
 * </ul>
 * <b>Watching just this.</b> Both lines come from THIS class's own logger -
 * {@code bz.stub.parallelconsumer.internal.admission.AdmissionController} - and both open with the same stable
 * prefix, {@value #LOG_PREFIX}, so they are greppable together and separable from everything else. Raising that
 * one logger to {@code INFO} with the rest of the library at {@code WARN} shows the target's whole trajectory
 * and nothing more:
 * <pre>{@code
 * <logger name="bz.stub.parallelconsumer.internal.admission.AdmissionController" level="info"/>
 * }</pre>
 * Both lines are INFO deliberately. A window is a second and most windows hold, so movements are infrequent by
 * construction; the mode is opt-in and off by default, so no user who has not asked for adaptive concurrency
 * ever sees either line; and the trajectory is the whole observable product of an experimental feature - DEBUG
 * would hide it behind a level nobody enables without already suspecting something.
 * <p>
 * <b>Threading.</b> The control loop owns the decision surface - {@link #tick()} and every published-state read -
 * but service-time samples arrive from the WORKER threads (one per user-function invocation, recorded where the
 * invocation ran), so all {@link #window} access and the recorded-signal counters are guarded by one internal
 * lock. The lock is uncontended in DISABLED (records return before reaching it) and cheap everywhere else: each
 * record is a counter bump or list append. The rebalance callbacks
 * ({@link #onPartitionsRevoked(Collection)} et al) arrive on the BROKER-POLL thread: they touch only the
 * assignment-tracking state (its own lock) and hand the decision to the control thread through one
 * {@link AtomicBoolean}, consumed at {@link #tick()} - the reset itself always runs control-loop-side.
 * <p>
 * Accessors deliberately have no {@code get} prefix, and this class stays OUT of the Truth-generator allowlist in
 * {@code parallel-consumer-core/pom.xml} - see {@code AbstractParallelEoSStreamProcessor#userFunctionTaskAccounting()}
 * for the constraint.
 */
@Slf4j
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
     * {@code INSUFFICIENT_SIGNAL} when the closed window carries too few samples, so a short window never moves the
     * target). Package-visible so tests derive their clock steps from it.
     */
    static final Duration SAMPLE_WINDOW_DURATION = Duration.ofSeconds(1);

    /**
     * How long the target stays frozen after a real assignment delta (the plan's R13/KTD9): the old assignment's
     * history was discarded, and the new workload gets this much injected-clock settle time before the law adapts
     * on it. Windows that close inside the cooldown are discarded, reason {@link AdmissionDecisionReason#COOLDOWN}.
     * Package-visible so tests derive their clock steps from it.
     */
    static final Duration REBALANCE_TARGET_FREEZE_COOLDOWN = Duration.ofSeconds(30);

    /**
     * The reasons that describe the target being HELD by something rather than adapting: a window closed, and the
     * constraint named is why the target is where it is. These are the ones worth SAYING out loud (see
     * {@link #maybeReportBindingConstraint}), because each is a steady state an operator can act on - raise the cap,
     * fix the downstream, feed the consumer more work - and none of them will change on their own.
     * <p>
     * {@code ADAPTING}, {@code BACKOFF} and {@code WARMUP} are deliberately absent: those MOVED the target, which
     * the movement counter and the target gauges already report.
     */
    private static final Set<AdmissionDecisionReason> NO_MOVEMENT_CONSTRAINTS = EnumSet.of(
            AdmissionDecisionReason.AT_CAP,
            AdmissionDecisionReason.AT_FLOOR,
            AdmissionDecisionReason.FAILURE_LIMITED,
            AdmissionDecisionReason.COOLDOWN,
            AdmissionDecisionReason.INSUFFICIENT_SIGNAL,
            AdmissionDecisionReason.WARMUP_EXHAUSTED,
            AdmissionDecisionReason.PLATEAU,
            AdmissionDecisionReason.NO_WORK,
            AdmissionDecisionReason.ORDERING_STARVED,
            AdmissionDecisionReason.SELF_THROTTLED,
            AdmissionDecisionReason.OFFSET_BACK_PRESSURE);

    /**
     * How often at most the binding-constraint line above may speak. The condition it describes is a steady state
     * that persists across every window, not an event, so unlimited it would repeat once per closed window forever -
     * the same shape (and the same interval) as the load-factor ceiling notice in
     * {@code AbstractParallelEoSStreamProcessor}.
     */
    private static final int CONSTRAINT_REPORT_INTERVAL_SECONDS = 5;

    /**
     * The stable opening of EVERY line this class logs - the one token an operator filters on
     * ({@code grep 'Adaptive concurrency'}) to see the target's trajectory and nothing else. Movements and holds
     * share it deliberately: they are two halves of one narration, and a reader following a target that stopped
     * moving needs the line that says why in the same filter as the ones that moved it.
     */
    private static final String LOG_PREFIX = "Adaptive concurrency";

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

    /**
     * Null in DISABLED. Not final: a real assignment delta RECONSTRUCTS the law from {@link #lawBuilder} rather
     * than resetting it field-by-field - reconstruction is provably complete (a hand-maintained {@code reset()}
     * would have to track every future law field, and missing one is silent), keeps the law itself pure, and
     * R13's "carry the target over as the best available prior" is exactly the builder's {@code initialLimit}.
     * Only the control thread reads or swaps it.
     */
    private AdmissionControlLaw law;

    /**
     * Retained for the reconstruction above - carries the caller's calibration (the test seam's tuning included),
     * so a rebuilt law is calibrated identically to the discarded one. Null in DISABLED.
     */
    private final AdmissionControlLaw.Builder lawBuilder;

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
     * <p>
     * Volatile because the reported state is now read off the control thread as well as on it - Micrometer scrapes
     * the gauges below from whatever thread the registry's publisher runs on, and the poller gate already read
     * {@link #currentTarget()} from the broker-poll thread. Only the control thread ever WRITES it, so this adds a
     * visibility guarantee and no coordination.
     */
    private volatile int adaptiveTarget;

    private Instant windowOpenedAt;

    /** Volatile for the same reason as {@link #adaptiveTarget} - the constraint gauge reads it. */
    private volatile AdmissionDecisionReason lastDecisionReason = null;

    private volatile Instant lastMovementAt = null;

    // Meters - held in FIELDS, as the processor holds its own: a Micrometer gauge that loses the object it reads
    // silently reports NaN forever, so the reference is never left to chance (PCMetrics#gaugeFromMetricDef sets
    // strongReference on the meter, and this keeps the meters themselves reachable for the same reason). Null when
    // no PCMetrics was supplied, and in DISABLED, which registers nothing at all.
    private Gauge targetGauge;
    private Gauge wouldBeTargetGauge;
    private Gauge constraintGauge;
    private Counter movementCounter;

    /** Limits how often {@link #maybeReportBindingConstraint} speaks - see the interval's javadoc. */
    private final RateLimiter constraintReportLimiter = new RateLimiter(CONSTRAINT_REPORT_INTERVAL_SECONDS);

    /**
     * End of the post-rebalance target freeze on the injected clock; null when no cooldown is running.
     * Control-thread-owned, like every other decision-surface field.
     */
    private Instant cooldownUntil = null;

    // ------------------------------------------------------------------
    // Assignment tracking (the plan's KTD9 delta gate). Callbacks arrive on the broker-poll thread; the reset they
    // may request runs control-loop-side at tick(). Guarded by assignmentLock - never windowLock, so a rebalance
    // never contends with worker-thread sample recording.
    // ------------------------------------------------------------------

    private final Object assignmentLock = new Object();

    /** This instance's current assignment as the callbacks have reported it. Guarded by {@link #assignmentLock}. */
    private final Set<TopicPartition> trackedAssignment = new HashSet<>();

    /**
     * The assignment as of the last completed rebalance cycle - what a cycle's outcome is compared against. Null
     * until the FIRST assignment, which establishes the baseline without flagging a delta: at startup there is no
     * history to protect, and a cooldown would only delay warmup. Guarded by {@link #assignmentLock}.
     */
    private Set<TopicPartition> assignmentBaseline = null;

    /** The broker-poll-thread-to-control-thread handoff: set on a real delta, consumed at {@link #tick()}. */
    private final AtomicBoolean assignmentDeltaPending = new AtomicBoolean(false);

    /**
     * Constructs an UNINSTRUMENTED controller - no meters are registered. For callers that have no metrics
     * subsystem to hand (bare-module and unit-test environments); {@code PCModule} uses the
     * {@link #AdmissionController(ParallelConsumerOptions, Clock, PCMetrics) instrumented} constructor.
     */
    public AdmissionController(ParallelConsumerOptions<?, ?> options, Clock clock) {
        this(options, clock, AdmissionControlLaw.newBuilder(), null);
    }

    /**
     * The production constructor: as above, plus the {@code pc.admission.*} meters bound to {@code pcMetrics}.
     */
    public AdmissionController(ParallelConsumerOptions<?, ?> options, Clock clock, PCMetrics pcMetrics) {
        this(options, clock, AdmissionControlLaw.newBuilder(), pcMetrics);
    }

    /**
     * Test seam: lets a test pre-tune the law's calibration (e.g. zero queue headroom so pure contraction can reach
     * the floor). The controller stamps {@code initialLimit} and {@code ceiling} onto the builder - those two are
     * ITS resolution to own, whatever the caller set.
     */
    AdmissionController(ParallelConsumerOptions<?, ?> options, Clock clock, AdmissionControlLaw.Builder lawBuilder) {
        this(options, clock, lawBuilder, null);
    }

    /** The tuned-law seam with metrics attached - what the instrumentation tests drive. */
    AdmissionController(ParallelConsumerOptions<?, ?> options, Clock clock, AdmissionControlLaw.Builder lawBuilder,
                        PCMetrics pcMetrics) {
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
            this.lawBuilder = null;
        } else {
            this.window = new AdmissionSampleWindow();
            this.lawBuilder = lawBuilder;
            // The law always adapts against the ceiling ENFORCE would use (KTD4): in ENFORCE that IS the
            // effective maximum; in OBSERVE it is what the would-be target is computed against, recorded as such.
            this.law = lawBuilder
                    .initialLimit(adaptiveTarget)
                    .ceiling(enforceCeiling)
                    .build();
        }
        this.windowOpenedAt = clock.instant();
        initMetrics(pcMetrics);
    }

    /**
     * Registers the {@code pc.admission.*} meters, mode-gated: {@code DISABLED} registers NOTHING, because an inert
     * controller has nothing to report and a flat gauge reads as "measured and steady" rather than "switched off".
     * <p>
     * The gate is the MODE, not the engine's resolved
     * {@code AbstractParallelEoSStreamProcessor#isAdaptiveConcurrencyActive()} flag, because that flag does not
     * exist yet at this point: under {@code ENFORCE} the module builds this controller while resolving
     * {@code dynamicExtraLoadFactor()}, which the processor forces BEFORE it resolves capability. The blast radius
     * is an engine that requested a mode and refused it (external engines, direct pull): its meters exist and sit
     * at the static values, alongside the WARN the refusal already logged.
     * <p>
     * Every meter is held in a field and registered through {@link PCMetrics}, so the existing
     * {@code deregisterMeters()} / {@link PCMetrics#close()} path reclaims them with everything else - the
     * controller owns no shutdown hook of its own.
     */
    private void initMetrics(PCMetrics pcMetrics) {
        if (pcMetrics == null || mode == AdaptiveConcurrencyMode.DISABLED) {
            return;
        }
        this.targetGauge = pcMetrics.gaugeFromMetricDef(PCMetricsDef.ADMISSION_TARGET,
                this, AdmissionController::currentTarget);
        this.wouldBeTargetGauge = pcMetrics.gaugeFromMetricDef(PCMetricsDef.ADMISSION_WOULD_BE_TARGET,
                this, AdmissionController::wouldBeTarget);
        this.constraintGauge = pcMetrics.gaugeFromMetricDef(PCMetricsDef.ADMISSION_CONSTRAINT,
                this, AdmissionController::bindingConstraintValue);
        this.movementCounter = pcMetrics.getCounterFromMetricDef(PCMetricsDef.ADMISSION_MOVEMENTS);
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
    // Rebalance callbacks - broker-poll thread (see the class javadoc's threading note).
    // ------------------------------------------------------------------

    /**
     * Reports partitions revoked from this instance. Pure set bookkeeping - a revocation is mid-cycle (the
     * consumer contract always follows it with {@link #onPartitionsAssigned(Collection)}, possibly empty), so the
     * delta check waits for the cycle to complete rather than reading a transient half-rebalanced set as a delta.
     */
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        if (mode == AdaptiveConcurrencyMode.DISABLED) {
            return;
        }
        synchronized (assignmentLock) {
            trackedAssignment.removeAll(partitions);
        }
    }

    /**
     * Reports partitions assigned to this instance - the completion of a rebalance cycle, so this is where the
     * KTD9 delta gate compares the resulting assignment against the pre-rebalance baseline. Only a REAL delta
     * requests the reset: cooperative (and identical-eager) rebalances fire the callbacks even when nothing moved
     * for this instance, and discarding valid history on those would let group churn starve the controller.
     */
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        if (mode == AdaptiveConcurrencyMode.DISABLED) {
            return;
        }
        synchronized (assignmentLock) {
            trackedAssignment.addAll(partitions);
            checkAssignmentDelta();
        }
    }

    /**
     * Reports partitions LOST (fenced, not revoked) - a cycle end with no assignment half, so the delta check runs
     * here too.
     */
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        if (mode == AdaptiveConcurrencyMode.DISABLED) {
            return;
        }
        synchronized (assignmentLock) {
            trackedAssignment.removeAll(partitions);
            checkAssignmentDelta();
        }
    }

    /** Caller holds {@link #assignmentLock}. */
    private void checkAssignmentDelta() {
        if (assignmentBaseline == null) {
            // First-ever assignment: baseline only - there is no history to protect yet, and a startup cooldown
            // would just delay warmup (see the field javadoc).
            assignmentBaseline = new HashSet<>(trackedAssignment);
            return;
        }
        if (!trackedAssignment.equals(assignmentBaseline)) {
            assignmentBaseline = new HashSet<>(trackedAssignment);
            assignmentDeltaPending.set(true);
        }
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
     * <p>
     * Two lifecycle arms run ahead of the window arithmetic (R13):
     * <ul>
     * <li>a pending assignment delta (checked first, mid-window included) discards the in-progress window AND the
     * law's history, and freezes the target for {@link #REBALANCE_TARGET_FREEZE_COOLDOWN} - see
     * {@link #resetForAssignmentDelta};</li>
     * <li>while the cooldown runs, windows still close on cadence but are DISCARDED, reason
     * {@link AdmissionDecisionReason#COOLDOWN}: settle-time samples describe a workload still rearranging itself,
     * and feeding them to a freshly-reset law would warm its baseline on noise.</li>
     * </ul>
     * <p>
     * This no-argument form closes windows with {@link AdmissionBoundarySignals#UNSAMPLED} - for callers with no
     * engine to sample (tests driving the law directly). The engine calls
     * {@link #tick(Supplier)} so every closed window carries real boundary signals.
     */
    public void tick() {
        tick(() -> AdmissionBoundarySignals.UNSAMPLED);
    }

    /**
     * As {@link #tick()}, sampling the engine's boundary signals through {@code boundarySampler} - invoked ONLY
     * when a window is actually due (never on the passes between boundaries, so the sampler's O(shards)
     * selectable-work read is paid about once a second), and exactly once per closed window. In DISABLED, and
     * while the window's time bound has not elapsed, the sampler is never called.
     * <p>
     * The closed window carries the MEASURED elapsed time since the window opened on the injected clock - never
     * the nominal {@link #SAMPLE_WINDOW_DURATION}, because windows drift (this method only runs when the control
     * loop passes, so an idle consumer produces one long window rather than several nominal ones).
     */
    public void tick(Supplier<AdmissionBoundarySignals> boundarySampler) {
        if (mode == AdaptiveConcurrencyMode.DISABLED) {
            return;
        }
        Instant now = clock.instant();
        if (assignmentDeltaPending.getAndSet(false)) {
            resetForAssignmentDelta(now);
            return;
        }
        if (Duration.between(windowOpenedAt, now).compareTo(SAMPLE_WINDOW_DURATION) < 0) {
            return;
        }
        long elapsedNanos = Duration.between(windowOpenedAt, now).toNanos();
        AdmissionBoundarySignals boundarySignals = boundarySampler.get();
        ClosedAdmissionWindow closed;
        synchronized (windowLock) {
            closed = window.close(elapsedNanos, boundarySignals);
        }
        windowOpenedAt = now;
        if (cooldownUntil != null) {
            if (now.isBefore(cooldownUntil)) {
                // Frozen: the closed window is discarded, the target holds at its carried-over value.
                lastDecisionReason = AdmissionDecisionReason.COOLDOWN;
                maybeReportBindingConstraint(AdmissionDecisionReason.COOLDOWN);
                return;
            }
            cooldownUntil = null;
        }
        AdmissionDecision decision = law.onWindowClosed(closed);
        lastDecisionReason = decision.getReason();

        // Defensive publish clamp - the law already clamps to [floor, ceiling], and its ceiling IS the enforce
        // ceiling, so this is min(effective maximum, estimate) restated (the plan's clamp expression, which KD6's
        // discovered-ceiling composition will extend here).
        int newTarget = clamp(decision.getTargetConcurrency(), AdmissionControlLaw.LIMIT_FLOOR_SLOTS, enforceCeiling);
        if (newTarget != adaptiveTarget) {
            int previousTarget = adaptiveTarget;
            adaptiveTarget = newTarget;
            lastMovementAt = now;
            if (movementCounter != null) {
                movementCounter.increment();
            }
            reportMovement(previousTarget, newTarget, decision.getReason(), closed);
        }
        maybeReportBindingConstraint(decision.getReason());
    }

    /**
     * Says that the target MOVED, and what moved it - the counterpart to {@link #maybeReportBindingConstraint},
     * and the line an operator watches a ramp on (see the class javadoc's "watching just this").
     * <p>
     * Not rate-limited, unlike the held line: a movement is an EVENT, one per closed window at most, and a window
     * is a second - dropping one would leave a gap in the very trajectory this exists to show. It carries the
     * window's own aggregates because the target alone is uninterpretable: the same {@code 8 -> 9} means
     * something different at 2ms service time with 200 samples than at 200ms with 11.
     * <p>
     * <b>And it carries the law's REASONING, not only its inputs.</b> "decided by ADAPTING" alone does not say
     * WHY the law moved: the number that decides everything is the elasticity verdict - the estimator's
     * band and slope - which is the law's own state, invisible from the window. So the verdict is logged whole
     * (band, elasticity, and the warmup allowance left when no verdict is in force yet), alongside the window's
     * useful throughput and binding classification, which are the estimator's actual inputs (R13: the movement
     * log carries the estimator's inputs for each decision). Service time still appears - the window carries it
     * for observability - but nothing in the law reads it (the design's R8).
     * <p>
     * Named for what actually moved: under {@code OBSERVE} the LIVE target never changes, so calling the moved
     * number "the admission target" there would report an enforcement that is not happening.
     */
    private void reportMovement(int previousTarget, int newTarget, AdmissionDecisionReason reason,
                                ClosedAdmissionWindow closed) {
        log.info(LOG_PREFIX + " ({}): {} target {} -> {} slot(s), decided by {} - elasticity {}, useful " +
                        "throughput {}/s over {} sample(s) ({}), service time mean {}, in-flight median {} " +
                        "(spread {}) over {} snapshot(s), warmup allowance left {}, effective maximum {}.{}",
                mode,
                mode == AdaptiveConcurrencyMode.ENFORCE ? "live admission" : "would-be",
                previousTarget,
                newTarget,
                reason,
                formatVerdict(law.currentVerdict()),
                String.format(Locale.ROOT, "%.1f", closed.successThroughputPerSecond()),
                closed.getSampleCount(),
                closed.bindingClassification(),
                formatNanosAsMillis(closed.getMeanServiceTimeNanos()),
                closed.getInFlightMedian(),
                closed.getInFlightSpread(),
                closed.getInFlightSampleCount(),
                String.format(Locale.ROOT, "%.1f", law.warmupAllowanceRemaining()),
                effectiveMaximum(),
                nonSuccessNote(closed));
    }

    /**
     * The verdict as the log renders it: the band plus the raw slope for a live one, or the no-verdict marker -
     * the one number the band machine turns on, made visible per movement.
     */
    private static String formatVerdict(AdmissionElasticityEstimator.Verdict verdict) {
        if (!verdict.isLive()) {
            return "none yet";
        }
        return String.format(Locale.ROOT, "%.3f (%s)", verdict.getElasticity(), verdict.getBand());
    }

    /**
     * The non-success fraction, rendered as a trailing sentence - and ONLY when it is non-zero. A healthy window
     * would otherwise carry a permanent " non-success 0%" on every line, which is noise that trains the reader to
     * stop looking at exactly the field that matters when it stops being zero.
     */
    private static String nonSuccessNote(ClosedAdmissionWindow closed) {
        double fraction = closed.nonSuccessFraction();
        if (fraction <= 0) {
            return "";
        }
        return String.format(Locale.ROOT, " Non-success fraction %.2f of %d outcome(s).",
                fraction, closed.totalOutcomeCount());
    }

    /**
     * Service time in milliseconds - the unit an operator thinks in. {@link Locale#ROOT} so the decimal separator
     * is the same in a log shipped from any machine.
     */
    private static String formatNanosAsMillis(double nanos) {
        return String.format(Locale.ROOT, "%.2fms", nanos / 1_000_000d);
    }

    /**
     * Says, at most once per {@link #CONSTRAINT_REPORT_INTERVAL_SECONDS}, which constraint is holding the target
     * where it is - the observe-only reporting channel that works with NO {@code MeterRegistry} configured, which
     * is the ordinary case for someone trying {@code OBSERVE} out.
     * <p>
     * Both targets are named because they differ in exactly the mode this matters most for: under {@code OBSERVE}
     * the live target is the static one and the would-be target is the finding.
     */
    private void maybeReportBindingConstraint(AdmissionDecisionReason reason) {
        if (!NO_MOVEMENT_CONSTRAINTS.contains(reason)) {
            return;
        }
        constraintReportLimiter.performIfNotLimited(() ->
                log.info(LOG_PREFIX + " ({}): the admission target is being held by {} - live target {} " +
                                "slot(s), would-be target {} slot(s), effective maximum {}.",
                        mode, reason, currentTarget(), wouldBeTarget(), effectiveMaximum()));
    }

    /**
     * The R13 rebalance reset, control-thread-side: the in-progress window is discarded (the old assignment's
     * samples describe partitions this instance may no longer own), the law is RECONSTRUCTED from
     * {@link #lawBuilder} - identical calibration, fresh elasticity history/verdict/warmup episode (see the
     * {@link #law} field javadoc for why reconstruction beats a hand-written reset; the law owns its estimator,
     * so reconstruction IS the rebalance invalidation until U6 refines it) - seeded with the current target as
     * the best available prior, and the target freezes for {@link #REBALANCE_TARGET_FREEZE_COOLDOWN}.
     */
    private void resetForAssignmentDelta(Instant now) {
        synchronized (windowLock) {
            window.discard();
        }
        law = lawBuilder
                .initialLimit(clamp(adaptiveTarget, AdmissionControlLaw.LIMIT_FLOOR_SLOTS, enforceCeiling))
                .ceiling(enforceCeiling)
                .build();
        windowOpenedAt = now;
        cooldownUntil = now.plus(REBALANCE_TARGET_FREEZE_COOLDOWN);
        lastDecisionReason = AdmissionDecisionReason.COOLDOWN;
    }

    /**
     * Drops the in-progress window's samples and restarts the window from now - the law and its elasticity
     * history survive untouched. The engine's pause-poison lever (R13): a {@code PAUSED} interval leaves the window holding
     * samples from before (and completions from during) the pause, and the first post-resume window must not
     * carry them; a pause says nothing about the downstream, so the law's history stays. No-op in DISABLED.
     */
    public void discardWindow() {
        if (mode == AdaptiveConcurrencyMode.DISABLED) {
            return;
        }
        synchronized (windowLock) {
            window.discard();
        }
        windowOpenedAt = clock.instant();
    }

    /**
     * Test seam (package-private): the live law instance, so same-package tests can prove reconstruction (new
     * instance, baseline zeroed) and survival (same instance) across rebalances. Null in DISABLED.
     */
    AdmissionControlLaw law() {
        return law;
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
     * {@link #lastDecisionReason()} as the number the {@code pc.admission.constraint} gauge publishes -
     * {@link AdmissionDecisionReason#NO_DECISION_YET_VALUE} until the first window closes (and always in
     * {@code DISABLED}, which never registers the gauge anyway).
     */
    public int bindingConstraintValue() {
        AdmissionDecisionReason reason = lastDecisionReason;
        return reason == null ? AdmissionDecisionReason.NO_DECISION_YET_VALUE : reason.getValue();
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
