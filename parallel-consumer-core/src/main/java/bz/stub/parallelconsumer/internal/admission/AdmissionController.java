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
 * Probe LIFECYCLE - excursions firing, restores, cadence backoff - narrates on the child channel
 * {@code ...AdmissionController.probe} instead ({@link #probeLog}, law-U14), so silencing probe chatter is one
 * logger-name line ({@code <logger name="...AdmissionController.probe" level="warn"/>}) that costs no movement
 * visibility: a probe whose conclusion pays logs the kept movement on the main channel like any other.
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

    // ------------------------------------------------------------------
    // U6 probe working constants (the design's R6/R14; the falsifier suite, not opinion, moves them).
    // ------------------------------------------------------------------

    /**
     * Consecutive floor windows before the escape probe fires (R6; Envoy's N=5, Uber arrived at the same design
     * independently). The counter is denominated in CLOSED, EVALUATED windows - never wall-clock (KTD3):
     * cooldown-discarded and pause-poisoned windows never advance it, and pause/rebalance clear it. Windows the
     * law holds as {@code INSUFFICIENT_SIGNAL} or unbound DO advance it, deliberately - the sample-count and
     * binding gates are themselves gated signals, and R6's whole point is that no gated signal can suppress the
     * escape: the floor-pinned trickle plant (one sample per window, forever) is exactly the strand this hatch
     * exists for. (This is a documented deviation from the unit packet's "unadjudicated windows do not advance":
     * at production calibration that reading makes the floor-pin falsifier unsatisfiable.)
     */
    static final int FLOOR_ESCAPE_WINDOWS = 5;

    /**
     * The escape's start jitter (ESCAPE safeguard 4): up to this fraction of {@link #FLOOR_ESCAPE_WINDOWS} extra
     * floor windows, drawn from {@link #escapeJitterRandom}, so a fleet does not probe in lockstep. Deterministic
     * under an injected seed (the test-seam constructor).
     */
    static final double ESCAPE_JITTER_FRACTION = 0.15;

    /**
     * How many evaluated windows a probe (escape or descent) runs for before concluding. Same denomination as
     * the escape counter - closed, evaluated windows, never wall-clock (KTD3).
     */
    static final int PROBE_DURATION_WINDOWS = 4;

    /**
     * Consecutive plateau-held windows above the floor before a descent probe fires (the U5 finding: a
     * throughput-steered law cannot descend a flat plateau on its own - the probe is R14's sweep-from-above).
     * Doubles on a failed probe up to {@link #DESCENT_CADENCE_WINDOWS_CAP} (the plant answered "the lower level
     * does not pay", so re-asking gets exponentially rarer), resets on a FALL/RISE band change (the plant moved).
     */
    static final int DESCENT_PLATEAU_WINDOWS = 3;

    /** The descent cadence's backoff ceiling: {@code 3 * 2^3}. */
    static final int DESCENT_CADENCE_WINDOWS_CAP = 24;

    /**
     * The descent probe's keep-vs-restore criterion: the probe's mean success throughput must be within this
     * fraction of the pre-probe reference or the deferred target is restored. THROUGHPUT ONLY - no latency term
     * exists anywhere in this decision (R8). The stagnation probe reuses it mirrored: the raised level is kept
     * only when its mean throughput beat the reference by MORE than this fraction (growth must demonstrably
     * pay; a within-noise result restores).
     */
    static final double DESCENT_THROUGHPUT_TOLERANCE = 0.02;

    /**
     * Consecutive {@code WARMUP_EXHAUSTED}-with-pending-growth windows before the stagnation probe fires.
     * Derivation: equal to the estimator's minimum entry count ({@code AdmissionControlLaw
     * .DEFAULT_ESTIMATOR_MIN_ENTRIES}) - eight bound, adjudicated windows are enough for a first verdict
     * whenever the horizon holds ANY in-flight spread, so eight exhausted windows without one prove the spread
     * is structurally absent at this operating point and only a level change can create it. Waiting longer
     * cannot help (the state was absorbing on the 2026-08-25 comparison IT: the pending-growth anchor
     * suppressed the descent probe's blind-exhausted arming, plateau arming needs a live HOLD verdict, and the
     * escape fires only at the floor - no other exit exists). Denominated in closed, evaluated windows like
     * every probe counter (KTD3).
     */
    static final int STAGNATION_PROBE_WINDOWS = 8;

    /**
     * The stagnation re-ask cadence's backoff ceiling: {@code 8 * 2^3} - the descent probe's doubling
     * discipline at the stagnation probe's base cadence. A permanently verdict-starved plant settles into a
     * rare bounded re-measurement, never a walk.
     */
    static final int STAGNATION_CADENCE_WINDOWS_CAP = 64;

    /**
     * Consecutive live-verdict parked windows (reason {@code PLATEAU} under a live HOLD verdict) before the
     * recovery re-ask probe fires (law-U13) - one accelerator step UP, throughput-adjudicated.
     * <p>
     * <b>Why a timer and not a drift gate.</b> The obvious trigger - fire only when the parked level's own
     * throughput drifts above its park-era reference - is provably insufficient: the FALL walk's marginal-pair
     * stop parks one cut BELOW the knee by construction (the first below-knee window is what reveals the
     * crossing), and below the knee a level's throughput is {@code slots/W0} with NO capacity term, so a
     * capacity recovery changes nothing observable at any level the controller visits. Measured on the
     * capacity-recovery falsifier: windows either side of a capacity-tripling boundary read bit-identical
     * ({@code target=19 throughput=380.0/s bound=true}, windows 78-85) while backlog grew against an idle
     * two-thirds of the downstream. Only asking upward can reveal recovery, so the ask is periodic and
     * bounded; drift, where it IS observable (a park left above the degraded knee, the comparison IT's
     * phase-3 shape), serves as a cadence ACCELERATOR - see {@code armProbes} - never the gate. Do not
     * "simplify" the timer back into a drift gate.
     * <p>
     * Derivation of 8: equal to the estimator's minimum entry count (and the stagnation probe's base cadence)
     * - eight parked windows are one full first-verdict budget of unchanged evidence, so asking sooner would
     * out-pace the law's own adjudication cadence. Denominated in closed, evaluated windows (KTD3).
     */
    static final int RECOVERY_REASK_WINDOWS = 8;

    /**
     * The recovery re-ask cadence's backoff ceiling: {@code 8 * 2^2}. Derived from the steady-state cost
     * budget: at the cap a park pays one {@link #PROBE_DURATION_WINDOWS 4-window} upward excursion per 36
     * windows (~11% of windows), below the descent probe's own worst case at ITS cap (4 per 28, ~14%) - the
     * up-ask must never cost a settled park more than the down-ask it already pays. The cap also bounds
     * recovery-detection latency: capacity restored just after a failed ask is noticed within
     * cap + probe windows, which is what the capacity-recovery falsifier's deadline arithmetic budgets for.
     */
    static final int RECOVERY_CADENCE_WINDOWS_CAP = 32;

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
            AdmissionDecisionReason.OFFSET_BACK_PRESSURE,
            AdmissionDecisionReason.ESCAPE_PROBE,
            AdmissionDecisionReason.DESCENT_PROBE,
            AdmissionDecisionReason.STAGNATION_PROBE,
            AdmissionDecisionReason.RECOVERY_PROBE);

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
     * The probe LIFECYCLE channel (law-U14, owner-ratified 2026-08-25): excursions firing, restores, and cadence
     * backoff log here, named {@code <this class>.probe}, while a probe whose conclusion PAYS - a real target
     * movement kept - logs on the MAIN channel with every other movement. Both channels sit at INFO on purpose:
     * an operator who finds probe chatter noisy silences it by logger NAME
     * ({@code <logger name="...AdmissionController.probe" level="warn"/>}), never by LEVEL - dropping the main
     * channel's level to quiet the probes would also hide the movement lines it exists to show. The split means
     * the main channel narrates only what the target IS (movements, holds, kept probes), and this one narrates
     * what the controller is doing to find out (asking, restoring, backing off).
     */
    private static final org.slf4j.Logger probeLog =
            org.slf4j.LoggerFactory.getLogger(AdmissionController.class.getName() + ".probe");

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
    private long activeTaskSamplesRecorded = 0;
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
    // U6 probe state - CONTROLLER-owned, never law-owned (KTD4): resetForAssignmentDelta reconstructs the law,
    // which destroys law fields, so the deferred restore value must live where the reset can read it. All
    // control-thread-owned.
    // ------------------------------------------------------------------

    /**
     * The two U6 probe kinds plus the quiescent state. Public rather than private only because the Truth
     * subject generator walks nested types of the classes it explores and its generated entry points (in
     * {@code bz.stub.parallelconsumer}) cannot reference a non-public nested type - the same constraint that
     * shapes {@link Outcome}; nothing outside this class consumes it.
     */
    public enum ProbeKind {
        NONE,
        /** R6's ungated floor escape: re-measure at the floor with a cleared history. */
        ESCAPE,
        /** R14's sweep-from-above: step down one accelerator step and ask whether throughput held. */
        DESCENT,
        /**
         * The stagnation exit (see {@link #STAGNATION_PROBE_WINDOWS}): step UP one accelerator step from a
         * verdict-less {@code WARMUP_EXHAUSTED} park and ask whether throughput improved.
         */
        STAGNATION,
        /**
         * The recovery re-ask (law-U13, see {@link #RECOVERY_REASK_WINDOWS}): step UP one accelerator step
         * from a LIVE-verdict park and ask whether capacity has recovered - the exit from the otherwise
         * absorbing below-knee park, where recovery is unobservable without asking.
         */
        RECOVERY,
    }

    private ProbeKind probeKind = ProbeKind.NONE;

    /**
     * The pre-probe target - what a pause or rebalance restores, and what a failed descent probe returns to
     * (ESCAPE safeguard 1: remember and restore, never re-derive). Meaningful only while a probe is in flight.
     */
    private int probeDeferredRestoreTarget;

    /** Evaluated windows left before the in-flight probe concludes (KTD3: window-denominated, not wall-clock). */
    private int probeWindowsRemaining;

    /** Whether EVERY window of the in-flight probe read limit-bound - the escape's re-entry-step criterion. */
    private boolean probeWindowsAllLimitBound;

    /** The plateau throughput the descent probe compares against - captured at probe entry, throughput only. */
    private double probeReferenceThroughput;

    private double probeThroughputSum;
    private int probeThroughputWindowCount;

    /** Consecutive evaluated windows at the floor - the escape's arming counter (see its constant's javadoc). */
    private int floorWindowStreak = 0;

    /** Consecutive plateau-held windows above the floor - the descent probe's arming counter. */
    private int plateauHoldStreak = 0;

    /**
     * Consecutive {@code WARMUP_EXHAUSTED}-with-pending-growth windows - the stagnation probe's arming counter
     * (see {@link #STAGNATION_PROBE_WINDOWS}). Distinct from {@link #plateauHoldStreak}: that arm requires the
     * pending adjudication to be RESOLVED (or absent) before probing down, while this one exists precisely
     * because the resolving verdict is structurally unreachable.
     */
    private int stagnationStreak = 0;

    /** The stagnation probe's current re-ask cadence in stagnant windows - doubles on a restore, capped. */
    private int stagnationCadenceWindows = STAGNATION_PROBE_WINDOWS;

    /** The descent probe's current cadence in plateau windows - doubles on a failed probe, capped. */
    private int descentCadenceWindows = DESCENT_PLATEAU_WINDOWS;

    /**
     * Consecutive live-verdict parked windows - the recovery re-ask's arming counter (law-U13; see
     * {@link #RECOVERY_REASK_WINDOWS}). Deliberately SURVIVES a descent probe that restores (the park is the
     * same park, and the descent cadence's floor of {@link #DESCENT_PLATEAU_WINDOWS} would otherwise preempt
     * this counter forever); reset when the park actually ends - a movement, a kept probe, a pause, a
     * rebalance, or this probe's own firing.
     */
    private int recoveryReaskStreak = 0;

    /** The recovery re-ask's current cadence in parked windows - doubles on a failed probe, capped. */
    private int recoveryCadenceWindows = RECOVERY_REASK_WINDOWS;

    /**
     * The parked level's own-throughput reference, captured on the park's first counted window - the drift
     * ACCELERATOR's baseline (see {@link #RECOVERY_REASK_WINDOWS} for why drift cannot be the gate): a parked
     * window whose throughput exceeds it beyond the {@link #DESCENT_THROUGHPUT_TOLERANCE} noise band (the
     * same "throughput meaningfully moved" tolerance every probe conclusion already uses - one physical
     * question, one tolerance) fires the re-ask immediately, cadence backoff notwithstanding. NaN while no
     * park is being counted; re-armed from the CURRENT window when counting restarts, so a failed probe's
     * restore cannot re-fire on the drift that armed it.
     */
    private double parkReferenceThroughput = Double.NaN;

    /** The jitter source the controller OWNS (deterministic under the test-seam seed). */
    private final java.util.Random escapeJitterRandom;

    /** Extra floor windows before the next escape fires - re-rolled per arming cycle. */
    private int escapeJitterExtraWindows;

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
     * The compounded newCount/oldCount partition ratio across the delta cycles pending consumption (KTD4's
     * shrink scaling input) - compounded so several cycles landing before one tick scale by their NET effect.
     * Guarded by {@link #assignmentLock}; reset to 1.0 when the reset consumes it.
     */
    private double pendingAssignmentSizeRatio = 1.0;

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
        this(options, clock, lawBuilder, pcMetrics, java.util.concurrent.ThreadLocalRandom.current().nextLong());
    }

    /**
     * The full seam: as above plus the escape jitter seed (U6) - production draws it randomly (a fleet must not
     * probe in lockstep, which is the jitter's whole point); tests inject it so probe timing is deterministic.
     */
    AdmissionController(ParallelConsumerOptions<?, ?> options, Clock clock, AdmissionControlLaw.Builder lawBuilder,
                        PCMetrics pcMetrics, long escapeJitterSeed) {
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
        this.escapeJitterRandom = new java.util.Random(escapeJitterSeed);
        rollEscapeJitter();
        initMetrics(pcMetrics);
    }

    /** Draws the next escape arming's extra floor windows - 0 to ~{@link #ESCAPE_JITTER_FRACTION} of N. */
    private void rollEscapeJitter() {
        int maxExtraWindows = (int) Math.round(FLOOR_ESCAPE_WINDOWS * ESCAPE_JITTER_FRACTION);
        this.escapeJitterExtraWindows = escapeJitterRandom.nextInt(maxExtraWindows + 1);
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
     * Records a per-control-loop-pass snapshot of how many SLOTS are occupied by active user-function tasks -
     * the binding verdict's evidence stream: {@link AdmissionSampleWindow} reduces these to their p90, which
     * {@link ClosedAdmissionWindow#bindingClassification()} compares against the target instead of trusting one
     * boundary instant (a point check froze the 2026-08-25 comparison-IT run).
     */
    public void recordActiveTasks(int activeTasks) {
        if (window == null) {
            return;
        }
        synchronized (windowLock) {
            window.addActiveTaskSample(activeTasks);
            activeTaskSamplesRecorded++;
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
            int oldSize = assignmentBaseline.size();
            int newSize = trackedAssignment.size();
            pendingAssignmentSizeRatio *= oldSize == 0 ? 1.0 : (double) newSize / oldSize;
            assignmentBaseline = new HashSet<>(trackedAssignment);
            assignmentDeltaPending.set(true);
        }
    }

    /** Control-thread-side read of the compounded partition ratio for the delta being reset on. */
    private double consumePendingAssignmentSizeRatio() {
        synchronized (assignmentLock) {
            double ratio = pendingAssignmentSizeRatio;
            pendingAssignmentSizeRatio = 1.0;
            return ratio;
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
        processClosedWindow(closed, now);
    }

    /**
     * Test seam (package-private): runs one PRE-CLOSED window through the full decision pipeline - the same
     * assignment-delta consumption, cooldown discard, probe machinery and law consult {@link #tick(Supplier)}
     * applies to a window its own accumulator closed - so the falsifier harness can drive a REAL controller
     * against the deterministic plant's windows (the U6 scenarios exercise pause/rebalance/probe machinery the
     * law alone does not carry). The caller owns the injected clock's cadence.
     */
    void injectClosedWindow(ClosedAdmissionWindow closed) {
        if (mode == AdaptiveConcurrencyMode.DISABLED) {
            return;
        }
        Instant now = clock.instant();
        if (assignmentDeltaPending.getAndSet(false)) {
            resetForAssignmentDelta(now);
            return;
        }
        windowOpenedAt = now;
        processClosedWindow(closed, now);
    }

    /**
     * The decision pipeline for one closed window: cooldown discard first (settle-time samples describe a
     * workload still rearranging itself), then the in-flight probe (which SUSPENDS normal law decisions - R6),
     * then the law, the publish clamp, and finally the U6 probe arming counters.
     */
    private void processClosedWindow(ClosedAdmissionWindow closed, Instant now) {
        if (cooldownUntil != null) {
            if (now.isBefore(cooldownUntil)) {
                // Frozen: the closed window is discarded, the target holds at its carried-over value. Discarded
                // windows never advance the probe counters or an in-flight probe's duration (KTD3).
                lastDecisionReason = AdmissionDecisionReason.COOLDOWN;
                maybeReportBindingConstraint(AdmissionDecisionReason.COOLDOWN);
                return;
            }
            cooldownUntil = null;
        }
        if (probeKind != ProbeKind.NONE) {
            processProbeWindow(closed, now);
            return;
        }
        int windowTarget = adaptiveTarget; // the level this window actually measured
        AdmissionDecision decision = law.onWindowClosed(closed);
        lastDecisionReason = decision.getReason();
        publishTarget(decision.getTargetConcurrency(), decision.getReason(), closed, now);
        maybeReportBindingConstraint(decision.getReason());
        armProbes(windowTarget, decision, closed, now);
    }

    /**
     * Publishes a desired target through the defensive clamp - the law already clamps to [floor, ceiling], and
     * its ceiling IS the enforce ceiling, so this is min(effective maximum, estimate) restated (the plan's clamp
     * expression, which KD6's discovered-ceiling composition will extend here) - recording and reporting the
     * movement when the published value actually changed.
     */
    private void publishTarget(int desired, AdmissionDecisionReason reason, ClosedAdmissionWindow closed,
                               Instant now) {
        int newTarget = clamp(desired, AdmissionControlLaw.LIMIT_FLOOR_SLOTS, enforceCeiling);
        if (newTarget != adaptiveTarget) {
            int previousTarget = adaptiveTarget;
            adaptiveTarget = newTarget;
            lastMovementAt = now;
            if (movementCounter != null) {
                movementCounter.increment();
            }
            reportMovement(previousTarget, newTarget, reason, closed);
        }
    }

    // ------------------------------------------------------------------
    // The U6 probes (R6, R14's sweep-from-above; probe state controller-owned per KTD4).
    // ------------------------------------------------------------------

    /**
     * Advances the probe arming counters after a NORMAL (non-probe, non-cooldown) window. Two independent arms:
     * <ul>
     * <li><b>Floor escape (R6)</b> - {@code windowTarget} at the floor advances {@link #floorWindowStreak}
     * whatever the law said about the window: the sample-count and binding gates are gated signals, and the
     * escape's defining property is that no gated signal can suppress it (see
     * {@link #FLOOR_ESCAPE_WINDOWS}).</li>
     * <li><b>Descent (R14)</b> - above the floor, a window the law parked on plateau evidence (reason
     * {@code PLATEAU} under a live HOLD verdict - never a settle park under a RISE verdict, which is a climb in
     * progress) or blind-exhausted ({@code WARMUP_EXHAUSTED} with no growth pending adjudication - the
     * stuck-blind state a verdict can never resolve at a spread-less operating point) advances
     * {@link #plateauHoldStreak}. While growth IS pending, the next verdict owes a confirm-or-retract and the
     * probe waits for it - probing first would erase the retraction anchor and park the target one blind step
     * high (measured on the pause-cycling plant: 24 instead of the knee's 20).</li>
     * <li><b>Stagnation</b> - the wait above is only sound while the owed verdict is REACHABLE. A window held
     * {@code WARMUP_EXHAUSTED} with growth still pending is a verdict-less park at a spread-less operating
     * point; {@link #STAGNATION_PROBE_WINDOWS} consecutive ones prove the verdict structurally unreachable
     * (its javadoc carries the derivation) and fire {@link #beginStagnationProbe} - one accelerator step UP
     * (the direction the blind growth went), throughput-evaluated. Without this arm the state is absorbing:
     * the 2026-08-25 comparison IT froze in it for 188 of 190 seconds.</li>
     * <li><b>Recovery re-ask (law-U13)</b> - the LIVE-verdict park's counterpart of the stagnation exit: a
     * park under a live HOLD verdict advances {@link #recoveryReaskStreak}, and after
     * {@link #RECOVERY_REASK_WINDOWS} parked windows (or immediately on an own-level throughput drift, where
     * one is observable) {@link #beginRecoveryProbe} asks one step up. Without it the post-contraction park
     * is absorbing when capacity RECOVERS: below the knee recovery changes nothing observable at the parked
     * level (the constant's javadoc carries the measurement), the descent probe asks only downward, and RISE
     * is locked out by the persisted HOLD verdict - the 2026-08-25 comparison IT's arm C sat at 3-5 slots
     * through a whole phase of tripled capacity.</li>
     * </ul>
     */
    private void armProbes(int windowTarget, AdmissionDecision decision, ClosedAdmissionWindow closed,
                           Instant now) {
        if (windowTarget <= AdmissionControlLaw.LIMIT_FLOOR_SLOTS) {
            plateauHoldStreak = 0;
            stagnationStreak = 0;
            recoveryReaskStreak = 0; // the floor strand is the escape probe's, not the recovery re-ask's
            parkReferenceThroughput = Double.NaN;
            if (enforceCeiling <= AdmissionControlLaw.LIMIT_FLOOR_SLOTS) {
                return; // floor == ceiling: there is nowhere to escape to
            }
            if (isAbsoluteBrake(decision.getReason())) {
                // A braked floor window is LIVE evidence actively holding the target down - the controller is
                // braked, not stranded, and a brake can only fire when there IS signal (drops, failures, a
                // blocked partition), so it cannot suppress the escape on the empty-signal strand R6 exists for.
                floorWindowStreak = 0;
                return;
            }
            floorWindowStreak++;
            if (floorWindowStreak >= FLOOR_ESCAPE_WINDOWS + escapeJitterExtraWindows) {
                beginEscapeProbe(closed, now);
            }
            return;
        }
        floorWindowStreak = 0;
        AdmissionElasticityEstimator.Verdict verdict = law.currentVerdict();
        boolean plateauHold = decision.getReason() == AdmissionDecisionReason.PLATEAU
                && verdict.isLive()
                && verdict.getBand() == AdmissionElasticityEstimator.Band.HOLD;
        boolean blindExhausted = decision.getReason() == AdmissionDecisionReason.WARMUP_EXHAUSTED
                && !law.hasPendingGrowth();
        if (plateauHold || blindExhausted) {
            stagnationStreak = 0;
            plateauHoldStreak++;
            if (plateauHold) {
                // The recovery re-ask arms on the LIVE-verdict park only (law-U13): a verdict-less park
                // belongs to the stagnation/descent machinery. Counted independently of the descent streak,
                // and BEFORE the descent check - when both are due, the up-ask this counter exists for wins.
                if (recoveryReaskStreak == 0) {
                    parkReferenceThroughput = closed.successThroughputPerSecond();
                }
                recoveryReaskStreak++;
                boolean throughputDrifted = !Double.isNaN(parkReferenceThroughput)
                        && closed.successThroughputPerSecond()
                        > parkReferenceThroughput * (1 + DESCENT_THROUGHPUT_TOLERANCE);
                if (throughputDrifted || recoveryReaskStreak >= recoveryCadenceWindows) {
                    beginRecoveryProbe(closed, now, throughputDrifted);
                    return;
                }
            } else {
                recoveryReaskStreak = 0;
                parkReferenceThroughput = Double.NaN;
            }
            if (plateauHoldStreak >= descentCadenceWindows) {
                beginDescentProbe(closed, now);
            }
            return;
        }
        boolean stagnant = decision.getReason() == AdmissionDecisionReason.WARMUP_EXHAUSTED
                && law.hasPendingGrowth();
        if (stagnant) {
            plateauHoldStreak = 0;
            recoveryReaskStreak = 0;
            parkReferenceThroughput = Double.NaN;
            stagnationStreak++;
            if (stagnationStreak >= stagnationCadenceWindows) {
                beginStagnationProbe(closed, now);
            }
            return;
        }
        plateauHoldStreak = 0;
        stagnationStreak = 0;
        recoveryReaskStreak = 0; // any other reason means the park ended - a movement, a brake, a starve
        parkReferenceThroughput = Double.NaN;
        if (verdict.isLive()) {
            // A verdict is in force again: the stagnation was resolved by evidence, so a backed-off re-ask
            // cadence re-arms briskly for the next episode.
            stagnationCadenceWindows = STAGNATION_PROBE_WINDOWS;
            if (verdict.getBand() != AdmissionElasticityEstimator.Band.HOLD) {
                // A FALL/RISE band: the plant moved, so backed-off descent and recovery cadences re-arm
                // briskly.
                descentCadenceWindows = DESCENT_PLATEAU_WINDOWS;
                recoveryCadenceWindows = RECOVERY_REASK_WINDOWS;
            }
        }
    }

    /** The law's ungated absolute brakes - live verdicts, so they never read as a strand (see armProbes). */
    private static boolean isAbsoluteBrake(AdmissionDecisionReason reason) {
        return reason == AdmissionDecisionReason.BACKOFF
                || reason == AdmissionDecisionReason.FAILURE_LIMITED
                || reason == AdmissionDecisionReason.OFFSET_BACK_PRESSURE;
    }

    /**
     * Fires the R6 floor escape: remember the deferred restore value (which at the floor IS the floor), pin the
     * published target to the floor with a CLEARED estimator history (ESCAPE safeguards 1-3), and suspend normal
     * law decisions for {@link #PROBE_DURATION_WINDOWS} evaluated windows. The probe's product is a fresh
     * limit-bound history at a known-low operating point.
     */
    private void beginEscapeProbe(ClosedAdmissionWindow closed, Instant now) {
        probeKind = ProbeKind.ESCAPE;
        probeDeferredRestoreTarget = adaptiveTarget;
        probeWindowsRemaining = PROBE_DURATION_WINDOWS;
        probeWindowsAllLimitBound = true;
        probeThroughputSum = 0;
        probeThroughputWindowCount = 0;
        floorWindowStreak = 0;
        law.pinForProbe(AdmissionControlLaw.LIMIT_FLOOR_SLOTS, true);
        publishTarget(AdmissionControlLaw.LIMIT_FLOOR_SLOTS, AdmissionDecisionReason.ESCAPE_PROBE, closed, now);
        lastDecisionReason = AdmissionDecisionReason.ESCAPE_PROBE;
        probeLog.info(LOG_PREFIX + " ({}): {} consecutive floor windows - firing the escape probe: re-measuring at "
                        + "the floor for {} windows with a cleared elasticity history (R6).",
                mode, FLOOR_ESCAPE_WINDOWS + escapeJitterExtraWindows, PROBE_DURATION_WINDOWS);
    }

    /**
     * Fires the R14 descent probe: remember the current target, step the published target DOWN one accelerator
     * step, and measure throughput there for {@link #PROBE_DURATION_WINDOWS} windows against the plateau's
     * reference. Keep the lower target if throughput held (it paid - the knee is at or below it); restore if it
     * fell. Throughput criterion only (R8).
     */
    private void beginDescentProbe(ClosedAdmissionWindow closed, Instant now) {
        int probeTarget = Math.max(AdmissionControlLaw.LIMIT_FLOOR_SLOTS,
                (int) Math.round(adaptiveTarget - AdmissionControlLaw.acceleratorStep(adaptiveTarget)));
        plateauHoldStreak = 0;
        if (probeTarget >= adaptiveTarget) {
            return; // one step down lands where we already are - nothing to measure
        }
        probeKind = ProbeKind.DESCENT;
        probeDeferredRestoreTarget = adaptiveTarget;
        probeReferenceThroughput = closed.successThroughputPerSecond();
        probeWindowsRemaining = PROBE_DURATION_WINDOWS;
        probeWindowsAllLimitBound = true;
        probeThroughputSum = 0;
        probeThroughputWindowCount = 0;
        law.pinForProbe(probeTarget, false);
        publishTarget(probeTarget, AdmissionDecisionReason.DESCENT_PROBE, closed, now);
        lastDecisionReason = AdmissionDecisionReason.DESCENT_PROBE;
        probeLog.info(LOG_PREFIX + " ({}): sustained plateau at {} slot(s) - descent probe to {} slot(s) for {} "
                        + "windows against reference throughput {}/s (R14 sweep-from-above).",
                mode, probeDeferredRestoreTarget, probeTarget, PROBE_DURATION_WINDOWS,
                String.format(Locale.ROOT, "%.1f", probeReferenceThroughput));
    }

    /**
     * Fires the stagnation probe (the C exit: no absorbing state above the floor): remember the current target,
     * step the published target UP one accelerator step - the direction the pending blind growth went - and
     * measure throughput there for {@link #PROBE_DURATION_WINDOWS} windows against the parked level's
     * reference. Keep the higher target only when throughput IMPROVED beyond the tolerance (growth must
     * demonstrably pay - the mirrored descent criterion); restore otherwise. Either way the probe's bound
     * windows are ADOPTED into the estimator: they sit at a DIFFERENT operating level, so they create exactly
     * the in-flight spread whose absence caused the stagnation - the evidence-driven exit. (This adoption is
     * the deliberate asymmetry with a failed DESCENT probe, whose dropped evidence would read as RISE fuel: a
     * failed UP-probe's flat-throughput-at-higher-x evidence reads as HOLD/FALL, which is the truth the probe
     * measured and precisely what unlocks the verdict.) Throughput criterion only (R8). The law keeps its
     * history - the conclusion compares against the parked level, like the descent probe.
     */
    private void beginStagnationProbe(ClosedAdmissionWindow closed, Instant now) {
        int probeTarget = Math.min(enforceCeiling,
                (int) Math.round(adaptiveTarget + AdmissionControlLaw.acceleratorStep(adaptiveTarget)));
        stagnationStreak = 0;
        if (probeTarget <= adaptiveTarget) {
            return; // at the cap there is nowhere up; the blind-exhausted descent arm owns the capped park
        }
        probeKind = ProbeKind.STAGNATION;
        probeDeferredRestoreTarget = adaptiveTarget;
        probeReferenceThroughput = closed.successThroughputPerSecond();
        probeWindowsRemaining = PROBE_DURATION_WINDOWS;
        probeWindowsAllLimitBound = true;
        probeThroughputSum = 0;
        probeThroughputWindowCount = 0;
        law.pinForProbe(probeTarget, false);
        publishTarget(probeTarget, AdmissionDecisionReason.STAGNATION_PROBE, closed, now);
        lastDecisionReason = AdmissionDecisionReason.STAGNATION_PROBE;
        probeLog.info(LOG_PREFIX + " ({}): {} verdict-less WARMUP_EXHAUSTED window(s) at {} slot(s) - the owed "
                        + "verdict is structurally unreachable (no in-flight spread), firing the stagnation "
                        + "probe: one step up to {} slot(s) for {} windows against reference throughput {}/s.",
                mode, STAGNATION_PROBE_WINDOWS, probeDeferredRestoreTarget, probeTarget, PROBE_DURATION_WINDOWS,
                String.format(Locale.ROOT, "%.1f", probeReferenceThroughput));
    }

    /**
     * Fires the recovery re-ask probe (law-U13, the exit from the otherwise absorbing live-verdict park; see
     * {@link #RECOVERY_REASK_WINDOWS} for why the trigger is a bounded timer with drift only accelerating it):
     * remember the parked target, step the published target UP one accelerator step, and measure throughput
     * there for {@link #PROBE_DURATION_WINDOWS} windows against the park's reference. Keep the higher target
     * only when throughput IMPROVED beyond the tolerance with every window bound (the stagnation probe's
     * mirrored criterion - growth must demonstrably pay); restore otherwise and double the cadence. Throughput
     * criterion only (R8). Evidence is adopted in BOTH outcomes, exactly as the stagnation probe's is and for
     * the same reason: the probe's whole product is cross-level spread at the park, a failed up-probe's
     * flat-throughput-at-higher-x evidence bands as HOLD/FALL - the truth it measured - and a KEPT probe's
     * pair is what re-opens the RISE band the park had locked out. (The failed-DESCENT-probe evidence drop
     * does not apply here: dropped descent evidence guards against reading a rejected LOWER level's lower
     * throughput as climb fuel; an up-probe's evidence carries no such false-climb reading.)
     * <p>
     * <b>The ask is HALF an accelerator step (rounded, at least one slot) - deliberately smaller than its
     * sibling probes' full step.</b> This probe fires from settled parks FOREVER (bounded-periodic is its
     * whole design), and legitimate parks settle up to one full step ABOVE the knee (climb-ladder
     * quantization - measured: the saturated-flicker plant parks at 23 on a knee of 20, because the descent
     * dip to 18 undershoots the knee and loses throughput). A full-step ask from such a park peaks two steps
     * above the knee - measured breaching the falsifier band (28 against a ceiling of 27) on every re-ask,
     * forever - while a half-step ask peaks ~1.5 steps above and stays inside. Detection stays sound: below
     * the knee a half-step buys {@code 1/(2*sqrt(park))} relative throughput - above the
     * {@link #DESCENT_THROUGHPUT_TOLERANCE} noise band for every park below ~625 slots, far past any
     * plausible ceiling. The RISE ladder a kept ask re-opens climbs by FULL steps as always; only the
     * question is asked gently.
     */
    private void beginRecoveryProbe(ClosedAdmissionWindow closed, Instant now, boolean firedByDrift) {
        int askStepSlots = Math.max(1, (int) Math.round(AdmissionControlLaw.acceleratorStep(adaptiveTarget) / 2));
        int probeTarget = Math.min(enforceCeiling, adaptiveTarget + askStepSlots);
        recoveryReaskStreak = 0;
        plateauHoldStreak = 0; // the excursion interrupts the park; both timers restart on the resumed park
        parkReferenceThroughput = Double.NaN;
        if (probeTarget <= adaptiveTarget) {
            return; // at the cap there is nowhere up to ask
        }
        probeKind = ProbeKind.RECOVERY;
        probeDeferredRestoreTarget = adaptiveTarget;
        probeReferenceThroughput = closed.successThroughputPerSecond();
        probeWindowsRemaining = PROBE_DURATION_WINDOWS;
        probeWindowsAllLimitBound = true;
        probeThroughputSum = 0;
        probeThroughputWindowCount = 0;
        law.pinForProbe(probeTarget, false);
        publishTarget(probeTarget, AdmissionDecisionReason.RECOVERY_PROBE, closed, now);
        lastDecisionReason = AdmissionDecisionReason.RECOVERY_PROBE;
        probeLog.info(LOG_PREFIX + " ({}): {} at the parked level of {} slot(s) - recovery re-ask probe: one step "
                        + "up to {} slot(s) for {} windows against reference throughput {}/s (below the knee "
                        + "a capacity recovery is invisible at the parked level, so the ask is periodic - "
                        + "law-U13).",
                mode,
                firedByDrift
                        ? "own-level throughput drifted above the park-era reference"
                        : "the re-ask cadence elapsed",
                probeDeferredRestoreTarget, probeTarget, PROBE_DURATION_WINDOWS,
                String.format(Locale.ROOT, "%.1f", probeReferenceThroughput));
    }

    /**
     * One evaluated window while a probe is in flight: the law only OBSERVES it (cursor advance plus buffering
     * of qualifying evidence - normal decisions stay suspended), the probe's own aggregates accumulate, and the
     * duration counts down in evaluated windows (KTD3).
     */
    private void processProbeWindow(ClosedAdmissionWindow closed, Instant now) {
        law.observeProbeWindow(closed);
        probeThroughputSum += closed.successThroughputPerSecond();
        probeThroughputWindowCount++;
        probeWindowsAllLimitBound &= closed.isLimitBound();
        final AdmissionDecisionReason reason;
        switch (probeKind) {
            case ESCAPE:
                reason = AdmissionDecisionReason.ESCAPE_PROBE;
                break;
            case DESCENT:
                reason = AdmissionDecisionReason.DESCENT_PROBE;
                break;
            case STAGNATION:
                reason = AdmissionDecisionReason.STAGNATION_PROBE;
                break;
            case RECOVERY:
                reason = AdmissionDecisionReason.RECOVERY_PROBE;
                break;
            default:
                throw new IllegalStateException("probe window with no probe in flight: " + probeKind);
        }
        lastDecisionReason = reason;
        probeWindowsRemaining--;
        if (probeWindowsRemaining <= 0) {
            concludeProbe(closed, now);
            return;
        }
        maybeReportBindingConstraint(reason);
    }

    /**
     * Concludes the in-flight probe (updates resume; the law opens a fresh warmup allowance - KTD2 - except on
     * a stagnation RESTORE, whose measured "growth does not pay" makes a refill the unbounded ratchet KTD2's
     * cap exists to prevent):
     * <ul>
     * <li><b>ESCAPE</b> - the probe's buffered limit-bound windows enter the estimator (valid by construction).
     * When EVERY probe window read limit-bound, even the floor saturates its slot, so capacity above the floor
     * plausibly exists: the conclusion takes ONE accelerator re-entry step up - provisional blind growth the next
     * verdict adjudicates exactly like a warmup grant. This step is the escape's liveness where the deferred
     * value IS the floor and restore alone would change nothing; on the sample-starved trickle plant no gated
     * band can ever act, so the named steady state is the escape cadence itself (KTD2). Un-bound probe windows
     * (a genuinely idle consumer) restore the floor unchanged - idleness must not fund growth.</li>
     * <li><b>DESCENT</b> - throughput held within {@link #DESCENT_THROUGHPUT_TOLERANCE}: the lower target PAID;
     * keep it, adopt its evidence, and the walk may repeat after another plateau streak. Throughput fell: restore
     * the deferred target, DROP the probe's evidence (a rejected level's lower throughput would read as positive
     * elasticity and teach the law to climb off the knee), and double the cadence up to its cap.</li>
     * <li><b>STAGNATION</b> - throughput improved beyond the tolerance with every window bound: the step up
     * PAID; keep it (fresh allowance - the throughput-gated climb continues). Otherwise restore WITHOUT a fresh
     * allowance and double the re-ask cadence. The evidence is adopted in BOTH outcomes - the probe's whole
     * purpose is to manufacture the in-flight spread the stagnant level could not, and a failed up-probe's
     * evidence bands as HOLD/FALL, never as climb fuel (see {@link #beginStagnationProbe}).</li>
     * </ul>
     */
    private void concludeProbe(ClosedAdmissionWindow closed, Instant now) {
        if (probeKind == ProbeKind.ESCAPE) {
            int concluded = probeWindowsAllLimitBound
                    ? (int) Math.round(probeDeferredRestoreTarget
                    + AdmissionControlLaw.acceleratorStep(probeDeferredRestoreTarget))
                    : probeDeferredRestoreTarget;
            law.concludeProbe(concluded, true, true);
            publishTarget(concluded, AdmissionDecisionReason.ESCAPE_PROBE, closed, now);
            // law-U14: a conclusion that PAID (a kept movement) is main-channel narration; a restore is lifecycle.
            (probeWindowsAllLimitBound ? log : probeLog)
                    .info(LOG_PREFIX + " ({}): escape probe concluded - {} - target {} slot(s), fresh warmup "
                                    + "allowance opened.",
                            mode,
                            probeWindowsAllLimitBound
                                    ? "the floor stayed limit-bound; taking one re-entry step"
                                    : "the floor did not bind; restoring unchanged",
                            adaptiveTarget);
        } else if (probeKind == ProbeKind.RECOVERY) {
            double probeMeanThroughput = probeThroughputSum / probeThroughputWindowCount;
            boolean recoveredCapacityFound = probeWindowsAllLimitBound
                    && probeMeanThroughput > probeReferenceThroughput * (1 + DESCENT_THROUGHPUT_TOLERANCE);
            if (recoveredCapacityFound) {
                // Capacity above the park exists: keep the probed level and adopt its evidence - the fresh
                // cross-level pair re-opens the RISE band, and the ordinary climb machinery takes it from
                // here (probe conclusion opens the settle, so the first qualifying verdict acts at once).
                law.concludeProbe(adaptiveTarget, false, true);
                recoveryCadenceWindows = RECOVERY_REASK_WINDOWS;
            } else {
                // The step up did not pay - the park IS still the knee. Restore and adopt the evidence
                // (see beginRecoveryProbe for why adoption is safe both ways), but through the
                // no-fresh-allowance seam: at a live-verdict park the warmup band is unreachable anyway, and
                // sharing the stagnation restore's seam keeps one restore semantics for both up-probes -
                // including the no-refill property the pause-cycling pin protects. Back the cadence off:
                // the plant answered, so re-asking gets exponentially rarer (the sibling probes' discipline).
                law.concludeProbeRestoringWithEvidence(probeDeferredRestoreTarget);
                publishTarget(probeDeferredRestoreTarget, AdmissionDecisionReason.RECOVERY_PROBE, closed, now);
                recoveryCadenceWindows = Math.min(RECOVERY_CADENCE_WINDOWS_CAP, recoveryCadenceWindows * 2);
            }
            (recoveredCapacityFound ? log : probeLog)
                    .info(LOG_PREFIX + " ({}): recovery re-ask probe concluded - throughput {}/s against reference "
                            + "{}/s: {} - target {} slot(s), next re-ask after {} parked window(s).",
                    mode,
                    String.format(Locale.ROOT, "%.1f", probeMeanThroughput),
                    String.format(Locale.ROOT, "%.1f", probeReferenceThroughput),
                    recoveredCapacityFound
                            ? "capacity above the park exists, keeping the step"
                            : "the park is still the knee, restoring",
                    adaptiveTarget, recoveryCadenceWindows);
        } else if (probeKind == ProbeKind.STAGNATION) {
            double probeMeanThroughput = probeThroughputSum / probeThroughputWindowCount;
            boolean higherTargetPaid = probeWindowsAllLimitBound
                    && probeMeanThroughput > probeReferenceThroughput * (1 + DESCENT_THROUGHPUT_TOLERANCE);
            if (higherTargetPaid) {
                // The step up demonstrably paid: keep the probed level (it is the law's pinned limit already)
                // and adopt its evidence - the two-level history unlocks the verdict that was unreachable. A
                // paying step earns a fresh episode (the ordinary concludeProbe semantics, KTD2): the climb
                // continues, throughput-gated one stagnation cycle at a time.
                law.concludeProbe(adaptiveTarget, false, true);
                stagnationCadenceWindows = STAGNATION_PROBE_WINDOWS;
            } else {
                // Flat (or unbound, or fallen): restore the parked level and adopt the bound evidence, but do
                // NOT open a fresh allowance - re-funding blind growth toward the level this probe just
                // rejected would ratchet without bound on a verdict-starved plant (the law seam's javadoc
                // carries the caught instance) - and back the re-ask cadence off: the plant answered, so
                // re-asking gets exponentially rarer, the descent probe's own discipline.
                law.concludeProbeRestoringWithEvidence(probeDeferredRestoreTarget);
                publishTarget(probeDeferredRestoreTarget, AdmissionDecisionReason.STAGNATION_PROBE, closed, now);
                stagnationCadenceWindows = Math.min(STAGNATION_CADENCE_WINDOWS_CAP, stagnationCadenceWindows * 2);
            }
            (higherTargetPaid ? log : probeLog)
                    .info(LOG_PREFIX + " ({}): stagnation probe concluded - throughput {}/s against reference "
                            + "{}/s: {} - target {} slot(s), next probe after {} stagnant window(s).",
                    mode,
                    String.format(Locale.ROOT, "%.1f", probeMeanThroughput),
                    String.format(Locale.ROOT, "%.1f", probeReferenceThroughput),
                    higherTargetPaid ? "the step up paid, keeping it" : "growth did not pay, restoring",
                    adaptiveTarget, stagnationCadenceWindows);
        } else {
            double probeMeanThroughput = probeThroughputSum / probeThroughputWindowCount;
            boolean lowerTargetPaid =
                    probeMeanThroughput >= probeReferenceThroughput * (1 - DESCENT_THROUGHPUT_TOLERANCE);
            if (lowerTargetPaid) {
                law.concludeProbe(adaptiveTarget, false, true); // the pinned probe value is the new level
                recoveryReaskStreak = 0; // the park MOVED - the recovery re-ask's timer restarts at the new one
                parkReferenceThroughput = Double.NaN;
            } else {
                law.concludeProbe(probeDeferredRestoreTarget, false, false);
                publishTarget(probeDeferredRestoreTarget, AdmissionDecisionReason.DESCENT_PROBE, closed, now);
                descentCadenceWindows = Math.min(DESCENT_CADENCE_WINDOWS_CAP, descentCadenceWindows * 2);
            }
            (lowerTargetPaid ? log : probeLog)
                    .info(LOG_PREFIX + " ({}): descent probe concluded - throughput {}/s against reference {}/s: "
                            + "{} - target {} slot(s), next probe after {} plateau window(s).",
                    mode,
                    String.format(Locale.ROOT, "%.1f", probeMeanThroughput),
                    String.format(Locale.ROOT, "%.1f", probeReferenceThroughput),
                    lowerTargetPaid ? "the lower target paid, keeping it" : "throughput fell, restoring",
                    adaptiveTarget, descentCadenceWindows);
        }
        probeKind = ProbeKind.NONE;
        rollEscapeJitter();
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
     * samples describe partitions this instance may no longer own) and the law is RECONSTRUCTED from
     * {@link #lawBuilder} - identical calibration, fresh elasticity history/verdict/warmup episode (see the
     * {@link #law} field javadoc for why reconstruction beats a hand-written reset). The target freezes for
     * {@link #REBALANCE_TARGET_FREEZE_COOLDOWN}.
     * <p>
     * <b>KTD4: restore BEFORE reconstruct, clamped to the new assignment.</b> Probe state is consulted FIRST:
     * with a probe in flight the seed is the DEFERRED restore value, never the pinned probe value - otherwise
     * the reset launders the probe's reduced target into the 30s-frozen post-rebalance prior, and group churn
     * ratchets the target down. Then, when the assignment SHRANK, the seed is scaled by the partition ratio
     * (floor one slot) - one-directional protection only, so a stale-high pre-rebalance target is not held
     * open-loop through the cooldown against a plant whose per-instance share just fell; growth is left for the
     * law to re-earn.
     */
    private void resetForAssignmentDelta(Instant now) {
        synchronized (windowLock) {
            window.discard();
        }
        int seed = adaptiveTarget;
        if (probeKind != ProbeKind.NONE) {
            seed = probeDeferredRestoreTarget; // KTD4: a rebalance invalidates the probe's measurement anyway
            probeKind = ProbeKind.NONE;
            rollEscapeJitter();
        }
        double sizeRatio = consumePendingAssignmentSizeRatio();
        if (sizeRatio < 1.0) {
            seed = Math.max(AdmissionControlLaw.LIMIT_FLOOR_SLOTS, (int) Math.round(seed * sizeRatio));
        }
        seed = clamp(seed, AdmissionControlLaw.LIMIT_FLOOR_SLOTS, enforceCeiling);
        if (seed != adaptiveTarget) {
            int previousTarget = adaptiveTarget;
            adaptiveTarget = seed;
            lastMovementAt = now;
            if (movementCounter != null) {
                movementCounter.increment();
            }
            log.info(LOG_PREFIX + " ({}): assignment delta moved the carried-over target {} -> {} slot(s) "
                            + "(probe restore and/or shrink scaling by ratio {}) before reconstruction.",
                    mode, previousTarget, seed, String.format(Locale.ROOT, "%.2f", sizeRatio));
        }
        law = lawBuilder
                .initialLimit(seed)
                .ceiling(enforceCeiling)
                .build();
        floorWindowStreak = 0;
        plateauHoldStreak = 0;
        stagnationStreak = 0;
        recoveryReaskStreak = 0;
        stagnationCadenceWindows = STAGNATION_PROBE_WINDOWS;
        descentCadenceWindows = DESCENT_PLATEAU_WINDOWS;
        recoveryCadenceWindows = RECOVERY_REASK_WINDOWS;
        parkReferenceThroughput = Double.NaN;
        windowOpenedAt = now;
        cooldownUntil = now.plus(REBALANCE_TARGET_FREEZE_COOLDOWN);
        lastDecisionReason = AdmissionDecisionReason.COOLDOWN;
    }

    /**
     * The engine's pause-poison lever, first post-resume tick (R13/KTD3): drops the in-progress window's samples
     * (pre-pause samples must not appear in the first post-resume window), ABORTS any in-flight probe restoring
     * its deferred target (a pause voids the probe's measurement), and stamps an invalidation boundary through
     * the law's estimator - entries predating a pause describe a plant an unknown span in the past, so history
     * and verdict die and the first bound post-resume windows land in the warmup band. The warmup EPISODE
     * deliberately survives (KTD2: pause/resume cycles share one allowance, so pause-cycling - PC's public
     * throttling idiom - cannot refill blind growth). No-op in DISABLED.
     */
    public void notifyPauseResumed() {
        if (mode == AdaptiveConcurrencyMode.DISABLED) {
            return;
        }
        synchronized (windowLock) {
            window.discard();
        }
        Instant now = clock.instant();
        windowOpenedAt = now;
        if (probeKind != ProbeKind.NONE) {
            int restored = clamp(probeDeferredRestoreTarget, AdmissionControlLaw.LIMIT_FLOOR_SLOTS, enforceCeiling);
            probeKind = ProbeKind.NONE;
            rollEscapeJitter();
            law.abortProbe(restored); // aborted, not concluded: restore, drop the evidence, allowance untouched
            if (restored != adaptiveTarget) {
                int previousTarget = adaptiveTarget;
                adaptiveTarget = restored;
                lastMovementAt = now;
                if (movementCounter != null) {
                    movementCounter.increment();
                }
                probeLog.info(LOG_PREFIX + " ({}): pause aborted the in-flight probe - target restored {} -> {} "
                        + "slot(s).", mode, previousTarget, restored);
            }
        }
        law.invalidateEstimator(AdmissionElasticityEstimator.InvalidationReason.PAUSE);
        floorWindowStreak = 0;
        plateauHoldStreak = 0;
        stagnationStreak = 0;
        recoveryReaskStreak = 0; // KTD3: a pause ends the park; its timer must not span the discontinuity
        parkReferenceThroughput = Double.NaN;
    }

    /**
     * Test seam (package-private): the live law instance, so same-package tests can prove reconstruction (new
     * instance, baseline zeroed) and survival (same instance) across rebalances. Null in DISABLED.
     */
    AdmissionControlLaw law() {
        return law;
    }

    /** Test seam (package-private): whether a U6 probe (escape or descent) is currently in flight. */
    boolean probeInFlight() {
        return probeKind != ProbeKind.NONE;
    }

    /** Test seam (package-private): the in-flight probe's deferred restore value (KTD4). */
    int probeDeferredRestoreTarget() {
        return probeDeferredRestoreTarget;
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
     * How many active-task snapshots have been recorded since construction. Zero always in DISABLED.
     */
    public long activeTaskSamplesRecorded() {
        synchronized (windowLock) {
            return activeTaskSamplesRecorded;
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
