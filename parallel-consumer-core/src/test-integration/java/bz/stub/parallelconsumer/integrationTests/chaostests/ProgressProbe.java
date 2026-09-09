package bz.stub.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import lombok.Getter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * First-class progress SLOs for chaos runs (Chaos Pain Suite Phase 1) - replaces "one big await" with
 * independent invariants sampled on a background thread. Violations carry enough context to be the
 * autopsy's headline. Deliberately asserts SLOs and invariants, never exact timings.
 * <p>
 * Probes (each threshold is a constant sized inside the measured gap between the healthy baseline and
 * the defect signature it discriminates - the per-bound arithmetic and its empirical basis live in
 * each constant's javadoc; margins vary per probe because the measured gaps do):
 * <ul>
 *   <li><b>Progress watermark</b>: while work remains, fleet-wide consumed count must advance within
 *   {@link #NO_PROGRESS_WINDOW} (generalises the confluentinc#857 investigation's "no progress for 11s" check).</li>
 *   <li><b>Instance progress</b> ({@code INSTANCE_STALL/NO_WORK_COMPLETED}): the same shape one
 *   granularity finer - for each LIVE fleet member that holds work (queued in shards or out for
 *   processing, read from PC's own {@code WorkManager}), a work result must be returned within
 *   {@link #INSTANCE_STALL_BOUND}. Wired per-run via {@link #withInstanceProgress}; inactive (like
 *   the watermark) in ambient mode. See the bound's javadoc for why this is instance-level rather
 *   than shard-level, and why it cannot fire on a merely-busy instance.</li>
 *   <li><b>Zombie-member / rebalance dwell</b>: the group must not dwell in
 *   {@code PREPARING_REBALANCE}/{@code COMPLETING_REBALANCE} beyond {@link #REBALANCE_DWELL_BOUND}.
 *   Keyed on protocol-unresponsiveness, NOT on "member holds partitions with zero consumption" - a
 *   legitimately draining member holds its assignment while finishing + committing (that is drain's
 *   purpose); what it must never do is block the group's rebalance. Healthy rebalances complete in
 *   seconds; {@link #REBALANCE_DWELL_BOUND} sits between the measured healthy peak (~6.7s) and the
 *   defect peak (~20.1s), cleanly separating them.</li>
 *   <li><b>Uncommitted completions</b> ({@code UNCOMMITTED_COMPLETIONS/COMMIT_NOT_LANDING}): per
 *   PARTITION, a member's own next-offset-to-commit must not stand above the group's committed
 *   offset, with that committed offset not moving and the group STABLE, for
 *   {@link UncommittedCompletionDetector#COMMIT_NOT_LANDING_SAMPLES} consecutive samples. Armed by
 *   the same {@link #withInstanceProgress} call; the detector owns why a difference between two
 *   positions discriminates where the demoted elapsed-time bound could not.</li>
 *   <li><b>Drain bound</b>: every STOP_DRAIN reported by the conductor must complete within
 *   {@link #DRAIN_BOUND}.</li>
 * </ul>
 * The end-of-run correctness ledger (no loss / bounded duplicates) is a static helper the test calls
 * after the fleet settles - see {@link #ledger}.
 * <p>
 * Two construction paths (one per {@link Mode}) share the same samplers:
 * <ul>
 *   <li><b>Chaos mode</b> ({@link #ProgressProbe(KafkaClientUtils, String, String, LongSupplier, long)}):
 *   all probes active, single topic, violations GATE the run (the chaos suite asserts them empty)
 *   while observations are reported only - see {@link #getObservations()}.</li>
 *   <li><b>Ambient observer mode</b> ({@link #ambientObserver(KafkaClientUtils, Supplier)}): flight
 *   recorder for every broker IT. Watches ALL topics the group has committed offsets for; the
 *   progress-watermark probe is inactive (no consumed-count supplier exists); violations and peaks are
 *   collected for the autopsy but NEVER gate - the chaos suite is the only place these
 *   chaos-calibrated thresholds are assertions.</li>
 * </ul>
 */
@Slf4j
public class ProgressProbe implements ChaosConductor.ChaosObserver {

    /** Fleet-wide consumption must advance at least this often while work remains. */
    public static final Duration NO_PROGRESS_WINDOW = Duration.ofSeconds(30);
    /**
     * The widened watermark for scenarios whose OWN churn legitimately pauses the whole fleet for
     * longer than {@link #NO_PROGRESS_WINDOW} - see {@link #withNoProgressWindow(Duration)}. Named
     * rather than repeated, because two scenarios now reach for the same number for two different
     * mechanisms and a future re-calibration must move both or neither:
     * <ul>
     *   <li>W4 ({@code AbstractRevokeUnderWorkScenario}): a storm-phase rebalance can pause much of
     *   the fleet for up to the eviction horizon, all of it under the eager assignor.</li>
     *   <li>W1 ({@code ChaosChurnStormIT}): the 45s heavy tail is redelivered by every eager
     *   revoke, so the fleet can sit wholly inside {@code HEAVY_SLEEP} with nothing completing while
     *   every member is working - the firings and their drain trajectories are in
     *   {@code docs/inflight/test-no-progress-window-may-not-transfer-to-w1.md}.</li>
     * </ul>
     * <b>It is a re-calibration, not a disabling</b>, and that is asserted rather than argued:
     * {@code NoProgressWindowIT} fires the detector at this bound on a fleet that genuinely stops.
     */
    public static final Duration CHURN_NO_PROGRESS_WINDOW = Duration.ofSeconds(60);
    /** Max continuous group-rebalancing dwell. Empirically calibrated (2026-07-30, seed 424242, same
     * schedule on both arms): healthy peak 6.7s (drainer participates, rebalance completes mid-drain) vs
     * defect peak 20.1s (protocol-absent drainer blocks the join until its LeaveGroup - the whole freeze
     * window, since PC's close bails on stragglers at ~11s). 15s = 2.2x the healthy peak, comfortably
     * inside the defect signature. NB the naive "5-min zombie" arithmetic doesn't survive contact with
     * the close path's give-up-on-stragglers behaviour - the freeze is drain-duration-bounded. */
    public static final Duration REBALANCE_DWELL_BOUND = Duration.ofSeconds(15);
    /** Drain-mode close must finish within this - MUST exceed the suite's heavy-tail sleep (a healthy
     * drain legitimately waits for the heaviest in-flight record) plus generous margin under load. */
    public static final Duration DRAIN_BOUND = Duration.ofSeconds(150);
    /** Progress watermark is skipped when this few records remain: the tail may be all heavy-tailed
     * records legitimately sleeping in-flight. The defect signature is a stall with THOUSANDS remaining. */
    public static final int TAIL_SLACK = 500;
    /** CLASS 2 probe (protocol-INVISIBLE stalls - the "locks forever, manual restart" confluentinc#857 reports):
     * no partition may hold real lag while its committed offset stagnates beyond this bound. Broker-side
     * clocks cannot see this class: the group is STABLE, heartbeats + polls flow, no rebalance is pending
     * so the 5-min eviction clock never starts - only lag observation (exactly how users notice) works.
     * Bound must exceed a heavy-tail REDELIVERY CHAIN: a hard stop can interrupt a heavy record
     * mid-dwell and at-least-once re-runs it fresh, so a partition's committed offset can be
     * legitimately blocked ~2 chained dwells (measured: 151s at a 90s dwell - hence the dwell was
     * reduced to 45s; 2x45=90s vs this 150s bound). A second legit-freeze class was measured during W4
     * calibration: EAGER reassignment restarts in-flight heavies on every storm membership change,
     * pinning commit low-watermarks for storm+dwell+slack - scenarios must keep that arithmetic under
     * this bound (see ChaosRevokeUnderWorkIT).
     * <p>
     * <b>Crossing this bound is an {@link #getObservations() observation}, not a violation - it does
     * not fail the run.</b> RED calibration was never achieved and is now closed rather than open:
     * three diagnostic replays, two of them of the seeds the sightings ledger itself nominated as its
     * best evidence, all crossed this bound and then drained completely. What the bound measures is
     * how long a watermark stays pinned, which is a speed question; the liveness question it was
     * standing in for belongs to {@link #INSTANCE_STALL_BOUND} - <b>but only at instance
     * granularity.</b> Demoting this detector therefore REDUCED per-shard liveness coverage rather
     * than relocating it: see {@link #INSTANCE_STALL_BOUND}'s own granularity note. Half of that
     * reduction was paid back on 2026-09-09 by {@link UncommittedCompletionDetector}, which gates on
     * the difference between a member's own next-offset-to-commit and the group's committed offset -
     * the correlated signal {@code docs/inflight/test-per-shard-liveness-has-no-gate.md} specified,
     * with the red control that note required. That detector and this one are complementary on the
     * same partition: <b>this observation firing while it stays silent is the slow case</b>, because
     * an incomplete record pins the local watermark too. The peak is still always measured - a timing
     * regression must stay visible, it just must not turn a correctness suite red. */
    public static final Duration LAG_STAGNATION_BOUND = Duration.ofSeconds(150);
    /**
     * Appended to every Class 2 observation so the interpretation arrives WITH the finding, not in a
     * document the reader must first decide to open. The gap it closes cost a measured day: three of
     * four 2026-08-19 arms failed this check while progressing normally, because the natural reading
     * of the bare message - "the library has stalled" - is wrong. That reading is now also what the
     * demotion to an observation prevents structurally, but the text stays: a green run's log is
     * read by someone who has no failure to prompt them to look it up.
     */
    static final String CLASS2_INTERPRETATION =
            "NOTE: this bound is a TIMING measurement, not a correctness verdict, and since 2026-08-25 "
                    + "it is an OBSERVATION that does not fail the run. A partition's committed offset "
                    + "cannot advance past one incomplete record, so a slow or repeatedly-redelivered "
                    + "record pins the watermark while the shard behind it completes work normally - a "
                    + "busy fleet and a wedged one are indistinguishable to it. Three replays now say "
                    + "so: seed 4734674029169027864 trips it 53 times on the eager arm and drains; "
                    + "seed 6825864417772979246 (the sightings ledger's own master-control seed) trips "
                    + "it twice and drains to inFlight=0; seed 4044221734199516240 trips it 46 times on "
                    + "the drain arm and drains. The gating liveness claim is INSTANCE_STALL, which "
                    + "watches completions and so cannot fire on slow-but-progressing - but it is "
                    + "per-INSTANCE, so a single wedged shard on an instance whose other shards keep "
                    + "completing is invisible to it. If you are here because a watermark froze while "
                    + "the fleet stayed busy: since 2026-09-09 the COMMIT half of that gap gates, as "
                    + "UNCOMMITTED_COMPLETIONS/COMMIT_NOT_LANDING - so this observation arriving with "
                    + "no such violation beside it says the completed work HAS reached the broker and "
                    + "the watermark is pinned by an incomplete record, which is the slow case. What "
                    + "still gates nothing is a key-order SHARD that will never be dispatched again "
                    + "inside a partition whose local watermark is pinned anyway; that is the case to "
                    + "rule out by hand. See docs/solutions/best-practices/a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md "
                    + "and docs/inflight/test-per-shard-liveness-has-no-gate.md";
    /** Ignore trivial tails - the Class 2 signature is real backlog going nowhere. */
    public static final long LAG_STAGNATION_MIN_LAG = 50;
    /**
     * The instance-progress bound - {@link InstanceStallDetector#INSTANCE_STALL_BOUND} owns it and its
     * rationale. Aliased here because {@code AmbientProbeExtension} and the records cite it by this name.
     */
    public static final Duration INSTANCE_STALL_BOUND = InstanceStallDetector.INSTANCE_STALL_BOUND;
    private static final Duration SAMPLE_INTERVAL = Duration.ofSeconds(1);
    /** A probe that cannot sample is a probe silently passing - after this many consecutive sampling
     * failures the degradation itself becomes a violation (false-GREEN guard), instead of the run
     * quietly flying blind. Transient admin hiccups under chaos stay tolerated below the threshold. */
    static final int MAX_CONSECUTIVE_SAMPLE_FAILURES = 10;

    /**
     * The probe's construction mode - the single authoritative switch the samplers consult.
     * {@link #topic} / {@link #totalConsumed} being null is mode-associated data absence, never the
     * mode signal itself.
     */
    public enum Mode {
        /** All probes active, single topic, violations GATE the run (the chaos suite asserts them
         * empty); {@link #getObservations() observations} are reported and never gate. */
        CHAOS("chaos-progress-probe", "chaos-probe"),
        /** Flight recorder for every broker IT: admin-read samplers only, all topics, violations NEVER gate. */
        AMBIENT_OBSERVER("ambient-probe-sampler", "ambient-probe");

        final String threadName;
        /** log tag distinguishing chaos-gating output from ambient flight-recorder output */
        final String logTag;

        Mode(String threadName, String logTag) {
            this.threadName = threadName;
            this.logTag = logTag;
        }
    }

    private final Mode mode;
    private final KafkaClientUtils kcu;
    /** Supplier, not a snapshot: ambient mode must follow tests that switch to a NEW_GROUP mid-test. */
    private final Supplier<String> groupIdSupplier;
    /** Chaos mode's single watched topic; null in {@link Mode#AMBIENT_OBSERVER} (all topics watched). */
    private final String topic;
    /** Chaos mode's fleet consumed-count; null in {@link Mode#AMBIENT_OBSERVER} (progress watermark inactive). */
    private final LongSupplier totalConsumed;
    private final long expectedTotal;

    /**
     * The instance-progress detector and its instruments - {@link InstanceStallDetector} owns the
     * bound's rationale, the busy-worker rule, the tokens and the dumps; this probe owns the sampler
     * thread that drives it and the sinks its findings land in.
     */
    private final InstanceStallDetector.FindingSink findingSink = new InstanceStallDetector.FindingSink() {
        @Override
        public void violate(String message) {
            ProgressProbe.this.violate(message);
        }

        @Override
        public void observe(String message) {
            ProgressProbe.this.observe(message);
        }
    };

    private final InstanceStallDetector instanceStall = new InstanceStallDetector(findingSink);

    /**
     * The per-partition liveness gate - {@link UncommittedCompletionDetector} owns the property, the
     * sample count and why the signal is a difference between two positions rather than an elapsed
     * time. Armed by the same {@link #withInstanceProgress} call the instance-stall detector is, since
     * both read the same live fleet view; sampled from {@link #sampleLagStagnation}, which has already
     * paid for the committed-offset round trip it needs.
     */
    private final UncommittedCompletionDetector uncommittedCompletions =
            new UncommittedCompletionDetector(findingSink);

    /**
     * The widest locally-completed-but-uncommitted gap seen, in records, for the end-of-run peaks line
     * - measured whether or not anything gated, the invariant {@code recordLagStagnation} states.
     */
    public long getPeakUncommittedCompletions() {
        return uncommittedCompletions.getPeakUncommittedCompletions();
    }

    /** The longest hold-work-return-nothing stretch seen, for the end-of-run peaks line. */
    public long getPeakInstanceStallMs() {
        return instanceStall.getPeakInstanceStallMs();
    }

    /** per-partition committed-offset watermarks for the Class 2 (lag stagnation) probe */
    private final Map<TopicPartition, Long> lastCommitted = new ConcurrentHashMap<>();
    private final Map<TopicPartition, Instant> lastCommittedMove = new ConcurrentHashMap<>();
    /** Latest per-partition lag observation - the autopsy's frozen-partition detail. */
    @Getter
    private final Map<TopicPartition, PartitionLagSnapshot> partitionLagSnapshots = new ConcurrentHashMap<>();

    @Getter
    private final List<String> violations = Collections.synchronizedList(new ArrayList<>());
    /**
     * Findings that are MEASURED and REPORTED but never gate - {@link #getViolations()}'s
     * non-failing sibling. A detector belongs here when what it measures is a timing property
     * rather than a correctness one, so that crossing its bound is a statement about speed and
     * cannot by itself mean the system is wrong.
     * <p>
     * Only {@code CLASS2_STALL/LAG_STAGNATION} is here today, demoted on 2026-08-25 after two
     * diagnostic replays of the sightings ledger's own nominated seeds fired it and then drained
     * completely - see {@link #CLASS2_INTERPRETATION} for the evidence and
     * {@code docs/inflight/bug-857-family.md} for the ledger those replays settle.
     */
    @Getter
    private final List<String> observations = Collections.synchronizedList(new ArrayList<>());
    private final Map<Integer, Instant> outstandingDrains = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread samplerThread;

    private long lastCount = -1;
    private Instant lastAdvance = Instant.now();
    /**
     * The sampler's clock, so {@code NoProgressWindowIT} can drive {@link #sampleProgress} without
     * spending wall time. Production reads {@link Instant#now()} and nothing else changes: the field
     * exists because the ONLY thing that can catch the sampler ceasing to consult
     * {@link #recordFleetProgress} is a test that runs the sampler, and every other test here drives
     * the decision method directly - a gap two independent reviews of astubbs/parallel-consumer#499
     * found at the same time.
     */
    private volatile Supplier<Instant> clock = Instant::now;
    private Instant rebalanceDwellStart = null;
    /**
     * The group state the dwell sampler last read, handed to {@link UncommittedCompletionDetector} so
     * it can decline to accumulate through a rebalance.
     * <p>
     * Carried across rather than re-read because the lag sampler runs at a fifth of the dwell
     * sampler's cadence and a second {@code describeConsumerGroups} round trip per lag sample would
     * buy nothing: the state it would read is at most one second newer. {@code UNKNOWN} until the
     * first successful read, which re-arms the detector rather than arming it - an unread group must
     * not gate.
     */
    private volatile ConsumerGroupState lastGroupState = ConsumerGroupState.UNKNOWN;
    /** Peak signatures observed - logged at stop(); the empirical basis for threshold calibration. */
    @Getter
    private volatile long peakRebalanceDwellMs = 0;
    @Getter
    private volatile long peakDrainDurationMs = 0;
    @Getter
    private volatile long peakLagStagnationMs = 0;

    /** Per-scenario toggles - defaults preserve W1 behaviour. The dwell PEAK is always measured;
     * disabling only suppresses the violation (a scenario must never lose the measurement). */
    private volatile boolean rebalanceDwellViolationEnabled = true;
    private volatile Duration noProgressWindow = NO_PROGRESS_WINDOW;

    /**
     * W4 uses this: with a low {@code max.poll.interval.ms} the deadlock-blocked rebalances self-resolve
     * by eviction, and Class 1 dwell violations would otherwise fire first and mask the Class 2
     * measurement this scenario exists for. Class 1 stays covered by W1, where the probe is armed.
     */
    public ProgressProbe disableRebalanceDwellViolation() {
        this.rebalanceDwellViolationEnabled = false;
        return this;
    }

    /** Widen the fleet-wide progress watermark for scenarios whose churn legitimately pauses everyone
     * (an eager rebalance revokes every member's partitions) longer than the W1 default tolerates. */
    public ProgressProbe withNoProgressWindow(Duration window) {
        this.noProgressWindow = window;
        return this;
    }

    /**
     * Arms the instance-progress probe ({@code INSTANCE_STALL/NO_WORK_COMPLETED} - see
     * {@link #INSTANCE_STALL_BOUND}) with a live view of the fleet. A supplier because the fleet
     * GROWS during a run (JOIN_NEW); {@code ChaosScenarioBase#startRun} wires it from the conductor
     * for every chaos scenario. Never wired in ambient mode, which has no fleet to watch.
     */
    public ProgressProbe withInstanceProgress(Supplier<List<InstanceProgressView>> fleetSupplier) {
        instanceStall.watch(fleetSupplier);
        // One call arms both fleet-reading detectors: they watch the same members, and a scenario that
        // armed one and not the other would gate on instance liveness while silently keeping the
        // per-partition blind spot this suite just closed.
        uncommittedCompletions.watch(fleetSupplier);
        return this;
    }

    /**
     * Replaces the thread-dump reader for a broker-free seam test - see
     * {@code InstanceStallDetector#threadDumpSource} for why counting the calls is the property, and
     * package-private because no run may swap it.
     */
    ProgressProbe withThreadDumpSource(IntFunction<String> source) {
        instanceStall.threadDumpSource(source);
        return this;
    }

    /** Chaos-mode construction: all probes active, single topic - the W1/W4 gating path. */
    public ProgressProbe(KafkaClientUtils kcu, String groupId, String topic, LongSupplier totalConsumed, long expectedTotal) {
        this(kcu, () -> groupId,
                Objects.requireNonNull(topic, "chaos mode requires a topic - use ambientObserver() for the all-topics observer"),
                Objects.requireNonNull(totalConsumed, "chaos mode requires a totalConsumed supplier - use ambientObserver() for the all-topics observer"),
                expectedTotal, Mode.CHAOS);
    }

    /**
     * Ambient observer-mode construction (see class javadoc): admin-read samplers only (rebalance
     * dwell + all-topic lag stagnation), progress watermark inactive. Tolerates the admin client not
     * existing yet ({@code kcu.open()} runs after extension callbacks start the probe) - samples are
     * silently skipped until it appears, and a group that never forms simply never trips anything.
     */
    public static ProgressProbe ambientObserver(KafkaClientUtils kcu, Supplier<String> groupIdSupplier) {
        return new ProgressProbe(kcu, groupIdSupplier, null, null, 0, Mode.AMBIENT_OBSERVER);
    }

    private ProgressProbe(KafkaClientUtils kcu, Supplier<String> groupIdSupplier, String topic,
                          LongSupplier totalConsumed, long expectedTotal, Mode mode) {
        this.kcu = kcu;
        this.groupIdSupplier = groupIdSupplier;
        this.topic = topic;
        this.totalConsumed = totalConsumed;
        this.expectedTotal = expectedTotal;
        this.mode = mode;
    }

    /**
     * A chaos-mode probe for a test that drives a sampler seam directly, with no broker.
     * <p>
     * The {@code null} {@link KafkaClientUtils} is legal because no sampler thread is started, so
     * nothing ever reaches a cluster - and that invariant lives here, next to the field it depends on,
     * rather than being re-explained at each call site. Three tests had grown their own copy of this
     * constructor call plus their own wording of the same reason, which is the drift
     * {@code AGENTS.md}'s "reuse test utilities - search before you add" rule exists to stop.
     *
     * @param groupId only appears in violation text; no group is joined
     * @param topic   only appears in violation text; no topic is read
     */
    static ProgressProbe forSeamTest(String groupId, String topic) {
        return forSeamTest(groupId, topic, 0);
    }

    /**
     * As {@link #forSeamTest(String, String)}, for a seam whose decision depends on the backlog size
     * - {@link #recordFleetProgress}'s {@link #TAIL_SLACK} term reads it, so a zero total makes every
     * consumed count look like the tail and the detector silent for reasons the test never intended.
     */
    static ProgressProbe forSeamTest(String groupId, String topic, long expectedTotal) {
        return forSeamTest(groupId, topic, expectedTotal, () -> 0L);
    }

    /**
     * As above, with a live consumed-count supplier - for a test that drives {@link #sampleProgress}
     * itself rather than {@link #recordFleetProgress}, which is the only way to catch the sampler
     * ceasing to consult the decision at all.
     */
    static ProgressProbe forSeamTest(String groupId, String topic, long expectedTotal, LongSupplier totalConsumed) {
        return new ProgressProbe(null, groupId, topic, totalConsumed, expectedTotal);
    }

    /** Observer mode never gates - violations are autopsy material only (ambient flight recorder). */
    public boolean isObserverMode() {
        return mode == Mode.AMBIENT_OBSERVER;
    }

    public void start() {
        running.set(true);
        lastAdvance = Instant.now();
        samplerThread = new Thread(this::sampleLoop, mode.threadName);
        samplerThread.setDaemon(true);
        samplerThread.start();
    }

    /**
     * Stops sampling and returns accumulated violations (also available via {@link #getViolations()}).
     * Idempotent - safe to call repeatedly and before {@link #start()} (the ambient extension stops in
     * {@code afterTestExecution} plus an {@code afterEach} safety net).
     */
    public List<String> stop() {
        running.set(false);
        if (samplerThread != null) {
            samplerThread.interrupt();
            try {
                samplerThread.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (isObserverMode()) {
            // quiet flight recorder: the extension owns end-of-test reporting (autopsy / DEBUG one-liner)
            log.debug("[{}] peaks: maxRebalanceDwell={}ms maxDrainDuration={}ms maxLagStagnation={}ms maxInstanceStall={}ms maxUncommittedCompletions={}records",
                    mode.logTag, peakRebalanceDwellMs, peakDrainDurationMs, peakLagStagnationMs,
                    instanceStall.getPeakInstanceStallMs(), uncommittedCompletions.getPeakUncommittedCompletions());
        } else {
            log.info("[{}] peaks: maxRebalanceDwell={}ms maxDrainDuration={}ms maxLagStagnation={}ms maxInstanceStall={}ms maxUncommittedCompletions={}records",
                    mode.logTag, peakRebalanceDwellMs, peakDrainDurationMs, peakLagStagnationMs,
                    instanceStall.getPeakInstanceStallMs(), uncommittedCompletions.getPeakUncommittedCompletions());
        }
        return getViolations();
    }

    public boolean hasViolations() {
        return !violations.isEmpty();
    }

    // --- ChaosObserver: drain-bound bookkeeping ---
    @Override
    public void onAction(int instanceId, ChaosConductor.ChaosAction action) {
        if (action == ChaosConductor.ChaosAction.STOP_DRAIN) {
            outstandingDrains.put(instanceId, Instant.now());
        }
    }

    @Override
    public void onDrainComplete(int instanceId) {
        Instant started = outstandingDrains.remove(instanceId);
        if (started != null) {
            long ms = Duration.between(started, Instant.now()).toMillis();
            if (ms > peakDrainDurationMs) peakDrainDurationMs = ms;
        }
    }

    private void sampleLoop() {
        int tick = 0;
        int consecutiveFailures = 0;
        while (running.get()) {
            try {
                Thread.sleep(SAMPLE_INTERVAL.toMillis());
                if (!isObserverMode()) {
                    sampleProgress();
                    sampleInstanceProgress(Instant.now());
                }
                sampleRebalanceDwell();
                sampleDrains();
                if (++tick % 5 == 0) {
                    sampleLagStagnation(); // heavier admin round-trip; 5s cadence is ample vs a 150s bound
                }
                consecutiveFailures = 0;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                // transient admin hiccups under chaos are expected; a PERSISTENTLY failing probe is
                // blind and must fail loud - a blind probe would otherwise pass any run GREEN
                if (++consecutiveFailures >= MAX_CONSECUTIVE_SAMPLE_FAILURES) {
                    violate("PROBE_DEGRADED: sampling failed " + consecutiveFailures
                            + " consecutive times (last: " + e + ") - the probe was blind; this run's GREEN cannot be trusted");
                    consecutiveFailures = 0; // re-arm: one violation per degradation episode
                } else {
                    log.debug("Probe sample error {}/{} (continuing): {}",
                            consecutiveFailures, MAX_CONSECUTIVE_SAMPLE_FAILURES, e.getMessage());
                }
            }
        }
    }

    /**
     * Package-private, not private, so a test can run the sampler itself - see {@link #clock}. Called
     * only from {@link #sampleLoop} in production.
     */
    void sampleProgress() {
        long now = totalConsumed.getAsLong();
        if (now != lastCount) {
            lastCount = now;
            lastAdvance = clock.get();
            return;
        }
        if (recordFleetProgress(now, Duration.between(lastAdvance, clock.get()))) {
            lastAdvance = clock.get(); // re-arm so a genuine stall reports once per window, not per sample
        }
    }

    /** Test seam for {@link #clock} - see that field for why it exists. */
    ProgressProbe withClock(Supplier<Instant> testClock) {
        this.clock = testClock;
        this.lastAdvance = testClock.get();
        return this;
    }

    /**
     * The window this probe is actually configured with. Exists so a test can assert that a SCENARIO
     * wired the bound it meant to, rather than re-applying the constant to a probe of its own and
     * asserting about that - the second half of the same gap {@link #clock} names.
     */
    Duration noProgressWindow() {
        return noProgressWindow;
    }

    /**
     * The NO_PROGRESS decision, split from its sampler so {@code NoProgressWindowIT} can drive it
     * with no broker and no wall clock - the {@link #recordRebalanceDwell} seam again, for the same
     * reason and under the same thread rule: this touches only the volatile window and the
     * synchronized violations list, never the sampler-confined clock fields, which is why the CALLER
     * re-arms rather than this method. Keep it that way or those tests become a data race.
     * <p>
     * Both terms are guards, and which one a re-calibration should move is not interchangeable.
     * {@link #TAIL_SLACK} excuses the tail; the window excuses the pause. Widening the slack far
     * enough to cover a churn scenario's firings would blind the detector to the "stall with
     * THOUSANDS remaining" its own javadoc names as the defect signature, so the window is the term
     * that moves - see {@link #CHURN_NO_PROGRESS_WINDOW}.
     *
     * @return whether a violation fired, i.e. whether the caller should re-arm the progress clock
     */
    boolean recordFleetProgress(long consumed, Duration stalled) {
        boolean workRemains = consumed < expectedTotal - TAIL_SLACK;
        if (!workRemains || stalled.compareTo(noProgressWindow) <= 0) {
            return false;
        }
        violate("NO_PROGRESS: fleet consumed count stuck at " + consumed + "/" + expectedTotal
                + " for " + stalled.getSeconds() + "s (bound " + noProgressWindow.getSeconds() + "s)");
        return true;
    }

    /**
     * One compact token per fleet member for a diagnostic run's log - see
     * {@link InstanceStallDetector#snapshot}, which owns the format and its blind spot. Kept here by
     * this name because {@code ChaosScenarioBase#logDiagnosticProgress} and the records cite it.
     */
    String instanceProgressSnapshot() {
        return instanceStall.snapshot();
    }

    /**
     * The instance-progress detector's sample - see {@link InstanceStallDetector#sample}. Kept here,
     * package-private and by this name, because {@code InstanceStallProbeIT} drives it and the
     * records cite it; the logic lives in the detector.
     */
    void sampleInstanceProgress(Instant now) {
        instanceStall.sample(now);
    }

    private void sampleRebalanceDwell() throws Exception {
        var adminOpt = kcu.adminIfOpen();
        if (!adminOpt.isPresent()) return; // outside the open()..close() window - skip this sample
        var admin = adminOpt.get();
        String groupId = groupIdSupplier.get();
        var group = admin.describeConsumerGroups(of(groupId)).all()
                .get(5, java.util.concurrent.TimeUnit.SECONDS).get(groupId);
        ConsumerGroupState state = group.state();
        lastGroupState = state; // see the field: the lag sampler reads it rather than paying for its own round trip
        boolean rebalancing = state == ConsumerGroupState.PREPARING_REBALANCE
                || state == ConsumerGroupState.COMPLETING_REBALANCE;
        if (!rebalancing) {
            rebalanceDwellStart = null;
            return;
        }
        if (rebalanceDwellStart == null) {
            rebalanceDwellStart = Instant.now();
            return;
        }
        if (recordRebalanceDwell(Duration.between(rebalanceDwellStart, Instant.now()), groupId, state)) {
            rebalanceDwellStart = Instant.now(); // re-arm
        }
    }

    /**
     * Classify one rebalance-dwell sample: always update the peak, and violate only when this
     * scenario has the Class 1 detector armed.
     * <p>
     * <b>Measuring the peak is unconditional on the toggle, and that is the invariant.</b> A scenario
     * whose own disturbances legitimately cross this bound suppresses the VIOLATION; it must never
     * lose the MEASUREMENT, or disabling the detector would quietly delete the evidence that would
     * later re-calibrate it. Extracted as a broker-free seam so that invariant is asserted rather than
     * assumed - {@code docs/inflight/test-chaos-phase2.md} records it as having had no fast coverage,
     * on the grounds that the samplers were private.
     *
     * <b>Safe to call from a thread other than the sampler, and that is not an accident.</b> It
     * touches only safely-published state - the volatile peak, the volatile enable flag, and the
     * synchronized violations list - and never the sampler-confined clock fields (`rebalanceDwellStart`
     * and friends), which is why the caller re-arms rather than this method. That is what makes
     * {@code RebalanceDwellToggleIT} driving it from the test thread legitimate rather than racy.
     * Keep it that way: reaching for a plain field here would silently make those tests a data race.
     *
     * @return whether a violation fired, i.e. whether the caller should re-arm the dwell clock
     */
    boolean recordRebalanceDwell(Duration dwell, String groupId, ConsumerGroupState state) {
        if (dwell.toMillis() > peakRebalanceDwellMs) {
            peakRebalanceDwellMs = dwell.toMillis();
        }
        if (!rebalanceDwellViolationEnabled || dwell.compareTo(REBALANCE_DWELL_BOUND) <= 0) {
            return false;
        }
        violate("ZOMBIE_MEMBER/REBALANCE_BLOCKED: group '" + groupId + "' dwelling in " + state
                + " for " + dwell.getSeconds() + "s (bound " + REBALANCE_DWELL_BOUND.getSeconds()
                + "s) - a member is not answering the rebalance (protocol-unresponsive)");
        return true;
    }

    /**
     * CLASS 2 detector: per-partition "real lag + stagnant committed offset" - catches
     * protocol-invisible stalls (counter drift, stuck throttle-pause) that every broker clock misses,
     * including PARTIAL stalls that a fleet-wide consumption counter hides behind healthy siblings.
     */
    private void sampleLagStagnation() throws Exception {
        var adminOpt = kcu.adminIfOpen();
        if (!adminOpt.isPresent()) return; // outside the open()..close() window - skip this sample
        var admin = adminOpt.get();
        String groupId = groupIdSupplier.get();
        var committedMap = admin.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata().get(5, java.util.concurrent.TimeUnit.SECONDS);
        var offsetSpecs = new java.util.HashMap<TopicPartition, org.apache.kafka.clients.admin.OffsetSpec>();
        for (var tp : committedMap.keySet()) {
            // chaos mode watches its single topic; the ambient observer watches everything the group commits to
            if (isObserverMode() || tp.topic().equals(topic)) {
                offsetSpecs.put(tp, org.apache.kafka.clients.admin.OffsetSpec.latest());
            }
        }
        if (offsetSpecs.isEmpty()) return;
        var endOffsets = admin.listOffsets(offsetSpecs).all().get(5, java.util.concurrent.TimeUnit.SECONDS);
        Instant now = Instant.now();
        for (var entry : endOffsets.entrySet()) {
            var tp = entry.getKey();
            var committedMeta = committedMap.get(tp);
            if (committedMeta == null) continue;
            long committed = committedMeta.offset();
            long end = entry.getValue().offset();
            long lag = end - committed;
            Long previous = lastCommitted.put(tp, committed);
            boolean moved = previous == null || committed != previous;
            if (moved) {
                lastCommittedMove.put(tp, now);
            }
            Instant since = lastCommittedMove.getOrDefault(tp, now);
            partitionLagSnapshots.put(tp, new PartitionLagSnapshot(tp, committed, end, lag, since));
            // Sampled on EVERY pass, including one where the committed offset just moved - the detector
            // re-arms on movement itself, and skipping the moved case would leave it reading a stretch
            // as continuous across a commit that landed inside it.
            sampleUncommittedCompletions(tp, committed, lag);
            if (moved) continue;
            long stagnantMs = Duration.between(since, now).toMillis();
            if (recordLagStagnation(tp, committed, lag, stagnantMs)) {
                lastCommittedMove.put(tp, now); // re-arm
            }
        }
    }

    /**
     * Classify one partition's stagnation sample: always update the peak, and record an
     * {@link #getObservations() observation} when the bound is crossed. Extracted from the admin
     * round-trip above so the classification has a broker-free seam - the samplers are otherwise
     * only reachable through a live cluster, which is why this rule had no fast coverage while it
     * was gating (recorded as open work in {@code docs/inflight/test-chaos-phase2.md}).
     * <p>
     * <b>Measuring the peak is unconditional on the bound, deliberately.</b> Suppressing the finding
     * must never lose the measurement - that is the same invariant the per-scenario dwell toggle
     * holds, and it is the whole reason a demoted detector still earns its keep.
     *
     * <b>Safe to call off the sampler thread</b>, for the same reason and under the same constraint as
     * {@link #recordRebalanceDwell}: only the volatile peak and the synchronized observations list are
     * touched here, never the sampler-confined `lastCommittedMove` bookkeeping the caller owns.
     *
     * @return whether the bound was crossed, i.e. whether the caller should re-arm this partition
     */
    boolean recordLagStagnation(TopicPartition tp, long committed, long lag, long stagnantMs) {
        if (lag < LAG_STAGNATION_MIN_LAG) {
            return false;
        }
        if (stagnantMs > peakLagStagnationMs) {
            peakLagStagnationMs = stagnantMs;
        }
        if (stagnantMs <= LAG_STAGNATION_BOUND.toMillis()) {
            return false;
        }
        observe("CLASS2_STALL/LAG_STAGNATION: partition " + tp + " lag=" + lag
                + " with committed offset stagnant at " + committed + " for " + (stagnantMs / 1000)
                + "s (bound " + LAG_STAGNATION_BOUND.getSeconds() + "s). "
                + CLASS2_INTERPRETATION);
        return true;
    }

    /**
     * The per-partition liveness detector's sample - see {@link UncommittedCompletionDetector#sample},
     * which owns the rule. Kept here, package-private and by this name, because
     * {@code UncommittedCompletionProbeIT} and {@code WedgedPartitionRedControlIT} drive it and the
     * records cite it; the logic lives in the detector.
     *
     * @return whether the detector fired on this sample
     */
    boolean sampleUncommittedCompletions(TopicPartition tp, long committed, long lag) {
        return uncommittedCompletions.sample(tp, committed, lag, lastGroupState);
    }

    /**
     * Sets the group state the per-partition detector sees, for a broker-free test that drives
     * {@link #sampleUncommittedCompletions} without a group to describe. Package-private because no
     * real run may set it - {@link #sampleRebalanceDwell} is the only writer there.
     */
    void withGroupStateForSeamTest(ConsumerGroupState state) {
        this.lastGroupState = state;
    }

    /**
     * One partition's latest lag-sample observation. The ambient autopsy uses these for the
     * frozen-committed detail; stagnation is measured from the last time the committed offset moved.
     */
    @Value
    public static class PartitionLagSnapshot {
        TopicPartition topicPartition;
        long committed;
        long endOffset;
        long lag;
        Instant committedLastMovedAt;

        /** Seconds the committed offset has been stagnant, as of NOW (call at report time). */
        public long stagnantSeconds() {
            return Duration.between(committedLastMovedAt, Instant.now()).getSeconds();
        }
    }

    private void sampleDrains() {
        for (Map.Entry<Integer, Instant> drain : outstandingDrains.entrySet()) {
            Duration elapsed = Duration.between(drain.getValue(), Instant.now());
            if (elapsed.compareTo(DRAIN_BOUND) > 0) {
                violate("DRAIN_OVERDUE: instance " + drain.getKey() + " draining for " + elapsed.getSeconds()
                        + "s (bound " + DRAIN_BOUND.getSeconds() + "s)");
                outstandingDrains.remove(drain.getKey()); // report once
            }
        }
    }

    /**
     * Record a finding that is reported but never fails the run - the non-gating counterpart of
     * {@link #violate(String)}. Logged at WARN so it is visible in a green run's output, which is
     * where a timing regression has to be noticed: nothing turns red to point at it.
     */
    private void observe(String message) {
        record(observations, message, /* gating */ false);
    }

    private void violate(String message) {
        record(violations, message, /* gating */ true);
    }

    /**
     * The one place a finding is stored and announced, shared by {@link #violate} and
     * {@link #observe} so the mode rule below cannot drift between them.
     * <p>
     * <b>Silent-on-green contract.</b> In observer mode a finding can occur during a PASSING test, so
     * the failure-time autopsy is the reporting surface and the live log stays at DEBUG. Outside it
     * the finding is announced as it happens: gating findings at ERROR because they will fail the
     * run, non-gating ones at WARN because nothing else will ever point at them.
     * <p>
     * <b>The non-gating text is load-bearing beyond this file.</b> {@code bin/chaos-test.sh} counts
     * observations per scenario by matching {@code OBSERVATION (does not fail the run)} literally, so
     * changing that string silently reports zero observations in the CI job summary. Its
     * {@code OBSERVATION_MARKER} is the other half of the pair - change both or neither.
     */
    private void record(List<String> sink, String message, boolean gating) {
        sink.add(message);
        if (isObserverMode()) {
            log.debug("[{}] {} recorded: {}", mode.logTag, gating ? "violation" : "observation", message);
        } else if (gating) {
            log.error("[{}] VIOLATION: {}", mode.logTag, message);
        } else {
            log.warn("[{}] OBSERVATION (does not fail the run): {}", mode.logTag, message);
        }
    }

    /**
     * End-of-run correctness ledger. No record may EVER be lost (at-least-once); duplicates are legal but
     * must stay bounded to the uncommitted tails of disturbed drains/stops - a per-disturbance
     * capacity-shaped allowance (the {@code DrainingMemberRebalanceIT} lesson: never a fraction of
     * throughput).
     *
     * @param perDisturbanceAllowance duplicates allowed per drain/stop disturbance (in-flight batch +
     *                                commit-interval lag for one instance)
     * @return list of ledger violations (empty = balanced)
     */
    public static List<String> ledger(java.util.Set<String> expectedKeys,
                                      java.util.Collection<String> allConsumedKeysWithDuplicates,
                                      int disturbanceCount,
                                      int perDisturbanceAllowance) {
        List<String> problems = new ArrayList<>();
        var unique = new java.util.HashSet<>(allConsumedKeysWithDuplicates);
        var missing = new java.util.HashSet<>(expectedKeys);
        missing.removeAll(unique);
        if (!missing.isEmpty()) {
            problems.add("LEDGER_LOSS: " + missing.size() + " produced records never consumed (sample: "
                    + missing.stream().limit(5).collect(java.util.stream.Collectors.toList()) + ")");
        }
        long duplicates = allConsumedKeysWithDuplicates.size() - unique.size();
        long rawAllowance = (long) disturbanceCount * perDisturbanceAllowance;
        // cap: on a stormy run (many disturbances) the per-disturbance sum can exceed the total volume,
        // making the bound vacuous - duplicating more than half of everything produced is pathological
        // no matter how disturbed the run was
        long allowance = Math.min(rawAllowance, expectedKeys.size() / 2L);
        if (duplicates > allowance) {
            problems.add("LEDGER_DUPLICATES: " + duplicates + " duplicate deliveries exceeds allowance "
                    + allowance + " (" + disturbanceCount + " disturbances x " + perDisturbanceAllowance
                    + (allowance < rawAllowance ? ", capped at half of expected " + expectedKeys.size() : "") + ")");
        }
        log.info("[chaos-ledger] expected={} uniqueConsumed={} duplicates={} allowance={} (raw={})",
                expectedKeys.size(), unique.size(), duplicates, allowance, rawAllowance);
        return problems;
    }
}
