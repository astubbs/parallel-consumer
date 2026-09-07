package bz.stub.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import bz.stub.parallelconsumer.integrationTests.utils.ManagedPCInstance;
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
     * than relocating it: see {@link #INSTANCE_STALL_BOUND}'s own granularity note, and
     * {@code docs/inflight/test-per-shard-liveness-has-no-gate.md} for what is uncovered and the
     * correlated gate that would close it. The peak is still always measured - a timing regression
     * must stay visible, it just must not turn a correctness suite red. */
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
                    + "completing is covered by NOTHING that gates. If you are here because a "
                    + "watermark froze while the fleet stayed busy, that gap is the case to rule out "
                    + "by hand. See docs/solutions/best-practices/a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md "
                    + "and docs/inflight/test-per-shard-liveness-has-no-gate.md";
    /** Ignore trivial tails - the Class 2 signature is real backlog going nowhere. */
    public static final long LAG_STAGNATION_MIN_LAG = 50;
    /**
     * INSTANCE-progress probe: no live instance may hold work while returning no work result for
     * longer than this. The liveness claim it makes is the one the Class 2 lag bound only
     * approximates: {@code CLASS2_STALL} watches a partition's COMMITTED offset, which one incomplete
     * record legitimately pins while the shard behind it completes work continuously - so a busy
     * fleet and a wedged fleet look identical to it (measured 2026-08-19, seed 4734674029169027864:
     * four arms all drained fully, three of four still tripped the 150s bound). This probe instead
     * watches COMPLETIONS, so it does not fire on an instance that is finishing records, however
     * slowly.
     * <p>
     * <b>Precisely: only a SUCCESSFUL result re-arms it, and that is narrower than "any returned
     * work result".</b> The harness re-arms through {@code WorkManager#addSuccessfulWorkListener},
     * which only {@code onSuccessResult} fires. {@code onFailureResult} and the revoked-partition
     * drop branch of {@code handleFutureResult} both return a work result and decrement the
     * in-flight count while notifying nothing. The bound arithmetic below budgets for the second of
     * those and not the first, so an instance whose work is all FAILING is, to this detector,
     * indistinguishable from a wedged one. Nothing has yet shown that case reachable in the chaos
     * scenarios (their user functions swallow interrupts and do not throw), which is why this is
     * recorded here rather than treated as a defect - but it is an assumption, not a proof, and a
     * scenario that adds a throwing user function invalidates it.
     * <p>
     * <b>Granularity is per INSTANCE, not per shard, and that is a reachability constraint, not the
     * ideal.</b> The owner's formulation is per shard ("no shard should go {@code INSTANCE_STALL_BOUND}
     * without returning a work result"), but "which shards hold queued work" lives in
     * {@code ShardManager}'s private {@code processingShards} map with no public accessor, and this
     * suite does not add main-code accessors for a probe. Per instance is still the confluentinc#857
     * wedge signature exactly: work results are counted where {@code WorkManager#onSuccessResult}
     * runs - PC's CONTROL thread - so a deadlocked control loop freezes the count even while worker
     * threads finish records and heartbeats keep flowing. What per-instance cannot see is one wedged
     * shard on an instance whose other shards keep completing; that case remains
     * {@code CLASS2_STALL}'s - which since 2026-08-25 reports it as an observation rather than
     * failing on it, precisely because it cannot tell that case from a slow one. <b>So that case has
     * no gating detector at all today.</b> That is a known, deliberate reduction in coverage, not an
     * oversight, and it is tracked in {@code docs/inflight/test-per-shard-liveness-has-no-gate.md};
     * do not read the demotion as evidence the case is covered elsewhere.
     * <p>
     * Bound arithmetic (why 150s cannot fire legitimately): a completion arrives at the end of every
     * user-function execution, so the longest legitimate GAP is one heaviest record - W1's 45s dwell,
     * 3.3x under the bound. The other legitimate quiet stretch is an eager storm, where completions
     * of revoked in-flight work are dropped as stale (no listener fire): storm (60s) plus one
     * eviction horizon (30s) is 90s, still 60s under.
     * <b>That 60s assumes a storm that ENDS.</b> W1 ({@code ChaosChurnStormIT}) leaves
     * {@code useCooperativeAssignor} false and drives a membership change every 500-1500ms for the
     * whole run, so its eager revocations are continuous rather than a bounded episode, and the
     * headroom this paragraph computes is not established for that scenario. Whether the bound
     * transfers to W1 is open, and it is the same transfer question
     * {@code docs/inflight/test-no-progress-window-may-not-transfer-to-w1.md} raises for the
     * fleet-level window. Settle it the way {@link #REBALANCE_DWELL_BOUND} was settled - against the
     * healthy peak, which {@link #getPeakInstanceStallMs()} already reports on every run. Sharing {@link #LAG_STAGNATION_BOUND}'s 150s
     * figure is deliberate - it keeps the two detectors' verdicts comparable on the same run: a run
     * where Class 2 fires and this stays silent is measured slow-but-progressing, not wedged.
     * <p>
     * <b>The W1 transfer question above is answered, 2026-09-07: the bound does NOT transfer to
     * continuous eager churn, and the fix is a second input rather than a bigger number.</b> Under
     * that churn every heavy dwell is revoked before it ends and redelivered while the old copy sleeps
     * on, so a member's workers fill with dwells whose results will be dropped, and its count freezes
     * for as long as the churn keeps them stale - 53s on the replayed seed, longer than the bound on
     * the CI sightings. That is a full member, not a stalled one, and the detector now asks
     * {@link InstanceProgressView#idleWorkers()} before it counts: a member with every worker running
     * user code re-arms the clock on every sample and is reported past this bound as a non-gating
     * {@code INSTANCE_SATURATED} observation; the violation is reserved for a member holding work with
     * a worker FREE, which is the only shape that accuses PC's control loop. The record, the dumps and
     * the control arm are in {@code docs/inflight/test-857-churn-storm-async-stalls.md},
     * "DIAGNOSED, 2026-09-07".
     */
    public static final Duration INSTANCE_STALL_BOUND = Duration.ofSeconds(150);
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
     * What the instance-progress probe samples from one fleet member. An interface rather than
     * {@code ManagedPCInstance} directly so the detector's decision logic is broker-free testable
     * against fake views ({@code InstanceStallProbeIT}) - the same pure-replay pattern as
     * {@link #ledger} and {@code KeyOrderLedger#check}.
     */
    public interface InstanceProgressView {
        int instanceId();

        /**
         * Started, not mid-stop/restart, and its PC is up and not failed. The chaos harness stops and
         * restarts members constantly; a stopped or restarting instance holds torn-down state and must
         * never be reported as stalled.
         */
        boolean isLive();

        /** Work queued in this instance's shards awaiting selection ({@code WorkManager}'s own count). */
        long queuedInShards();

        /** Records this instance currently has out for processing ({@code WorkManager}'s own count). */
        long outForProcessing();

        /**
         * One line of engine counters for a stall dump, read straight off the live {@code WorkManager}
         * - what the instance holds and whether any of it is parked for retry, which the three
         * progress counters above cannot say. Default empty so a scripted view need not fake it.
         */
        default String engineSnapshot() {
            return "";
        }

        /**
         * Workers NOT running user code right now - the difference between a member that is stalled
         * and one that is merely full. {@link #IDLE_WORKERS_UNKNOWN} when the view cannot say, which
         * the detector treats as "assume one is free": the pre-2026-09-07 rule, kept for scripted views.
         * <p>
         * <b>Why the detector needs it.</b> The instance-stall line on {@code ChaosChurnStormIT} was
         * classified as a wedge and turned out to be all ten workers asleep in the scenario's heavy
         * dwell, on records revoked out from under them - a saturated member, with a control loop that
         * had nothing to finish ({@code docs/inflight/test-857-churn-storm-async-stalls.md},
         * "DIAGNOSED, 2026-09-07"). Held work plus a frozen completion count is the detector's whole
         * signal, and a full member produces it for as long as its user functions run. Only a member
         * with a worker FREE and work still held is making a claim about PC.
         */
        default int idleWorkers() {
            return IDLE_WORKERS_UNKNOWN;
        }

        /** Monotone count of work results returned - see {@code ManagedPCInstance#workResultsReturned}. */
        long workResultsReturned();

        /**
         * Identity that changes when the instance brings up a NEW PC (a restart). A fresh incarnation
         * gets a fresh full bound-window rather than inheriting the old PC's silence.
         */
        Object incarnationMarker();

        /** The live adapter over a real fleet member, reading PC's own {@code WorkManager} state. */
        static InstanceProgressView of(ManagedPCInstance pc) {
            return new InstanceProgressView() {
                @Override
                public int instanceId() {
                    return pc.getInstanceId();
                }

                @Override
                public boolean isLive() {
                    var parallelConsumer = pc.getParallelConsumer();
                    return pc.isStarted() && !pc.isClosePending()
                            && parallelConsumer != null && !parallelConsumer.isClosedOrFailed();
                }

                @Override
                public long queuedInShards() {
                    var parallelConsumer = pc.getParallelConsumer();
                    // the count can be transiently negative by its own javadoc (counter races) - floor it
                    return parallelConsumer == null ? 0
                            : Math.max(0, parallelConsumer.getWm().getNumberOfWorkQueuedInShardsAwaitingSelection());
                }

                @Override
                public long outForProcessing() {
                    var parallelConsumer = pc.getParallelConsumer();
                    return parallelConsumer == null ? 0
                            : Math.max(0, parallelConsumer.getWm().getNumberRecordsOutForProcessing());
                }

                @Override
                public long workResultsReturned() {
                    return pc.getWorkResultsReturnedCount();
                }

                @Override
                public Object incarnationMarker() {
                    return pc.getParallelConsumer();
                }

                @Override
                public int idleWorkers() {
                    return pc.getParallelConsumer() == null ? IDLE_WORKERS_UNKNOWN
                            : idleWorkersOf(pc.getInstanceId(), pc.getConfig().getMaxConcurrency());
                }

                @Override
                public String engineSnapshot() {
                    var parallelConsumer = pc.getParallelConsumer();
                    if (parallelConsumer == null) {
                        return "no PC";
                    }
                    var wm = parallelConsumer.getWm();
                    var sm = wm.getSm();
                    return "closedOrFailed=" + parallelConsumer.isClosedOrFailed()
                            + " incompleteOffsets=" + wm.getNumberOfIncompleteOffsets()
                            + " recordsInShards=" + sm.getNumberOfRecordsInShards()
                            + " parkedForRetry=" + sm.getNumberOfRecordsParkedForRetry()
                            + " lowestRetryIn=" + wm.getLowestRetryTime().map(Duration::toString).orElse("none");
                }
            };
        }
    }

    /** Instance-progress bookkeeping: the completion count last seen, which PC it was seen on, and
     * since when it has not advanced. */
    @Value
    private static class InstanceProgressMark {
        long workResultsReturned;
        Object incarnation;
        Instant since;
    }

    /** Fleet supplier for the instance-progress probe; null (ambient mode, or not wired) = inactive. */
    private volatile Supplier<List<InstanceProgressView>> instanceProgressSupplier;
    private final Map<Integer, InstanceProgressMark> instanceProgressMarks = new ConcurrentHashMap<>();
    /** Instances already given an early stall dump in their CURRENT frozen stretch - one per stretch, not per sample. */
    private final java.util.Set<Integer> stallDumpedThisStretch = ConcurrentHashMap.newKeySet();
    /** When each instance's CURRENT full-workers stretch began; absent = not saturated. */
    private final Map<Integer, Instant> saturatedSince = new ConcurrentHashMap<>();
    /** Instances whose current saturated stretch has already been reported - once per stretch. */
    private final java.util.Set<Integer> saturationObservedThisStretch = ConcurrentHashMap.newKeySet();
    @Getter
    private volatile long peakInstanceStallMs = 0;

    /**
     * How long an instance may hold work and return nothing before its threads are dumped -
     * {@code -Dchaos.instanceStallDumpAfterSeconds=<n>}, defaulting to the bound itself, so an
     * unconfigured run dumps exactly once per firing and nowhere else. Lower it under
     * {@code -Dchaos.diagnoseStallRecovery=true} to see inside a stretch the run outlives: the
     * tokens from {@link #instanceProgressSnapshot} can show a member frozen for the whole tail of a
     * run that still finishes under the bound, and then there is no firing to hang a dump on.
     */
    static final Duration INSTANCE_STALL_DUMP_AFTER = Duration.ofSeconds(
            Long.getLong("chaos.instanceStallDumpAfterSeconds", INSTANCE_STALL_BOUND.getSeconds()));

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
    private Instant rebalanceDwellStart = null;
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
        this.instanceProgressSupplier = fleetSupplier;
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
        return new ProgressProbe(null, groupId, topic, () -> 0L, 0);
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
            log.debug("[{}] peaks: maxRebalanceDwell={}ms maxDrainDuration={}ms maxLagStagnation={}ms maxInstanceStall={}ms",
                    mode.logTag, peakRebalanceDwellMs, peakDrainDurationMs, peakLagStagnationMs, peakInstanceStallMs);
        } else {
            log.info("[{}] peaks: maxRebalanceDwell={}ms maxDrainDuration={}ms maxLagStagnation={}ms maxInstanceStall={}ms",
                    mode.logTag, peakRebalanceDwellMs, peakDrainDurationMs, peakLagStagnationMs, peakInstanceStallMs);
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

    private void sampleProgress() {
        long now = totalConsumed.getAsLong();
        if (now != lastCount) {
            lastCount = now;
            lastAdvance = Instant.now();
            return;
        }
        boolean workRemains = now < expectedTotal - TAIL_SLACK;
        Duration stalled = Duration.between(lastAdvance, Instant.now());
        if (workRemains && stalled.compareTo(noProgressWindow) > 0) {
            violate("NO_PROGRESS: fleet consumed count stuck at " + now + "/" + expectedTotal
                    + " for " + stalled.getSeconds() + "s (bound " + noProgressWindow.getSeconds() + "s)");
            lastAdvance = Instant.now(); // re-arm so a genuine stall reports once per window, not per sample
        }
    }

    /**
     * One compact token per fleet member for a {@code -Dchaos.diagnoseStallRecovery=true} run's log:
     * {@code <id>(live|down q=<queued> out=<outForProcessing> res=<workResultsReturned>)}.
     * <p>
     * <b>Why this exists.</b> An {@code INSTANCE_STALL/NO_WORK_COMPLETED} violation is a claim about
     * ONE member, and the recovery diagnostic that settled the fleet-level
     * {@code NO_PROGRESS} line logs fleet consumed/started only. Watching the fleet drain therefore
     * says nothing about whether the accused instance recovered, so every instance-level sighting in
     * {@code docs/inflight/test-857-churn-storm-async-stalls.md} is unclassified. The discriminating
     * observation is this instance's own {@code res=} advancing and its {@code out=} draining after
     * the firing, which is exactly what these tokens carry.
     * <p>
     * <b>Every member is rendered, including a stopped one</b> ({@code down}), which the detector
     * itself skips. Omitting it would read as a fleet that never had that member - the
     * silence-is-not-evidence trap recorded in
     * {@code docs/solutions/best-practices/silence-from-an-instrument-that-could-not-have-spoken-is-not-evidence.md}.
     * For the same reason a supplier that throws yields an {@code unreadable} marker rather than an
     * empty string: this is called from inside an awaitility condition, so it must neither abort the
     * wait nor let a failed read look like an empty fleet.
     * <p>
     * <b>What it cannot tell you, and it is the same trap one instrument along: a RESTART forges
     * recovery.</b> {@code res=} is {@code ManagedPCInstance#workResultsReturned}, which deliberately
     * spans incarnations and is never reset, and {@code out=} reads whatever {@code WorkManager} is
     * current. So when the conductor stops a wedged member and starts it again - {@code RESTART}
     * carries weight 3 in the churn storm, so it is a routine draw rather than an edge case - the
     * following tokens show {@code res=} climbing on from the frozen value and {@code out=} filling
     * from a brand-new {@code WorkManager}, which is exactly the shape read here as recovery. The
     * DETECTOR is immune, because {@link #sampleInstanceProgress} re-arms on
     * {@link InstanceProgressView#incarnationMarker()}; the rendered token carries no incarnation, so
     * a human reading the line is not. Until it does, pair an apparent recovery with the conductor's
     * own action log for that instance id before classifying anything from it.
     */
    String instanceProgressSnapshot() {
        var supplier = instanceProgressSupplier;
        if (supplier == null) {
            return "";
        }
        try {
            return supplier.get().stream()
                    .map(view -> view.instanceId()
                            + (view.isLive() ? "(live q=" : "(down q=") + view.queuedInShards()
                            + " out=" + view.outForProcessing()
                            + " res=" + view.workResultsReturned() + ")")
                    .collect(java.util.stream.Collectors.joining(" "));
        } catch (Exception e) {
            return "unreadable (" + e + ")";
        }
    }

    /**
     * INSTANCE-progress detector - see {@link #INSTANCE_STALL_BOUND} for the property it asserts and
     * the granularity reasoning. Per live instance: if it holds work (queued in shards, or records out
     * for processing) and its returned-work-result count has not advanced within the bound, that is a
     * violation. The clock re-arms on ANY of: a result returned, the instance going idle (nothing
     * held), a restart (new PC incarnation), or the instance leaving the live set - so only a
     * continuous hold-work-return-nothing stretch can accumulate.
     * <p>
     * Package-private and taking {@code now} explicitly so {@code InstanceStallProbeIT} can drive it
     * deterministically, broker-free, in both directions - the sampler thread calls it with
     * {@code Instant.now()}.
     */
    void sampleInstanceProgress(Instant now) {
        var supplier = instanceProgressSupplier;
        if (supplier == null) return; // not wired (ambient mode, or a scenario predating the probe)
        for (InstanceProgressView view : supplier.get()) {
            int id = view.instanceId();
            if (!view.isLive()) {
                // stopped or mid-restart: torn-down state must never read as a stall
                instanceProgressMarks.remove(id);
                continue;
            }
            long returned = view.workResultsReturned();
            Object incarnation = view.incarnationMarker();
            InstanceProgressMark mark = instanceProgressMarks.get(id);
            boolean advanced = mark == null
                    || mark.getWorkResultsReturned() != returned
                    || mark.getIncarnation() != incarnation;
            long queued = view.queuedInShards();
            long outForProcessing = view.outForProcessing();
            boolean holdsWork = queued > 0 || outForProcessing > 0;
            if (advanced || !holdsWork) {
                instanceProgressMarks.put(id, new InstanceProgressMark(returned, incarnation, now));
                stallDumpedThisStretch.remove(id);
                saturatedSince.remove(id);
                continue;
            }
            // Held work and a frozen count: the signal. Now ask what the workers are doing, because a
            // member whose every worker is running user code produces this signal for as long as those
            // functions run, and it is not a claim about PC (see InstanceProgressView#idleWorkers).
            if (view.idleWorkers() == 0) {
                // Saturated: the stall clock does not run. It is re-armed to NOW on every sample, so it
                // starts the moment a worker frees while the count is still frozen - which is the case
                // that IS PC's. The saturation itself is reported once, non-gating, past the same bound,
                // so a fleet that spends its whole tail full is visible without failing the run.
                instanceProgressMarks.put(id, new InstanceProgressMark(returned, incarnation, now));
                Instant since = saturatedSince.computeIfAbsent(id, ignored -> now);
                long saturatedMs = Duration.between(since, now).toMillis();
                if (saturatedMs > INSTANCE_STALL_BOUND.toMillis() && saturationObservedThisStretch.add(id)) {
                    observe("INSTANCE_SATURATED: instance " + id + " has held work (queued=" + queued
                            + ", outForProcessing=" + outForProcessing + ") for " + (saturatedMs / 1000)
                            + "s with every worker running user code and no work result returned - a full "
                            + "member, not a stalled control loop; the stall clock starts when a worker frees");
                }
                continue;
            }
            saturatedSince.remove(id);
            saturationObservedThisStretch.remove(id);
            long stalledMs = Duration.between(mark.getSince(), now).toMillis();
            if (stalledMs > peakInstanceStallMs) peakInstanceStallMs = stalledMs;
            if (stalledMs > INSTANCE_STALL_DUMP_AFTER.toMillis() && stallDumpedThisStretch.add(id)) {
                // Diagnostic only, and only when the property lowers it below the bound: a stretch that
                // ends before the bound leaves no violation and no dump, so a wedge that clears when
                // the run happens to finish first was invisible - which is how seed 6077035105695 read
                // as clean on a tree where its instance 0 sat frozen for the whole tail of the run.
                log.warn("INSTANCE_STALL early dump ({}s frozen, bound {}s) for instance {}: {}\n{}",
                        stalledMs / 1000, INSTANCE_STALL_BOUND.getSeconds(), id, view.engineSnapshot(),
                        instanceThreadDump(id));
            }
            if (stalledMs > INSTANCE_STALL_BOUND.toMillis()) {
                violate("INSTANCE_STALL/NO_WORK_COMPLETED: instance " + id + " holds work (queued="
                        + queued + ", outForProcessing=" + outForProcessing
                        + ") but has returned no work result for " + (stalledMs / 1000) + "s (bound "
                        + INSTANCE_STALL_BOUND.getSeconds() + "s) at " + returned
                        + " results returned - completions are counted on PC's control thread, so this "
                        + "instance's control loop is holding work and finishing nothing");
                // The dump is taken HERE, inside the sample that fired, because the accused instance's
                // threads are what the violation is a claim about, and nothing else captures them - a
                // gating run aborts on this violation and a CI log outlives the JVM by nothing.
                log.warn("INSTANCE_STALL thread dump for instance {} at the moment the detector fired: {}\n{}",
                        id, view.engineSnapshot(), instanceThreadDump(id));
                instanceProgressMarks.put(id, new InstanceProgressMark(returned, incarnation, now)); // re-arm
                stallDumpedThisStretch.remove(id); // the re-armed stretch may earn its own early dump
            }
        }
    }

    /** Enough frames to see past the executor plumbing to whatever a worker is actually parked in. */
    private static final int STALL_DUMP_FRAMES = 40;

    /** {@link InstanceProgressView#idleWorkers()} when the view cannot count them. */
    static final int IDLE_WORKERS_UNKNOWN = -1;

    /**
     * How many of an instance's {@code capacity} workers are NOT running user code, read from the
     * worker threads' own stacks - the same reading {@link #instanceThreadDump} hands a human, made
     * mechanical. A worker between tasks is parked inside {@code ThreadPoolExecutor.getTask}; one
     * running a task has no such frame. Threads the pool has not created yet are idle by definition,
     * which is why the answer is {@code capacity - busy} rather than a count of parked threads.
     * <p>
     * Read from the stacks rather than from the pool because the pool is the engine's, held behind a
     * protected getter, and this suite does not add main-code accessors for a probe (the same rule
     * {@link #INSTANCE_STALL_BOUND}'s granularity note records). {@code Thread.getAllStackTraces} is
     * a JVM-wide walk, so the detector asks only for a member already holding work with a frozen
     * count - never on the healthy path.
     * <p>
     * Membership is the {@code pc-pool-} prefix and the exact {@code -PC-<id>} suffix, the names
     * {@code AbstractParallelEoSStreamProcessor#setupWorkerPool} gives the default factory's threads;
     * a scenario that supplies its own {@code managedThreadFactory} would count nothing busy and read
     * as all-idle, which is the old rule, not a silent exemption.
     */
    static int idleWorkersOf(int instanceId, int capacity) {
        String suffix = "-PC-" + instanceId;
        int busy = 0;
        for (var entry : Thread.getAllStackTraces().entrySet()) {
            String name = entry.getKey().getName();
            if (!name.startsWith("pc-pool-") || !name.endsWith(suffix)) continue;
            boolean betweenTasks = false;
            for (StackTraceElement frame : entry.getValue()) {
                if (frame.getClassName().equals("java.util.concurrent.ThreadPoolExecutor")
                        && frame.getMethodName().equals("getTask")) {
                    betweenTasks = true;
                    break;
                }
            }
            if (!betweenTasks) busy++;
        }
        return Math.max(capacity - busy, 0);
    }

    /**
     * Every thread belonging to one fleet member, with its state, the lock it is waiting for and who
     * holds it, and its top {@value #STALL_DUMP_FRAMES} frames - the discriminator an
     * {@code INSTANCE_STALL/NO_WORK_COMPLETED} firing has never had.
     * <p>
     * <b>Why this exists.</b> The per-instance tokens in {@link #instanceProgressSnapshot} classified
     * the churn storm's instance-stall line as a live member that keeps accepting work and returns
     * nothing ({@code docs/inflight/test-857-churn-storm-async-stalls.md}, the {@code CLASSIFIED}
     * section) - and then stopped, because a counter can say the workers return nothing but not
     * what they are doing instead. Only their stacks say that, and a gating run destroys them at
     * the moment of detection, so the dump has to be taken by the detector itself.
     * <p>
     * <b>Membership is by thread name, and it is exact.</b> PC names every thread it owns with the
     * instance's {@code myId} as a suffix - the worker pool, the control thread and the broker-poll
     * thread all end {@code -PC-<instanceId>}, which {@code ManagedPCInstance#start} sets. The match is
     * {@code endsWith}, not {@code contains}: {@code -PC-1} is a suffix of nothing but instance 1,
     * whereas a substring match would fold instance 14's threads into it.
     * <p>
     * <b>An empty result is reported as such, never as an empty string.</b> No matching threads means
     * either the instance's threads have already exited or the naming does not match, and both are
     * findings a reader must not mistake for "nothing was happening" -
     * {@code docs/solutions/best-practices/silence-from-an-instrument-that-could-not-have-spoken-is-not-evidence.md}.
     * For the same reason a bean that throws yields a marker rather than propagating: this runs on
     * the sampler thread, and the violation it accompanies has already been recorded.
     * <p>
     * Package-private so {@code InstanceStallProbeIT} can pin the exact-suffix rule and the
     * explicit-absence marker broker-free. The naming itself is PC's contract, not this file's:
     * {@code CloseInterruptLivelockTest} finds a real PC's control thread by
     * {@code "pc-control-" + myId}, so a rename there fails that test before it silently empties
     * this dump.
     */
    static String instanceThreadDump(int instanceId) {
        String suffix = "-PC-" + instanceId;
        try {
            long[] ids = Thread.getAllStackTraces().keySet().stream()
                    .filter(t -> t.getName().endsWith(suffix))
                    .mapToLong(Thread::getId)
                    .toArray();
            if (ids.length == 0) {
                return "  (no threads named *" + suffix + " exist - the instance's threads have exited, or "
                        + "the naming contract this dump relies on has changed)";
            }
            java.lang.management.ThreadMXBean bean = java.lang.management.ManagementFactory.getThreadMXBean();
            StringBuilder out = new StringBuilder();
            for (java.lang.management.ThreadInfo info : bean.getThreadInfo(ids, true, true)) {
                if (info == null) continue; // exited between the name scan and the bean read
                out.append("  \"").append(info.getThreadName()).append("\" ").append(info.getThreadState());
                if (info.getLockName() != null) {
                    out.append(" waiting on ").append(info.getLockName());
                    if (info.getLockOwnerName() != null) {
                        out.append(" held by \"").append(info.getLockOwnerName()).append("\"");
                    }
                }
                out.append('\n');
                StackTraceElement[] frames = info.getStackTrace();
                int shown = Math.min(frames.length, STALL_DUMP_FRAMES);
                for (int i = 0; i < shown; i++) {
                    out.append("      at ").append(frames[i]).append('\n');
                }
                if (frames.length > shown) {
                    out.append("      ... ").append(frames.length - shown).append(" more\n");
                }
                for (var monitor : info.getLockedMonitors()) {
                    out.append("      holds monitor ").append(monitor).append('\n');
                }
                for (var sync : info.getLockedSynchronizers()) {
                    out.append("      holds ").append(sync).append('\n');
                }
            }
            return out.toString();
        } catch (RuntimeException e) {
            return "  (thread dump unreadable: " + e + ")";
        }
    }

    private void sampleRebalanceDwell() throws Exception {
        var adminOpt = kcu.adminIfOpen();
        if (!adminOpt.isPresent()) return; // outside the open()..close() window - skip this sample
        var admin = adminOpt.get();
        String groupId = groupIdSupplier.get();
        var group = admin.describeConsumerGroups(of(groupId)).all()
                .get(5, java.util.concurrent.TimeUnit.SECONDS).get(groupId);
        ConsumerGroupState state = group.state();
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
