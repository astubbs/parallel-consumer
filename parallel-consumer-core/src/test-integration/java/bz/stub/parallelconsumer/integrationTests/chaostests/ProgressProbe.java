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
                    + "by hand. See docs/inflight/test-class2-probe-asserts-timing-not-correctness.md "
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
     * watches COMPLETIONS: any returned work result re-arms it, so it structurally cannot fire on an
     * instance that is slow-but-progressing - only on one that is holding work and finishing nothing.
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
     * eviction horizon (30s) is 90s, still 60s under. Sharing {@link #LAG_STAGNATION_BOUND}'s 150s
     * figure is deliberate - it keeps the two detectors' verdicts comparable on the same run: a run
     * where Class 2 fires and this stays silent is measured slow-but-progressing, not wedged.
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
    @Getter
    private volatile long peakInstanceStallMs = 0;

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
                continue;
            }
            long stalledMs = Duration.between(mark.getSince(), now).toMillis();
            if (stalledMs > peakInstanceStallMs) peakInstanceStallMs = stalledMs;
            if (stalledMs > INSTANCE_STALL_BOUND.toMillis()) {
                violate("INSTANCE_STALL/NO_WORK_COMPLETED: instance " + id + " holds work (queued="
                        + queued + ", outForProcessing=" + outForProcessing
                        + ") but has returned no work result for " + (stalledMs / 1000) + "s (bound "
                        + INSTANCE_STALL_BOUND.getSeconds() + "s) at " + returned
                        + " results returned - completions are counted on PC's control thread, so this "
                        + "instance's control loop is holding work and finishing nothing");
                instanceProgressMarks.put(id, new InstanceProgressMark(returned, incarnation, now)); // re-arm
            }
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
        Duration dwell = Duration.between(rebalanceDwellStart, Instant.now());
        if (dwell.toMillis() > peakRebalanceDwellMs) peakRebalanceDwellMs = dwell.toMillis();
        if (rebalanceDwellViolationEnabled && dwell.compareTo(REBALANCE_DWELL_BOUND) > 0) {
            violate("ZOMBIE_MEMBER/REBALANCE_BLOCKED: group '" + groupId + "' dwelling in " + state
                    + " for " + dwell.getSeconds() + "s (bound " + REBALANCE_DWELL_BOUND.getSeconds()
                    + "s) - a member is not answering the rebalance (protocol-unresponsive)");
            rebalanceDwellStart = Instant.now(); // re-arm
        }
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
