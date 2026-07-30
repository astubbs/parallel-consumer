package io.confluent.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2020-2026 Confluent, Inc. and contributors
 */

import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
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
 * Probes (thresholds are generous constants - sized >=5x a healthy baseline and <=1/5 of the defect
 * signature they discriminate, per the Phase 1 plan):
 * <ul>
 *   <li><b>Progress watermark</b>: while work remains, fleet-wide consumed count must advance within
 *   {@link #NO_PROGRESS_WINDOW} (generalises the #857 investigation's "no progress for 11s" check).</li>
 *   <li><b>Zombie-member / rebalance dwell</b>: the group must not dwell in
 *   {@code PREPARING_REBALANCE}/{@code COMPLETING_REBALANCE} beyond {@link #REBALANCE_DWELL_BOUND}.
 *   Keyed on protocol-unresponsiveness, NOT on "member holds partitions with zero consumption" - a
 *   legitimately draining member holds its assignment while finishing + committing (that is drain's
 *   purpose); what it must never do is block the group's rebalance. Healthy rebalances complete in
 *   seconds; a zombie drainer blocks until {@code max.poll.interval.ms} (5 min). 60s cleanly separates
 *   them.</li>
 *   <li><b>Drain bound</b>: every STOP_DRAIN reported by the conductor must complete within
 *   {@link #DRAIN_BOUND}.</li>
 * </ul>
 * The end-of-run correctness ledger (no loss / bounded duplicates) is a static helper the test calls
 * after the fleet settles - see {@link #ledger}.
 * <p>
 * Two construction paths (one per {@link Mode}) share the same samplers:
 * <ul>
 *   <li><b>Chaos mode</b> ({@link #ProgressProbe(KafkaClientUtils, String, String, LongSupplier, long)}):
 *   all probes active, single topic, violations GATE the run (the chaos suite asserts them empty).</li>
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
    /** CLASS 2 probe (protocol-INVISIBLE stalls - the "locks forever, manual restart" #857 reports):
     * no partition may hold real lag while its committed offset stagnates beyond this bound. Broker-side
     * clocks cannot see this class: the group is STABLE, heartbeats + polls flow, no rebalance is pending
     * so the 5-min eviction clock never starts - only lag observation (exactly how users notice) works.
     * Bound must exceed a heavy-tail REDELIVERY CHAIN: a hard stop can interrupt a heavy record
     * mid-dwell and at-least-once re-runs it fresh, so a partition's committed offset can be
     * legitimately blocked ~2 chained dwells (measured: 151s at a 90s dwell - hence the dwell was
     * reduced to 45s; 2x45=90s vs this 150s bound). RED calibration of this probe awaits the W4
     * revoke-under-work trigger scenario on a composition lacking the counter-drift fixes. */
    public static final Duration LAG_STAGNATION_BOUND = Duration.ofSeconds(150);
    /** Ignore trivial tails - the Class 2 signature is real backlog going nowhere. */
    public static final long LAG_STAGNATION_MIN_LAG = 50;
    private static final Duration SAMPLE_INTERVAL = Duration.ofSeconds(1);

    /**
     * The probe's construction mode - the single authoritative switch the samplers consult.
     * {@link #topic} / {@link #totalConsumed} being null is mode-associated data absence, never the
     * mode signal itself.
     */
    public enum Mode {
        /** All probes active, single topic, violations GATE the run (the chaos suite asserts them empty). */
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
    /** per-partition committed-offset watermarks for the Class 2 (lag stagnation) probe */
    private final Map<TopicPartition, Long> lastCommitted = new ConcurrentHashMap<>();
    private final Map<TopicPartition, Instant> lastCommittedMove = new ConcurrentHashMap<>();
    /** Latest per-partition lag observation - the autopsy's frozen-partition detail. */
    @Getter
    private final Map<TopicPartition, PartitionLagSnapshot> partitionLagSnapshots = new ConcurrentHashMap<>();

    @Getter
    private final List<String> violations = Collections.synchronizedList(new ArrayList<>());
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
            log.debug("[{}] peaks: maxRebalanceDwell={}ms maxDrainDuration={}ms maxLagStagnation={}ms",
                    mode.logTag, peakRebalanceDwellMs, peakDrainDurationMs, peakLagStagnationMs);
        } else {
            log.info("[{}] peaks: maxRebalanceDwell={}ms maxDrainDuration={}ms maxLagStagnation={}ms",
                    mode.logTag, peakRebalanceDwellMs, peakDrainDurationMs, peakLagStagnationMs);
        }
        return getViolations();
    }

    public boolean hasViolations() {
        return !violations.isEmpty();
    }

    // --- ChaosObserver: drain-bound bookkeeping (null action = drain-finished marker) ---
    @Override
    public void onAction(int instanceId, ChaosConductor.ChaosAction action) {
        if (action == ChaosConductor.ChaosAction.STOP_DRAIN) {
            outstandingDrains.put(instanceId, Instant.now());
        } else if (action == null) {
            Instant started = outstandingDrains.remove(instanceId);
            if (started != null) {
                long ms = Duration.between(started, Instant.now()).toMillis();
                if (ms > peakDrainDurationMs) peakDrainDurationMs = ms;
            }
        }
    }

    private void sampleLoop() {
        int tick = 0;
        while (running.get()) {
            try {
                Thread.sleep(SAMPLE_INTERVAL.toMillis());
                if (!isObserverMode()) {
                    sampleProgress();
                }
                sampleRebalanceDwell();
                sampleDrains();
                if (++tick % 5 == 0) {
                    sampleLagStagnation(); // heavier admin round-trip; 5s cadence is ample vs a 150s bound
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                // admin hiccups under chaos are expected; probe keeps sampling
                log.debug("Probe sample error (continuing): {}", e.getMessage());
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
        if (workRemains && stalled.compareTo(NO_PROGRESS_WINDOW) > 0) {
            violate("NO_PROGRESS: fleet consumed count stuck at " + now + "/" + expectedTotal
                    + " for " + stalled.getSeconds() + "s (bound " + NO_PROGRESS_WINDOW.getSeconds() + "s)");
            lastAdvance = Instant.now(); // re-arm so a genuine stall reports once per window, not per sample
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
        if (dwell.compareTo(REBALANCE_DWELL_BOUND) > 0) {
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
            if (lag >= LAG_STAGNATION_MIN_LAG && stagnantMs > peakLagStagnationMs) {
                peakLagStagnationMs = stagnantMs;
            }
            if (lag >= LAG_STAGNATION_MIN_LAG && stagnantMs > LAG_STAGNATION_BOUND.toMillis()) {
                violate("CLASS2_STALL/LAG_STAGNATION: partition " + tp + " lag=" + lag
                        + " with committed offset stagnant at " + committed + " for " + (stagnantMs / 1000)
                        + "s (bound " + LAG_STAGNATION_BOUND.getSeconds() + "s) - protocol-invisible stall: "
                        + "group STABLE + heartbeats flowing, yet this partition's backlog is going nowhere");
                lastCommittedMove.put(tp, now); // re-arm
            }
        }
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

    private void violate(String message) {
        violations.add(message);
        if (isObserverMode()) {
            // silent-on-green contract: an ambient violation can occur during a PASSING test, so the
            // failure-time autopsy is the reporting surface - DEBUG matches the green-path precedent
            log.debug("[{}] violation recorded: {}", mode.logTag, message);
        } else {
            log.error("[{}] VIOLATION: {}", mode.logTag, message);
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
        long allowance = (long) disturbanceCount * perDisturbanceAllowance;
        if (duplicates > allowance) {
            problems.add("LEDGER_DUPLICATES: " + duplicates + " duplicate deliveries exceeds allowance "
                    + allowance + " (" + disturbanceCount + " disturbances x " + perDisturbanceAllowance + ")");
        }
        log.info("[chaos-ledger] expected={} uniqueConsumed={} duplicates={} allowance={}",
                expectedKeys.size(), unique.size(), duplicates, allowance);
        return problems;
    }
}
