package io.confluent.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2020-2026 Confluent, Inc. and contributors
 */

import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.ConsumerGroupState;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

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
 */
@Slf4j
public class ProgressProbe implements ChaosConductor.ChaosObserver {

    /** Fleet-wide consumption must advance at least this often while work remains. */
    public static final Duration NO_PROGRESS_WINDOW = Duration.ofSeconds(30);
    /** Max continuous group-rebalancing dwell: >> seconds-healthy, << 5-min zombie eviction. */
    public static final Duration REBALANCE_DWELL_BOUND = Duration.ofSeconds(60);
    /** Drain-mode close must finish within this - MUST exceed the suite's heavy-tail sleep (a healthy
     * drain legitimately waits for the heaviest in-flight record) plus generous margin under load. */
    public static final Duration DRAIN_BOUND = Duration.ofSeconds(150);
    /** Progress watermark is skipped when this few records remain: the tail may be all heavy-tailed
     * records legitimately sleeping in-flight. The defect signature is a stall with THOUSANDS remaining. */
    public static final int TAIL_SLACK = 500;
    private static final Duration SAMPLE_INTERVAL = Duration.ofSeconds(1);

    private final KafkaClientUtils kcu;
    private final String groupId;
    private final LongSupplier totalConsumed;
    private final long expectedTotal;

    @Getter
    private final List<String> violations = Collections.synchronizedList(new ArrayList<>());
    private final Map<Integer, Instant> outstandingDrains = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread samplerThread;

    private long lastCount = -1;
    private Instant lastAdvance = Instant.now();
    private Instant rebalanceDwellStart = null;

    public ProgressProbe(KafkaClientUtils kcu, String groupId, LongSupplier totalConsumed, long expectedTotal) {
        this.kcu = kcu;
        this.groupId = groupId;
        this.totalConsumed = totalConsumed;
        this.expectedTotal = expectedTotal;
    }

    public void start() {
        running.set(true);
        lastAdvance = Instant.now();
        samplerThread = new Thread(this::sampleLoop, "chaos-progress-probe");
        samplerThread.setDaemon(true);
        samplerThread.start();
    }

    /** Stops sampling and returns accumulated violations (also available via {@link #getViolations()}). */
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
            outstandingDrains.remove(instanceId);
        }
    }

    private void sampleLoop() {
        while (running.get()) {
            try {
                Thread.sleep(SAMPLE_INTERVAL.toMillis());
                sampleProgress();
                sampleRebalanceDwell();
                sampleDrains();
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
        var group = kcu.getAdmin().describeConsumerGroups(of(groupId)).all()
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
        if (dwell.compareTo(REBALANCE_DWELL_BOUND) > 0) {
            violate("ZOMBIE_MEMBER/REBALANCE_BLOCKED: group '" + groupId + "' dwelling in " + state
                    + " for " + dwell.getSeconds() + "s (bound " + REBALANCE_DWELL_BOUND.getSeconds()
                    + "s) - a member is not answering the rebalance (protocol-unresponsive)");
            rebalanceDwellStart = Instant.now(); // re-arm
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
        log.error("[chaos-probe] VIOLATION: {}", message);
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
