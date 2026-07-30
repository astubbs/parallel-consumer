package io.confluent.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.confluent.parallelconsumer.integrationTests.utils.ManagedPCInstance;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

/**
 * Chaos Pain Suite - W4 "revoke under work" (Phase 2): the trigger scenario for <b>Class 2,
 * protocol-INVISIBLE stalls</b> - the "#857 locks forever until manual restart" family, where the group
 * stays STABLE, heartbeats and polls keep flowing, no rebalance is pending (so the broker's 5-minute
 * eviction clock never starts), yet a partition's committed offset stops moving while its lag is real.
 * {@link ProgressProbe}'s {@code CLASS2_STALL/LAG_STAGNATION} probe is the detector this scenario
 * exists to exercise.
 * <p>
 * Shape (vs W1's churn storm) - <b>two phases</b>, discovered by fail-first calibration (run 1 on the
 * defect arm went RED via the CLASS 1 dwell probe instead: under CONTINUOUS churn, a revoke-path
 * deadlock blocks the rebalance itself, i.e. the stall is protocol-VISIBLE - the Class 2 signature can
 * only emerge after rebalances settle):
 * <ol>
 *   <li><b>Storm</b> ({@link #STORM_DURATION}): <b>no drain stops at all</b>
 *   ({@link ChaosConductor#defaultW4Weights()}) - drains open the Class 1 zombie window and mask the
 *   Class 2 mechanism. Hard stops, restarts and joins force frequent partition REVOCATIONS while heavy
 *   non-interruptible work is in flight, with {@link CommitMode#PERIODIC_CONSUMER_SYNC} to maximise
 *   revoke-path vs commit-path lock contention (the upstream #857 deadlock recipe:
 *   {@code synchronized(commitCommand)} between {@code onPartitionsRevoked} and
 *   {@code commitOffsetsThatAreReady}). A denser heavy tail than W1 raises the odds a revocation
 *   catches heavy work mid-flight.</li>
 *   <li><b>Quiet observation</b>: chaos stops; a low {@code max.poll.interval.ms} (30s) means any
 *   member wedged during the storm gets evicted quickly, pending rebalances resolve, and the group
 *   re-STABILIZES - the exact production shape of the "locks forever" reports. From here the only
 *   possible defect signal is protocol-invisible: a partition whose committed offset stagnates on real
 *   lag while everything looks healthy. The Class 1 dwell VIOLATION is disabled for this scenario
 *   (measurement still logged; Class 1 gating belongs to W1) and the fleet-wide watermark is widened to
 *   tolerate storm-phase full-fleet rebalance pauses.</li>
 * </ol>
 * <p>
 * <b>Calibration status (2026-07-30, seed 7612284256787897904, same schedule both arms)</b>: the
 * scenario is calibrated ARTIFACT-FREE; the Class 2 probe's RED trigger remains an open hunt.
 * <ul>
 *   <li>First shape (90s storm, 45s dwell) went RED on BOTH arms - root-caused as a workload artifact,
 *   not a stall: eager reassignment restarts in-flight heavies every storm tick, legitimately pinning
 *   commit low-watermarks for storm+dwell+slack ~155s, past the 150s bound. Diagnostic run proved the
 *   freeze RESOLVES (~155s) and the backlog completes. Recalibrated to 60s+20s so the legit window
 *   (~100s arithmetic; 117-123s measured peaks) sits under the bound.</li>
 *   <li>Recalibrated verdicts: defect arm (plain master) GREEN with dwell peak 30.1s - the wedged
 *   member's protocol-visible block, capped exactly by the 30s eviction horizon; fixed arm GREEN with
 *   dwell peak 7.9s. The dwell MEASUREMENT discriminates the defect even though its violation is
 *   delegated to W1.</li>
 *   <li>No true (unbounded) Class 2 stall reproduced on master under this seed/shape - the open #857
 *   root-cause stall did not bite here. Next levers, rostered: defect-arm seed sweep;
 *   cooperative-sticky variant (also removes the eager-restart artifact class entirely).</li>
 * </ul>
 * <p>
 * Seed protocol: {@code -Dchaos.seed=<long>} replays a schedule; unset = random seed, always logged.
 * Excluded from default suites via {@code @Tag("chaos")}; run with {@code -Dincluded.groups=chaos}.
 */
@Tag("chaos")
@Timeout(600)
@Testcontainers
@Slf4j
class ChaosRevokeUnderWorkIT extends ChaosScenarioBase {

    private static final int PARTITIONS = 80;
    /** Sized so the backlog SURVIVES the storm (calibration run 2: a 10-14 instance fleet consumed
     * 100k inside the 90s storm, leaving the quiet phase nothing to stagnate on - the Class 2 probe
     * needs real remaining lag to measure). ~100k drains per 90s, so 250k leaves ~150k for quiet. */
    private static final int EXPECTED_MESSAGES = 250_000;
    private static final int INITIAL_FLEET = 10;
    private static final int MAX_FLEET = 14;
    private static final double PRE_PRODUCE_FRACTION = 0.3;
    /** Storm phase length - long enough for many revoke-under-work collisions at 300-1000ms ticks (~75
     * actions), short enough that the LEGITIMATE commit-freeze window stays under the Class 2 bound.
     * Measured mechanism (calibration diag, seed 7612284256787897904): under the EAGER assignor every
     * storm membership change reassigns ALL partitions, restarting every in-flight heavy from scratch -
     * so heavies never complete during the storm and each heavy-bearing partition's commit
     * low-watermark is legitimately pinned from storm start. Worst legit freeze = STORM_DURATION +
     * HEAVY_SLEEP + ~20s commit slack = 60+20+20 = 100s vs the 150s LAG_STAGNATION_BOUND (a REAL
     * Class 2 stall is unbounded, so the bound still catches it with 50s margin). At 90s storm + 45s
     * dwell the legit window was ~155s - structurally over the bound, false-positive on BOTH arms. */
    private static final Duration STORM_DURATION = Duration.ofSeconds(60);
    /** Quiet-phase cap: must exceed LAG_STAGNATION_BOUND (150s) by margin so a stall wedged late in the
     * storm still has time to trip the Class 2 probe before the await gives up. */
    private static final Duration QUIET_CAP = Duration.ofMinutes(5);
    /** Low eviction horizon: a storm-wedged (deadlocked) member stops polling and gets evicted ~30s
     * later, letting pending rebalances resolve and the group re-stabilize for the quiet phase. */
    private static final int MAX_POLL_INTERVAL_MS = 30_000;
    /**
     * Denser heavy tail than W1 (2k vs 4k): W4's mechanism needs revocations to CATCH heavy work in
     * flight, so more concurrent heavies = more collision opportunities per rebalance. Same
     * redelivery-chain arithmetic as W1: 2 chained 45s dwells = 90s, under the Class 2 probe's 150s
     * bound with margin.
     */
    private static final int HEAVY_EVERY = 2_000;
    /** 20s, not W1's 45s - part of the legit-freeze-window arithmetic above (60+20+20 < 150s bound). */
    private static final Duration HEAVY_SLEEP = Duration.ofSeconds(20);

    @Test
    void revokeUnderWorkStaysProtocolHonest() throws Exception {
        long seed = Long.getLong("chaos.seed", RandomUtils.nextLong());
        log.info("=== CHAOS W4 revoke-under-work: seed={} (replay with -Dchaos.seed={}) ===", seed, seed);

        String topic = getClass().getSimpleName() + "-w4-" + RandomUtils.nextInt();
        ensureTopic(topic, PARTITIONS);

        AtomicLong totalConsumed = new AtomicLong();
        Queue<String> allConsumed = new ConcurrentLinkedQueue<>();

        Properties quickEviction = new Properties();
        quickEviction.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, MAX_POLL_INTERVAL_MS);
        ManagedPCInstance.Config pcConfig = ManagedPCInstance.Config.builder()
                // sync commits sharpen revoke-vs-commit lock contention - the #857 deadlock recipe
                .commitMode(CommitMode.PERIODIC_CONSUMER_SYNC)
                .order(ProcessingOrder.UNORDERED)
                .inputTopic(topic)
                .pollDelayMs(1)
                .maxConcurrency(10)
                .extraConsumerProps(quickEviction)
                .build();

        ExecutorService pcExecutor = Executors.newWorkStealingPool();

        Set<String> expectedKeys = new ConcurrentSkipListSet<>();
        int preProduce = (int) (EXPECTED_MESSAGES * PRE_PRODUCE_FRACTION);
        produceRange(topic, 0, preProduce, expectedKeys);

        // protected first member - chaos never touches it, so the group always has a healthy survivor
        ManagedPCInstance pc0 = newInstance(pcConfig, HEAVY_EVERY, HEAVY_SLEEP, totalConsumed, allConsumed);
        pc0.start(pcExecutor);
        await().atMost(30, SECONDS).until(() -> totalConsumed.get() > 100);

        Thread producerThread = new Thread(() -> produceRange(topic, preProduce, EXPECTED_MESSAGES, expectedKeys),
                "chaos-background-producer");
        producerThread.start();

        List<ManagedPCInstance> initialFleet = new ArrayList<>();
        initialFleet.add(pc0);
        for (int i = 1; i < INITIAL_FLEET; i++) {
            ManagedPCInstance pc = newInstance(pcConfig, HEAVY_EVERY, HEAVY_SLEEP, totalConsumed, allConsumed);
            initialFleet.add(pc);
            pc.start(pcExecutor);
        }

        ProgressProbe probe = new ProgressProbe(getKcu(), getKcu().getGroupId(), topic,
                totalConsumed::get, EXPECTED_MESSAGES)
                // Class 1 dwell gating belongs to W1; here blocked rebalances self-resolve by eviction
                // (30s max.poll.interval) and firing on them would mask the Class 2 measurement
                .disableRebalanceDwellViolation()
                // storm-phase eager rebalances legitimately pause the WHOLE fleet for up to the
                // eviction horizon; widen the watermark beyond it
                .withNoProgressWindow(Duration.ofSeconds(60));

        ChaosConductor conductor = ChaosConductor.builder()
                .seed(seed)
                // faster ticks than W1: more rebalances per run = more revoke-under-work collisions
                .minTick(Duration.ofMillis(300))
                .maxTick(Duration.ofMillis(1000))
                .weights(ChaosConductor.defaultW4Weights()) // NO drain stops - see class javadoc
                .joinAfterDrainBias(0) // no drains to bias after
                .maxFleetSize(MAX_FLEET)
                .pcExecutor(pcExecutor)
                .instanceFactory(() -> newInstance(pcConfig, HEAVY_EVERY, HEAVY_SLEEP, totalConsumed, allConsumed))
                .protectedInstance(pc0)
                .initialFleet(initialFleet)
                .observer(probe)
                .build();

        probe.start();
        conductor.start();

        try {
            // Phase 1 - storm: revocations under heavy in-flight work; bail early on any violation
            Instant stormEnd = Instant.now().plus(STORM_DURATION);
            while (Instant.now().isBefore(stormEnd) && !probe.hasViolations()) {
                Thread.sleep(1_000);
            }
            conductor.stop();
            log.info("=== W4 storm phase over (consumed={}) - entering quiet observation ===",
                    totalConsumed.get());

            // Phase 2 - quiet observation: group must settle, evict any storm-wedged member, and FINISH.
            // The only defect signal that can fire here is the protocol-invisible kind - exactly Class 2.
            await().alias("backlog drained after the storm settles (quiet phase)")
                    .atMost(QUIET_CAP)
                    .pollInterval(Duration.ofSeconds(2))
                    .failFast("probe violation", probe::hasViolations)
                    .until(() -> totalConsumed.get() >= EXPECTED_MESSAGES
                            && allConsumedCovers(expectedKeys, allConsumed));
        } finally {
            settleRun(conductor, probe, producerThread, totalConsumed);
        }

        assertScenarioSlos(probe, conductor, seed, expectedKeys, allConsumed);
    }
}
