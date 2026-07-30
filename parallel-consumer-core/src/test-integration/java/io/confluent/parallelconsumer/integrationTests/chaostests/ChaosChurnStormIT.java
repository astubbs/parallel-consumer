package io.confluent.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2020-2026 Confluent, Inc. and contributors
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.confluent.parallelconsumer.integrationTests.utils.ManagedPCInstance;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

/**
 * Chaos Pain Suite - W1 "churn storm" (Phase 1 skateboard; origin design:
 * {@code docs/plans/2026-07-30-001-feat-chaos-pain-suite-design-plan.md}, plan:
 * {@code ...-002-feat-chaos-pain-suite-phase1-plan.md}).
 * <p>
 * A fleet of PC instances in one group processes a message backlog while a seeded {@link ChaosConductor}
 * churns membership: drain-mode stops (with a join-after-drain bias - the zombie-drain collision), hard
 * stops, restarts and new joiners. {@link ProgressProbe} continuously asserts the run's SLOs (progress
 * watermark, rebalance-dwell zombie probe, drain bound) and the end-of-run ledger asserts correctness
 * (no record ever lost; duplicates bounded per disturbance).
 * <p>
 * <b>Calibration status</b>: this scenario MUST go red (zombie/dwell or drain-bound probe) on the pre-fix
 * drain-defect composition (the real bug) ({@code experiment/stall-uber-nofix}) and green here - see plan Unit 5.
 * <p>
 * Seed protocol: {@code -Dchaos.seed=<long>} replays a schedule; unset = random seed, always logged.
 * Excluded from default suites via {@code @Tag("chaos")}; run with {@code -Dincluded.groups=chaos}.
 */
@Tag("chaos")
@Timeout(600)
@Testcontainers
@Slf4j
class ChaosChurnStormIT extends ChaosScenarioBase {

    private static final int PARTITIONS = 80;
    private static final int EXPECTED_MESSAGES = 100_000;
    private static final int INITIAL_FLEET = 12;
    private static final int MAX_FLEET = 16;
    private static final double PRE_PRODUCE_FRACTION = 0.3;
    private static final Duration RUN_CAP = Duration.ofMinutes(5);
    /**
     * Heavy-tailed work: 1 in HEAVY_EVERY records sleeps HEAVY_SLEEP in the user function. This is what
     * makes drains take real time - the zombie-drain defect freezes the group for the DURATION of a
     * drain, so without a heavy tail the freeze clears in seconds and the rebalance-dwell probe (60s)
     * cannot discriminate defect from healthy. Healthy arm: heavy records occupy one worker slot each and
     * drains still complete within ProgressProbe#DRAIN_BOUND (which must exceed HEAVY_SLEEP).
     */
    private static final int HEAVY_EVERY = 4_000;
    /** 45s, not longer: the zombie window only needs drains occupied (close bails at ~11s regardless),
     * while the Class 2 lag probe must tolerate a REDELIVERY CHAIN - a hard stop can interrupt a heavy
     * record mid-dwell and at-least-once re-runs it fresh, legitimately blocking that partition's
     * committed offset for ~2 chained dwells (observed 151s at 90s dwell = false positive). 2x45s=90s
     * sits comfortably under LAG_STAGNATION_BOUND (150s). */
    private static final Duration HEAVY_SLEEP = Duration.ofSeconds(45);

    @Test
    void churnStormMeetsSlosAndBalancesLedger() throws Exception {
        long seed = Long.getLong("chaos.seed", RandomUtils.nextLong());
        log.info("=== CHAOS W1 churn storm: seed={} (replay with -Dchaos.seed={}) ===", seed, seed);

        String topic = getClass().getSimpleName() + "-w1-" + RandomUtils.nextInt();
        ensureTopic(topic, PARTITIONS); // explicit partition count (base numPartitions is package-private)

        // fleet-wide consumption tracking (the probe's watermark + the ledger's evidence)
        AtomicLong totalConsumed = new AtomicLong();
        Queue<String> allConsumed = new ConcurrentLinkedQueue<>();

        ManagedPCInstance.Config pcConfig = ManagedPCInstance.Config.builder()
                .commitMode(CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS)
                .order(ProcessingOrder.UNORDERED)
                .inputTopic(topic)
                .pollDelayMs(1)   // some in-flight dwell so drains have real work to finish
                .maxConcurrency(10)
                .build();

        ExecutorService pcExecutor = Executors.newWorkStealingPool();

        // pre-produce a backlog, keep the rest flowing in the background (mirrors the existing
        // MultiInstanceRebalanceTest shape; suite-local orchestration because runTest there is private)
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
                totalConsumed::get, EXPECTED_MESSAGES);

        ChaosConductor conductor = ChaosConductor.builder()
                .seed(seed)
                .minTick(Duration.ofMillis(500))
                .maxTick(Duration.ofMillis(1500))
                .joinAfterDrainBias(0.9)
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
            // the run: everything produced must be consumed by SOMEONE within the cap, chaos or not
            await().alias("all messages consumed under churn")
                    .atMost(RUN_CAP)
                    .pollInterval(Duration.ofSeconds(2))
                    .failFast("probe violation during run", probe::hasViolations)
                    .until(() -> totalConsumed.get() >= EXPECTED_MESSAGES
                            && allConsumedCovers(expectedKeys, allConsumed));
        } finally {
            settleRun(conductor, probe, producerThread, totalConsumed);
        }

        assertScenarioSlos(probe, conductor, seed, expectedKeys, allConsumed);
    }

}
