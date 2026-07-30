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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.truth.Truth.assertWithMessage;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

/**
 * The shared two-phase "revoke under work" driver behind the W4 scenario family - see
 * {@link ChaosRevokeUnderWorkIT} (eager assignor; the original, with the full design + calibration
 * javadoc) and {@link ChaosRevokeUnderWorkCooperativeIT} (cooperative-sticky). Variants differ only by
 * the two abstract methods below; the storm/quiet mechanics, probe configuration, and ledger are
 * identical so the assignor is the single experimental variable.
 * <p>
 * The {@code protected static final} constants are SHARED fixed calibration values, not per-variant
 * knobs: static field references in the inherited driver resolve at compile time to this class, so a
 * subclass shadowing one would silently no-op. A future variant needing a different value (e.g. the
 * rostered W1-coop) must first promote that constant to an overridable accessor referenced via
 * {@code this}.
 */
@Slf4j
abstract class AbstractRevokeUnderWorkScenario extends ChaosScenarioBase {

    protected static final int PARTITIONS = 80;
    /** Sized so the backlog SURVIVES the storm (calibration run 2: a 10-14 instance fleet consumed
     * 100k inside the 90s storm, leaving the quiet phase nothing to stagnate on - the Class 2 probe
     * needs real remaining lag to measure). ~100k drains per 90s, so 250k leaves ~150k for quiet. */
    protected static final int EXPECTED_MESSAGES = 250_000;
    protected static final int INITIAL_FLEET = 10;
    protected static final int MAX_FLEET = 14;
    protected static final double PRE_PRODUCE_FRACTION = 0.3;
    /** Storm phase length - long enough for many revoke-under-work collisions at 300-1000ms ticks (~75
     * actions), short enough that the LEGITIMATE commit-freeze window stays under the Class 2 bound.
     * Measured mechanism (calibration diag, seed 7612284256787897904): under the EAGER assignor every
     * storm membership change reassigns ALL partitions, restarting every in-flight heavy from scratch -
     * so heavies never complete during the storm and each heavy-bearing partition's commit
     * low-watermark is legitimately pinned from storm start. Worst legit freeze = STORM_DURATION +
     * HEAVY_SLEEP + ~20s commit slack = 60+20+20 = 100s vs the 150s LAG_STAGNATION_BOUND (a REAL
     * Class 2 stall is unbounded, so the bound still catches it with 50s margin). At 90s storm + 45s
     * dwell the legit window was ~155s - structurally over the bound, false-positive on BOTH arms. */
    protected static final Duration STORM_DURATION = Duration.ofSeconds(60);
    /** Quiet-phase cap: must exceed LAG_STAGNATION_BOUND (150s) by margin so a stall wedged late in the
     * storm still has time to trip the Class 2 probe before the await gives up. */
    protected static final Duration QUIET_CAP = Duration.ofMinutes(5);
    /** Low eviction horizon: a storm-wedged (deadlocked) member stops polling and gets evicted ~30s
     * later, letting pending rebalances resolve and the group re-stabilize for the quiet phase. */
    protected static final int MAX_POLL_INTERVAL_MS = 30_000;
    /**
     * Denser heavy tail than W1 (2k vs 4k): the mechanism needs revocations to CATCH heavy work in
     * flight, so more concurrent heavies = more collision opportunities per rebalance. Same
     * redelivery-chain arithmetic as W1: 2 chained dwells stay well under the Class 2 probe's 150s
     * bound.
     */
    protected static final int HEAVY_EVERY = 2_000;
    /** 20s, not W1's 45s - part of the legit-freeze-window arithmetic above (60+20+20 < 150s bound). */
    protected static final Duration HEAVY_SLEEP = Duration.ofSeconds(20);

    /** The single experimental variable between W4 variants. */
    protected abstract boolean useCooperativeAssignor();

    /** Short label for topic names and log lines (e.g. "w4" / "w4coop"). */
    protected abstract String scenarioLabel();

    protected void runRevokeUnderWorkScenario() throws Exception {
        long seed = Long.getLong("chaos.seed", RandomUtils.nextLong());
        log.info("=== CHAOS {} revoke-under-work (cooperative={}): seed={} (replay with -Dchaos.seed={}) ===",
                scenarioLabel(), useCooperativeAssignor(), seed, seed);

        String topic = getClass().getSimpleName() + "-" + scenarioLabel() + "-" + RandomUtils.nextInt();
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
                .useCooperativeAssignor(useCooperativeAssignor())
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
                // storm-phase rebalances can legitimately pause much of the fleet for up to the
                // eviction horizon (all of it, under the eager assignor); widen the watermark beyond it
                .withNoProgressWindow(Duration.ofSeconds(60));

        ChaosConductor conductor = ChaosConductor.builder()
                .seed(seed)
                // faster ticks than W1: more rebalances per run = more revoke-under-work collisions
                .minTick(Duration.ofMillis(300))
                .maxTick(Duration.ofMillis(1000))
                .weights(ChaosConductor.defaultW4Weights()) // NO drain stops - see scenario javadoc
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
            log.info("=== {} storm phase over (consumed={}) - entering quiet observation ===",
                    scenarioLabel(), totalConsumed.get());

            // Phase 2 - quiet observation: group must settle, evict any storm-wedged member, and FINISH.
            // The only defect signal that can fire here is the protocol-invisible kind - exactly Class 2.
            await().alias("backlog drained after the storm settles (quiet phase)")
                    .atMost(QUIET_CAP)
                    .pollInterval(Duration.ofSeconds(2))
                    .failFast("probe violation", probe::hasViolations)
                    .until(() -> totalConsumed.get() >= EXPECTED_MESSAGES
                            && allConsumedCovers(expectedKeys, allConsumed));
        } finally {
            conductor.stop();
            List<String> violations = probe.stop();
            producerThread.join(10_000);
            settleFleet(conductor);
            log.info("Run summary: consumed={} probe violations={}", totalConsumed.get(), violations);
        }

        assertWithMessage("chaos probes must be violation-free (each violation carries the diagnosis; " +
                "seed %s replays this schedule)", seed)
                .that(probe.getViolations()).isEmpty();

        int disturbances = (int) conductor.getTimeline().stream()
                .filter(entry -> entry.contains("STOP_") || entry.contains("RESTART")).count();
        List<String> ledgerProblems = ProgressProbe.ledger(expectedKeys, allConsumed,
                Math.max(disturbances, 1), /* perDisturbanceAllowance */ 5_000);
        assertWithMessage("correctness ledger must balance (seed %s)", seed)
                .that(ledgerProblems).isEmpty();
    }
}
