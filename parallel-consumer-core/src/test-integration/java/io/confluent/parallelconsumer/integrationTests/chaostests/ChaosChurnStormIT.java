package io.confluent.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.confluent.parallelconsumer.Quarantined;
import io.confluent.parallelconsumer.integrationTests.BrokerIntegrationTest;
import io.confluent.parallelconsumer.integrationTests.utils.ManagedPCInstance;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
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
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.truth.Truth.assertWithMessage;
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
 * <p>
 * <b>Usage - probing a fix PR (the suite's primary purpose)</b>: on the fix PR's branch (merge
 * master in first if the branch predates the suite landing there), run
 * <pre>{@code ./mvnw -Pci -pl parallel-consumer-core -am verify \
 *     -DskipUTs=true -Dlicense.skip -Dincluded.groups=chaos -Dexcluded.groups=}</pre>
 * at a commit BEFORE the fix (expect RED - the probe violation names the mechanism) and again at the
 * fix commit (expect GREEN). The RED->GREEN flip is the evidence that the fix addresses the mechanism
 * the probe watches. Add {@code -Dchaos.seed=<seed>} to replay a specific schedule; on-demand CI runs
 * via {@code .github/workflows/chaos-pain.yml} (workflow_dispatch: seed, reps). See AGENTS.md
 * "Chaos Pain Suite".
 */
@Tag("chaos")
@Timeout(600)
@Testcontainers
@Slf4j
class ChaosChurnStormIT extends BrokerIntegrationTest<String, String> {

    private static final int PARTITIONS = 80;
    private static final int EXPECTED_MESSAGES = 100_000;
    private static final int INITIAL_FLEET = 12;
    private static final int MAX_FLEET = 16;
    private static final double PRE_PRODUCE_FRACTION = 0.3;
    private static final Duration RUN_CAP = Duration.ofMinutes(5);
    /**
     * Heavy-tailed work: 1 in HEAVY_EVERY records sleeps HEAVY_SLEEP in the user function. This is what
     * makes drains take real time - the zombie-drain defect freezes the group for the DURATION of a
     * drain, so without a heavy tail the freeze clears in seconds and the rebalance-dwell probe
     * ({@link ProgressProbe#REBALANCE_DWELL_BOUND}) cannot discriminate defect from healthy. Healthy arm:
     * heavy records occupy one worker slot each and
     * drains still complete within ProgressProbe#DRAIN_BOUND (which must exceed HEAVY_SLEEP).
     */
    private static final int HEAVY_EVERY = 4_000;
    /** 45s, not longer: the zombie window only needs drains occupied (close bails at ~11s regardless),
     * while the Class 2 lag probe must tolerate a REDELIVERY CHAIN - a hard stop can interrupt a heavy
     * record mid-dwell and at-least-once re-runs it fresh, legitimately blocking that partition's
     * committed offset for ~2 chained dwells (observed 151s at 90s dwell = false positive). 2x45s=90s
     * sits comfortably under LAG_STAGNATION_BOUND (150s). */
    private static final Duration HEAVY_SLEEP = Duration.ofSeconds(45);

    @Quarantined(
            reason = "ZOMBIE_MEMBER/REBALANCE_BLOCKED on master composition: the #857-family drain-zombie - " +
                    "a draining member stops polling and wedges the group's rebalance until eviction. This is " +
                    "the bug the scenario was built to detect, reproduced workload-robustly across seeds and " +
                    "schedules (master-baseline calibration record in the class javadoc).",
            tracking = "docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md (lands with PR #80)",
            fixedBy = "#80")
    @Test
    void churnStormMeetsSlosAndBalancesLedger() throws Exception {
        String seedProp = System.getProperty("chaos.seed");
        long seed = seedProp == null ? RandomUtils.nextLong() : Long.parseLong(seedProp);
        // the FULL replay invocation, not just the seed - a raw CI log must be self-sufficient to
        // reproduce (the chaos tag is excluded by default, so the seed alone is not enough)
        String replayCmd = "./mvnw -Pci -pl parallel-consumer-core -am verify -DskipUTs=true"
                + " -Dlicense.skip -Dincluded.groups=chaos -Dexcluded.groups= -Dchaos.seed=" + seed;
        log.info("=== CHAOS W1 churn storm: seed={} (replay: {}) ===", seed, replayCmd);

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
        ManagedPCInstance pc0 = newInstance(pcConfig, totalConsumed, allConsumed);
        pc0.start(pcExecutor);
        await().atMost(30, SECONDS).until(() -> totalConsumed.get() > 100);

        Thread producerThread = new Thread(() -> produceRange(topic, preProduce, EXPECTED_MESSAGES, expectedKeys),
                "chaos-background-producer");
        producerThread.start();

        List<ManagedPCInstance> initialFleet = new ArrayList<>();
        initialFleet.add(pc0);
        for (int i = 1; i < INITIAL_FLEET; i++) {
            ManagedPCInstance pc = newInstance(pcConfig, totalConsumed, allConsumed);
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
                .instanceFactory(() -> newInstance(pcConfig, totalConsumed, allConsumed))
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
            conductor.stop(); // also joins outstanding drain threads (bounded) - fleet is quiesced after
            List<String> violations = probe.stop();
            producerThread.join(10_000);
            // settle the fleet
            for (ManagedPCInstance pc : conductor.getFleet()) {
                try {
                    // let any stopAsync background close finish first - close() below is then a no-op
                    long waited = 0;
                    while (pc.isClosePending() && waited < 15_000) {
                        Thread.sleep(100);
                        waited += 100;
                    }
                    if (pc.getParallelConsumer() != null && !pc.getParallelConsumer().isClosedOrFailed()) {
                        pc.getParallelConsumer().close();
                    }
                } catch (Exception e) {
                    log.warn("Settle-close of instance {}: {}", pc.getInstanceId(), e.getMessage());
                }
            }
            pcExecutor.shutdownNow();
            log.info("Run summary: consumed={} (unique tracking via ledger below), probe violations={}",
                    totalConsumed.get(), violations);
        }

        // SLO verdict
        assertWithMessage("chaos probes must be violation-free (each violation carries the diagnosis; " +
                "replay: %s)", replayCmd)
                .that(probe.getViolations()).isEmpty();

        // end-of-run canary sweep: every instance's terminal failure cause must be classified - an
        // instance stopped and never restarted would otherwise carry an unexpected error silently
        // (restart is the only other place classification happens)
        List<String> unexpectedFailures = new ArrayList<>();
        for (ManagedPCInstance pc : conductor.getFleet()) {
            var consumer = pc.getParallelConsumer();
            Exception cause = consumer == null ? null : consumer.getFailureCause();
            if (cause != null && !ManagedPCInstance.isExpectedCloseException(cause)) {
                unexpectedFailures.add("instance " + pc.getInstanceId() + ": " + cause);
            }
        }
        assertWithMessage("no instance may end the run with an unclassified failure cause (replay: %s)",
                replayCmd)
                .that(unexpectedFailures).isEmpty();

        // correctness ledger: no loss ever; duplicates bounded per disturbance
        int disturbances = conductor.getDisturbanceCount();
        List<String> ledgerProblems = ProgressProbe.ledger(expectedKeys, allConsumed,
                Math.max(disturbances, 1), /* perDisturbanceAllowance */ 5_000);
        assertWithMessage("correctness ledger must balance (replay: %s)", replayCmd)
                .that(ledgerProblems).isEmpty();
    }

    private ManagedPCInstance newInstance(ManagedPCInstance.Config config,
                                          AtomicLong totalConsumed, Queue<String> allConsumed) {
        return new ManagedPCInstance(config, getKcu(), key -> {
            if (isHeavyKey(key)) {
                // NON-interruptible dwell (sleep-until-deadline): PC's close path force-interrupts stuck
                // workers after ~5s (awaitTermination -> shutdownNow), which would cap every drain at
                // seconds and shrink the zombie-freeze window below what any probe can discriminate.
                // Real-world slow work often ignores interrupts too (JDBC, native calls, CPU loops).
                long deadline = System.currentTimeMillis() + HEAVY_SLEEP.toMillis();
                boolean interrupted = false;
                long left;
                while ((left = deadline - System.currentTimeMillis()) > 0) {
                    try {
                        Thread.sleep(Math.min(left, 1_000));
                    } catch (InterruptedException e) {
                        interrupted = true; // note it, keep dwelling until the deadline
                    }
                }
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            totalConsumed.incrementAndGet();
            allConsumed.add(key);
        });
    }

    /** key format is "key-N"; every HEAVY_EVERY-th record is heavy. */
    private static boolean isHeavyKey(String key) {
        int n = Integer.parseInt(key.substring(key.indexOf('-') + 1));
        return n > 0 && n % HEAVY_EVERY == 0;
    }

    /** Coverage check is expensive at full message scale (see EXPECTED_MESSAGES) - only evaluate once the
     * counter says it's plausible. */
    private boolean allConsumedCovers(Set<String> expectedKeys, Queue<String> allConsumed) {
        var unique = new java.util.HashSet<>(allConsumed);
        return unique.containsAll(expectedKeys);
    }

    private void produceRange(String topic, int fromInclusive, int toExclusive, Set<String> expectedKeys) {
        try (Producer<String, String> producer = getKcu().createNewProducer(false)) {
            List<Future<RecordMetadata>> sends = new ArrayList<>();
            for (int i = fromInclusive; i < toExclusive; i++) {
                String key = "key-" + i;
                expectedKeys.add(key);
                sends.add(producer.send(new ProducerRecord<>(topic, key, "v-" + i)));
            }
            for (Future<RecordMetadata> send : sends) {
                send.get();
            }
            log.info("Produced [{}..{})", fromInclusive, toExclusive);
        } catch (Exception e) {
            throw new RuntimeException("Producer failed at range [" + fromInclusive + ".." + toExclusive + ")", e);
        }
    }
}
