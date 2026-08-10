package io.confluent.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
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
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

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
 *     -DskipUTs=true -Dincluded.groups=chaos -Dexcluded.groups=}</pre>
 * at a commit BEFORE the fix (expect RED - the probe violation names the mechanism) and again at the
 * fix commit (expect GREEN). The RED->GREEN flip is the evidence that the fix addresses the mechanism
 * the probe watches. Add {@code -Dchaos.seed=<seed>} to replay a specific schedule; on-demand CI runs
 * via {@code .github/workflows/chaos-pain.yml} (workflow_dispatch: seed, reps). See
 * <a href="https://github.com/astubbs/parallel-consumer/blob/master/docs/testing.md">docs/testing.md</a>,
 * "Chaos Pain Suite".
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

    @Test
    void churnStormMeetsSlosAndBalancesLedger() throws Exception {
        long seed = resolveSeed();
        String replayCmd = replayCommand(seed);
        log.info("=== CHAOS W1 churn storm: seed={} (replay: {}) ===", seed, replayCmd);

        String topic = getClass().getSimpleName() + "-w1-" + RandomUtils.nextInt();
        ensureTopic(topic, PARTITIONS); // explicit partition count (base numPartitions is package-private)

        ManagedPCInstance.Config pcConfig = ManagedPCInstance.Config.builder()
                .commitMode(CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS)
                .order(ProcessingOrder.UNORDERED)
                .inputTopic(topic)
                .pollDelayMs(1)   // some in-flight dwell so drains have real work to finish
                .maxConcurrency(10)
                .build();

        FleetBootstrap fleet = bootstrapFleet(topic, pcConfig, EXPECTED_MESSAGES, PRE_PRODUCE_FRACTION,
                INITIAL_FLEET, HEAVY_EVERY, HEAVY_SLEEP);
        AtomicLong totalConsumed = fleet.getTotalConsumed();
        Queue<String> allConsumed = fleet.getAllConsumed();
        Set<String> expectedKeys = fleet.getExpectedKeys();
        ProgressProbe probe = fleet.getProbe();

        ChaosConductor conductor = conductorFor(fleet, pcConfig, HEAVY_EVERY, HEAVY_SLEEP, MAX_FLEET)
                .seed(seed)
                .minTick(Duration.ofMillis(500))
                .maxTick(Duration.ofMillis(1500))
                .joinAfterDrainBias(0.9)
                .build();

        startRun(probe, conductor);

        try {
            // the run: everything produced must be consumed by SOMEONE within the cap, chaos or not
            await().alias("all messages consumed under churn")
                    .atMost(RUN_CAP)
                    .pollInterval(Duration.ofSeconds(2))
                    .failFast("probe violation during run", probe::hasViolations)
                    .until(() -> totalConsumed.get() >= EXPECTED_MESSAGES
                            && allConsumedCovers(expectedKeys, allConsumed));
        } finally {
            settleRun(conductor, probe, fleet.getProducerThread(), fleet.getPcExecutor(), totalConsumed);
        }

        assertScenarioSlos(probe, conductor, replayCmd, expectedKeys, allConsumed);
    }
}
