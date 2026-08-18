package bz.stub.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.PollContext;
import bz.stub.parallelconsumer.integrationTests.utils.ManagedPCInstance;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigInteger;
import java.time.Duration;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.truth.Truth.assertWithMessage;
import static org.awaitility.Awaitility.await;

/**
 * Chaos Pain Suite - W5 "key order under churn": the only scenario that puts PC's <b>headline</b>
 * guarantee - key concurrency <em>without</em> losing per-key order - under the suite's membership
 * churn, and asserts it. W1 and W4 hunt liveness classes and run {@link ProcessingOrder#UNORDERED},
 * where PC promises nothing about order; this one runs {@link ProcessingOrder#KEY} over a REPEATED key
 * space, so there is an order to keep and a ledger that fails when it is not kept
 * ({@link KeyOrderLedger}, which owns the guarantee's exact window and why each part of it is there).
 *
 * <h2>Two things about the workload are load-bearing, not incidental</h2>
 * <ul>
 *   <li><b>Keys repeat.</b> {@link #KEY_SPACE} keys carry {@link #EXPECTED_MESSAGES} records, so every
 *   key is a shard with a real sequence in it. W1/W4 produce a unique key per record, which makes ANY
 *   per-key ordering assertion vacuous by construction - {@link KeyOrderLedger#check} says so out loud
 *   rather than passing quietly.</li>
 *   <li><b>The ledger's record IDENTITY moves into the value.</b> With keys repeating, the key no
 *   longer identifies a record, so the loss/duplicate half of the ledger would read a second record of
 *   a key as a duplicate delivery of the first. The value carries the unique identity instead
 *   ({@link #identityFor} / {@link #identityOf}), leaving {@link ProgressProbe#ledger} asserting
 *   exactly what it asserts everywhere else.</li>
 * </ul>
 *
 * <h2>Calibration</h2>
 * Both calibration constants are set by how KEY ordering changes the LEGITIMATE commit-freeze window
 * that {@link ProgressProbe}'s Class 2 lag probe tolerates, and both were moved by measurement rather
 * than by argument - each carries its measured evidence:
 * <ul>
 *   <li>{@link #HEAVY_SLEEP} - a dwell longer than the gap between rebalances chains indefinitely under
 *   at-least-once, pinning a partition's commit watermark for the whole run.</li>
 *   <li>{@link #KEY_SPACE} - coprime with {@link #HEAVY_EVERY}, or the entire heavy tail serialises onto
 *   one key's shard; {@link #heavyRecordsMustNotAllShareOneKey} is the check, not a comment.</li>
 * </ul>
 * Volume is sized per instance, for the reason on {@link #EXPECTED_MESSAGES}.
 *
 * <h2>The ledger is calibrated RED as well as GREEN</h2>
 * The control arm is this scenario with one character changed - {@link ProcessingOrder#UNORDERED}
 * instead of {@code KEY}, everything else identical - which removes the guarantee while leaving the
 * workload, the fleet and the churn alone. Measured 2026-08-18 at 90k:
 * <ul>
 *   <li><b>KEY (this scenario)</b>: {@code orderRegressions=0 overlaps=0} over 90,401 deliveries, of
 *   which 85,433 were compared against a predecessor in their own window. GREEN.</li>
 *   <li><b>UNORDERED (control)</b>: {@code orderRegressions=1 overlaps=857} over 90,530 deliveries.
 *   RED - and the shape of the RED is informative: PC still hands out a partition's records in
 *   ascending offset order, so an ORDER regression is rare; what UNORDERED actually loses is the
 *   serialisation, which is why {@code LEDGER_KEY_CONCURRENCY} exists. An offset-order-only detector
 *   would have called this arm green 857 times over.</li>
 * </ul>
 * <p>
 * Seed protocol: {@code -Dchaos.seed=<long>} replays a schedule; unset = random seed, always logged.
 * Excluded from default suites via {@code @Tag("chaos")}; run with {@code -Dincluded.groups=chaos}.
 */
@Tag("chaos")
@Timeout(600)
@Testcontainers
@Slf4j
class ChaosKeyOrderIT extends ChaosScenarioBase {

    private static final int PARTITIONS = 80;
    /**
     * Sized so the run lasts long enough to be CHURNED, which is the constraint here - not throughput
     * and not the cap. At 24k the calibrated shape drained in 14s, which at a 500-1500ms tick is barely
     * twenty disturbances and about two assignment epochs per key: green, but asserting almost nothing
     * about order under rebalance. Measured on this axis: 24k = 14s; 60k = 26s, 16 disturbances,
     * lag-stagnation peak 11s; 90k = 42s, 20 disturbances, peak 22s; 120k = 88s, 39 disturbances, peak
     * 77s. The peak tracks run
     * length and the Class 2 bound is 150s, so volume buys churn against margin - and CI runs the chaos
     * lane with {@code -DforkCount=4}, where everything here is slower than these single-fork numbers.
     * This is the constant to cut first if that probe ever comes close.
     */
    private static final int EXPECTED_MESSAGES = 90_000;
    /**
     * ~40 records per key, so a key's shard holds a real sequence for the ordering ledger to be able to
     * fail on. Also >> the fleet's total worker slots ({@code MAX_FLEET x maxConcurrency} = 100), so
     * per-key serialisation does not starve the run of parallelism and turn it into a throughput test.
     * <p>
     * COPRIME with {@link #HEAVY_EVERY}, and asserted so at the top of the run - see
     * {@link #heavyRecordsMustNotAllShareOneKey}.
     */
    private static final int KEY_SPACE = 2_251;
    private static final int INITIAL_FLEET = 8;
    private static final int MAX_FLEET = 10;
    private static final double PRE_PRODUCE_FRACTION = 0.3;
    private static final Duration RUN_CAP = Duration.ofMinutes(5);
    private static final int HEAVY_EVERY = 2_000;
    /**
     * 3s, far below W1's 45s, because a dwell interacts with churn differently under KEY ordering. A
     * revoke fences a record's completion, and at-least-once re-runs it FRESH on the next owner - so a
     * dwell longer than the gap between rebalances (measured here: one per instance every ~10s) chains
     * indefinitely, and the record's partition commit watermark is pinned for the whole run. At 10s that
     * chain never terminated: three runs measured a lag-stagnation peak of 154s against
     * {@link ProgressProbe#LAG_STAGNATION_BOUND}'s 150s. 3s completes inside the gap, so the chain is
     * short and the legitimate freeze window stays far under the bound - while still being ~3000x a
     * normal record, which is what makes revokes catch real in-flight work.
     */
    private static final Duration HEAVY_SLEEP = Duration.ofSeconds(3);

    /** One recorder for the whole fleet and the whole run - see {@link ChaosScenarioBase#orderRecorder}. */
    private final KeyOrderLedger.Recorder recorder = new KeyOrderLedger.Recorder();

    @Override
    protected KeyOrderLedger.Recorder orderRecorder() {
        return recorder;
    }

    /** Repeated keys: this is the shard, and so the unit PC promises order within. */
    @Override
    protected String keyFor(int i) {
        return "k-" + (i % KEY_SPACE);
    }

    /** The unique record identity - matches the value {@code produceRange} sends ({@code "v-" + i}). */
    @Override
    protected String identityFor(int i) {
        return "v-" + i;
    }

    @Override
    protected String identityOf(PollContext<String, String> context) {
        return context.value();
    }

    /**
     * The heavy tail is spaced on the record identity, so which KEYS carry it is decided by
     * {@code HEAVY_EVERY mod KEY_SPACE} - and when {@link #KEY_SPACE} divides {@link #HEAVY_EVERY},
     * EVERY heavy record lands on key {@code k-0}. KEY ordering then serialises the whole heavy tail
     * onto one shard on one partition, pinning that partition's commit watermark until the run ends:
     * measured at {@code KEY_SPACE=400, HEAVY_EVERY=2000} as a 154s lag stagnation on exactly one
     * partition, whose blocking record was {@code o:40:k:k-0}. That reads as a Class 2 stall and is a
     * workload artifact, so it is checked rather than commented.
     */
    @Test
    void heavyRecordsMustNotAllShareOneKey() {
        assertWithMessage("KEY_SPACE and HEAVY_EVERY must be coprime, or the whole heavy tail serialises "
                + "onto one key's shard and pins its partition's commit watermark for the run")
                .that(BigInteger.valueOf(KEY_SPACE).gcd(BigInteger.valueOf(HEAVY_EVERY)).intValue())
                .isEqualTo(1);
    }

    @Test
    void perKeyOrderSurvivesChurn() throws Exception {
        ChaosSeed seed = resolveSeed();
        String replayCmd = seed.replayCommand();
        log.info("=== CHAOS W5 key order: seed={} (replay: {}) ===", seed.getValue(), replayCmd);

        String topic = getClass().getSimpleName() + "-w5-" + RandomUtils.nextInt();
        ensureTopic(topic, PARTITIONS);

        ManagedPCInstance.Config pcConfig = ManagedPCInstance.Config.builder()
                .commitMode(CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS)
                // the whole point of this scenario - the other two run UNORDERED
                .order(ProcessingOrder.KEY)
                .inputTopic(topic)
                .pollDelayMs(1)
                .maxConcurrency(10)
                .build();

        FleetBootstrap fleet = bootstrapFleet(topic, pcConfig, EXPECTED_MESSAGES, PRE_PRODUCE_FRACTION,
                INITIAL_FLEET, HEAVY_EVERY, HEAVY_SLEEP);
        AtomicLong totalConsumed = fleet.getTotalConsumed();
        Queue<String> allConsumed = fleet.getAllConsumed();
        Set<String> expectedKeys = fleet.getExpectedKeys();
        ProgressProbe probe = fleet.getProbe();

        ChaosConductor conductor = conductorFor(fleet, pcConfig, HEAVY_EVERY, HEAVY_SLEEP, MAX_FLEET)
                .seed(seed.getValue())
                // slower than W1's 500-1500ms, and the ONE thing that has to be different about this
                // scenario's churn: at W1's rate membership changes arrive faster than an EAGER rebalance
                // can complete, so the group never returns to Stable and ProgressProbe's CONTINUOUS
                // rebalance-dwell clock accrues past its 15s bound with no member actually being a zombie
                // (measured 15.5s). It is also what this ledger needs - a group that never stabilises
                // splits every key's sequence into windows of one delivery, which assert nothing
                .minTick(Duration.ofMillis(1_000))
                .maxTick(Duration.ofMillis(2_500))
                .joinAfterDrainBias(0.9)
                .build();

        startRun(probe, conductor);

        try {
            await().alias("all messages consumed under churn, in key order")
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
