package bz.stub.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.PollContext;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigInteger;
import java.time.Duration;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Chaos Pain Suite - W4's ORDERING cell: {@link ProcessingOrder#KEY} over repeated keys under the
 * revoke-under-work storm, so {@link KeyOrderLedger} is exercised by the suite's most violent
 * revocation shape (eager assignor, hard stops and joins, sync commits, heavy work in flight at
 * every revoke). Closes the gap the 2026-08-19 assignor/stop-mode matrix left open: all four cells
 * verified no-loss, bounded-duplicates and completion, but ran {@code UNORDERED} over a unique key
 * per record - so none of them asserted anything about the guarantee PC exists for. {@code W5}
 * ({@link ChaosKeyOrderIT}) asserts it under W1-style continuous churn; this cell asserts it under
 * W4's storm-then-quiet revocation shape, where redelivery chains and assignment epochs are at
 * their densest.
 *
 * <h2>Deliberately a NEW cell, not a change to the matrix</h2>
 * The four {@code UNORDERED} cells stay exactly as measured: switching the shared driver to KEY
 * would serialise per-key work and invalidate the matrix's four recorded measurements (and with the
 * driver's unique-key workload it would also assert nothing -
 * {@code KeyOrderLedger#check} calls such a history vacuous out loud). This cell overrides the
 * driver's promoted hooks ({@code processingOrder()}, {@code heavySleep()}) plus
 * {@link ChaosScenarioBase}'s identity trio, and changes nothing else - storm, weights, commit mode
 * and fleet shape are inherited, so the ordering claim is added to the same disturbance the matrix
 * measured.
 *
 * <h2>Three calibration traps - two already paid for in {@link ChaosKeyOrderIT}, one paid for here</h2>
 * <ul>
 *   <li><b>{@link #KEY_SPACE} must be coprime with {@code HEAVY_EVERY}</b> (2000): the heavy tail is
 *   spaced on the record index, so a common factor lands the entire tail on few keys - in the worst
 *   case one - and KEY ordering then serialises every dwell onto one shard, pinning its partition's
 *   commit watermark into a measured 154s false Class 2 stall.
 *   {@link #heavyRecordsMustNotAllShareOneKey} is the check, not a comment.</li>
 *   <li><b>{@link #HEAVY_SLEEP_KEY} is 3s, not the matrix cells' 20s</b>: a revoke fences a heavy
 *   record's completion and at-least-once re-runs it fresh on the next owner, so a dwell longer than
 *   the gap between storm rebalances (300-1000ms ticks here) chains for the whole storm - under KEY
 *   ordering with the whole shard queued behind it. 3s completes inside the post-storm gap, keeping
 *   the legitimate freeze window (60s storm + short chains + commit slack, ~90s) well under the
 *   150s Class 2 bound, while still ~3000x a normal record so revokes genuinely catch work in
 *   flight.</li>
 *   <li><b>Storm ticks are W5's 1000-2500ms, not the matrix cells' 300-1000ms</b> - eager rebalances
 *   must COMPLETE between membership changes or every key's sequence splits into single-delivery
 *   epoch windows and the ordering ledger asserts nothing; see {@link #STORM_TICK_MIN}, whose
 *   javadoc carries the measured cost of getting this wrong.</li>
 * </ul>
 * <p>
 * ~111 records per key ({@code EXPECTED_MESSAGES} 250k / 2251), so every key's shard holds a real
 * sequence; {@link #KEY_SPACE} is also far above the fleet's ~140 worker slots
 * ({@code MAX_FLEET x maxConcurrency}), so per-key serialisation does not starve the run of
 * parallelism. The record IDENTITY moves into the VALUE ({@link #identityFor}/{@link #identityOf}),
 * keeping the loss/duplicate ledger asserting exactly what it asserts in every other scenario.
 * <p>
 * Seed protocol: {@code -Dchaos.seed=<long>} replays a schedule; unset = random seed, always logged.
 * Excluded from default suites via {@code @Tag("chaos")}; run with {@code -Dincluded.groups=chaos}.
 */
@Tag("chaos")
@Timeout(600)
@Testcontainers
@Slf4j
class ChaosRevokeUnderWorkKeyOrderIT extends AbstractRevokeUnderWorkScenario {

    /**
     * Prime (so coprime with any {@code HEAVY_EVERY}), sized like {@code ChaosKeyOrderIT}'s {@code KEY_SPACE}
     * and for the same reasons - see the class javadoc's trap list.
     */
    static final int KEY_SPACE = 2_251;

    /** See the class javadoc's trap list for why this is far below the matrix cells' 20s. */
    private static final Duration HEAVY_SLEEP_KEY = Duration.ofSeconds(3);

    /**
     * W5's tick range (1000-2500ms), NOT the matrix cells' 300-1000ms - the third trap, and this one
     * was rediscovered the expensive way before this override existed: at 300-1000ms the first run of
     * this cell (2026-08-19, seed 4734674029169027864) had eager rebalances arriving faster than they
     * complete, which under KEY ordering both shreds key sequences into single-delivery epoch windows
     * (asserting nothing - the vacuity {@code KeyOrderLedger#check} exists to call out) and re-delivers
     * every revoked shard's queue on each storm tick, pinning two partitions' commit watermarks to a
     * 154s stagnation against the 150s Class 2 bound. The ordering claim needs windows a rebalance
     * GAP long; the revoke-under-work collisions it also needs survive at this rate (the storm still
     * lands ~40 membership actions on heavy in-flight work).
     */
    private static final Duration STORM_TICK_MIN = Duration.ofMillis(1_000);
    private static final Duration STORM_TICK_MAX = Duration.ofMillis(2_500);

    /** One recorder for the whole fleet and the whole run - see {@link ChaosScenarioBase#orderRecorder}. */
    private final KeyOrderLedger.Recorder recorder = new KeyOrderLedger.Recorder();

    @Override
    protected boolean useCooperativeAssignor() {
        // the eager arm: every membership change revokes ALL partitions from ALL members, which is
        // the maximum redelivery/epoch churn available - the strongest test of the ordering window
        return false;
    }

    @Override
    protected String scenarioLabel() {
        return "w4key";
    }

    @Override
    protected ProcessingOrder processingOrder() {
        return ProcessingOrder.KEY;
    }

    @Override
    protected Duration heavySleep() {
        return HEAVY_SLEEP_KEY;
    }

    @Override
    protected Duration minTick() {
        return STORM_TICK_MIN;
    }

    @Override
    protected Duration maxTick() {
        return STORM_TICK_MAX;
    }

    @Override
    protected KeyOrderLedger.Recorder orderRecorder() {
        return recorder;
    }

    /** Repeated keys: the shard, and so the unit PC promises order within. */
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

    /** Same workload-artifact guard as {@code ChaosKeyOrderIT}'s {@code heavyRecordsMustNotAllShareOneKey}. */
    @Test
    void heavyRecordsMustNotAllShareOneKey() {
        assertWithMessage("KEY_SPACE and HEAVY_EVERY must be coprime, or the whole heavy tail serialises "
                + "onto one key's shard and pins its partition's commit watermark for the run")
                .that(BigInteger.valueOf(KEY_SPACE).gcd(BigInteger.valueOf(HEAVY_EVERY)).intValue())
                .isEqualTo(1);
    }

    @Test
    void perKeyOrderSurvivesRevokeUnderWork() throws Exception {
        runRevokeUnderWorkScenario();
    }
}
