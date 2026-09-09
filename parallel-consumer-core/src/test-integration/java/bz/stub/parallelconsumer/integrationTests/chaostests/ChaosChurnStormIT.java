package bz.stub.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.integrationTests.utils.ManagedPCInstance;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

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
 * <b>Recovery diagnostic - already answered, do not re-derive</b>: under
 * {@code -Dchaos.diagnoseStallRecovery=true} (see {@link ChaosScenarioBase#DIAGNOSE_STALL_RECOVERY})
 * this scenario's asynchronous no-progress line was watched to a verdict on 2026-08-28 - the backlog
 * DRAINED on all six firings collected, which is what demoted that line to a timing proxy rather
 * than a distinct defect. A run that drains reproduces a known result; a run that stays FLAT is the
 * finding worth reporting.
 * <p>
 * <b>The no-progress WINDOW was widened on that verdict, 2026-09-09</b>, to
 * {@link ProgressProbe#CHURN_NO_PROGRESS_WINDOW} - the same bound W4 already held, reached here by a
 * different mechanism. Three things settled it, and the third is the one that chose the number:
 * <ul>
 *   <li><b>Nine firings have now been watched past detection and all nine drained, on four seeds</b>
 *   - the six above, the 2026-09-08 replay of {@code 5650361238717170909} (93487 to 101070 with 6513
 *   outstanding at the firing), the 2026-09-09 hosted-runner run of {@code 3717713223451201639}, and
 *   a local replay of {@code 87978223167568} (97633 to 100742, full key coverage). Zero flat.</li>
 *   <li><b>The other term could not have been the one to move.</b> The firings sit 1196-6513 records
 *   short, against a {@link ProgressProbe#TAIL_SLACK} of 500: a slack wide enough to excuse them
 *   would be several percent of the backlog, and would blind the detector to the "stall with
 *   THOUSANDS remaining" that constant's own javadoc names as the defect signature. The window is
 *   crossed by seconds; the slack would have to be crossed by an order of magnitude.</li>
 *   <li><b>The fleet's own pause length was measured, on PASSING runs too</b> - the longest stretch
 *   with no completion anywhere in the fleet while the detector was armed, read off the
 *   {@code [diagnose]} series rather than off the violation line (which can only ever say "the bound
 *   plus detection latency"). Across thirteen replays on one desktop the armed peak reached 32.1s,
 *   with three further runs at 28.2-30.1s that did not fire: the 30s bound sat inside the ordinary
 *   distribution and three passing runs missed it by under two seconds. 60s is 1.9x that peak -
 *   the same METHOD {@link ProgressProbe#REBALANCE_DWELL_BOUND} was sized by, a multiple of a
 *   measured healthy peak, though at a smaller multiple than its 2.2x.</li>
 * </ul>
 * <b>The widening is not a disabling</b>, and that is asserted rather than argued.
 * {@code NoProgressWindowIT} fires the detector at the wider bound on a fleet that genuinely stops,
 * pins the calibrated numbers as values so a later edit to them cannot pass silently, drives the
 * SAMPLER rather than only the decision it calls, and asserts that this class's
 * {@link #configureProbe} really applies the wider bound - each of those arms verified by a one-term
 * sabotage that reddens it and nothing else.
 * <p>
 * <b>What it costs, stated rather than left to be discovered</b>: a genuine fleet-wide stall of 31
 * to 60 seconds with work outstanding now passes this scenario. A real wedge does not stop at 60s,
 * so what is lost is early detection rather than detection - and the finer cases the fleet-wide
 * counter never covered (one wedged member, one wedged partition) have their own owners. The
 * separate question of whether this detector MISSES real failures ({@code docs/testing.md},
 * "Experiment runners") is untouched and a wider window can only make it more pressing. The seeds,
 * the trajectories and the per-run numbers are in
 * {@code docs/inflight/test-no-progress-window-may-not-transfer-to-w1.md}.
 * <p>
 * <b>The instance-stall line - also answered, 2026-09-07, do not re-derive</b>: seed
 * {@code 6077035105695} replays the shape behind every {@code INSTANCE_STALL/NO_WORK_COMPLETED}
 * firing on this scenario - one live member's returned-result count frozen while its records-out
 * climbs - on every run, and a thread dump taken inside that window
 * ({@code -Dchaos.instanceStallDumpAfterSeconds=20} with the diagnostic above) shows all ten of its
 * workers inside {@code HEAVY_SLEEP}, the control thread idle on its mailbox, and most of the
 * records out belonging to partitions it no longer owns. It is worker saturation by redelivered
 * heavy dwells: the eager assignor revokes the whole assignment every few seconds under this churn,
 * so each 45s dwell is stale before it ends and is redelivered while the old copy keeps sleeping. A
 * cooperative-assignor control arm on the same seed removes the amplification and leaves one honest
 * dwell. The detector therefore fires on the length of the tail, not on a PC defect; the record,
 * the dumps and the arithmetic are in {@code docs/inflight/test-857-churn-storm-async-stalls.md},
 * "DIAGNOSED, 2026-09-07".
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
 * {@code docs/testing.md}, "Chaos Pain Suite".
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

    /**
     * This scenario's own prior art for {@link ChaosScenarioBase#DIAGNOSE_STALL_RECOVERY}, which was a
     * long time arriving: unlike {@code AbstractRevokeUnderWorkScenario}, the flag was silently ignored
     * here until this override existed, so the asynchronous no-progress line went unsettled for weeks.
     * It has since been answered - this class's "Calibration status" javadoc carries the verdict and
     * its date. A drain is therefore a re-derivation; a flat backlog is the finding.
     */
    @Override
    protected void logDiagnosticContext() {
        log.warn("=== BEFORE INTERPRETING THIS RUN, read this class's 'Calibration status' javadoc. " +
                "The recovery diagnostic has engaged on this scenario before and the backlog DRAINED " +
                "on every one of nine firings across four seeds, which is what demoted the " +
                "asynchronous stall to a timing proxy and then widened this scenario's no-progress " +
                "window to 60s. If your result is 'it drains', you have reproduced a known result - " +
                "the finding worth reporting is a run that stays FLAT. ===");
    }

    /**
     * This scenario's probe configuration, named so it can be ASSERTED rather than only run.
     * {@code NoProgressWindowIT} calls it, so deleting the widening below turns a fast broker-free
     * test red instead of quietly changing what a five-minute chaos run gates on - a run whose own
     * replay data says the crossing fires roughly once in thirteen, so a regression here would
     * otherwise hide for a long time.
     * <p>
     * This scenario's own churn crosses the 30s default while the fleet is merely slow: the eager
     * assignor revokes the whole assignment every few seconds, so each 45s {@link #HEAVY_SLEEP} is
     * redelivered before it ends and the fleet can sit wholly inside the heavy tail with nothing
     * COMPLETING while every member is working. Widened on the evidence, not on the resemblance -
     * see this class's "Calibration status" javadoc.
     */
    static ProgressProbe configureProbe(ProgressProbe probe) {
        return probe.withNoProgressWindow(ProgressProbe.CHURN_NO_PROGRESS_WINDOW);
    }

    @Test
    void churnStormMeetsSlosAndBalancesLedger() throws Exception {
        // The @Timeout clock starts here, so effectiveDiagnosticQuietCap's time-remaining sum must too.
        Instant methodStart = Instant.now();
        ChaosSeed seed = resolveSeed();
        log.info("=== CHAOS W1 churn storm: seed={} (replay: {}) ===", seed.getValue(), seed.replayCommand());

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
        AtomicLong totalStarted = fleet.getTotalStarted();
        Queue<String> allConsumed = fleet.getAllConsumed();
        Set<String> expectedKeys = fleet.getExpectedKeys();
        ProgressProbe probe = configureProbe(fleet.getProbe());

        ChaosConductor conductor = conductorFor(fleet, pcConfig, HEAVY_EVERY, HEAVY_SLEEP, MAX_FLEET)
                .seed(seed.getValue())
                .minTick(Duration.ofMillis(500))
                .maxTick(Duration.ofMillis(1500))
                .joinAfterDrainBias(0.9)
                .build();

        startRun(probe, conductor);

        try {
            // the run: everything produced must be consumed by SOMEONE within the cap, chaos or not -
            // or, under -Dchaos.diagnoseStallRecovery=true, watched PAST any violation instead of
            // aborting on it, to see whether the backlog drains or consumption stays flat (see
            // ChaosScenarioBase#diagnosableWait).
            diagnosableWait("all messages consumed under churn", methodStart, RUN_CAP,
                    "probe violation during run", probe)
                    .until(() -> {
                        boolean done = totalConsumed.get() >= EXPECTED_MESSAGES
                                && allConsumedCovers(expectedKeys, allConsumed);
                        logDiagnosticProgress("run", EXPECTED_MESSAGES, totalStarted, totalConsumed, probe, done);
                        return done;
                    });
        } finally {
            settleRun(conductor, probe, fleet.getProducerThread(), fleet.getPcExecutor(), totalConsumed);
        }

        assertScenarioSlos(probe, conductor, seed.replayCommand(), expectedKeys, allConsumed);
    }
}
