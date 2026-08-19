package bz.stub.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.integrationTests.utils.ManagedPCInstance;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

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

    /**
     * <b>Diagnostic only - never a way to make this test pass.</b> Set {@code -Dchaos.diagnoseStallRecovery=true}
     * to answer the one question the gating configuration structurally cannot: when a Class 2 stall
     * fires, does the frozen partition <b>ever</b> recover, or is it wedged forever?
     * <p>
     * The scenario's own arithmetic above asserts that "a REAL Class 2 stall is unbounded", and the
     * probe's javadoc records RED calibration as still open. Neither has been tested, because the
     * gating run destroys the evidence at the moment of detection: {@code failFast} aborts the wait on
     * the first violation and {@code QUIET_CAP} gives up at 5 minutes, so every observation to date
     * ends the instant the stall is confirmed. Unbounded and merely-slow are indistinguishable from
     * that data.
     * <p>
     * In this mode the quiet phase does not bail on a violation and waits {@link #DIAGNOSTIC_QUIET_CAP}
     * instead, logging consumption progress each poll. The discriminator is which way the wait ends:
     * the backlog drains (the stall was bounded - a starvation or fairness defect) or it times out with
     * consumption flat (unbounded - lost state, a partition paused and never resumed, or a lost wakeup).
     * <p>
     * <b>It cannot turn a red run green.</b> {@code assertScenarioSlos} still asserts the probe's
     * violations are empty after the wait, whichever way the wait ended, so a run that trips the probe
     * still fails - it just fails having recorded what happened next. Off by default; the gating
     * configuration is byte-for-byte unchanged when the property is absent.
     */
    private static final boolean DIAGNOSE_STALL_RECOVERY = Boolean.getBoolean("chaos.diagnoseStallRecovery");

    /**
     * Quiet cap in {@link #DIAGNOSE_STALL_RECOVERY} mode - long enough that "never recovered" means
     * something. Override with {@code -Dchaos.diagnosticQuietCapMinutes=<n>}.
     * <p>
     * <b>The scenario class's {@code @Timeout} is the real ceiling, and it wins silently.</b> Those
     * are 600s today, so a quiet cap above about six minutes cannot be reached - JUnit kills the
     * test first, mid-observation, and the run then looks like one that stopped for its own reasons
     * rather than one that was cut off. Raise the annotation if you genuinely need a longer watch;
     * do not just raise this number and believe the result.
     */
    private static final Duration DIAGNOSTIC_QUIET_CAP =
            Duration.ofMinutes(Integer.getInteger("chaos.diagnosticQuietCapMinutes", 20));
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

    /**
     * The conductor's action mix. Defaults to {@link ChaosConductor#defaultW4Weights()} - <b>no drain
     * stops at all</b>, because a drain opens the Class 1 zombie window and would mask the Class 2
     * mechanism this scenario exists to isolate.
     * <p>
     * A subclass overrides this only to run a deliberate CONTROL ARM against that choice, and owes
     * the reader why in its own javadoc: inverting it changes which failure class the scenario can
     * see, so a variant that overrides this is answering a different question, not running the same
     * test more gently.
     */
    protected java.util.Map<ChaosConductor.ChaosAction, Integer> chaosWeights() {
        return ChaosConductor.defaultW4Weights();
    }

    protected void runRevokeUnderWorkScenario() throws Exception {
        ChaosSeed seed = resolveSeed();
        log.info("=== CHAOS {} revoke-under-work (cooperative={}): seed={} (replay: {}) ===",
                scenarioLabel(), useCooperativeAssignor(), seed.getValue(), seed.replayCommand());

        String topic = getClass().getSimpleName() + "-" + scenarioLabel() + "-" + RandomUtils.nextInt();
        ensureTopic(topic, PARTITIONS);

        Properties quickEviction = new Properties();
        quickEviction.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, MAX_POLL_INTERVAL_MS);
        ManagedPCInstance.Config pcConfig = ManagedPCInstance.Config.builder()
                // sync commits sharpen revoke-vs-commit lock contention - the confluentinc#857 deadlock recipe
                .commitMode(CommitMode.PERIODIC_CONSUMER_SYNC)
                .order(ProcessingOrder.UNORDERED)
                .inputTopic(topic)
                .pollDelayMs(1)
                .maxConcurrency(10)
                .useCooperativeAssignor(useCooperativeAssignor())
                .extraConsumerProps(quickEviction)
                .build();

        FleetBootstrap fleet = bootstrapFleet(topic, pcConfig, EXPECTED_MESSAGES, PRE_PRODUCE_FRACTION,
                INITIAL_FLEET, HEAVY_EVERY, HEAVY_SLEEP);
        AtomicLong totalConsumed = fleet.getTotalConsumed();
        AtomicLong totalStarted = fleet.getTotalStarted();
        Queue<String> allConsumed = fleet.getAllConsumed();
        Set<String> expectedKeys = fleet.getExpectedKeys();
        ProgressProbe probe = fleet.getProbe()
                // Class 1 dwell gating belongs to W1; here blocked rebalances self-resolve by eviction
                // (30s max.poll.interval) and firing on them would mask the Class 2 measurement
                .disableRebalanceDwellViolation()
                // storm-phase rebalances can legitimately pause much of the fleet for up to the
                // eviction horizon (all of it, under the eager assignor); widen the watermark beyond it
                .withNoProgressWindow(Duration.ofSeconds(60));

        ChaosConductor conductor = conductorFor(fleet, pcConfig, HEAVY_EVERY, HEAVY_SLEEP, MAX_FLEET)
                .seed(seed.getValue())
                // faster ticks than W1: more rebalances per run = more revoke-under-work collisions
                .minTick(Duration.ofMillis(300))
                .maxTick(Duration.ofMillis(1000))
                .weights(chaosWeights())
                .joinAfterDrainBias(0) // no drains to bias after
                .build();

        startRun(probe, conductor);

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
            org.awaitility.core.ConditionFactory quiet =
                    await().alias("backlog drained after the storm settles (quiet phase)")
                            .pollInterval(Duration.ofSeconds(2));
            if (DIAGNOSE_STALL_RECOVERY) {
                // Deliberately no failFast: the whole point is to keep watching AFTER the violation.
                log.warn("=== chaos.diagnoseStallRecovery ACTIVE - quiet cap {} and no fail-fast. " +
                        "This is a DIAGNOSTIC run: violations are still asserted at the end, so this " +
                        "cannot make the test pass. ===", DIAGNOSTIC_QUIET_CAP);
                // The prior art nobody greps, delivered at the moment it is about to be repeated: the
                // six prior-art checks in AGENTS.md search docs, PRs and issues, and none of them
                // reaches a class javadoc. This exact experiment was run once before at the 90s/45s
                // shape and is recorded there, and was re-derived in August 2026 by someone who had
                // done the documented checks correctly.
                log.warn("=== BEFORE INTERPRETING THIS RUN, read the scenario class's 'Calibration " +
                        "status' javadoc. A recovery diagnostic has been run on this family before " +
                        "and already established that the freeze RESOLVES and the backlog completes " +
                        "at the pre-2026-07-30 shape. If your result is 'it recovers', you have " +
                        "reproduced a known result - the NEW question is whether it still recovers " +
                        "at the current shape, and how long it takes against the {} bound. ===",
                        ProgressProbe.LAG_STAGNATION_BOUND);
                quiet = quiet.atMost(DIAGNOSTIC_QUIET_CAP);
            } else {
                quiet = quiet.atMost(QUIET_CAP).failFast("probe violation", probe::hasViolations);
            }
            quiet.until(() -> {
                boolean done = totalConsumed.get() >= EXPECTED_MESSAGES
                        && allConsumedCovers(expectedKeys, allConsumed);
                if (DIAGNOSE_STALL_RECOVERY) {
                    // Both ends of the user function, because a completion counter alone cannot tell
                    // "nothing is finishing" from "nothing is happening": a fleet all sitting inside
                    // HEAVY_SLEEP reads as a flat line while it is fully busy. inFlight is the
                    // difference, and it is what makes a flat consumed count interpretable.
                    long started = totalStarted.get();
                    long consumed = totalConsumed.get();
                    log.info("[diagnose] quiet phase: consumed={}/{} started={} inFlight={} violations={} done={}",
                            consumed, EXPECTED_MESSAGES, started, started - consumed,
                            probe.getViolations().size(), done);
                }
                return done;
            });
        } finally {
            settleRun(conductor, probe, fleet.getProducerThread(), fleet.getPcExecutor(), totalConsumed);
        }

        assertScenarioSlos(probe, conductor, seed.replayCommand(), expectedKeys, allConsumed);
    }
}
