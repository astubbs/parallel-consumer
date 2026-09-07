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
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.awaitility.Awaitility.await;

/**
 * The shared two-phase "revoke under work" driver behind the W4 scenario family - see
 * {@link ChaosRevokeUnderWorkIT} (eager assignor; the original, with the full design + calibration
 * javadoc) and {@link ChaosRevokeUnderWorkCooperativeIT} (cooperative-sticky). Variants differ only by
 * the abstract and overridable hooks below; the storm/quiet mechanics, probe configuration, and
 * ledger are identical so each variant changes exactly one experimental variable.
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
     * The Class 2 diagnostic recovery result, recorded here (rather than restated at every call site)
     * because it is THIS FAMILY's own prior art for {@link ChaosScenarioBase#DIAGNOSE_STALL_RECOVERY}:
     * the scenario's own arithmetic above asserts that "a REAL Class 2 stall is unbounded", and until
     * 2026-08-25 that had never been tested, because a gating run destroys the evidence at the moment
     * of detection - {@code failFast} aborted the wait on the first violation and {@code QUIET_CAP}
     * gave up at 5 minutes, so every observation ended the instant the finding was confirmed.
     * <p>
     * <b>It has now been run, and the answer was "it recovers".</b> Two replays of the seeds the
     * sightings ledger nominated as its strongest evidence both crossed the bound and then drained to
     * {@code inFlight=0} with full key coverage - which is why the Class 2 bound is a non-gating
     * observation today rather than a violation. Numbers in
     * {@code docs/inflight/bug-857-family.md}'s 2026-08-25 entry. The mode stays because the question
     * recurs per seed, not because it is unanswered.
     */
    @Override
    protected void logDiagnosticContext() {
        // The prior art nobody greps, delivered at the moment it is about to be repeated: the six
        // prior-art checks in AGENTS.md search docs, PRs and issues, and none of them reaches a class
        // javadoc. This exact experiment was run once before at the 90s/45s shape and is recorded
        // there, and was re-derived in August 2026 by someone who had done the documented checks
        // correctly.
        log.warn("=== BEFORE INTERPRETING THIS RUN, read the scenario class's 'Calibration " +
                "status' javadoc. A recovery diagnostic has been run on this family before " +
                "and already established that the freeze RESOLVES and the backlog completes " +
                "at the pre-2026-07-30 shape. If your result is 'it recovers', you have " +
                "reproduced a known result - the NEW question is whether it still recovers " +
                "at the current shape, and how long it takes against the {} bound. ===",
                ProgressProbe.LAG_STAGNATION_BOUND);
    }

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
    /**
     * The commit mode this scenario runs in. Defaults to {@code PERIODIC_CONSUMER_SYNC} - sync
     * commits sharpen revoke-versus-commit lock contention, which is the confluentinc#857 deadlock
     * recipe and why every scenario in this family used it.
     *
     * <p><b>Promoted to an accessor so the transactional half of the family is reachable at all.</b>
     * It was a hardcoded constant, and combined with {@code ManagedPCInstance} never wiring a
     * producer, that made {@code PERIODIC_TRANSACTIONAL_PRODUCER} unreachable by construction rather
     * than by decision. The consequence was not academic: astubbs/parallel-consumer#44 - the only
     * issue upstream ever labelled a verified bug - is in that mode, and so is the unbounded revoke
     * wait in {@code docs/inflight/bug-857-transactional-revoke-wait.md}. The suite built to hunt
     * this family could not enter the room they are in.
     *
     * <p>Overriding it changes what a scenario measures, so a variant that does should say what it
     * expects to see that the sync arm cannot - see {@link ChaosRevokeUnderWorkTransactionalIT}.
     */
    protected CommitMode commitMode() {
        return CommitMode.PERIODIC_CONSUMER_SYNC;
    }

    protected abstract boolean useCooperativeAssignor();

    /** Short label for topic names and log lines (e.g. "w4" / "w4coop"). */
    protected abstract String scenarioLabel();

    /**
     * The processing order - {@link ProcessingOrder#UNORDERED} for the assignor/stop-mode matrix
     * cells, which make no ordering claim (and produce a unique key per record, so a KEY override
     * without the {@code ChaosScenarioBase} identity-trio overrides would assert nothing). Promoted
     * to an accessor - per this class's constants rule above - for
     * {@link ChaosRevokeUnderWorkKeyOrderIT}, the cell that exists to make the ordering claim.
     */
    protected ProcessingOrder processingOrder() {
        return ProcessingOrder.UNORDERED;
    }

    /**
     * The heavy-tail dwell - {@link #HEAVY_SLEEP} (20s) for the matrix cells, whose legit-freeze
     * arithmetic above depends on it. Promoted to an accessor because a KEY-ordered cell CANNOT run
     * 20s dwells: a dwell longer than the gap between storm rebalances chains indefinitely under
     * at-least-once, and KEY ordering pins the whole shard behind it -
     * {@code ChaosKeyOrderIT}'s {@code HEAVY_SLEEP} carries the measurement (a 154s stagnation at a 10s dwell).
     */
    protected Duration heavySleep() {
        return HEAVY_SLEEP;
    }

    /**
     * Storm tick range - 300-1000ms for the matrix cells (more rebalances per run = more
     * revoke-under-work collisions, the class javadoc's calibration). Promoted for the KEY cell,
     * which CANNOT run that fast under the eager assignor: {@code ChaosKeyOrderIT}'s
     * {@code minTick} javadoc records that membership changes arriving faster than an eager
     * rebalance completes keep the group permanently unstable, which both accrues the Class 1 dwell
     * clock artificially AND splits every key's sequence into single-delivery assignment-epoch
     * windows - and a window with one delivery asserts nothing, so the ordering ledger it exists for
     * would go vacuous ({@code LEDGER_ORDER_VACUOUS}).
     */
    protected Duration minTick() {
        return Duration.ofMillis(300);
    }

    /** See {@link #minTick}. */
    protected Duration maxTick() {
        return Duration.ofMillis(1000);
    }

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
    protected Map<ChaosConductor.ChaosAction, Integer> chaosWeights() {
        return ChaosConductor.defaultW4Weights();
    }

    /**
     * The drain-only action mix, shared by this family's CONTROL ARMS
     * ({@link ChaosRevokeUnderWorkDrainIT} and {@link ChaosRevokeUnderWorkCooperativeDrainIT}) so the
     * two cells cannot drift apart and silently stop being the same control.
     * <p>
     * {@code STOP_DRAIN} takes the weight {@link ChaosConductor#defaultW4Weights()} gives
     * {@code STOP_NO_DRAIN}; RESTART and JOIN_NEW keep theirs, so the membership churn RATE is
     * comparable and only the MANNER of leaving changes. That is what keeps a control arm a control
     * arm rather than a different workload.
     */
    protected Map<ChaosConductor.ChaosAction, Integer> drainOnlyChaosWeights() {
        Map<ChaosConductor.ChaosAction, Integer> weights = new EnumMap<>(ChaosConductor.ChaosAction.class);
        weights.put(ChaosConductor.ChaosAction.STOP_DRAIN, 3); // was STOP_NO_DRAIN at 3
        weights.put(ChaosConductor.ChaosAction.RESTART, 3);
        weights.put(ChaosConductor.ChaosAction.JOIN_NEW, 2);
        return weights;
    }

    protected void runRevokeUnderWorkScenario() throws Exception {
        // The @Timeout clock starts here, so the time-remaining sum below has to measure from here too.
        Instant methodStart = Instant.now();
        ChaosSeed seed = resolveSeed();
        log.info("=== CHAOS {} revoke-under-work (cooperative={}): seed={} (replay: {}) ===",
                scenarioLabel(), useCooperativeAssignor(), seed.getValue(), seed.replayCommand());

        String topic = getClass().getSimpleName() + "-" + scenarioLabel() + "-" + RandomUtils.nextInt();
        ensureTopic(topic, PARTITIONS);

        Properties quickEviction = new Properties();
        quickEviction.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, MAX_POLL_INTERVAL_MS);
        ManagedPCInstance.Config pcConfig = ManagedPCInstance.Config.builder()
                .commitMode(commitMode())
                .order(processingOrder())
                .inputTopic(topic)
                .pollDelayMs(1)
                .maxConcurrency(10)
                .useCooperativeAssignor(useCooperativeAssignor())
                .extraConsumerProps(quickEviction)
                .build();

        FleetBootstrap fleet = bootstrapFleet(topic, pcConfig, EXPECTED_MESSAGES, PRE_PRODUCE_FRACTION,
                INITIAL_FLEET, HEAVY_EVERY, heavySleep());
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

        ChaosConductor conductor = conductorFor(fleet, pcConfig, HEAVY_EVERY, heavySleep(), MAX_FLEET)
                .seed(seed.getValue())
                // matrix cells: faster ticks than W1 - more rebalances per run = more
                // revoke-under-work collisions (the KEY cell overrides - see minTick())
                .minTick(minTick())
                .maxTick(maxTick())
                .weights(chaosWeights())
                // 0 in every cell, for different reasons: the matrix cells have no drains to bias
                // after, and the drain control arms deliberately keep the unbiased join schedule so
                // only the MANNER of leaving changes against their no-drain counterparts
                .joinAfterDrainBias(0)
                .build();

        startRun(probe, conductor);

        try {
            // Phase 1 - storm: revocations under heavy in-flight work; bail early on any VIOLATION.
            // Since the Class 2 lag bound became a non-gating observation (2026-08-25) it no longer
            // trips hasViolations(), so a storm that crosses that bound now runs its full
            // STORM_DURATION where it used to cut short. That is intended - the bound measures speed,
            // and ending the disturbance early because the fleet was slow removed exactly the load the
            // storm exists to apply - but it does lengthen those particular runs, so a wall-clock
            // comparison against a pre-demotion run is not like-for-like.
            Instant stormEnd = Instant.now().plus(STORM_DURATION);
            while (Instant.now().isBefore(stormEnd) && !probe.hasViolations()) {
                Thread.sleep(1_000);
            }
            conductor.stop();
            log.info("=== {} storm phase over (consumed={}) - entering quiet observation ===",
                    scenarioLabel(), totalConsumed.get());

            // Phase 2 - quiet observation: group must settle, evict any storm-wedged member, and FINISH.
            // The protocol-invisible signals are the ones that can still fire here. Since 2026-08-25 the
            // Class 2 lag bound is a non-gating OBSERVATION, so what can fail this phase is
            // INSTANCE_STALL - which watches completions and cannot fire on slow-but-progressing - plus
            // the fleet watermark and the end-of-run ledger. INSTANCE_STALL is per-instance, so a
            // single wedged shard beside busy siblings fails nothing here: see
            // docs/inflight/test-per-shard-liveness-has-no-gate.md.
            diagnosableWait("backlog drained after the storm settles (quiet phase)", methodStart, QUIET_CAP,
                    "probe violation", probe)
                    .until(() -> {
                        boolean done = totalConsumed.get() >= EXPECTED_MESSAGES
                                && allConsumedCovers(expectedKeys, allConsumed);
                        logDiagnosticProgress("quiet phase", EXPECTED_MESSAGES, totalStarted, totalConsumed,
                                probe, done);
                        return done;
                    });
        } finally {
            settleRun(conductor, probe, fleet.getProducerThread(), fleet.getPcExecutor(), totalConsumed);
        }

        assertScenarioSlos(probe, conductor, seed.replayCommand(), expectedKeys, allConsumed);
    }
}
