package bz.stub.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.AdaptiveConcurrencyMode;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.integrationTests.utils.ConstantRateFeeder;
import bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption;
import bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils.ProducerMode;
import bz.stub.parallelconsumer.integrationTests.utils.SyntheticCongestionCurve;
import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.internal.admission.AdmissionController;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Testcontainers;
import pl.tlinkowski.unij.api.UniSets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

/**
 * <b>The end-to-end claim under load that MOVES: adaptive concurrency against a hand-tuned static setting, three
 * arms, four asserted phases</b> (the 2026-08-24-003 design's R15, minus the rate-limit phase - see "Phase 4"
 * below). A compiled concurrency setting cannot change and the load can; a test that holds conditions still can
 * only ask whether adaptive beats a static number on the workload that number was tuned for, which is the weak
 * form nobody should publish. So here the downstream's capacity moves under all three arms while the arrival
 * rate holds constant, and everything asserted is measured <b>end to end</b> - residence from record creation to
 * completion, and useful completions per phase - never the target value itself (a controller that moves
 * impressively is not a controller that helps; {@code perf/split-shard-inflight} moved dispatch 10x and
 * end-to-end 0%).
 *
 * <h2>The three arms - and the middle one is the real opponent</h2>
 * <ol>
 * <li><b>A - static low</b> ({@code maxConcurrency} {@value #ARM_A_CONCURRENCY}, DISABLED): the floor. Recorded
 * only, never asserted - it makes the claim true and easy, which is why it proves nothing.</li>
 * <li><b>B - static hand-tuned</b> ({@code maxConcurrency} {@value #HEALTHY_KNEE_SLOTS}, DISABLED): what a
 * careful operator deploys after sweeping static values against the load they can see at deploy time. <b>The
 * phase-1 sweep IS how B is chosen</b>: the falsifier suite's FixedLimit initial-condition sweep against the
 * slots oracle establishes that the optimum for phase 1's plant is the knee itself
 * ({@code L* = mu_max * W0 = } {@value #PHASE1_CAPACITY_PER_SECOND}{@code /s * }{@value
 * #BASE_SERVICE_TIME_MS}{@code ms = }{@value #HEALTHY_KNEE_SLOTS}{@code slots}), so this IT instantiates that
 * known-optimal value directly rather than re-running the sweep against a broker. B is also the negative
 * control: a controller that never moves degenerates into exactly this arm and therefore cannot beat it.</li>
 * <li><b>C - adaptive</b> (ENFORCE, ceiling {@value #ARM_C_CEILING} - comfortably above every knee in the run,
 * seeded at {@value #ARM_C_SEED} so phase 1 shows the exploration cost honestly).</li>
 * </ol>
 *
 * <h2>The phases, the plant, and the oracle in slots</h2>
 * Arrival holds at {@value #ARRIVAL_RATE_PER_SECOND}/s for the whole schedule - {@value #ARRIVAL_FRACTION}
 * of phase-1 capacity, below phase-1 saturation (steady in-flight {@code = lambda * W0 = 6} slots, under the
 * knee of {@value #HEALTHY_KNEE_SLOTS}). The downstream is simulated inside the user function - the honest
 * place, since the user function IS the abstraction over whatever the real downstream is - as a
 * {@link SyntheticCongestionCurve} switched per phase:
 * <pre>
 *   phase  span         curve                          capacity  L* (slots)  what it models
 *   1      0-25s        quadratic(40ms, knee 8)        200/s     8           healthy steady state
 *   2      25s-85s      quadratic(40ms, knee 4)        100/s     4           degradation: capacity halves,
 *                                                                            over-driving collapses throughput
 *   3      85s-145s     quadratic(40ms, knee 12)       300/s     12          recovery beyond phase 1 (1.5x)
 *   5      145s-190s    linear(40ms, knee 4)           100/s     4           graceful saturation: flat plateau,
 *                                                                            latency climbs, arrival &gt; capacity
 *   drain  190s-end     linear(40ms, knee 4)           100/s     -           see "the drain is part of the
 *                                                                            measurement" below
 * </pre>
 * The oracle is absolute and per phase, not a fitted band: capacity and uncongested service time are SET by the
 * test, so Little's Law gives the correct admission target for each phase directly ({@code L* = capacity * W0 =}
 * the knee, batch size 1), and it moves when the phase moves - which is what makes the assertions falsifiable: a
 * frozen controller is provably wrong in at least three of the four phases.
 * <p>
 * <b>Why phases 1-3 are quadratic and phase 5 is linear - a documented deviation from the unit packet's single
 * linear formula.</b> Under an open-loop arrival process, a linear curve's throughput plateaus exactly flat
 * above the knee, so a saturated over-driven arm and a saturated at-the-knee arm complete records at the
 * identical rate - and with identical completion rates and FIFO order, every record's residence is identical
 * too. Linear-everywhere makes B and C <em>provably indistinguishable end to end</em> in every congested phase:
 * the wash is arithmetic, not noise. The comparison needs over-driving to cost something, which is the quadratic
 * curve (congestion collapse: throughput {@code = knee^2 / (W0 * inFlight)} above the knee). Phase 5 keeps the
 * linear plateau deliberately, because its claim is different: a flat plateau is the one shape where FALL can
 * never fire, so holding near the knee there exercises the HOLD band and the descent probe - the phase neither 2
 * (falls) nor 4 (rejects) exercises. Phase 5's residence assertion is carried by the queue-depth difference the
 * arms bring INTO it, which is the compounded cost of phases 2-3.
 * <p>
 * <b>Why {@value #ARRIVAL_FRACTION} of phase-1 capacity, not the packet's ~0.65.</b> Phase 3's assertion needs
 * both arms backlogged (work-rich) long enough that completions measure capacity, not luck: B completes
 * {@code capacity_B * len3 = 200/s * 60s = 12,000} while work-rich, so C beats it only if C's own work
 * (phase-2 backlog + arrivals) exceeds that. At 0.65 the margin is ~400 records (noise); at 0.75 it is ~1,500
 * (~13%). The phase-1 steady in-flight is still 6 of 8 slots - below saturation, as R15 requires.
 * <p>
 * <b>The drain is part of the measurement.</b> Residence is create-to-completion, and at phase-5 end both B and
 * C still hold queued records - so the feeder stops and the run drains before percentiles are computed
 * (censoring instead would clamp both arms' unfinished records to the same floor and erase the very difference
 * being measured). The drain plant is {@code linear(40ms, knee 4)}: on that plateau every arm's capacity is the
 * same 100/s regardless of its slot count, so draining changes no arm's relative queue depth and the phase-5
 * difference survives the drain intact. Arm A drowns by design (capacity 100/s under a 150/s arrival from phase
 * 1 onwards), so A alone gets a bounded drain ({@value #ARM_A_DRAIN_CAP_SECONDS}s) and its recorded-only rows
 * are labelled truncated.
 *
 * <h2>What is asserted, strictly, and what is only recorded</h2>
 * Phase 1 is recorded, never asserted: its measure is "match within tolerance", and until the tolerance is set
 * at benchmark time (the design's open question 1) an assertion would force a controller that games steady
 * state. Phases 2, 3 and 5 carry strict inequalities - see the constants for each margin's derivation:
 * <ul>
 * <li><b>Phase 2:</b> C's p95 residence strictly below B's. B over-drives the halved knee (8 in-flight on a
 * knee of 4 = 4x service time = 50/s against 150/s arrival); C backs off toward 4 and completes faster, so its
 * queue - and every record's wait - stays shorter. Predicted gap: several seconds on a ~30s p95.</li>
 * <li><b>Phase 3:</b> C's useful completions strictly above B's. B is capped at 200/s by the slots it was
 * hand-tuned to; C grows into the new headroom (knee 12 = 300/s). Both stay work-rich on their phase-2
 * backlogs, so completions measure capacity. Predicted gap: ~1,500 records.</li>
 * <li><b>Phase 5:</b> C's target settles near the knee ({@value #PHASE5_KNEE_BAND_FRACTION} band around
 * {@value #DEGRADED_KNEE_SLOTS} slots - the descent probe walking the plateau down), and C's p95 residence
 * strictly below B's (the queue-depth lead compounds; equal plateau capacity preserves it).</li>
 * </ul>
 *
 * <h2>Phase 4 (rate limiting) - SKIPPED-BLOCKED, deliberately absent rather than disabled</h2>
 * The sharpest phase - a downstream that stops slowing and starts REJECTING above a token-bucket rate, where
 * latency stays flat while useful throughput collapses, invisible to any latency-steered controller - has no
 * test method here at all. It is blocked on the 004 pressure-signal plan
 * ({@code docs/plans/2026-08-24-004-feat-downstream-pressure-signal-plan.md}): today
 * {@code AdmissionOutcomeClassifier.classifyFailure} scores every failure IGNORE, so {@code OVERLOAD_DROP} is
 * unreachable and a phase-4 test would pass for the wrong reason. Per the dark-test rules ({@code
 * docs/test-hardening/}), a phase that cannot yet assert honestly is named here and absent below - never
 * {@code @Disabled}.
 *
 * <h2>Run it</h2>
 * <pre>{@code
 * ./mvnw --batch-mode -pl parallel-consumer-core -am verify \
 *     -DskipUTs=true -Dit.test=AdaptiveConcurrencyComparisonIT \
 *     -Dfailsafe.failIfNoSpecifiedTests=false
 * }</pre>
 * The per-phase, per-arm results table is logged at the end of the run - read it, not just the tick.
 *
 * <h2>Local run record, 2026-08-25 (local machine) - GREEN, all strict phases</h2>
 * Residence in ms (p50/p95 by creation phase), completions by wall phase:
 * <pre>
 *   arm                     ph1              ph2                ph3                ph5
 *   A static-4   (rec-only) 7714/14748 2318  34354/51046  5537  71579/88322  5545  -/- 4159 (truncated drain)
 *   B static-8              45/48      3744  35245/39938  2949  30538/39351 11113  53732/65147  4328
 *   C adaptive              901/1671   3743  27832/34348  3947  14624/23612 12863  27731/39848  4078
 * </pre>
 * Every strict assertion held, most by wide margins: phase 2 p95 C 34,348 vs B 39,938 (C also completed 34%
 * more during the degrade window); phase 3 completions C 12,863 vs B 11,113 (and p95 23,612 vs 39,351);
 * phase 5 final-third median target 5.0 inside [2, 6] with p95 39,848 vs 65,147. Both asserted arms
 * completed their full 28,500. Arm C's target trajectory did what the law's falsifiers pin: knee 8 reached
 * at t=10s; the phase-2 collapse walked 15 -&gt; 3 in ~11s at one FALL cut per adjudicated window (the U12
 * fast-down lane - the previous run's 57s cadence-paying walk was this IT's phase-2 finding); phase 3's
 * tripled capacity was found by the U13 recovery re-ask probe (the previous run's absorbing 3-5-slot park
 * was this IT's phase-3 finding) and the re-opened RISE ladder hovered 6-15 around the knee of 12; phase 5
 * walked back down and straddled the knee of 4.
 *
 * <h2>History: the first run's freeze (2026-08-25, earlier the same day) - the finding that drove the fixes</h2>
 * The first run of this IT against the band-machine law was <b>RED - and the red was the finding, not a
 * harness defect.</b> Its table, kept for the record:
 * <pre>
 *   arm                     ph1              ph2                ph3                ph5
 *   A static-4   (rec-only) 8962/17227 2179  40383/61008  5181  81518/98489  5128  -/- 3862 (truncated drain)
 *   B static-8              48/52      3742  37266/40484  2895  35412/49898 10297  65503/77915  4190
 *   C adaptive              5973/14528 2579  41635/52999  4405  72676/89050  6395  104123/116375 4100
 * </pre>
 * Feeder verdicts: all three arms null (achieved 150.0/s exactly). The PLANT behaved to arithmetic: B collapsed
 * to ~48/s in phase 2 (predicted 50/s) and recovered to ~172/s in phase 3; A ran ~86/s (predicted 100/s).
 * <p>
 * <b>Arm C's controller froze at 5 slots at t=2s and never moved again for the remaining 188s</b> (trajectory
 * {@code t=0s:2, t=0s:3, t=2s:5}; phase-5 min=max=5; two movements in the whole run, zero probes). 5 slots is
 * below the 6 the arrival needs ({@code 150/s * 40ms}), so C ran the entire experiment as a saturated
 * static-5 arm - which is why phases 2/3/5 all read C worse than B, and why the phase-5 target band "passed"
 * (5 happens to sit inside [2,6]). The freeze mechanism, from the controller's own held-lines (45x
 * {@code SELF_THROTTLED}, 16x {@code WARMUP_EXHAUSTED}, nothing else):
 * <ol>
 * <li>seed 2 + the whole 4-slot warmup allowance = 6.0 fractional, published (int) as 5 - below the arrival's
 * requirement, so C is permanently limit-bound;</li>
 * <li>the estimator's FIRST verdict needs 8 entries within its 12s horizon with in-flight spread >= 1, but
 * under a deep backlog most window boundaries read {@code SELF_THROTTLED} (poller self-paused + a momentarily
 * empty slot), so bound windows arrive too sparsely for the climb levels (2, 3, 5) to coexist in the horizon -
 * and every later bound window is at level 5, spread 0: the verdict is structurally unreachable;</li>
 * <li>with no verdict, {@code WARMUP_EXHAUSTED} is an absorbing hold: the warmup episode only resets on an
 * acted verdict, the first warmup grant leaves {@code pendingGrowthBaseline=2 < limit} forever which suppresses
 * the descent probe's {@code blindExhausted} arming, the plateau arming needs a live HOLD verdict, and the
 * escape probe only fires at the floor. No path out exists above the floor.</li>
 * </ol>
 * The strict assertions were left strict: this IT is the end-to-end falsifier, and a controller that freezes
 * below the arrival rate is exactly what it exists to catch (the deterministic falsifier suite missed it
 * because its plant emitted densely limit-bound windows, so the climb's spread always reached the estimator).
 * The law-side fixes landed as the p90 active-task binding evidence plus the stagnation probe (this freeze),
 * then the U12 fast-FALL lane (a later run's 57s phase-2 contraction walk) and the U13 recovery re-ask probe
 * (that run's absorbing phase-3 park) - each red first reproduced deterministically in
 * {@code FalsifierScenarios} (the saturated-flicker, capacity-collapse and capacity-recovery scenarios),
 * which is where the mechanism write-ups now live. The green record above supersedes this one.
 *
 * @see AdaptiveConcurrencyClosedLoopIT the closed-loop single-instance sibling (no static opponent)
 * @see AdaptiveConcurrencyEnforceIT the liveness sibling (open loop, rebalance)
 */
// TODO(refactor): U10 - add phase 4 (token-bucket rate limiting) once the 004 pressure-signal plan ships a real
//  AdmissionOutcomeClassifier; see the "Phase 4" javadoc section above.
@Timeout(1200)
@Testcontainers
@Slf4j
class AdaptiveConcurrencyComparisonIT extends BrokerIntegrationTest<String, String> {

    /** W0: the downstream's uncongested service time, every phase. */
    private static final long BASE_SERVICE_TIME_MS = 40;

    /** Phase 1 / phase 3-reference knee: the healthy plant's L* in slots, and arm B's hand-tuned value. */
    private static final int HEALTHY_KNEE_SLOTS = 8;

    /** Phase 2 and phase 5 knee: capacity halved from phase 1 (the R15 degrade step). */
    private static final int DEGRADED_KNEE_SLOTS = 4;

    /** Phase 3 knee: capacity 1.5x phase 1 (the R15 recover-beyond step). */
    private static final int RECOVERED_KNEE_SLOTS = 12;

    /** Phase-1 capacity, records/second: {@code HEALTHY_KNEE_SLOTS / W0}. */
    private static final int PHASE1_CAPACITY_PER_SECOND = 200;

    /**
     * Arrival as a fraction of phase-1 capacity. 0.75 rather than the packet's ~0.65: phase 3's strict
     * completion inequality needs C's phase-2 backlog plus phase-3 arrivals to exceed B's work-rich completion
     * capacity of {@code 200/s * 60s}; at 0.65 the predicted margin is ~400 records (inside broker noise), at
     * 0.75 it is ~1,500. Phase-1 steady in-flight stays at {@code 150/s * 40ms = 6} of 8 slots - below
     * saturation, as R15 requires. Documented deviation; see the class javadoc.
     */
    private static final double ARRIVAL_FRACTION = 0.75;

    /** The one arrival rate every phase of every arm holds: {@code ARRIVAL_FRACTION * PHASE1_CAPACITY}. */
    private static final double ARRIVAL_RATE_PER_SECOND = ARRIVAL_FRACTION * PHASE1_CAPACITY_PER_SECOND;

    /**
     * Phase lengths. Phase 1 is short (recorded only; C's ramp to serving the full arrival takes ~15s). Phases
     * 2 and 3 are sized to the law's settle cadence - a band movement lands at most every ~9 windows of ~1s, so
     * a contraction from 8 toward 4, or a climb from 4 to 12, costs 40-60s. Phase 5 is sized to the descent
     * probe's walk (3 plateau windows arm it, 4 windows measure, per step down) plus a settled final third.
     */
    private static final int PHASE1_SECONDS = 25;
    private static final int PHASE2_SECONDS = 60;
    private static final int PHASE3_SECONDS = 60;
    private static final int PHASE5_SECONDS = 45;

    private static final int SCHEDULE_SECONDS = PHASE1_SECONDS + PHASE2_SECONDS + PHASE3_SECONDS + PHASE5_SECONDS;

    /** Phase labels as the plan numbers them - slot 4 is the drain. Phase 4 is skipped-blocked (class javadoc). */
    private static final int[] PHASE_LABELS = {1, 2, 3, 5};

    private static final int ARM_A_CONCURRENCY = 4;
    private static final int ARM_C_CEILING = 32;
    private static final int ARM_C_SEED = 2;

    /**
     * Phase 5's knee band, as a fraction each side of the knee (the R15 "settles near the knee" bound): the
     * final-third median target must land in {@code [knee * 0.5, knee * 1.5]} = [2, 6] slots. The descent probe
     * walks a flat plateau down one accelerator step per cycle, so from an entry near 12 the walk to 4 costs
     * ~3 cycles of ~7-8 windows - inside the phase with a settled third to spare.
     */
    private static final double PHASE5_KNEE_BAND_FRACTION = 0.5;

    /**
     * Arm A alone gets a bounded drain: its capacity is 100/s under a 150/s arrival from phase 1 onward, so a
     * full drain would cost ~90s+ for an arm nothing asserts on. Its rows are labelled truncated.
     */
    private static final int ARM_A_DRAIN_CAP_SECONDS = 45;

    /** How long B and C get to drain completely - roughly twice the predicted worst backlog over drain capacity. */
    private static final int DRAIN_TIMEOUT_SECONDS = 240;

    /** Per-phase curves - see the class javadoc's table for the derivation of each. */
    private static final SyntheticCongestionCurve PHASE1_CURVE =
            SyntheticCongestionCurve.quadratic(BASE_SERVICE_TIME_MS, HEALTHY_KNEE_SLOTS);
    private static final SyntheticCongestionCurve PHASE2_CURVE =
            SyntheticCongestionCurve.quadratic(BASE_SERVICE_TIME_MS, DEGRADED_KNEE_SLOTS);
    private static final SyntheticCongestionCurve PHASE3_CURVE =
            SyntheticCongestionCurve.quadratic(BASE_SERVICE_TIME_MS, RECOVERED_KNEE_SLOTS);
    private static final SyntheticCongestionCurve PHASE5_CURVE =
            SyntheticCongestionCurve.linear(BASE_SERVICE_TIME_MS, DEGRADED_KNEE_SLOTS);
    /** Drain: linear at the degraded knee - every arm's capacity is the identical 100/s (class javadoc). */
    private static final SyntheticCongestionCurve DRAIN_CURVE = PHASE5_CURVE;

    @BeforeEach
    void setUp() {
        numPartitions = 4;
    }

    @Test
    void adaptiveBeatsTheHandTunedStaticOnceTheLoadMoves() throws Exception {
        ArmResult armA = runArm("armA-static-low", false, ARM_A_CONCURRENCY);
        ArmResult armB = runArm("armB-static-hand-tuned", false, HEALTHY_KNEE_SLOTS);
        ArmResult armC = runArm("armC-adaptive", true, ARM_C_CEILING);

        log.info("Comparison results, per phase per arm (residence in ms; phase 1 recorded, not asserted):\n{}",
                renderResults(armA, armB, armC));

        // ---- the arrival schedule must have been delivered for every arm, or the numbers measure the producer
        for (ArmResult arm : new ArmResult[]{armA, armB, armC}) {
            assertWithMessage("arm %s: the feeder must have held its schedule - a voided arrival run answers a "
                    + "question about the producer, not the consumer", arm.name)
                    .that(arm.feederVerdict).isNull();
        }

        // ---- phase 2: degradation. B over-drives the halved knee and its completions collapse to ~50/s; C
        // backs off toward the new L* of 4 and keeps ~2x B's completion rate, so C's queue - and every
        // record's residence - stays materially shorter. Strict, no tolerance: the predicted gap is seconds.
        assertWithMessage("phase 2 (degrade): adaptive p95 residence must be strictly below hand-tuned static's "
                + "- B over-drives the halved knee (8 in-flight on a knee of 4 = 4x service time), C backs off")
                .that(armC.residenceP95Ms[1]).isLessThan(armB.residenceP95Ms[1]);

        // ---- phase 3: recovery beyond phase 1. B is capped at its hand-tuned 8 slots = 200/s; C grows into
        // the fresh headroom. Both arms are work-rich on their phase-2 backlogs, so completions measure
        // capacity, not arrival. Strict, no tolerance: the predicted gap is ~1,500 records (~13%).
        assertWithMessage("phase 3 (recover beyond): adaptive useful completions must be strictly above "
                + "hand-tuned static's - B strands the new headroom its compiled setting cannot reach")
                .that(armC.completionsByWallPhase[2]).isGreaterThan(armB.completionsByWallPhase[2]);

        // ---- phase 5: graceful saturation. The plateau is flat, so FALL can never fire - holding near the
        // knee is the HOLD band plus the descent probe, which neither phase 2 nor 4 exercises.
        double kneeBandLow = DEGRADED_KNEE_SLOTS * (1 - PHASE5_KNEE_BAND_FRACTION);
        double kneeBandHigh = DEGRADED_KNEE_SLOTS * (1 + PHASE5_KNEE_BAND_FRACTION);
        double phase5FinalThirdMedian = armC.phase5FinalThirdMedianTarget;
        assertWithMessage("phase 5 (plateau): the adaptive target's final-third median must settle near the "
                + "knee of %s - at least %s - the descent probe walking the flat plateau down; the full "
                + "trajectory is %s", DEGRADED_KNEE_SLOTS, kneeBandLow, armC.targetTrajectory)
                .that(phase5FinalThirdMedian).isAtLeast(kneeBandLow);
        assertWithMessage("phase 5 (plateau): the adaptive target's final-third median must settle near the "
                + "knee of %s - at most %s - a median above the band means the plateau never pulled the target "
                + "down; the full trajectory is %s", DEGRADED_KNEE_SLOTS, kneeBandHigh, armC.targetTrajectory)
                .that(phase5FinalThirdMedian).isAtMost(kneeBandHigh);
        assertWithMessage("phase 5 (plateau): adaptive p95 residence must be strictly below hand-tuned "
                + "static's - the queue-depth lead from phases 2-3 compounds, and the equal-capacity plateau "
                + "preserves it")
                .that(armC.residenceP95Ms[3]).isLessThan(armB.residenceP95Ms[3]);

        // ---- correctness ledger for the asserted arms: everything fed was completed (arm A is truncated by
        // design and recorded only).
        assertWithMessage("arm B: every record fed on the schedule must have completed after the drain")
                .that((long) armB.completedDistinct).isAtLeast(armB.fedRecords);
        assertWithMessage("arm C: every record fed on the schedule must have completed after the drain")
                .that((long) armC.completedDistinct).isAtLeast(armC.fedRecords);
    }

    // ------------------------------------------------------------------
    // The arm runner
    // ------------------------------------------------------------------

    private ArmResult runArm(String armName, boolean adaptive, int maxConcurrency) throws Exception {
        log.info("=== Arm {} starting: {} maxConcurrency={} ===", armName,
                adaptive ? "ADAPTIVE" : "STATIC", maxConcurrency);
        String topic = setupTopic(armName);

        AtomicInteger inFlight = new AtomicInteger();
        Set<String> completedKeys = ConcurrentHashMap.newKeySet();
        List<List<Long>> residenceByCreationPhase = new ArrayList<>();
        for (int i = 0; i < PHASE_LABELS.length; i++) {
            residenceByCreationPhase.add(Collections.synchronizedList(new ArrayList<>()));
        }
        AtomicLong[] completionsByWallPhase = new AtomicLong[PHASE_LABELS.length + 1]; // + drain
        for (int i = 0; i < completionsByWallPhase.length; i++) {
            completionsByWallPhase[i] = new AtomicLong();
        }

        ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> builder =
                ParallelConsumerOptions.<String, String>builder()
                        .consumer(getKcu().createNewConsumer(GroupOption.NEW_GROUP))
                        .commitMode(PERIODIC_CONSUMER_ASYNCHRONOUS)
                        // UNORDERED: under key/partition ordering in-flight is capped by key count, the law
                        // correctly reports application-limited, and the experiment measures the shard model
                        .ordering(UNORDERED)
                        .maxConcurrency(maxConcurrency);
        if (adaptive) {
            builder.adaptiveConcurrencyMode(AdaptiveConcurrencyMode.ENFORCE)
                    .adaptiveConcurrencyInitialTarget(ARM_C_SEED);
        }
        ParallelConsumerOptions<String, String> options = builder.build();
        PCModule<String, String> module = new PCModule<>(options);
        ParallelEoSStreamProcessor<String, String> pc = new ParallelEoSStreamProcessor<>(options, module);
        pc.subscribe(UniSets.of(topic));

        // Take the controller reference BEFORE poll() starts any engine thread - the same determinism guard the
        // sibling ITs carry (a racing first touch once constructed TWO controllers).
        AdmissionController controller = adaptive ? module.admissionController() : null;

        Properties feederProps = new Properties();
        // linger 0: the schedule is the experiment - nothing may sit in an accumulator waiting for a batch
        feederProps.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        KafkaProducer<String, String> producer =
                getKcu().createNewProducer(ProducerMode.NOT_TRANSACTIONAL, feederProps);
        ConstantRateFeeder feeder = new ConstantRateFeeder(producer, topic, ARRIVAL_RATE_PER_SECOND,
                SECONDS.toMillis(SCHEDULE_SECONDS), () -> completedKeys.size(), armName + "-");

        pc.poll(context -> {
            int concurrent = inFlight.incrementAndGet();
            try {
                long t0 = feeder.getScheduleStartMillis();
                long now = System.currentTimeMillis();
                // before t0 (the warmup record) the plant serves phase 1's healthy curve and records nothing
                SyntheticCongestionCurve curve = t0 == 0 ? PHASE1_CURVE : curveAt(now - t0);
                sleepQuietly(curve.serviceTimeMillis(concurrent));
                long completedAt = System.currentTimeMillis();
                if (t0 > 0) {
                    long createdAt = context.getSingleConsumerRecord().timestamp();
                    int creationSlot = phaseSlotAt(createdAt - t0);
                    if (creationSlot >= 0 && creationSlot < PHASE_LABELS.length) {
                        residenceByCreationPhase.get(creationSlot).add(completedAt - createdAt);
                    }
                    int wallSlot = phaseSlotAt(completedAt - t0);
                    if (wallSlot >= 0) {
                        completionsByWallPhase[Math.min(wallSlot, PHASE_LABELS.length)].incrementAndGet();
                    }
                }
                completedKeys.add(context.key());
            } finally {
                inFlight.decrementAndGet();
            }
        });

        if (controller != null) {
            assertWithMessage("arm %s must actually be in ENFORCE, or nothing here tests the feature", armName)
                    .that(controller.mode()).isEqualTo(AdaptiveConcurrencyMode.ENFORCE);
        }

        List<TargetSample> targetSamples = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch stopSampling = new CountDownLatch(1);
        if (controller != null) {
            startTargetSampler(controller, feeder, targetSamples, stopSampling);
        }

        try {
            feeder.start();
            assertWithMessage("arm %s: the consumer must complete the warmup record so the schedule can start",
                    armName)
                    .that(feeder.awaitScheduleStart(ConstantRateFeeder.WARMUP_TIMEOUT_MS, TimeUnit.MILLISECONDS))
                    .isTrue();
            log.info("Arm {}: schedule started at rate {}/s for {}s", armName, ARRIVAL_RATE_PER_SECOND,
                    SCHEDULE_SECONDS);
            assertWithMessage("arm %s: the feeder must finish its schedule", armName)
                    .that(feeder.awaitFinished(SCHEDULE_SECONDS + 60, SECONDS)).isTrue();
            long fed = feeder.getFedRecords();
            log.info("Arm {}: schedule complete - fed {} records at achieved {}/s; draining ({} completed so "
                            + "far)", armName, fed, String.format(Locale.ROOT, "%.1f",
                            feeder.getAchievedRatePerSecond()), completedKeys.size());

            // +1: the warmup record completed too, and lives in the same distinct-key ledger
            long expectedCompletions = fed + 1;
            if (adaptive || maxConcurrency == HEALTHY_KNEE_SLOTS) {
                // B and C drain fully: residence percentiles need every record's completion (class javadoc)
                await().alias("arm " + armName + " drains completely")
                        .atMost(DRAIN_TIMEOUT_SECONDS, SECONDS)
                        .failFast(pc::isClosedOrFailed)
                        .untilAsserted(() -> assertWithMessage("all fed records complete")
                                .that((long) completedKeys.size()).isAtLeast(expectedCompletions));
            } else {
                // arm A drowns by design - bounded drain, recorded-only rows labelled truncated
                long deadline = System.currentTimeMillis() + SECONDS.toMillis(ARM_A_DRAIN_CAP_SECONDS);
                while (completedKeys.size() < expectedCompletions && System.currentTimeMillis() < deadline
                        && !pc.isClosedOrFailed()) {
                    sleepQuietly(500);
                }
            }
            assertWithMessage("arm %s: the engine must not have died during the run", armName)
                    .that(pc.isClosedOrFailed()).isFalse();

            // -1: the warmup record is not part of the fed schedule, so it leaves the completion ledger too
            return buildResult(armName, feeder, completedKeys.size() - 1, residenceByCreationPhase,
                    completionsByWallPhase, targetSamples);
        } finally {
            stopSampling.countDown();
            pc.close();
            producer.close();
            log.info("=== Arm {} finished ===", armName);
        }
    }

    // ------------------------------------------------------------------
    // Phase geometry
    // ------------------------------------------------------------------

    /** The curve in force at the given offset from the schedule's t0 (drain curve once the schedule is over). */
    private static SyntheticCongestionCurve curveAt(long offsetMillis) {
        int slot = phaseSlotAt(offsetMillis);
        switch (slot) {
            case 0:
                return PHASE1_CURVE;
            case 1:
                return PHASE2_CURVE;
            case 2:
                return PHASE3_CURVE;
            case 3:
                return PHASE5_CURVE;
            default:
                return DRAIN_CURVE;
        }
    }

    /**
     * Phase slot (0..3 for phases 1/2/3/5, 4 for the drain) at an offset from t0; -1 before the schedule
     * (the warmup record).
     */
    private static int phaseSlotAt(long offsetMillis) {
        if (offsetMillis < 0) {
            return -1;
        }
        long seconds = offsetMillis / 1000;
        if (seconds < PHASE1_SECONDS) {
            return 0;
        }
        if (seconds < PHASE1_SECONDS + PHASE2_SECONDS) {
            return 1;
        }
        if (seconds < PHASE1_SECONDS + PHASE2_SECONDS + PHASE3_SECONDS) {
            return 2;
        }
        if (seconds < SCHEDULE_SECONDS) {
            return 3;
        }
        return 4;
    }

    // ------------------------------------------------------------------
    // Measurement plumbing
    // ------------------------------------------------------------------

    /** One sample of arm C's live target - taken every 250ms, so medians over a window are time-weighted. */
    private static final class TargetSample {
        final long offsetMillis;
        final int target;

        TargetSample(long offsetMillis, int target) {
            this.offsetMillis = offsetMillis;
            this.target = target;
        }
    }

    /**
     * Samples the controller's public reported target on a fixed cadence - the state an operator's dashboard
     * reads. Offsets are against the feeder's t0 so samples align with the phase clock; samples before t0 are
     * recorded at offset 0 (the ramp during the group join belongs to phase 1's exploration cost).
     */
    private void startTargetSampler(AdmissionController controller, ConstantRateFeeder feeder,
                                    List<TargetSample> samples, CountDownLatch stop) {
        Thread sampler = new Thread(() -> {
            while (true) {
                long t0 = feeder.getScheduleStartMillis();
                long offset = t0 == 0 ? 0 : System.currentTimeMillis() - t0;
                samples.add(new TargetSample(offset, controller.currentTarget()));
                try {
                    if (stop.await(250, TimeUnit.MILLISECONDS)) {
                        return;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }, "admission-target-sampler");
        sampler.setDaemon(true);
        sampler.start();
    }

    private ArmResult buildResult(String name, ConstantRateFeeder feeder, int completedDistinct,
                                  List<List<Long>> residenceByCreationPhase, AtomicLong[] completionsByWallPhase,
                                  List<TargetSample> targetSamples) {
        ArmResult result = new ArmResult();
        result.name = name;
        result.fedRecords = feeder.getFedRecords();
        result.completedDistinct = completedDistinct;
        result.achievedRate = feeder.getAchievedRatePerSecond();
        result.feederVerdict = feeder.verdict();
        for (int slot = 0; slot < PHASE_LABELS.length; slot++) {
            List<Long> residences;
            List<Long> raw = residenceByCreationPhase.get(slot);
            synchronized (raw) {
                residences = new ArrayList<>(raw);
            }
            Collections.sort(residences);
            result.residenceCount[slot] = residences.size();
            result.residenceP50Ms[slot] = percentile(residences, 0.50);
            result.residenceP95Ms[slot] = percentile(residences, 0.95);
            result.completionsByWallPhase[slot] = completionsByWallPhase[slot].get();
        }
        result.drainCompletions = completionsByWallPhase[PHASE_LABELS.length].get();
        List<TargetSample> samples;
        synchronized (targetSamples) {
            samples = new ArrayList<>(targetSamples);
        }
        if (!samples.isEmpty()) {
            result.targetTrajectory = renderTrajectory(samples);
            long phase5Start = SECONDS.toMillis(SCHEDULE_SECONDS - PHASE5_SECONDS);
            long finalThirdStart = SECONDS.toMillis(SCHEDULE_SECONDS) - SECONDS.toMillis(PHASE5_SECONDS) / 3;
            List<Long> finalThird = new ArrayList<>();
            List<Long> wholePhase5 = new ArrayList<>();
            for (TargetSample sample : samples) {
                if (sample.offsetMillis >= phase5Start && sample.offsetMillis < SECONDS.toMillis(SCHEDULE_SECONDS)) {
                    wholePhase5.add((long) sample.target);
                    if (sample.offsetMillis >= finalThirdStart) {
                        finalThird.add((long) sample.target);
                    }
                }
            }
            Collections.sort(finalThird);
            result.phase5FinalThirdMedianTarget = percentile(finalThird, 0.50);
            Collections.sort(wholePhase5);
            result.phase5TargetMin = wholePhase5.isEmpty() ? -1 : wholePhase5.get(0);
            result.phase5TargetMax = wholePhase5.isEmpty() ? -1 : wholePhase5.get(wholePhase5.size() - 1);
        }
        return result;
    }

    /** Nearest-rank percentile of an ALREADY SORTED list; -1 on an empty one (rendered as "-"). */
    private static long percentile(List<Long> sorted, double quantile) {
        if (sorted.isEmpty()) {
            return -1;
        }
        int index = (int) Math.min(sorted.size() - 1, Math.floor(sorted.size() * quantile));
        return sorted.get(index);
    }

    /** The target's distinct movements as {@code t=..s:v} pairs - compact enough for an assertion message. */
    private static String renderTrajectory(List<TargetSample> samples) {
        StringBuilder rendered = new StringBuilder();
        int last = Integer.MIN_VALUE;
        for (TargetSample sample : samples) {
            if (sample.target != last) {
                if (rendered.length() > 0) {
                    rendered.append("  ");
                }
                rendered.append(String.format(Locale.ROOT, "t=%ds:%d", sample.offsetMillis / 1000, sample.target));
                last = sample.target;
            }
        }
        return rendered.toString();
    }

    private static String renderResults(ArmResult... arms) {
        StringBuilder table = new StringBuilder();
        table.append(String.format(Locale.ROOT, "%-26s %-7s %12s %12s %12s %14s%n",
                "arm", "phase", "p50 res", "p95 res", "completions", "created-count"));
        for (ArmResult arm : arms) {
            for (int slot = 0; slot < PHASE_LABELS.length; slot++) {
                table.append(String.format(Locale.ROOT, "%-26s %-7d %12s %12s %12d %14d%n",
                        arm.name, PHASE_LABELS[slot],
                        renderMs(arm.residenceP50Ms[slot]), renderMs(arm.residenceP95Ms[slot]),
                        arm.completionsByWallPhase[slot], arm.residenceCount[slot]));
            }
            table.append(String.format(Locale.ROOT,
                    "%-26s fed=%d completed=%d (%s) achievedRate=%.1f/s drainCompletions=%d%n",
                    arm.name, arm.fedRecords, arm.completedDistinct,
                    arm.completedDistinct >= arm.fedRecords ? "complete" : "TRUNCATED",
                    arm.achievedRate, arm.drainCompletions));
            if (arm.targetTrajectory != null) {
                table.append(String.format(Locale.ROOT,
                        "%-26s phase-5 target min=%d max=%d finalThirdMedian=%.1f; trajectory: %s%n",
                        arm.name, arm.phase5TargetMin, arm.phase5TargetMax, arm.phase5FinalThirdMedianTarget,
                        arm.targetTrajectory));
            }
        }
        return table.toString();
    }

    private static String renderMs(long value) {
        return value < 0 ? "-" : value + "ms";
    }

    /** Everything one arm's run produced - the per-phase numbers the assertions and the results table read. */
    private static final class ArmResult {
        String name;
        long fedRecords;
        int completedDistinct;
        double achievedRate;
        String feederVerdict;
        final long[] residenceP50Ms = new long[PHASE_LABELS.length];
        final long[] residenceP95Ms = new long[PHASE_LABELS.length];
        final int[] residenceCount = new int[PHASE_LABELS.length];
        final long[] completionsByWallPhase = new long[PHASE_LABELS.length];
        long drainCompletions;
        String targetTrajectory;
        double phase5FinalThirdMedianTarget = -1;
        long phase5TargetMin = -1;
        long phase5TargetMax = -1;
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
