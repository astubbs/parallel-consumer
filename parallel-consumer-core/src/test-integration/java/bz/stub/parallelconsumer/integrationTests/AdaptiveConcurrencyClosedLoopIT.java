package bz.stub.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.AdaptiveConcurrencyMode;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption;
import bz.stub.parallelconsumer.integrationTests.utils.SyntheticCongestionCurve;
import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.internal.admission.AdmissionController;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Testcontainers;
import pl.tlinkowski.unij.api.UniSets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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
 * <b>Adaptive concurrency in a CLOSED loop: the handler's latency is a function of the concurrency the controller
 * chose.</b> Its sibling {@link AdaptiveConcurrencyEnforceIT} runs an <em>open</em> loop - a fixed-latency handler
 * that never answers back, so the controller's own actions never return to it as measurement and the only claim
 * available is liveness. Here the downstream pushes back: every extra slot the controller admits makes every
 * invocation slower, so the target the law picks is the input to the throughput it then measures. The whole arc
 * the feature exists for happens inside one run - ramp up, cross the elbow, throughput sags as latency degrades,
 * the law contracts, concurrency falls back to the elbow, throughput recovers - and where it lands is a property
 * of the control law, not of the test.
 *
 * <h2>Run it, and watch the target ramp, break, and hunt</h2>
 * <pre>{@code
 * JAVA_HOME=$HOME/.sdkman/candidates/java/17.0.18-tem ./mvnw --batch-mode \
 *     -pl parallel-consumer-core -am verify \
 *     -DskipUTs=true -Dit.test=AdaptiveConcurrencyClosedLoopIT \
 *     -Dfailsafe.failIfNoSpecifiedTests=false
 * }</pre>
 * That last flag is not optional: {@code -am} builds the parent module too, failsafe runs there as well, and it
 * fails the build for matching no tests - which looks exactly like this test failing.
 * {@code logback-test.xml} already leaves the controller's own logger ON at {@code INFO} while the rest of the
 * library sits at {@code WARN}, so the PC output of this run IS the target's trajectory. The same one line makes it
 * watchable in <b>your</b> application, where the library defaults to silent:
 * <pre>{@code
 * <logger name="bz.stub.parallelconsumer.internal.admission.AdmissionController" level="info"/>
 * }</pre>
 * Or filter an existing log on the prefix every one of those lines opens with: {@code grep 'Adaptive concurrency'}.
 * This test additionally prints its OWN trace - one line per distinct target value, carrying the deciding reason
 * plus the live in-flight count and the latency the model was serving at that instant - and dumps the whole
 * trajectory at the end. That trace is the point of the test; read it, do not just read the green tick.
 *
 * <h2>The synthetic downstream: flat, then an elbow</h2>
 * Latency is driven by the concurrency the handler ITSELF observes - an {@link AtomicInteger} incremented on entry
 * and decremented in a {@code finally} - never by {@link AdmissionController#currentTarget()}. That is the whole
 * point: a real downstream does not know what the controller decided, it only feels how many callers arrived.
 * <p>
 * The curve is the shared {@link SyntheticCongestionCurve#quadratic quadratic} congestion-collapse shape, flat
 * below a knee and rising as the square above it:
 * <pre>
 *     latency = BASE * max(1, inflight / KNEE)^2
 * </pre>
 * With {@value #BASE_LATENCY_MS}ms and a knee of {@value #KNEE_INFLIGHT}, that is:
 * <pre>
 *     in-flight  :   6     12     14     15     16     18     20     24     32
 *     latency    :  80ms   80ms  109ms  125ms  142ms  180ms  222ms  320ms  569ms
 *     throughput : 75/s  150/s  128/s  120/s  113/s  100/s   90/s   75/s   56/s
 * </pre>
 * Quadratic rather than linear because the law is throughput-steered: above a quadratic knee, total throughput
 * {@code inflight / latency} FALLS as {@code knee^2 / (BASE * inflight)}, which is what the FALL band's negative
 * elasticity reads. A linear curve's throughput plateaus exactly flat above the knee - the correct response there
 * is HOLD plus the descent probe, which is a different experiment (the falsifier suite's plateau scenario, and
 * {@link AdaptiveConcurrencyComparisonIT}'s phase 5).
 *
 * <h2>The arithmetic the model was designed against (the 2026-08-24-003 band machine)</h2>
 * <b>What actually makes the law contract.</b> The elasticity estimator regresses log useful-throughput on log
 * in-flight over its short horizon. Below the knee the slope is +1 (throughput is {@code inflight / BASE}); above
 * it the slope is -1 (throughput is {@code KNEE^2 / (BASE * inflight)}). The RISE band needs slope above 0.25,
 * FALL needs it below zero - so crossing the elbow flips the verdict from RISE through HOLD to FALL with margin,
 * no latency reference anywhere. Growth is also <em>provisional</em>: each RISE step remembers its pre-growth
 * baseline, and the first post-settle verdict that says the step did not pay RETRACTS it - so the law converges
 * to the last level that paid, which this plant makes the knee itself.
 * <p>
 * <b>How long the ramp takes.</b> The warmup band grants {@code sqrt(limit)} per window up to its 4-slot
 * episode allowance ({@value #INITIAL_TARGET} to ~6 in about three windows), then each further step waits out
 * the settle cadence of 8 offered one-second windows: 6 -&gt; 8.4 -&gt; 11.4 -&gt; 14.8, crossing the knee of
 * {@value #KNEE_INFLIGHT} thirty-to-forty seconds in. The first contraction is the verdict on that overshoot
 * step - at ~15 in-flight the plant serves 125ms and ~120/s against ~142/s at 11.4, an unambiguous "did not
 * pay" - so it lands within one further settle period. {@link #SETTLE_OBSERVATION_SECONDS} of watching is then
 * bought outright, so the run shows what the law does after the first break.
 * <p>
 * <b>Records needed.</b> Throughput is {@code concurrency / latency}, bounded by the knee capacity of ~150/s -
 * call it 15,000 records for a two-minute run. A workload that runs dry looks EXACTLY like a controller that
 * stopped adapting (the window goes app-limited and the law correctly holds), so rather than guessing a total up
 * front, {@link #startBacklogFeeder} tops the topic up whenever the un-processed backlog falls below
 * {@value #BACKLOG_LOW_WATER}. The backlog is therefore never smaller than three times the
 * {@value #MAX_CONCURRENCY}-slot ceiling, and never so large that the final drain dominates the run.
 * <p>
 * <b>{@code UNORDERED}, deliberately.</b> Under key or partition ordering, in-flight concurrency is capped by the
 * key/partition count rather than by admission, the law correctly reports an application-limited workload, and the
 * experiment measures the shard model instead of the controller.
 *
 * <h2>What a run shows now - and the walk this test used to document, which is fixed</h2>
 * Under the previous law (the Gradient2 port this repo shipped first), this test's headline finding was a
 * RATCHET: the hunting band did not stay put - {@code 17..18}, then {@code 18..19}, then a step to 20 - because
 * the long-term latency baseline absorbed the very degradation it was the reference for; simulated for 400
 * windows the target walked 17 -&gt; 27 with no fixed point below the ceiling. That finding (recorded in
 * {@code docs/inflight/pr-333-adaptive-concurrency-outstanding.md} and this file's history) is what forced the
 * law's replacement by the 2026-08-24-003 band machine, whose HOLD band never converts a plateau into growth and
 * whose learned-latency state no longer exists to drift.
 * <p>
 * So <b>a settled band IS asserted here now</b> - the modest, run-length-independent form: over the final third
 * of the observation window the target's range must be no wider than two accelerator steps ({@code sqrt} of the
 * median - one provisional RISE step above the paid level, one descent-probe dip below it, both transient by
 * design), and its time-weighted median must sit within one accelerator step of the plant's actual knee. The old
 * law fails both at any run length (its band's median walks); a law that parks at the cap or the floor fails the
 * median bound outright. Expected excursions inside the band: a RISE overshoot to ~15 retracted by the next
 * verdict, and rare 4-window descent probes to ~9 restored when the lower level does not pay.
 *
 * <h2>Local run record, 2026-08-25 - GREEN (113.2s)</h2>
 * Green on the law as shipped - the p90 active-task binding evidence, the stagnation probe, the U12
 * fast-FALL contraction lane and the U13 recovery re-ask probe all in place: the ramp crossed the elbow, the
 * first contraction landed, and the settled-band assertions (final-third range within two accelerator steps,
 * time-weighted median within one step of the knee) held with the recovery re-ask's bounded half-step
 * excursions inside the band. Nothing was weakened.
 *
 * <h2>History: the first band-machine run (2026-08-25, earlier) - RED, deliberately left red at the time</h2>
 * First run against the band-machine law: the target moved {@code 2 -> 3 -> 5} on warmup grants in the first
 * ten seconds and then froze at 5 for the rest of the run; the elbow await (knee 12) timed out at 120s. The
 * window boundary classified most windows {@code SELF_THROTTLED} (22 of 27 held-lines), so the estimator never
 * accumulated the 8-entry, spread-&gt;=1 history its FIRST verdict needs, the warmup allowance exhausted below
 * the knee, and no probe could arm above the floor. The same freeze, byte-identical trajectory included,
 * appeared on {@link AdaptiveConcurrencyEnforceIT}'s open-loop plant and on
 * {@link AdaptiveConcurrencyComparisonIT}'s arrival-controlled plant - <b>that class's history record owns
 * the full mechanism write-up</b>. The deterministic falsifier suite could not see it until its plant learned
 * broker-fidelity boundaries (the saturated-flicker scenario, which now reproduces this freeze exactly).
 * Nothing was weakened - the assertions above describe the law working, this red was the broker-scope
 * evidence that it did not yet, and the green record above is the evidence that it now does.
 *
 * @see AdaptiveConcurrencyEnforceIT
 * @see AdmissionController
 */
@Timeout(600)
@Testcontainers
@Slf4j
class AdaptiveConcurrencyClosedLoopIT extends BrokerIntegrationTest<String, String> {

    /**
     * Latency of the synthetic downstream when it is not congested - the flat part of the curve, and the value the
     * law's long-term baseline warms up on during the ramp. Large enough that a one-second window carries far more
     * than the law's ten-sample minimum even at the seed ({@code 2 / 80ms} = 25 samples per window), and large
     * enough that the whole experiment costs ~10,000 records rather than ~100,000.
     */
    private static final int BASE_LATENCY_MS = 80;

    /**
     * The elbow. Below this many concurrent invocations the synthetic downstream is unloaded and latency is flat;
     * above it, latency rises as the square of the overshoot. Chosen well clear of both ends of the range so there
     * is room to ramp INTO the elbow from the seed and room to fall BACK from it without hitting the floor.
     */
    private static final int KNEE_INFLIGHT = 12;

    /**
     * The ceiling, which is {@code maxConcurrency}. Nearly three times the knee, so a controller that ignores the
     * elbow entirely has somewhere visible to go - "it pinned at the cap" has to be a distinguishable outcome, or
     * the test cannot report it.
     */
    private static final int MAX_CONCURRENCY = 32;

    /** The seeded starting target, well below the knee so the ramp into the elbow is the first thing observed. */
    private static final int INITIAL_TARGET = 2;

    /**
     * How far the target must climb before the elbow can possibly have been crossed. It IS the knee: reaching it
     * means the controller drove the synthetic downstream to the point where its latency starts to bend, which is
     * the precondition for every claim after it.
     */
    private static final int ELBOW_WATERMARK = KNEE_INFLIGHT;

    /**
     * How long to keep watching after the first downward movement. Bought outright rather than derived from a
     * convergence condition: an await that waited for a band would pass on the first accident, whereas a fixed
     * window makes the settled-band assertion below read a period the law had no say over. Sized to several
     * settle cadences (8 offered windows each) plus at least one descent-probe cycle, so the final third is
     * genuinely post-transient.
     */
    private static final int SETTLE_OBSERVATION_SECONDS = 60;

    /** Seed backlog, enough to saturate the ramp before the feeder's first top-up. */
    private static final int SEED_RECORDS = 2_000;

    /** Below this many un-processed records the feeder tops the topic up - see the class javadoc's arithmetic. */
    private static final int BACKLOG_LOW_WATER = 1_000;

    /** How many records one top-up adds. */
    private static final int FEED_CHUNK = 1_000;

    /** How often the feeder re-checks the backlog. */
    private static final int FEED_POLL_MS = 250;

    /**
     * Width of the time slices the hunting band is reported over. Long enough to contain several windows of a
     * two-value hunt, short enough that a band which walks upward over the run shows up as different slices rather
     * than as one wide band.
     */
    private static final int BAND_SLICE_SECONDS = 20;

    /** Live concurrency AS THE DOWNSTREAM SEES IT - the model's only input. Never the controller's target. */
    private final AtomicInteger inFlight = new AtomicInteger();

    /** The most recent latency the model served, for the trace. */
    private final AtomicLong lastLatencyMillis = new AtomicLong();

    /** The worst latency the model ever served - the direct evidence that the elbow was actually crossed. */
    private final AtomicLong observedMaxLatencyMillis = new AtomicLong();

    /** The highest concurrency the downstream ever actually saw. */
    private final AtomicInteger observedMaxInFlight = new AtomicInteger();

    /**
     * Every DISTINCT value the live admission target took, in order, each with when it happened, why the law said
     * it happened, and what the downstream was doing at the time. ONE list, not a list of numbers beside a list of
     * rendered strings: the assertions and the human-readable trace are two views of the same observation, and two
     * lists would be two things to keep in step.
     */
    private final List<TargetChange> trajectory = Collections.synchronizedList(new ArrayList<>());

    private final CountDownLatch stopSampling = new CountDownLatch(1);
    private final CountDownLatch stopFeeding = new CountDownLatch(1);

    @BeforeEach
    void setUp() {
        numPartitions = 4;
        setupTopic();
    }

    @AfterEach
    void tearDown() {
        stopSampling.countDown();
        stopFeeding.countDown();
    }

    @Test
    void concurrencyFindsItsOwnLevelAgainstADownstreamThatPushesBack() throws Exception {
        Set<String> producedKeys = Collections.synchronizedSet(new HashSet<>(
                produceMessages(SEED_RECORDS, "seed-")));
        Set<String> processedKeys = ConcurrentHashMap.newKeySet();

        ParallelConsumerOptions<String, String> options = ParallelConsumerOptions.<String, String>builder()
                .consumer(getKcu().createNewConsumer(GroupOption.NEW_GROUP))
                .commitMode(PERIODIC_CONSUMER_ASYNCHRONOUS)
                .ordering(UNORDERED)
                .maxConcurrency(MAX_CONCURRENCY)
                .adaptiveConcurrencyMode(AdaptiveConcurrencyMode.ENFORCE)
                .adaptiveConcurrencyInitialTarget(INITIAL_TARGET)
                .build();
        PCModule<String, String> module = new PCModule<>(options);
        ParallelEoSStreamProcessor<String, String> pc = new ParallelEoSStreamProcessor<>(options, module);
        pc.subscribe(UniSets.of(getTopic()));

        // Take the controller reference BEFORE poll() starts any engine thread. PCModule#admissionController() is
        // synchronized now, so a racing first touch can no longer yield TWO controllers and leave this test
        // watching an orphan sitting at its seed forever - but taking the reference while this is the only thread
        // makes that independent of the accessor's guarantees.
        AdmissionController controller = module.admissionController();

        pc.poll(context -> {
            int concurrent = inFlight.incrementAndGet();
            try {
                long latency = latencyForInFlight(concurrent);
                lastLatencyMillis.set(latency);
                bumpMax(observedMaxLatencyMillis, latency);
                bumpMaxInt(observedMaxInFlight, concurrent);
                sleepQuietly(latency);
                processedKeys.add(context.key());
            } finally {
                inFlight.decrementAndGet();
            }
        });

        assertWithMessage("the run must actually be in ENFORCE, or nothing below is testing the feature")
                .that(controller.mode()).isEqualTo(AdaptiveConcurrencyMode.ENFORCE);
        assertWithMessage("the seed must be in force at t=0, or 'it ramped' means nothing")
                .that(controller.currentTarget()).isEqualTo(INITIAL_TARGET);

        long startedAt = System.nanoTime();
        startTargetSampler(controller, startedAt);
        Thread feeder = startBacklogFeeder(producedKeys, processedKeys);

        try {
            // ---- 1) it ramps INTO the elbow. ~13 one-second windows at the law's additive 0.8 slots per window.
            await().alias("the admission target ramps from its seed up to the elbow")
                    .atMost(120, SECONDS)
                    .failFast(pc::isClosedOrFailed)
                    .untilAsserted(() -> assertWithMessage(
                            "the controller must drive concurrency from its seed of %s up to the knee of %s - if "
                                    + "this times out with a flat target, read the held-constraint lines in the "
                                    + "log: they name what is pinning it", INITIAL_TARGET, ELBOW_WATERMARK)
                            .that(controller.currentTarget()).isAtLeast(ELBOW_WATERMARK));
            log.info("Crossed the elbow watermark of {} - trajectory so far: {}", ELBOW_WATERMARK, renderTrace(snapshotTrajectory()));

            // ---- 2) the downstream pushes back, and the law CONTRACTS. The overshoot step past the knee is
            // adjudicated by the first post-settle verdict (~8 offered windows); the bound is generous because
            // the claim is that it happens, not when.
            await().alias("the admission target is moved DOWN by the degradation it caused")
                    .atMost(180, SECONDS)
                    .failFast(pc::isClosedOrFailed)
                    .untilAsserted(() -> assertWithMessage(
                            "at least one window must move the target DOWN - above the knee more concurrency "
                                    + "buys strictly less throughput (knee^2/(base*inflight)), so the elasticity "
                                    + "verdict must go FALL or retract the unpaid step; a run that only ever "
                                    + "grows means the law absorbed the collapse instead of reacting to it. "
                                    + "Trajectory: %s", renderTrace(snapshotTrajectory()))
                            .that(downwardMoveCount()).isAtLeast(1));
            log.info("First contraction seen - trajectory so far: {}", renderTrace(snapshotTrajectory()));

            // ---- 3) watch what it does next. Bought time, not a convergence await: see the constant's javadoc.
            log.info("Observing for a further {}s to see where the target settles...", SETTLE_OBSERVATION_SECONDS);
            assertWithMessage("the engine must stay alive through the observation window")
                    .that(stopSampling.await(SETTLE_OBSERVATION_SECONDS, SECONDS)).isFalse();
            assertWithMessage("the engine must not have died during the observation window")
                    .that(pc.isClosedOrFailed()).isFalse();

            int finalTarget = controller.currentTarget();
            double observedEndSeconds = (System.nanoTime() - startedAt) / 1_000_000_000d;
            List<TargetChange> observed = snapshotTrajectory();
            List<Integer> targets = targetsOf(observed);

            // ---- correctness throughout: stop feeding, then everything produced must be processed.
            stopFeeding.countDown();
            feeder.join(SECONDS.toMillis(60));
            await().alias("the whole workload drains")
                    .atMost(180, SECONDS)
                    .failFast(pc::isClosedOrFailed)
                    .untilAsserted(() -> assertWithMessage("no loss: every produced record processed")
                            .that(processedKeys).containsAtLeastElementsIn(producedKeys));

            log.info("Admission target trajectory, in full:\n{}", renderTrace(observed));
            log.info("Ledger: produced {}, processed {}. Target: seed {}, max {}, min-after-peak {}, final {}. "
                            + "Downstream: max in-flight {}, max latency {}ms (base {}ms, knee {}).",
                    producedKeys.size(), processedKeys.size(), INITIAL_TARGET, Collections.max(targets),
                    minAfterPeak(targets), finalTarget, observedMaxInFlight.get(),
                    observedMaxLatencyMillis.get(), BASE_LATENCY_MS, KNEE_INFLIGHT);
            // The band, sliced by time. Under the old law this line was the evidence of the ratchet (the band
            // walked upward slice by slice); under the band machine it is the evidence of settling, and the
            // final third IS asserted below.
            log.info("Band by {}s slice (knee is {}): {}", BAND_SLICE_SECONDS, KNEE_INFLIGHT,
                    String.join("  ", bandsBySlice(observed)));

            // ---- assertions, in descending order of confidence

            assertWithMessage("1) it RAMPED: the target must have risen materially above its seed of %s, all the "
                    + "way to the knee. Trajectory: %s", INITIAL_TARGET, targets)
                    .that(Collections.max(targets)).isAtLeast(ELBOW_WATERMARK);

            assertWithMessage("2) it CONTRACTED: at least one window must have moved the target down. "
                    + "Trajectory: %s", targets)
                    .that(downwardMoveCount()).isAtLeast(1);

            assertWithMessage("3a) it did not end pinned at the FLOOR - a collapse to 1 slot is a control law "
                    + "that shed everything rather than finding a level. Trajectory: %s", targets)
                    .that(finalTarget).isGreaterThan(1);
            assertWithMessage("3b) it did not end pinned at the effective maximum of %s - parking at the cap "
                    + "means the elbow was never respected. Trajectory: %s", MAX_CONCURRENCY, targets)
                    .that(finalTarget).isLessThan(MAX_CONCURRENCY);

            assertWithMessage("the elbow must actually have been CROSSED in measurement, not merely in target: "
                    + "one accelerator step past the knee serves (15.5/12)^2 = 1.67x base, so a worst observed "
                    + "latency below %sms (1.5x base) would mean this run never drove the downstream into the "
                    + "collapsing region and the contraction above was measured off nothing",
                    (long) (BASE_LATENCY_MS * 1.5))
                    .that(observedMaxLatencyMillis.get()).isAtLeast((long) (BASE_LATENCY_MS * 1.5));

            // ---- 5) it SETTLES: the final third of the observation window is a band, not a walk. Two bounds,
            // both run-length independent (see the class javadoc's settled-band section): the range is at most
            // two accelerator steps (one provisional RISE step up, one descent-probe dip down - both transient
            // by design), and the time-weighted median sits within one accelerator step of the plant's actual
            // knee - which is the bound the old ratcheting law fails at any run length, because its band's
            // median walked away from the knee it had already found.
            BandStats finalThird = bandOver(observed, observedEndSeconds * 2.0 / 3.0, observedEndSeconds);
            double acceleratorStep = Math.ceil(Math.sqrt(finalThird.medianTarget));
            log.info("Final-third band [{}..{}], time-weighted median {} (knee {}, accelerator step {})",
                    finalThird.minTarget, finalThird.maxTarget, finalThird.medianTarget, KNEE_INFLIGHT,
                    acceleratorStep);
            assertWithMessage("5a) SETTLED range: over the final third of the observation window the target "
                    + "must stay within two accelerator steps of width - band was [%s..%s], median %s. "
                    + "Trajectory: %s", finalThird.minTarget, finalThird.maxTarget, finalThird.medianTarget,
                    targets)
                    .that((double) (finalThird.maxTarget - finalThird.minTarget))
                    .isAtMost(2 * acceleratorStep);
            assertWithMessage("5b) SETTLED level: the final-third time-weighted median must sit within one "
                    + "accelerator step of the knee of %s - a median that has walked away from a knee the law "
                    + "already found is the ratchet this law was rewritten to kill. Trajectory: %s",
                    KNEE_INFLIGHT, targets)
                    .that((double) Math.abs(finalThird.medianTarget - KNEE_INFLIGHT)).isAtMost(acceleratorStep);

            assertWithMessage("6) exactly the produced records were processed - no loss, and nothing invented")
                    .that(processedKeys).containsExactlyElementsIn(producedKeys);
        } finally {
            stopFeeding.countDown();
            stopSampling.countDown();
            pc.close();
        }
    }

    /**
     * The synthetic downstream - the shared {@link SyntheticCongestionCurve#quadratic quadratic}
     * congestion-collapse curve whose worked table is in the class javadoc. Flat below the knee so climbing
     * windows show clean positive elasticity; collapsing above it so crossing the elbow reads as negative
     * elasticity within one estimator horizon rather than asymptotically.
     */
    private static final SyntheticCongestionCurve DOWNSTREAM_CURVE =
            SyntheticCongestionCurve.quadratic(BASE_LATENCY_MS, KNEE_INFLIGHT);

    private static long latencyForInFlight(int concurrent) {
        return DOWNSTREAM_CURVE.serviceTimeMillis(concurrent);
    }

    /**
     * Keeps the topic from running dry. A starved engine and a controller that stopped adapting are
     * indistinguishable from the outside - both show a flat target - so the backlog is held above a low-water mark
     * for the whole experiment rather than sized by a guess up front. Each chunk gets its own key prefix: the keys
     * are the correctness ledger's identity, so a repeated prefix would silently understate the produced set.
     */
    private Thread startBacklogFeeder(Set<String> producedKeys, Set<String> processedKeys) {
        Thread feeder = new Thread(() -> {
            int chunk = 0;
            try {
                while (!stopFeeding.await(FEED_POLL_MS, TimeUnit.MILLISECONDS)) {
                    int backlog = producedKeys.size() - processedKeys.size();
                    if (backlog < BACKLOG_LOW_WATER) {
                        producedKeys.addAll(getKcu().produceMessages(
                                getTopic(), FEED_CHUNK, "feed-" + chunk + "-"));
                        chunk++;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.error("Backlog feeder failed - the workload may run dry and the run will misreport", e);
            }
        }, "backlog-feeder");
        feeder.setDaemon(true);
        feeder.start();
        return feeder;
    }

    /**
     * Records every distinct value the live target takes, off the control thread, together with the law's reason
     * and what the downstream was doing at that instant. Polling the controller's public reported state rather
     * than hooking it: that state is what an operator's dashboard reads, and a hook would test a path nothing
     * else uses.
     */
    private void startTargetSampler(AdmissionController controller, long startedAt) {
        Thread sampler = new Thread(() -> {
            int last = -1;
            while (true) {
                int current = controller.currentTarget();
                if (current != last) {
                    String reason = controller.lastDecisionReason()
                            .map(Enum::name).orElse("NO_DECISION_YET");
                    double elapsed = (System.nanoTime() - startedAt) / 1_000_000_000d;
                    trajectory.add(new TargetChange(elapsed, last < 0 ? INITIAL_TARGET : last, current, reason,
                            inFlight.get(), lastLatencyMillis.get()));
                    last = current;
                }
                try {
                    if (stopSampling.await(25, TimeUnit.MILLISECONDS)) {
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

    /** How many recorded movements took the target DOWN - the contraction evidence. */
    private int downwardMoveCount() {
        int downs = 0;
        for (TargetChange change : snapshotTrajectory()) {
            if (change.to < change.from) {
                downs++;
            }
        }
        return downs;
    }

    /** Min, max and time-weighted median of the target's step function over one time window. */
    private static final class BandStats {
        final int minTarget;
        final int maxTarget;
        final int medianTarget;

        BandStats(int minTarget, int maxTarget, int medianTarget) {
            this.minTarget = minTarget;
            this.maxTarget = maxTarget;
            this.medianTarget = medianTarget;
        }
    }

    /**
     * The target's band over {@code [fromSeconds, toSeconds)}, read off the step function the trajectory's
     * change-points define. Time-weighted, because the trajectory records CHANGES: a probe dip that lasted four
     * seconds must not count the same as the level the law held for twenty - the median is the level the law
     * actually occupied, which is what "settled" means.
     */
    private static BandStats bandOver(List<TargetChange> changes, double fromSeconds, double toSeconds) {
        List<double[]> segments = new ArrayList<>(); // {target, durationSeconds}
        int current = INITIAL_TARGET;
        double segmentStart = fromSeconds;
        for (TargetChange change : changes) {
            if (change.atSeconds <= fromSeconds) {
                current = change.to; // still before the window: just track the level in force at its start
                continue;
            }
            if (change.atSeconds >= toSeconds) {
                break;
            }
            segments.add(new double[]{current, change.atSeconds - segmentStart});
            current = change.to;
            segmentStart = change.atSeconds;
        }
        segments.add(new double[]{current, toSeconds - segmentStart});
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        double total = 0;
        for (double[] segment : segments) {
            if (segment[1] <= 0) {
                continue;
            }
            min = Math.min(min, (int) segment[0]);
            max = Math.max(max, (int) segment[0]);
            total += segment[1];
        }
        segments.sort((a, b) -> Double.compare(a[0], b[0]));
        double cumulative = 0;
        int median = min;
        for (double[] segment : segments) {
            cumulative += segment[1];
            if (cumulative >= total / 2) {
                median = (int) segment[0];
                break;
            }
        }
        return new BandStats(min, max, median);
    }

    /** The lowest target reached AFTER the peak - how far the contraction actually walked it back. */
    private static int minAfterPeak(List<Integer> targets) {
        int peakIndex = targets.indexOf(Collections.max(targets));
        int min = targets.get(peakIndex);
        for (int i = peakIndex; i < targets.size(); i++) {
            min = Math.min(min, targets.get(i));
        }
        return min;
    }

    /**
     * The [min, max] target within each {@value #BAND_SLICE_SECONDS}-second slice of the run - the view that makes
     * a RATCHETING band distinguishable from a settled one. A single min/max over the whole run cannot tell those
     * two apart, which is exactly the distinction this experiment exists to draw.
     */
    private static List<String> bandsBySlice(List<TargetChange> changes) {
        List<String> slices = new ArrayList<>();
        if (changes.isEmpty()) {
            return slices;
        }
        double end = changes.get(changes.size() - 1).atSeconds;
        for (double from = 0; from < end; from += BAND_SLICE_SECONDS) {
            Integer min = null;
            Integer max = null;
            for (TargetChange change : changes) {
                if (change.atSeconds >= from && change.atSeconds < from + BAND_SLICE_SECONDS) {
                    min = min == null ? change.to : Math.min(min, change.to);
                    max = max == null ? change.to : Math.max(max, change.to);
                }
            }
            slices.add(String.format("[%3.0f-%3.0fs]=%s", from, from + BAND_SLICE_SECONDS,
                    min == null ? "-" : (min.equals(max) ? min.toString() : min + ".." + max)));
        }
        return slices;
    }

    private static List<Integer> targetsOf(List<TargetChange> changes) {
        List<Integer> targets = new ArrayList<>();
        for (TargetChange change : changes) {
            targets.add(change.to);
        }
        return targets;
    }

    private static String renderTrace(List<TargetChange> changes) {
        StringBuilder rendered = new StringBuilder();
        for (TargetChange change : changes) {
            rendered.append(change).append('\n');
        }
        return rendered.toString();
    }

    private List<TargetChange> snapshotTrajectory() {
        synchronized (trajectory) {
            return new ArrayList<>(trajectory);
        }
    }

    /** One movement of the live admission target, with everything needed to interpret it. */
    private static final class TargetChange {
        private final double atSeconds;
        private final int from;
        private final int to;
        private final String reason;
        private final int inFlight;
        private final long latencyMillis;

        private TargetChange(double atSeconds, int from, int to, String reason, int inFlight, long latencyMillis) {
            this.atSeconds = atSeconds;
            this.from = from;
            this.to = to;
            this.reason = reason;
            this.inFlight = inFlight;
            this.latencyMillis = latencyMillis;
        }

        @Override
        public String toString() {
            return String.format("t=%6.1fs  target %2d -> %2d  (%s)  in-flight=%2d  latency=%dms",
                    atSeconds, from, to, reason, inFlight, latencyMillis);
        }
    }

    private static void bumpMax(AtomicLong holder, long candidate) {
        long seen;
        while (candidate > (seen = holder.get()) && !holder.compareAndSet(seen, candidate)) {
            // another worker raised it first - re-read and retry
        }
    }

    private static void bumpMaxInt(AtomicInteger holder, int candidate) {
        int seen;
        while (candidate > (seen = holder.get()) && !holder.compareAndSet(seen, candidate)) {
            // another worker raised it first - re-read and retry
        }
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
