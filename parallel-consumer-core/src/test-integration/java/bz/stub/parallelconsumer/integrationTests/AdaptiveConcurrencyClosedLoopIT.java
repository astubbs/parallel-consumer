package bz.stub.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.AdaptiveConcurrencyMode;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption;
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
 * invocation slower, so the target the law picks is the input to the latency it then measures. The whole arc the
 * feature exists for happens inside one run - ramp up, cross the elbow, latency degrades, the law contracts,
 * concurrency falls back below the elbow, latency recovers - and where it lands is a property of the control law,
 * not of the test.
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
 * The curve is the textbook quadratic queueing shape, flat below a knee and rising as the square above it:
 * <pre>
 *     latency = BASE * max(1, inflight / KNEE)^2
 * </pre>
 * With {@value #BASE_LATENCY_MS}ms and a knee of {@value #KNEE_INFLIGHT}, that is:
 * <pre>
 *     in-flight :   6     12     14     15     16     18     20     24     32
 *     latency   :  80ms   80ms  109ms  125ms  142ms  180ms  222ms  320ms  569ms
 *     vs base   : 1.00x  1.00x  1.36x  1.56x  1.78x  2.25x  2.78x  4.00x  7.11x
 * </pre>
 * A quadratic rather than a kink because the law does not react to a slope, it reacts to a RATIO - see below - and
 * a linear ramp reaches that ratio so far out that the ceiling arrives first.
 *
 * <h2>The arithmetic the model was designed against, before any {@code await} was written</h2>
 * <b>What actually makes the law contract.</b> The gradient is
 * {@code clamp(TOLERANCE * long / short, 0.5, 1.0)} with a tolerance of
 * {@code AdmissionControlLaw.DEFAULT_SERVICE_TIME_TOLERANCE} = 1.5, so short-term latency must exceed the long-term
 * baseline by more than <b>1.5x</b> before the gradient leaves 1.0 at all. That alone is not enough to move the
 * target down, because growth is additive: one window computes
 * {@code limit*(1-s) + (limit*g + headroom)*s} with smoothing 0.2 and headroom 4, so the target only DECREASES when
 * {@code limit * (1 - g) > 4}. At a limit of 17 that needs {@code g < 0.76}, i.e. a measured
 * <b>short/long ratio above about 2.0</b>. The curve above delivers 2.25x at 18 in-flight and 2.78x at 20, so
 * crossing the elbow clears the bar with margin. A gentle 20% rise would never trigger anything, and this test
 * would pass while proving nothing - which is why {@link #observedMaxLatencyMillis} is asserted directly.
 * <p>
 * <b>How long the ramp takes.</b> While the gradient is pinned at 1.0 the target grows by a constant 0.8 slots per
 * one-second window, so seed {@value #INITIAL_TARGET} to knee {@value #KNEE_INFLIGHT} is
 * {@code (12 - 2) / 0.8 = 13} windows, and the first genuine contraction lands a further ~25 windows out (a
 * simulation of the law against this exact curve put it at window 41). {@link #SETTLE_OBSERVATION_SECONDS} of
 * further watching is then bought outright, so the run has a chance to show what it does after the first break.
 * <p>
 * <b>Records needed.</b> Throughput is {@code concurrency / latency}, so this workload's own arithmetic bounds it:
 * ~90/s while ramping under the knee, ~105/s through the elbow, ~100/s once hunting - call it 10,000 records for a
 * two-minute run. A workload that runs dry looks EXACTLY like a controller that stopped adapting (the window goes
 * app-limited and the law correctly holds), so rather than guessing a total up front, {@link #startBacklogFeeder}
 * tops the topic up whenever the un-processed backlog falls below {@value #BACKLOG_LOW_WATER}. The backlog is
 * therefore never smaller than three times the {@value #MAX_CONCURRENCY}-slot ceiling, and never so large that the
 * final drain dominates the run.
 * <p>
 * <b>{@code UNORDERED}, deliberately.</b> Under key or partition ordering, in-flight concurrency is capped by the
 * key/partition count rather than by admission, the law correctly reports an application-limited workload, and the
 * experiment measures the shard model instead of the controller.
 *
 * <h2>What a run of this actually shows - and the one thing it shows that is not good news</h2>
 * Three consecutive runs on the same machine agreed to within about a second on every movement, and landed on
 * identical max/final targets and an identical worst observed latency (222ms). The target ramps
 * cleanly 2 -&gt; 12 in ~14s, keeps climbing to 17 by ~21s, and the elbow then bites: the first DOWN movement lands
 * at ~28s, 18 -&gt; 17, at a measured 180ms against an ~80ms baseline. From there it does not converge on a value -
 * it <b>hunts between two adjacent slots</b>, 17 and 18, taking each in turn as the latency it just caused flips
 * the gradient back and forth. That much is a control loop working: the elbow is found, respected, and defended
 * without ever touching the floor or the cap.
 * <p>
 * The unflattering part is what the {@code Hunting band by 20s slice} line exposes. The band does not stay put:
 * <pre>
 *     [  0- 20s]=2..16   [ 20- 40s]=17..18   [ 40- 60s]=17..18   [ 60- 80s]=18..19   [ 80-100s]=18..19
 * </pre>
 * The pair walks UPWARD - 17/18, then 18/19, then a step to 20 - because the long-term baseline is an EWMA over
 * {@code AdmissionControlLaw.DEFAULT_LONG_BASELINE_WINDOW} = 600 windows and it slowly absorbs the very
 * degradation it is supposed to be the reference for. As the baseline creeps toward the operating latency, the
 * short/long ratio falls, the gradient relaxes, and the additive headroom wins another slot - which raises latency
 * again. The PR-88 anti-drift decay in the law only pulls a STALE-HIGH baseline down; nothing pulls a
 * slowly-inflating one back. Simulating this same curve against the law for 400 windows walks the target from 17
 * to 27 and still climbing, so this is a ratchet with no fixed point below the ceiling, not a slow approach to
 * one.
 * <p>
 * Hence <b>no band is asserted here.</b> A band is genuinely observed, but it drifts, so any bound would be a
 * number fitted to one run length and would go red the day someone lengthened the run - the failure mode this
 * repo's hypothesis register exists to record. The band is printed and left for a reader; the assertions below
 * claim only what is stable across runs. The finding itself is tracked in
 * {@code docs/inflight/pr-333-adaptive-concurrency-outstanding.md}.
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
     * convergence condition, because whether this law converges at all is the open question - an await that waited
     * for a band would either pass on the first accident or hang on a law that hunts forever, and neither reports
     * what happened.
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

            // ---- 2) the downstream pushes back, and the law CONTRACTS. The predicted window is ~25 further
            // windows out; the bound is generous because the claim is that it happens, not when.
            await().alias("the admission target is moved DOWN by the degradation it caused")
                    .atMost(180, SECONDS)
                    .failFast(pc::isClosedOrFailed)
                    .untilAsserted(() -> assertWithMessage(
                            "at least one window must move the target DOWN - the elbow makes latency %sx base at "
                                    + "18 in-flight, well past the law's 1.5x tolerance, so a run that only ever "
                                    + "grows means the law absorbed the degradation instead of reacting to it. "
                                    + "Trajectory: %s", "2.25", renderTrace(snapshotTrajectory()))
                            .that(downwardMoveCount()).isAtLeast(1));
            log.info("First contraction seen - trajectory so far: {}", renderTrace(snapshotTrajectory()));

            // ---- 3) watch what it does next. Bought time, not a convergence await: see the constant's javadoc.
            log.info("Observing for a further {}s to see where the target settles...", SETTLE_OBSERVATION_SECONDS);
            assertWithMessage("the engine must stay alive through the observation window")
                    .that(stopSampling.await(SETTLE_OBSERVATION_SECONDS, SECONDS)).isFalse();
            assertWithMessage("the engine must not have died during the observation window")
                    .that(pc.isClosedOrFailed()).isFalse();

            int finalTarget = controller.currentTarget();
            List<TargetChange> observed = snapshotTrajectory();
            List<Integer> targets = targetsOf(observed);

            // ---- 4) correctness throughout: stop feeding, then everything produced must be processed.
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
            // The hunting band, sliced by time. NOT asserted - see the class javadoc's "what a run of this
            // actually shows": the band is real but it RATCHETS, so any fixed bound would be a number tuned to one
            // run length. Printed because the drift is the finding.
            log.info("Hunting band by {}s slice (knee is {}): {}", BAND_SLICE_SECONDS, KNEE_INFLIGHT,
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
                    + "the law contracts only above a %sx short/long ratio, so a worst observed latency below "
                    + "that of %sms would mean this run never presented the law with a reason to back off",
                    "1.5", (long) (BASE_LATENCY_MS * 1.5))
                    .that(observedMaxLatencyMillis.get()).isAtLeast((long) (BASE_LATENCY_MS * 1.5));

            assertWithMessage("4) exactly the produced records were processed - no loss, and nothing invented")
                    .that(processedKeys).containsExactlyElementsIn(producedKeys);
        } finally {
            stopFeeding.countDown();
            stopSampling.countDown();
            pc.close();
        }
    }

    /**
     * The synthetic downstream's service-time curve - {@code BASE * max(1, inflight/KNEE)^2}, the quadratic
     * queueing shape whose worked table is in the class javadoc. Flat below the knee so the law's long-term
     * baseline has something clean to warm up on; square above it so crossing the elbow clears the law's 1.5x
     * tolerance quickly rather than asymptotically.
     */
    private static long latencyForInFlight(int concurrent) {
        double overshoot = Math.max(1.0, concurrent / (double) KNEE_INFLIGHT);
        return Math.round(BASE_LATENCY_MS * overshoot * overshoot);
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
