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
import bz.stub.parallelconsumer.metrics.PCMetricsDef;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.search.Search;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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

import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

/**
 * <b>Adaptive concurrency, in {@code ENFORCE}, against a real broker.</b> The claim is liveness and correctness,
 * never performance: an instance whose admission target is being steered by the controller <em>runs</em>, does not
 * stall, loses no records, adapts, and survives a rebalance. Every other adaptive test in this repo is a unit test
 * driven by an injected clock and a mock consumer, so this is the only place the controller and a genuinely
 * running engine are observed composing - real poll cadence, real commits, real rebalance, real scheduling.
 *
 * <h2>Run it, and watch the target climb</h2>
 * <pre>{@code
 * JAVA_HOME=$HOME/.sdkman/candidates/java/17.0.18-tem ./mvnw --batch-mode \
 *     -pl parallel-consumer-core -am verify \
 *     -DskipUTs=true -Dit.test=AdaptiveConcurrencyEnforceIT \
 *     -Dfailsafe.failIfNoSpecifiedTests=false
 * }</pre>
 * That last flag is not optional: {@code -am} builds the parent module too, failsafe runs there as well, and
 * it fails the build for matching no tests - which looks exactly like this test failing.
 * That is already watchable: {@code logback-test.xml} leaves the controller's own logger ON at {@code INFO} while
 * the rest of the library sits at {@code WARN}, so the PC output of this run IS the target's trajectory and
 * nothing else. The same one line makes it watchable in <b>your</b> application, where the library defaults to
 * silent:
 * <pre>{@code
 * <logger name="bz.stub.parallelconsumer.internal.admission.AdmissionController" level="info"/>
 * }</pre>
 * Or filter an existing log on the prefix every one of those lines opens with: {@code grep 'Adaptive concurrency'}.
 * Two shapes appear - the target MOVING, and (rate-limited) the target being HELD by a named constraint:
 * <pre>
 * 49:14.248 5f3ed23e [pc-control-5f3ed23e] Adaptive concurrency (ENFORCE): live admission target 2 -&gt; 3
 *     slot(s), decided by ADAPTING - service time mean 12.12ms against long-run baseline 12.28ms, ratio 0.99
 *     (contracts above 1.50), in-flight median 2 (spread 0) over 154 snapshot(s), 162 sample(s), effective
 *     maximum 32.
 * 49:22.827 5f3ed23e [pc-control-5f3ed23e] Adaptive concurrency (ENFORCE): the admission target is being held
 *     by COOLDOWN - live target 9 slot(s), would-be target 9 slot(s), effective maximum 32.
 * </pre>
 * Both are real lines from a run of this test - the second is the post-rebalance freeze, which is what the
 * target does for thirty seconds after a member joins.
 * <p>
 * Two things in there are what make a two-instance run readable at all, and both are worth knowing about when you
 * read your own logs. {@code 5f3ed23e} is the instance id ({@code %X{pcId}}, and the suffix on the thread name):
 * this test runs a second instance in the same JVM, and without it the two trajectories interleave into one
 * stream that reads as a single controller jumping from 9 back to 2. And {@code against long-run baseline ...
 * ratio ... (contracts above ...)} is the comparison the law actually decided on - the short-term figure alone
 * never says why it means grow.
 *
 * <h2>Why this shape of workload</h2>
 * The controller's law needs at least ten service-time samples in a one-second window before it will move
 * anything, and it only reads a window as informative when in-flight concurrency is at least half the current
 * target - so a workload that is instant, or too small to keep the pipeline full, produces a run in which the
 * controller correctly does nothing and the test proves nothing. Hence a deliberately slow handler
 * ({@value #HANDLER_LATENCY_MS}ms), a backlog big enough to keep it saturated for tens of windows, and a
 * {@link ParallelConsumerOptions#getAdaptiveConcurrencyInitialTarget() seed} of {@value #INITIAL_TARGET} well
 * below the {@value #MAX_CONCURRENCY}-slot ceiling, so the ramp has somewhere to go and is visible inside a short
 * run. Growth is a constant additive step of roughly 0.8 slots per window, so the distance from seed to ceiling
 * is what buys wall-clock time here, not the record count.
 *
 * @see AdmissionController
 */
@Timeout(300)
@Testcontainers
@Slf4j
class AdaptiveConcurrencyEnforceIT extends BrokerIntegrationTest<String, String> {

    /**
     * Per-record handler latency. Slow enough that a one-second window fills with well over the law's ten-sample
     * minimum at any target we reach, and slow enough that in-flight concurrency actually sits at the target
     * rather than emptying between polls. A sleep, deliberately: it consumes no CPU, so measured service time
     * stays flat as concurrency grows and the gradient is free to ask for more - a CPU-bound handler would
     * (correctly) be throttled by the very law under test, which is a different claim.
     */
    private static final int HANDLER_LATENCY_MS = 10;

    /**
     * The seeded starting target. Low on purpose: the assertion below is that the controller MOVED it, and a run
     * that starts at the ceiling has nowhere to move to.
     */
    private static final int INITIAL_TARGET = 2;

    /** The ceiling, which is {@code maxConcurrency} - high enough that growth is never clamped in this run. */
    private static final int MAX_CONCURRENCY = 32;

    /**
     * How far the target must climb before the test moves on to the rebalance. Not merely "off the seed": one
     * step could be a single lucky window, whereas six consecutive growth steps is a controller tracking a
     * workload. It is also what makes the run worth WATCHING - the rebalance freezes the target for a cooldown,
     * so whatever ramp is going to be visible has to happen before it.
     */
    private static final int RAMP_WATERMARK = INITIAL_TARGET + 6;

    /**
     * Backlog for phase one. Sized by the RAMP, not by the ledger: growth is time-based (~0.8 slots per
     * one-second window) and only continues while the pipeline stays full, so the backlog has to outlast the
     * climb to {@link #RAMP_WATERMARK} with room to spare - a backlog that drains first ends the ramp early and
     * the await fails on a controller that was behaving perfectly.
     */
    private static final int PHASE_ONE_RECORDS = 12_000;

    /** Phase two, trickled in so the second instance is guaranteed to find work after the rebalance. */
    private static final int PHASE_TWO_CHUNKS = 30;
    private static final int PHASE_TWO_CHUNK_SIZE = 100;
    private static final int PHASE_TWO_CHUNK_PAUSE_MS = 200;

    /**
     * Duplicate ceiling for the correctness ledger. At-least-once delivery plus one rebalance legitimately
     * re-delivers an uncommitted tail (in-flight work at revocation, plus commit-interval lag), which is
     * capacity-shaped and small. The bound exists to catch the failure that is NOT that: a hand-over that ignored
     * the committed offsets and reprocessed the topic wholesale.
     */
    private static final int DUPLICATE_BOUND = 1_000;

    private SimpleMeterRegistry meterRegistry;

    /**
     * Successive DISTINCT values of the live admission target, sampled off instance A's controller by
     * {@link #startTargetSampler}. The log lines are the operator's view of this; the list is the test's, and it
     * is what the movement assertion reads.
     */
    private final List<Integer> targetTrajectory = Collections.synchronizedList(new ArrayList<>());

    private final CountDownLatch stopSampling = new CountDownLatch(1);

    @BeforeEach
    void setUp() {
        numPartitions = 4; // enough that a completed rebalance can actually split work between two members
        setupTopic();
        meterRegistry = new SimpleMeterRegistry();
    }

    @AfterEach
    void tearDown() {
        stopSampling.countDown();
        meterRegistry.close();
    }

    @Test
    void enforcesAdaptivelyAgainstARealBrokerAcrossARebalance() throws Exception {
        Set<String> producedKeys = Collections.synchronizedSet(new HashSet<>(
                produceMessages(PHASE_ONE_RECORDS, "phase-one-")));

        // ---- instance A, alone, seeded deliberately low so the ramp is visible
        Set<String> processedByA = ConcurrentHashMap.newKeySet();
        ParallelConsumerOptions<String, String> optionsA = enforcingOptions(GroupOption.NEW_GROUP, meterRegistry);
        PCModule<String, String> moduleA = new PCModule<>(optionsA);
        ParallelEoSStreamProcessor<String, String> pcA = new ParallelEoSStreamProcessor<>(optionsA, moduleA);
        pcA.subscribe(UniSets.of(getTopic()));

        // Take the controller reference BEFORE poll() starts any engine thread, and keep doing so even though
        // PCModule#admissionController() is now synchronized. The first run of this test raced that accessor -
        // a touch from this thread against the control loop's first pass constructed TWO controllers, and the
        // one the test held then sat at its seed for the full ninety-second await while a complete 2 -> 12 ramp
        // printed beside it from the other. Taking the reference before any engine thread exists keeps this
        // deterministic regardless of the accessor's guarantees, and the isSameInstanceAs guard below fails
        // loudly if a second controller ever appears again.
        AdmissionController controllerA = moduleA.admissionController();

        pcA.poll(context -> {
            sleepQuietly(HANDLER_LATENCY_MS);
            processedByA.add(context.key());
        });

        assertWithMessage("the run must actually be in ENFORCE, or nothing below is testing the feature")
                .that(controllerA.mode()).isEqualTo(AdaptiveConcurrencyMode.ENFORCE);
        assertWithMessage("the seed must be in force at t=0, or 'it moved' means nothing")
                .that(controllerA.currentTarget()).isEqualTo(INITIAL_TARGET);
        startTargetSampler(controllerA);

        // ---- 1) it runs, and 2) it ADAPTS: the target must climb under a live control loop.
        // Growth is ~0.8 slots per one-second window, so the watermark is roughly eight seconds of ramp; the
        // bound is generous because this is a liveness claim, not a rate claim.
        await().alias("the admission target ramps up from its seed")
                .atMost(90, SECONDS)
                .failFast(pcA::isClosedOrFailed)
                .untilAsserted(() -> assertWithMessage(
                        "the controller must ramp the live target from its seed of %s up to %s - if this times "
                                + "out with a flat target, read the held-constraint lines in the log: they name "
                                + "what is pinning it", INITIAL_TARGET, RAMP_WATERMARK)
                        .that(controllerA.currentTarget()).isAtLeast(RAMP_WATERMARK));

        log.info("Target has ramped to the watermark: trajectory so far {}", snapshotTrajectory());

        // The wiring guard for the race above: if the module's field no longer holds the instance this test is
        // watching, a second controller was constructed and every assertion below is reading an orphan.
        assertWithMessage("the module must still hold the controller this test is asserting on - a different "
                + "instance means the lazy initialiser raced and the engine is steering a different controller")
                .that(moduleA.admissionController()).isSameInstanceAs(controllerA);

        // The metric must have moved with the controller - the gauge is how an operator without this test sees
        // it. Compared against the SEED rather than against a second live read of the target, which the control
        // thread is free to change between the two reads.
        assertWithMessage("pc.admission.target must track the live target, which has left its seed")
                .that(gaugeValue(PCMetricsDef.ADMISSION_TARGET)).isGreaterThan((double) INITIAL_TARGET);
        assertWithMessage("pc.admission.movements must have counted the movement(s) the target made")
                .that(counterValue(PCMetricsDef.ADMISSION_MOVEMENTS)).isGreaterThan(0.0);

        // ---- 3) it survives a rebalance. Phase two is trickled in from a background thread so that work is
        // still arriving while B joins - otherwise A can drain the whole backlog first and B, having nothing to
        // do, would make the "processing continues afterwards" assertion vacuous.
        CountDownLatch phaseTwoProduced = new CountDownLatch(1);
        Thread trickle = startPhaseTwoProducer(producedKeys, phaseTwoProduced);

        Set<String> processedByB = ConcurrentHashMap.newKeySet();
        ParallelConsumerOptions<String, String> optionsB = enforcingOptions(GroupOption.REUSE_GROUP, null);
        ParallelEoSStreamProcessor<String, String> pcB = new ParallelEoSStreamProcessor<>(optionsB);
        pcB.subscribe(UniSets.of(getTopic()));
        pcB.poll(context -> {
            sleepQuietly(HANDLER_LATENCY_MS);
            processedByB.add(context.key());
        });

        try {
            await().alias("the second member gets work after the rebalance")
                    .atMost(120, SECONDS)
                    .failFast(() -> pcA.isClosedOrFailed() || pcB.isClosedOrFailed())
                    .untilAsserted(() -> assertWithMessage(
                            "B joined the group and must be assigned and processing - if it never is, the "
                                    + "rebalance did not complete")
                            .that(processedByB.size()).isAtLeast(1));

            assertWithMessage("A must still be alive after the rebalance - the claim is that it SURVIVES one")
                    .that(pcA.isClosedOrFailed()).isFalse();

            assertWithMessage("the phase-two producer should have finished feeding the topic")
                    .that(phaseTwoProduced.await(120, SECONDS)).isTrue();

            // ---- 4) it does not stall, and loses nothing: every produced record processed by some member.
            await().alias("the whole workload completes after the rebalance")
                    .atMost(180, SECONDS)
                    .failFast(() -> pcA.isClosedOrFailed() || pcB.isClosedOrFailed())
                    .untilAsserted(() -> {
                        Set<String> union = ConcurrentHashMap.newKeySet();
                        union.addAll(processedByA);
                        union.addAll(processedByB);
                        assertWithMessage("no loss: every produced record processed by some member")
                                .that(union).containsAtLeastElementsIn(producedKeys);
                    });
        } finally {
            trickle.join(SECONDS.toMillis(30));
            stopSampling.countDown();
            pcA.close();
            pcB.close();
        }

        // ---- the ledger, once both instances are quiet
        Set<String> union = new HashSet<>(processedByA);
        union.addAll(processedByB);
        Set<String> duplicates = new HashSet<>(processedByA);
        duplicates.retainAll(processedByB);
        List<Integer> trajectory = snapshotTrajectory();
        log.info("Ledger: produced {}, A processed {}, B processed {}, union {}, duplicates {}. "
                        + "Admission target trajectory: {}",
                producedKeys.size(), processedByA.size(), processedByB.size(), union.size(), duplicates.size(),
                trajectory);

        assertWithMessage("exactly the produced records were processed - no loss, and nothing invented")
                .that(union).containsExactlyElementsIn(producedKeys);
        assertWithMessage("hand-over must honour the committed offsets: duplicates bounded to the uncommitted "
                + "tail, not a wholesale reprocess of %s records", producedKeys.size())
                .that(duplicates.size()).isAtMost(DUPLICATE_BOUND);
        assertWithMessage("the controller must have been LIVE, not inert - the target's whole trajectory was %s",
                trajectory)
                .that(Collections.max(trajectory)).isAtLeast(RAMP_WATERMARK);
    }

    /**
     * The options both instances run: {@code ENFORCE}, seeded low, with a ceiling high enough to grow into.
     * {@code UNORDERED} because the claim is about admission, and key- or partition-ordering would cap in-flight
     * concurrency at the key/partition count instead - the controller would then correctly report an
     * application-limited workload and never move.
     *
     * @param registry the meter registry to publish into, or null for an instance whose metrics nothing asserts
     */
    private ParallelConsumerOptions<String, String> enforcingOptions(GroupOption groupOption,
                                                                     SimpleMeterRegistry registry) {
        return ParallelConsumerOptions.<String, String>builder()
                .consumer(getKcu().createNewConsumer(groupOption))
                .commitMode(PERIODIC_CONSUMER_ASYNCHRONOUS)
                .ordering(UNORDERED)
                .maxConcurrency(MAX_CONCURRENCY)
                .adaptiveConcurrencyMode(AdaptiveConcurrencyMode.ENFORCE)
                .adaptiveConcurrencyInitialTarget(INITIAL_TARGET)
                .meterRegistry(registry)
                .build();
    }

    /**
     * Feeds phase two in chunks rather than one batch, so records are still arriving while the second instance
     * joins - see the call site for why a drained topic would make the post-rebalance assertion vacuous.
     */
    private Thread startPhaseTwoProducer(Set<String> producedKeys, CountDownLatch done) {
        Thread producer = new Thread(() -> {
            try {
                for (int chunk = 0; chunk < PHASE_TWO_CHUNKS; chunk++) {
                    // distinct prefix per chunk: keys are prefix + "key-" + index, so a repeated prefix would
                    // produce the SAME keys again and silently understate the produced set
                    producedKeys.addAll(getKcu().produceMessages(
                            getTopic(), PHASE_TWO_CHUNK_SIZE, "phase-two-" + chunk + "-"));
                    sleepQuietly(PHASE_TWO_CHUNK_PAUSE_MS);
                }
            } catch (Exception e) {
                log.error("Phase two producer failed - the completion assertion will fail with it", e);
            } finally {
                done.countDown();
            }
        }, "phase-two-producer");
        producer.setDaemon(true);
        producer.start();
        return producer;
    }

    /**
     * Records every distinct value the live target takes, off the control thread. Polling rather than hooking the
     * controller: the seam under test is its public reported state, which is what an operator's dashboard reads,
     * and a hook would test a path nothing else uses.
     */
    private void startTargetSampler(AdmissionController controller) {
        Thread sampler = new Thread(() -> {
            int last = -1;
            while (true) {
                int current = controller.currentTarget();
                if (current != last) {
                    targetTrajectory.add(current);
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

    private List<Integer> snapshotTrajectory() {
        synchronized (targetTrajectory) {
            return new ArrayList<>(targetTrajectory);
        }
    }

    private double gaugeValue(PCMetricsDef def) {
        Gauge gauge = Search.in(meterRegistry).name(def.getName()).gauge();
        assertWithMessage("meter %s must be registered", def.getName()).that(gauge).isNotNull();
        return gauge.value();
    }

    private double counterValue(PCMetricsDef def) {
        Counter counter = Search.in(meterRegistry).name(def.getName()).counter();
        assertWithMessage("meter %s must be registered", def.getName()).that(counter).isNotNull();
        return counter.count();
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
