package bz.stub.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.ProgressBarUtils;
import bz.stub.parallelconsumer.internal.utils.ProgressTracker;
import bz.stub.parallelconsumer.internal.utils.TrimListRepresentation;
import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.integrationTests.chaostests.ProgressProbe;
import bz.stub.parallelconsumer.integrationTests.utils.ManagedPCInstance;
import lombok.Builder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.internal.StandardComparisonStrategy;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.awaitility.core.TerminalFailureException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.MDC;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS;
import static bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor.MDC_INSTANCE_ID;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.IterableUtil.toCollection;
import static org.awaitility.Awaitility.waitAtMost;

/**
 * Tests running multiple instances of parallel-consumer against one consumer group, under membership
 * churn (instances stopping and starting, forcing rebalances).
 * <p>
 * <b>One scenario implementation, two kinds of profile</b> - because "does PC handle rebalance churn
 * correctly?" and "how much churn can PC survive?" are different questions that one test cannot
 * answer at once (a correctness gate must pass 100%; a capacity probe's legitimate output is a rate):
 * <ul>
 *   <li><b>Correctness profiles</b> (no {@code performance} tag - they gate): deterministic by
 *   construction. Churn is a {@link Churn#SCRIPTED_ROUNDS scripted, event-anchored schedule} rather
 *   than a random storm, the broker is fresh/uncontended (contention is a confound here, not the
 *   subject), and the assertion is <em>progress</em> - the consumed count must advance within
 *   {@link ProgressProbe#NO_PROGRESS_WINDOW} while work remains - never "all N records within T",
 *   which fails a slow run and a stalled run identically.</li>
 *   <li><b>Capacity profiles</b> ({@code @Tag("performance")} - the performance lane, which never
 *   gates a merge): the original {@link Churn#RANDOM_STORM random chaos-monkey storm} at full
 *   aggression, on the shared (contended) broker. Their pass <em>rate</em> over many runs is the
 *   measurement; a single run's outcome is not a verdict on PC.</li>
 * </ul>
 * The astubbs#68 precedent cuts both ways here: giving every test an uncontended broker made the
 * suite green and thereby <em>hid</em> the confluentinc#857 deadlock - so uncontended is right for
 * the correctness arm (isolate the subject) and deliberately wrong for the capacity arm (contention
 * is part of what it measures).
 */
@Slf4j
public class MultiInstanceRebalanceTest extends BrokerIntegrationTest<String, String> {

    static final int DEFAULT_MAX_POLL = 500;
    public static final int DEFAULT_CHAOS_FREQUENCY = 500;
    public static final int DEFAULT_POLL_DELAY = 150;

    AtomicInteger count = new AtomicInteger();

    static {
        MDC.put(MDC_INSTANCE_ID, "Test-Thread");
    }

    /** How membership churn is injected into the running fleet. */
    public enum Churn {
        /**
         * The original chaos monkey: a background thread that, at random intervals up to
         * {@link Scenario#chaosFrequencyMs}, toggles (stop/start) up to 60% of the secondary
         * instances at random. Non-deterministic by design - capacity profiles only. (With a single
         * secondary the toggle count rounds down to zero, so the two-instance correctness profiles
         * run this as a no-op: their churn is just the second instance's initial join.)
         */
        RANDOM_STORM,
        /**
         * Deterministic schedule: {@link Scenario#scriptedToggleRounds} rounds, each synchronously
         * stopping one secondary (round-robin victim - a leave-group rebalance), asserting the
         * survivors make progress, restarting it (a join rebalance), and asserting progress again.
         * Every step is anchored to an observed event, never a sleep, and nothing is random.
         */
        SCRIPTED_ROUNDS
    }

    /**
     * All the knobs of the multi-instance churn scenario, so one implementation
     * ({@link #runScenario(Scenario)}) drives both the gating correctness profiles and the
     * performance-lane capacity profiles.
     */
    @Builder
    static class Scenario {
        @Builder.Default
        final int maxPoll = DEFAULT_MAX_POLL;
        final CommitMode commitMode;
        final ProcessingOrder order;
        /** Total records produced; every key must eventually be consumed at least once. */
        final int messageCount;
        /** Total PC instances; instance 0 is never churned. */
        final int instances;
        /** Fraction of {@link #messageCount} produced before PC-0 starts; the rest stream in behind. */
        @Builder.Default
        final double preProduceFraction = 1.0;
        /** Per-record processing delay - throttles throughput so work remains while churn happens. */
        @Builder.Default
        final int pollDelayMs = DEFAULT_POLL_DELAY;
        @Builder.Default
        final boolean cooperativeAssignor = false;
        @Builder.Default
        final Churn churn = Churn.RANDOM_STORM;
        /** {@link Churn#RANDOM_STORM} only: max ms between chaos rounds (lower = more aggressive). */
        @Builder.Default
        final int chaosFrequencyMs = DEFAULT_CHAOS_FREQUENCY;
        /** {@link Churn#SCRIPTED_ROUNDS} only: number of stop/restart rounds. */
        @Builder.Default
        final int scriptedToggleRounds = 0;
        /**
         * Outer bound on the whole completion await. Deliberately generous - it exists to stop a
         * runaway build, and must never be the binding constraint: the meaningful failure is the
         * stall detector below, which fires long before this.
         */
        @Builder.Default
        final Duration completionCeiling = ofMinutes(5);
        /**
         * Stall window for correctness profiles: while work remains, the fleet-wide consumed count
         * must advance within this, or the run fails as NO_PROGRESS with a full instance-state dump.
         * Null selects the capacity profiles' legacy detector (a {@link ProgressTracker} allowing 11
         * consecutive progress-free 1s checks) unchanged, preserving their measured baseline.
         */
        final Duration noProgressWindow;
        /** How long PC-0 gets to consume its first records (group formation + initial fetch). */
        @Builder.Default
        final Duration initialConsumeWindow = ofSeconds(10);
    }

    @ParameterizedTest
    @EnumSource(ProcessingOrder.class)
    void consumeWithMultipleInstancesPeriodicConsumerSync(ProcessingOrder order) {
        numPartitions = 2;
        runScenario(Scenario.builder()
                .commitMode(CommitMode.PERIODIC_CONSUMER_SYNC)
                .order(order)
                .messageCount((order == PARTITION) ? 100 : 1000)
                .instances(2)
                .build());
    }

    @ParameterizedTest
    @EnumSource(ProcessingOrder.class)
    void consumeWithMultipleInstancesPeriodicConsumerAsynchronous(ProcessingOrder order) {
        numPartitions = 2;
        runScenario(Scenario.builder()
                .commitMode(PERIODIC_CONSUMER_ASYNCHRONOUS)
                .order(order)
                .messageCount((order == PARTITION) ? 100 : 1000)
                .instances(2)
                .build());
    }

    /**
     * The gating CORRECTNESS profile for rebalance churn: a modest fleet (3 instances, 8 partitions)
     * under a deterministic, event-anchored churn schedule - the correctness twin of the
     * {@link #largeNumberOfInstances() capacity measurement}, which keeps the random storm and lives
     * in the performance lane.
     * <p>
     * <b>Why this is not simply {@code largeNumberOfInstances} with smaller numbers:</b> a scaled-down
     * copy of a probabilistic test inherits the same non-determinism at lower probability - a 99%
     * test instead of an 80% one, which is worse, because it fails rarely enough to be dismissed as
     * noise. Determinism here is by construction, with every probabilistic ingredient removed rather
     * than diluted:
     * <ul>
     *   <li><b>No random churn</b>: {@link Churn#SCRIPTED_ROUNDS} - a fixed number of stop/restart
     *   rounds, round-robin victims, each phase gated on an observed progress event before the next
     *   begins. No {@code Math.random()} anywhere in the schedule.</li>
     *   <li><b>No producer race</b>: all records are pre-produced and acked before churn starts.</li>
     *   <li><b>No wall-clock completion deadline</b>: the assertion is progress - the consumed count
     *   must advance within {@link ProgressProbe#NO_PROGRESS_WINDOW} while work remains (the chaos
     *   suite's NO_PROGRESS model, reused rather than reinvented). A slow machine keeps passing; a
     *   genuine stall fails within the window, with a full instance-state dump.</li>
     *   <li><b>No broker contention</b>: {@link BrokerIntegrationTest#resetKafkaContainer() fresh
     *   container}, so leftover topics/groups/metadata from earlier tests (or an earlier reused
     *   container) are not a confound. NB the astubbs#68 caveat in the class javadoc: uncontended is
     *   correct for THIS arm only.</li>
     * </ul>
     * Config (cooperative-sticky assignor, async commit, unordered) mirrors the capacity profile, so
     * this exercises the same code paths as the confluentinc#857 investigation - one leave and one
     * join rebalance per round, verified stall-free, instead of a storm whose convergence is
     * probabilistic.
     * <p>
     * <b>Determinism evidence</b>: 17/17 consecutive local passes at these parameters (2026-08-18,
     * Docker/TestContainers, 16 solo runs of ~45-53s each plus one inside a full-class run - the run
     * log is in the PR that split this profile out). If this test fails in CI, treat it as a real
     * regression signal, not a flake: there is no random input to blame, and the failure names the
     * stalled phase.
     */
    @Test
    void scriptedChurnRoundsCompleteWithoutStall() {
        numPartitions = 8;
        resetKafkaContainer(); // uncontended broker - contention is a confound for correctness, not the subject
        runScenario(Scenario.builder()
                .commitMode(PERIODIC_CONSUMER_ASYNCHRONOUS)
                .order(UNORDERED)
                .messageCount(30_000)
                .instances(3)
                .pollDelayMs(25) // throttle so work genuinely remains across all churn rounds
                .cooperativeAssignor(true)
                .churn(Churn.SCRIPTED_ROUNDS)
                .scriptedToggleRounds(4)
                .noProgressWindow(ProgressProbe.NO_PROGRESS_WINDOW)
                .initialConsumeWindow(ofSeconds(60)) // fresh container: allow slower first group formation
                .build());
    }

    /**
     * CAPACITY measurement (performance lane - its pass RATE over runs is the output, and a single
     * red run is not a verdict): 12 PC instances on 80 partitions with an aggressive chaos monkey
     * toggling up to 6 of 11 secondary instances every 0-500ms. PC-0 is never toggled and should
     * always be alive. The deterministic correctness twin that gates merges is
     * {@link #scriptedChurnRoundsCompleteWithoutStall()}.
     * <p>
     * Originally created to reproduce state and concurrency issues (confluentinc#188,
     * confluentinc#189), re-enabled for the confluentinc#857 investigation.
     * <p>
     * <b>What the test does:</b>
     * <ol>
     *   <li>Pre-produces 30% of 500k messages, starts PC-0, waits for it to consume</li>
     *   <li>Starts 11 more PCs + a background producer for the remaining 70%</li>
     *   <li>Chaos monkey continuously toggles (stop/start) random secondary instances</li>
     *   <li>Waits up to 5 minutes for ALL 500k keys to be consumed by any instance</li>
     *   <li>Fails if no progress is made for 11 consecutive 1-second checks</li>
     * </ol>
     * <p>
     * <b>Measured output: the pass rate (last measured ~90%; 80%+ expected).</b> This test
     * deliberately pushes the Kafka consumer group rebalance protocol to its limits, so the rate is
     * a measurement of how much churn the stack survives, not a gate. The residual failure occurs
     * when rapid membership changes prevent the group coordinator from completing partition
     * assignment (consumers show assignedPartitions=0). This is documented Kafka behaviour
     * under extreme churn, not a PC bug — all PC-internal issues have been fixed.
     * If the pass rate drops materially, reassess: a new PC bug may have been introduced.
     * <p>
     * <b>Corollary, and read it before backing the parameters off: the paragraph above is asserted,
     * never measured.</b> No experiment separates "the group coordinator cannot converge at this
     * churn rate" from "PC has a defect that only appears at this churn rate" — both look identical
     * from outside, as instances alive with an empty assignment and no progress. That matters
     * because the obvious response to a flaky stress test is to reduce the churn until it passes,
     * and if any part of the residual is PC's, that <em>hides</em> a defect rather than removing a
     * confound. It is the same shape that let the confluentinc#857 deadlock survive four months:
     * astubbs#68 gave every test an uncontended broker, the suite went green, and the defect was
     * untouched. What would settle it is a control arm — the same churn against a plain
     * KafkaConsumer group with no PC in the path. Until then, do NOT reduce this profile's churn:
     * its residual failure rate is the baseline that investigation measures against.
     * TODO(refactor): settle the residual-failure attribution — see
     * docs/inflight/test-largenumberofinstances-residual-failures-unmeasured.md
     * <p>
     * <b>Fixes applied (from confluentinc#857 investigation):</b>
     * <ul>
     *   <li>commitCommand deadlock — ReentrantLock.tryLock() in onPartitionsRevoked</li>
     *   <li>Non-blocking stopAsync() in chaos monkey — prevents 30-40s close() freeze</li>
     *   <li>ThreadConfinedConsumer wrapper — runtime thread-safety enforcement</li>
     *   <li>Raw consumer field removed from PC — all access via ConsumerManager/DI</li>
     *   <li>ArchUnit rules — compile-time consumer field isolation</li>
     *   <li>Multiple defensive fixes (counter adjustment, throttle reset, lifecycle guard)</li>
     * </ul>
     * <p>
     * For the full investigation history, see branch {@code bugs/857-paused-consumption-multi-consumers-bug}
     * and {@code docs/BUG_857_INVESTIGATION.md (deleted 2026-08-18; retrieve with `git show 262629aab:docs/BUG_857_INVESTIGATION.md`)}.
     *
     * @see <a href="https://github.com/confluentinc/parallel-consumer/issues/857">#857</a>
     */
    @Tag("performance")
    @Test
    void largeNumberOfInstances() {

        numPartitions = 80;
        // Use CooperativeStickyAssignor — under the eager (Range) protocol, rapid membership
        // changes restart the JoinGroup phase from scratch, leaving all consumers with
        // assignment=[] indefinitely. Cooperative rebalancing lets consumers keep their
        // existing assignments during rebalance. See confluentinc#857 investigation.
        runScenario(Scenario.builder()
                .commitMode(PERIODIC_CONSUMER_ASYNCHRONOUS)
                .order(UNORDERED)
                .messageCount(500000)
                .instances(12)
                .preProduceFraction(0.3)
                .pollDelayMs(1)
                .cooperativeAssignor(true)
                .build());
    }

    /**
     * Variant of {@link #largeNumberOfInstances()} using CooperativeStickyAssignor, which is the assignor
     * that issue confluentinc#857 reporters say makes the bug more visible. Cooperative rebalancing revokes and assigns
     * partitions in separate callbacks, creating a wider window for stale container races.
     * <p>
     * Uses parameters closer to the production environments reported in confluentinc#857: 30 partitions, 4 consumers.
     */
    @Tag("performance")
    @Test
    void cooperativeStickyRebalanceShouldNotStall() {

        numPartitions = 30;
        runScenario(Scenario.builder()
                .commitMode(PERIODIC_CONSUMER_ASYNCHRONOUS)
                .order(UNORDERED)
                .messageCount(100_000)
                .instances(4)
                .preProduceFraction(0.3)
                .pollDelayMs(1)
                .cooperativeAssignor(true)
                .chaosFrequencyMs(3000) // gentle chaos — let group settle between rebalances
                .build());
    }

    /**
     * Gentler version of {@link #largeNumberOfInstances()} — toggles only 1 instance at a time with a 3-second
     * cooldown between rounds. This lets the consumer group settle between rebalances, isolating any PC-internal
     * bugs from the rebalance storm effect seen in the aggressive test.
     * <p>
     * If this test passes but {@link #largeNumberOfInstances()} fails, the issue is rebalance storm tolerance,
     * not a PC state management bug.
     */
    @Tag("performance")
    @Test
    void gentleChaosRebalance() {

        numPartitions = 30;
        runScenario(Scenario.builder()
                .commitMode(PERIODIC_CONSUMER_ASYNCHRONOUS)
                .order(UNORDERED)
                .messageCount(200_000)
                .instances(6)
                .preProduceFraction(0.5)
                .pollDelayMs(1)
                .chaosFrequencyMs(3000) // 3 seconds between chaos rounds — lets the group settle
                .build());
    }

    ProgressBar overallProgress;
    Set<String> overallConsumedKeys = new ConcurrentSkipListSet<>();

    @SneakyThrows
    private void runScenario(Scenario scenario) {
        String inputName = setupTopic(this.getClass().getSimpleName() + "-input-" + RandomUtils.nextInt());

        overallProgress = ProgressBarUtils.getNewMessagesBar("overall", log, scenario.messageCount);

        ExecutorService pcExecutor = Executors.newWorkStealingPool();

        var sendingProgress = ProgressBarUtils.getNewMessagesBar("sending", log, scenario.messageCount);

        ManagedPCInstance.Config pcConfig = ManagedPCInstance.Config.builder()
                .maxPoll(scenario.maxPoll)
                .commitMode(scenario.commitMode)
                .order(scenario.order)
                .inputTopic(inputName)
                .pollDelayMs(scenario.pollDelayMs)
                .useCooperativeAssignor(scenario.cooperativeAssignor)
                .build();

        // pre-produce messages to input-topic
        Set<String> expectedKeys = new ConcurrentSkipListSet<>();
        log.info("Producing {} messages before starting test", scenario.messageCount);
        List<Future<RecordMetadata>> sends = new ArrayList<>();
        int preProduceCount = (int) (scenario.messageCount * scenario.preProduceFraction);
        try (Producer<String, String> kafkaProducer = getKcu().createNewProducer(false)) {
            for (int i = 0; i < preProduceCount; i++) {
                String key = "key-" + i;
                Future<RecordMetadata> send = kafkaProducer.send(new ProducerRecord<>(inputName, key, "value-" + i), (meta, exception) -> {
                    if (exception != null) {
                        log.error("Error sending, ", exception);
                    }
                    sendingProgress.step();
                });
                sends.add(send);
                expectedKeys.add(key);
            }
            log.debug("Finished sending test data");
        }

        // make sure we finish sending before next stage
        log.debug("Waiting for broker acks");
        for (Future<RecordMetadata> send : sends) {
            send.get();
        }
        assertThat(sends).hasSizeGreaterThanOrEqualTo(preProduceCount);

        // Submit first parallel-consumer
        log.info("Running first instance of pc");
        ManagedPCInstance pc1 = new ManagedPCInstance(pcConfig, getKcu(), key -> {
            count.incrementAndGet();
            overallProgress.step();
            overallConsumedKeys.add(key);
        });
        pcExecutor.submit(pc1);

        // Wait for first consumer to consume messages, also effectively waits for the group.initial.rebalance.delay.ms (3s by default)
        Awaitility.waitAtMost(scenario.initialConsumeWindow)
                .until(() -> pc1.getConsumedKeys().size() > 1);

        // keep producing more messages in the background
        var sender = new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                // pre-produce messages to input-topic
                log.info("Producing {} messages before starting test", scenario.messageCount);
                try (Producer<String, String> kafkaProducer = getKcu().createNewProducer(false)) {
                    for (int i = preProduceCount; i < scenario.messageCount; i++) {
                        // slow things down just a tad
//                        Thread.sleep(1);
                        String key = "key-" + i;
                        log.debug("sending {}", key);
                        Future<RecordMetadata> send = kafkaProducer.send(new ProducerRecord<>(inputName, key, "value-" + i), (meta, exception) -> {
                            if (exception != null) {
                                log.error("Error sending, ", exception);
                            }
                            sendingProgress.step();
                        });
                        send.get();
                        sends.add(send);
                        expectedKeys.add(key);
                    }
                    log.info("Finished sending test data");
                }
            }
        };
        pcExecutor.submit(sender);

        // start more PCs
        var secondaryPcs = Collections.synchronizedList(IntStream.range(1, scenario.instances)
                .mapToObj(value -> {
                            try {
                                int jitterRangeMs = 2;
                                Thread.sleep((int) (Math.random() * jitterRangeMs)); // jitter pc start
                            } catch (InterruptedException e) {
                                log.error(e.getMessage(), e);
                            }
                            log.info("Running pc instance {}", value);
                            ManagedPCInstance instance = new ManagedPCInstance(pcConfig, getKcu(), key -> {
                                count.incrementAndGet();
                                overallProgress.step();
                                overallConsumedKeys.add(key);
                            });
                            pcExecutor.submit(instance);
                            return instance;
                        }
                ).collect(Collectors.toList()));
        final List<ManagedPCInstance> allPCRunners = Collections.synchronizedList(new ArrayList<>());
        allPCRunners.add(pc1);
        allPCRunners.addAll(secondaryPcs);

        switch (scenario.churn) {
            case RANDOM_STORM -> submitChaosMonkey(scenario, pcExecutor, secondaryPcs, allPCRunners);
            case SCRIPTED_ROUNDS -> runScriptedChurnRounds(scenario, pcExecutor, secondaryPcs, allPCRunners);
        }

        // wait for all produced messages to be processed
        Assertions.useRepresentation(new TrimListRepresentation());
        var failureMessage = msg("All keys sent to input-topic should be processed, within time (expected: {} commit: {} order: {} max poll: {})",
                scenario.messageCount, scenario.commitMode, scenario.order, scenario.maxPoll);
        // capacity profiles keep the legacy detector (11 consecutive progress-free 1s checks) so
        // their measured pass-rate baseline is undisturbed; correctness profiles use the sliding
        // NO_PROGRESS watermark (see Scenario#noProgressWindow)
        ProgressTracker progressTracker = new ProgressTracker(count);
        ProgressWatermark watermark = new ProgressWatermark(scenario.noProgressWindow, count.get());
        try {
            waitAtMost(scenario.completionCeiling)
                    // dynamic reason support still waiting https://github.com/awaitility/awaitility/issues/240
                    .failFast("A PC has died - check logs", () -> !noneHaveFailed(allPCRunners)) // dynamic reason requires https://github.com/awaitility/awaitility/issues/240
                    .alias(failureMessage)
                    .pollInterval(1, SECONDS)
                    .untilAsserted(() -> {
                        log.trace("Processed-count: {}", getAllConsumedKeys(allPCRunners).size());
                        boolean stalled = scenario.noProgressWindow == null
                                ? progressTracker.hasProgressNotBeenMade()
                                : watermark.stalledBeyondWindow(count.get());
                        if (stalled) {
                            // Dump full state of every PC instance to diagnose the stall
                            dumpInstanceState(allPCRunners);
                            expectedKeys.removeAll(getAllConsumedKeys(allPCRunners));
                            throw scenario.noProgressWindow == null
                                    ? progressTracker.constructError(msg("No progress, missing keys: {}.", expectedKeys))
                                    : new RuntimeException(msg("NO_PROGRESS: consumed count stuck at {} beyond the {} watermark window, missing keys: {}.",
                                    count.get(), scenario.noProgressWindow, expectedKeys));
                        }
                        SoftAssertions all = new SoftAssertions();
                        all.assertThat(overallConsumedKeys.containsAll(expectedKeys)).as("contains all: all expected are consumed at least once").isTrue();

                        // is this redundant? containsAll means has size => always true
                        // NB: Re-balance causes re-processing, and this is probably expected. Leaving test like this anyway
                        all.assertThat(overallConsumedKeys).as("size: all expected are consumed only once").hasSizeGreaterThanOrEqualTo(expectedKeys.size());

                        all.assertAll();
                    });
        } catch (Throwable error) {
            // this should be replaceable with dynamic reason generation: https://github.com/awaitility/awaitility/issues/240
            List<Exception> exceptions = checkForFailure(allPCRunners);
            if (error instanceof TerminalFailureException) {
                Optional<Exception> any = exceptions.stream().findAny();
                String message = msg("{} \n Terminal failure in one or more of the PCs. Reported exception states are: {} \n {}", failureMessage, exceptions, error);
                throw new RuntimeException(message, any.orElse(null));
            } else {
                String message = msg("{} \n Assertion error. PC reported exception states: {} \n {}", failureMessage, exceptions, error);
                throw new RuntimeException(message, error);
            }
        } finally {
            overallProgress.close();
            sendingProgress.close();
        }

        allPCRunners.forEach(ManagedPCInstance::close);

        assertThat(pc1.getConsumedKeys()).hasSizeGreaterThan(0);
        assertThat(getAllConsumedKeys(secondaryPcs))
                .as("Second PC should have taken over some of the work and consumed some records")
                .hasSizeGreaterThan(0);

        pcExecutor.shutdown();

        Collection<?> duplicates = toCollection(StandardComparisonStrategy.instance()
                .duplicatesFrom(getAllConsumedKeys(allPCRunners)));
        log.info("Duplicate consumed keys (at least one is expected due to the rebalance): {}", duplicates);
        double percentageDuplicateTolerance = 0.2;
        assertThat(duplicates)
                .as("There should be few duplicate keys")
                .hasSizeLessThan((int) (scenario.messageCount * percentageDuplicateTolerance)); // in some env, there are a lot more. i.e. Jenkins running parallel suits


    }

    /** The original random chaos monkey - {@link Churn#RANDOM_STORM}. Randomly stops and starts PCs. */
    private void submitChaosMonkey(Scenario scenario, ExecutorService pcExecutor,
                                   List<ManagedPCInstance> secondaryPcs, List<ManagedPCInstance> allPCRunners) {
        var chaosMonkey = new Runnable() {
            @Override
            public void run() {
                try {
                    while (noneHaveFailed(allPCRunners)) {
                        Thread.sleep((int) (scenario.chaosFrequencyMs * Math.random()));
                        boolean makeChaos = Math.random() > 0.2; // small chance it will let the test do a run without chaos
//                        boolean makeChaos = true;
                        if (makeChaos) {
                            int size = secondaryPcs.size();
                            int numberToMessWith = (int) (Math.random() * size * 0.6);
                            if (numberToMessWith > 0) {
                                log.info("Will mess with {} instances", numberToMessWith);
                                IntStream.range(0, numberToMessWith).forEach(value -> {
                                    int instanceToGet = (int) ((size - 1) * Math.random());
                                    ManagedPCInstance victim = secondaryPcs.get(instanceToGet);
                                    log.info("Victim is instance: " + victim.getInstanceId());
                                    victim.toggle(pcExecutor);
                                });
                            }
                        }
                    }
                } catch (Throwable e) {
                    log.error("Error in chaos loop", e);
                    throw new RuntimeException(e);
                }
                log.error("Ending chaos as a PC instance has died");
            }
        };
        pcExecutor.submit(chaosMonkey);
    }

    /**
     * {@link Churn#SCRIPTED_ROUNDS}: the deterministic churn schedule. Runs on the test thread, so
     * by the time it returns every scheduled rebalance has already happened and been verified
     * stall-free - the completion await that follows only has to see the fleet finish the work.
     * <p>
     * Each round: pick the next secondary round-robin, stop it synchronously (leave-group
     * rebalance), require fleet progress within the watermark window, restart it (join rebalance),
     * require progress again. Every wait is for an observed event; there are no sleeps and no
     * randomness. A round where the work is already complete passes trivially (the churn still
     * happens, but there is nothing left to stall) - completion is checked, never awaited.
     */
    private void runScriptedChurnRounds(Scenario scenario, ExecutorService pcExecutor,
                                        List<ManagedPCInstance> secondaryPcs, List<ManagedPCInstance> allPCRunners) {
        // Barrier: every secondary must have joined and consumed before churn begins, so a round's
        // stop is a real leave-group of an active member, not a no-op on a still-starting instance
        waitAtMost(scenario.initialConsumeWindow)
                .alias("all secondaries have joined the group and consumed at least one record")
                .failFast("A PC has died - check logs", () -> !noneHaveFailed(allPCRunners))
                .until(() -> secondaryPcs.stream().noneMatch(pc -> pc.getConsumedKeys().isEmpty()));

        for (int round = 0; round < scenario.scriptedToggleRounds; round++) {
            ManagedPCInstance victim = secondaryPcs.get(round % secondaryPcs.size());
            log.info("Scripted churn round {}: stopping instance {}", round, victim.getInstanceId());
            victim.stop(); // synchronous close - the leave-group rebalance has begun when this returns
            awaitProgressOrCompletion(scenario, allPCRunners,
                    msg("round {}: survivors after stopping instance {}", round, victim.getInstanceId()));

            log.info("Scripted churn round {}: restarting instance {}", round, victim.getInstanceId());
            boolean submitted = victim.start(pcExecutor); // join rebalance
            assertThat(submitted)
                    .as("scripted restart of instance %s must submit - nothing else races for the start slot", victim.getInstanceId())
                    .isTrue();
            awaitProgressOrCompletion(scenario, allPCRunners,
                    msg("round {}: group after restarting instance {}", round, victim.getInstanceId()));
        }
    }

    /**
     * Progress, not completion: the fleet-wide consumed count must advance beyond its current value
     * within {@link Scenario#noProgressWindow} - unless the work is already complete, which passes
     * trivially. A timeout here is a NO_PROGRESS verdict (a genuine stall), never "the machine was
     * slow": a slow machine still advances the count.
     */
    private void awaitProgressOrCompletion(Scenario scenario, List<ManagedPCInstance> allPCRunners, String phase) {
        long before = count.get();
        try {
            waitAtMost(scenario.noProgressWindow)
                    .alias(phase)
                    .failFast("A PC has died - check logs", () -> !noneHaveFailed(allPCRunners))
                    .until(() -> count.get() > before || overallConsumedKeys.size() >= scenario.messageCount);
        } catch (ConditionTimeoutException stall) {
            dumpInstanceState(allPCRunners);
            throw new AssertionError(msg("NO_PROGRESS during scripted churn ({}): consumed count stuck at {} for {} " +
                    "with work remaining - see the instance state dump above", phase, before, scenario.noProgressWindow), stall);
        }
    }

    /**
     * Sliding stall watermark for the completion await, modeled on
     * {@link ProgressProbe#NO_PROGRESS_WINDOW the chaos suite's progress-watermark probe} (which is
     * not reused directly here only because its consumed-count sampling runs on its own thread and
     * gates via violation collection; this is the same invariant expressed inside an Awaitility
     * poll). Unlike {@link ProgressTracker}'s duration mode - whose deadline is fixed at
     * construction - the window slides: it measures time since the count last advanced.
     */
    private static class ProgressWatermark {
        private final Duration window;
        private long lastSeen;
        private Instant lastAdvance = Instant.now();

        ProgressWatermark(Duration window, long initialCount) {
            this.window = window;
            this.lastSeen = initialCount;
        }

        /** @return true when the count has not advanced for longer than the window */
        boolean stalledBeyondWindow(long current) {
            if (current > lastSeen) {
                lastSeen = current;
                lastAdvance = Instant.now();
                return false;
            }
            return Duration.between(lastAdvance, Instant.now()).compareTo(window) > 0;
        }
    }

    /**
     * Dump the internal state of every PC instance when a stall is detected.
     * This tells us exactly what each component thinks is happening:
     * - Is the PC alive or dead?
     * - How many records are queued in shards vs out for processing?
     * - What's the partition assignment?
     * - Is the consumer paused?
     * - What does the WorkManager think about incomplete offsets?
     */
    private void dumpInstanceState(List<ManagedPCInstance> instances) {
        log.error("=== STALL DETECTED — dumping all instance state ===");
        for (var instance : instances) {
            var pc = instance.getParallelConsumer();
            if (pc == null) {
                log.error("  Instance {}: PC is null (never started?), started={}", instance.getInstanceId(), instance.isStarted());
                continue;
            }
            try {
                var wm = pc.getWm();
                // Check if the shard manager has any processing shards at all
                var sm = wm.getSm();
                long totalWorkTracked = sm.getNumberOfWorkQueuedInShardsAwaitingSelection();
                boolean hasIncompletes = wm.hasIncompleteOffsets();

                log.error("  Instance {}: closed/failed={}, failureCause={}, started={}, " +
                                "assignedPartitions={}, queuedInShards={}, outForProcessing={}, " +
                                "incompleteOffsets={}, hasIncompletes={}, " +
                                "pausedPartitions={}, consumedKeys={}",
                        instance.getInstanceId(),
                        pc.isClosedOrFailed(),
                        pc.getFailureCause() != null ? pc.getFailureCause().getMessage() : "none",
                        instance.isStarted(),
                        pc.getAssignmentSize(),
                        totalWorkTracked,
                        wm.getNumberRecordsOutForProcessing(),
                        wm.getNumberOfIncompleteOffsets(),
                        hasIncompletes,
                        pc.getPausedPartitionSize(),
                        instance.getConsumedKeys().size()
                );
            } catch (Exception e) {
                log.error("  Instance {}: error dumping state: {}", instance.getInstanceId(), e.getMessage(), e);
            }
        }
        log.error("=== END STATE DUMP ===");
    }

    private boolean noneHaveFailed(List<ManagedPCInstance> pcs) {
        return checkForFailure(pcs).isEmpty();
    }

    private List<Exception> checkForFailure(List<ManagedPCInstance> pcs) {
        return pcs.stream().filter(instance -> {
            var pc = instance.getParallelConsumer();
            if (pc == null) return false; // hasn't started
            if (!pc.isClosedOrFailed()) return false; // still open
            boolean failed = pc.getFailureCause() != null; // actually failed
            return failed;
        }).map(instance -> instance.getParallelConsumer().getFailureCause()).collect(Collectors.toList());
    }

    List<String> getAllConsumedKeys(List<ManagedPCInstance> instances) {
        return instances.stream()
                .flatMap(instance -> instance.getConsumedKeys().stream())
                .collect(Collectors.toList());
    }

}
