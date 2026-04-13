package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2026 Confluent, Inc. and contributors
 */

import io.confluent.csid.utils.ProgressBarUtils;
import io.confluent.csid.utils.ProgressTracker;
import io.confluent.csid.utils.TrimListRepresentation;
import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.confluent.parallelconsumer.integrationTests.utils.ManagedPCInstance;
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
import org.awaitility.core.TerminalFailureException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.MDC;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor.MDC_INSTANCE_ID;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.IterableUtil.toCollection;
import static org.awaitility.Awaitility.waitAtMost;

/**
 * Test running with multiple instances of parallel-consumer consuming from topic with two partitions.
 */
//@Isolated // performance sensitive
@Slf4j
public class MultiInstanceRebalanceTest extends BrokerIntegrationTest<String, String> {

    static final int DEFAULT_MAX_POLL = 500;
    public static final int DEFAULT_CHAOS_FREQUENCY = 500;
    public static final int DEFAULT_POLL_DELAY = 150;

    /** Per-test override for chaos frequency (ms). Higher = gentler chaos. */
    int chaosFrequency = DEFAULT_CHAOS_FREQUENCY;
    AtomicInteger count = new AtomicInteger();

    static {
        MDC.put(MDC_INSTANCE_ID, "Test-Thread");
    }

    @ParameterizedTest
    @EnumSource(ProcessingOrder.class)
    void consumeWithMultipleInstancesPeriodicConsumerSync(ProcessingOrder order) {
        numPartitions = 2;
        int expectedMessageCount = (order == PARTITION) ? 100 : 1000;
        int numberOfPcsToRun = 2;
        runTest(DEFAULT_MAX_POLL, CommitMode.PERIODIC_CONSUMER_SYNC, order, expectedMessageCount,
                numberOfPcsToRun, 1.0, DEFAULT_POLL_DELAY, false);
    }

    @ParameterizedTest
    @EnumSource(ProcessingOrder.class)
    void consumeWithMultipleInstancesPeriodicConsumerAsynchronous(ProcessingOrder order) {
        numPartitions = 2;
        int expectedMessageCount = (order == PARTITION) ? 100 : 1000;
        runTest(DEFAULT_MAX_POLL, CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS, order, expectedMessageCount,
                2, 1.0, DEFAULT_POLL_DELAY, false);
    }

    /**
     * Tests with very large numbers of parallel consumer instances to try to reproduce state and concurrency issues
     * (#188, #189, #857).
     * <p>
     * This test takes some time, but seems required in order to expose some race conditions without synthetically
     * creating them. Re-enabled to investigate
     * <a href="https://github.com/confluentinc/parallel-consumer/issues/857">#857</a> — paused consumption after
     * rebalance with multiple consumers. A community member reported this test fails ~50% of runs with
     * "No progress, missing keys: ..." which is consistent with the symptoms in #857.
     * <p>
     * Local testing shows ~80% failure rate (4/5 runs). Stalls at different progress points (17%-74%)
     * confirming a timing-dependent race condition. Tagged as performance test so it runs on the
     * self-hosted runner rather than regular CI.
     */
    @Tag("performance")
    @Test
    void largeNumberOfInstances() {

        numPartitions = 80;
        int numberOfPcsToRun = 12;
        int expectedMessageCount = 500000;
        // Use CooperativeStickyAssignor — under the eager (Range) protocol, rapid membership
        // changes restart the JoinGroup phase from scratch, leaving all consumers with
        // assignment=[] indefinitely. Cooperative rebalancing lets consumers keep their
        // existing assignments during rebalance. See #857 investigation.
        runTest(DEFAULT_MAX_POLL, CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS, ProcessingOrder.UNORDERED, expectedMessageCount,
                numberOfPcsToRun, 0.3, 1, true);
    }

    /**
     * Variant of {@link #largeNumberOfInstances()} using CooperativeStickyAssignor, which is the assignor
     * that issue #857 reporters say makes the bug more visible. Cooperative rebalancing revokes and assigns
     * partitions in separate callbacks, creating a wider window for stale container races.
     * <p>
     * Uses parameters closer to the production environments reported in #857: 30 partitions, 4 consumers.
     */
    @Tag("performance")
    @Test
    void cooperativeStickyRebalanceShouldNotStall() {

        numPartitions = 30;
        int numberOfPcsToRun = 4;
        int expectedMessageCount = 100_000;
        chaosFrequency = 3000; // gentle chaos — let group settle between rebalances
        runTest(DEFAULT_MAX_POLL, CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS, ProcessingOrder.UNORDERED,
                expectedMessageCount, numberOfPcsToRun, 0.3, 1, true);
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
        int numberOfPcsToRun = 6;
        int expectedMessageCount = 200_000;
        chaosFrequency = 3000; // 3 seconds between chaos rounds — lets the group settle
        runTest(DEFAULT_MAX_POLL, CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS, ProcessingOrder.UNORDERED,
                expectedMessageCount, numberOfPcsToRun, 0.5, 1, false);
    }

    ProgressBar overallProgress;
    Set<String> overallConsumedKeys = new ConcurrentSkipListSet<>();

    @SneakyThrows
    private void runTest(int maxPoll, CommitMode commitMode, ProcessingOrder order, int expectedMessageCount,
                         int numberOfPcsToRun, double fractionOfMessagesToPreProduce, int pollDelayMs,
                         boolean useCooperativeAssignor) {
        String inputName = setupTopic(this.getClass().getSimpleName() + "-input-" + RandomUtils.nextInt());

        overallProgress = ProgressBarUtils.getNewMessagesBar("overall", log, expectedMessageCount);

        ExecutorService pcExecutor = Executors.newWorkStealingPool();

        var sendingProgress = ProgressBarUtils.getNewMessagesBar("sending", log, expectedMessageCount);

        ManagedPCInstance.Config pcConfig = ManagedPCInstance.Config.builder()
                .maxPoll(maxPoll)
                .commitMode(commitMode)
                .order(order)
                .inputTopic(inputName)
                .pollDelayMs(pollDelayMs)
                .useCooperativeAssignor(useCooperativeAssignor)
                .build();

        // pre-produce messages to input-topic
        Set<String> expectedKeys = new ConcurrentSkipListSet<>();
        log.info("Producing {} messages before starting test", expectedMessageCount);
        List<Future<RecordMetadata>> sends = new ArrayList<>();
        int preProduceCount = (int) (expectedMessageCount * fractionOfMessagesToPreProduce);
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
        Awaitility.waitAtMost(ofSeconds(10))
                .until(() -> pc1.getConsumedKeys().size() > 1);

        // keep producing more messages in the background
        var sender = new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                // pre-produce messages to input-topic
                log.info("Producing {} messages before starting test", expectedMessageCount);
                try (Producer<String, String> kafkaProducer = getKcu().createNewProducer(false)) {
                    for (int i = preProduceCount; i < expectedMessageCount; i++) {
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
        var secondaryPcs = Collections.synchronizedList(IntStream.range(1, numberOfPcsToRun)
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

        // Randomly start and stop PCs
        var chaosMonkey = new Runnable() {
            @Override
            public void run() {
                try {
                    while (noneHaveFailed(allPCRunners)) {
                        Thread.sleep((int) (chaosFrequency * Math.random()));
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


        // wait for all pre-produced messages to be processed
        Assertions.useRepresentation(new TrimListRepresentation());
        var failureMessage = msg("All keys sent to input-topic should be processed, within time (expected: {} commit: {} order: {} max poll: {})",
                expectedMessageCount, commitMode, order, maxPoll);
        ProgressTracker progressTracker = new ProgressTracker(count);
        try {
            waitAtMost(ofMinutes(5))
                    // dynamic reason support still waiting https://github.com/awaitility/awaitility/issues/240
                    // .failFast( () -> pc1.getFailureCause(), () -> pc1.isClosedOrFailed()) // requires https://github.com/awaitility/awaitility/issues/240
                    .failFast("A PC has died - check logs", () -> !noneHaveFailed(allPCRunners)) // dynamic reason requires https://github.com/awaitility/awaitility/issues/240
                    .alias(failureMessage)
                    .pollInterval(1, SECONDS)
                    .untilAsserted(() -> {
                        log.trace("Processed-count: {}", getAllConsumedKeys(allPCRunners).size());
                        if (progressTracker.hasProgressNotBeenMade()) {
                            // Dump full state of every PC instance to diagnose the stall
                            dumpInstanceState(allPCRunners);
                            expectedKeys.removeAll(getAllConsumedKeys(allPCRunners));
                            throw progressTracker.constructError(msg("No progress, missing keys: {}.", expectedKeys));
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
                .hasSizeLessThan((int) (expectedMessageCount * percentageDuplicateTolerance)); // in some env, there are a lot more. i.e. Jenkins running parallel suits


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
