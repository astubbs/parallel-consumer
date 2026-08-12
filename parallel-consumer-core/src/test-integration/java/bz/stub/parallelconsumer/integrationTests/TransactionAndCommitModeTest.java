package bz.stub.parallelconsumer.integrationTests;
/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.ArgumentSetsBuilder;
import bz.stub.parallelconsumer.internal.utils.ProgressBarUtils;
import bz.stub.parallelconsumer.internal.utils.ProgressTracker;
import bz.stub.parallelconsumer.internal.utils.TrimListRepresentation;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.internal.ConsumerOffsetCommitter;
import bz.stub.parallelconsumer.internal.OffsetCommitter;
import bz.stub.parallelconsumer.internal.ProducerManager;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.cartesian.ArgumentSets;
import org.junitpioneer.jupiter.cartesian.CartesianTest;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;
import static bz.stub.parallelconsumer.AbstractParallelEoSStreamProcessorTestBase.defaultTimeout;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.*;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.waitAtMost;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Originally created to reproduce bug confluentinc#25 https://github.com/confluentinc/parallel-consumer/issues/25 which was a known
 * issue with multi-threaded use of the {@link KafkaProducer}.
 * <p>
 * After fixing multi threading issues, using Producer transactions was made optional, and this test grew to uncover
 * several issues with the new implementation of committing offsets through the {@link KafkaConsumer}.
 *
 * @see OffsetCommitter
 * @see ConsumerOffsetCommitter
 * @see ProducerManager
 */
@Tag("transactions")
@Slf4j
class TransactionAndCommitModeTest extends BrokerIntegrationTest<String, String> {

    int LOW_MAX_POLL_RECORDS_CONFIG = 1;
    int DEFAULT_MAX_POLL_RECORDS_CONFIG = 500;
    int HIGH_MAX_POLL_RECORDS_CONFIG = 10_000;

    // is sensitive to changes in metadata size
    @CartesianTest
    @CartesianTest.MethodFactory("enumSets")
    void testDefaultMaxPoll(CommitMode commitMode, ProcessingOrder order) {
        int numMessages = 5000;
        if (order.equals(PARTITION))
            numMessages = 1000; // much slower, do less
        runTest(DEFAULT_MAX_POLL_RECORDS_CONFIG, commitMode, order, numMessages);
    }

    @Test
    void testDefaultMaxPollConsumerSyncSlow() {
        runTest(DEFAULT_MAX_POLL_RECORDS_CONFIG, PERIODIC_CONSUMER_SYNC, UNORDERED);
    }

    static ArgumentSets enumSets() {
        return ArgumentSetsBuilder.builder().add(CommitMode.class)
                .add(ProcessingOrder.class).build();
    }

    @RepeatedTest(5)
    void testTransactionalDefaultMaxPoll() {
        runTest(DEFAULT_MAX_POLL_RECORDS_CONFIG, PERIODIC_TRANSACTIONAL_PRODUCER, KEY);
    }

    // is sensitive to changes in metadata size
//    @ResourceLock(value = OffsetMapCodecManager.METADATA_DATA_SIZE_RESOURCE_LOCK, mode = READ)
    @CartesianTest
    @CartesianTest.MethodFactory("enumSets")
    public void testLowMaxPoll(CommitMode commitMode, ProcessingOrder order) {
        int numMessages = 5000;
        if (order.equals(PARTITION))
            numMessages = 1000; // much slower
        runTest(LOW_MAX_POLL_RECORDS_CONFIG, commitMode, order, numMessages);
    }

    @CartesianTest
    @CartesianTest.MethodFactory("enumSets")
    public void testHighMaxPollEnum(CommitMode commitMode, ProcessingOrder order) {
        int numMessages = 10000;
        if (order.equals(PARTITION))
            numMessages = 1000; // much slower

        runTest(HIGH_MAX_POLL_RECORDS_CONFIG, commitMode, order, numMessages);
    }

    private void runTest(int maxPoll, CommitMode commitMode, ProcessingOrder order) {
        int expectedMessageCount = 30_000;
        runTest(maxPoll, commitMode, order, expectedMessageCount);
    }

    /**
     * The concurrency this test runs at. 64 is the gating value, and it was an <em>increase</em> for
     * stability, not a reduction.
     * <p>
     * The ladder around it - {@code 2}, {@code 100}, {@code 1000} - arrived already commented in
     * {@code 2b0ab66b} (2020-11-27) beside the live {@code numThreads = 16}, and was deleted in
     * {@code e67d8b89}. None of those values was ever live, so the ladder was the only record of the
     * concurrencies anyone had run this matrix at. Reach them without editing the file:
     *
     * <pre>./mvnw verify -Pci -Dtransactions.concurrency=2</pre>
     *
     * The low rung is the interesting one: it is where an ordering assumption that high concurrency
     * was papering over would show up.
     */
    private static int concurrency() {
        return Integer.getInteger("transactions.concurrency", GATING_CONCURRENCY);
    }

    static final int GATING_CONCURRENCY = 64;

    /**
     * Scales the shared {@link bz.stub.parallelconsumer.AbstractParallelEoSStreamProcessorTestBase#defaultTimeout}
     * by how far below the gating concurrency this run is, because the same message count takes
     * proportionally longer with fewer threads. Without this the ladder's low rungs would be
     * selectable but guaranteed to time out - reachable in name only.
     * <p>
     * At the gating concurrency the multiplier is exactly one, so the default run's deadline is
     * unchanged. Raising concurrency never shortens it.
     */
    private static Duration timeoutFor(int threads) {
        return defaultTimeout.multipliedBy(Math.max(1, GATING_CONCURRENCY / Math.max(1, threads)));
    }

    @SneakyThrows
    private void runTest(int maxPoll, CommitMode commitMode, ProcessingOrder order, int expectedCount) {
        String inputName = setupTopic(this.getClass().getSimpleName() + "-input-" + RandomUtils.nextInt());
        String outputName = setupTopic(this.getClass().getSimpleName() + "-output-" + RandomUtils.nextInt());

        int expectedMessageCount = expectedCount;

        ProgressBar bar = ProgressBarUtils.getNewMessagesBar(log, expectedMessageCount);

        // pre-produce messages to input-topic
        List<String> expectedKeys = new ArrayList<>();
        log.info("Producing {} messages before starting test", expectedMessageCount);
        List<Future<RecordMetadata>> sends = new ArrayList<>();
        try (Producer<String, String> kafkaProducer = getKcu().createNewProducer(false)) {
            for (int i = 0; i < expectedMessageCount; i++) {
                String key = "key-" + i;
                Future<RecordMetadata> send = kafkaProducer.send(new ProducerRecord<>(inputName, key, "value-" + i), (meta, exception) -> {
                    if (exception != null) {
                        log.error("Error sending, ", exception);
                    }
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
        assertThat(sends).hasSize(expectedMessageCount);

        // run parallel-consumer
        log.debug("Starting test");
        KafkaProducer<String, String> newProducer = getKcu().createNewProducer(commitMode);

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPoll);
        KafkaConsumer<String, String> newConsumer = getKcu().createNewConsumer(true, consumerProps);

        // increased PC concurrency - improves test stability and performance.
        int numThreads = concurrency();
        var pc = new ParallelEoSStreamProcessor<String, String>(ParallelConsumerOptions.<String, String>builder()
                .ordering(order)
                .consumer(newConsumer)
                .producer(newProducer)
                .commitMode(commitMode)
                .maxConcurrency(numThreads)
                .build());
        pc.subscribe(of(inputName));

        pc.setTimeBetweenCommits(ofSeconds(1));

        // sanity
        TopicPartition tp = new TopicPartition(inputName, 0);
        Map<TopicPartition, Long> beginOffsets = newConsumer.beginningOffsets(of(tp));
        Map<TopicPartition, Long> endOffsets = newConsumer.endOffsets(of(tp));
        assertThat(endOffsets).containsEntry(tp, ((long) expectedMessageCount));
        assertThat(beginOffsets.get(tp)).isZero();


        List<String> consumedKeys = Collections.synchronizedList(new ArrayList<>());
        List<String> producedKeysAcknowledged = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger processedCount = new AtomicInteger(0);
        AtomicInteger producedCount = new AtomicInteger(0);

        pc.pollAndProduce(record -> {
                    log.debug("Polled {}", record.offset());
                    consumedKeys.add(record.key());
                    processedCount.incrementAndGet();
                    return new ProducerRecord<>(outputName, record.key(), "data");
                }, consumeProduceResult -> {
                    log.debug("Produced {}", consumeProduceResult.getOut());
                    producedCount.incrementAndGet();
                    producedKeysAcknowledged.add(consumeProduceResult.getIn().key());
                    bar.step();
                }
        );

        // wait for all pre-produced messages to be processed and produced
        Assertions.useRepresentation(new TrimListRepresentation());

        // Rounds are deliberately not used here: ProgressTracker rejects being given both a round
        // count and a timeout, and this test tracks by duration. The open question of whether any
        // round without progress should be tolerated is recorded in docs/refactoring.md.
        Duration deadline = timeoutFor(numThreads);
        ProgressTracker progressTracker = new ProgressTracker(processedCount, null, deadline);
        var failureMessage = msg("All keys sent to input-topic should be processed and produced, within time (expected: {} commit: {} order: {} max poll: {})",
                expectedMessageCount, commitMode, order, maxPoll);
        try {
            waitAtMost(deadline)
                    // dynamic reason support still waiting
                    // https://github.com/awaitility/awaitility/pull/193#issuecomment-873116199
                    // https://github.com/confluentinc/parallel-consumer/issues/199
                    .failFast("PC died, check logs.",
                            () -> pc.isClosedOrFailed()
                                    || producedCount.get() > expectedMessageCount)
//                            () -> {
//                                if (pc.isClosedOrFailed())
//                                    return pc.getFailureCause();
//                                else
//                                    return new TerminalFailureException(msg("Too many messages? processedCount.get() {} > expectedMessageCount {}",
//                                            producedCount.get(), expectedMessageCount)); // needs fail-fast feature in 4.0.4
//                            })
                    .alias(failureMessage)
                    .untilAsserted(() -> {
                        log.trace("Processed-count: {}, Produced-count: {}", processedCount.get(), producedCount.get());
                        int delta = producedCount.get() - processedCount.get();
                        if (delta == numThreads && progressTracker.getRounds().get() > 1) {
                            log.error("Here we go fishy...");
                        }

                        //
                        progressTracker.checkForProgressExceptionally();

                        //
                        SoftAssertions all = new SoftAssertions();
                        all.assertThat(new ArrayList<>(consumedKeys)).as("all expected are consumed").hasSameSizeAs(expectedKeys);
                        all.assertThat(new ArrayList<>(producedKeysAcknowledged)).as("all consumed are produced ok ").hasSameSizeAs(expectedKeys);
                        all.assertAll();
                    });
        } catch (ConditionTimeoutException e) {
            log.debug("Expected keys (size {})", expectedKeys.size());
            log.debug("Consumed keys ack'd (size {})", consumedKeys.size());
            log.debug("Produced keys (size {})", producedKeysAcknowledged.size());
            expectedKeys.removeAll(consumedKeys);
            log.info("Missing keys from consumed: {}", expectedKeys);
            fail(failureMessage + "\n" + e.getMessage());
        }

        pc.closeDrainFirst();

        assertThat(processedCount.get())
                .as("messages processed and produced by parallel-consumer should be equal")
                .isEqualTo(producedCount.get());

        // sanity
        assertThat(expectedMessageCount).isEqualTo(processedCount.get());
        assertThat(producedKeysAcknowledged).hasSameSizeAs(expectedKeys);
        // todo performance: tighten up progress check (<2)
        assertThat(progressTracker.getHighestRoundCountSeen()).isLessThan(40);
        bar.close();
    }

}
