package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import io.confluent.csid.utils.ProgressBarUtils;
import io.confluent.csid.utils.ProgressTracker;
import io.confluent.csid.utils.TrimListRepresentation;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.internal.StandardComparisonStrategy;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.csid.utils.StringUtils.msg;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.util.IterableUtil.toCollection;
import static org.awaitility.Awaitility.waitAtMost;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Test running with multiple instances of parallel-consumer consuming from topic with two partitions.
 */
@Slf4j
class MultiInstanceRebalanceTest extends BrokerIntegrationTest<String, String> {

    static final int DEFAULT_MAX_POLL = 500;
    AtomicInteger count = new AtomicInteger();

    @ParameterizedTest
    @EnumSource(ProcessingOrder.class)
    void consumeWithMultipleInstancesPeriodicConsumerSync(ProcessingOrder order) {
        runTest(DEFAULT_MAX_POLL, CommitMode.PERIODIC_CONSUMER_SYNC, order);
    }

    @ParameterizedTest
    @EnumSource(ProcessingOrder.class)
    void consumeWithMultipleInstancesPeriodicConsumerAsync(ProcessingOrder order) {
        runTest(DEFAULT_MAX_POLL, CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS, order);
    }

    @SneakyThrows
    private void runTest(int maxPoll, CommitMode commitMode, ProcessingOrder order) {
        numPartitions = 2;
        String inputName = setupTopic(this.getClass().getSimpleName() + "-input-" + RandomUtils.nextInt());

        // pre-produce messages to input-topic
        List<String> expectedKeys = new ArrayList<>();
        int expectedMessageCount = 100;
        log.info("Producing {} messages before starting test", expectedMessageCount);
        List<Future<RecordMetadata>> sends = new ArrayList<>();
        try (Producer<String, String> kafkaProducer = kcu.createNewProducer(false)) {
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

        ProgressBar bar1 = ProgressBarUtils.getNewMessagesBar("PC1", log, expectedMessageCount);
        ProgressBar bar2 = ProgressBarUtils.getNewMessagesBar("PC2", log, expectedMessageCount);

        ExecutorService pcExecutor = Executors.newFixedThreadPool(2);

        // Submit first parallel-consumer
        log.info("Running first instance of pc");
        ParallelConsumerRunnable pc1 = new ParallelConsumerRunnable(maxPoll, commitMode, order, inputName, bar1);
        pcExecutor.submit(pc1);

        // Wait for first consumer to consume messages
        Awaitility.waitAtMost(ofSeconds(10))
                .until(() -> pc1.getConsumedKeys().size() > 10);

        // Submit second parallel-consumer
        log.info("Running second instance of pc");
        ParallelConsumerRunnable pc2 = new ParallelConsumerRunnable(maxPoll, commitMode, order, inputName, bar2);
        pcExecutor.submit(pc2);

        // wait for all pre-produced messages to be processed and produced
        Assertions.useRepresentation(new TrimListRepresentation());
        var failureMessage = msg("All keys sent to input-topic should be processed, within time (expected: {} commit: {} order: {} max poll: {})",
                expectedMessageCount, commitMode, order, maxPoll);
        ProgressTracker progressTracker = new ProgressTracker(count);
        try {
            waitAtMost(ofSeconds(30))
                    // dynamic reason support still waiting https://github.com/awaitility/awaitility/pull/193#issuecomment-873116199
                    // .failFast( () -> pc1.getFailureCause(), () -> pc1.isClosedOrFailed()) // requires https://github.com/awaitility/awaitility/issues/178#issuecomment-734769761
                    .failFast("PC died - check logs", () -> pc1.getParallelConsumer().isClosedOrFailed()) // requires https://github.com/awaitility/awaitility/issues/178#issuecomment-734769761
                    .alias(failureMessage)
                    .pollInterval(1, SECONDS)
                    .untilAsserted(() -> {
                        log.trace("Processed-count: {}", getAllConsumedKeys(pc1, pc2).size());
                        if (progressTracker.hasProgressNotBeenMade()) {
                            expectedKeys.removeAll(getAllConsumedKeys(pc1, pc2));
                            throw progressTracker.constructError(msg("No progress, missing keys: {}.", expectedKeys));
                        }
                        SoftAssertions all = new SoftAssertions();
                        all.assertThat(new ArrayList<>(getAllConsumedKeys(pc1, pc2))).as("all expected are consumed").containsAll(expectedKeys);
                        // NB: Re-balance causes re-processing, and this is probably expected. Leaving test like this anyway
                        all.assertThat(new ArrayList<>(getAllConsumedKeys(pc1, pc2))).as("all expected are consumed only once").hasSizeGreaterThanOrEqualTo(expectedKeys.size());

                        all.assertAll();
                    });
        } catch (ConditionTimeoutException e) {
            fail(failureMessage + "\n" + e.getMessage());
        }

        bar1.close();
        bar2.close();

        pc1.getParallelConsumer().closeDrainFirst();
        pc2.getParallelConsumer().closeDrainFirst();

        assertThat(pc1.consumedKeys).hasSizeGreaterThan(0);
        assertThat(pc2.consumedKeys).as("Second PC should have taken over some of the work and consumed some records")
                .hasSizeGreaterThan(0);

        pcExecutor.shutdown();

        Collection<?> duplicates = toCollection(StandardComparisonStrategy.instance()
                .duplicatesFrom(getAllConsumedKeys(pc1, pc2)));
        log.info("Duplicate consumed keys (at least one is expected due to the rebalance): {}", duplicates);
        assertThat(duplicates)
                .as("There should be few duplicate keys")
                .hasSizeLessThan(10); // in some env, there are a lot more. i.e. Jenkins running parallel suits
    }

    ArrayList<String> getAllConsumedKeys(ParallelConsumerRunnable pc1, ParallelConsumerRunnable pc2) {
        ArrayList<String> keys = new ArrayList<>();
        keys.addAll(pc1.consumedKeys);
        keys.addAll(pc2.consumedKeys);
        return keys;
    }

    int instanceId = 0;

    @Getter
    @RequiredArgsConstructor
    class ParallelConsumerRunnable implements Runnable {

        private final int maxPoll;
        private final CommitMode commitMode;
        private final ProcessingOrder order;
        private final String inputTopic;
        private final ProgressBar bar;
        private ParallelEoSStreamProcessor<String, String> parallelConsumer;
        private final List<String> consumedKeys = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void run() {
            log.info("Running consumer!");

            Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPoll);
            KafkaConsumer<String, String> newConsumer = kcu.createNewConsumer(false, consumerProps);

            this.parallelConsumer = new ParallelEoSStreamProcessor<>(ParallelConsumerOptions.<String, String>builder()
                    .ordering(order)
                    .consumer(newConsumer)
                    .commitMode(commitMode)
                    .maxConcurrency(1)
                    .build());
            parallelConsumer.setMyId(Optional.of("id: " + instanceId));
            instanceId++;

            parallelConsumer.subscribe(of(inputTopic));

            parallelConsumer.poll(record -> {
                        // simulate work
                        try {
                            Thread.sleep(150);
                        } catch (InterruptedException e) {
                            // ignore
                        }
                        count.incrementAndGet();
                        bar.stepBy(1);
                        consumedKeys.add(record.key());
                    }
            );
        }
    }


}
