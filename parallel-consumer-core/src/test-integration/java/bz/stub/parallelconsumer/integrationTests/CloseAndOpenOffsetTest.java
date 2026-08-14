package bz.stub.parallelconsumer.integrationTests;
/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.Range;
import bz.stub.parallelconsumer.FakeRuntimeException;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import bz.stub.parallelconsumer.offsets.OffsetEncoding;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager;
import bz.stub.parallelconsumer.offsets.OffsetSimultaneousEncoder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static bz.stub.parallelconsumer.AbstractParallelEoSStreamProcessorTestBase.defaultTimeout;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.parallel.ResourceAccessMode.READ;

/**
 * Series of tests that check when we close a PC with incompletes encoded, when we open a new one, the correct messages
 * are skipped.
 *
 * @see OffsetMapCodecManager
 */
@Timeout(value = 60)
@Slf4j
class CloseAndOpenOffsetTest extends BrokerIntegrationTest<String, String> {

    Duration normalTimeout = ofSeconds(5);
    Duration debugTimeout = Duration.ofMinutes(1);

    // use debug timeout while debugging
//     Duration timeoutToUse = debugTimeout;
    Duration timeoutToUse = normalTimeout;

    String rebalanceTopic;

    @BeforeEach
    void setup() {
        rebalanceTopic = "close-and-open-" + RandomUtils.nextInt();
    }

    /**
     * Publish some messages, some fail, shutdown, startup again, consume again - check we only consume the failed
     * messages
     * <p>
     * Test with different encodings to make sure each encoding can be used to reload
     * <p>
     * Sometimes fails as 5 is not committed in the first run and comes out in the 2nd
     * <p>
     * NB: messages 4 and 2 are made to fail
     */
    @Timeout(value = 60)
    @SneakyThrows
    @ParameterizedTest
    @EnumSource()
    @ResourceLock(value = OffsetMapCodecManager.METADATA_DATA_SIZE_RESOURCE_LOCK, mode = READ)
    void offsetsOpenClose(OffsetEncoding encoding) {
        var skip = UniLists.of(OffsetEncoding.ByteArray, OffsetEncoding.ByteArrayCompressed, OffsetEncoding.KafkaStreams, OffsetEncoding.KafkaStreamsV2);
        assumeFalse(skip.contains(encoding));

        // todo remove - not even relevant to this test? smelly
        OffsetMapCodecManager.forcedCodec = Optional.of(encoding);
        OffsetSimultaneousEncoder.compressionForced = true;

        // 2 partition topic
        try {
            ensureTopic(rebalanceTopic, 1);
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
        }

        //
        KafkaConsumer<String, String> newConsumerOne = getKcu().createNewConsumer();
        KafkaProducer<String, String> producerOne = getKcu().createNewProducer(true);
        var options = ParallelConsumerOptions.<String, String>builder()
                .ordering(UNORDERED)
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                .consumer(newConsumerOne)
                .producer(producerOne)
                .build();

        // first client
        {
            //
            var asyncOne = new ParallelEoSStreamProcessor<String, String>(options);

            //
            asyncOne.subscribe(UniLists.of(rebalanceTopic));

            // read some messages
            var successfullInOne = new ConcurrentLinkedQueue<ConsumerRecord<String, String>>();
            asyncOne.poll(x -> {
                log.info("Read by consumer ONE: {}", x);
                if (x.value().equals("4")) {
                    log.info("Throwing fake error for message 4");
                    throw new FakeRuntimeException("Fake error - Message 4");
                }
                if (x.value().equals("2")) {
                    log.info("Throwing fake error for message 2");
                    throw new FakeRuntimeException("Fake error - Message 2");
                }
                successfullInOne.add(x.getSingleConsumerRecord());
            });

            // No wait needed before producing: the consumer group is new for this randomly named topic and
            // reads from EARLIEST, so records produced before the assignment completes are still delivered -
            // and the await below only passes once they have been.
            send(rebalanceTopic, 0, 0);
            send(rebalanceTopic, 0, 1);
            send(rebalanceTopic, 0, 2);
            send(rebalanceTopic, 0, 3);
            send(rebalanceTopic, 0, 4);
            send(rebalanceTopic, 0, 5);

            // all are processed except msg 2 and 4, which holds up the queue
            await().alias("check all except 2 and 4 are processed").atMost(normalTimeout).untilAsserted(() -> {
                        ArrayList<ConsumerRecord<String, String>> copy = new ArrayList<>(successfullInOne);
                        assertThat(copy.stream()
                                .map(ConsumerRecord::value).collect(Collectors.toList()))
                                .containsOnly("0", "1", "3", "5");
                    }
            );

            // wait until all expected records have been processed AND their offset data committed.
            // The work manager is "dirty" from the moment a record succeeds until the offsets covering it
            // have been committed, so !isDirty() IS the commit-happened event - no need to decode the
            // offsets topic or touch the (not thread safe) Consumer#committed.
            //
            // The last success has to be established FIRST, though: a work manager with nothing succeeded
            // yet is also not dirty, and setDirty only fires downstream of the success
            // (WorkManager.onSuccessResult -> PartitionState.onSuccess). Waiting on !isDirty() alone can
            // therefore pass before offset 5 has even been processed, let alone committed.
            var rebalancePartition = new TopicPartition(rebalanceTopic, 0);
            await().alias("the last successful record (offset 5) has been registered as succeeded")
                    .atMost(normalTimeout)
                    .until(() -> asyncOne.getWm().getPm().getPartitionState(rebalancePartition)
                            .getOffsetHighestSucceeded() >= 5L);
            await().alias("completed work has been committed")
                    .atMost(normalTimeout)
                    .until(() -> !asyncOne.getWm().isDirty());

            // commit what we've done so far, don't wait for failing messages to be retried (message 4)
            log.info("Closing consumer, committing offset map");
            asyncOne.closeDontDrainFirst();

            await().alias("check all except 2 and 4 are processed")
                    .atMost(normalTimeout)
                    .untilAsserted(() ->
                            assertThat(successfullInOne.stream()
                                    .map(x -> x.value()).collect(Collectors.toList()))
                                    .containsOnly("0", "1", "3", "5"));

            assertThat(asyncOne.getFailureCause()).isNull();
        }

        // second client
        {
            //
            KafkaConsumer<String, String> newConsumerThree = getKcu().createNewConsumer(customClientId("THREE-my-client"));
            KafkaProducer<String, String> producerThree = getKcu().createNewProducer(true);
            var optionsThree = options.toBuilder().consumer(newConsumerThree).producer(producerThree).build();
            try (var asyncThree = new ParallelEoSStreamProcessor<String, String>(optionsThree)) {
                asyncThree.subscribe(UniLists.of(rebalanceTopic));

                // read what we're given
                var processedByThree = new ConcurrentLinkedQueue<ConsumerRecord<String, String>>();
                asyncThree.poll(x -> {
                    log.info("Read by consumer THREE: {}", x.value());
                    processedByThree.add(x.getSingleConsumerRecord());
                });

                //
                await().alias("only 2 and 4 should be delivered again, as everything else was processed successfully")
                        .atMost(timeoutToUse)
                        .untilAsserted(() ->
                                assertThat(processedByThree).extracting(ConsumerRecord::value)
                                        .containsExactlyInAnyOrder("2", "4"));
            }
        }

        OffsetMapCodecManager.forcedCodec = Optional.empty();
        OffsetSimultaneousEncoder.compressionForced = false;
    }

    private Properties customClientId(final String id) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, id);
        return properties;
    }

    private void send(String topic, int partition, Integer value) throws InterruptedException, ExecutionException {
        RecordMetadata recordMetadata = getKcu().getProducer().send(new ProducerRecord<>(topic, partition, value.toString(), value.toString())).get();
    }

    private void send(int quantity, String topic, int partition) throws InterruptedException, ExecutionException {
        log.debug("Sending {} messages to {}", quantity, topic);
        var futures = new ArrayList<Future<RecordMetadata>>();
        // async
        for (Long index : Range.range(quantity)) {
            Future<RecordMetadata> send = getKcu().getProducer().send(new ProducerRecord<>(topic, partition, index.toString(), index.toString()));
            futures.add(send);
        }
        // block until finished
        for (Future<RecordMetadata> future : futures) {
            future.get();
        }
        log.debug("Finished sending {} messages", quantity);
    }


    /**
     * Make sure we commit a basic offset correctly - send a single message, read, commit, close, open, read - should be
     * nothing
     */
    @Test
    void correctOffsetVerySimple() {
        setupTopic();

        // send a single message
        String expectedPayload = "0";
        getKcu().getProducer().send(new ProducerRecord<>(topic, expectedPayload, expectedPayload));

        KafkaConsumer<String, String> consumer = getKcu().createNewConsumer();
        KafkaProducer<String, String> producerOne = getKcu().createNewProducer(true);
        var options = ParallelConsumerOptions.<String, String>builder()
                .ordering(UNORDERED)
                .consumer(consumer)
                .producer(producerOne)
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                .build();

        try (var asyncOne = new ParallelEoSStreamProcessor<String, String>(options)) {

            asyncOne.subscribe(UniLists.of(topic));

            var readByOne = new ArrayList<ConsumerRecord<String, String>>();
            asyncOne.poll(msg -> {
                log.debug("Reading {}", msg);
                readByOne.add(msg.getSingleConsumerRecord());
            });

            // the single message is processed
            await().untilAsserted(() -> assertThat(readByOne)
                    .extracting(ConsumerRecord::value)
                    .containsExactly(expectedPayload));

        } finally {
            log.debug("asyncOne closed");
        }

        //
        log.debug("Starting up new client");
        KafkaConsumer<String, String> newConsumerThree = getKcu().createNewConsumer(customClientId("THREE-my-client"));
        KafkaProducer<String, String> producerThree = getKcu().createNewProducer(true);
        ParallelConsumerOptions<String, String> optionsThree = options.toBuilder()
                .consumer(newConsumerThree)
                .producer(producerThree)
                .build();
        try (var asyncThree = new ParallelEoSStreamProcessor<String, String>(optionsThree)) {
            asyncThree.subscribe(UniLists.of(topic));

            // read what we're given - concurrent, as the assertions below read it from the test thread while
            // PC's worker thread may be adding to it
            var readByThree = new ConcurrentLinkedQueue<ConsumerRecord<String, String>>();
            asyncThree.poll(x -> {
                log.info("Three read: {}", x.value());
                readByThree.add(x.getSingleConsumerRecord());
            });

            // Asserting an ABSENCE proves nothing unless the consumer that saw nothing was actually listening,
            // so anchor the window to the assignment first. PartitionStateManager only holds a state for a
            // partition it has been assigned (onPartitionsAssigned populates the map), and that map is a
            // ConcurrentHashMap - so unlike Consumer#assignment, this is safe to read from the test thread
            // while PC's poll thread is running.
            var partition = new TopicPartition(topic, 0);
            await().alias("PC three has been assigned the partition - without this, 'nothing was read' is equally "
                            + "true of a consumer that never finished joining the group")
                    .atMost(timeoutToUse)
                    .until(() -> asyncThree.getWm().getPm().getPartitionState(partition) != null);

            // nothing should be read back: asyncOne processed and committed the only record there was.
            // No atLeast() here - pollDelay already puts the single evaluation at >= 1s, so an atLeast at or
            // below that can never fail, and an assertion that cannot fail is decoration.
            await().alias("nothing should be read back")
                    .pollDelay(ofSeconds(1))
                    .atMost(ofSeconds(2))
                    .untilAsserted(() -> {
                                assertThat(readByThree).as("Nothing should be read into the collection")
                                        .extracting(ConsumerRecord::value)
                                        .isEmpty();
                            }
                    );

            // Prove the detector works. Assignment says three joined; it does not say three's poller ever
            // reached the topic. A record produced now must come through - if it does not, the emptiness above
            // was measuring a consumer that could not have reported a re-read even if one had happened.
            String afterCommitPayload = "1";
            getKcu().getProducer().send(new ProducerRecord<>(topic, afterCommitPayload, afterCommitPayload));
            await().alias("a record produced after the absence window IS read - proves three was live throughout it")
                    .atMost(timeoutToUse)
                    .untilAsserted(() -> assertThat(readByThree)
                            .extracting(ConsumerRecord::value)
                            .containsExactly(afterCommitPayload));
        }
    }

    /**
     * @see KafkaClientUtils#MAX_POLL_RECORDS
     */
    @SneakyThrows
    @Test
    void largeNumberOfMessagesSmallOffsetBitmap() {
        setupTopic();

        int quantity = 10_000;
        assertThat(quantity).as("Test expects to process all the produced messages in a single poll")
                .isLessThanOrEqualTo(KafkaClientUtils.MAX_POLL_RECORDS);
        send(quantity, topic, 0);

        var baseOptions = ParallelConsumerOptions.<String, String>builder()
                .ordering(UNORDERED)
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                .build();

        Set<String> failingMessages = UniSets.of("123", "2345", "8765");
        int numberOfFailingMessages = failingMessages.size();

        // step 1
        {
            KafkaConsumer<String, String> consumer = getKcu().createNewConsumer();
            KafkaProducer<String, String> producerOne = getKcu().createNewProducer(true);
            var options = baseOptions.toBuilder()
                    .consumer(consumer)
                    .producer(producerOne)
                    .build();
            var asyncOne = new ParallelEoSStreamProcessor<String, String>(options);

            asyncOne.subscribe(UniLists.of(topic));

            var readByOne = new ConcurrentSkipListSet<String>();
            asyncOne.poll(x -> {
                String value = x.value();
                if (failingMessages.contains(value)) {
                    throw new FakeRuntimeException("Fake error for message " + value);
                }
                readByOne.add(value);
            });

            // the single message is not processed
            await().atMost(defaultTimeout).untilAsserted(() -> assertThat(readByOne.size())
                    .isEqualTo(quantity - numberOfFailingMessages));

            //
            // TODO: fatal vs retriable exceptions. Retry limits particularly for draining state?
            asyncOne.closeDontDrainFirst();

            // sanity - post close
            assertThat(readByOne.size()).isEqualTo(quantity - numberOfFailingMessages);
        }

        // step 2
        {
            //
            KafkaConsumer<String, String> newConsumerThree = getKcu().createNewConsumer(customClientId("THREE-my-client"));
            KafkaProducer<String, String> producerThree = getKcu().createNewProducer(true);
            var optionsThree = baseOptions.toBuilder()
                    .consumer(newConsumerThree)
                    .producer(producerThree)
                    .build();
            try (var asyncThree = new ParallelEoSStreamProcessor<String, String>(optionsThree)) {
                asyncThree.subscribe(UniLists.of(topic));

                // read what we're given
                var readByThree = new ConcurrentSkipListSet<String>();
                asyncThree.poll(x -> {
                    log.info("Three read: {}", x.value());
                    readByThree.add(x.value());
                });

                // No atLeast() - pollDelay already puts the single evaluation at >= 1s, so an atLeast at or
                // below that can never fail, and an assertion that cannot fail is decoration
                await().alias("Only the one remaining failing message should be submitted for processing")
                        .pollDelay(ofMillis(1000))
                        .untilAsserted(() -> {
                                    assertThat(readByThree)
                                            .as("Contains only previously failed messages")
                                            .hasSize(numberOfFailingMessages);
                                }
                        );

                // A restatement, not an independent check - it cannot fail unless the await above passed.
                // Its previous comment claimed it was a "double check after closing", which it never was: it
                // sits inside the try, so it runs before close. Moving it out does not earn the claim either -
                // measured, by producing an extra record here and letting it reach the broker before close:
                // PC stops fetching at close, so the record is never delivered and BOTH positions still pass.
                // Catching a late delivery would need a quiet window or a settle anchor while PC still runs.
                assertThat(readByThree).hasSize(numberOfFailingMessages);
            }
        }
    }


}
