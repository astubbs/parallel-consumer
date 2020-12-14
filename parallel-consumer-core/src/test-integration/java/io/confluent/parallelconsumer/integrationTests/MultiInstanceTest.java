package io.confluent.parallelconsumer.integrationTests;

import io.confluent.csid.utils.ProgressBarUtils;
import io.confluent.csid.utils.StringUtils;
import io.confluent.csid.utils.TrimListRepresentation;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.waitAtMost;
import static pl.tlinkowski.unij.api.UniLists.of;

@Slf4j
class MultiInstanceTest extends BrokerIntegrationTest<String, String> {

    public List<String> consumedKeys = Collections.synchronizedList(new ArrayList<>());
    public List<String> producedKeysAcknowledged = Collections.synchronizedList(new ArrayList<>());
    public AtomicInteger processedCount = new AtomicInteger(0);
    public AtomicInteger producedCount = new AtomicInteger(0);

    int maxPoll = 500;

    ParallelConsumerOptions.CommitMode commitMode = ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC;
    ParallelConsumerOptions.ProcessingOrder order = ParallelConsumerOptions.ProcessingOrder.KEY;


    @SneakyThrows
    @Test
        // todo multi commit mode, multi partition count, multi instance count? 2,3,10,100?
    void multiInstance() {
        numPartitions = 12;
        String inputName = setupTopic(this.getClass().getSimpleName() + "-input-" + RandomUtils.nextInt());

        List<String> expectedKeys = new ArrayList<>();
        int expectedMessageCount = 1_000_000;
        log.info("Producing {} messages before starting test", expectedMessageCount);

        produceMessages(inputName, expectedKeys, expectedMessageCount);

        // setup
        ParallelEoSStreamProcessor<String, String> pcOne = buildPc(inputName, expectedMessageCount);
        ParallelEoSStreamProcessor<String, String> pcTwo = buildPc(inputName, expectedMessageCount);
        ParallelEoSStreamProcessor<String, String> pcThree = buildPc(inputName, expectedMessageCount);

        // run
        var consumedByOne = Collections.synchronizedList(new ArrayList<ConsumerRecord<?, ?>>());
        var consumedByTwo = Collections.synchronizedList(new ArrayList<ConsumerRecord<?, ?>>());
        var consumedByThree = Collections.synchronizedList(new ArrayList<ConsumerRecord<?, ?>>());
        run(expectedMessageCount / 3, pcOne, consumedByOne);
        run(expectedMessageCount / 3, pcTwo, consumedByTwo);
        run(expectedMessageCount / 3, pcThree, consumedByThree);

        // wait for all pre-produced messages to be processed and produced
        Assertions.useRepresentation(new TrimListRepresentation());
        var failureMessage = StringUtils.msg("All keys sent to input-topic should be processed and produced, within time " +
                        "(expected: {} commit: {} order: {} max poll: {})",
                expectedMessageCount, commitMode, order, maxPoll);
        try {
            waitAtMost(ofSeconds(30))
                    .failFast(() -> pcThree.isClosedOrFailed(), () -> pcThree.getFailureCause()) // requires https://github.com/awaitility/awaitility/issues/178#issuecomment-734769761
                    .alias(failureMessage)
                    .pollInterval(1, SECONDS)
                    .untilAsserted(() -> {
                        log.trace("Processed-count: {}, Produced-count: {}", processedCount.get(), producedCount.get());
                        SoftAssertions all = new SoftAssertions();
                        all.assertThat(new ArrayList<>(consumedKeys)).as("all expected are consumed").hasSameSizeAs(expectedKeys);
//                        all.assertThat(new ArrayList<>(producedKeysAcknowledged)).as("all consumed are produced ok ").hasSameSizeAs(expectedKeys);
                        all.assertAll();
                    });
        } catch (ConditionTimeoutException e) {
            fail(failureMessage + "\n" + e.getMessage());
        }

//        bar.close();
//
//        pc.closeDrainFirst();

        assertThat(processedCount.get())
                .as("messages processed and produced by parallel-consumer should be equal")
                .isEqualTo(expectedMessageCount);

        // sanity
        assertThat(expectedMessageCount).isEqualTo(processedCount.get());
        assertThat(producedKeysAcknowledged).hasSameSizeAs(expectedKeys);
    }

    Integer barId = 0;

    private ProgressBar run(final int expectedMessageCount, final ParallelEoSStreamProcessor<String, String> pc, List<ConsumerRecord<?, ?>> consumed) {
        ProgressBar bar = ProgressBarUtils.getNewMessagesBar(log, expectedMessageCount);
        bar.setExtraMessage(" id: " + barId);
        pc.setMyId(Optional.of("id: " + barId));
        barId++;
        pc.poll(record -> {
                    processRecord(bar, record, consumed);
                }
//                , consumeProduceResult -> {
//                    callBack(consumeProduceResult);
//                }
        );
        return bar;
    }

    private void callBack(final io.confluent.parallelconsumer.ParallelStreamProcessor.ConsumeProduceResult<String, String, String, String> consumeProduceResult) {
        producedCount.incrementAndGet();
        producedKeysAcknowledged.add(consumeProduceResult.getIn().key());
    }

    private void processRecord(final ProgressBar bar,
                               final ConsumerRecord<String, String> record,
                               List<ConsumerRecord<?, ?>> consumed) {
        // by not having any sleep here, this test really test the overhead of the system
//                    try {
//                        Thread.sleep(5);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                    try {
//                        // 1/5 chance of taking a long time
//                        int chance = 10;
//                        int dice = RandomUtils.nextInt(0, chance);
//                        if (dice == 0) {
//                            Thread.sleep(100);
//                        } else {
//                            Thread.sleep(RandomUtils.nextInt(3, 20));
//                        }
        bar.stepBy(1);
        consumedKeys.add(record.key());
        processedCount.incrementAndGet();
        consumed.add(record);
//        return new ProducerRecord<>(outputName, record.key(), "data");
    }

    private ParallelEoSStreamProcessor<String, String> buildPc(final String inputName, final int expectedMessageCount) {
        log.debug("Starting test");
//        KafkaProducer<String, String> newProducer = kcu.createNewProducer(commitMode.equals(PERIODIC_TRANSACTIONAL_PRODUCER));

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPoll);
        KafkaConsumer<String, String> newConsumer = kcu.createNewConsumer(false, consumerProps);

        var pc = new ParallelEoSStreamProcessor<String, String>(ParallelConsumerOptions.<String, String>builder()
                .ordering(order)
                .consumer(newConsumer)
                .commitMode(commitMode)
                .maxConcurrency(100)
                .build());
        pc.subscribe(of(inputName));

        // sanity
        TopicPartition tp = new TopicPartition(inputName, 0);
        Map<TopicPartition, Long> beginOffsets = newConsumer.beginningOffsets(of(tp));
        Map<TopicPartition, Long> endOffsets = newConsumer.endOffsets(of(tp));
        assertThat(endOffsets.get(tp)).isEqualTo(expectedMessageCount);
        assertThat(beginOffsets.get(tp)).isEqualTo(0L);
        return pc;
    }

    private void produceMessages(final String inputName, final List<String> expectedKeys, final int expectedMessageCount) throws InterruptedException, java.util.concurrent.ExecutionException {
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
    }

}
