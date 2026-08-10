package io.confluent.parallelconsumer.streamsspike.integrationTests;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.integrationTests.BrokerIntegrationTest;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import io.confluent.parallelconsumer.streamsspike.PcDispatchSwitch;
import io.confluent.parallelconsumer.streamsspike.PcTaskDispatcher;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The crash-safety proof for U9 (astubbs#255): a consumer-group commit made while a record is still in
 * flight must never cover that record.
 * <p>
 * <b>The scenario is the frontier's defining case.</b> The record at the head of the partition parks on a
 * latch inside the chain; records behind it, on other keys, complete out of order around it. A commit then
 * lands (short commit interval - deliberately <em>not</em> via {@code suspend()}, whose
 * {@code pumpUntilQuiescent} drain would mask the defect). The committed offset must be the parked record's
 * offset - the frontier - because committing anything higher records the parked record as done while it is
 * still running: crash there and it is silently lost.
 * <p>
 * <b>Written red-first.</b> Against the pre-U9 mechanism this test FAILS: with the partition group empty on
 * the PC path, stock's {@code committableOffsetsAndMetadata()} falls back to {@code consumer.position()}
 * and commits every polled record - the parked one included. That red run is the demonstration of the
 * defect U9 removes; the assertion messages state what the failure means.
 *
 * @author Antony Stubbs
 * @see io.confluent.parallelconsumer.streamsspike.PcTaskDispatcher
 */
@Slf4j
// PcDispatchSwitch is process-wide; a concurrent test flipping it would change which dispatch path this
// class measures.
@Isolated
class CommitFrontierCrashRestartTest extends BrokerIntegrationTest<String, String> {

    private static final int POOL_SIZE = 4;

    private static final String BLOCKER_VALUE = "blocker";

    private static final int FAST_RECORDS = 10;

    /** Short, so a commit lands while the blocker is parked, without waiting on the default 30s. */
    private static final Duration COMMIT_INTERVAL = Duration.ofMillis(500);

    /**
     * Parks the blocker record inside the chain. Static because the topology lambda runs on PC worker
     * threads in this JVM; recreated per test so one test's release cannot leak into the next.
     */
    private static volatile CountDownLatch blockerParkedUntil;

    @BeforeEach
    void usePcDispatch() {
        blockerParkedUntil = new CountDownLatch(1);
        PcDispatchSwitch.enable(POOL_SIZE);
    }

    @AfterEach
    void releaseAndReset() {
        // Frees any still-parked worker so close() does not ride the pool-termination timeout.
        blockerParkedUntil.countDown();
        PcDispatchSwitch.resetToDefault();
    }

    @Test
    void commitNeverCoversTheParkedRecord() {
        String inputTopic = setupTopic("frontier-in");
        String outputTopic = setupTopic("frontier-out");
        ensureTopic(inputTopic, 1);
        ensureTopic(outputTopic, 1);
        TopicPartition inputPartition = new TopicPartition(inputTopic, 0);

        String appId = "frontier-" + System.nanoTime();
        produceBlockerThenFastRecords(inputTopic);

        KafkaStreams streams = startParkableTopology(appId, inputTopic, outputTopic);
        try {
            awaitOutputs(outputTopic, FAST_RECORDS);

            // A commit must land while the blocker is parked - that is the moment under test.
            OffsetAndMetadata committed = awaitFirstCommit(appId, inputPartition);
            log.info("=== committed while blocker parked: offset={} metadata.length={}",
                    committed.offset(), committed.metadata().length());

            assertThat(committed.offset())
                    .as("THE FRONTIER PROPERTY (R10): the record at offset 0 is still IN FLIGHT - parked "
                            + "inside the chain - so the committed offset must be exactly 0, the frontier. "
                            + "A higher value records the parked record as done while it is running: crash "
                            + "now and it is silently lost. Pre-U9 this commits the consumer position "
                            + "(all %s records), which is the defect this test exists to demonstrate.",
                            FAST_RECORDS + 1)
                    .isEqualTo(0L);
        } finally {
            blockerParkedUntil.countDown();
            streams.close(Duration.ofSeconds(30));
        }
    }

    /**
     * R10 end to end: crash mid-run, restart, nothing lost.
     * <p>
     * The crash is a genuine abort - {@link PcTaskDispatcher#abortAllActive()} kills the workers with no
     * drain, no completion feed-back and no final commit, so the restart inherits exactly what a real crash
     * leaves: a committed frontier at the parked record, and in-flight work that simply vanished. A clean
     * {@code close()} would instead drain via the patched {@code suspend()} and commit on the way down,
     * handing the "crash" a repair pass a real one never gets.
     * <p>
     * The parked record is the one the pre-U9 mechanism silently loses: its offset was covered by the
     * consumer-position commit, so a restart never redelivers it. With the frontier committed, redelivery is
     * guaranteed. Records completed beyond the frontier are replayed too - the permitted at-least-once
     * duplicate, since metadata read-back is U9's recorded non-goal - so the assertion is on presence, not
     * exact counts.
     */
    @Test
    void killRestartLosesNothing() {
        String inputTopic = setupTopic("kill-in");
        String outputTopic = setupTopic("kill-out");
        ensureTopic(inputTopic, 1);
        ensureTopic(outputTopic, 1);
        TopicPartition inputPartition = new TopicPartition(inputTopic, 0);

        String appId = "kill-restart-" + System.nanoTime();
        produceBlockerThenFastRecords(inputTopic);

        // Run 1: fast records complete around the parked blocker; a frontier commit lands; then crash.
        KafkaStreams firstRun = startParkableTopology(appId, inputTopic, outputTopic);
        try {
            awaitOutputs(outputTopic, FAST_RECORDS);
            OffsetAndMetadata committed = awaitFirstCommit(appId, inputPartition);
            assertThat(committed.offset())
                    .as("precondition: the frontier commit landed while the blocker was parked")
                    .isEqualTo(0L);

            PcTaskDispatcher.abortAllActive();
            log.info("=== CRASHED with the blocker parked and frontier 0 committed");
        } finally {
            firstRun.close(Duration.ofSeconds(30));
        }

        // Run 2: the blocker is no longer parked - the latch is released, as a restarted process's record
        // would simply process normally. The committed frontier makes the restart redeliver it. The reader
        // is scoped to records produced AFTER the crash, so every assertion below is about what the
        // RESTART did - run 1's durable outputs cannot satisfy them.
        TopicPartition outputPartition = new TopicPartition(outputTopic, 0);
        long preRestartEnd = outputEndOffset(outputPartition);
        blockerParkedUntil.countDown();
        KafkaStreams secondRun = startParkableTopology(appId, inputTopic, outputTopic);
        try {
            List<String> outputs = drainFrom(outputPartition, preRestartEnd, FAST_RECORDS + 1);
            assertThat(outputs)
                    .as("R10: the record that was IN FLIGHT at the crash must be redelivered and processed "
                            + "BY THE RESTART - this is the record the pre-U9 consumer-position commit "
                            + "silently lost")
                    .contains(BLOCKER_VALUE);
            for (int i = 0; i < FAST_RECORDS; i++) {
                assertThat(outputs)
                        .as("the frontier commit means the restart replays every record at or beyond it - "
                                + "each fast record must be re-processed by run 2, not merely present from "
                                + "run 1's output")
                        .contains("fast-" + i);
            }
            log.info("=== RESTART itself produced {} outputs: blocker present, nothing lost", outputs.size());
        } finally {
            secondRun.close(Duration.ofSeconds(30));
        }
    }

    /**
     * The compatibility half of KTD-S7: a group whose commits carry PC's frontier-and-holes payload must
     * still be usable by STOCK Kafka Streams. {@code TopicPartitionMetadata.decode} reads PC's magic byte as
     * an unsupported version, warns, and degrades to UNKNOWN partition time - asserted here behaviourally
     * (the seam-off restart runs and processes) rather than by pinning the log line.
     */
    @Test
    void stockRestartOnPcCommittedGroupDegradesGracefully() {
        String inputTopic = setupTopic("stock-restart-in");
        String outputTopic = setupTopic("stock-restart-out");
        ensureTopic(inputTopic, 1);
        ensureTopic(outputTopic, 1);
        TopicPartition inputPartition = new TopicPartition(inputTopic, 0);

        String appId = "stock-restart-" + System.nanoTime();
        produceBlockerThenFastRecords(inputTopic);

        // Phase 1: PC-dispatched run commits the frontier with PC metadata in the group, then CRASHES -
        // an orderly close would let the blocker finish and the final commit would carry a bare offset
        // with EMPTY metadata (nothing beyond the frontier left to encode), and phase 2 would decode
        // nothing. The abort keeps the holes-bearing payload as the group's LAST commit, which is the
        // exact input the decode-leniency claim is about (U9 review finding on this scenario).
        KafkaStreams pcRun = startParkableTopology(appId, inputTopic, outputTopic);
        TopicPartition outputPartition = new TopicPartition(outputTopic, 0);
        long phaseOneEnd;
        try {
            awaitOutputs(outputTopic, FAST_RECORDS);
            OffsetAndMetadata committed = awaitFirstCommit(appId, inputPartition);
            assertThat(committed.metadata())
                    .as("precondition: the group's commit carries PC's encoded payload")
                    .isNotEmpty();
            phaseOneEnd = outputEndOffset(outputPartition);
            PcTaskDispatcher.abortAllActive();
        } finally {
            pcRun.close(Duration.ofSeconds(30));
        }

        // Phase 2: stock dispatch, same group, PC payload still in the committed metadata. It must decode
        // leniently and process - and the reader is scoped past phase 1's outputs, so isNotEmpty can only
        // be satisfied by records STOCK produced after taking the group over.
        blockerParkedUntil = new CountDownLatch(0);
        PcDispatchSwitch.disable();
        KafkaStreams stockRun = startParkableTopology(appId, inputTopic, outputTopic);
        try {
            List<String> outputs = drainFrom(outputPartition, phaseOneEnd, 1);
            assertThat(outputs)
                    .as("stock Streams must decode the PC payload leniently, resume from the frontier, and "
                            + "process - the parked blocker replays here, produced by STOCK, not by phase 1")
                    .contains(BLOCKER_VALUE);
            log.info("=== STOCK takeover decoded the PC payload and processed {} outputs", outputs.size());
        } finally {
            stockRun.close(Duration.ofSeconds(30));
        }
    }

    /**
     * The output topic's current end offset - captured between phases so the next phase's reader can be
     * scoped to records produced AFTER this point. Without it, an earliest-reading consumer re-reads the
     * previous phase's durable outputs and the restart assertions pass on evidence the restart never
     * produced (U9 review findings on this class - the vacuous-restart-assert defect).
     */
    private long outputEndOffset(final TopicPartition outputPartition) {
        try (KafkaConsumer<String, String> consumer =
                     getKcu().createNewConsumer(KafkaClientUtils.GroupOption.NEW_GROUP)) {
            return consumer.endOffsets(UniSets.of(outputPartition)).get(outputPartition);
        }
    }

    /**
     * Reads only records at or after {@code fromOffset} - assign+seek, never subscribe-from-earliest, so
     * the caller's assertions can only be satisfied by records the phase under test itself produced.
     */
    private List<String> drainFrom(final TopicPartition outputPartition, final long fromOffset, final int atLeast) {
        List<String> outputs = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer =
                     getKcu().createNewConsumer(KafkaClientUtils.GroupOption.NEW_GROUP)) {
            consumer.assign(UniLists.of(outputPartition));
            consumer.seek(outputPartition, fromOffset);
            await().atMost(Duration.ofSeconds(90)).until(() -> {
                ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : polled) {
                    outputs.add(record.value());
                }
                return outputs.size() >= atLeast;
            });
            // a few quiet polls so late arrivals (replays) are captured before asserting on contents
            for (int quiet = 0; quiet < 3; quiet++) {
                ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : polled) {
                    outputs.add(record.value());
                }
            }
        }
        return outputs;
    }

    private void produceBlockerThenFastRecords(final String inputTopic) {
        try (KafkaProducer<String, String> producer =
                     getKcu().createNewProducer(KafkaClientUtils.ProducerMode.NOT_TRANSACTIONAL)) {
            producer.send(new ProducerRecord<>(inputTopic, "key-blocker", BLOCKER_VALUE));
            for (int i = 0; i < FAST_RECORDS; i++) {
                producer.send(new ProducerRecord<>(inputTopic, "key-fast-" + i, "fast-" + i));
            }
            producer.flush();
        }
        log.info("Produced blocker at offset 0 + {} fast records into {}", FAST_RECORDS, inputTopic);
    }

    private KafkaStreams startParkableTopology(final String appId,
                                               final String inputTopic,
                                               final String outputTopic) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(inputTopic);
        stream.mapValues(value -> {
            if (BLOCKER_VALUE.equals(value)) {
                try {
                    // The block IS the scenario: an in-flight record straddling a commit.
                    blockerParkedUntil.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Blocker interrupted while parked", e);
                }
            }
            return value;
        }).to(outputTopic);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.consumerPrefix("auto.offset.reset"), "earliest");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL.toMillis());

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        AtomicInteger polls = new AtomicInteger();
        await().atMost(Duration.ofSeconds(60)).until(() -> {
            KafkaStreams.State state = streams.state();
            if (polls.getAndIncrement() % 10 == 0) {
                log.info("Waiting for Streams to run, state={}", state);
            }
            return state == KafkaStreams.State.RUNNING;
        });
        return streams;
    }

    private void awaitOutputs(final String outputTopic, final int expected) {
        List<String> outputs = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer =
                     getKcu().createNewConsumer(KafkaClientUtils.GroupOption.NEW_GROUP)) {
            consumer.subscribe(UniLists.of(outputTopic));
            await().atMost(Duration.ofSeconds(60)).until(() -> {
                ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : polled) {
                    outputs.add(record.value());
                }
                return outputs.size() >= expected;
            });
        }
        log.info("All {} fast records emitted while the blocker stays parked", expected);
    }

    /**
     * Reads the Streams application's own committed offset. The reader consumer carries the app's group id
     * but never subscribes, so it performs an OffsetFetch without joining - it cannot trigger a rebalance
     * of the topology under test.
     */
    private OffsetAndMetadata awaitFirstCommit(final String appId, final TopicPartition inputPartition) {
        ConcurrentLinkedQueue<OffsetAndMetadata> seen = new ConcurrentLinkedQueue<>();
        try (KafkaConsumer<String, String> groupReader = getKcu().createNewConsumer(appId)) {
            await().atMost(Duration.ofSeconds(60))
                    .pollInterval(Duration.ofMillis(250))
                    .until(() -> {
                        Map<TopicPartition, OffsetAndMetadata> committed =
                                groupReader.committed(UniSets.of(inputPartition));
                        OffsetAndMetadata offset = committed.get(inputPartition);
                        if (offset != null) {
                            seen.add(offset);
                            return true;
                        }
                        return false;
                    });
        }
        return seen.peek();
    }
}
