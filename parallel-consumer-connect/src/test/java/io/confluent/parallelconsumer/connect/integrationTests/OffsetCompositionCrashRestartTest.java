package io.confluent.parallelconsumer.connect.integrationTests;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.connect.PcSinkTaskLane;
import io.confluent.parallelconsumer.connect.PcSinkTaskLaneRouter;
import io.confluent.parallelconsumer.integrationTests.BrokerIntegrationTest;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import io.confluent.parallelconsumer.streams.PcTaskDispatcher;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import pl.tlinkowski.unij.api.UniLists;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The crash-safety half of U3 in
 * {@code docs/plans/2026-08-10-001-investigate-connect-offset-composition.md}: the arms in
 * {@code OffsetCompositionProbeTest} prove the composition rule is internally sound, against a model of
 * Connect assembled by reading its source. This one runs against a <b>real broker</b>, so a misreading of
 * that model has somewhere to show up. That is the whole reason it is not optional - the probe's negative
 * control inverts the composition function, not the model, so it cannot detect a modelling error.
 *
 * <p><b>What this does NOT prove.</b> There is no Connect runtime here: no worker, no converter, no
 * connector lifecycle, no rebalance. {@code PcConnectDispatchBridge.enabled()} still returns a hard-coded
 * {@code false}. The poll/dispatch/commit loop below is written by hand and stands in for wiring that does
 * not exist yet. What is under test is the <em>frontier</em> - that a consumer-group commit never covers a
 * record no lane durably wrote - not that a connector runs.
 *
 * <p>The sink is a Kafka topic, chosen because it is durable and independently readable: after the kill we
 * can ask what the sink really wrote rather than asking the sink, which would not have survived.
 */
@Slf4j
@Isolated
class OffsetCompositionCrashRestartTest extends BrokerIntegrationTest<String, String> {

    /** The record no lane will ever declare durable. First in the partition, so it is the frontier. */
    private static final String PARKED_VALUE = "parked";

    private static final int FAST_RECORDS = 8;
    private static final int LANES = 4;
    private static final int POOL_SIZE = 4;

    @Test
    void aCommitNeverCoversARecordNoLaneDurablyWrote() {
        final String inputTopic = setupTopic("connect-frontier-in");
        final String outputTopic = setupTopic("connect-frontier-out");
        final TopicPartition inputPartition = new TopicPartition(inputTopic, 0);
        final String groupId = "connect-frontier-" + inputTopic;

        produceParkedThenFastRecords(inputTopic);

        final OffsetAndMetadata committed = runUntilCommitted(inputTopic, outputTopic, groupId, inputPartition);

        assertThat(committed)
                .as("the driver must have committed something, or the assertions below are vacuous")
                .isNotNull();
        assertThat(committed.offset())
                .as("offset 0 is the parked record and no lane ever declared it durable, so the committed "
                        + "offset must be 0 - anything higher records it as done, and a crash there loses it")
                .isEqualTo(0L);

        // --- the crash. Everything above is gone; only the broker's state survives. ---

        assertThat(redeliveredFrom(groupId, inputPartition))
                .as("restarting on the committed offset must hand the parked record back")
                .contains(PARKED_VALUE);

        assertThat(sinkContents(outputTopic))
                .as("and the fast records really were durably written - so the frontier is being held back by "
                        + "the parked record specifically, not by the sink having written nothing at all")
                .hasSizeGreaterThanOrEqualTo(FAST_RECORDS);
    }

    /**
     * Polls, dispatches, runs a durability cycle and commits - the loop the real runtime would own. Stops as
     * soon as the sink has durably written the fast records and a commit has landed, so the assertion sees a
     * commit made while the parked record is genuinely still outstanding.
     */
    @SneakyThrows
    private OffsetAndMetadata runUntilCommitted(final String inputTopic, final String outputTopic,
                                                final String groupId, final TopicPartition inputPartition) {
        final List<PcSinkTaskLane> lanes = new ArrayList<>();
        final List<TopicSinkTask> tasks = new ArrayList<>();
        PcTaskDispatcher dispatcher = null;

        try (KafkaProducer<String, String> sinkProducer =
                     getKcu().createNewProducer(KafkaClientUtils.ProducerMode.NOT_TRANSACTIONAL);
             KafkaConsumer<String, String> consumer = getKcu().createNewConsumer(groupId)) {

            for (int lane = 0; lane < LANES; lane++) {
                final TopicSinkTask task = new TopicSinkTask(sinkProducer, outputTopic);
                tasks.add(task);
                lanes.add(new PcSinkTaskLane(task));
            }
            final PcSinkTaskLaneRouter router =
                    new PcSinkTaskLaneRouter(lanes, OffsetCompositionCrashRestartTest::project);

            dispatcher = new PcTaskDispatcher("connect-crash", Collections.singleton(inputPartition), POOL_SIZE);
            consumer.assign(UniLists.of(inputPartition));
            consumer.seek(inputPartition, 0);

            OffsetAndMetadata lastCommitted = null;
            final long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
            while (System.nanoTime() < deadline) {
                final ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(200));
                if (!polled.isEmpty()) {
                    dispatcher.registerRecords(inputPartition, toBytes(polled.records(inputPartition)));
                }
                dispatcher.dispatchAvailable(router);
                router.runDurabilityCycle();

                final Map<TopicPartition, OffsetAndMetadata> toCommit = dispatcher.collectCommitData();
                if (toCommit.containsKey(inputPartition)) {
                    consumer.commitSync(toCommit);
                    dispatcher.onCommitSuccess(toCommit);
                    lastCommitted = toCommit.get(inputPartition);
                }

                final int durable = tasks.stream().mapToInt(TopicSinkTask::durableCount).sum();
                if (lastCommitted != null && durable >= FAST_RECORDS) {
                    sinkProducer.flush();
                    return lastCommitted;
                }
            }
            return lastCommitted;
        } finally {
            if (dispatcher != null) {
                dispatcher.close();
            }
        }
    }

    /** What a restarting consumer in the same group is handed - the real proof nothing was skipped. */
    private List<String> redeliveredFrom(final String groupId, final TopicPartition inputPartition) {
        final List<String> redelivered = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = getKcu().createNewConsumer(groupId)) {
            consumer.assign(UniLists.of(inputPartition));
            // No seek: the group's committed offset is the resume point, which is exactly what is under test.
            await().atMost(Duration.ofSeconds(60)).until(() -> {
                pollInto(consumer, redelivered);
                return !redelivered.isEmpty();
            });
        }
        return redelivered;
    }

    /**
     * Reads the sink topic from the beginning. Safe here, unlike the streams module's phase-scoped reader:
     * this topic is created fresh for this test and written only by the phase under test, so an
     * earliest-read cannot be satisfied by data from an earlier phase.
     */
    private List<String> sinkContents(final String outputTopic) {
        final TopicPartition outputPartition = new TopicPartition(outputTopic, 0);
        final List<String> written = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer =
                     getKcu().createNewConsumer(KafkaClientUtils.GroupOption.NEW_GROUP)) {
            consumer.assign(UniLists.of(outputPartition));
            consumer.seekToBeginning(UniLists.of(outputPartition));
            await().atMost(Duration.ofSeconds(60)).until(() -> {
                pollInto(consumer, written);
                return written.size() >= FAST_RECORDS;
            });
        }
        return written;
    }

    private static void pollInto(final KafkaConsumer<String, String> consumer, final List<String> values) {
        for (final ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
            values.add(record.value());
        }
    }

    @SneakyThrows
    private void produceParkedThenFastRecords(final String inputTopic) {
        try (KafkaProducer<String, String> producer =
                     getKcu().createNewProducer(KafkaClientUtils.ProducerMode.NOT_TRANSACTIONAL)) {
            // Offset 0, so the parked record IS the frontier - the defining case.
            producer.send(new ProducerRecord<>(inputTopic, "key-parked", PARKED_VALUE)).get();
            for (int i = 0; i < FAST_RECORDS; i++) {
                producer.send(new ProducerRecord<>(inputTopic, "key-fast-" + i, "fast-" + i));
            }
            producer.flush();
        }
    }

    private static List<ConsumerRecord<byte[], byte[]>> toBytes(final List<ConsumerRecord<String, String>> records) {
        final List<ConsumerRecord<byte[], byte[]>> converted = new ArrayList<>(records.size());
        for (final ConsumerRecord<String, String> record : records) {
            converted.add(new ConsumerRecord<>(record.topic(), record.partition(), record.offset(),
                    record.key() == null ? null : record.key().getBytes(StandardCharsets.UTF_8),
                    record.value().getBytes(StandardCharsets.UTF_8)));
        }
        return converted;
    }

    private static SinkRecord project(final ConsumerRecord<byte[], byte[]> record) {
        return new SinkRecord(record.topic(), record.partition(), Schema.OPTIONAL_BYTES_SCHEMA, record.key(),
                Schema.OPTIONAL_BYTES_SCHEMA, record.value(), record.offset());
    }

    /**
     * A sink that writes to a Kafka topic, and refuses one record.
     *
     * <p>Its {@code preCommit} is honest in exactly the way a real connector's is: it reports the highest
     * <b>contiguous</b> prefix of the records IT received that it durably wrote. Refusing one record
     * therefore pins its own watermark below that record forever, which is what a sink that cannot write a
     * poison record actually does.
     */
    private static final class TopicSinkTask extends SinkTask {

        private final KafkaProducer<String, String> producer;
        private final String outputTopic;
        private final NavigableSet<Long> durable = new TreeSet<>();
        private volatile long lowestRefused = Long.MAX_VALUE;

        private TopicSinkTask(final KafkaProducer<String, String> producer, final String outputTopic) {
            this.producer = producer;
            this.outputTopic = outputTopic;
        }

        @Override
        public void put(final Collection<SinkRecord> records) {
            for (final SinkRecord record : records) {
                final String value = new String((byte[]) record.value(), StandardCharsets.UTF_8);
                synchronized (this) {
                    if (PARKED_VALUE.equals(value)) {
                        // Cannot write this one, ever. Note it still RETURNS normally - buffering succeeded,
                        // durability did not, and conflating the two is the defect under investigation.
                        lowestRefused = Math.min(lowestRefused, record.kafkaOffset());
                        continue;
                    }
                }
                producer.send(new ProducerRecord<>(outputTopic, record.key() == null ? null
                        : new String((byte[]) record.key(), StandardCharsets.UTF_8), value));
                synchronized (this) {
                    durable.add(record.kafkaOffset());
                }
            }
            producer.flush();
        }

        @Override
        public synchronized Map<TopicPartition, OffsetAndMetadata> preCommit(
                final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
            if (currentOffsets.isEmpty()) {
                return Collections.emptyMap();
            }
            final TopicPartition partition = currentOffsets.keySet().iterator().next();
            // The highest contiguous prefix of MY OWN records that is durable, stopping at anything refused.
            long watermark = 0;
            for (final Long offset : durable) {
                if (offset >= lowestRefused) {
                    break;
                }
                watermark = offset + 1;
            }
            return Collections.singletonMap(partition, new OffsetAndMetadata(watermark));
        }

        synchronized int durableCount() {
            return durable.size();
        }

        @Override
        public String version() {
            return "crash-restart-probe";
        }

        @Override
        public void start(final Map<String, String> props) {
            // nothing to start
        }

        @Override
        public void stop() {
            // the producer is owned by the test, which closes it
        }
    }
}
