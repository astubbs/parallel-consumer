package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A {@link ClientRuntime} that records whether it was asked for anything.
 * <p>
 * It is how the definition tests prove the two claims that cannot be seen from the outside: that a definition which
 * is written and never started constructs no client at all, and that a definition needing no producer never asks for
 * one (AE7, R4). Both are assertions about a call that did <em>not</em> happen, so they need a seam that counts.
 */
class RecordingClientRuntime implements ClientRuntime {

    int consumerCalls;

    int producerCalls;

    final List<DefinitionView> definitionsSeen = new ArrayList<>();

    private final LongPollingMockConsumer<byte[], byte[]> consumer;

    private final MockProducer<byte[], byte[]> producer;

    /**
     * Whether this runtime hands back a producer instance, or declines and leaves Parallel Consumer to build one
     * from the definition's properties - the two answers {@link ClientRuntime#producer} distinguishes.
     */
    private final boolean suppliesAProducer;

    RecordingClientRuntime() {
        this(true);
    }

    /**
     * A runtime that counts the producer request and then declines it, as the real Kafka runtime does so that
     * producer recovery stays available (R1, astubbs#410).
     */
    static RecordingClientRuntime decliningToSupplyAProducer() {
        return new RecordingClientRuntime(false);
    }

    private RecordingClientRuntime(boolean suppliesAProducer) {
        this.consumer = new LongPollingMockConsumer<>(OffsetResetStrategy.EARLIEST);
        this.producer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
        this.suppliesAProducer = suppliesAProducer;
    }

    @Override
    public Consumer<byte[], byte[]> consumer(DefinitionView definition) {
        consumerCalls++;
        definitionsSeen.add(definition);
        return consumer;
    }

    @Override
    public Optional<Producer<byte[], byte[]>> producer(DefinitionView definition) {
        producerCalls++;
        definitionsSeen.add(definition);
        // Empty is the seam's way of saying "build your own, and keep producer recovery" (R1, astubbs#410).
        return suppliesAProducer ? Optional.of(producer) : Optional.empty();
    }

    boolean builtNothing() {
        return consumerCalls == 0 && producerCalls == 0;
    }

    // ---------------------------------------------------------------- driving a real instance over the mocks

    LongPollingMockConsumer<byte[], byte[]> mockConsumer() {
        return consumer;
    }

    MockProducer<byte[], byte[]> mockProducer() {
        return producer;
    }

    /**
     * Starts a definition against these mocks and performs the manual rebalance the shipped mock consumer needs.
     * <p>
     * {@code MockConsumer} is not a correct implementation of the consumer contract - subscribing assigns nothing,
     * so a test that only started the definition would poll for ever against partitions nobody owns. This is the
     * same dance {@code AbstractParallelEoSStreamProcessorTestBase} performs, in the one place every fluent engine
     * test needs it, so that no test grows its own copy.
     *
     * @param partitionsPerTopic how many partitions each of the definition's topics is assigned
     */
    ParallelConsumerInstance startAndAssign(ParallelConsumerDefinition definition, int partitionsPerTopic) {
        List<String> topics = new ArrayList<>(definition.topics());
        seedBeginningOffsets(topics, partitionsPerTopic);
        ParallelConsumerInstance handle = definition.start(this);
        commitOften(handle);
        consumer.subscribeWithRebalanceAndAssignment(topics, partitionsPerTopic);
        return handle;
    }

    /**
     * <b>Seed the offsets before anything can be assigned, not after.</b> {@code MockConsumer.poll} throws
     * "didn't have beginning offset specified, but tried to seek to beginning" for an assigned partition it has no
     * beginning offset for, and {@code subscribeWithRebalanceAndAssignment} assigns first and seeds second - a
     * window the classic test base never opens, because the classic API subscribes and starts polling in two
     * separate calls while {@link ParallelConsumerDefinition#start} does both at once. Measured: three of five
     * scenarios died on the poll thread with that error before this was hoisted.
     */
    private void seedBeginningOffsets(List<String> topics, int partitionsPerTopic) {
        Map<TopicPartition, Long> beginning = new HashMap<>();
        for (String topic : topics) {
            for (int partition = 0; partition < partitionsPerTopic; partition++) {
                beginning.put(new TopicPartition(topic, partition), 0L);
            }
        }
        consumer.updateBeginningOffsets(beginning);
    }

    /**
     * Teardown's close, and deliberately <b>not</b> the handle's.
     * <p>
     * {@link ParallelConsumerInstance#close()} drains first (R17), and a scenario about parking or retrying
     * leaves work that
     * by definition never completes - a parked record is incomplete until somebody resumes it, and a record under
     * an unbounded limit never stops failing. Draining those waits out the whole drain timeout and reports a
     * {@code TimeoutException} from the teardown of a test that passed. Teardown also runs on the failure path, so
     * it must not be able to hang; the classic test base closes without draining for the same reason.
     */
    static void closeWithoutDraining(ParallelConsumerInstance handle) {
        ((AbstractParallelEoSStreamProcessor<?, ?>) handle.processor()).closeDontDrainFirst();
    }

    /**
     * The engine commits on a five-second interval by default, which is a long time to wait to read an offset. The
     * setter is deprecated and still the only way in - the fluent definition deliberately does not expose the
     * commit interval - and it is what the classic test base uses for the same reason.
     */
    @SuppressWarnings("deprecation")
    private static void commitOften(ParallelConsumerInstance handle) {
        ((AbstractParallelEoSStreamProcessor<?, ?>) handle.processor())
                .setTimeBetweenCommits(Duration.ofMillis(100));
    }

    /**
     * Publishes one record, keys and values encoded as UTF-8 bytes - which is what the facade's consumer reads,
     * whatever the route then decodes them into.
     */
    void publish(String topic, int partition, long offset, String key, String value) {
        consumer.addRecord(new ConsumerRecord<>(topic, partition, offset,
                key == null ? null : key.getBytes(StandardCharsets.UTF_8),
                value == null ? null : value.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * The metadata on the last commit for a partition - Parallel Consumer's encoded offset map, which is what
     * carries the records that completed <em>past</em> a record that is still incomplete.
     *
     * @return an empty string when nothing has been committed for it, or when the commit carried no map
     */
    String committedMetadata(String topic, int partition) {
        TopicPartition wanted = new TopicPartition(topic, partition);
        String metadata = "";
        for (Map<TopicPartition, OffsetAndMetadata> commit : consumer.getCommitHistoryInt()) {
            OffsetAndMetadata offsets = commit.get(wanted);
            if (offsets != null && offsets.metadata() != null && !offsets.metadata().isEmpty()) {
                metadata = offsets.metadata();
            }
        }
        return metadata;
    }

    /**
     * The highest offset committed for a partition, as the broker side of the mock consumer saw it.
     *
     * @return -1 when nothing has been committed for it yet
     */
    long committedOffset(String topic, int partition) {
        TopicPartition wanted = new TopicPartition(topic, partition);
        long highest = -1;
        for (Map<TopicPartition, OffsetAndMetadata> commit : consumer.getCommitHistoryInt()) {
            OffsetAndMetadata offsets = commit.get(wanted);
            if (offsets != null && offsets.offset() > highest) {
                highest = offsets.offset();
            }
        }
        return highest;
    }
}
