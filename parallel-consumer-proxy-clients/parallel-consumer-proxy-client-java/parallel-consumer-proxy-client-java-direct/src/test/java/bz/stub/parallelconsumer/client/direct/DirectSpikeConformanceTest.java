package bz.stub.parallelconsumer.client.direct;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.client.AsyncRecordProcessor;
import bz.stub.parallelconsumer.client.ClientOptions;
import bz.stub.parallelconsumer.client.RecordProcessor;
import bz.stub.parallelconsumer.client.conformance.SpikeConformanceTest;
import bz.stub.parallelconsumer.client.conformance.SpikeFixture;
import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import bz.stub.parallelconsumer.model.CommitHistory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletionStage;

/**
 * The shared spike suite under the direct transport: no RPC, no serialization - the transport is a plain
 * method call into core, which is exactly the evidence KTD1 needs at the smallest scale. The suite runs
 * unmodified; only this fixture is transport-specific, binding the client to core's mock Kafka clients (the
 * same {@code LongPollingMockConsumer} pattern the engine-side harness uses, from core's test-jar) so the
 * whole run stays in the surefire lane.
 * <p>
 * <b>Deliberately no {@code ProxyHarness} here:</b> the proxy test-jar drags protobuf and gRPC, and this
 * module's {@code bannedDependencies} rule (AE26) forbids both anywhere on its classpath - the ban is the
 * proof that nothing transport-shaped leaked into the shared API, so the fixture rebuilds the small mock
 * arrangement from core's own pieces instead of importing the harness.
 *
 * @author Antony Stubbs
 */
class DirectSpikeConformanceTest extends SpikeConformanceTest {

    @Override
    protected SpikeFixture fixture(String topic, List<SpikeFixture.Seed> seeds) {
        return new DirectFixture(topic, seeds);
    }

    private static final class DirectFixture implements SpikeFixture {

        private final String topic;
        private final TopicPartition topicPartition;
        private final List<Seed> seeds;

        private final LongPollingMockConsumer<byte[], byte[]> mockConsumer =
                new LongPollingMockConsumer<>(OffsetResetStrategy.EARLIEST);
        private final MockProducer<byte[], byte[]> mockProducer =
                new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());

        private DirectParallelConsumerClient client;

        private DirectFixture(String topic, List<Seed> seeds) {
            this.topic = topic;
            this.topicPartition = new TopicPartition(topic, 0);
            this.seeds = seeds;
        }

        @Override
        public void start(ClientOptions options, RecordProcessor processor) {
            build(options).poll(processor);
            arrangeEngineSide();
        }

        @Override
        public void startAsync(ClientOptions options, AsyncRecordProcessor processor) {
            build(options).pollAsync(processor);
            arrangeEngineSide();
        }

        private DirectParallelConsumerClient build(ClientOptions options) {
            client = DirectParallelConsumerClient.builder()
                    .options(options)
                    .consumer(mockConsumer)
                    .producer(mockProducer)
                    .build();
            return client;
        }

        private void arrangeEngineSide() {
            // the manual rebalance dance a MockConsumer needs, as the engine-side harness documents:
            // MockConsumer#rebalance assigns the partition but fires no listener, so the client (a rebalance
            // listener by delegation to core) is told separately - and seeding must follow assignment
            mockConsumer.subscribeWithRebalanceAndAssignment(Collections.singletonList(topic), 1);
            client.onPartitionsAssigned(Collections.singletonList(topicPartition));
            long offset = 0;
            for (Seed seed : seeds) {
                mockConsumer.addRecord(new ConsumerRecord<>(topic, topicPartition.partition(), offset++,
                        seed.key().getBytes(StandardCharsets.UTF_8),
                        seed.value().getBytes(StandardCharsets.UTF_8)));
            }
        }

        @Override
        public OptionalLong committedOffset() {
            return CommitHistory.forPartition(mockConsumer.getCommitHistoryInt(), topicPartition).highestCommit()
                    .map(OptionalLong::of)
                    .orElseGet(OptionalLong::empty);
        }

        @Override
        public List<ProducedRecord> produced() {
            var produced = new ArrayList<ProducedRecord>();
            for (var record : mockProducer.history()) {
                produced.add(ProducedRecord.decodeUtf8(record.topic(), record.key(), record.value()));
            }
            return produced;
        }

        @Override
        public long recordsOutForProcessing() {
            return client.recordsOutForProcessing();
        }

        @Override
        public CompletionStage<Void> sessionEnd() {
            return client.sessionEnd();
        }

        @Override
        public void close() {
            if (client != null) {
                client.close();
            }
        }
    }
}
