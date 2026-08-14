package bz.stub.parallelconsumer.client.direct;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.RecordContext;
import bz.stub.parallelconsumer.client.ClientOptions;
import bz.stub.parallelconsumer.client.InboundRecord;
import bz.stub.parallelconsumer.client.Outcome;
import bz.stub.parallelconsumer.client.OutboundRecord;
import bz.stub.parallelconsumer.client.Outcomes;
import bz.stub.parallelconsumer.client.ParallelConsumerClient;
import bz.stub.parallelconsumer.client.ProcessingOrder;
import bz.stub.parallelconsumer.client.RecordProcessor;
import bz.stub.parallelconsumer.ExceptionInUserFunctionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The direct transport: the client wrapper bound straight to {@code parallel-consumer-core}, in-process - the
 * degenerate case of the one client model (KTD1), with no IPC, no serialization, no executor spawning and no
 * protocol anywhere beneath it. The user's {@link RecordProcessor} runs on core's own worker threads, and the
 * per-record {@link Outcome} maps onto core's own success and failure paths, so ordering, retry and commit
 * semantics are core's, untouched.
 * <p>
 * <b>This module's classpath has no protobuf and no gRPC, enforced by {@code bannedDependencies} in its pom
 * (AE26)</b> - which is what makes the shared API's transport-independence checkable: anything this class
 * would have to stub out is transport detail that leaked into the API. There is nothing here about epochs,
 * tokens, dispatch queues or executor counts, because those are wire concepts with no meaning one layer down.
 * <p>
 * The Kafka clients are built from {@link ClientOptions#kafkaProperties()} by default; the builder accepts
 * ready-made instances instead, which is core-idiomatic (core's own options take client <em>objects</em>) and
 * is how tests substitute mock clients. This class also implements {@link ConsumerRebalanceListener} by
 * delegation to core, mirroring core's processor, so an embedded {@code MockConsumer} fixture can perform the
 * manual rebalance dance a mock requires.
 *
 * @author Antony Stubbs
 */
@Slf4j
public class DirectParallelConsumerClient implements ParallelConsumerClient, ConsumerRebalanceListener {

    private final ClientOptions options;
    private final Consumer<byte[], byte[]> consumer;
    private final Producer<byte[], byte[]> producer;

    /** Created by {@link #poll}; volatile so the rebalance delegation can run from a consumer thread. */
    private volatile ParallelEoSStreamProcessor<byte[], byte[]> processor;

    private DirectParallelConsumerClient(Builder builder) {
        this.options = builder.options;
        this.consumer = builder.consumer != null ? builder.consumer
                : buildConsumer(builder.options.kafkaProperties());
        this.producer = builder.producer != null ? builder.producer
                : buildProducer(builder.options.kafkaProperties());
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void poll(RecordProcessor recordProcessor) {
        synchronized (this) {
            if (processor != null) {
                throw new IllegalStateException("poll may be called at most once per client");
            }
            processor = new ParallelEoSStreamProcessor<>(toCoreOptions());
        }
        processor.subscribe(options.topics());
        // core's own poll-and-produce flow IS the wrapper contract here: the outcome's produce list becomes
        // the records core produces before the input offset may commit, and a failure outcome becomes a throw
        // into core's retry scheduling
        processor.pollAndProduceMany(context -> {
            var outcome = Outcomes.applyProcessor(recordProcessor, toInboundRecord(context.getSingleRecord()));
            if (!outcome.isSuccess()) {
                throw new ProcessingFailedException(outcome.failureReason().orElse(""));
            }
            return toProducerRecords(outcome.produce());
        });
    }

    @Override
    public void close() {
        var current = processor;
        if (current != null && !current.isClosedOrFailed()) {
            current.close();
        }
    }

    private ParallelConsumerOptions<byte[], byte[]> toCoreOptions() {
        var builder = ParallelConsumerOptions.<byte[], byte[]>builder()
                .consumer(consumer)
                .producer(producer);
        options.maxConcurrency().ifPresent(builder::maxConcurrency);
        options.ordering().ifPresent(ordering -> builder.ordering(toCoreOrdering(ordering)));
        options.commitInterval().ifPresent(builder::commitInterval);
        options.defaultMessageRetryDelay().ifPresent(builder::defaultMessageRetryDelay);
        return builder.build();
    }

    private static ParallelConsumerOptions.ProcessingOrder toCoreOrdering(ProcessingOrder ordering) {
        switch (ordering) {
            case UNORDERED:
                return ParallelConsumerOptions.ProcessingOrder.UNORDERED;
            case PARTITION:
                return ParallelConsumerOptions.ProcessingOrder.PARTITION;
            case KEY:
            default:
                return ParallelConsumerOptions.ProcessingOrder.KEY;
        }
    }

    private static InboundRecord toInboundRecord(RecordContext<byte[], byte[]> recordContext) {
        // normalised because core's RecordContext#getLastFailureReason returns a NULL Optional before the
        // first failure (WorkContainer's field has no initializer); the wire transport's serializer meets the
        // same quirk and normalises identically
        Optional<Throwable> lastFailureReason =
                Optional.ofNullable(recordContext.getLastFailureReason()).flatMap(reason -> reason);
        return new InboundRecord(
                recordContext.topic(),
                recordContext.partition(),
                recordContext.offset(),
                recordContext.key(),
                recordContext.value(),
                // 1 on first delivery, 2 on the first redelivery - the same product data the wire's
                // Dispatch.attempt carries (R5)
                recordContext.getNumberOfFailedAttempts() + 1,
                recordContext.getLastFailureAt().orElse(null),
                lastFailureReason.map(DirectParallelConsumerClient::failureReasonText).orElse(null));
    }

    /**
     * The recorded throwable as redelivery text, matching the wire transport's semantics: core's own
     * user-function wrapper is unwrapped first, because it names the plumbing rather than the failure.
     */
    private static String failureReasonText(Throwable recorded) {
        Throwable cause = recorded;
        while (cause instanceof ExceptionInUserFunctionException && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause.getMessage() != null ? cause.getMessage() : cause.toString();
    }

    private static List<ProducerRecord<byte[], byte[]>> toProducerRecords(List<OutboundRecord> produce) {
        var records = new ArrayList<ProducerRecord<byte[], byte[]>>(produce.size());
        for (OutboundRecord outbound : produce) {
            records.add(new ProducerRecord<>(outbound.topic(), outbound.key(), outbound.value()));
        }
        return records;
    }

    private static Consumer<byte[], byte[]> buildConsumer(Map<String, String> kafkaProperties) {
        return new KafkaConsumer<>(new HashMap<String, Object>(kafkaProperties),
                new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    private static Producer<byte[], byte[]> buildProducer(Map<String, String> kafkaProperties) {
        return new KafkaProducer<>(new HashMap<String, Object>(kafkaProperties),
                new ByteArraySerializer(), new ByteArraySerializer());
    }

    // --- ConsumerRebalanceListener by delegation, mirroring core's processor (see class javadoc) ---

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        requireStarted().onPartitionsAssigned(partitions);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        requireStarted().onPartitionsRevoked(partitions);
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        requireStarted().onPartitionsLost(partitions);
    }

    private ParallelEoSStreamProcessor<byte[], byte[]> requireStarted() {
        var current = processor;
        if (current == null) {
            throw new IllegalStateException("the client is not polling yet - call poll first");
        }
        return current;
    }

    /**
     * How many records core currently counts as out for processing - the conformance suite's standing leak
     * check, read from core's own accumulator. Package-private: a test observation seam, not API surface.
     */
    long recordsOutForProcessing() {
        return requireStarted().getWm().getNumberRecordsOutForProcessing();
    }

    /** A failure {@link Outcome} on its way into core's retry scheduling; the message is the reason. */
    static final class ProcessingFailedException extends RuntimeException {
        ProcessingFailedException(String reason) {
            super(reason);
        }
    }

    public static class Builder {
        private ClientOptions options;
        private Consumer<byte[], byte[]> consumer;
        private Producer<byte[], byte[]> producer;

        /** Required. */
        public Builder options(ClientOptions options) {
            this.options = options;
            return this;
        }

        /** Optional: a ready-made consumer instead of one built from {@code kafkaProperties}. */
        public Builder consumer(Consumer<byte[], byte[]> consumer) {
            this.consumer = consumer;
            return this;
        }

        /** Optional: a ready-made producer instead of one built from {@code kafkaProperties}. */
        public Builder producer(Producer<byte[], byte[]> producer) {
            this.producer = producer;
            return this;
        }

        public DirectParallelConsumerClient build() {
            if (options == null) {
                throw new IllegalStateException("options are required");
            }
            return new DirectParallelConsumerClient(this);
        }
    }
}
