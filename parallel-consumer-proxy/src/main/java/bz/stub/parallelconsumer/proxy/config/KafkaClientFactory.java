package bz.stub.parallelconsumer.proxy.config;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Constructs the proxy's Kafka consumer and producer from the credential map {@code Configure} carried (R48) -
 * the one seam where the wire's {@code kafka_properties} become real clients, so it is also the one seam a
 * test fixture replaces to put {@code MockConsumer}/{@code MockProducer} behind the engine instead (the
 * R39-exception the test-mode sidecar documents).
 * <p>
 * <b>Credential hygiene:</b> implementations receive the raw property map, which carries credentials. No
 * implementation may log an entry of it at any level - not the map, not a single value, not a message whose
 * {@code toString} embeds one. (The Kafka clients themselves log their parsed config at INFO, redacting
 * password-typed values as {@code [hidden]} - that redaction is theirs; this rule is ours.)
 *
 * @author Antony Stubbs
 * @see ConfigureHandler
 */
public interface KafkaClientFactory {

    /** Builds the consumer the engine will poll. Key and value stay bytes: the proxy never deserializes. */
    Consumer<byte[], byte[]> consumer(Map<String, String> kafkaProperties);

    /** Builds the producer for the success report's produce payload (R6). */
    Producer<byte[], byte[]> producer(Map<String, String> kafkaProperties);

    /**
     * The production factory: real {@link KafkaConsumer}/{@link KafkaProducer} over the wire-supplied
     * properties, with byte-array (de)serializers supplied as objects so no serde class name needs to travel.
     * <p>
     * {@code enable.auto.commit} is forced {@code false} whatever the map says: Parallel Consumer owns offset
     * commits, and core shuts down on a consumer that auto-commits underneath it.
     */
    static KafkaClientFactory production() {
        return new KafkaClientFactory() {
            @Override
            public Consumer<byte[], byte[]> consumer(Map<String, String> kafkaProperties) {
                Map<String, Object> config = new HashMap<>(kafkaProperties);
                config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
                return new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
            }

            @Override
            public Producer<byte[], byte[]> producer(Map<String, String> kafkaProperties) {
                Map<String, Object> config = new HashMap<>(kafkaProperties);
                return new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer());
            }
        };
    }
}
