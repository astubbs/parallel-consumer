package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import static com.google.common.truth.Truth.assertThat;

/**
 * R4 and KTD7's property split: the keys the facade owns are consumed, and everything else reaches each route's
 * deserialisers exactly as a Kafka client would pass it to its own.
 * <p>
 * This is what makes a schema-registry deserialiser usable on a route without the user configuring it twice, and it
 * is invisible from the outside - the only way to see it is a deserialiser that records what it was configured with.
 */
class ConnectionPropertiesTest {

    /**
     * Records its configuration so the test can assert on a call that has no other effect.
     */
    static class RecordingDeserializer implements Deserializer<String> {

        Map<String, Object> configuredWith = Collections.emptyMap();

        boolean configuredAsKey;

        boolean configured;

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            this.configuredWith = new LinkedHashMap<>(configs);
            this.configuredAsKey = isKey;
            this.configured = true;
        }

        @Override
        public String deserialize(String topic, byte[] data) {
            return new String(data, StandardCharsets.UTF_8);
        }
    }

    private static Properties props() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "connection-properties-test");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("schema.registry.url", "http://registry:8081");
        properties.put("specific.avro.reader", "true");
        return properties;
    }

    @Test
    void passThroughPropertiesReachEachRoutesDeserialisersAndTheFacadesOwnKeysDoNot() {
        RecordingDeserializer key = new RecordingDeserializer();
        RecordingDeserializer value = new RecordingDeserializer();
        var pc = ParallelConsumer.define(props());
        pc.topic("orders").consumed(Consumed.with(key, value)).process(context -> Outcome.succeeded());

        pc.validate();

        assertThat(key.configuredWith).containsEntry("schema.registry.url", "http://registry:8081");
        assertThat(key.configuredWith).containsEntry("specific.avro.reader", "true");
        assertThat(key.configuredWith).doesNotContainKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
        assertThat(key.configuredWith).doesNotContainKey(ConsumerConfig.GROUP_ID_CONFIG);
        assertThat(key.configuredWith).doesNotContainKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
        assertThat(value.configuredWith).containsEntry("schema.registry.url", "http://registry:8081");
    }

    /**
     * Kafka's own deserialisers are told which side they are on, and a registry deserialiser reads different subject
     * settings for each; the facade passes the same signal.
     */
    @Test
    void eachSideIsToldWhetherItIsTheKey() {
        RecordingDeserializer key = new RecordingDeserializer();
        RecordingDeserializer value = new RecordingDeserializer();
        var pc = ParallelConsumer.define(props());
        pc.topic("orders").consumed(Consumed.with(key, value)).process(context -> Outcome.succeeded());

        pc.validate();

        assertThat(key.configured).isTrue();
        assertThat(value.configured).isTrue();
        assertThat(key.configuredAsKey).isTrue();
        assertThat(value.configuredAsKey).isFalse();
    }

    @Test
    void theViewSeparatesTheClientPropertiesFromThePassThroughOnes() {
        var pc = ParallelConsumer.define(props());
        pc.string("orders").process(context -> Outcome.succeeded());

        assertThat(pc.connectionProperties()).containsEntry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        assertThat(pc.passThroughProperties()).doesNotContainKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
        assertThat(pc.passThroughProperties()).containsEntry("schema.registry.url", "http://registry:8081");
    }
}
