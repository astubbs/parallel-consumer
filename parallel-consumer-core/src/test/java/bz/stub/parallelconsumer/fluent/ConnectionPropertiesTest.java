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
import static org.junit.jupiter.api.Assertions.assertThrows;

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
    void formatPropertiesReachEachRoutesDeserialisersAndTheFacadesOwnKeysDoNot() {
        RecordingDeserializer key = new RecordingDeserializer();
        RecordingDeserializer value = new RecordingDeserializer();
        var pc = ParallelConsumer.connect(props());
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
     * A key supplied through a parent {@code Properties}' defaults reaches a route's deserialiser with its
     * <b>value</b>, not with null.
     * <p>
     * {@code Properties.stringPropertyNames()} lists keys inherited from the defaults chain, but
     * {@code Properties.get} is {@code Hashtable.get} and does not consult them - so reading the copy back with
     * {@code get} stored {@code name -> null} for exactly those keys, and the null travelled on into
     * {@code configure(...)} and the producer's configuration. Layering registry settings under a defaults parent
     * is an ordinary way to write these, and nothing went red for it.
     */
    @Test
    void aPropertySuppliedThroughDefaultsKeepsItsValue() {
        Properties defaults = new Properties();
        defaults.put("schema.registry.url", "http://from-defaults:8081");
        Properties layered = new Properties(defaults);
        layered.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        layered.put(ConsumerConfig.GROUP_ID_CONFIG, "connection-properties-defaults-test");

        RecordingDeserializer key = new RecordingDeserializer();
        RecordingDeserializer value = new RecordingDeserializer();
        var pc = ParallelConsumer.connect(layered);
        pc.topic("orders").consumed(Consumed.with(key, value)).process(context -> Outcome.succeeded());

        pc.validate();

        assertThat(key.configuredWith).containsEntry("schema.registry.url", "http://from-defaults:8081");
        assertThat(value.configuredWith).containsEntry("schema.registry.url", "http://from-defaults:8081");
    }

    /**
     * Kafka's own deserialisers are told which side they are on, and a registry deserialiser reads different subject
     * settings for each; the facade passes the same signal.
     */
    @Test
    void eachSideIsToldWhetherItIsTheKey() {
        RecordingDeserializer key = new RecordingDeserializer();
        RecordingDeserializer value = new RecordingDeserializer();
        var pc = ParallelConsumer.connect(props());
        pc.topic("orders").consumed(Consumed.with(key, value)).process(context -> Outcome.succeeded());

        pc.validate();

        assertThat(key.configured).isTrue();
        assertThat(value.configured).isTrue();
        assertThat(key.configuredAsKey).isTrue();
        assertThat(value.configuredAsKey).isFalse();
    }

    @Test
    void theViewSeparatesTheClientPropertiesFromThePassThroughOnes() {
        var pc = ParallelConsumer.connect(props());
        pc.string("orders").process(context -> Outcome.succeeded());

        assertThat(pc.connectionProperties()).containsEntry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        assertThat(pc.formatProperties()).doesNotContainKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
        assertThat(pc.formatProperties()).containsEntry("schema.registry.url", "http://registry:8081");
    }

    /**
     * Kafka's consumer auto-commits by default and Parallel Consumer refuses to run one that does, so a definition
     * given nothing but a bootstrap address and a group would have failed at start with a message about a client the
     * user never built - which is what {@code FluentQuickstartIT} found against a real broker before the runtime
     * disabled it (R1).
     * <p>
     * The consumer the runtime builds is not reachable from here without a broker, so what is pinned is the
     * arithmetic that decides it: the connection properties the runtime is handed carry no auto-commit setting at
     * all, which is exactly the case Kafka defaults to true.
     */
    @Test
    void nothingInTheConnectionPropertiesDisablesAutoCommit_soTheRuntimeMust() {
        var pc = ParallelConsumer.connect(props());
        pc.string("orders").process(context -> Outcome.succeeded());

        assertThat(pc.connectionProperties()).doesNotContainKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
    }

    /**
     * Asking for auto-commit is refused at definition time, naming the setting, rather than silently overridden: a
     * user who typed it believes something about how their offsets are committed, and it is wrong.
     */
    @Test
    void anExplicitAutoCommitIsRefusedBeforeAnythingIsOpened() {
        var pc = aDefinitionWithAutoCommitSetTo(true);

        var refusal = assertThrows(IllegalArgumentException.class, pc::validate);
        assertThat(refusal).hasMessageThat().contains(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        assertThat(refusal).hasMessageThat().contains("commits offsets for you");
    }

    /**
     * The string form Kafka itself accepts, so that a properties file loaded from disk is refused the same way an
     * in-code {@code true} is.
     */
    @Test
    void anExplicitAutoCommitIsRefusedWhenItArrivesAsAString() {
        assertThrows(IllegalArgumentException.class, aDefinitionWithAutoCommitSetTo("true")::validate);
    }

    /**
     * An explicit {@code false} is what a careful user writes, and it agrees with what the facade does - so it is
     * accepted rather than refused for naming a setting the facade owns.
     */
    @Test
    void anExplicitlyDisabledAutoCommitIsAccepted() {
        aDefinitionWithAutoCommitSetTo(false).validate();
    }

    /**
     * A one-route definition whose connection properties name auto-commit, however the user spelled the value -
     * Kafka accepts a boolean and the string form, and a properties file loaded from disk gives the string.
     */
    private static ParallelConsumerDefinition aDefinitionWithAutoCommitSetTo(Object value) {
        Properties properties = props();
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, value);
        var pc = ParallelConsumer.connect(properties);
        pc.string("orders").process(context -> Outcome.succeeded());
        return pc;
    }
}
