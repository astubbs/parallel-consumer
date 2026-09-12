package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import pl.tlinkowski.unij.api.UniSets;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

/**
 * One property bag, cut three ways: what a definition validates, what a route's formats are configured with, and
 * what a producer is built from.
 * <p>
 * The splitting is the fluent API's own job and exists nowhere else in the library - the engine takes finished
 * configuration maps and has no opinion about which key belongs to which client. Holding it here rather than in
 * {@link ParallelConsumerDefinition} keeps the two lists that decide the cuts beside the three methods that apply
 * them, so a key added to either list cannot be added to one reader and forgotten by another.
 * <p>
 * Immutable once built. Every accessor either returns an unmodifiable view or a fresh copy, because a runtime seam
 * that could edit these would be changing a definition that has already been validated.
 */
final class ConnectionProperties {

    /**
     * Connection properties the facade owns, so they are not passed on to a route's deserialisers: the two that
     * address the cluster, and the client serialisers, which the facade sets to raw bytes itself (KTD7).
     */
    private static final Set<String> FACADE_OWNED = UniSets.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            ConsumerConfig.GROUP_ID_CONFIG,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);

    /**
     * Producer-side keys that mean nothing to a producer and would only log an unknown-configuration warning.
     */
    private static final Set<String> CONSUMER_ONLY = UniSets.of(
            ConsumerConfig.GROUP_ID_CONFIG,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            ConsumerConfig.ISOLATION_LEVEL_CONFIG,
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
            ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG);

    /**
     * The copy taken at construction rather than the caller's object: somebody who goes on editing the
     * {@link Properties} they passed in must not be able to change what the definition validates, or what its
     * clients are built from, after the fact.
     */
    private final Map<String, Object> properties;

    private ConnectionProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    /**
     * Takes the copy. Two passes, and the first one is not redundant.
     *
     * @param supplied the caller's connection properties, never null
     */
    static ConnectionProperties copyOf(Properties supplied) {
        Objects.requireNonNull(supplied, "Connection properties must be supplied");
        Map<String, Object> copy = new LinkedHashMap<>();
        // getProperty, not get: stringPropertyNames() includes keys inherited from a parent Properties' defaults,
        // and Properties.get is Hashtable.get, which does not consult them - so a defaulted key was copied in
        // with a NULL value, and the entrySet pass below cannot repair it because entrySet does not see defaults
        // either. Those nulls reached the deserialisers' configure() and the producer's properties.
        for (String name : supplied.stringPropertyNames()) {
            copy.put(name, supplied.getProperty(name));
        }
        // Properties may carry non-String values when built programmatically; stringPropertyNames misses those.
        for (Map.Entry<Object, Object> entry : supplied.entrySet()) {
            if (entry.getKey() instanceof String) {
                copy.put((String) entry.getKey(), entry.getValue());
            }
        }
        return new ConnectionProperties(copy);
    }

    /**
     * Everything, read-only: a runtime seam builds its clients from these, and a seam that could edit them would be
     * changing a definition that has already been validated.
     */
    Map<String, Object> all() {
        return Collections.unmodifiableMap(properties);
    }

    /**
     * Built fresh each call from {@link #FACADE_OWNED}, so the one list of what the facade owns decides both what a
     * route's formats are configured with and what they are not - a schema-registry URL reaches them, the bootstrap
     * servers and the client serialisers do not (KTD7).
     */
    Map<String, Object> forFormats() {
        Map<String, Object> withoutFacadeKeys = new LinkedHashMap<>(properties);
        withoutFacadeKeys.keySet().removeAll(FACADE_OWNED);
        return Collections.unmodifiableMap(withoutFacadeKeys);
    }

    /**
     * The producer's own configuration: everything here minus the keys that mean nothing to a producer, plus the
     * raw-bytes serialisers the facade requires. Handing this to Parallel Consumer rather than a finished producer
     * is what keeps producer recovery available (R1, astubbs#410).
     */
    Map<String, Object> forProducer() {
        Map<String, Object> config = new LinkedHashMap<>(properties);
        config.keySet().removeAll(CONSUMER_ONLY);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return config;
    }

    /**
     * Whether a key was declared at all - which is a different question from what it was set to, and the one the
     * definition-time refusals ask about a setting they will not accept in any form.
     */
    boolean contains(String key) {
        return properties.containsKey(key);
    }

    /**
     * The declared value, or null when the key was not declared. Used by the one refusal that cares what a setting
     * says rather than that it was said.
     */
    Object get(String key) {
        return properties.get(key);
    }
}
