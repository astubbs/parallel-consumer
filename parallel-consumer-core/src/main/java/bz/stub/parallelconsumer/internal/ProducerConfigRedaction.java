package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.experimental.UtilityClass;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Renders producer configuration for logs and messages without ever rendering a secret.
 * <p>
 * Deny by default: a key is rendered with its value only when it is on {@link #ALLOW_LISTED_KEYS}, and every other
 * key is rendered as {@code key=<redacted>}. There is no mode that reveals more. Kafka's own {@code Password} typing
 * is not the discriminator because it covers only the keys Kafka itself declares - it says nothing about serializer,
 * interceptor or Schema Registry secrets such as {@code basic.auth.user.info}, which arrive as plain strings.
 * <p>
 * Adding a key to the list is a claim that its value can never carry a credential; {@code ProducerConfigRedactionTest}
 * holds the list to a few name-shaped rules, and a reviewer holds it to the rest.
 */
@UtilityClass
public class ProducerConfigRedaction {

    static final String REDACTED = "<redacted>";

    /**
     * Keys whose values are safe to render: addresses, sizes, timeouts, class names and mode switches. Nothing here
     * is a credential, a key store, a JAAS string or a Schema Registry user-info pair.
     */
    public static final Set<String> ALLOW_LISTED_KEYS = Collections.unmodifiableSet(new LinkedHashSet<>(Arrays.asList(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
            CommonClientConfigs.CLIENT_ID_CONFIG,
            ProducerConfig.TRANSACTIONAL_ID_CONFIG,
            ProducerConfig.ACKS_CONFIG,
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
            ProducerConfig.COMPRESSION_TYPE_CONFIG,
            ProducerConfig.LINGER_MS_CONFIG,
            ProducerConfig.BATCH_SIZE_CONFIG,
            ProducerConfig.MAX_BLOCK_MS_CONFIG,
            ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
            CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG,
            ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
            "sasl.mechanism"
    )));

    /**
     * @return the configuration as {@code {key=value, key=<redacted>, ...}} in key order, {@code "null"} for a null
     *         map - the shape {@link java.util.AbstractMap#toString()} gives, so it reads like the rest of an
     *         options {@code toString()}
     */
    public static String render(Map<String, Object> config) {
        if (config == null) {
            return "null";
        }
        return new TreeMap<>(config).entrySet().stream()
                .map(entry -> entry.getKey() + "=" + (ALLOW_LISTED_KEYS.contains(entry.getKey()) ? entry.getValue() : REDACTED))
                .collect(Collectors.joining(", ", "{", "}"));
    }
}
