package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * The default {@link ClientRuntime}: a real {@link KafkaConsumer} on raw bytes, and no producer instance - the
 * facade hands Parallel Consumer the producer configuration instead, so that producer recovery stays available
 * (R1, astubbs#410).
 * <p>
 * It holds no client: it builds one and hands it back, and the caller puts it straight into the options builder
 * (KTD3).
 */
@Slf4j
class KafkaClientRuntime implements ClientRuntime {

    /**
     * Builds the real consumer, on raw bytes because each route deserialises its own (KTD2). It refuses a definition
     * that names neither a broker nor a group before constructing anything, so the complaint is about the definition
     * rather than about a client that failed to connect.
     */
    @Override
    public Consumer<byte[], byte[]> consumer(DefinitionView definition) {
        Map<String, Object> config = definition.connectionProperties();
        requireConnection(config);
        return new KafkaConsumer<>(withAutoCommitDisabled(config), new ByteArrayDeserializer(),
                new ByteArrayDeserializer());
    }

    /**
     * Kafka's consumer auto-commits by default and Parallel Consumer refuses to run one that does, since it commits
     * offsets itself - so a definition given nothing but a bootstrap address and a group would fail at start with a
     * message about a client the user never built (R1).
     * <p>
     * Set here rather than at definition time because it is a property of the client this class constructs: a
     * definition that supplies its own consumer, or runs in the sandbox, has already answered the question. An
     * explicit {@code true} in the connection properties is refused before this, by
     * {@link ParallelConsumerDefinition}, rather than being silently overridden.
     */
    private static Map<String, Object> withAutoCommitDisabled(Map<String, Object> config) {
        Map<String, Object> withoutAutoCommit = new LinkedHashMap<>(config);
        withoutAutoCommit.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return withoutAutoCommit;
    }

    /**
     * A real admin client on the definition's connection properties, for the start-time topic-existence check.
     * <p>
     * <b>The properties are filtered to the ones an admin client knows.</b> The connection properties are a
     * consumer's, so they carry keys this client has no use for - the group id above all - and every one of them
     * would be logged as "supplied but isn't a known config" at start, which reads as a misconfiguration in a
     * definition that has none. Everything an admin client needs to reach a secured broker is a known config and
     * survives the filter; what does not is a schema-registry URL and the consumer's own settings, neither of
     * which it would have used.
     * <p>
     * <b>No broker address means there is nothing to ask, which is the empty answer {@link ClientRuntime#admin}
     * documents</b> - not a refusal. A definition may supply finished clients <em>in place of</em> connection
     * properties (R1), and one that does has no address here for this client to reach: refusing it would fail a
     * start that R1 allows, under every policy including {@link MissingTopic#IGNORE}, for a question about a
     * cluster only the user's own clients know how to find. The consumer and producer this class builds still
     * refuse a definition that named no broker - see {@link #requireConnection} - so the address is only optional
     * for the definition that is not asking this class to build anything.
     * <p>
     * The group id is deliberately <b>not</b> required either: an admin client has no use for one, and the filter
     * above drops it before the client is constructed.
     */
    @Override
    public Optional<Admin> admin(DefinitionView definition) {
        Map<String, Object> config = definition.connectionProperties();
        if (!config.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            log.debug("No {} in the connection properties, so this definition's clients are its connection source "
                            + "and there is no cluster for the topic-existence check to ask",
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
            return Optional.empty();
        }
        Map<String, Object> adminConfig = new LinkedHashMap<>();
        for (Map.Entry<String, Object> property : config.entrySet()) {
            if (AdminClientConfig.configNames().contains(property.getKey())) {
                adminConfig.put(property.getKey(), property.getValue());
            }
        }
        return Optional.of(Admin.create(adminConfig));
    }

    /**
     * Empty: Parallel Consumer builds the producer from the definition's properties, which is what keeps producer
     * recovery available.
     */
    @Override
    public Optional<Producer<byte[], byte[]>> producer(DefinitionView definition) {
        return Optional.empty();
    }

    /**
     * Checked here rather than at definition time because a definition started against a fake needs neither key: the
     * refusal belongs to whoever is about to open a socket.
     */
    private void requireConnection(Map<String, Object> config) {
        if (!config.containsKey(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
            throw new IllegalArgumentException(msg("No {} in the connection properties - starting against a broker "
                            + "needs one. A definition started in the sandbox supplies its own clients and needs "
                            + "neither this nor {}.",
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConsumerConfig.GROUP_ID_CONFIG));
        }
        if (!config.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
            throw new IllegalArgumentException(msg("No {} in the connection properties - Parallel Consumer commits "
                    + "offsets for a consumer group, so one is required", ConsumerConfig.GROUP_ID_CONFIG));
        }
    }
}
