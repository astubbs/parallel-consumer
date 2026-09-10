package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

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
class KafkaClientRuntime implements ClientRuntime {

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
