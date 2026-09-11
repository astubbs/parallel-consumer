package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.AfterEach;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * The fixture every fluent test that starts a real engine needs: a runtime over the shipped mock consumer, the
 * handle it hands back, connection properties, and a teardown that cannot hang.
 * <p>
 * Shared rather than repeated because it <em>was</em> repeated - eleven suites carried the same four members and the
 * same {@code props()} builder, differing only in the group id, and the duplication report caught it. Same shape and
 * same reasoning as {@code RetryQueueTestBase} in the state package.
 *
 * @see RecordingClientRuntime
 */
abstract class AbstractFluentEngineTest {

    /**
     * The topic almost every scenario uses. A suite that needs a second one names it itself.
     */
    static final String TOPIC = "orders";

    final RecordingClientRuntime runtime = new RecordingClientRuntime();

    /**
     * The handle under test. Set it when a test starts one, and clear it when the test closes it itself, so the
     * teardown below does not close an instance twice.
     */
    ConsumerHandle handle;

    /**
     * Teardown's close, and deliberately <b>not</b> the handle's - see
     * {@link RecordingClientRuntime#closeWithoutDraining} for why draining a parked record waits out the whole
     * drain timeout of a test that has already passed.
     */
    /**
     * The payload every decode-failure scenario uses to make a deserialiser throw. Shared so that the value a
     * test publishes and the value the deserialiser refuses cannot drift apart into two spellings.
     */
    static final String POISON = "poison";

    /**
     * A deserialiser that reads UTF-8 and refuses exactly {@link #POISON}.
     * <p>
     * It is the smallest thing that fails the way a real one does - a {@link SerializationException} out of
     * {@code deserialize} - and it was written out three times, identically, in the suites that need it; the
     * duplicate-code check flagged two of them against each other.
     */
    static Deserializer<String> rejecting() {
        return (topic, data) -> {
            String value = new String(data, StandardCharsets.UTF_8);
            if (value.equals(POISON)) {
                throw new SerializationException("this payload cannot be read");
            }
            return value;
        };
    }

    @AfterEach
    void closeTheInstance() {
        if (handle != null) {
            RecordingClientRuntime.closeWithoutDraining(handle);
            handle = null;
        }
    }

    /**
     * Connection properties for this suite. The group id is the test class's own name, so two suites running
     * concurrently can never be reading each other's - and nobody has to invent one.
     */
    Properties props() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, getClass().getSimpleName());
        return properties;
    }
}
