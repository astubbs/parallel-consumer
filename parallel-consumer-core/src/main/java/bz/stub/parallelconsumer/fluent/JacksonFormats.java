package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The Jackson half of {@link Formats}, kept in its own class so that nothing here is loaded until
 * {@link Formats#json} has already confirmed Jackson is on the classpath and refused with a message naming it if it
 * is not (KTD7). Jackson is an <em>optional</em> dependency of core: a classic-API user gains nothing transitive.
 */
final class JacksonFormats {

    /**
     * One mapper for every JSON format built here. Jackson's is documented thread-safe once configured, and
     * configuring one per route would cost a code-cache copy per topic for no benefit.
     * <p>
     * <b>{@code findAndRegisterModules()} is not optional decoration.</b> A bare mapper refuses every
     * {@code java.time} type - an {@link java.time.Instant} field on an otherwise ordinary record fails to write
     * with "not supported by default: add Module ..." - and a user reading the fluent API's one-line
     * {@code json(Order.class)} has nowhere to add that module. This picks up every Jackson datatype module on the
     * user's classpath through the {@link java.util.ServiceLoader}, which is how the JSR-310 one arrives, and does
     * nothing at all when none is there.
     */
    private static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();

    private JacksonFormats() {
    }

    static <T> Format<T> of(Class<T> type) {
        return Format.named(deserializer(type), serializer(), "json(" + type.getSimpleName() + ")", type);
    }

    /**
     * The class-free form: the payload as a map of field names to values, so a topic nobody has a class for can be
     * consumed and inspected by field name with nothing declared (R4).
     */
    static Format<Map<String, Object>> ofMap() {
        @SuppressWarnings({"unchecked", "rawtypes"})
        Class<Map<String, Object>> mapType = (Class) LinkedHashMap.class;
        return Format.named(deserializer(mapType), JacksonFormats.<Map<String, Object>>serializer(), "json(map)",
                mapType);
    }

    private static <T> Deserializer<T> deserializer(Class<T> type) {
        // Resolved once, when the format is built, rather than per record: readValue(bytes, Class) looks the type
        // up and builds a reader on every call, and this one runs for every record of every route that declared a
        // JSON format. The reader is immutable and thread-safe, which is what lets one serve every worker.
        ObjectReader reader = MAPPER.readerFor(type);
        return new Deserializer<T>() {
            @Override
            public T deserialize(String topic, byte[] data) {
                if (data == null) {
                    return null;
                }
                try {
                    return reader.readValue(data);
                } catch (Exception e) {
                    // Transient by default (R12) - a route that can tell corrupt from unavailable wraps this with
                    // Formats.classifyDecodeFailures.
                    throw new SerializationException("Could not read JSON on topic " + topic + " as "
                            + type.getName(), e);
                }
            }
        };
    }

    private static <T> Serializer<T> serializer() {
        return new Serializer<T>() {
            @Override
            public byte[] serialize(String topic, T data) {
                if (data == null) {
                    return null;
                }
                try {
                    return MAPPER.writeValueAsBytes(data);
                } catch (Exception e) {
                    throw new SerializationException("Could not write JSON for topic " + topic, e);
                }
            }
        };
    }
}
