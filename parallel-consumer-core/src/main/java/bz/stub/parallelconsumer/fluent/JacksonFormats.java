package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.fasterxml.jackson.databind.ObjectMapper;
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
     */
    private static final ObjectMapper MAPPER = new ObjectMapper();

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
        return new Deserializer<T>() {
            @Override
            public T deserialize(String topic, byte[] data) {
                if (data == null) {
                    return null;
                }
                try {
                    return MAPPER.readValue(data, type);
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
