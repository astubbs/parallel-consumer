package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.fluent.Format;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Bytes;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * What Java type is behind a route's {@link Format}, which is the one thing the generator needs and the one thing
 * erasure destroys.
 *
 * <h2>Three answers, in order</h2>
 * <ol>
 *   <li><b>The format was told.</b> Every {@link bz.stub.parallelconsumer.fluent.Formats} helper knows -
 *       {@code json(Order.class)} was handed the class - and carries it on {@link Format#type()}.</li>
 *   <li><b>The deserialiser is one of Kafka's own.</b> {@code Consumed.with(Serdes.String(), ...)} produces a
 *       format with no type, but its deserialiser is {@code StringDeserializer} and that is not ambiguous. The
 *       table below covers the whole of {@code Serdes}.</li>
 *   <li><b>Nothing knows.</b> A hand-written deserialiser over a hand-written class: the sandbox refuses the
 *       route naming the topic, because inventing a value it cannot read back would fail one poll later with a
 *       message about the payload rather than about the definition.</li>
 * </ol>
 * The cure for the third case is on the definition, not here: a format helper, or the {@code Class}-taking
 * {@code Format.of(deserializer, serializer, type)}.
 */
final class ValueTypes {

    private static final Map<String, Class<?>> KAFKA_DESERIALIZERS = kafkaDeserializers();

    private ValueTypes() {
    }

    /**
     * @return the type this format reads into, or null when it cannot be established
     */
    static Class<?> of(Format<?> format) {
        Class<?> declared = format.type();
        if (declared != null) {
            return declared;
        }
        Deserializer<?> deserializer = format.deserializer();
        if (deserializer == null) {
            return null;
        }
        return KAFKA_DESERIALIZERS.get(deserializer.getClass().getName());
    }

    private static Map<String, Class<?>> kafkaDeserializers() {
        Map<String, Class<?>> known = new LinkedHashMap<>();
        String pkg = "org.apache.kafka.common.serialization.";
        known.put(pkg + "StringDeserializer", String.class);
        known.put(pkg + "ByteArrayDeserializer", byte[].class);
        known.put(pkg + "ByteBufferDeserializer", ByteBuffer.class);
        known.put(pkg + "BytesDeserializer", Bytes.class);
        known.put(pkg + "IntegerDeserializer", Integer.class);
        known.put(pkg + "ShortDeserializer", Short.class);
        known.put(pkg + "LongDeserializer", Long.class);
        known.put(pkg + "FloatDeserializer", Float.class);
        known.put(pkg + "DoubleDeserializer", Double.class);
        known.put(pkg + "BooleanDeserializer", Boolean.class);
        known.put(pkg + "UUIDDeserializer", UUID.class);
        return Collections.unmodifiableMap(known);
    }
}
