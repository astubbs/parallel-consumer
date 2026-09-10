package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;
import java.util.Objects;

/**
 * One side of a route's typing: how bytes become a value, and - when it is known - how a value becomes bytes.
 * <p>
 * It is a Kafka {@link Serde} so that {@link org.apache.kafka.common.serialization.Serdes Serdes.String()} and a
 * format helper from {@link Formats} are interchangeable wherever {@link Consumed} and {@link Produced} take one.
 * The difference from a plain {@code Serde} is that the serialiser may be <em>absent</em>: a route declared with a
 * hand-written {@link Deserializer} can read a topic that nothing here can write. {@link #hasSerializer()} answers
 * that, and the sandbox - which must encode what it generates - refuses such a route naming its topic (KTD9).
 *
 * @param <T> the value type this format reads and, when it can, writes
 */
@InterfaceStability.Unstable
public final class Format<T> implements Serde<T> {

    private final Deserializer<T> deserializer;

    private final Serializer<T> serializer;

    private final String description;

    private Format(Deserializer<T> deserializer, Serializer<T> serializer, String description) {
        this.deserializer = deserializer;
        this.serializer = serializer;
        this.description = description;
    }

    /**
     * A format that can only read.
     */
    public static <T> Format<T> reading(Deserializer<T> deserializer) {
        Objects.requireNonNull(deserializer, "A deserializer must be supplied");
        return new Format<>(deserializer, null, deserializer.getClass().getSimpleName());
    }

    /**
     * A format that can only write - the produced side of a route whose values nothing here needs to read back.
     */
    public static <T> Format<T> writing(Serializer<T> serializer) {
        Objects.requireNonNull(serializer, "A serializer must be supplied");
        return new Format<>(null, serializer, serializer.getClass().getSimpleName());
    }

    /**
     * A format that can read and write.
     */
    public static <T> Format<T> of(Deserializer<T> deserializer, Serializer<T> serializer) {
        Objects.requireNonNull(deserializer, "A deserializer must be supplied");
        Objects.requireNonNull(serializer, "A serializer must be supplied");
        return new Format<>(deserializer, serializer, deserializer.getClass().getSimpleName());
    }

    /**
     * A format from a Kafka {@link Serde}, which always has both halves.
     */
    public static <T> Format<T> of(Serde<T> serde) {
        if (serde instanceof Format) {
            @SuppressWarnings("unchecked") Format<T> already = (Format<T>) serde;
            return already;
        }
        Objects.requireNonNull(serde, "A serde must be supplied");
        return new Format<>(serde.deserializer(), serde.serializer(), serde.getClass().getSimpleName());
    }

    static <T> Format<T> named(Deserializer<T> deserializer, Serializer<T> serializer, String description) {
        return new Format<>(deserializer, serializer, description);
    }

    /**
     * @return the deserialiser, or null when this format can only write - test with {@link #hasDeserializer()} first
     */
    @Override
    public Deserializer<T> deserializer() {
        return deserializer;
    }

    /**
     * Whether this format can read as well as write. Every consumed side of a route needs this to be true.
     */
    public boolean hasDeserializer() {
        return deserializer != null;
    }

    /**
     * @return the serialiser, or null when this format can only read - test with {@link #hasSerializer()} first
     */
    @Override
    public Serializer<T> serializer() {
        return serializer;
    }

    /**
     * Whether this format can write as well as read. A route that produces, and a route the sandbox must generate
     * records for, needs this to be true.
     */
    public boolean hasSerializer() {
        return serializer != null;
    }

    /**
     * Passes the definition's remaining connection properties to both halves, as Kafka clients do for their own
     * serialisers (R4, KTD7). The facade's own keys have already been removed by the caller.
     */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (deserializer != null) {
            deserializer.configure(configs, isKey);
        }
        if (serializer != null) {
            serializer.configure(configs, isKey);
        }
    }

    @Override
    public void close() {
        if (deserializer != null) {
            deserializer.close();
        }
        if (serializer != null) {
            serializer.close();
        }
    }

    @Override
    public String toString() {
        return description;
    }
}
