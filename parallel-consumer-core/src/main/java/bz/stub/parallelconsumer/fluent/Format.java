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

    /**
     * How bytes become a value, or null on a write-only format. Null is a legitimate half of this type, which is
     * the one way it differs from a plain {@link Serde} - hence {@link #hasDeserializer()} rather than a cast.
     */
    private final Deserializer<T> deserializer;

    /**
     * How a value becomes bytes, or null on a read-only format - a route may be declared with a hand-written
     * {@link Deserializer} for a topic that nothing here can write (KTD9).
     */
    private final Serializer<T> serializer;

    /**
     * What this format calls itself in a message. Held rather than derived on demand because a format is named in
     * refusals about the route that declared it, and a deserialiser's class name is all there is to go on once a
     * lambda or an anonymous class has been handed in.
     */
    private final String description;

    /**
     * The Java type this format reads into, or null when nothing told it. Nothing in the facade reads this; it is
     * carried for the sandbox, which cannot generate a record for a class it cannot name.
     *
     * @see #type()
     */
    private final Class<T> type;

    /**
     * Private, so that every format arrives through a factory which has already decided which halves it has. The
     * four arguments are not independent: at least one of the two serialisers must be present, and the factories
     * are where that is enforced.
     */
    private Format(Deserializer<T> deserializer, Serializer<T> serializer, String description, Class<T> type) {
        this.deserializer = deserializer;
        this.serializer = serializer;
        this.description = description;
        this.type = type;
    }

    /**
     * A format that can only read.
     */
    public static <T> Format<T> reading(Deserializer<T> deserializer) {
        Objects.requireNonNull(deserializer, "A deserializer must be supplied");
        return new Format<>(deserializer, null, deserializer.getClass().getSimpleName(), null);
    }

    /**
     * A format that can only write - the produced side of a route whose values nothing here needs to read back.
     */
    public static <T> Format<T> writing(Serializer<T> serializer) {
        Objects.requireNonNull(serializer, "A serializer must be supplied");
        return new Format<>(null, serializer, serializer.getClass().getSimpleName(), null);
    }

    /**
     * A format that can read and write.
     */
    public static <T> Format<T> of(Deserializer<T> deserializer, Serializer<T> serializer) {
        Objects.requireNonNull(deserializer, "A deserializer must be supplied");
        Objects.requireNonNull(serializer, "A serializer must be supplied");
        return new Format<>(deserializer, serializer, deserializer.getClass().getSimpleName(), null);
    }

    /**
     * A format that can read and write, naming the Java type it carries - which is what lets the sandbox generate
     * records for a route declared with hand-written serialisers.
     *
     * @see #type()
     */
    public static <T> Format<T> of(Deserializer<T> deserializer, Serializer<T> serializer, Class<T> type) {
        Objects.requireNonNull(deserializer, "A deserializer must be supplied");
        Objects.requireNonNull(serializer, "A serializer must be supplied");
        return new Format<>(deserializer, serializer, deserializer.getClass().getSimpleName(), type);
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
        return new Format<>(serde.deserializer(), serde.serializer(), serde.getClass().getSimpleName(), null);
    }

    /**
     * The factory for this package's own format helpers, which are the only callers that can name a format better
     * than its deserialiser's class does - {@code json(Order.class)} reads as itself in a refusal, where the public
     * factories can only report whatever class the user handed in. Package-private for that reason.
     */
    static <T> Format<T> named(Deserializer<T> deserializer,
                               Serializer<T> serializer,
                               String description,
                               Class<T> type) {
        return new Format<>(deserializer, serializer, description, type);
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
     * The Java type this format reads into, when it is known, and null when it is not.
     * <p>
     * A {@link Formats} helper always knows - {@code json(Order.class)} was told. A format built from a bare
     * {@link Deserializer} or a Kafka {@link Serde} does not: the type is erased and nothing here can recover it.
     * <p>
     * Nothing in the facade needs this. <b>The sandbox does</b>, because generating a record means filling an
     * instance of a class, and a route whose type it cannot name is refused there naming the topic (KTD9) - the
     * cure being either a format helper or the {@code Class}-taking factories above.
     */
    public Class<T> type() {
        return type;
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

    /**
     * Closes whichever halves this format has, satisfying {@link Serde#close()} for a type where either half may be
     * absent - the null tests are the difference from a plain serde, not defensiveness.
     */
    @Override
    public void close() {
        if (deserializer != null) {
            deserializer.close();
        }
        if (serializer != null) {
            serializer.close();
        }
    }

    /**
     * The description, alone: a format is printed inside refusals that already name the topic and the side, so
     * anything more here would repeat what surrounds it.
     */
    @Override
    public String toString() {
        return description;
    }
}
