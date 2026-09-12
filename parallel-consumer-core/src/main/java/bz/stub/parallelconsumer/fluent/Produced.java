package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.serialization.Serde;

import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serializer;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * A route's produced types: how the values its function returns become bytes (R3, R4).
 * <p>
 * Declaring these re-types the route, and that is what makes producing from a route that declared none a compile
 * error rather than a runtime failure - see {@link Outcome}. The shape is Kafka Streams' {@code Produced.with}, in
 * this library's own package (KD3).
 *
 * @param <PK> the produced key type
 * @param <PV> the produced value type
 */
@InterfaceStability.Unstable
public final class Produced<PK, PV> {

    /**
     * How a value the function returns becomes key bytes. Held as a {@link Format} rather than a bare serialiser so
     * that a Serde, a serialiser and a {@link Formats} helper all arrive at the route in one shape.
     */
    private final Format<PK> key;

    /**
     * The same for the value side. Both are checked writable as they are stored, so a read-only format is a
     * definition error rather than a failure on the first record the route tries to produce.
     */
    private final Format<PV> value;

    /**
     * Private: the {@link #with} overloads are the only way in, which is what keeps every instance a pair of formats
     * that can write, and lets the overloads accept whichever shapes the caller already has.
     */
    private Produced(Format<PK> key, Format<PV> value) {
        this.key = requireWritable(key, "key");
        this.value = requireWritable(value, "value");
    }

    /**
     * <b>Each worker thread gets its own pair of serialisers</b>, made by these suppliers - the mirror of
     * {@link Consumed#perWorker(Supplier, Supplier)}, and needed for the same reason: the produce path runs on the
     * same worker threads a decode does, so a stateful serialiser is no safer there than a stateful deserialiser is
     * (owner-directed, 2026-09-12).
     */
    public static <PK, PV> Produced<PK, PV> perWorker(Supplier<Serializer<PK>> key,
                                                      Supplier<Serializer<PV>> value) {
        return new Produced<>(Format.writingPerWorker(key), Format.writingPerWorker(value));
    }

    /**
     * Both sides from Serdes - the common case, for a caller who already holds a Serde for each type.
     * <p>
     * <b>A serde hands out one serialiser, so both are shared by every worker thread</b> and must be thread-safe;
     * {@link #perWorker(Supplier, Supplier)} gives each worker its own.
     */
    public static <PK, PV> Produced<PK, PV> with(Serde<PK> key, Serde<PV> value) {
        return new Produced<>(Format.of(key), Format.of(value));
    }

    /**
     * A Serde for the key and a bare serialiser for the value, for a value type this route only ever writes.
     */
    public static <PK, PV> Produced<PK, PV> with(Serde<PK> key, Serializer<PV> value) {
        return new Produced<>(Format.of(key), Format.writing(value));
    }

    /**
     * The mirror of {@link #with(Serde, Serializer)}. Both halves exist so a caller never has to invent the reading
     * half of a Serde that the produced side would not use.
     */
    public static <PK, PV> Produced<PK, PV> with(Serializer<PK> key, Serde<PV> value) {
        return new Produced<>(Format.writing(key), Format.of(value));
    }

    /**
     * Bare serialisers on both sides, for produced types that are written here and read somewhere else entirely.
     */
    public static <PK, PV> Produced<PK, PV> with(Serializer<PK> key, Serializer<PV> value) {
        return new Produced<>(Format.writing(key), Format.writing(value));
    }

    /**
     * A produced side must be able to write. A read-only {@link Format} - a hand-written deserialiser with no
     * serialiser beside it - is a definition error here rather than a failure on the first produced record.
     */
    private static <T> Format<T> requireWritable(Format<T> format, String side) {
        if (!format.hasSerializer()) {
            throw new IllegalArgumentException(msg("Produced {} format {} has no serializer, so nothing could write "
                            + "the records this route returns - supply a Serde, or a Deserializer and Serializer pair",
                    side, format));
        }
        return format;
    }

    /**
     * The key format, read by the route when it wires up the records the function returns.
     */
    public Format<PK> key() {
        return key;
    }

    /**
     * The value format, the other half of what the route needs to write a returned record.
     */
    public Format<PV> value() {
        return value;
    }

    /**
     * Names both formats, because a refusal about produced types is read beside the route it came from and the type
     * name alone would not say which of the pair was wrong.
     */
    @Override
    public String toString() {
        return "Produced(key=" + key + ", value=" + value + ")";
    }
}
