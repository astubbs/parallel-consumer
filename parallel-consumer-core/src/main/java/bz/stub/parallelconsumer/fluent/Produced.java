package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.serialization.Serde;
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

    private final Format<PK> key;

    private final Format<PV> value;

    private Produced(Format<PK> key, Format<PV> value) {
        this.key = requireWritable(key, "key");
        this.value = requireWritable(value, "value");
    }

    public static <PK, PV> Produced<PK, PV> with(Serde<PK> key, Serde<PV> value) {
        return new Produced<>(Format.of(key), Format.of(value));
    }

    public static <PK, PV> Produced<PK, PV> with(Serde<PK> key, Serializer<PV> value) {
        return new Produced<>(Format.of(key), Format.writing(value));
    }

    public static <PK, PV> Produced<PK, PV> with(Serializer<PK> key, Serde<PV> value) {
        return new Produced<>(Format.writing(key), Format.of(value));
    }

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

    public Format<PK> key() {
        return key;
    }

    public Format<PV> value() {
        return value;
    }

    @Override
    public String toString() {
        return "Produced(key=" + key + ", value=" + value + ")";
    }
}
