package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;

/**
 * A route's consumed types: how its key and value bytes become values (R4).
 * <p>
 * The shape is Kafka Streams' {@code Consumed.with}, in this library's own package so no Streams dependency arrives,
 * because every Kafka Java developer already reads it (KD3). Either half may be a Kafka
 * {@link org.apache.kafka.common.serialization.Serdes Serde}, a bare {@link Deserializer}, or a {@link Formats}
 * helper.
 *
 * @param <K> the consumed key type
 * @param <V> the consumed value type
 */
@InterfaceStability.Unstable
public final class Consumed<K, V> {

    private final Format<K> key;

    private final Format<V> value;

    private Consumed(Format<K> key, Format<V> value) {
        this.key = key;
        this.value = value;
    }

    public static <K, V> Consumed<K, V> with(Serde<K> key, Serde<V> value) {
        return new Consumed<>(Format.of(key), Format.of(value));
    }

    public static <K, V> Consumed<K, V> with(Serde<K> key, Deserializer<V> value) {
        return new Consumed<>(Format.of(key), Format.reading(value));
    }

    public static <K, V> Consumed<K, V> with(Deserializer<K> key, Serde<V> value) {
        return new Consumed<>(Format.reading(key), Format.of(value));
    }

    public static <K, V> Consumed<K, V> with(Deserializer<K> key, Deserializer<V> value) {
        return new Consumed<>(Format.reading(key), Format.reading(value));
    }

    public Format<K> key() {
        return key;
    }

    public Format<V> value() {
        return value;
    }

    @Override
    public String toString() {
        return "Consumed(key=" + key + ", value=" + value + ")";
    }
}
