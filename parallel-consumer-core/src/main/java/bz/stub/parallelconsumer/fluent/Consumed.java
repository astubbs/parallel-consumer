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
 * <h2>What it deliberately does not carry</h2>
 * The borrowed shape has five fields; this has two, and the three that are missing are missing structurally rather
 * than by oversight. A <b>timestamp extractor</b> has nothing to feed: this library has no stream-time or
 * event-time model, so nothing would ever read one. A <b>per-source offset reset policy</b> cannot exist here,
 * because the facade subscribes one consumer to the union of every route's topics - so {@code auto.offset.reset} is
 * necessarily a connection property on
 * {@link bz.stub.parallelconsumer.ParallelConsumer#connect(java.util.Properties) connect}, and a per-route value
 * would be a promise one consumer cannot keep. A <b>source name</b> would be redundant by construction: a topic
 * carries exactly one route (KD11) and a route's parked set is asked for as {@code handle.topic("orders")}, so the
 * topic already is the route's name.
 * <p>
 * <b>Both halves are required and there is no configuration-level default.</b> The original accepts a null half and
 * falls back to a default serde declared in configuration; this has no such fallback, so a null here throws rather
 * than quietly reading with something the route never named.
 *
 * @param <K> the consumed key type
 * @param <V> the consumed value type
 */
@InterfaceStability.Unstable
public final class Consumed<K, V> {

    /**
     * How key bytes become a key. Not checked readable here: the check belongs where the route binds the pair, which
     * is the only place a refusal can name the topic it is about.
     */
    private final Format<K> key;

    /**
     * The same for the value side. Both are held as a {@link Format} so that a Serde, a bare deserialiser and a
     * {@link Formats} helper all reach the route in one shape.
     */
    private final Format<V> value;

    /**
     * Private: the {@link #with} overloads are the only way in, so the type parameters are always fixed by what the
     * caller passed rather than inferred from the route they are handed to.
     */
    private Consumed(Format<K> key, Format<V> value) {
        this.key = key;
        this.value = value;
    }

    /**
     * Both sides from Serdes - the common case, for a caller who already holds a Serde for each type.
     */
    public static <K, V> Consumed<K, V> with(Serde<K> key, Serde<V> value) {
        return new Consumed<>(Format.of(key), Format.of(value));
    }

    /**
     * A Serde for the key and a bare deserialiser for the value, for a value type this route only ever reads.
     */
    public static <K, V> Consumed<K, V> with(Serde<K> key, Deserializer<V> value) {
        return new Consumed<>(Format.of(key), Format.reading(value));
    }

    /**
     * The mirror of {@link #with(Serde, Deserializer)}. Both halves exist so a caller never has to invent the
     * writing half of a Serde that a route which produces nothing would not use.
     */
    public static <K, V> Consumed<K, V> with(Deserializer<K> key, Serde<V> value) {
        return new Consumed<>(Format.reading(key), Format.of(value));
    }

    /**
     * Bare deserialisers on both sides - the shortest declaration a route that never produces can be given.
     */
    public static <K, V> Consumed<K, V> with(Deserializer<K> key, Deserializer<V> value) {
        return new Consumed<>(Format.reading(key), Format.reading(value));
    }

    /**
     * The key format, read by the route when it decodes a record before dispatch.
     */
    public Format<K> key() {
        return key;
    }

    /**
     * The value format, the other half of what the route needs to decode a record.
     */
    public Format<V> value() {
        return value;
    }

    /**
     * Names both formats, because a refusal about consumed types is read beside the route it came from and the type
     * name alone would not say which of the pair was wrong.
     */
    @Override
    public String toString() {
        return "Consumed(key=" + key + ", value=" + value + ")";
    }
}
