package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Objects;

/**
 * One topic's way in: {@link #pipeInput(Object, Object)} puts a record on it, from the calling thread.
 *
 * <h2>Why a per-topic object, when the sandbox can take the topic as an argument</h2>
 * It is the shape the broker-free test drivers of the stream-processing libraries users compare us with have, and
 * the verb is theirs too, so a user arriving from one of them writes what they already know (KTD16). A test that
 * pipes many records into one topic names the topic once here rather than on every line, and a test that pipes one
 * record has {@link Sandbox#pipe(String, Object, Object)}. Both reach the same encoding and the same partition
 * choice: there is one way in, reached two ways, so neither can drift from the other.
 *
 * <h2>Two deliberate differences from theirs, both forced by what a route already declares</h2>
 * <b>No serialiser arguments.</b> Theirs are handed to the factory call, because that driver has no idea what a
 * topic carries; ours does - the route declared both halves of its format - and asking for them again would let a
 * test encode a record its own definition cannot read.
 * <p>
 * <b>A pipe returns the offset</b> rather than nothing, because the record lands on a real partition of a real mock
 * broker, and a test asserting on committed offsets needs to know where.
 *
 * @param <K> the key type this topic accepts - the type the route's key format reads on the fluent path, the
 *            instance's own key type on the classic one
 * @param <V> the value type, the same way
 */
@InterfaceStability.Unstable
public final class SandboxInputTopic<K, V> {

    /**
     * The topic every pipe through this object goes to. Held rather than passed, which is the whole point of the
     * object.
     */
    private final String topic;

    /**
     * Where a piped record actually goes - the sandbox's own pipe. Held as a function rather than as a sandbox
     * reference so that the fluent and the classic sandbox can each hand over their own, with their own types,
     * and this class needs to know about neither.
     */
    private final Piping<K, V> piping;

    /**
     * Package-private: an input topic is reached through {@code createInputTopic} on a sandbox, which is what
     * checks that the topic is one the definition actually consumes.
     */
    SandboxInputTopic(String topic, Piping<K, V> piping) {
        this.topic = Objects.requireNonNull(topic, "A topic must be supplied");
        this.piping = Objects.requireNonNull(piping, "Somewhere to pipe to must be supplied");
    }

    /**
     * The topic this object pipes into.
     */
    public String topic() {
        return topic;
    }

    /**
     * Pipes one record in, from the calling thread.
     * <p>
     * <b>Piping a record is not processing it.</b> This engine polls on its own thread, dispatches on a worker and
     * commits on the control thread, so the sandbox's {@code awaitSettled()} is what makes an assertion afterwards
     * mean anything - the one place this module cannot follow the drivers it takes its names from, their engines
     * being single-threaded.
     *
     * @param key   the key - may be null for a format that encodes one
     * @param value the value, of the type this topic's route reads
     * @return the offset it was published at
     * @throws IllegalStateException if the sandbox is no longer running
     */
    public long pipeInput(K key, V value) {
        return piping.pipe(topic, key, value);
    }

    /**
     * How an input topic reaches its sandbox's pipe. Package-private, and a method reference in both callers.
     *
     * @param <K> the key type the sandbox's pipe takes
     * @param <V> its value type
     */
    @FunctionalInterface
    interface Piping<K, V> {

        /**
         * @return the offset the record was published at
         */
        long pipe(String topic, K key, V value);
    }
}
