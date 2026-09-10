package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Objects;

/**
 * The three-way result of a route's decode step (R12): a value, a permanent failure, or a transient one.
 * <p>
 * A permanent failure is parked at once without consuming attempts; a transient one is a failed attempt like any
 * other. A stock Kafka {@link org.apache.kafka.common.serialization.Deserializer} that throws yields a
 * <em>transient</em> failure, because it cannot tell a corrupt payload from a registry outage - a route that needs
 * the distinction says so with {@link Formats#classifyDecodeFailures}.
 * <p>
 * This type is the classifier's vocabulary. What travels to the dispatch wrapper is a throw:
 * {@link PermanentDecodeFailureException} for permanent, and the original exception for transient.
 *
 * @param <T> the value type being decoded
 */
@InterfaceStability.Unstable
public final class Decode<T> {

    private final T value;

    private final Exception failure;

    private final boolean permanent;

    private Decode(T value, Exception failure, boolean permanent) {
        this.value = value;
        this.failure = failure;
        this.permanent = permanent;
    }

    public static <T> Decode<T> value(T value) {
        return new Decode<>(value, null, false);
    }

    /**
     * This payload will never decode - park it now rather than spending its attempts on it (R12, R27).
     */
    public static <T> Decode<T> permanentFailure(Exception cause) {
        return new Decode<>(null, Objects.requireNonNull(cause, "A cause must be supplied"), true);
    }

    /**
     * Decoding failed for a reason that may pass - a registry outage, say. A failed attempt under R9 and R10.
     */
    public static <T> Decode<T> transientFailure(Exception cause) {
        return new Decode<>(null, Objects.requireNonNull(cause, "A cause must be supplied"), false);
    }

    public boolean isFailure() {
        return failure != null;
    }

    public boolean isPermanent() {
        return permanent;
    }

    public T value() {
        return value;
    }

    public Exception failure() {
        return failure;
    }

    @Override
    public String toString() {
        if (!isFailure()) {
            return "Decode(value)";
        }
        return "Decode(" + (permanent ? "permanent" : "transient") + " failure: " + failure + ")";
    }
}
