package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Objects;

/**
 * A classifier's verdict on a decode failure (R12): <em>permanent</em>, or <em>transient</em>.
 * <p>
 * A permanent failure is parked at once without consuming attempts; a transient one is a failed attempt like any
 * other. A stock Kafka {@link org.apache.kafka.common.serialization.Deserializer} that throws yields a
 * <em>transient</em> failure, because it cannot tell a corrupt payload from a registry outage - a route that needs
 * the distinction says so with {@link Formats#classifyDecodeFailures}.
 * <p>
 * <b>There is no success arm, because a classifier is only ever asked about a failure.</b> Its input is the
 * exception the deserialiser threw, so a verdict carrying a decoded value could never be constructed from one; the
 * decoded value travels the ordinary return path instead. This type is the classifier's whole vocabulary, and what
 * travels on from it to the dispatch wrapper is a throw: {@link PermanentDecodeFailureException} for permanent, and
 * the original exception, untouched, for transient.
 *
 * @param <T> the value type the wrapped format decodes. Nothing here holds a {@code T}; the parameter exists so
 *            that {@link Formats#classifyDecodeFailures}'s {@code Function<Exception, Decode<T>>} ties the verdict
 *            to the format being wrapped, which is what lets a lambda be written without naming the type.
 */
@InterfaceStability.Unstable
public final class Decode<T> {

    private final Exception failure;

    private final boolean permanent;

    private Decode(Exception failure, boolean permanent) {
        this.failure = failure;
        this.permanent = permanent;
    }

    /**
     * This payload will never decode - park it now rather than spending its attempts on it (R12, R27).
     */
    public static <T> Decode<T> permanentFailure(Exception cause) {
        return new Decode<>(Objects.requireNonNull(cause, "A cause must be supplied"), true);
    }

    /**
     * Decoding failed for a reason that may pass - a registry outage, say. A failed attempt under R9 and R10.
     */
    public static <T> Decode<T> transientFailure(Exception cause) {
        return new Decode<>(Objects.requireNonNull(cause, "A cause must be supplied"), false);
    }

    public boolean isPermanent() {
        return permanent;
    }

    @Override
    public String toString() {
        return "Decode(" + (permanent ? "permanent" : "transient") + " failure: " + failure + ")";
    }
}
