package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Objects;

/**
 * A classifier's verdict on a failure to decode a record's bytes: <em>permanent</em>, or <em>transient</em> (R12).
 *
 * <h2>What is being decoded, and why the answer matters</h2>
 * A record arrives from Kafka as two byte arrays - a key and a value - and nothing more. The formats a route
 * declares are what turn those bytes into the key and value its function is handed, and that turning is decoding.
 * This API does it <b>inside the record's own attempt</b>, not on the thread that polled the broker, and the
 * placement is the whole point: a payload the route cannot read is then one record's problem, reported as that
 * record's outcome, rather than an exception on the poll thread that would take the instance down and leave every
 * other record on the partition unprocessed.
 * <p>
 * Which outcome it should be depends on <em>why</em> the bytes did not read, and only the user's code can tell.
 * A corrupt or wrong-format payload will never read, however often it is tried, so the record should park now and
 * spend none of its attempts on it. A schema registry that could not be reached will read perfectly well in a
 * minute, so the record should simply be retried. This type is how a route says which of the two it is looking at.
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

    /**
     * The exception the deserialiser threw, held so {@link #toString()} can name it. It is never read back out: what
     * travels on is a throw - {@link PermanentDecodeFailureException} wrapping this, or this one untouched.
     */
    private final Exception failure;

    /**
     * The verdict, as a flag rather than a second type, because there are exactly two arms and no third is coming - a
     * classifier is only ever handed a failure.
     */
    private final boolean permanent;

    /**
     * Private: {@link #permanentFailure} and {@link #transientFailure} are the whole vocabulary, and naming the arm
     * at the call site is what makes a one-line classifier readable.
     */
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

    /**
     * True when the payload will never decode, so the record parks now and spends no attempt (R12). False is an
     * ordinary failed attempt, which is also what an unclassified deserialiser failure amounts to.
     */
    public boolean isPermanent() {
        return permanent;
    }

    /**
     * Names the arm and the failure under it, so a log line about a parked record says which verdict put it there.
     */
    @Override
    public String toString() {
        return "Decode(" + (permanent ? "permanent" : "transient") + " failure: " + failure + ")";
    }
}
