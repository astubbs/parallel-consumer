package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * The terminal outcome a route's processing function reports for one record.
 * <p>
 * Every record reaches exactly one terminal outcome - succeeded, filtered, parked or exported - and a retry is a step
 * towards one of these rather than an outcome of its own: a retry is reported by throwing, never by returning
 * (R7, R9).
 *
 * <h2>Why this type is generic</h2>
 * The two parameters are the route's <em>produced</em> key and value types, and they exist so that a route which has
 * not declared produced types cannot return a produced record. A route declared with no {@link Produced} is typed
 * {@code Route<K, V, Void, Void>}, so its function must return an {@code Outcome<Void, Void>} and
 * {@link #produce(ProducerRecord[])} for any real record cannot be assigned to it. Declaring produced types re-types
 * the route, and only then does {@code produce} compile (R3). The wire form, which has no compiler, refuses a
 * produced record from a non-producing route at definition time instead.
 * <p>
 * {@link #succeeded()}, {@link #filtered()}, {@link #park(String)} and {@link #stop(String)} carry no produced record,
 * so they infer whichever pair the route asks for and are available on every route.
 *
 * @param <PK> the route's produced key type, or {@link Void} on a route that declares no produced types
 * @param <PV> the route's produced value type, or {@link Void} on a route that declares no produced types
 */
@InterfaceStability.Unstable
public final class Outcome<PK, PV> {

    /**
     * Which terminal outcome was reported. The dispatch wrapper maps each of these onto the engine.
     */
    @InterfaceStability.Unstable
    public enum Kind {
        /**
         * A normal return: the record completes and its offset commits (R7).
         */
        SUCCEEDED,
        /**
         * The record was deliberately not processed. It completes and commits exactly as a success does, and is
         * counted separately (R8).
         */
        FILTERED,
        /**
         * The record completes, and the records it carries are sent on the produce path first (R3).
         */
        PRODUCE,
        /**
         * The record is hopeless: park it now rather than spending its remaining attempts (R8, R27).
         */
        PARK,
        /**
         * A request about the <em>instance</em>, not a terminal outcome of the record: stop fetching work and close.
         * The stopping record is left incomplete so it is delivered again after a restart (R24).
         */
        STOP
    }

    /**
     * The two outcomes that carry nothing, so one instance of each serves every record and every route.
     * <p>
     * This class is immutable and defines no {@code equals}, so its identity is unobservable - the same reason
     * {@code Collections.emptyList()} is a singleton. They were allocated per record on the success path, which is
     * the hottest path this library has.
     */
    private static final Outcome<?, ?> SUCCEEDED_INSTANCE =
            new Outcome<>(Kind.SUCCEEDED, Collections.emptyList(), null);

    private static final Outcome<?, ?> FILTERED_INSTANCE =
            new Outcome<>(Kind.FILTERED, Collections.emptyList(), null);

    private final Kind kind;

    private final List<ProducerRecord<PK, PV>> records;

    private final String reason;

    private Outcome(Kind kind, List<ProducerRecord<PK, PV>> records, String reason) {
        this.kind = kind;
        this.records = records;
        this.reason = reason;
    }

    /**
     * The record was processed. Available on every route, producing or not.
     */
    @SuppressWarnings("unchecked")
    public static <PK, PV> Outcome<PK, PV> succeeded() {
        return (Outcome<PK, PV>) SUCCEEDED_INSTANCE;
    }

    /**
     * The record was deliberately skipped: it completes and commits like a success and is counted separately (R8).
     */
    @SuppressWarnings("unchecked")
    public static <PK, PV> Outcome<PK, PV> filtered() {
        return (Outcome<PK, PV>) FILTERED_INSTANCE;
    }

    /**
     * Park this record now, skipping its remaining attempts, because the function already knows it is hopeless (R8).
     *
     * @param reason recorded against the parked record and reported by the parked view (R28)
     */
    public static <PK, PV> Outcome<PK, PV> park(String reason) {
        return new Outcome<>(Kind.PARK, Collections.<ProducerRecord<PK, PV>>emptyList(),
                Objects.requireNonNull(reason,
                        "A park reason must be supplied - it is what the parked view reports"));
    }

    /**
     * Ask the instance to stop (R24). The instance fetches no new work and closes on its declared close path; this
     * record is left incomplete, so a restart delivers it again and the function will stop again unless the
     * definition's author breaks that loop.
     *
     * @param reason recorded once, and reported to the caller awaiting shutdown
     */
    public static <PK, PV> Outcome<PK, PV> stop(String reason) {
        return new Outcome<>(Kind.STOP, Collections.<ProducerRecord<PK, PV>>emptyList(),
                Objects.requireNonNull(reason,
                        "A stop reason must be supplied - it is what the awaiting caller is told"));
    }

    /**
     * Produce these records, then complete this one. Only compiles on a route that declared {@link Produced} types,
     * and the records are typed by those (R3).
     */
    // @SafeVarargs promises callers this array is never written to; "varargs" silences the same point being made
    // again at the one place it is read, where Arrays.asList cannot see the promise.
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <PK, PV> Outcome<PK, PV> produce(ProducerRecord<PK, PV>... produced) {
        return produce(Arrays.asList(produced));
    }

    /**
     * @see #produce(ProducerRecord[])
     */
    public static <PK, PV> Outcome<PK, PV> produce(List<ProducerRecord<PK, PV>> produced) {
        Objects.requireNonNull(produced,
                "Produced records must not be null - return Outcome.succeeded() to produce nothing");
        return new Outcome<>(Kind.PRODUCE, Collections.unmodifiableList(new ArrayList<>(produced)), null);
    }

    public Kind kind() {
        return kind;
    }

    /**
     * The records to produce - empty for every kind but {@link Kind#PRODUCE}.
     */
    public List<ProducerRecord<PK, PV>> records() {
        return records;
    }

    /**
     * The reason given for {@link Kind#PARK} or {@link Kind#STOP}, otherwise null.
     */
    public String reason() {
        return reason;
    }

    @Override
    public String toString() {
        return "Outcome(" + kind + (reason == null ? "" : ", reason=" + reason)
                + (records.isEmpty() ? "" : ", records=" + records.size()) + ")";
    }
}
