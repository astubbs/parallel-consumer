package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.time.Duration;
import java.util.Objects;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * One topic - or one set of topics (R5) - bound to one processing function, with its own policy.
 * <p>
 * A route is a statement: it starts at {@code topic(...)} or a format-named helper on the definition and ends at
 * {@link #process}, so a formatter cannot hide the boundary between two routes. Everything between those two is
 * data: a retry limit, a delay, an admission target, a park policy, a breaker. Each of them is a per-route setting
 * with an instance default, and a route that declares none takes a copy of every default (R6).
 *
 * <h2>The type parameters, and why producing is a compile error by default</h2>
 * A fresh route is {@code Route<byte[], byte[], Void, Void>}: raw bytes in, nothing produced. {@link #consumed}
 * re-types the consumed pair, {@link #produced} re-types the produced pair, and each hands back a differently typed
 * view over the same route. Because the produced pair is {@code Void, Void} until {@code produced(...)} is called,
 * {@link Outcome#produce} for any real record does not compile on a route that never declared what it produces
 * (R3, AE8).
 *
 * @param <K>  the consumed key type
 * @param <V>  the consumed value type
 * @param <PK> the produced key type, {@link Void} until {@link #produced} is called
 * @param <PV> the produced value type, {@link Void} until {@link #produced} is called
 */
@InterfaceStability.Unstable
public final class Route<K, V, PK, PV> {

    /**
     * The route itself. This class is only a typed view over it: {@link #consumed} and {@link #produced} each hand
     * back a new {@code Route} wrapping this same object, so several views of different types can exist for one
     * route and every one of them declares into the same state.
     */
    private final RouteState state;

    /**
     * Package-private, so a route can only be started by the definition or re-typed by the two methods that hand a
     * new view over existing state. A user-reachable constructor would be a route the definition never registered,
     * and so a route that never runs.
     */
    Route(RouteState state) {
        this.state = state;
    }

    /**
     * Declare what this route consumes, when a format helper does not fit: a non-string key, or a deserialiser of
     * your own (R4).
     */
    public <K2, V2> Route<K2, V2, PK, PV> consumed(Consumed<K2, V2> consumed) {
        Objects.requireNonNull(consumed, "Consumed types must be supplied");
        requireReadable(consumed.key(), "key");
        requireReadable(consumed.value(), "value");
        state.consumed(consumed.key(), consumed.value());
        return new Route<>(state);
    }

    /**
     * Declare what this route produces. Now, and only now, does {@link #process} accept a function that returns
     * {@link Outcome#produce} (R3).
     */
    public <PK2, PV2> Route<K, V, PK2, PV2> produced(Produced<PK2, PV2> produced) {
        Objects.requireNonNull(produced, "Produced types must be supplied");
        state.produced(produced.key(), produced.value());
        return new Route<>(state);
    }

    /**
     * The route's one function, and the end of the statement (KD3).
     */
    public void process(ProcessFunction<K, V, PK, PV> function) {
        Objects.requireNonNull(function, msg("A processing function must be supplied for topic {}",
                state.describeTopics()));
        if (state.hasFunction()) {
            throw new IllegalArgumentException(msg("Topic {} already has a processing function - a topic has exactly "
                    + "one route and a route has exactly one function; declare a second topic instead",
                    state.describeTopics()));
        }
        state.function(function);
    }

    /**
     * How many attempts after the first this route allows before the record parks (R10). Overrides the instance
     * default for this route only.
     */
    public Route<K, V, PK, PV> retryLimit(int attempts) {
        if (attempts < 0) {
            throw new IllegalArgumentException(msg("retryLimit ({}) on topic {} cannot be negative - it counts the "
                            + "attempts after the first; use retryForever() to ask for unbounded retries",
                    attempts, state.describeTopics()));
        }
        state.ownRetryLimit(attempts);
        return this;
    }

    /**
     * Retry forever, as the classic API always has. Opt-in on purpose: without a limit a record that can never
     * succeed holds a worker, or its key's shard, indefinitely (R10).
     */
    public Route<K, V, PK, PV> retryForever() {
        state.ownUnboundedRetries();
        return this;
    }

    /**
     * How long a failed record waits before its next attempt.
     */
    public Route<K, V, PK, PV> retryDelay(Duration delay) {
        state.ownRetryDelay(requirePositive(delay, "retryDelay"));
        return this;
    }

    /**
     * This route's admission target: how many of its records may be in flight at once. Routes do not compete for one
     * shared limit (R23, KD6).
     */
    public Route<K, V, PK, PV> concurrency(int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException(msg("concurrency ({}) on topic {} must be at least one - it is this "
                    + "route's admission target", limit, state.describeTopics()));
        }
        state.ownConcurrency(limit);
        return this;
    }

    /**
     * What happens once this route's records run out of attempts (R27).
     */
    public Route<K, V, PK, PV> afterRetries(AfterRetries policy) {
        state.ownAfterRetries(Objects.requireNonNull(policy, "An after-retries policy must be supplied"));
        return this;
    }

    /**
     * Be told once when a record on this route parks: after its last attempt, before its offset commits (R16).
     * <p>
     * Optional sugar over the parked outcome, not a second construct the definition needs - the park is counted and
     * queryable whether anybody observes it or not (KD3, R28). Overrides the instance default for this route only.
     *
     * @see ParkObserver for what arrives, and what a throw from it does (nothing)
     */
    public Route<K, V, PK, PV> onParked(ParkObserver<K, V> observer) {
        state.ownParkObserver(Objects.requireNonNull(observer, "A park observer must be supplied"));
        return this;
    }

    /**
     * Refused: ordering is the instance default in this version.
     * <p>
     * Per-route ordering needs a change at the engine's shard-key seam - the shard key and the shard's head check -
     * so it is a later milestone (R6). The method exists so the refusal can name the setting at the point you reach
     * for it, rather than leaving you to find out from a document.
     * <p>
     * Not marked {@code @DoNotCall}, which Error Prone suggests for a method that always throws: the same
     * setting must be refused on the wire form too, which has no compiler, and one message in one place is
     * what keeps the two bindings saying the same thing (AE7).
     *
     * @throws IllegalArgumentException always
     */
    @SuppressWarnings("DoNotCallSuggester")
    public Route<K, V, PK, PV> ordering(ProcessingOrder ordering) {
        throw new IllegalArgumentException(msg("ordering ({}) cannot be declared on topic {} in this version - "
                        + "ordering is the instance default, declared with defaultOrdering(...) on the definition. "
                        + "Per-route ordering needs a change at the engine's shard-key seam and is a later "
                        + "milestone (R6).",
                ordering, state.describeTopics()));
    }

    /**
     * Refused: the commit mode is instance-wide.
     * <p>
     * The engine has one consumer, so there is one offset commit per group, and the transactional mode wraps that
     * commit in one producer's transaction. A per-route commit mode would be two producers and two transactions over
     * one consumer, which is two instances - that is the line (KD11, R6).
     * <p>
     * Not marked {@code @DoNotCall}, which Error Prone suggests for a method that always throws: the same
     * setting must be refused on the wire form too, which has no compiler, and one message in one place is
     * what keeps the two bindings saying the same thing (AE7).
     *
     * @throws IllegalArgumentException always
     */
    @SuppressWarnings("DoNotCallSuggester")
    public Route<K, V, PK, PV> commitMode(CommitMode commitMode) {
        throw new IllegalArgumentException(msg("commitMode ({}) cannot be declared on topic {} - it is instance-wide, "
                        + "declared with commitMode(...) on the definition: the engine has one consumer and one "
                        + "commit, so a per-route commit mode would be a second instance (KD11, R6)",
                commitMode, state.describeTopics()));
    }

    /**
     * A consumed format must be able to read: a write-only {@link Format} on the consumed side is a route whose
     * records nothing could decode, and it is refused here - at the call that declared it, naming the side and the
     * topic - rather than surviving to the first record (R4).
     *
     * @param side {@code key} or {@code value}, so the refusal says which half of the pair is wrong
     */
    private void requireReadable(Format<?> format, String side) {
        if (!format.hasDeserializer()) {
            throw new IllegalArgumentException(msg("Consumed {} format {} on topic {} has no deserializer, so "
                            + "nothing could read this route's records", side, format, state.describeTopics()));
        }
    }

    /**
     * Rejects a negative duration and returns the value, so the caller reads as one statement. Zero is allowed: a
     * delay of none is a legitimate declaration, unlike a wait that runs backwards.
     *
     * @param setting the name the user wrote, so the refusal quotes their call and not an internal field
     */
    private Duration requirePositive(Duration value, String setting) {
        Objects.requireNonNull(value, msg("A {} must be supplied", setting));
        if (value.isNegative()) {
            throw new IllegalArgumentException(msg("{} ({}) on topic {} cannot be negative", setting, value,
                    state.describeTopics()));
        }
        return value;
    }

    /**
     * Delegates, so that a route named in a message reads the same whichever typed view of it the reader happens to
     * hold - the type parameters are the compiler's business and say nothing a user would recognise.
     */
    @Override
    public String toString() {
        return state.toString();
    }
}
