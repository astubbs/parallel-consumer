package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.OptionalInt;
import java.util.Set;

/**
 * A route's data, separate from the typed {@link Route} facade the user chains calls on.
 * <p>
 * They are separate because declaring {@link Produced} types <em>re-types</em> a route - that is what makes producing
 * from a route which declared none a compile error (R3) - and a type parameter cannot change on an object that
 * already exists. So {@code produced(...)} hands back a second {@code Route} view over this same state, and the
 * definition only ever holds the state.
 * <p>
 * A setting left null here was not declared on the route, and resolves to the definition's per-route default, taken
 * as an independent copy so that a route which overrides part of a policy does not edit the default every other
 * route shares (R6).
 */
class RouteState implements RouteView {

    private final ParallelConsumerDefinition owner;

    private final Set<String> topics;

    private Format<?> consumedKey;

    private Format<?> consumedValue;

    private Format<?> producedKey;

    private Format<?> producedValue;

    private ProcessFunction<?, ?, ?, ?> function;

    /**
     * This route's own retry limit, as a tri-state that cannot be set inconsistently: <b>null</b> means the route
     * declared none and takes the definition's default, <b>empty</b> means it declared retries unbounded, and a
     * present value is the limit it declared. It was two fields - an {@code Integer} limit and an
     * {@code ownUnboundedRetries} flag - whose agreement was maintained by hand in the two setters below.
     */
    private OptionalInt ownRetryLimit;

    private Duration ownRetryDelay;

    private Integer ownConcurrency;

    private AfterRetries ownAfterRetries;

    private ParkObserver<?, ?> ownParkObserver;

    /**
     * Whether {@link #resolveDefaults()} has run. <b>Volatile, and it is the publication edge for every
     * {@code resolved*} field below</b>: resolveDefaults writes them all and then writes this flag last, and every
     * reader tests this flag before reading them, so the volatile write/read pair is what makes those plain fields
     * visible to the worker threads that read a route's policy once per failed record. Without it the only edge
     * was the engine's thread-start, inherited from the definition being validated on the thread that then starts
     * the engine - true today, stated nowhere, and lost the moment anything resolves later than that.
     */
    private volatile boolean resolved;

    private OptionalInt resolvedRetryLimit;

    private Duration resolvedRetryDelay;

    private int resolvedConcurrency;

    private AfterRetries resolvedAfterRetries;

    private ParkObserver<?, ?> resolvedParkObserver;

    RouteState(ParallelConsumerDefinition owner, Set<String> topics, Format<?> consumedKey, Format<?> consumedValue) {
        this.owner = owner;
        this.topics = Collections.unmodifiableSet(new LinkedHashSet<>(topics));
        this.consumedKey = consumedKey;
        this.consumedValue = consumedValue;
    }

    /**
     * Fills every resolved field from the route's own declarations, falling back to the definition's per-route
     * defaults. Idempotent, and run by validation before any view is handed out.
     */
    void resolveDefaults() {
        if (resolved) {
            return;
        }
        resolvedRetryLimit = ownRetryLimit != null ? ownRetryLimit : owner.defaultRetryLimitValue();
        resolvedRetryDelay = ownRetryDelay != null ? ownRetryDelay : owner.defaultRetryDelayValue();
        resolvedConcurrency = ownConcurrency != null ? ownConcurrency : owner.defaultConcurrencyValue();
        AfterRetries afterRetries = ownAfterRetries != null ? ownAfterRetries : owner.defaultAfterRetriesValue();
        resolvedAfterRetries = afterRetries == null ? AfterRetries.park() : afterRetries.copy();
        // Not copied: an observer is the user's own object, and there is nothing about it a route could override
        // part of. It is wired like every other setting - the route's own, or the instance default (R6, R16).
        resolvedParkObserver = ownParkObserver != null ? ownParkObserver : owner.defaultParkObserverValue();
        // Last, and volatile: everything above is published by this write. See the field.
        resolved = true;
    }

    private void invalidateResolution() {
        resolved = false;
    }

    @Override
    public Set<String> topics() {
        return topics;
    }

    @Override
    public Format<?> consumedKey() {
        return consumedKey;
    }

    @Override
    public Format<?> consumedValue() {
        return consumedValue;
    }

    @Override
    public Format<?> producedKey() {
        return producedKey;
    }

    @Override
    public Format<?> producedValue() {
        return producedValue;
    }

    @Override
    public boolean producesRecords() {
        return producedKey != null;
    }

    @Override
    public OptionalInt retryLimit() {
        resolveDefaults();
        return resolvedRetryLimit;
    }

    @Override
    public Duration retryDelay() {
        resolveDefaults();
        return resolvedRetryDelay;
    }

    @Override
    public int concurrency() {
        resolveDefaults();
        return resolvedConcurrency;
    }

    @Override
    public AfterRetries afterRetries() {
        resolveDefaults();
        return resolvedAfterRetries;
    }

    ProcessFunction<?, ?, ?, ?> function() {
        return function;
    }

    /**
     * The observer told once when a record on this route parks, or null when neither the route nor the definition
     * declared one (R16).
     * <p>
     * Deliberately <b>not</b> on {@link RouteView}: that view is the wire-shaped read of a route, and an observer is
     * a callback rather than data (R18). The dispatch wrapper reads it from here.
     */
    ParkObserver<?, ?> parkObserver() {
        resolveDefaults();
        return resolvedParkObserver;
    }

    boolean hasFunction() {
        return function != null;
    }

    /**
     * The topics, rendered for a validation message that must name the offending route.
     */
    String describeTopics() {
        return describeTopics(topics);
    }

    /**
     * The same rendering for a caller that holds a route's topics without holding the route - {@link RouteHandle},
     * which is handed only the set. One owner, because the two spellings of it must name a route identically:
     * a refusal and a handle's {@code toString} are read side by side.
     */
    static String describeTopics(Set<String> topics) {
        return topics.size() == 1 ? topics.iterator().next() : topics.toString();
    }

    void consumed(Format<?> key, Format<?> value) {
        this.consumedKey = key;
        this.consumedValue = value;
    }

    void produced(Format<?> key, Format<?> value) {
        this.producedKey = key;
        this.producedValue = value;
    }

    void function(ProcessFunction<?, ?, ?, ?> function) {
        this.function = function;
    }

    void ownRetryLimit(int limit) {
        this.ownRetryLimit = OptionalInt.of(limit);
        invalidateResolution();
    }

    void ownUnboundedRetries() {
        this.ownRetryLimit = OptionalInt.empty();
        invalidateResolution();
    }

    void ownRetryDelay(Duration delay) {
        this.ownRetryDelay = delay;
        invalidateResolution();
    }

    void ownConcurrency(int concurrency) {
        this.ownConcurrency = concurrency;
        invalidateResolution();
    }

    void ownAfterRetries(AfterRetries policy) {
        this.ownAfterRetries = policy;
        invalidateResolution();
    }

    void ownParkObserver(ParkObserver<?, ?> observer) {
        this.ownParkObserver = observer;
        invalidateResolution();
    }

    @Override
    public String toString() {
        return "Route(" + describeTopics() + ")";
    }
}
