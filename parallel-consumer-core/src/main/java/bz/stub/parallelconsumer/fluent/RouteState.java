package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;

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
     * Null means "not declared on this route": take the definition's default.
     */
    private Integer ownRetryLimit;

    private boolean ownUnboundedRetries;

    private Duration ownRetryDelay;

    private Integer ownConcurrency;

    private AfterRetries ownAfterRetries;

    private CircuitBreakerPolicy ownCircuitBreaker;

    private boolean resolved;

    private OptionalInt resolvedRetryLimit;

    private Duration resolvedRetryDelay;

    private int resolvedConcurrency;

    private AfterRetries resolvedAfterRetries;

    private CircuitBreakerPolicy resolvedCircuitBreaker;

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
        resolvedRetryLimit = ownUnboundedRetries
                ? OptionalInt.empty()
                : ownRetryLimit != null ? OptionalInt.of(ownRetryLimit) : owner.defaultRetryLimitValue();
        resolvedRetryDelay = ownRetryDelay != null ? ownRetryDelay : owner.defaultRetryDelayValue();
        resolvedConcurrency = ownConcurrency != null ? ownConcurrency : owner.defaultConcurrencyValue();
        AfterRetries afterRetries = ownAfterRetries != null ? ownAfterRetries : owner.defaultAfterRetriesValue();
        resolvedAfterRetries = afterRetries == null ? AfterRetries.park() : afterRetries.copy();
        CircuitBreakerPolicy breaker =
                ownCircuitBreaker != null ? ownCircuitBreaker : owner.defaultCircuitBreakerValue();
        resolvedCircuitBreaker = breaker == null ? null : breaker.copy();
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
    public ProcessingOrder ordering() {
        return owner.defaultOrderingValue();
    }

    @Override
    public AfterRetries afterRetries() {
        resolveDefaults();
        return resolvedAfterRetries;
    }

    @Override
    public CircuitBreakerPolicy circuitBreaker() {
        resolveDefaults();
        return resolvedCircuitBreaker;
    }

    ProcessFunction<?, ?, ?, ?> function() {
        return function;
    }

    boolean hasFunction() {
        return function != null;
    }

    /**
     * The topics, rendered for a validation message that must name the offending route.
     */
    String describeTopics() {
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
        this.ownRetryLimit = limit;
        this.ownUnboundedRetries = false;
        invalidateResolution();
    }

    void ownUnboundedRetries() {
        this.ownRetryLimit = null;
        this.ownUnboundedRetries = true;
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

    void ownCircuitBreaker(CircuitBreakerPolicy policy) {
        this.ownCircuitBreaker = policy;
        invalidateResolution();
    }

    @Override
    public String toString() {
        return "Route(" + describeTopics() + ")";
    }
}
