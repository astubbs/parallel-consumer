package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;

import java.time.Duration;
import java.util.OptionalInt;

/**
 * The per-route settings' instance-wide defaults: the value a route takes when it declares none of its own (KD11,
 * R6).
 * <p>
 * These are the settings whose fluent setters carry a {@code default} prefix, and they are the only ones a route
 * resolves against - which is why they are one object with one reader, {@link RouteState#resolveDefaults()}, rather
 * than six fields on the definition reached through six accessors. The genuinely instance-wide settings, the commit
 * mode and the close path, are deliberately <b>not</b> here: nothing copies them into a route, and putting them
 * beside these would blur the one distinction this package's surface is built to make.
 * <p>
 * Mutable, because the fluent setters are: a definition is built by a sequence of calls and every one of them may
 * move a default until the definition starts. Confined to the definition's own thread by construction - a
 * definition is assembled and started by one caller - so nothing here is synchronised.
 */
final class InstanceDefaults {

    /**
     * The ordering guarantee every route resolves to. Key ordering by default: it keeps a key's records in order
     * while letting unrelated keys run at once, which is the guarantee this library exists to give.
     */
    private ProcessingOrder ordering = ProcessingOrder.KEY;

    /**
     * How many records the instance may have in flight at once - handed to the engine as its own limit.
     * <p>
     * It was the target a route copied, and the engine was given the sum over the routes (R23). Per-route
     * concurrency is withdrawn from this milestone (owner-directed, 2026-09-12), so nothing copies this and the
     * engine is given it directly; it stays in this class, beside ordering, because it is still declared as a
     * per-route default and expected to become one again.
     */
    private int concurrency = ParallelConsumerOptions.DEFAULT_MAX_CONCURRENCY;

    /**
     * Ten attempts after the first, then park - the default that at last gives the engine's inert failure-history
     * setting of ten a meaning (R10).
     */
    private OptionalInt retryLimit = OptionalInt.of(10);

    /**
     * How long a failed record waits before its next attempt, on every route that declares no delay. It is also
     * what the dispatch wrapper answers for a topic no route claims, which the engine can ask about while a
     * partition is being revoked.
     */
    private Duration retryDelay = Duration.ofSeconds(1);

    /**
     * What a route copies when it declares no after-retries policy of its own (R27). Null until something declares
     * one, and a route that resolves against null parks in place - the default that holds the record and commits
     * nothing past it, rather than one that discards work.
     */
    private AfterRetries afterRetries;

    /**
     * The park observer a route copies when it declares none (R16). Wildcard-typed because one instance-wide
     * observer spans routes whose consumed types differ, so there are no types it could be declared over.
     */
    private ParkObserver<?, ?> parkObserver;

    /**
     * Read by a route resolving its own, even though per-route ordering cannot yet differ from it: the route asks
     * the same question as every other setting, so the seam is already where a later milestone needs it (R6).
     */
    ProcessingOrder ordering() {
        return ordering;
    }

    void ordering(ProcessingOrder value) {
        this.ordering = value;
    }

    /**
     * The instance's limit, read straight into the engine's {@code maxConcurrency} - see the field for what this
     * used to mean.
     */
    int concurrency() {
        return concurrency;
    }

    void concurrency(int value) {
        this.concurrency = value;
    }

    /**
     * Empty means retry forever, which is a declared setting rather than a missing one - so a route copying this
     * inherits "no limit" as deliberately as it inherits a number (R10).
     */
    OptionalInt retryLimit() {
        return retryLimit;
    }

    void retryLimit(OptionalInt value) {
        this.retryLimit = value;
    }

    /**
     * Never null: a route resolving against this always ends up with a delay, so no route has to answer what to
     * wait when nothing declared one.
     */
    Duration retryDelay() {
        return retryDelay;
    }

    void retryDelay(Duration value) {
        this.retryDelay = value;
    }

    /**
     * Null when the definition declared none, which a route resolves to parking in place rather than to nothing -
     * the default that holds a record and commits nothing past it (R27).
     */
    AfterRetries afterRetries() {
        return afterRetries;
    }

    void afterRetries(AfterRetries value) {
        this.afterRetries = value;
    }

    /**
     * Null when nothing declared one, in which case a route with no observer of its own tells nobody it parked -
     * the parked view is still the record of it (R16, R28).
     */
    ParkObserver<?, ?> parkObserver() {
        return parkObserver;
    }

    void parkObserver(ParkObserver<?, ?> value) {
        this.parkObserver = value;
    }
}
