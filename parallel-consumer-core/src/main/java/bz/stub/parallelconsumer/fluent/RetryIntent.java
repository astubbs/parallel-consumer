package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.time.Duration;

/**
 * What the dispatch wrapper meant by the throw it is about to make, and how long the record should wait (KTD4).
 * <p>
 * The engine has one hand-back path - fail the record - so a park, a retry and everything the later units add all
 * leave the wrapper as an exception. This is the note the wrapper leaves for the retry-delay provider, which the
 * engine calls synchronously inside that failure path.
 *
 * <h2>Why the kind is a flag and not an enum</h2>
 * The Truth assertion generator discovers <b>every enum on the source path</b>, not only the classes listed in the
 * core pom, and writes one subject per simple type name per package into a class in the parent package. Two
 * consequences bite here, and neither failure names the enum that caused it: a second nested {@code Kind} in this
 * package collides with {@link Outcome.Kind}'s generated subject, and an enum nested in a <em>package-private</em>
 * class generates an import the parent package cannot resolve. So an internal enum in this package would have to be
 * public, which is a poor reason to put internal machinery on the API. A unit that needs more shapes than park and
 * retry - export, a breaker withhold, a stop fence - grows this class with what it needs and pays that cost then,
 * knowing what it is for.
 *
 * @see RetryIntents
 */
final class RetryIntent {

    private final boolean park;

    private final Duration delay;

    private RetryIntent(boolean park, Duration delay) {
        this.park = park;
        this.delay = delay;
    }

    /**
     * An ordinary attempt failed and the record should be tried again after the route's retry delay.
     */
    static RetryIntent retryAfter(Duration delay) {
        return new RetryIntent(false, delay);
    }

    /**
     * The record is out of attempts, or was declared hopeless: it waits for the far-future delay, which is to say
     * until an operator resumes it or a restart re-delivers it (R27).
     */
    static RetryIntent parkFor(Duration delay) {
        return new RetryIntent(true, delay);
    }

    boolean isPark() {
        return park;
    }

    Duration delay() {
        return delay;
    }

    @Override
    public String toString() {
        return "RetryIntent(" + (park ? "park" : "retry") + ", " + delay + ")";
    }
}
