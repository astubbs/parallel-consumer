package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.annotation.InterfaceStability;

import java.time.Duration;
import java.util.Objects;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * A route's circuit breaker: a failure rate over a window of attempts, and how long the route stays open (R29).
 * <p>
 * Retries and the breaker answer different failures. A retry is one record's transient failure; the breaker is a
 * dependency that is down, so it opens on the first records rather than after each has exhausted its retries. A
 * failed attempt is a throw, a transient decode failure, or a park or export on exhaustion. No breaker state is
 * shared between routes: the breaker declared on the definition is the per-route default and each route takes a
 * {@link #copy()} of it (R6).
 * <p>
 * The policy is data (R18); the behaviour that reads it is a later unit.
 */
@InterfaceStability.Unstable
public final class CircuitBreakerPolicy {

    private final double failureRate;

    private int window = 100;

    private Duration openFor = Duration.ofSeconds(30);

    private int halfOpenProbes = 5;

    private CircuitBreakerPolicy(double failureRate) {
        this.failureRate = failureRate;
    }

    /**
     * @param failureRate the fraction of the window that must fail before the route opens, between zero and one
     */
    public static CircuitBreakerPolicy failureRate(double failureRate) {
        if (!(failureRate > 0d) || failureRate > 1d) {
            throw new IllegalArgumentException(msg("A circuit breaker failureRate ({}) must be above zero and at "
                    + "most one - it is the fraction of the attempt window that must fail", failureRate));
        }
        return new CircuitBreakerPolicy(failureRate);
    }

    /**
     * How many recent attempts the rate is measured over.
     */
    public CircuitBreakerPolicy over(int attempts) {
        if (attempts < 1) {
            throw new IllegalArgumentException(msg("A circuit breaker window ({}) must be at least one attempt",
                    attempts));
        }
        this.window = attempts;
        return this;
    }

    /**
     * How long an open route withholds its records, counting no attempt against them, before letting probes through.
     */
    public CircuitBreakerPolicy openFor(Duration duration) {
        Objects.requireNonNull(duration, "An open duration must be supplied");
        if (duration.isNegative() || duration.isZero()) {
            throw new IllegalArgumentException(msg("A circuit breaker openFor ({}) must be positive", duration));
        }
        this.openFor = duration;
        return this;
    }

    /**
     * How many records are let through half-open. The route closes when they all succeed and re-opens for another
     * open duration when any of them fails.
     */
    public CircuitBreakerPolicy halfOpenProbes(int probes) {
        if (probes < 1) {
            throw new IllegalArgumentException(msg("A circuit breaker halfOpenProbes ({}) must be at least one",
                    probes));
        }
        this.halfOpenProbes = probes;
        return this;
    }

    public double failureRate() {
        return failureRate;
    }

    public int window() {
        return window;
    }

    public Duration openFor() {
        return openFor;
    }

    public int halfOpenProbes() {
        return halfOpenProbes;
    }

    /**
     * An independent copy, so a route overriding part of the instance default does not edit the default itself.
     */
    public CircuitBreakerPolicy copy() {
        CircuitBreakerPolicy copy = new CircuitBreakerPolicy(failureRate);
        copy.window = window;
        copy.openFor = openFor;
        copy.halfOpenProbes = halfOpenProbes;
        return copy;
    }

    @Override
    public String toString() {
        return "CircuitBreakerPolicy(failureRate=" + failureRate + ", over=" + window + ", openFor=" + openFor
                + ", halfOpenProbes=" + halfOpenProbes + ")";
    }
}
