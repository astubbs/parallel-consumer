package io.confluent.parallelconsumer.examples.support;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * The shape every example's fake downstream dependency takes: a named service that costs a fixed
 * amount of time to call and fails a fixed fraction of calls.
 * <p>
 * Example scaffolding, not a library type. Deliberately domain-free - the industry naming (the fraud
 * scorer, the pricing service, the carrier API) belongs to the example that owns it, which supplies the
 * actual work as a lambda. Keeping the domain out here is what stops this growing into a framework the
 * reader has to learn before reading the example.
 * <p>
 * The latency is a sleep, so it is simulated. The concurrency around it - how many of these calls
 * Parallel Consumer has in flight at once - is real, and is what the examples are demonstrating.
 * <p>
 * Failures are deterministic, not random: every Nth call fails, N derived from the failure fraction. A
 * seeded random would still let a test see a different number of failures if calls were reordered.
 */
@Slf4j
@Getter
public class SimulatedService {

    /**
     * Names the service in logs and failure messages, e.g. "fraud scorer".
     */
    private final String name;

    /**
     * How long every call takes. Simulated with a sleep.
     */
    private final Duration latency;

    /**
     * Fraction of calls that fail, in {@code [0, 1)}. 0 never fails.
     */
    private final double failureFraction;

    /**
     * Every {@code failEveryNth}-th call fails. 0 means never.
     */
    private final long failEveryNth;

    private final AtomicLong callCount = new AtomicLong();

    private final AtomicLong failureCount = new AtomicLong();

    public SimulatedService(String name, Duration latency, double failureFraction) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("name must be set - it identifies the service in logs and failures");
        }
        if (latency == null || latency.isNegative()) {
            throw new IllegalArgumentException("latency must be non-negative, was: " + latency);
        }
        if (failureFraction < 0d || failureFraction >= 1d) {
            throw new IllegalArgumentException("failureFraction must be in [0, 1), was: " + failureFraction);
        }
        this.name = name;
        this.latency = latency;
        this.failureFraction = failureFraction;
        this.failEveryNth = failureFraction == 0d ? 0L : Math.max(1L, Math.round(1d / failureFraction));
    }

    /**
     * A service that always succeeds.
     */
    public SimulatedService(String name, Duration latency) {
        this(name, latency, 0d);
    }

    /**
     * Waits out the service's latency, then either fails (deterministically) or runs the caller's work.
     *
     * @param work the domain-specific work this service stands in for
     * @return whatever {@code work} returned
     * @throws SimulatedFailureException on the calls the failure fraction says should fail
     */
    public <T> T call(Supplier<T> work) {
        long callNumber = callCount.incrementAndGet();
        sleepForLatency();
        if (failEveryNth > 0 && callNumber % failEveryNth == 0) {
            failureCount.incrementAndGet();
            throw new SimulatedFailureException(name + " failed on call " + callNumber
                    + " (simulated, 1 in " + failEveryNth + ")");
        }
        return work.get();
    }

    /**
     * {@link #call(Supplier)} for work with no result.
     */
    public void run(Runnable work) {
        call(() -> {
            work.run();
            return null;
        });
    }

    private void sleepForLatency() {
        long nanos = latency.toNanos();
        if (nanos <= 0) {
            return;
        }
        try {
            TimeUnit.NANOSECONDS.sleep(nanos);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SimulatedFailureException(name + " was interrupted while simulating latency", e);
        }
    }

    /**
     * What a simulated downstream failure looks like to the example - an ordinary unchecked exception,
     * so Parallel Consumer's retry behaviour sees exactly what a real failure would look like.
     */
    public static class SimulatedFailureException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        public SimulatedFailureException(String message) {
            super(message);
        }

        public SimulatedFailureException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
