package bz.stub.parallelconsumer.examples.support;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Watches how much work an example actually had in flight at once, so its run summary can report
 * observed concurrency rather than claim it.
 * <p>
 * Example scaffolding, not a library type: an example's tagged (copied) region never references this
 * class directly - each example wraps it in a small local method that lives outside the tag.
 * <p>
 * Thread safe. Every counter is atomic and the thread-name set is concurrent, because the whole point
 * is that many Parallel Consumer worker threads call {@link #enter()} at the same moment.
 * <p>
 * The observed window runs from the <em>first</em> {@link #enter()} to the <em>last</em> exit, never
 * from application start: at example record counts, Parallel Consumer's subscribe / assign / first-poll
 * / commit cost is comparable to the processing window, so a clock started at application start would
 * report mostly startup cost wearing a benchmark's clothes.
 */
@Slf4j
public class ConcurrencyObserver {

    /**
     * Sentinel for "no observation yet". {@link Long#MIN_VALUE} so that the last-exit accumulator's
     * {@code max} is correct on its very first update, and so it can never collide with a real
     * {@link System#nanoTime()} reading.
     */
    private static final long NO_OBSERVATION = Long.MIN_VALUE;

    private final AtomicInteger inFlight = new AtomicInteger();

    private final AtomicInteger peakInFlight = new AtomicInteger();

    private final AtomicLong completed = new AtomicLong();

    private final Set<String> threadNames = ConcurrentHashMap.newKeySet();

    private final AtomicLong firstEnterNanos = new AtomicLong(NO_OBSERVATION);

    private final AtomicLong lastExitNanos = new AtomicLong(NO_OBSERVATION);

    /**
     * Opens an observation scope for one unit of work. Use it with try-with-resources, or prefer
     * {@link #observe(Runnable)} / {@link #observeAndReturn(Supplier)}, so the scope closes even when
     * the work throws - a leaked scope permanently inflates the in-flight count and every later peak.
     *
     * @return the scope, which must be closed by the same logical unit of work that opened it
     */
    public Scope enter() {
        firstEnterNanos.compareAndSet(NO_OBSERVATION, System.nanoTime());
        threadNames.add(Thread.currentThread().getName());
        int current = inFlight.incrementAndGet();
        peakInFlight.accumulateAndGet(current, Math::max);
        log.debug("Entered work scope on {} - {} now in flight", Thread.currentThread().getName(), current);
        return new Scope();
    }

    /**
     * Runs {@code work} inside an observation scope. The scope closes even if {@code work} throws, so a
     * failing record does not leak the in-flight count.
     */
    public void observe(Runnable work) {
        observeAndReturn(() -> {
            work.run();
            return null;
        });
    }

    /**
     * Runs {@code work} inside an observation scope and returns its result. The scope closes even if
     * {@code work} throws, so a failing record does not leak the in-flight count.
     */
    public <T> T observeAndReturn(Supplier<T> work) {
        try (Scope ignored = enter()) {
            return work.get();
        }
    }

    /**
     * Number of scopes open right now.
     */
    public int getInFlight() {
        return inFlight.get();
    }

    /**
     * The most scopes ever open at the same time. Monotonic - it never falls back when a later, smaller
     * burst of work runs.
     */
    public int getPeakInFlight() {
        return peakInFlight.get();
    }

    /**
     * Number of scopes that have closed, whether their work succeeded or threw.
     */
    public long getCompleted() {
        return completed.get();
    }

    /**
     * Names of every thread that opened at least one scope, sorted for a stable summary rendering.
     */
    public Set<String> getThreadNames() {
        return new TreeSet<>(threadNames);
    }

    public int getDistinctThreadCount() {
        return threadNames.size();
    }

    public boolean hasObservations() {
        return firstEnterNanos.get() != NO_OBSERVATION;
    }

    /**
     * {@link System#nanoTime()} reading of the first {@link #enter()}, or {@link Long#MIN_VALUE} if
     * nothing has been observed yet. Only meaningful relative to {@link #getLastExitNanos()}.
     */
    public long getFirstEnterNanos() {
        return firstEnterNanos.get();
    }

    /**
     * {@link System#nanoTime()} reading of the last scope close, or {@link Long#MIN_VALUE} if no scope
     * has closed yet.
     */
    public long getLastExitNanos() {
        return lastExitNanos.get();
    }

    /**
     * The processing window: first {@link #enter()} to last scope close. {@link Duration#ZERO} while
     * nothing has been observed, or while work is still in flight and no scope has closed yet.
     * <p>
     * Deliberately not measured from application start - see the class javadoc.
     */
    public Duration getObservedWindow() {
        long start = firstEnterNanos.get();
        long end = lastExitNanos.get();
        if (start == NO_OBSERVATION || end == NO_OBSERVATION) {
            return Duration.ZERO;
        }
        return Duration.ofNanos(end - start);
    }

    private void exit() {
        int remaining = inFlight.decrementAndGet();
        completed.incrementAndGet();
        lastExitNanos.accumulateAndGet(System.nanoTime(), Math::max);
        log.debug("Left work scope on {} - {} still in flight", Thread.currentThread().getName(), remaining);
    }

    /**
     * One unit of observed work. Closing is idempotent, so an accidental double close cannot drive the
     * in-flight count negative.
     */
    public class Scope implements AutoCloseable {

        private final AtomicBoolean closed = new AtomicBoolean();

        private Scope() {
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                exit();
            }
        }
    }
}
