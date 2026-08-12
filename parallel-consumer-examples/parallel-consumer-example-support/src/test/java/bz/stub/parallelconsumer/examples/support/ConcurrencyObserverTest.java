package io.confluent.parallelconsumer.examples.support;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Every concurrency claim here is forced by a {@link CyclicBarrier}, never by sleeping and hoping: the
 * barrier makes "all N were in flight at once" a fact of the test rather than a timing accident.
 */
@Slf4j
class ConcurrencyObserverTest {

    private static final int BARRIER_TIMEOUT_SECONDS = 30;

    @Test
    void peakAndDistinctThreadsAreObservedUnderForcedConcurrency() {
        ConcurrencyObserver observer = new ConcurrencyObserver();

        runConcurrently(observer, 4);

        assertThat(observer.getPeakInFlight())
                .as("all four entrants were held inside their scopes by the barrier")
                .isEqualTo(4);
        assertThat(observer.getDistinctThreadCount()).isEqualTo(4);
        assertThat(observer.getThreadNames()).hasSize(4);
    }

    @Test
    void inFlightReturnsToZeroAndCompletedCountsEveryScope() {
        ConcurrencyObserver observer = new ConcurrencyObserver();

        for (int i = 0; i < 5; i++) {
            try (ConcurrencyObserver.Scope ignored = observer.enter()) {
                assertThat(observer.getInFlight()).isEqualTo(1);
            }
        }

        assertThat(observer.getInFlight()).isZero();
        assertThat(observer.getCompleted()).isEqualTo(5);
        assertThat(observer.getPeakInFlight()).isEqualTo(1);
    }

    @Test
    void peakNeverDecreasesWhenALaterBurstIsSmaller() {
        ConcurrencyObserver observer = new ConcurrencyObserver();

        runConcurrently(observer, 3);
        assertThat(observer.getPeakInFlight()).isEqualTo(3);

        runConcurrently(observer, 2);

        assertThat(observer.getPeakInFlight())
                .as("peak is the high water mark of the whole run, not of the latest burst")
                .isEqualTo(3);
        assertThat(observer.getCompleted()).isEqualTo(5);
        assertThat(observer.getInFlight()).isZero();
    }

    @Test
    void scopeClosesWhenTheWorkThrows() {
        ConcurrencyObserver observer = new ConcurrencyObserver();

        assertThatThrownBy(() -> observer.observe(() -> {
            assertThat(observer.getInFlight()).isEqualTo(1);
            throw new IllegalStateException("simulated failure");
        })).isInstanceOf(IllegalStateException.class);

        assertThat(observer.getInFlight())
                .as("a failing record must not leak the in-flight count")
                .isZero();
        assertThat(observer.getCompleted()).isEqualTo(1);
    }

    @Test
    void tryWithResourcesScopeClosesWhenTheBodyThrows() {
        ConcurrencyObserver observer = new ConcurrencyObserver();

        assertThatThrownBy(() -> {
            try (ConcurrencyObserver.Scope ignored = observer.enter()) {
                throw new IllegalStateException("simulated failure");
            }
        }).isInstanceOf(IllegalStateException.class);

        assertThat(observer.getInFlight()).isZero();
        assertThat(observer.getCompleted()).isEqualTo(1);
    }

    @Test
    void closingAScopeTwiceDoesNotDoubleCount() {
        ConcurrencyObserver observer = new ConcurrencyObserver();

        ConcurrencyObserver.Scope scope = observer.enter();
        scope.close();
        scope.close();

        assertThat(observer.getInFlight()).isZero();
        assertThat(observer.getCompleted()).isEqualTo(1);
    }

    @Test
    void observeAndReturnPassesTheResultBackThroughTheScope() {
        ConcurrencyObserver observer = new ConcurrencyObserver();

        String result = observer.observeAndReturn(() -> "authorised");

        assertThat(result).isEqualTo("authorised");
        assertThat(observer.getCompleted()).isEqualTo(1);
        assertThat(observer.getInFlight()).isZero();
    }

    @Test
    void theWindowSpansFirstEnterToLastExitAndNothingElse() {
        ConcurrencyObserver observer = new ConcurrencyObserver();

        assertThat(observer.hasObservations()).isFalse();
        assertThat(observer.getObservedWindow())
                .as("nothing observed yet, so there is no window - never a clock started at application start")
                .isEqualTo(Duration.ZERO);

        runConcurrently(observer, 2);

        assertThat(observer.hasObservations()).isTrue();
        // exact arithmetic, not a wall-clock threshold: the window IS last exit minus first enter
        assertThat(observer.getObservedWindow())
                .isEqualTo(Duration.ofNanos(observer.getLastExitNanos() - observer.getFirstEnterNanos()));
    }

    /**
     * Runs {@code entrants} scopes that are provably open at the same time: each enters its scope, then
     * waits on the barrier, so none can leave until all have arrived.
     */
    private void runConcurrently(ConcurrencyObserver observer, int entrants) {
        AtomicInteger threadNumber = new AtomicInteger();
        ThreadFactory namedThreads = runnable ->
                new Thread(runnable, "observer-test-worker-" + threadNumber.incrementAndGet());
        ExecutorService pool = Executors.newFixedThreadPool(entrants, namedThreads);
        try {
            CyclicBarrier allInside = new CyclicBarrier(entrants);
            List<Future<?>> futures = new ArrayList<>(entrants);
            for (int i = 0; i < entrants; i++) {
                futures.add(pool.submit(() -> {
                    try (ConcurrencyObserver.Scope ignored = observer.enter()) {
                        allInside.await(BARRIER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    }
                    return null;
                }));
            }
            for (Future<?> future : futures) {
                future.get(BARRIER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            throw new IllegalStateException("concurrent entrants did not all reach the barrier", e);
        } finally {
            pool.shutdownNow();
        }
    }
}
