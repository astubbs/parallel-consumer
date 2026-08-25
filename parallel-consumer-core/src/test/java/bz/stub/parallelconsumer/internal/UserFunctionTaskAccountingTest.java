package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Pins the conservation arithmetic in {@link UserFunctionTaskAccounting} - that the four counters only ever move the
 * way its javadoc enumerates, and that the two derived figures cannot go negative.
 * <p>
 * The counters replace {@code ThreadPoolExecutor.getQueue().size()} and {@code getActiveCount()}, so the important
 * property is not that they hold some value but that <b>every path in and out is counted</b>. A missed increment on
 * one side drifts the derived figure permanently, and the figure gates how much work Parallel Consumer fetches from
 * the broker - so drift stalls the consumer while it still looks alive.
 * <p>
 * The agreement with a real executor's own figures is asserted separately, in
 * {@link WorkerPoolAccountingAgreementTest}, which is the only independent oracle these numbers have.
 */
class UserFunctionTaskAccountingTest {

    @Test
    void aFreshAccountingReadsZeroOnBothDerivedFigures() {
        var accounting = new UserFunctionTaskAccounting();

        // What PipelinePressureLoggingTest depends on: a processor that has been constructed but never started must
        // report an empty pool, not an unknown one.
        assertThat(accounting.getQueued()).isEqualTo(0);
        assertThat(accounting.getActive()).isEqualTo(0);
    }

    @Test
    void aSubmittedTaskIsQueuedUntilItStartsAndActiveUntilItFinishes() {
        var accounting = new UserFunctionTaskAccounting();

        accounting.onSubmitting();
        assertThat(accounting.getQueued()).isEqualTo(1);
        assertThat(accounting.getActive()).isEqualTo(0);

        accounting.onTaskStarted();
        assertThat(accounting.getQueued()).isEqualTo(0);
        assertThat(accounting.getActive()).isEqualTo(1);

        accounting.onTaskFinished();
        assertThat(accounting.getQueued()).isEqualTo(0);
        assertThat(accounting.getActive()).isEqualTo(0);
    }

    /**
     * Occupancy - the admission sampler's figure - spans the WHOLE dispatched-to-finished life, submit-to-start
     * handoff included: a task the pool has been handed but has not begun running still holds its slot. Sampling
     * {@code getActive()} at the post-dispatch instant instead read the handoff as an empty slot, which is what
     * made a saturated broker run classify as starved (the 2026-08-25 comparison-IT freeze's second act).
     */
    @Test
    void occupancyCountsTheSubmitToStartHandoffAsAHeldSlot() {
        var accounting = new UserFunctionTaskAccounting();

        accounting.onSubmitting();
        assertWithMessage("submitted-not-started holds its slot")
                .that(accounting.getOccupied()).isEqualTo(1);

        accounting.onTaskStarted();
        assertThat(accounting.getOccupied()).isEqualTo(1);

        accounting.onSubmitting(); // a second task enters the handoff while the first runs
        assertWithMessage("occupied is queued plus active, whatever the split")
                .that(accounting.getOccupied()).isEqualTo(2);

        accounting.onTaskFinished();
        assertThat(accounting.getOccupied()).isEqualTo(1);
    }

    /** A rejected submit releases its slot - occupancy must not count a task that will never run. */
    @Test
    void occupancyReleasesARejectedSubmitsSlot() {
        var accounting = new UserFunctionTaskAccounting();

        accounting.onSubmitting();
        accounting.onSubmitRejected();

        assertThat(accounting.getOccupied()).isEqualTo(0);
    }

    @Test
    void aRejectedSubmitLeavesNothingQueued() {
        var accounting = new UserFunctionTaskAccounting();

        accounting.onSubmitting();
        accounting.onSubmitRejected();

        // The defect this guards: without onSubmitRejected(), a pool that starts rejecting - AbortPolicy, or any
        // executor after shutdown - leaves the queue depth permanently high by one per rejection, and the pressure
        // system reads a backlog that does not exist.
        assertThat(accounting.getQueued()).isEqualTo(0);
        assertThat(accounting.getActive()).isEqualTo(0);
        assertThat(accounting.getNeverStartedTotal()).isEqualTo(1);
    }

    @Test
    void tasksDiscardedOnShutdownLeaveNothingQueued() {
        var accounting = new UserFunctionTaskAccounting();

        for (int i = 0; i < 5; i++) {
            accounting.onSubmitting();
        }
        accounting.onTaskStarted();
        accounting.onTaskFinished();

        // the four that never ran, drained out of the executor's queue on the way down
        accounting.onTasksDiscarded(4);

        assertThat(accounting.getQueued()).isEqualTo(0);
        assertThat(accounting.getActive()).isEqualTo(0);
    }

    @Test
    void discardingNothingIsNotCountedAsSomething() {
        var accounting = new UserFunctionTaskAccounting();
        accounting.onSubmitting();

        // Both shutdown paths routinely hand back an empty list - a virtual-thread executor always does, because
        // every task it accepted already has a thread. That must not move the counter.
        accounting.onTasksDiscarded(0);

        assertThat(accounting.getQueued()).isEqualTo(1);
        assertThat(accounting.getNeverStartedTotal()).isEqualTo(0);
    }

    @Test
    void theConservationInvariantHoldsOnceTheWorkIsDone() {
        var accounting = new UserFunctionTaskAccounting();

        for (int i = 0; i < 10; i++) {
            accounting.onSubmitting();
        }
        for (int i = 0; i < 7; i++) {
            accounting.onTaskStarted();
            accounting.onTaskFinished();
        }
        accounting.onSubmitRejected();
        accounting.onTasksDiscarded(2);

        // The whole point of deriving rather than maintaining: the relationship between the counters is a statement
        // a test can check, where "is this running total correct?" is not.
        assertThat(accounting.getSubmittedTotal())
                .isEqualTo(accounting.getStartedTotal() + accounting.getNeverStartedTotal());
        assertThat(accounting.getStartedTotal()).isEqualTo(accounting.getFinishedTotal());
        assertThat(accounting.getQueued()).isEqualTo(0);
        assertThat(accounting.getActive()).isEqualTo(0);
    }

    /**
     * The ordering hazard the class exists to rule out. A virtual thread can be running its task before
     * {@code submit()} has returned, so the submit side must increment first; and both derived figures read their
     * subtrahends first so that a start observed against a not-yet-visible submission still reads as zero rather
     * than as minus one.
     */
    @Test
    void neitherDerivedFigureEverGoesNegativeUnderConcurrentSubmitAndRun() throws Exception {
        var accounting = new UserFunctionTaskAccounting();
        int tasks = 20_000;
        var random = new Random(20260822L);
        var sawNegative = new AtomicBoolean();
        var readerStarted = new CountDownLatch(1);
        var done = new AtomicBoolean();

        ExecutorService workers = Executors.newFixedThreadPool(8);
        Thread reader = new Thread(() -> {
            readerStarted.countDown();
            while (!done.get()) {
                if (accounting.getQueued() < 0 || accounting.getActive() < 0) {
                    sawNegative.set(true);
                }
            }
        }, "accounting-reader");
        reader.start();
        readerStarted.await();

        try {
            var latch = new CountDownLatch(tasks);
            for (int i = 0; i < tasks; i++) {
                accounting.onSubmitting();
                int spin = random.nextInt(4);
                workers.execute(() -> {
                    accounting.onTaskStarted();
                    try {
                        // Vary how long each task holds "started but not finished" so the reader below observes
                        // real interleavings rather than a lock-step submit/run/finish rhythm.
                        for (int s = 0; s < spin; s++) {
                            Thread.yield();
                        }
                    } finally {
                        accounting.onTaskFinished();
                        latch.countDown();
                    }
                });
            }
            assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
        } finally {
            done.set(true);
            reader.join(TimeUnit.SECONDS.toMillis(10));
            workers.shutdownNow();
        }

        assertThat(sawNegative.get()).isFalse();
        assertThat(accounting.getQueued()).isEqualTo(0);
        assertThat(accounting.getActive()).isEqualTo(0);
        assertThat(accounting.getSubmittedTotal()).isEqualTo(tasks);
        assertThat(accounting.getFinishedTotal()).isEqualTo(tasks);
    }
}
