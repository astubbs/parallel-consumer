package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.state.ModelUtils;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertThat;

/**
 * The only independent check the derived pool figures have: on the platform path the executor keeps the same two
 * numbers itself, so {@link UserFunctionTaskAccounting} can be held against them.
 * <p>
 * This matters because {@code getNumberOfUserFunctionsQueued()} is load-bearing for the default engine that every
 * user runs, not only for the opt-in virtual-thread mode. The alternative design - branching on
 * {@code instanceof ThreadPoolExecutor} and returning a hardcoded zero for a virtual-thread pool - has no oracle at
 * all, because the branch that would be wrong is the branch with nothing to compare against.
 * <p>
 * A virtual-thread pool has no queue and no active count to compare with, which is the whole reason the counters
 * exist; its correctness rests on {@link UserFunctionTaskAccountingTest}'s conservation invariant instead.
 */
class WorkerPoolAccountingAgreementTest {

    private static final int POOL_SIZE = 2;

    private static final int TASKS = 6;

    @Test
    void theDerivedFiguresMatchTheExecutorsOwnWhileWorkIsHeldUp() throws Exception {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST))
                .maxConcurrency(POOL_SIZE)
                // Pinned, not inherited. This test IS the platform path's oracle, so it has to keep running on the
                // platform path even when CI's execution-mode axis sets -Dpc.virtualThreads=true for the suite -
                // otherwise the one test that can check these figures against an independent source quietly stops
                // checking them in the very run that changed them.
                .useVirtualThreads(false)
                .build();

        try (var pc = new TestParallelEoSStreamProcessor<>(options)) {
            ExecutorService pool = pc.getWorkerThreadPool().get();
            assertThat(pool).isInstanceOf(ThreadPoolExecutor.class);
            var tpe = (ThreadPoolExecutor) pool;
            var accounting = pc.userFunctionTaskAccounting();

            var release = new CountDownLatch(1);
            var running = new CountDownLatch(POOL_SIZE);

            // Submitted through the accounting the way submitWorkToPoolInner does - counted before the submit,
            // started inside the task, finished in a finally.
            for (int i = 0; i < TASKS; i++) {
                accounting.onSubmitting();
                pool.execute(() -> {
                    accounting.onTaskStarted();
                    try {
                        running.countDown();
                        release.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        accounting.onTaskFinished();
                    }
                });
            }

            assertThat(running.await(30, TimeUnit.SECONDS)).isTrue();
            awaitExecutorQuiescence(tpe, TASKS - POOL_SIZE);

            // The comparison this test exists for. Both figures, both sources, at a point where neither is zero.
            assertThat(accounting.getQueued()).isEqualTo(tpe.getQueue().size());
            assertThat(accounting.getActive()).isEqualTo(tpe.getActiveCount());
            assertThat(accounting.getQueued()).isEqualTo(TASKS - POOL_SIZE);
            assertThat(accounting.getActive()).isEqualTo(POOL_SIZE);

            release.countDown();
            awaitDrained(accounting);

            assertThat(accounting.getQueued()).isEqualTo(tpe.getQueue().size());
            assertThat(accounting.getActive()).isEqualTo(tpe.getActiveCount());
            assertThat(accounting.getSubmittedTotal())
                    .isEqualTo(accounting.getStartedTotal() + accounting.getNeverStartedTotal());
            assertThat(accounting.getStartedTotal()).isEqualTo(accounting.getFinishedTotal());
        }
    }

    /**
     * The executor's own queue size lags the submits by however long the pool takes to pick tasks up, so comparing
     * immediately would be comparing against a value still in motion. Wait for it to settle at the expected depth
     * rather than sleeping a guessed interval.
     */
    private static void awaitExecutorQuiescence(ThreadPoolExecutor tpe, int expectedQueueDepth) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (tpe.getQueue().size() != expectedQueueDepth && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(tpe.getQueue().size()).isEqualTo(expectedQueueDepth);
    }

    /**
     * Closing with work still in the executor's queue must leave the accounting balanced.
     * <p>
     * Those tasks are dropped rather than run, so nothing downstream will ever count them: no
     * {@code onTaskStarted()}, no {@code onTaskFinished()}. Left unaccounted, the derived queue depth stays high by
     * the number discarded, forever. Today nothing reads it after close, which is exactly why the gap would sit
     * there unnoticed until something did - and that is the shape of a drift defect, not a harmless omission.
     * <p>
     * Found by sabotage: removing the {@code onTasksDiscarded} call in
     * {@code AbstractParallelEoSStreamProcessor#discardQueuedWork()} left the whole suite green until this test
     * existed.
     */
    @Test
    void queuedWorkDiscardedOnCloseIsAccountedFor() throws Exception {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST))
                .maxConcurrency(1)
                .useVirtualThreads(false)
                // The occupied worker never finishes on its own, so close() has to reach the interrupt path.
                // Two seconds rather than the ten-second default keeps this test quick without changing what it
                // exercises.
                .shutdownTimeout(java.time.Duration.ofSeconds(2))
                .build();

        var pc = new TestParallelEoSStreamProcessor<>(options);
        var accounting = pc.userFunctionTaskAccounting();
        ExecutorService pool = pc.getWorkerThreadPool().get();

        var running = new CountDownLatch(1);
        var release = new CountDownLatch(1);

        // One task occupies the single worker; the rest pile up in the queue and will never run.
        for (int i = 0; i < TASKS; i++) {
            accounting.onSubmitting();
            pool.execute(() -> {
                accounting.onTaskStarted();
                try {
                    running.countDown();
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    accounting.onTaskFinished();
                }
            });
        }
        assertThat(running.await(30, TimeUnit.SECONDS)).isTrue();
        int queuedBeforeDiscard = accounting.getQueued();
        assertThat(queuedBeforeDiscard).isGreaterThan(0);

        // Driven directly rather than through close(). A processor that was never started transitions UNUSED ->
        // CLOSED without going through innerDoClose(), so close() here would drain nothing and the test would pass
        // whether or not the accounting call existed - which is precisely how the first version of this test
        // stayed green under sabotage.
        pc.discardQueuedWork();

        release.countDown();
        pc.close();

        assertThat(accounting.getQueued()).isEqualTo(0);
        assertThat(accounting.getSubmittedTotal())
                .isEqualTo(accounting.getStartedTotal() + accounting.getNeverStartedTotal());
    }

    /**
     * The hazard that only exists once the pool is virtual, made deterministic.
     * <p>
     * On a {@link ThreadPoolExecutor}, {@code submit()} enqueues and returns; a worker picks the task up later, so
     * counting the submission after that call is <em>almost</em> always fine, and a race test does not reliably
     * catch it - verified: reversing the read order and running 20,000 concurrent tasks stayed green. A
     * virtual-thread-per-task executor starts the task immediately, so the task's own {@code onTaskStarted()} can
     * land <b>before</b> the submit call returns, and a submission counted after that call would make the derived
     * queue depth negative.
     * <p>
     * This makes it deterministic on any JDK, including the JDK 17 default lane where no real virtual thread exists
     * to demonstrate it: the accounting is read at the instant the executor is handed the task, which is strictly
     * before {@code submit()} returns. Counted correctly, one task is queued at that moment; counted after the
     * submit, none is.
     */
    @Test
    void theSubmissionIsCountedBeforeTheExecutorEverSeesTheTask() {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST))
                .useVirtualThreads(false)
                .build();

        var observedAtHandover = new ArrayList<Integer>();
        var mu = new ModelUtils();

        try (var pc = new TestParallelEoSStreamProcessor<String, String>(options) {
            @Override
            protected ExecutorService setupWorkerPool(int poolSize) {
                return new ObservingInlineExecutorService(observedAtHandover);
            }
        }) {
            pc.setWm(mu.getModule().workManager());
            // The observer needs the accounting, which is only reachable once the processor exists.
            ObservingInlineExecutorService.accounting = pc.userFunctionTaskAccounting();
            observedAtHandover.clear();

            pc.submitWorkToPool(
                    context -> UniLists.of("done"),
                    ignored -> {
                    },
                    UniLists.of(mu.createWorkFor(0)));
        } finally {
            ObservingInlineExecutorService.accounting = null;
        }

        assertThat(observedAtHandover).hasSize(1);
        // The discriminator: 1 if the submission was counted before the hand-over, 0 if it was counted after.
        assertThat(observedAtHandover.get(0)).isEqualTo(1);
    }

    /**
     * Records the derived queue depth at the moment the executor is handed a task, then runs it inline.
     */
    private static final class ObservingInlineExecutorService extends AbstractExecutorService {

        static volatile UserFunctionTaskAccounting accounting;

        private final List<Integer> observations;

        private volatile boolean shutdown;

        ObservingInlineExecutorService(List<Integer> observations) {
            this.observations = observations;
        }

        @Override
        public void execute(Runnable command) {
            UserFunctionTaskAccounting current = accounting;
            if (current != null) {
                observations.add(current.getQueued());
            }
            command.run();
        }

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdown = true;
            return UniLists.of();
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return shutdown;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return true;
        }
    }

    private static void awaitDrained(UserFunctionTaskAccounting accounting) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (accounting.getActive() + accounting.getQueued() > 0 && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(accounting.getActive() + accounting.getQueued()).isEqualTo(0);
    }
}
