package io.confluent.parallelconsumer.integrationTests.utils;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Lifecycle regression for {@link ManagedPCInstance}: one instance must never end up with two
 * concurrent {@code run()} invocations or two concurrent closers.
 * <p>
 * Guards the {@code ChaosRevokeUnderWorkCooperativeIT} failure under seed 8291601231857558952. The
 * conductor marks an instance STOPPED the moment {@code stopAsync()} returns (the close finishes in
 * the background), so a STOP_NO_DRAIN/RESTART pair could be redrawn while the previous restart was
 * still parked in {@code run()}'s close-wait loop. That submitted {@code run()} twice: two PC objects
 * raced on the {@code parallelConsumer} field, one was orphaned as a group member nobody would ever
 * close ({@code ZOMBIE_MEMBER/REBALANCE_BLOCKED}), and two threads closed the other's consumer
 * ({@code ConcurrentModificationException: KafkaConsumer is not safe for multi-threaded access}).
 * <p>
 * Pure lifecycle, no broker - and deliberately NOT tagged {@code chaos}: the invariant it guards is
 * relied on by every chaos run, so it gates each default integration build.
 */
class ManagedPCInstanceLifecycleIT {

    private static final int CLOSE_ENTRY_TIMEOUT_MS = 5_000;

    private ManagedPCInstance newInstance() {
        ManagedPCInstance.Config config = ManagedPCInstance.Config.builder()
                .commitMode(CommitMode.PERIODIC_CONSUMER_SYNC)
                .order(ProcessingOrder.UNORDERED)
                .inputTopic("lifecycle-regression-topic")
                .build();
        return new ManagedPCInstance(config, null, key -> {
        });
    }

    /**
     * The double-submission itself: a second start while the first is still queued must be refused,
     * not silently queued behind it.
     */
    @Test
    void secondStartIsRefusedWhileTheFirstIsStillInFlight() {
        ManagedPCInstance instance = newInstance();
        RecordingExecutor executor = new RecordingExecutor();

        assertThat(instance.start(executor)).isTrue();
        assertThat(executor.tasks).hasSize(1);

        assertThat(instance.start(executor)).isFalse();
        assertThat(executor.tasks).hasSize(1); // refused, not queued behind the first
    }

    /**
     * A stop drawn while a start is still queued must win: the queued {@code run()} aborts rather than
     * bringing up a PC the conductor believes is stopped and will therefore never close - the orphan
     * that became the zombie group member. Running the queued task must also release the guard, so the
     * next restart is accepted.
     */
    @Test
    void aStopWhileQueuedAbortsTheStartAndReleasesTheGuard() {
        ManagedPCInstance instance = newInstance();
        RecordingExecutor executor = new RecordingExecutor();

        assertThat(instance.start(executor)).isTrue();
        instance.stopAsync(); // conductor redraws a stop before the queued run() gets to execute

        // run() must abort before touching the (null) KafkaClientUtils - reaching the broker path
        // would NPE, so completing normally is itself the assertion that it aborted early
        executor.runAll();

        assertThat(instance.getParallelConsumer()).isNull(); // no orphan PC was created
        assertThat(instance.start(executor)).isTrue(); // guard released
        assertThat(executor.tasks).hasSize(1); // the aborted task was drained by runAll()
    }

    /**
     * Two closers on one PC put two threads inside the same {@code KafkaConsumer}, which is what threw
     * {@code ConcurrentModificationException} against the still-running poll thread.
     */
    @Test
    void aSecondAsyncStopDoesNotStartASecondCloser() throws InterruptedException {
        ManagedPCInstance instance = newInstance();
        @SuppressWarnings("unchecked")
        ParallelEoSStreamProcessor<String, String> pc = mock(ParallelEoSStreamProcessor.class);

        AtomicInteger closesEntered = new AtomicInteger();
        CountDownLatch releaseClose = new CountDownLatch(1);
        doAnswer(invocation -> {
            closesEntered.incrementAndGet();
            releaseClose.await(CLOSE_ENTRY_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            return null;
        }).when(pc).close();
        instance.setParallelConsumerForTest(pc);

        instance.stopAsync();
        awaitCloseEntered(closesEntered);

        instance.stopAsync(); // must be refused while the first close is still running
        Thread.sleep(200); // give a (wrongly) spawned second closer time to enter close()

        assertThat(closesEntered.get()).isEqualTo(1);
        assertThat(instance.isClosePending()).isTrue();

        releaseClose.countDown();
    }

    private void awaitCloseEntered(AtomicInteger closesEntered) throws InterruptedException {
        int waited = 0;
        while (closesEntered.get() == 0 && waited < CLOSE_ENTRY_TIMEOUT_MS) {
            Thread.sleep(20);
            waited += 20;
        }
        assertThat(closesEntered.get()).isEqualTo(1);
    }

    /** Captures submissions instead of running them, so the queued-start window stays observable. */
    private static class RecordingExecutor extends AbstractExecutorService {

        private final List<Runnable> tasks = new CopyOnWriteArrayList<>();

        void runAll() {
            List<Runnable> toRun = List.copyOf(tasks);
            tasks.clear();
            toRun.forEach(Runnable::run);
        }

        @Override
        public void execute(Runnable command) {
            tasks.add(command);
        }

        @Override
        public void shutdown() {
        }

        @Override
        public List<Runnable> shutdownNow() {
            return List.of();
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return true;
        }
    }
}
