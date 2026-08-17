package bz.stub.parallelconsumer.integrationTests.utils;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

    /** ManagedPCInstance.run()'s close-wait budget; the early-exit test must finish well inside it. */
    private static final long CLOSE_WAIT_BUDGET_MS = 10_000;

    /**
     * How long to keep watching for a second closer that must never appear. This is the reliability
     * knob for that negative assertion: a wrongly-spawned closer is started synchronously inside
     * {@code stopAsync()}, so it only has to be scheduled and reach the mocked {@code close()} - but
     * under CI contention thread-start latency alone can run into the hundreds of milliseconds.
     */
    private static final int SECOND_CLOSER_WATCH_MS = 1_000;

    /** A PC that never reports itself closed, so {@code run()} genuinely enters the close-wait loop. */
    @SuppressWarnings("unchecked")
    private static ParallelEoSStreamProcessor<String, String> neverClosingPc() {
        ParallelEoSStreamProcessor<String, String> pc = mock(ParallelEoSStreamProcessor.class);
        when(pc.isClosedOrFailed()).thenReturn(false);
        return pc;
    }

    /**
     * The close-wait loop's {@code !stopRequested} term. Without it a queued {@code run()} sleeps out
     * the full 10s budget before noticing the stop, holding a carrier thread on a bounded
     * work-stealing pool for the whole time - during exactly the churn this harness exists to create.
     * <p>
     * No other test reaches this loop: they leave {@code parallelConsumer} null, so the whole block
     * is skipped and only the post-loop abort runs.
     */
    @Test
    void aStopEndsTheCloseWaitImmediatelyRatherThanWaitingOutTheBudget() {
        ManagedPCInstance instance = BrokerlessInstances.newInstance("close-wait-topic");
        RecordingExecutor executor = new RecordingExecutor();
        instance.setParallelConsumerForTest(neverClosingPc());

        assertThat(instance.start(executor)).isTrue();
        instance.stopAsync(); // stop lands while the start is queued

        long startedAt = System.nanoTime();
        executor.runAll();
        long elapsedMs = (System.nanoTime() - startedAt) / 1_000_000;

        // the PC never reports closed, so without the term this cannot finish before the 10s budget
        assertThat(elapsedMs).isLessThan(CLOSE_WAIT_BUDGET_MS);
    }

    /**
     * The {@code stopRequested = false} reset in {@code start()}. Without it the flag stays true after
     * the first stop forever, so every later restart aborts on arrival and the fleet silently shrinks
     * to nothing - a failure that looks like the chaos scenario simply losing instances.
     * <p>
     * Proving it needs a start that runs to completion <em>past</em> the abort branch, which is why
     * no earlier test caught it: they all stop at "queued", or abort by design.
     */
    @Test
    void aRestartAfterAStopRunsRatherThanAbortingOnTheStaleFlag() {
        ManagedPCInstance instance = BrokerlessInstances.newInstance("restart-after-stop-topic");
        RecordingExecutor executor = new RecordingExecutor();

        assertThat(instance.start(executor)).isTrue();
        instance.stopAsync();
        executor.runAll(); // aborts, releasing the guard

        assertThat(instance.start(executor)).isTrue();
        // This run() must get PAST the abort branch. It then reaches the broker path and NPEs on the
        // null KafkaClientUtils - which RecordingExecutor surfaces. That NPE is the proof: it can
        // only be thrown from beyond the point a stale stopRequested would have returned at.
        AssertionError proceeded = assertThrows(AssertionError.class, executor::runAll);
        assertThat(proceeded).hasCauseThat().isInstanceOf(NullPointerException.class);
    }

    /**
     * The double-submission itself: a second start while the first is still queued must be refused,
     * not silently queued behind it.
     */
    @Test
    void secondStartIsRefusedWhileTheFirstIsStillInFlight() {
        ManagedPCInstance instance = BrokerlessInstances.newInstance("lifecycle-regression-topic");
        RecordingExecutor executor = new RecordingExecutor();

        assertThat(instance.start(executor)).isTrue();
        assertThat(executor.getTasks()).hasSize(1);

        assertThat(instance.start(executor)).isFalse();
        assertThat(executor.getTasks()).hasSize(1); // refused, not queued behind the first
    }

    /**
     * A stop drawn while a start is still queued must win: the queued {@code run()} aborts rather than
     * bringing up a PC the conductor believes is stopped and will therefore never close - the orphan
     * that became the zombie group member. Running the queued task must also release the guard, so the
     * next restart is accepted.
     */
    @Test
    void aStopWhileQueuedAbortsTheStartAndReleasesTheGuard() {
        ManagedPCInstance instance = BrokerlessInstances.newInstance("lifecycle-regression-topic");
        RecordingExecutor executor = new RecordingExecutor();

        assertThat(instance.start(executor)).isTrue();
        instance.stopAsync(); // conductor redraws a stop before the queued run() gets to execute

        // run() must abort before touching the (null) KafkaClientUtils - reaching the broker path
        // would NPE, so completing normally is itself the assertion that it aborted early
        executor.runAll();

        assertThat(instance.getParallelConsumer()).isNull(); // no orphan PC was created
        assertThat(instance.start(executor)).isTrue(); // guard released
        assertThat(executor.getTasks()).hasSize(1); // the aborted task was drained by runAll()
    }

    /**
     * Two closers on one PC put two threads inside the same {@code KafkaConsumer}, which is what threw
     * {@code ConcurrentModificationException} against the still-running poll thread.
     */
    @Test
    void aSecondAsyncStopDoesNotStartASecondCloser() throws InterruptedException {
        ManagedPCInstance instance = BrokerlessInstances.newInstance("lifecycle-regression-topic");
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
        await().atMost(Duration.ofMillis(CLOSE_ENTRY_TIMEOUT_MS))
                .untilAsserted(() -> assertThat(closesEntered.get()).isEqualTo(1));

        instance.stopAsync(); // must be refused while the first close is still running

        // A negative assertion: nothing must happen. Sampling once after a fixed sleep would pass
        // whenever a wrongly-spawned closer simply had not been scheduled yet - a silent green on a
        // loaded box, which is the failure this test exists to catch. Poll instead, so a second
        // closer fails the test the moment it enters close(), and hold the window long enough that
        // thread-start latency cannot hide one.
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(SECOND_CLOSER_WATCH_MS);
        while (System.nanoTime() < deadline) {
            assertThat(closesEntered.get()).isEqualTo(1);
            Thread.sleep(10);
        }
        assertThat(instance.isClosePending()).isTrue();

        releaseClose.countDown();
    }
}
