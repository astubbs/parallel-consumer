package bz.stub.parallelconsumer.internal.utils;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import pl.tlinkowski.unij.api.UniLists;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertThat;
import static java.time.Duration.ofSeconds;
import static org.awaitility.Awaitility.await;

/**
 * The queue-to-{@link java.util.stream.Stream} bridge waits for results instead of ending on an empty queue.
 *
 * @author Antony Stubbs
 * @see Java8StreamUtils
 * @see <a href="https://github.com/confluentinc/parallel-consumer/issues/912">confluentinc#912</a>
 */
class Java8StreamUtilsTest {

    /**
     * The regression. An empty queue is not the end of the stream while the source is still running - the old
     * bridge returned false here and the caller's terminal operation finished on the spot.
     */
    @Timeout(30)
    @Test
    void anEmptyQueueDoesNotEndTheStreamWhileTheSourceIsRunning() throws Exception {
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        var finished = new AtomicBoolean(false);
        List<String> collected = new CopyOnWriteArrayList<>();
        var done = new CountDownLatch(1);

        var consumer = new Thread(() -> {
            Java8StreamUtils.setupStreamFromQueue(queue, finished::get).forEach(collected::add);
            done.countDown();
        }, "test-consumer");
        consumer.start();

        // nothing to take yet: the consumer must wait rather than finish
        Thread.sleep(300);
        assertThat(done.getCount()).isEqualTo(1);

        queue.add("late");
        finished.set(true);

        assertThat(done.await(20, TimeUnit.SECONDS)).isTrue();
        assertThat(collected).containsExactly("late");
    }

    /**
     * Anything queued before the source finished belongs to the caller, so it is delivered rather than
     * dropped when the stream ends.
     */
    @Timeout(30)
    @Test
    void drainsWhatIsAlreadyQueuedThenEndsOnceTheSourceIsFinished() {
        BlockingQueue<String> queue = new LinkedBlockingQueue<>(UniLists.of("one", "two", "three"));

        List<String> collected = Java8StreamUtils.setupStreamFromQueue(queue, () -> true)
                .collect(Collectors.toList());

        assertThat(collected).containsExactly("one", "two", "three").inOrder();
        assertThat(queue).isEmpty();
    }

    /**
     * The race the second poll exists for, and the only thing that covers it.
     * <p>
     * A result can be queued in the window between the timed poll giving up on an empty queue and the source
     * reporting finished. Without the second look it would be dropped - and silently losing a result that was
     * already produced and acknowledged is the same class of defect as
     * <a href="https://github.com/confluentinc/parallel-consumer/issues/912">confluentinc#912</a> itself, just
     * one result at a time instead of all of them.
     * <p>
     * The window is reproduced rather than waited for: the source queues its element on the first
     * finished-check, which by construction happens after that poll has already timed out.
     */
    @Timeout(30)
    @Test
    void anElementQueuedAsTheSourceFinishesIsStillDelivered() {
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        var finishedChecks = new AtomicInteger();

        BooleanSupplier sourceFinished = () -> {
            if (finishedChecks.getAndIncrement() == 0) {
                queue.add("slipped in");
            }
            return true;
        };

        List<String> collected = Java8StreamUtils.setupStreamFromQueue(queue, sourceFinished)
                .collect(Collectors.toList());

        assertThat(collected).containsExactly("slipped in");
        assertThat(queue).isEmpty();
    }

    /**
     * A consumer asked to stop ends its stream instead of waiting out the source, and leaves the interrupt flag
     * set - the thread belongs to the caller, so swallowing its interrupt would strand whatever asked it to stop.
     * <p>
     * The source here never finishes, so ending is only reachable through the interrupt: if that path stopped
     * working this test would hang rather than pass.
     */
    @Timeout(30)
    @Test
    void interruptingAWaitingConsumerEndsTheStreamAndLeavesTheFlagSet() throws Exception {
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        var flagStillSet = new AtomicBoolean();
        var done = new CountDownLatch(1);

        var consumer = new Thread(() -> {
            Java8StreamUtils.setupStreamFromQueue(queue, () -> false).forEach(element -> {
            });
            flagStillSet.set(Thread.currentThread().isInterrupted());
            done.countDown();
        }, "test-consumer");
        consumer.start();

        await("the consumer to park on the empty queue")
                .atMost(ofSeconds(10))
                .until(() -> consumer.getState() == Thread.State.TIMED_WAITING
                        || consumer.getState() == Thread.State.WAITING);
        consumer.interrupt();

        assertThat(done.await(20, TimeUnit.SECONDS)).isTrue();
        assertThat(flagStillSet.get()).isTrue();
    }

    /**
     * Pins the "not splittable" claim the spliterator's javadoc makes. A split half would race the other for the
     * same live queue, so there is no correct way to divide it - stated as a test rather than only in prose.
     */
    @Test
    void theStreamIsNotSplittable() {
        BlockingQueue<String> queue = new LinkedBlockingQueue<>(UniLists.of("only"));

        var spliterator = Java8StreamUtils.setupStreamFromQueue(queue, () -> true).spliterator();

        assertThat(spliterator.trySplit()).isNull();
    }
}
