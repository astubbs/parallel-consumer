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
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertThat;

/**
 * The queue-to-{@link java.util.stream.Stream} bridge waits for results instead of ending on an empty queue.
 *
 * @author Antony Stubbs
 * @see Java8StreamUtils
 * @see <a href="https://github.com/confluentinc/parallel-consumer/issues/912">confluentinc#912</a>
 */
class Java8StreamUtilsTest {

    @Test
    void drainsWhatIsAlreadyQueuedThenEndsOnceTheSourceIsFinished() {
        BlockingQueue<String> queue = new LinkedBlockingQueue<>(UniLists.of("a", "b"));

        List<String> collected = Java8StreamUtils.setupStreamFromQueue(queue, () -> true)
                .collect(Collectors.toList());

        assertThat(collected).containsExactly("a", "b").inOrder();
    }

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
    void resultsQueuedBeforeTheSourceFinishedAreStillDelivered() {
        BlockingQueue<String> queue = new LinkedBlockingQueue<>(UniLists.of("one", "two", "three"));

        List<String> collected = Java8StreamUtils.setupStreamFromQueue(queue, () -> true)
                .collect(Collectors.toList());

        assertThat(collected).containsExactly("one", "two", "three").inOrder();
        assertThat(queue).isEmpty();
    }
}
