package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static com.google.common.truth.Truth.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Tests the bounded, blocking result buffer that replaced the unbounded deque behind the
 * {@code JStream*} processors.
 * <p>
 * These cover the three defects from astubbs#122 / confluentinc#912 and, just as importantly, the two
 * ways a blocking implementation can be <i>worse</i> than the leak it replaces: a producer that never
 * unblocks, and a consumer that never terminates.
 *
 * @see JStreamResultBuffer
 */
@Slf4j
class JStreamResultBufferTest {

    private static final int TIMEOUT_SECONDS = 20;

    /**
     * <b>The headline regression.</b> The old iterator returned {@code !deque.isEmpty()} from
     * {@code hasNext()}, so the stream ended the instant the consumer caught up - and everything produced
     * afterwards accumulated behind a {@code forEach} that had already returned. This is exactly what the
     * reporter of confluentinc#912 described: <i>"if stream becomes empty at some point and is populated
     * later on again"</i>.
     * <p>
     * The gap here is deliberate: the consumer is made to drain the buffer completely and wait, which is
     * precisely the state that used to terminate the stream.
     */
    @Test
    @Timeout(TIMEOUT_SECONDS)
    void streamSurvivesGoingTransientlyEmpty() throws Exception {
        var buffer = new JStreamResultBuffer<String>(10);
        var received = new CopyOnWriteArrayList<String>();
        var consumerFinished = new CountDownLatch(1);

        var consumer = new Thread(() -> {
            buffer.getStream().forEach(received::add);
            consumerFinished.countDown();
        }, "test-consumer");
        consumer.start();

        buffer.add("first");
        await().until(() -> received.contains("first"));

        // The buffer is now empty and the consumer is waiting on it - the old implementation had already
        // ended the stream by this point.
        await().until(() -> buffer.size() == 0);
        assertThat(consumerFinished.getCount()).isEqualTo(1);

        buffer.add("second");
        await().until(() -> received.contains("second"));

        assertThat(received).containsExactly("first", "second").inOrder();

        buffer.close();
        assertThat(consumerFinished.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    }

    /**
     * A consumer parked on an empty buffer must be released by close, or every user of this API hangs on
     * shutdown. This is the failure mode that makes a naive blocking implementation worse than the leak.
     */
    @Test
    @Timeout(TIMEOUT_SECONDS)
    void closeEndsAStreamThatIsWaitingOnAnEmptyBuffer() throws Exception {
        var buffer = new JStreamResultBuffer<String>(10);
        var consumerFinished = new CountDownLatch(1);

        var consumer = new Thread(() -> {
            buffer.getStream().forEach(x -> log.info("got {}", x));
            consumerFinished.countDown();
        }, "test-consumer");
        consumer.start();

        // Let it get as far as blocking on the empty buffer.
        Thread.sleep(JStreamResultBuffer.POLL_MILLIS * 3);
        assertThat(consumerFinished.getCount()).isEqualTo(1);

        buffer.close();

        assertThat(consumerFinished.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    }

    /**
     * Closing must hand over what was already buffered rather than discarding it - a draining close keeps
     * producing results, and the consumer asked for them.
     */
    @Test
    @Timeout(TIMEOUT_SECONDS)
    void resultsBufferedBeforeCloseAreStillDelivered() throws Exception {
        var buffer = new JStreamResultBuffer<Integer>(100);
        IntStream.range(0, 50).forEach(buffer::add);

        buffer.close();

        // Consume only after closing - everything buffered should still arrive.
        var received = new ArrayList<Integer>();
        buffer.getStream().forEach(received::add);

        assertThat(received).hasSize(50);
        assertThat(received.get(0)).isEqualTo(0);
        assertThat(received.get(49)).isEqualTo(49);
    }

    /**
     * The actual fix for the leak: past capacity the producer waits instead of allocating. Without this
     * the buffer grows until the heap is gone, which is the reported bug.
     */
    @Test
    @Timeout(TIMEOUT_SECONDS)
    void producerBlocksOnceTheBufferIsFull() throws Exception {
        var buffer = new JStreamResultBuffer<Integer>(2);
        var thirdAddReturned = new AtomicBoolean(false);

        buffer.add(1);
        buffer.add(2);
        assertThat(buffer.size()).isEqualTo(2);

        var producer = new Thread(() -> {
            buffer.add(3);
            thirdAddReturned.set(true);
        }, "test-producer");
        producer.start();

        // It must still be waiting - if this passes immediately the buffer is not bounded.
        Thread.sleep(JStreamResultBuffer.POLL_MILLIS * 3);
        assertThat(thirdAddReturned.get()).isFalse();
        assertThat(buffer.size()).isEqualTo(2);

        // Draining one makes room, and the producer proceeds.
        var iterator = buffer.getStream().iterator();
        assertThat(iterator.next()).isEqualTo(1);

        await().untilTrue(thirdAddReturned);
        producer.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
    }

    /**
     * <b>The deadlock guard.</b> A producer blocked against a full buffer with nobody consuming must be
     * released by close, otherwise shutdown wedges - and on the Vert.x processor, whose worker pool is
     * hard-coded to a single thread, that would hang the entire processor. A hang is a worse outcome than
     * the leak this change exists to fix, so this is the test that decides whether the approach is viable
     * at all.
     */
    @Test
    @Timeout(TIMEOUT_SECONDS)
    void closeReleasesAProducerBlockedOnAFullBufferWithNoConsumer() throws Exception {
        var buffer = new JStreamResultBuffer<Integer>(1);
        var blockedAddReturned = new CountDownLatch(1);

        buffer.add(1);

        var producer = new Thread(() -> {
            buffer.add(2);
            blockedAddReturned.countDown();
        }, "test-producer");
        producer.start();

        Thread.sleep(JStreamResultBuffer.POLL_MILLIS * 3);
        assertThat(blockedAddReturned.getCount()).isEqualTo(1);

        buffer.close();

        assertThat(blockedAddReturned.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    }

    /**
     * Order matters - these are per-record results, and users correlate them with their input.
     */
    @Test
    @Timeout(TIMEOUT_SECONDS)
    void resultsAreDeliveredInOrder() {
        var buffer = new JStreamResultBuffer<Integer>(1000);
        IntStream.range(0, 500).forEach(buffer::add);
        buffer.close();

        List<Integer> received = new ArrayList<>();
        buffer.getStream().forEach(received::add);

        assertThat(received).hasSize(500);
        assertThat(received).isInOrder();
    }

    /**
     * A zero or negative capacity would either reject everything or throw deep inside the queue, long
     * after the mistake was made.
     */
    @Test
    void capacityIsValidated() {
        assertThatThrownBy(() -> new JStreamResultBuffer<String>(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least 1");
        assertThatThrownBy(() -> new JStreamResultBuffer<String>(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
