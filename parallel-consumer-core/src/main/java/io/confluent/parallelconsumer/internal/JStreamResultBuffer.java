package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * The result buffer behind the {@code JStream*} processors: a <b>bounded</b> queue exposed as a
 * {@link Stream}, with real backpressure onto the producer.
 * <p>
 * This replaces an unbounded {@link java.util.concurrent.ConcurrentLinkedDeque} whose stream ended the
 * first time it was <i>transiently</i> empty. That combination was unusable - see
 * <a href="https://github.com/astubbs/parallel-consumer/issues/122">astubbs#122</a> and
 * <a href="https://github.com/confluentinc/parallel-consumer/issues/912">confluentinc#912</a>, where a
 * user following the shipped example ran out of heap. Three defects, all fixed here:
 * <ol>
 *     <li><b>The stream ended early.</b> The old iterator's {@code hasNext()} returned
 *     {@code !deque.isEmpty()}, and a {@link Stream} finishes as soon as that is false - so
 *     {@code forEach} returned the moment the consumer caught up, and everything produced afterwards
 *     piled up behind a consumer that had already finished. Here {@code hasNext()} <b>blocks</b> while
 *     the buffer is open, and returns false only once {@link #close()} has been called and the
 *     remaining results drained.</li>
 *     <li><b>Growth was unbounded.</b> The queue now has a fixed capacity and {@link #add} blocks when
 *     it is full, so a slow consumer applies backpressure through the worker pool to the broker instead
 *     of consuming heap.</li>
 *     <li><b>The size hint was a lie.</b> The old stream was built with
 *     {@code Spliterators.spliterator(iterator, deque.size(), ...)}, which reports {@code SIZED} using
 *     the size <i>at construction time</i> - always zero, since results arrive later. Operations that
 *     trust that hint (such as {@code count()} or {@code toArray()}) could therefore see nothing. This
 *     uses {@link Spliterators#spliteratorUnknownSize} instead.</li>
 * </ol>
 * <b>Close can never deadlock.</b> {@link #add} parks with a timed {@code offer} rather than a
 * blocking {@code put}, re-checking the closing flag each time, so a producer stuck against a full
 * buffer with no consumer still returns promptly once {@link #close()} is called. Symmetrically, a
 * consumer parked on an empty buffer wakes within {@link #POLL_MILLIS} of the close. This matters
 * because the Vert.x processor's worker pool is hard-coded to a single thread, so a producer that
 * could block indefinitely would wedge the whole processor.
 *
 * @param <T> the result type handed to the user
 */
@Slf4j
public class JStreamResultBuffer<T> {

    /**
     * Default number of unconsumed results held before the producer is made to wait.
     */
    public static final int DEFAULT_CAPACITY = 10_000;

    /**
     * How long a parked producer or consumer waits before re-checking the closing flag. Bounds how long
     * {@link #close()} can take to be noticed.
     */
    static final long POLL_MILLIS = 100;

    private final BlockingDeque<T> queue;

    /**
     * The single {@link Stream} view of this buffer. Note this is still consumable only once - that is
     * inherent to {@link Stream} - but unlike the previous implementation it no longer terminates early,
     * so a single {@code forEach} now runs for the life of the processor as users expect.
     */
    @Getter
    private final Stream<T> stream;

    private volatile boolean closing;

    private final AtomicLong blockedAdds = new AtomicLong();

    public JStreamResultBuffer() {
        this(DEFAULT_CAPACITY);
    }

    public JStreamResultBuffer(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("Result buffer capacity must be at least 1, got: " + capacity);
        }
        this.queue = new LinkedBlockingDeque<>(capacity);
        this.stream = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(new BlockingResultIterator(), Spliterator.NONNULL | Spliterator.ORDERED),
                false);
    }

    /**
     * Hands a result to the consumer, waiting if the buffer is full.
     * <p>
     * Waiting here is the point: it is what stops an unconsumed stream from becoming a memory leak. The
     * wait is abandoned once {@link #close()} is called, in which case the result is dropped - by then
     * nobody is going to read it, and blocking shutdown to preserve it would be worse.
     */
    public void add(T result) {
        while (!closing) {
            try {
                if (queue.offer(result, POLL_MILLIS, TimeUnit.MILLISECONDS)) {
                    return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.debug("Interrupted while handing a result to the stream - dropping it", e);
                return;
            }
            warnAboutBackpressure();
        }
        log.debug("Buffer closing - dropping an unconsumed result");
    }

    /**
     * Ends the stream. Any results already buffered are still delivered first, so a draining close hands
     * the consumer everything it produced; only then does {@code hasNext()} report false.
     */
    public void close() {
        closing = true;
        int remaining = queue.size();
        if (remaining > 0) {
            log.info("Result stream closing with {} buffered result(s) still to drain", remaining);
        }
    }

    /**
     * Current depth. Cheap - {@link LinkedBlockingDeque} maintains a count, unlike the
     * {@link java.util.concurrent.ConcurrentLinkedDeque} this replaces, whose {@code size()} was O(n).
     * Suitable for a metrics gauge (astubbs#216).
     */
    public int size() {
        return queue.size();
    }

    public boolean isClosing() {
        return closing;
    }

    private void warnAboutBackpressure() {
        long blocked = blockedAdds.incrementAndGet();
        // Once per ~5s of continuous blocking, not once per failed offer.
        long every = TimeUnit.SECONDS.toMillis(5) / POLL_MILLIS;
        if (blocked % every == 1) {
            log.warn("Result stream is full ({} results) and has been blocking the producer - the consumer is not " +
                    "keeping up. Consume the stream faster, or use the callback-based API which does not buffer. " +
                    "See https://github.com/astubbs/parallel-consumer/issues/122", queue.size());
        }
    }

    /**
     * Blocks while the buffer is open and empty, rather than declaring the stream finished.
     */
    private class BlockingResultIterator implements Iterator<T> {

        /**
         * {@link Iterator#hasNext()} must not consume, so the element found while waiting is held here
         * until {@link #next()} asks for it.
         */
        private T prefetched;

        @Override
        public boolean hasNext() {
            if (prefetched != null) {
                return true;
            }
            while (true) {
                try {
                    prefetched = queue.poll(POLL_MILLIS, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.debug("Interrupted while waiting for the next result - ending the stream", e);
                    return false;
                }
                if (prefetched != null) {
                    return true;
                }
                if (closing && queue.isEmpty()) {
                    return false;
                }
            }
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException("Result stream has ended");
            }
            T next = prefetched;
            prefetched = null;
            return next;
        }
    }
}
