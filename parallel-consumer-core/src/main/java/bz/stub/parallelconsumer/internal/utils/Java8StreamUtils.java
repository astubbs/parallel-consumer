package bz.stub.parallelconsumer.internal.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.experimental.UtilityClass;

import java.time.Duration;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@UtilityClass
public class Java8StreamUtils {

    /**
     * How long a waiting consumer parks between checks that the source has finished. It bounds how long the
     * stream takes to notice a shutdown, not how long it waits for a result - an arriving result wakes the
     * consumer immediately.
     */
    private static final Duration FINISHED_CHECK_INTERVAL = Duration.ofMillis(100);

    /*
     * Why a timed poll rather than a sentinel enqueued at close, which would let this block indefinitely on
     * take() and wake the instant the producer finished: a sentinel can only be enqueued by whoever performs
     * the close, and a processor can also finish by its control thread closing itself on an unhandled error.
     * Nothing enqueues a sentinel on that path, so a consumer would wait forever rather than the bounded
     * interval below. Asking the source whether it has finished covers both.
     */

    /**
     * Bridges a queue that another thread is filling to a {@link Stream} the caller consumes, ending the
     * stream when {@code sourceFinished} reports no more elements can arrive.
     *
     * @param sourceFinished must only report true once the producer has stopped for good; anything the
     *                       producer already queued is still delivered before the stream ends
     */
    public static <T> Stream<T> setupStreamFromQueue(BlockingQueue<? extends T> queue, BooleanSupplier sourceFinished) {
        return StreamSupport.stream(new QueueSpliterator<>(queue, sourceFinished), false);
    }

    /**
     * Waits for the next element rather than treating an empty queue as the end of the stream.
     * <p>
     * {@link Spliterator#tryAdvance} has only two outcomes - deliver an element, or return {@code false} -
     * and {@code false} means <b>no more, ever</b>. There is no way to say "nothing right now". So a
     * spliterator over a source that is still producing has to block; returning {@code false} the first time
     * the queue happened to be empty reports permanent completion to signal a momentary gap, and the caller's
     * terminal operation finishes while the producer is still running. That is the defect behind
     * <a href="https://github.com/confluentinc/parallel-consumer/issues/912">confluentinc#912</a>: results
     * produced after the consumer walked away accumulated with nothing left to drain them.
     * <p>
     * The end of the stream is therefore the only thing that genuinely ends it - the producer finishing - and
     * whatever it queued before then is drained first, because those results are the caller's.
     * <p>
     * Not splittable: {@link #trySplit()} returns null. The source is a single live queue, so a split half
     * could only race the other for the same elements.
     */
    private static class QueueSpliterator<T> extends Spliterators.AbstractSpliterator<T> {

        private final BlockingQueue<? extends T> queue;
        private final BooleanSupplier sourceFinished;

        QueueSpliterator(BlockingQueue<? extends T> queue, BooleanSupplier sourceFinished) {
            super(Long.MAX_VALUE, Spliterator.NONNULL | Spliterator.ORDERED);
            this.queue = queue;
            this.sourceFinished = sourceFinished;
        }

        @Override
        public Spliterator<T> trySplit() {
            return null;
        }

        @Override
        public boolean tryAdvance(Consumer<? super T> action) {
            while (true) {
                T next;
                try {
                    next = queue.poll(FINISHED_CHECK_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    // The consuming thread was asked to stop; end the stream and leave the flag for it to see.
                    Thread.currentThread().interrupt();
                    return false;
                }
                if (next != null) {
                    action.accept(next);
                    return true;
                }
                if (sourceFinished.getAsBoolean()) {
                    // One last look: an element may have been queued between the poll above timing out and
                    // the source reporting finished. After this the queue can only be empty for good.
                    next = queue.poll();
                    if (next != null) {
                        action.accept(next);
                        return true;
                    }
                    return false;
                }
            }
        }
    }
}
