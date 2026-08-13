package bz.stub.parallelconsumer.internal.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.experimental.UtilityClass;

import java.util.Deque;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@UtilityClass
public class Java8StreamUtils {

    public static <T> Stream<T> setupStreamFromDeque(Deque<? extends T> userProcessResultsStream) {
        Spliterator<T> spliterator = new DequeSpliterator<>(userProcessResultsStream);

        return StreamSupport.stream(spliterator, false);
    }

    /**
     * Takes from a deque that another thread may be draining or clearing concurrently, ending the stream once
     * it comes up empty.
     * <p>
     * The take is a <b>single</b> {@code poll()} rather than an {@code isEmpty()} test followed by one: those
     * two calls are not atomic, so the deque can empty in between and the {@code poll()} then returns
     * {@code null} into a stream that declares {@link Spliterator#NONNULL}.
     */
    private static class DequeSpliterator<T> extends Spliterators.AbstractSpliterator<T> {

        private final Deque<? extends T> userProcessResultsStream;

        DequeSpliterator(Deque<? extends T> userProcessResultsStream) {
            // Estimate 0 rather than "unknown": it keeps AbstractSpliterator.trySplit() disabled, so a caller
            // that asks for a parallel stream cannot have a split poll ahead of this single-shot source.
            super(0, Spliterator.NONNULL);
            this.userProcessResultsStream = userProcessResultsStream;
        }

        @Override
        public boolean tryAdvance(Consumer<? super T> action) {
            T polled = userProcessResultsStream.poll();
            if (polled == null) {
                return false;
            }
            action.accept(polled);
            return true;
        }
    }
}
