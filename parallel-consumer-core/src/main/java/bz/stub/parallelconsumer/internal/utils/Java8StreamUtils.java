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
        Spliterator<T> spliterator = new DequeSpliterator<>(userProcessResultsStream, userProcessResultsStream.size());

        return StreamSupport.stream(spliterator, false);
    }

    /**
     * Ends the stream when the deque is (momentarily) empty, deciding that with a <b>single</b> {@code poll()}
     * rather than an {@code isEmpty()} followed by a {@code poll()}.
     * <p>
     * The two-call form - which this replaced - is not atomic: another consumer, or the clear-on-close the
     * {@code JStream*} processors perform, can empty the deque between the two, and the second call then
     * returns {@code null} into a stream declared {@link Spliterator#NONNULL}, typically surfacing as an NPE
     * inside the caller's terminal operation at shutdown. One {@code poll()} cannot observe that gap.
     * <p>
     * Termination behaviour is otherwise unchanged: the stream still ends the first time the deque is found
     * empty, which is one of the reasons the {@code JStream*} API is deprecated.
     *
     * @see <a href="https://github.com/confluentinc/parallel-consumer/issues/912">confluentinc/parallel-consumer#912</a>
     */
    private static class DequeSpliterator<T> extends Spliterators.AbstractSpliterator<T> {

        private final Deque<? extends T> userProcessResultsStream;

        DequeSpliterator(Deque<? extends T> userProcessResultsStream, long estimatedSize) {
            super(estimatedSize, Spliterator.NONNULL);
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
