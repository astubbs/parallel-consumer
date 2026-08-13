package bz.stub.parallelconsumer.internal.utils;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;

/**
 * The stream handed back by the deprecated {@code JStream*} processors declares itself
 * {@link java.util.Spliterator#NONNULL}, so it must never emit a {@code null} - not even when the deque
 * behind it empties underneath the consumer, which the clear-on-close those processors perform can do.
 *
 * @author Antony Stubbs
 * @see Java8StreamUtils
 * @see <a href="https://github.com/confluentinc/parallel-consumer/issues/912">confluentinc/parallel-consumer#912</a>
 */
class Java8StreamUtilsTest {

    @Test
    void streamsEntriesAndEndsWhenTheDequeEmpties() {
        var deque = new ConcurrentLinkedDeque<>(UniLists.of("a", "b"));

        List<String> collected = Java8StreamUtils.setupStreamFromDeque(deque).collect(Collectors.toList());

        Assertions.assertThat(collected).containsExactly("a", "b");
        Assertions.assertThat(deque).isEmpty();
    }

    /**
     * Models the window the old {@code isEmpty()}-then-{@code poll()} pair left open: a deque that answers
     * "not empty" and then hands back nothing, because someone else took the last entry - or cleared the
     * whole deque on close - in between. Taking with a single {@code poll()} cannot observe that state, so
     * the stream ends instead of pushing a {@code null} into the caller's terminal operation.
     */
    @Test
    void neverEmitsNullWhenTheDequeEmptiesBetweenTheCheckAndTheTake() {
        var emptiedUnderneath = new ConcurrentLinkedDeque<String>() {
            @Override
            public String poll() {
                return null; // the entry below is gone by the time we take it
            }
        };
        emptiedUnderneath.add("lost to a concurrent close");

        var stream = Java8StreamUtils.setupStreamFromDeque(emptiedUnderneath);

        // map dereferences every element, so a null reaches the test as an NPE rather than as a quiet pass
        Assertions.assertThat(stream.map(String::length).collect(Collectors.toList())).isEmpty();
    }
}
