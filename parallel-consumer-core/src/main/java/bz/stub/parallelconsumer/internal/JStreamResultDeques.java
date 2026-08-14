package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.util.Deque;

/**
 * Shared handling for the result deques behind the deprecated {@code JStream*} processors.
 * <p>
 * Those processors hand results back as a {@link java.util.stream.Stream} backed by an unbounded
 * {@link java.util.concurrent.ConcurrentLinkedDeque}: entries are pushed for every processed record and
 * only leave when the caller consumes the stream. A caller that consumes slowly, or not at all, grows the
 * deque for the life of the instance. Both processors need identical clear-on-close behaviour, and they
 * share no common supertype, so it lives here rather than being copied into each.
 *
 * @see <a href="https://github.com/astubbs/parallel-consumer/issues/122">astubbs#122</a>
 * @see <a href="https://github.com/confluentinc/parallel-consumer/issues/912">confluentinc#912</a>
 */
@Slf4j
@UtilityClass
public class JStreamResultDeques {

    /**
     * Drops anything the caller never consumed, so closing the processor actually releases it.
     * <p>
     * Reports only <i>that</i> results were dropped, never how many: counting them means
     * {@link java.util.concurrent.ConcurrentLinkedDeque#size()}, which walks the whole deque, and the
     * backlog is largest exactly when this runs. {@link Deque#isEmpty()} answers the question the log line
     * actually asks without touching more than the head.
     * <p>
     * Best-effort on a <b>failed</b> close: if the shutdown timed out, worker threads can still be live and
     * their callbacks may enqueue after this returns. A close that completes normally has no such window -
     * its workers are finished before this runs.
     */
    public void clearOnClose(Deque<?> deque) {
        if (!deque.isEmpty()) {
            log.info("Clearing unconsumed result(s) from the stream on close");
        }
        deque.clear();
    }
}
