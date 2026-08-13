package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.util.Deque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Shared handling for the result deques behind the deprecated {@code JStream*} processors.
 * <p>
 * Those processors hand results back as a {@link java.util.stream.Stream} backed by an unbounded
 * {@link java.util.concurrent.ConcurrentLinkedDeque}: entries are pushed for every processed record and
 * only leave when the caller consumes the stream. A caller that consumes slowly, or not at all, grows the
 * deque for the life of the instance. Both processors need identical backlog-warning and clear-on-close
 * behaviour, and they share no common supertype, so it lives here rather than being copied into each.
 *
 * @see <a href="https://github.com/confluentinc/parallel-consumer/issues/912">confluentinc/parallel-consumer#912</a>
 */
@Slf4j
@UtilityClass
public class JStreamResultDeques {

    /**
     * How many additions between backlog checks. Also the size below which a backlog is not worth reporting.
     */
    private static final int BACKLOG_WARN_INTERVAL = 10_000;

    /**
     * Adds a result, warning periodically while the deque is backlogged.
     * <p>
     * The size check is sampled once per {@link #BACKLOG_WARN_INTERVAL} additions rather than performed on
     * every one, because {@link java.util.concurrent.ConcurrentLinkedDeque#size()} is <b>O(n)</b> - it walks
     * the whole deque. Checking it per result would make each addition cost more as the leak got worse,
     * which is precisely the situation the warning exists to report.
     *
     * @param addCounter per-instance count of additions, owned by the caller
     */
    public <T> void addAndWarnIfBacklogged(Deque<T> deque, AtomicLong addCounter, T result) {
        deque.add(result);
        if (addCounter.incrementAndGet() % BACKLOG_WARN_INTERVAL == 0) {
            int size = deque.size();
            if (size >= BACKLOG_WARN_INTERVAL) {
                log.warn("Result stream backlog: {} items. Unconsumed results accumulate in memory - consume the " +
                        "stream, or use the callback-based API. " +
                        "See https://github.com/confluentinc/parallel-consumer/issues/912", size);
            }
        }
    }

    /**
     * Drops anything the caller never consumed, so closing the processor actually releases it.
     */
    public void clearOnClose(Deque<?> deque) {
        int remaining = deque.size();
        if (remaining > 0) {
            log.info("Clearing {} unconsumed result(s) from the stream on close", remaining);
        }
        deque.clear();
    }
}
