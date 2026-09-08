package bz.stub.parallelconsumer.internal.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;

@Slf4j
public class ThreadUtils {

    /**
     * Sleeps for {@code duration}, restoring the interrupt flag and failing loudly if interrupted.
     * <p>
     * Distinct from both siblings, and deliberately so. {@link #sleepLog(int)} catches the interrupt and
     * returns, which reads to the caller as "the sleep completed"; {@link #sleepQuietly(long)} is
     * {@link SneakyThrows @SneakyThrows}, so it rethrows the {@link InterruptedException} unchecked but
     * leaves the interrupt flag cleared and says nothing about what was interrupted. For a test whose
     * accounting depends on real elapsed time - a retry budget, or a feed spread across commit cycles -
     * a shortened sleep silently changes what is being measured, so the interrupt has to become a named
     * failure rather than an early return.
     *
     * @param interruptedMessage what the caller was in the middle of, so the failure names it
     */
    public static void sleepOrFail(Duration duration, String interruptedMessage) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interruptedMessage, e);
        }
    }

    @SneakyThrows
    public static void sleepQuietly(final int ms) {
        log.debug("Sleeping for {}", ms);
        Thread.sleep(ms);
        log.debug("Woke up (slept for {})", ms);
    }

    public static void sleepLog(final int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            log.error("Sleep of {} interrupted", e, ms);
        }
    }

    @SneakyThrows
    public static void sleepQuietly(long ms) {
        sleepQuietly((int) ms);
    }

    /**
     * The {@link Duration} form, so a caller holding one does not hand-roll its own three-line wrapper. Two already
     * had, in test source roots that can both see this class - which is the drift this overload exists to stop.
     */
    public static void sleepQuietly(Duration duration) {
        sleepQuietly(duration.toMillis());
    }

    public static void sleepSecondsLog(int seconds) {
        sleepLog(seconds * 1000);
    }

    /**
     * Joins {@code thread} for at most {@code timeout}, restoring the interrupt flag if interrupted.
     * <p>
     * It lives here rather than in the one test that calls it today because this wrapper is what gets
     * hand-rolled: `join` with a timeout plus an interrupt restore is four lines that every thread-racing test
     * in {@code state} needs, and a second private copy is how the drift starts. Putting it in the shared
     * utility on the way past is cheaper than deduplicating two copies later.
     * <p>
     * A {@code null} thread is a no-op, so a {@code finally} block can call this before it knows whether the
     * thread was ever started - which is the shape that makes it usable for cleanup and not only for the happy
     * path.
     */
    public static void joinQuietly(Thread thread, Duration timeout) {
        if (thread == null) {
            return;
        }
        try {
            thread.join(timeout.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
