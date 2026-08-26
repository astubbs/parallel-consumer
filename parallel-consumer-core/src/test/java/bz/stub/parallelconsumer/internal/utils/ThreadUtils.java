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

    public static void sleepSecondsLog(int seconds) {
        sleepLog(seconds * 1000);
    }
}
