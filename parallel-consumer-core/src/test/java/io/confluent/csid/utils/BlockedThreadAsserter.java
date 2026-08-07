package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.google.common.truth.Truth;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.Duration.ofSeconds;
import static org.awaitility.Awaitility.await;

/**
 * System for asserting that a given method blocks for some period of time, and optionally unblocks.
 * <p>
 * JUnit has {@link org.junit.jupiter.api.Assertions#assertTimeoutPreemptively} which is useful but has limitations.
 *
 * @author Antony Stubbs
 */
@Slf4j
public class BlockedThreadAsserter {

    /**
     * Could do this faster with a {@link java.util.concurrent.CountDownLatch}
     */
    private final AtomicBoolean methodReturned = new AtomicBoolean(false);

    public boolean functionHasCompleted() {
        return methodReturned.get();
    }


    public void assertFunctionBlocks(Runnable functionExpectedToBlock) {
        assertFunctionBlocks(functionExpectedToBlock, ofSeconds(1));
    }

    public void assertFunctionBlocks(Runnable functionExpectedToBlock, final Duration blockedForAtLeast) {
        Thread blocked = new Thread(() -> {
            try {
                log.debug("Running function expected to block for at least {}...", blockedForAtLeast);
                functionExpectedToBlock.run();
                log.debug("Blocked function finished.");
            } catch (Exception e) {
                log.error("Error in blocking function", e);
            }
            methodReturned.set(true);
        });
        blocked.start();

        await()
                .pollDelay(blockedForAtLeast) // makes sure it is still blocked after 1 second
                .atMost(blockedForAtLeast.plus(Duration.ofSeconds(1)))
                .untilAsserted(
                        () -> Truth.assertWithMessage("Thread should be sleeping/blocked and not have returned")
                                .that(methodReturned.get())
                                .isFalse());
    }

    /**
     * Never observed, used to make an un-ticked sequence slot obviously wrong rather than accidentally
     * ordered - {@code 0} would compare as "before" everything.
     */
    private static final int NOT_YET_TICKED = Integer.MAX_VALUE;

    /**
     * Asserts that {@code functionExpectedToBlock} does not return until {@code unblockingFunction} has run.
     * <p>
     * This is a <em>causality</em> assertion, not a duration one. An earlier version scheduled the unblocker on
     * a timer and then asserted the blocked function's wall-clock elapsed time was at least that long, which
     * made it both slow (it genuinely slept for the timeout) and flaky - it failed in CI at
     * {@code 19.985s >= 20s}, a 15ms scheduler-jitter miss that said nothing about the behaviour under test.
     * <p>
     * The property the test actually cares about is an ordering fact, so it is asserted as one: both events
     * take a tick from a shared monotonic sequence, and the blocked function's return must come after the
     * unblocker's. No clocks, no sleeps, no timer - and it fails correctly in both directions:
     * <ul>
     *     <li>function never blocked -> it returns first, gets the lower tick, assertion fails</li>
     *     <li>function never unblocked -> {@link #methodReturned} stays false and the await below times out</li>
     * </ul>
     */
    public void assertUnblocksAfter(final Runnable functionExpectedToBlock,
                                    final Runnable unblockingFunction) {
        final AtomicInteger sequence = new AtomicInteger();
        final AtomicInteger unblockerTick = new AtomicInteger(NOT_YET_TICKED);
        final AtomicInteger blockedReturnTick = new AtomicInteger(NOT_YET_TICKED);
        final CountDownLatch blockedFunctionEntered = new CountDownLatch(1);

        Thread blocked = new Thread(() -> {
            blockedFunctionEntered.countDown();
            try {
                log.debug("Running function expected to block until the unblocker runs...");
                functionExpectedToBlock.run();
            } catch (Exception e) {
                log.error("Error in blocking function", e);
            }
            blockedReturnTick.set(sequence.incrementAndGet());
            methodReturned.set(true);
            log.debug("Blocked function returned (tick {})", blockedReturnTick.get());
        }, "blocked-function-under-assert");
        blocked.start();

        // Wait on the event - the thread reaching the call - rather than guessing how long it takes to get
        // there. This is the whole point: nothing here sleeps.
        try {
            blockedFunctionEntered.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted waiting for the blocked function to start", e);
        }

        log.debug("Running unblocking function - blocked function must return ONLY after this");
        unblockerTick.set(sequence.incrementAndGet());
        try {
            unblockingFunction.run();
        } catch (Exception e) {
            log.error("Error in unblocking function", e);
        }

        await("blocked function returns once unblocked").untilTrue(methodReturned);

        Truth.assertWithMessage(
                        "Blocked function must not return until the unblocking function has run "
                                + "(unblocker tick %s, blocked-function return tick %s). A lower return tick means it "
                                + "never actually blocked.",
                        unblockerTick.get(), blockedReturnTick.get())
                .that(blockedReturnTick.get())
                .isGreaterThan(unblockerTick.get());
    }

    public void awaitReturnFully() {
        log.debug("Waiting for blocked method to fully finish...");
        await().untilTrue(this.methodReturned);
        log.debug("Waiting on blocked method to fully finish is complete.");
    }
}
