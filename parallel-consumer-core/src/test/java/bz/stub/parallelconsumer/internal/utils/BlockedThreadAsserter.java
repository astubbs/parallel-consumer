package bz.stub.parallelconsumer.internal.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.google.common.truth.Truth;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.awaitility.Awaitility.await;

/**
 * System for asserting that a given function blocks. Two properties, two idioms - pick by what you can name as
 * the thing that would end the block:
 * <ul>
 *     <li>{@link #assertFunctionBlocks} - <em>stays blocked for a window</em>. Nothing is expected to release it,
 *     so there is no event to wait for and the window <em>is</em> the assertion: watch for a while, and require
 *     that the function has not returned.</li>
 *     <li>{@link #assertUnblocksAfter} - <em>does not return until a specific thing has happened</em>. That is a
 *     causality assertion, so it is asserted as an ordering fact rather than as a duration. It still opens a short
 *     window first, to establish the function is genuinely parked rather than merely started - without one,
 *     "it has not returned yet" says nothing except that this thread got scheduled first.</li>
 * </ul>
 * <p>
 * A single asserter instance runs a single function: the returned/threw state below is per-instance, so use a
 * fresh one per assertion.
 * <p>
 * JUnit has {@link org.junit.jupiter.api.Assertions#assertTimeoutPreemptively} which is useful but has limitations.
 * <p>
 * {@code BlockedThreadAsserterTest} is the negative control for both idioms - it requires that a function which
 * never blocks, and one which is never unblocked, each make the assertion fail.
 *
 * @author Antony Stubbs
 */
@Slf4j
public class BlockedThreadAsserter {

    private final AtomicBoolean methodReturned = new AtomicBoolean(false);

    /**
     * The same fact as {@link #methodReturned}, in the form that can be <em>waited on</em> rather than polled.
     */
    private final CountDownLatch functionReturned = new CountDownLatch(1);

    /**
     * What the function under assert threw, if anything.
     * <p>
     * Captured rather than swallowed because a function that throws on entry returns immediately and is otherwise
     * indistinguishable from one that blocked and then returned cleanly. {@link Throwable}, not {@link Exception},
     * so that an {@link Error} is reported too instead of being left to hang whatever is waiting for the return.
     */
    private final AtomicReference<Throwable> blockedFunctionThrew = new AtomicReference<>();

    public boolean functionHasCompleted() {
        return methodReturned.get();
    }

    public void assertFunctionBlocks(Runnable functionExpectedToBlock) {
        assertFunctionBlocks(functionExpectedToBlock, ofSeconds(1));
    }

    public void assertFunctionBlocks(Runnable functionExpectedToBlock, final Duration blockedForAtLeast) {
        Thread blocked = new Thread(() -> {
            log.debug("Running function expected to block for at least {}...", blockedForAtLeast);
            runCapturingThrowable(functionExpectedToBlock);
            log.debug("Blocked function finished.");
            markReturned();
        }, "blocked-function-under-assert");
        // daemon: a function that never unblocks leaves this thread parked forever, and a non-daemon one would
        // then hold the JVM open past the end of the suite
        blocked.setDaemon(true);
        blocked.start();

        assertStillBlockedFor(blockedForAtLeast,
                () -> "Thread should be sleeping/blocked and not have returned" + describeThrew());
    }

    /**
     * Never observed, used to make an un-ticked sequence slot obviously wrong rather than accidentally
     * ordered - {@code 0} would compare as "before" everything.
     */
    private static final int NOT_YET_TICKED = Integer.MAX_VALUE;

    /**
     * How long the function must be observed to stay parked before the unblocker is allowed to run.
     * <p>
     * This is the legitimate "proving a negative needs a window" case: there is no event meaning "the thread has
     * reached the blocking call and is now parked", so the only evidence obtainable is that it has not returned
     * after a while. Deliberately short - it establishes a precondition, it does not measure a timeout.
     */
    private static final Duration STILL_BLOCKED_WINDOW = ofMillis(500);

    /**
     * How long the function gets to return once the unblocker has run. 20s, not Awaitility's 10s default, which
     * is tight under PIT's instrumented JVM - the same budget as the awaits in {@code ProducerManagerTest}, this
     * class's main caller.
     */
    private static final Duration RETURN_AFTER_UNBLOCK_BUDGET = ofSeconds(20);

    public void assertUnblocksAfter(final Runnable functionExpectedToBlock,
                                    final Runnable unblockingFunction) {
        assertUnblocksAfter(functionExpectedToBlock, unblockingFunction, RETURN_AFTER_UNBLOCK_BUDGET);
    }

    /**
     * Asserts that {@code functionExpectedToBlock} does not return until {@code unblockingFunction} has run.
     * <p>
     * This is a <em>causality</em> assertion, not a duration one. An earlier version scheduled the unblocker on a
     * timer and then asserted the blocked function's wall-clock elapsed time was at least that long, which made it
     * both slow (it genuinely slept for the timeout) and flaky - it failed in CI at {@code 19.985s >= 20s}, a 15ms
     * scheduler-jitter miss that said nothing about the behaviour under test.
     * <p>
     * Three things have to hold, and each of them is a different way a function can fail to block:
     * <ol>
     *     <li><b>It is still parked when the unblocker runs.</b> The entered latch below is counted down
     *     <em>before</em> the call, so it says the thread started and nothing more. Ticking the unblocker straight
     *     off it turns the whole assertion into a race between two threads, which the calling thread usually wins
     *     - so a function that never blocks at all scores a clean pass. Measured on the version that did exactly
     *     that: a non-blocking function passed 177 times out of 200. Hence the {@link #STILL_BLOCKED_WINDOW}.</li>
     *     <li><b>It did not throw.</b> See {@link #blockedFunctionThrew} - an immediate throw otherwise reads as a
     *     clean block-then-return.</li>
     *     <li><b>Its return is ordered after the unblocker.</b> Both events take a tick from a shared monotonic
     *     sequence, and the ticks are compared. Second line of defence behind the window, and the one that would
     *     catch a function which unparks for some reason other than the unblocker.</li>
     * </ol>
     * A function that is never unblocked fails differently again: {@link #methodReturned} stays false and the
     * final await times out after {@code returnBudget}.
     *
     * @param returnBudget how long to allow the function to return in, once the unblocker has run
     */
    public void assertUnblocksAfter(final Runnable functionExpectedToBlock,
                                    final Runnable unblockingFunction,
                                    final Duration returnBudget) {
        final AtomicInteger sequence = new AtomicInteger();
        final AtomicInteger unblockerTick = new AtomicInteger(NOT_YET_TICKED);
        final AtomicInteger blockedReturnTick = new AtomicInteger(NOT_YET_TICKED);
        final CountDownLatch blockedFunctionEntered = new CountDownLatch(1);

        Thread blocked = new Thread(() -> {
            blockedFunctionEntered.countDown();
            log.debug("Running function expected to block until the unblocker runs...");
            runCapturingThrowable(functionExpectedToBlock);
            blockedReturnTick.set(sequence.incrementAndGet());
            markReturned();
            log.debug("Blocked function returned (tick {})", blockedReturnTick.get());
        }, "blocked-function-under-assert");
        blocked.setDaemon(true); // see assertFunctionBlocks
        blocked.start();

        // liveness only - this says the thread started, NOT that it reached the blocking call
        LatchTestUtils.awaitLatch(blockedFunctionEntered);

        // ...so establish it is actually parked, before handing it something to unpark from
        assertStillBlockedFor(STILL_BLOCKED_WINDOW,
                () -> "function should still be blocked before the unblocker runs, but it has already returned"
                        + " - so it never blocked" + describeThrew());

        log.debug("Running unblocking function - blocked function must return ONLY after this");
        unblockerTick.set(sequence.incrementAndGet());
        // deliberately NOT caught: this runs on the calling thread, so letting it out fails the test with the real
        // cause, instead of leaving the await below to time out saying nothing useful
        unblockingFunction.run();

        await("blocked function returns once unblocked")
                .atMost(returnBudget)
                .untilTrue(methodReturned);

        Truth.assertWithMessage("Blocked function threw instead of blocking and then returning cleanly: %s",
                        blockedFunctionThrew.get())
                .that(blockedFunctionThrew.get())
                .isNull();

        Truth.assertWithMessage(
                        "Blocked function must not return until the unblocking function has run "
                                + "(unblocker tick %s, blocked-function return tick %s). A lower return tick means it "
                                + "never actually blocked.",
                        unblockerTick.get(), blockedReturnTick.get())
                .that(blockedReturnTick.get())
                .isGreaterThan(unblockerTick.get());
    }

    /**
     * Requires that the function has NOT returned within {@code window}.
     * <p>
     * Waits on {@link #functionReturned} rather than polling {@link #methodReturned}: the property is monotonic -
     * a function that has returned never un-returns - so re-polling can only ever re-confirm what the first look
     * saw. Latching makes the failure land the instant the function returns, and land as the assertion error that
     * names the cause; polling would instead burn the whole window and report a timeout.
     */
    private void assertStillBlockedFor(final Duration window, final Supplier<String> failureMessage) {
        final boolean returnedInsideWindow;
        try {
            returnedInsideWindow = functionReturned.await(window.toMillis(), MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while checking the function stays blocked", e);
        }
        Truth.assertWithMessage(failureMessage.get())
                .that(returnedInsideWindow)
                .isFalse();
    }

    private void runCapturingThrowable(final Runnable function) {
        try {
            function.run();
        } catch (Throwable t) { // NOSONAR - an Error must be reported here too, not left to hang the waiter
            log.error("Error in function under assert", t);
            blockedFunctionThrew.set(t);
        }
    }

    private void markReturned() {
        methodReturned.set(true);
        functionReturned.countDown();
    }

    private String describeThrew() {
        Throwable throwable = blockedFunctionThrew.get();
        return throwable == null
                ? ""
                : " (it threw: " + throwable + ")";
    }

    public void awaitReturnFully() {
        log.debug("Waiting for blocked method to fully finish...");
        await().untilTrue(this.methodReturned);
        log.debug("Waiting on blocked method to fully finish is complete.");
    }
}
