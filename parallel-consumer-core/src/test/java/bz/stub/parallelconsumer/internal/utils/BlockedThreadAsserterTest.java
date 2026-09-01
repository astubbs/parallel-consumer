package bz.stub.parallelconsumer.internal.utils;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.google.common.truth.Truth;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * The negative control for {@link BlockedThreadAsserter}: it is an assertion helper, so the thing that has to be
 * demonstrated is that it FAILS when it should.
 * <p>
 * This exists because two ways of not-blocking were once scored as passes. A function that returned immediately
 * passed 177 attempts out of 200 - the asserter's latch marked "thread started", not "thread reached the blocking
 * call", so whether it passed came down to which of two threads was scheduled first. And a function that threw
 * was caught and logged, then recorded as having returned, which is indistinguishable from having blocked and
 * returned cleanly.
 * <p>
 * Both are covered below, along with the case that pins the guard down deterministically - a function that
 * returns on a timer of its own, which the ordering check behind the window provably cannot catch.
 *
 * @author Antony Stubbs
 */
@Slf4j
@Timeout(value = 120)
class BlockedThreadAsserterTest {

    /**
     * Enough for the asserter to see that a function has not returned, without making the suite wait.
     */
    private static final Duration SHORT_RETURN_BUDGET = ofSeconds(2);

    /**
     * How many times the returns-immediately case is retried. One attempt would not control for anything: which
     * of the two threads is scheduled first decides whether the ordering check happens to notice, so a single
     * attempt can fail to fail. This reproduces the reported defect literally (it was measured as passing 177
     * attempts out of 200), and the deterministic counterpart is
     * {@link #functionThatReturnsOnItsOwnScheduleIsRejected} below.
     */
    private static final int RETURNS_IMMEDIATELY_ATTEMPTS = 20;

    /**
     * Short enough to be well inside {@code BlockedThreadAsserter}'s still-blocked window, long enough that the
     * calling thread has certainly ticked the unblocker by the time it elapses.
     */
    private static final Duration BLOCKS_BRIEFLY_THEN_RETURNS = Duration.ofMillis(300);

    @Test
    void functionThatBlocksUntilUnblockedPasses() {
        var releaseMe = new CountDownLatch(1);

        new BlockedThreadAsserter().assertUnblocksAfter(
                () -> awaitQuietly(releaseMe),
                releaseMe::countDown,
                SHORT_RETURN_BUDGET);
    }

    /**
     * The deterministic negative control for the still-blocked window.
     * <p>
     * The function returns on its own schedule rather than because of the unblocker - which is the property
     * being asserted, violated - but it returns <em>after</em> the unblocker has ticked, so the ordering check
     * behind the window sees exactly what a correctly blocking function looks like and cannot object. Only the
     * window can reject this one, and it always does: the return lands inside it every time. Delete the window
     * and this test goes red on every run rather than on most of them.
     */
    @Test
    void functionThatReturnsOnItsOwnScheduleIsRejected() {
        var failure = assertAsserterFails("a function that returns on a timer rather than on the unblocker",
                () -> new BlockedThreadAsserter().assertUnblocksAfter(
                        () -> ThreadUtils.sleepQuietly(BLOCKS_BRIEFLY_THEN_RETURNS.toMillis()),
                        () -> log.debug("Unblocker that the function does not actually wait for"),
                        SHORT_RETURN_BUDGET));

        Truth.assertThat(failure).isInstanceOf(AssertionError.class);
        Truth.assertThat(failure).hasMessageThat().contains("never blocked");
    }

    @Test
    void functionThatNeverBlocksIsRejected() {
        for (int attempt = 1; attempt <= RETURNS_IMMEDIATELY_ATTEMPTS; attempt++) {
            var failure = assertAsserterFails("attempt " + attempt + " of a function that returns immediately",
                    () -> new BlockedThreadAsserter().assertUnblocksAfter(
                            () -> log.debug("Not blocking at all - returning straight away"),
                            () -> log.debug("Nothing to unblock"),
                            SHORT_RETURN_BUDGET));

            // deliberately not asserted on the wording: either guard is a legitimate rejection here - the
            // still-blocked window, or the tick ordering behind it - and pinning the message would make this
            // test fail for the wrong reason on an attempt the second guard happened to catch
            Truth.assertWithMessage("attempt %s must be rejected for never having blocked", attempt)
                    .that(failure)
                    .isInstanceOf(AssertionError.class);
        }
    }

    @Test
    void functionThatIsNeverUnblockedIsRejected() {
        var neverReleased = new CountDownLatch(1);

        var failure = assertAsserterFails("a function that is never unblocked",
                () -> new BlockedThreadAsserter().assertUnblocksAfter(
                        () -> awaitQuietly(neverReleased),
                        () -> log.debug("Unblocker that does not unblock anything"),
                        SHORT_RETURN_BUDGET));

        Truth.assertThat(failure).isInstanceOf(ConditionTimeoutException.class);
    }

    @Test
    void functionThatThrowsImmediatelyIsRejected() {
        var failure = assertAsserterFails("a function that throws instead of blocking",
                () -> new BlockedThreadAsserter().assertUnblocksAfter(
                        () -> {
                            throw new IllegalStateException("blew up instead of blocking");
                        },
                        () -> log.debug("Nothing to unblock"),
                        SHORT_RETURN_BUDGET));

        Truth.assertThat(failure).isInstanceOf(AssertionError.class);
        Truth.assertThat(failure).hasMessageThat().contains("blew up instead of blocking");
    }

    @Test
    void functionThatThrowsOnceUnblockedIsRejected() {
        var releaseMe = new CountDownLatch(1);

        var failure = assertAsserterFails("a function that blocks, then throws once released",
                () -> new BlockedThreadAsserter().assertUnblocksAfter(
                        () -> {
                            awaitQuietly(releaseMe);
                            throw new IllegalStateException("blew up on the way out");
                        },
                        releaseMe::countDown,
                        SHORT_RETURN_BUDGET));

        Truth.assertThat(failure).isInstanceOf(AssertionError.class);
        Truth.assertThat(failure).hasMessageThat().contains("blew up on the way out");
    }

    /**
     * The sibling idiom needs its own control: {@link BlockedThreadAsserter#assertFunctionBlocks} must reject a
     * function that returns straight away rather than staying blocked for the window it was given.
     */
    @Test
    void assertFunctionBlocksRejectsAFunctionThatReturnsImmediately() {
        var failure = assertAsserterFails("a function that does not block at all",
                () -> new BlockedThreadAsserter().assertFunctionBlocks(
                        () -> log.debug("Not blocking at all"),
                        Duration.ofMillis(500)));

        Truth.assertThat(failure).isInstanceOf(AssertionError.class);
        Truth.assertThat(failure).hasMessageThat().contains("should be sleeping/blocked");
    }

    /**
     * Runs an assertion that is expected to fail, and returns how it failed.
     * <p>
     * Both outcomes count as "the asserter rejected it": a Truth {@link AssertionError} for the ordering and
     * still-blocked checks, and Awaitility's {@link ConditionTimeoutException} for the never-returns one. What is
     * not tolerated is the call returning normally - that is the asserter passing something it must not.
     */
    private static Throwable assertAsserterFails(final String description, final Runnable assertionExpectedToFail) {
        try {
            assertionExpectedToFail.run();
        } catch (AssertionError | ConditionTimeoutException rejected) {
            log.debug("Rejected as expected ({}): {}", description, rejected.toString());
            return rejected;
        }
        throw new AssertionError("BlockedThreadAsserter accepted " + description + ", but it must have rejected it");
    }

    private static void awaitQuietly(final CountDownLatch latch) {
        try {
            // generous: this is the "stays blocked" side of every case here, so it must outlast the budget the
            // asserter is given, or the block would end on a timeout rather than on the unblocker
            latch.await(60, SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
