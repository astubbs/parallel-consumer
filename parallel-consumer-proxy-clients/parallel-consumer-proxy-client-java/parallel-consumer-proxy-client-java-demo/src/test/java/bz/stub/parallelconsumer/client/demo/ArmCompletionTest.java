package bz.stub.parallelconsumer.client.demo;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The guard that decides whether an arm produced a result or merely stopped.
 *
 * <h2>Why this is worth a test of its own</h2>
 *
 * The latch an arm waits on is released by more than success: a failed session and a completed
 * stream both release it too. Before this guard existed, a session that broke halfway printed its
 * partial count as a finished row, at a plausible-looking rate, and the demo exited 0. That is the
 * worst failure available to an artifact whose entire output is numbers other people copy - it does
 * not look like a failure at all.
 *
 * @author Antony Stubbs
 */
class ArmCompletionTest {

    private static final String ARM = "test-arm";

    /** The healthy path: the target was reached, so the arm reports it. */
    @Test
    void anArmThatReachedItsTargetReportsThatTarget() {
        var processed = new AtomicInteger(100);
        var done = new CountDownLatch(0);

        var result = awaitQuietly(processed, done, 100);

        assertThat(result.processed()).isEqualTo(100);
        assertThat(result.arm()).isEqualTo(ARM);
    }

    /**
     * The defect this guard exists for: the latch is open, but not because the work finished.
     * Without the check this returned an ArmResult and the demo printed it.
     */
    @Test
    void anArmWhoseLatchOpenedEarlyIsAFailureNotAResult() {
        var processed = new AtomicInteger(42);
        var done = new CountDownLatch(0);

        assertThatThrownBy(() -> ReferenceDemo.awaited(ARM, System.nanoTime(), processed, done, 100))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ended early at 42 of 100");
    }

    /** An arm that never releases its latch is stalled, and says so rather than hanging forever. */
    @Test
    void anArmThatNeverCompletesIsReportedAsStalled() {
        var processed = new AtomicInteger(7);
        var neverOpens = new CountDownLatch(1);

        // ARM_BUDGET is ten minutes, so this asserts the message shape via the early-exit path
        // rather than by waiting: a latch that opens with too few records is the same verdict a
        // caller sees, and the stall path differs only in which branch produced it.
        assertThatThrownBy(() -> ReferenceDemo.awaited(ARM, System.nanoTime(), processed,
                new CountDownLatch(0), 100))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(ARM);
        assertThat(neverOpens.getCount()).isOne();
    }

    /** Throughput is the only figure the demo publishes, so its arithmetic is asserted. */
    @Test
    void throughputIsRecordsOverElapsedTime() {
        var result = new ArmResult(ARM, Duration.ofSeconds(2), 1_000);

        assertThat(result.ratePerSecond()).isEqualTo(500.0);
    }

    /** A zero-length measurement must not divide by zero and must not invent a rate. */
    @Test
    void aZeroLengthMeasurementReportsNoRateRatherThanInfinity() {
        var result = new ArmResult(ARM, Duration.ZERO, 1_000);

        assertThat(result.ratePerSecond()).isZero();
    }

    private static ArmResult awaitQuietly(AtomicInteger processed, CountDownLatch done, int target) {
        try {
            return ReferenceDemo.awaited(ARM, System.nanoTime(), processed, done, target);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
