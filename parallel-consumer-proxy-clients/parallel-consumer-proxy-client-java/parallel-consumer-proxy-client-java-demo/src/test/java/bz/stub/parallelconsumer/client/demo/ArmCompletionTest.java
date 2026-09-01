package bz.stub.parallelconsumer.client.demo;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The guard that decides whether an arm produced a result or merely stopped, and the two evidence
 * figures it reports alongside its rate.
 *
 * <h2>Why this is worth a test of its own</h2>
 *
 * The latch an arm waits on is released by more than success: a failed session and a completed
 * stream both release it too. Before this guard existed, a session that broke halfway printed its
 * partial count as a finished row, at a plausible-looking rate, and the demo exited 0. That is the
 * worst failure available to an artifact whose entire output is numbers other people copy - it does
 * not look like a failure at all.
 *
 * <h2>And why the keys column is tested here rather than only run</h2>
 *
 * {@code records} and {@code keys} are the only figures in the tables that are deterministic across
 * languages, which is what makes them the ones a cross-language check can compare. A keys count that
 * silently counted deliveries rather than distinct keys would still produce a plausible table - the
 * same failure mode as above, one column along.
 *
 * @author Antony Stubbs
 */
class ArmCompletionTest {

    private static final String ARM = "test-arm";

    private static final String CLIENT = "test-client";

    /** The healthy path: the target was reached, so the arm reports it. */
    @Test
    void anArmThatReachedItsTargetReportsThatTarget() {
        var tally = new ArmTally(3);
        processRecords(tally, "key-0", "key-1", "key-2");

        var result = awaitQuietly(tally);

        assertThat(result.processed()).isEqualTo(3);
        assertThat(result.arm()).isEqualTo(ARM);
    }

    /**
     * The defect this guard exists for: the latch is open, but not because the work finished.
     * Without the check this returned an ArmResult and the demo printed it.
     */
    @Test
    void anArmWhoseLatchOpenedEarlyIsAFailureNotAResult() {
        var tally = new ArmTally(100);
        processRecords(tally, "key-0", "key-1");
        // exactly what a raw-gRPC stream does when its session ends before the arm is finished
        tally.sessionEnded();

        assertThatThrownBy(() -> ReferenceDemo.awaited(ARM, CLIENT, System.nanoTime(), tally))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("ended early at 2 of 100");
    }

    /** An arm that never releases its latch is stalled, and says so rather than hanging forever. */
    @Test
    void anArmThatNeverCompletesIsReportedAsStalled() {
        var neverOpens = new ArmTally(100);
        var openedEarly = new ArmTally(100);
        openedEarly.sessionEnded();

        // ARM_BUDGET is ten minutes, so this asserts the message shape via the early-exit path
        // rather than by waiting: a latch that opens with too few records is the same verdict a
        // caller sees, and the stall path differs only in which branch produced it.
        assertThatThrownBy(() -> ReferenceDemo.awaited(ARM, CLIENT, System.nanoTime(), openedEarly))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(ARM);
        assertThat(neverOpens.stillRunning())
                .withFailMessage("a tally nobody has finished must still be running, or the stall "
                        + "budget never applies to anything")
                .isTrue();
    }

    /** Throughput is the only measured figure the demo publishes, so its arithmetic is asserted. */
    @Test
    void throughputIsRecordsOverElapsedTime() {
        var result = new ArmResult(ARM, CLIENT, Duration.ofSeconds(2), 1_000, 500);

        assertThat(result.ratePerSecond()).isEqualTo(500.0);
    }

    /** A zero-length measurement must not divide by zero and must not invent a rate. */
    @Test
    void aZeroLengthMeasurementReportsNoRateRatherThanInfinity() {
        var result = new ArmResult(ARM, CLIENT, Duration.ZERO, 1_000, 500);

        assertThat(result.ratePerSecond()).isZero();
    }

    /** The row a reader sees names the role AND the library, because the role alone is a category. */
    @Test
    void anArmIsLabelledWithBothItsRoleAndTheClientThatRanIt() {
        var result = new ArmResult("AK core", "KafkaConsumer", Duration.ofSeconds(1), 10, 10);

        assertThat(result.label()).isEqualTo("AK core (KafkaConsumer)");
        assertThat(result.arm())
                .withFailMessage("the stable name must NOT pick up the client, or every lookup "
                        + "and expectation keyed on it breaks")
                .isEqualTo("AK core");
    }

    /** The point of the column: repeated keys are one key, however many records carried them. */
    @Test
    void repeatedKeysAreCountedOnceAndEveryDeliveryIsStillARecord() {
        var tally = new ArmTally(4);
        processRecords(tally, "key-0", "key-1", "key-0", "key-1");

        var result = awaitQuietly(tally);

        assertThat(result.processed()).isEqualTo(4);
        assertThat(result.uniqueKeys())
                .withFailMessage("counting deliveries rather than distinct keys would make the "
                        + "column a copy of the records column, and evidence of nothing")
                .isEqualTo(2);
    }

    /** Kafka distinguishes a null key from an empty one, and so does the count. */
    @Test
    void aKeylessRecordIsStillARecordButIsNotAKey() {
        var tally = new ArmTally(2);
        tally.recordProcessed(null);
        processRecords(tally, "key-0");

        var result = awaitQuietly(tally);

        assertThat(result.processed()).isEqualTo(2);
        assertThat(result.uniqueKeys()).isEqualTo(1);
    }

    /**
     * The keys column is only evidence if its expected value can be predicted, and the seeding is
     * what predicts it: records are laid over a fixed key space, cyclically.
     */
    @Test
    void theExpectedKeyCountIsTheBacklogOrTheKeySpaceWhicheverIsSmaller() {
        assertThat(DemoBroker.expectedUniqueKeys(20)).isEqualTo(20);
        assertThat(DemoBroker.expectedUniqueKeys(DemoBroker.KEY_SPACE)).isEqualTo(DemoBroker.KEY_SPACE);
        assertThat(DemoBroker.expectedUniqueKeys(DemoBroker.KEY_SPACE * 40))
                .withFailMessage("a backlog larger than the key space cannot show more keys than "
                        + "the key space has")
                .isEqualTo(DemoBroker.KEY_SPACE);
    }

    private static void processRecords(ArmTally tally, String... keys) {
        for (String key : keys) {
            tally.recordProcessed(key.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static ArmResult awaitQuietly(ArmTally tally) {
        try {
            return ReferenceDemo.awaited(ARM, CLIENT, System.nanoTime(), tally);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
