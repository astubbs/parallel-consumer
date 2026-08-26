package bz.stub.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Boundary regression for {@link KeyOrderLedger#check} - the per-key ordering half of the end-of-run
 * correctness ledger, pinned against CONSTRUCTED histories rather than only against live chaos.
 * <p>
 * Both directions are the point, and the second one is the one that decides whether the check survives
 * contact with the suite:
 * <ul>
 *   <li>a genuine ordering violation must be caught - a bound that cannot fire is decoration;</li>
 *   <li>the LEGITIMATE at-least-once redelivery that churn produces on purpose (an assignment moves, a
 *   revoked partition's uncommitted work is re-run from the last commit, so an earlier offset follows a
 *   later one) must NOT be caught. A detector that fires on correct behaviour gets disabled within a
 *   week, and takes the suite's credibility with it.</li>
 * </ul>
 * Pure function, no broker - and deliberately NOT tagged {@code chaos}, so it gates every default
 * integration build, exactly like its sibling {@link ProgressProbeLedgerIT}.
 */
class KeyOrderLedgerIT {

    private static final String PC_A = "PC-1#1";
    private static final String PC_B = "PC-2#1";
    private static final int P0 = 0;

    /** A delivery that ran to completion between {@code startSeq} and {@code endSeq}. */
    /**
     * Builds a delivery that FINISHED. Overloaded so the fixtures can keep writing plain integer
     * literals. Java widens {@code int -> long} or boxes {@code long -> Long}, never both in one
     * step, so without this every end value would need an {@code L} suffix - 34 call sites edited to
     * satisfy a signature rather than to say anything.
     */
    private static KeyOrderLedger.Delivery delivery(String incarnation, int partition, long epoch, String key,
                                             long offset, long startSeq, int endSeq) {
        return delivery(incarnation, partition, epoch, key, offset, startSeq, Long.valueOf(endSeq));
    }

    /**
     * The real one. {@code endSeq} is {@code null} for a delivery that never finished - the absence
     * of an end, not a value standing in for one. {@code null} is not applicable to the {@code int}
     * overload above, so it resolves here unambiguously.
     */
    private static KeyOrderLedger.Delivery delivery(String incarnation, int partition, long epoch, String key,
                                             long offset, long startSeq, Long endSeq) {
        return new KeyOrderLedger.Delivery(incarnation, partition, epoch, key, offset, startSeq, endSeq);
    }

    /** A sequential, non-overlapping run of offsets in one window, starting at sequence {@code fromSeq}. */
    private static List<KeyOrderLedger.Delivery> sequential(String incarnation, long epoch, String key,
                                                            long fromSeq, long... offsets) {
        List<KeyOrderLedger.Delivery> deliveries = new ArrayList<>();
        long seq = fromSeq;
        for (long offset : offsets) {
            deliveries.add(delivery(incarnation, P0, epoch, key, offset, seq, seq + 1));
            seq += 2;
        }
        return deliveries;
    }

    // --- the check must be able to fire ---

    @Test
    void outOfOrderInsideOneWindowIsCaught() {
        // one instance, one partition, one epoch, one key: 10, then 12, then 11 - a real regression
        List<KeyOrderLedger.Delivery> history = of(
                delivery(PC_A, P0, 4, "k-1", 10, 1, 2L),
                delivery(PC_A, P0, 4, "k-1", 12, 3, 4L),
                delivery(PC_A, P0, 4, "k-1", 11, 5, 6L));

        List<String> problems = KeyOrderLedger.check(history);

        assertThat(problems).hasSize(1);
        assertThat(problems.get(0)).contains("LEDGER_KEY_ORDER");
        assertThat(problems.get(0)).contains("1 per-key ordering regression");
        assertThat(problems.get(0)).contains("offset 11 of key 'k-1'");
    }

    @Test
    void twoDeliveriesOfOneKeyInFlightAtOnceIsCaught() {
        // offsets ascend, so the order check is happy - but the two executions OVERLAP (offset 11 starts
        // at seq 2, before offset 10 ends at seq 4), which abandons per-key order rather than reordering it
        List<KeyOrderLedger.Delivery> history = of(
                delivery(PC_A, P0, 4, "k-1", 10, 1, 4L),
                delivery(PC_A, P0, 4, "k-1", 11, 2, 3L));

        List<String> problems = KeyOrderLedger.check(history);

        assertThat(problems).hasSize(1);
        assertThat(problems.get(0)).contains("LEDGER_KEY_CONCURRENCY");
        assertThat(problems.get(0)).contains("1 overlapping delivery pair");
    }

    @Test
    void aRegressionIsFoundEvenWhenTheHistoryArrivesOutOfOrder() {
        // the live recorder claims its sequence atomically but appends after, so two worker threads can
        // enqueue in the opposite order to the one they started in - check() sorts by startSeq, and this
        // is what proves it does
        List<KeyOrderLedger.Delivery> history = new ArrayList<>(of(
                delivery(PC_A, P0, 4, "k-1", 10, 1, 2L),
                delivery(PC_A, P0, 4, "k-1", 12, 3, 4L),
                delivery(PC_A, P0, 4, "k-1", 11, 5, 6L)));
        Collections.shuffle(history, new Random(42));

        assertThat(KeyOrderLedger.check(history).get(0)).contains("LEDGER_KEY_ORDER");
    }

    @Test
    void everyRegressionIsCountedButOnlyASampleIsPrinted() {
        List<KeyOrderLedger.Delivery> history = new ArrayList<>();
        history.add(delivery(PC_A, P0, 4, "k-1", 1_000, 1, 2L));
        long seq = 3;
        for (int offset = 0; offset < KeyOrderLedger.MAX_REPORTED_PER_KIND + 3; offset++) {
            history.add(delivery(PC_A, P0, 4, "k-1", offset, seq, seq + 1));
            seq += 2;
        }

        List<String> problems = KeyOrderLedger.check(history);

        assertThat(problems).hasSize(1);
        assertThat(problems.get(0)).contains((KeyOrderLedger.MAX_REPORTED_PER_KIND + 3) + " per-key ordering regression");
        // the sample is capped: a systemic break must not print thousands of identical lines
        assertThat(problems.get(0)).doesNotContain("offset " + (KeyOrderLedger.MAX_REPORTED_PER_KIND + 2) + " of key");
    }

    @Test
    void everyOverlapIsCountedButOnlyASampleIsPrinted() {
        // sibling of the regression-cap test above, for the LEDGER_KEY_CONCURRENCY kind: one long
        // delivery spans the window, every later one overlaps it, offsets ascend so order stays quiet
        List<KeyOrderLedger.Delivery> history = new ArrayList<>();
        history.add(delivery(PC_A, P0, 4, "k-1", 10, 1, 1_000));
        long seq = 2;
        int overlapping = KeyOrderLedger.MAX_REPORTED_PER_KIND + 3;
        for (int i = 1; i <= overlapping; i++) {
            history.add(delivery(PC_A, P0, 4, "k-1", 10 + i, seq, seq + 1));
            seq += 2;
        }

        List<String> problems = KeyOrderLedger.check(history);

        assertThat(problems).hasSize(1);
        assertThat(problems.get(0)).contains(overlapping + " overlapping delivery pair");
        // the sample is capped: a systemic break must not print thousands of identical lines
        assertThat(problems.get(0)).doesNotContain("offset " + (10 + overlapping) + " of key");
    }

    // --- and must NOT fire on the legitimate redelivery churn produces on purpose ---

    @Test
    void redeliveryAfterARevokeOpensANewWindow() {
        // THE false-positive case: epoch 4 processed 10,11,12; the partition was revoked and reassigned
        // (epoch 5), so uncommitted work is legitimately re-run from the last commit - 10 after 12
        List<KeyOrderLedger.Delivery> history = new ArrayList<>();
        history.addAll(sequential(PC_A, 4, "k-1", 1, 10, 11, 12));
        history.addAll(sequential(PC_A, 5, "k-1", 7, 10, 11, 12));

        assertWithMessage("redelivery after a revoke is the contract working, not a violation")
                .that(KeyOrderLedger.check(history)).isEmpty();
    }

    @Test
    void redeliveryToAnotherInstanceIsNotAViolation() {
        // the assignment moved to a different member, which re-runs from the last commit
        List<KeyOrderLedger.Delivery> history = new ArrayList<>();
        history.addAll(sequential(PC_A, 4, "k-1", 1, 10, 11, 12));
        history.addAll(sequential(PC_B, 0, "k-1", 7, 10, 11, 12));

        assertThat(KeyOrderLedger.check(history)).isEmpty();
    }

    @Test
    void aRestartOfTheSameInstanceIsNotAViolation() {
        // a chaos restart builds a fresh PC whose epoch counter starts again at zero - keying the window
        // on the chaos instance id instead of the INCARNATION would collide these two lifetimes and call
        // the second one's legitimate redelivery a regression
        List<KeyOrderLedger.Delivery> history = new ArrayList<>();
        history.addAll(sequential("PC-3#1", 0, "k-1", 1, 10, 11, 12));
        history.addAll(sequential("PC-3#2", 0, "k-1", 7, 10, 11, 12));

        assertThat(KeyOrderLedger.check(history)).isEmpty();
    }

    @Test
    void aHeavyRecordStillRunningWhenItsPartitionIsRevokedIsNotAnOverlap() {
        // offset 10 of epoch 4 is a heavy record: PC does not interrupt in-flight work on revoke, so it
        // is still running (ends at seq 9) while epoch 5's redelivery of 10 and 11 runs. Different
        // windows - the epoch is read off the record's own WorkContainer, so the straggler keeps epoch 4
        List<KeyOrderLedger.Delivery> history = new ArrayList<>(of(
                delivery(PC_A, P0, 4, "k-1", 9, 1, 2L),
                delivery(PC_A, P0, 4, "k-1", 10, 3, 9L),
                delivery(PC_A, P0, 5, "k-1", 10, 4, 6L),
                delivery(PC_A, P0, 5, "k-1", 11, 7, 8L)));

        assertThat(KeyOrderLedger.check(history)).isEmpty();
    }

    @Test
    void thePartitionIsPartOfTheWindow() {
        // the KEY shard carries the record's TopicPartition, so the same key on two partitions is two
        // independent orders (only reachable with a non-default partitioner, but the window says so)
        List<KeyOrderLedger.Delivery> history = new ArrayList<>(of(
                delivery(PC_A, 0, 4, "k-1", 10, 1, 2L),
                delivery(PC_A, 0, 4, "k-1", 11, 3, 4L),
                delivery(PC_A, 7, 4, "k-1", 5, 5, 6L),
                delivery(PC_A, 7, 4, "k-1", 6, 7, 8L)));

        assertThat(KeyOrderLedger.check(history)).isEmpty();
    }

    @Test
    void differentKeysAreIndependentOfEachOther() {
        // the concurrency PC exists to give: k-2's whole sequence runs interleaved with, and out of
        // offset order relative to, k-1's - which is not an order anyone promised
        List<KeyOrderLedger.Delivery> history = new ArrayList<>(of(
                delivery(PC_A, P0, 4, "k-1", 40, 1, 4L),
                delivery(PC_A, P0, 4, "k-2", 5, 2, 3L),
                delivery(PC_A, P0, 4, "k-2", 6, 5, 6L),
                delivery(PC_A, P0, 4, "k-1", 41, 7, 8L)));

        assertThat(KeyOrderLedger.check(history)).isEmpty();
    }

    @Test
    void theSameOffsetTwiceIsTheDuplicateLedgersBusiness() {
        // a redelivery of the SAME record is a duplicate, bounded by ProgressProbe#ledger - it is not an
        // out-of-order execution, and reporting it here would double-count one event as two defects
        List<KeyOrderLedger.Delivery> history = sequential(PC_A, 4, "k-1", 1, 10, 10, 11);

        assertThat(KeyOrderLedger.check(history)).isEmpty();
    }

    @Test
    void aWedgedDeliveryStillOverlapsLaterStartsInItsWindow() {
        // THE confluentinc#857 wedge shape: offset 10 never finishes (a stuck worker holds the shard),
        // yet PC hands offset 11 of the SAME window to another worker. Offsets ascend, so the order
        // half is silent - only the overlap half can see this, and a never-finished delivery overlaps
        // every later start in its window BY CONSTRUCTION (finished() is finally-guaranteed, so
        // UNFINISHED means genuinely still running, not instrumentation noise)
        List<KeyOrderLedger.Delivery> history = new ArrayList<>(of(
                delivery(PC_A, P0, 4, "k-1", 10, 1, null),
                delivery(PC_A, P0, 4, "k-1", 11, 2, 3L)));

        List<String> problems = KeyOrderLedger.check(history);

        assertWithMessage("a wedged delivery is in flight for the rest of the run - a later same-window "
                + "start is a certain overlap, not an unknowable one")
                .that(problems).hasSize(1);
        assertThat(problems.get(0)).contains("LEDGER_KEY_CONCURRENCY");
    }

    @Test
    void anUnfinishedDeliveryReportsBothTheRegressionAndTheOverlap() {
        // still running at teardown: the ordering regression (11 after 12) is reported, AND the
        // never-finished 12 is an open interval, so 11 starting inside it is also a certain overlap
        List<KeyOrderLedger.Delivery> history = new ArrayList<>(of(
                delivery(PC_A, P0, 4, "k-1", 12, 1, null),
                delivery(PC_A, P0, 4, "k-1", 11, 2, 3L)));

        List<String> problems = KeyOrderLedger.check(history);

        assertThat(problems).hasSize(2);
        assertThat(problems.get(0)).contains("LEDGER_KEY_ORDER");
        assertThat(problems.get(1)).contains("LEDGER_KEY_CONCURRENCY");
    }

    @Test
    void aFinishedEndIsNotForgottenWhenAnUnfinishedDeliveryFollowsIt() {
        // d1 ends at seq 10; d2 (unfinished) is caught overlapping d1; d3 starts at seq 4, inside
        // BOTH d1's known interval and d2's open one - the known end must survive d2, not be wiped
        List<KeyOrderLedger.Delivery> history = new ArrayList<>(of(
                delivery(PC_A, P0, 4, "k-1", 20, 1, 10L),
                delivery(PC_A, P0, 4, "k-1", 21, 2, null),
                delivery(PC_A, P0, 4, "k-1", 22, 4, 5L)));

        List<String> problems = KeyOrderLedger.check(history);

        assertThat(problems).hasSize(1);
        assertThat(problems.get(0)).contains("LEDGER_KEY_CONCURRENCY");
        assertThat(problems.get(0)).contains("2 overlapping delivery pair");
    }

    @Test
    void anOrderedRunIsClean() {
        assertThat(KeyOrderLedger.check(sequential(PC_A, 4, "k-1", 1, 10, 11, 12, 13))).isEmpty();
    }

    // --- and must say so when it asserted nothing ---

    @Test
    void aUniqueKeyPerRecordIsReportedAsVacuous() {
        // exactly what W1/W4 produce: every window holds one delivery, so nothing is ever compared. The
        // check going quiet must be a failure, not a pass
        List<KeyOrderLedger.Delivery> history = new ArrayList<>(of(
                delivery(PC_A, P0, 4, "key-1", 10, 1, 2L),
                delivery(PC_A, P0, 4, "key-2", 11, 3, 4L),
                delivery(PC_A, P0, 4, "key-3", 12, 5, 6L)));

        List<String> problems = KeyOrderLedger.check(history);

        assertThat(problems).hasSize(1);
        assertThat(problems.get(0)).contains("LEDGER_ORDER_VACUOUS");
        assertThat(problems.get(0)).contains("3 deliveries across 3 keys");
    }

    @Test
    void anEmptyHistoryIsVacuous() {
        assertThat(KeyOrderLedger.check(of()).get(0)).contains("LEDGER_ORDER_VACUOUS");
    }

    @Test
    void aScenarioThatMakesNoOrderingClaimRecordsNothingAndIsNotJudged() {
        // the UNORDERED scenarios pass no recorder at all, which must be silence rather than a vacuity
        // complaint about a history they never had
        assertThat(KeyOrderLedger.checkIfRecording(null)).isEmpty();
    }
}
