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
    private static KeyOrderLedger.Delivery d(String incarnation, int partition, long epoch, String key,
                                             long offset, long startSeq, long endSeq) {
        return new KeyOrderLedger.Delivery(incarnation, partition, epoch, key, offset, startSeq, endSeq);
    }

    /** A sequential, non-overlapping run of offsets in one window, starting at sequence {@code fromSeq}. */
    private static List<KeyOrderLedger.Delivery> sequential(String incarnation, long epoch, String key,
                                                            long fromSeq, long... offsets) {
        List<KeyOrderLedger.Delivery> deliveries = new ArrayList<>();
        long seq = fromSeq;
        for (long offset : offsets) {
            deliveries.add(d(incarnation, P0, epoch, key, offset, seq, seq + 1));
            seq += 2;
        }
        return deliveries;
    }

    // --- the check must be able to fire ---

    @Test
    void outOfOrderInsideOneWindowIsCaught() {
        // one instance, one partition, one epoch, one key: 10, then 12, then 11 - a real regression
        List<KeyOrderLedger.Delivery> history = of(
                d(PC_A, P0, 4, "k-1", 10, 1, 2),
                d(PC_A, P0, 4, "k-1", 12, 3, 4),
                d(PC_A, P0, 4, "k-1", 11, 5, 6));

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
                d(PC_A, P0, 4, "k-1", 10, 1, 4),
                d(PC_A, P0, 4, "k-1", 11, 2, 3));

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
                d(PC_A, P0, 4, "k-1", 10, 1, 2),
                d(PC_A, P0, 4, "k-1", 12, 3, 4),
                d(PC_A, P0, 4, "k-1", 11, 5, 6)));
        Collections.shuffle(history, new Random(42));

        assertThat(KeyOrderLedger.check(history).get(0)).contains("LEDGER_KEY_ORDER");
    }

    @Test
    void everyRegressionIsCountedButOnlyASampleIsPrinted() {
        List<KeyOrderLedger.Delivery> history = new ArrayList<>();
        history.add(d(PC_A, P0, 4, "k-1", 1_000, 1, 2));
        long seq = 3;
        for (int offset = 0; offset < KeyOrderLedger.MAX_REPORTED_PER_KIND + 3; offset++) {
            history.add(d(PC_A, P0, 4, "k-1", offset, seq, seq + 1));
            seq += 2;
        }

        List<String> problems = KeyOrderLedger.check(history);

        assertThat(problems).hasSize(1);
        assertThat(problems.get(0)).contains((KeyOrderLedger.MAX_REPORTED_PER_KIND + 3) + " per-key ordering regression");
        // the sample is capped: a systemic break must not print thousands of identical lines
        assertThat(problems.get(0)).doesNotContain("offset " + (KeyOrderLedger.MAX_REPORTED_PER_KIND + 2) + " of key");
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
                d(PC_A, P0, 4, "k-1", 9, 1, 2),
                d(PC_A, P0, 4, "k-1", 10, 3, 9),
                d(PC_A, P0, 5, "k-1", 10, 4, 6),
                d(PC_A, P0, 5, "k-1", 11, 7, 8)));

        assertThat(KeyOrderLedger.check(history)).isEmpty();
    }

    @Test
    void thePartitionIsPartOfTheWindow() {
        // the KEY shard carries the record's TopicPartition, so the same key on two partitions is two
        // independent orders (only reachable with a non-default partitioner, but the window says so)
        List<KeyOrderLedger.Delivery> history = new ArrayList<>(of(
                d(PC_A, 0, 4, "k-1", 10, 1, 2),
                d(PC_A, 0, 4, "k-1", 11, 3, 4),
                d(PC_A, 7, 4, "k-1", 5, 5, 6),
                d(PC_A, 7, 4, "k-1", 6, 7, 8)));

        assertThat(KeyOrderLedger.check(history)).isEmpty();
    }

    @Test
    void differentKeysAreIndependentOfEachOther() {
        // the concurrency PC exists to give: k-2's whole sequence runs interleaved with, and out of
        // offset order relative to, k-1's - which is not an order anyone promised
        List<KeyOrderLedger.Delivery> history = new ArrayList<>(of(
                d(PC_A, P0, 4, "k-1", 40, 1, 4),
                d(PC_A, P0, 4, "k-2", 5, 2, 3),
                d(PC_A, P0, 4, "k-2", 6, 5, 6),
                d(PC_A, P0, 4, "k-1", 41, 7, 8)));

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
    void anUnfinishedDeliveryOnlySuppressesTheOverlapClaim() {
        // torn down mid-flight: its end is unknowable, so the overlap check on the next delivery is
        // skipped (the safe direction) - but the ordering regression is still reported
        List<KeyOrderLedger.Delivery> history = new ArrayList<>(of(
                d(PC_A, P0, 4, "k-1", 12, 1, KeyOrderLedger.Delivery.UNFINISHED),
                d(PC_A, P0, 4, "k-1", 11, 2, 3)));

        List<String> problems = KeyOrderLedger.check(history);

        assertThat(problems).hasSize(1);
        assertThat(problems.get(0)).contains("LEDGER_KEY_ORDER");
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
                d(PC_A, P0, 4, "key-1", 10, 1, 2),
                d(PC_A, P0, 4, "key-2", 11, 3, 4),
                d(PC_A, P0, 4, "key-3", 12, 5, 6)));

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
