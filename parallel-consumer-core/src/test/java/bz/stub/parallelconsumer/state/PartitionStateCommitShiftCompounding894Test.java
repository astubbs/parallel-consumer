package bz.stub.parallelconsumer.state;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.offsets.OffsetDecodingError;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Follow-up to {@link PartitionStateCommitEncodeShift894Test}, settling the two things the approving reviewer on
 * <a href="https://github.com/confluentinc/parallel-consumer/pull/893">confluentinc#893</a> suspected but never
 * chased. His words, 2025-11-05:
 * <blockquote>
 * The Parallel Consumer has a bug somewhere in marking state dirty and advancing offset to commit by 1 - so after
 * multiple rebalances it ends up committing not offset 10 - but offset 11 - which brings subscription out of valid
 * range and causes auto offset reset to happen
 * </blockquote>
 * That splits into two claims, one per test here.
 * <p>
 * <b>A - the shift is +1, not +2, at the right fixture.</b> The sibling test shifts by two, because completing the
 * only incomplete empties the set and the second read of the offset to commit jumps to
 * {@code offsetHighestSucceeded + 1}. With a second incomplete sitting above the one that completes, the second read
 * instead returns the next incomplete, and the shift is exactly one. The magnitude is a property of the state at
 * commit time, not a constant of the defect - so "committing 11 not 10" needs no separate bug to explain it.
 * <p>
 * <b>B - what repeating it actually does.</b> Encode, commit, decode into a fresh {@link PartitionState}, deliver
 * whatever records really exist, race again - for as many cycles as the partition allows. What compounds, what
 * plateaus, and whether the committed offset ever passes the log end offset is measured, not assumed; the per-cycle
 * ledger is in the failure message and in the log either way.
 * <p>
 * The partition is <b>static</b> - offsets 0 to 3 exist and nothing further is ever produced. That is
 * <a href="https://github.com/confluentinc/parallel-consumer/issues/894">confluentinc#894</a>'s own first
 * precondition ("no new offset on this partition") and the reporter's flow diagram repeats it.
 *
 * @author Antony Stubbs
 * @see PartitionStateCommitEncodeShift894Test the single-hop reproduction this builds on
 */
@Slf4j
class PartitionStateCommitShiftCompounding894Test {

    ModelUtils mu = new ModelUtils(new PCModuleTestEnv());

    TopicPartition tp = new TopicPartition("topic", 0);

    /** Offsets 0 to 3 were produced, and nothing after them ever is. */
    static final long HIGHEST_OFFSET_EVER_PRODUCED = 3L;

    /** One past the highest offset produced - a commit above this is out of range on the next poll. */
    static final long LOG_END_OFFSET = HIGHEST_OFFSET_EVER_PRODUCED + 1;

    /** Enough rebalance cycles to tell compounding from a plateau, and to let a self-correction show itself. */
    static final int CYCLES = 6;

    /**
     * Log end offset of the single-incomplete fixture: offsets 0 to 2 have been produced. The growing-partition
     * loop starts here and adds to it.
     */
    static final long SINGLE_INCOMPLETE_LOG_END_OFFSET = 3L;

    /**
     * How many records the partition produces between one commit cycle and the next. The single free parameter of
     * the growing-partition loop, swept rather than picked: 0 reproduces the static case, and the rest bracket the
     * point where new records arrive faster than the fabricated state runs away from them. It is stated here rather
     * than buried in the loop because it is the one assumption the result depends on.
     */
    static final long[] RECORDS_PRODUCED_PER_CYCLE = {0L, 1L, 2L, 3L};

    /**
     * Highest offset polled by the wide-payload fixture. With one offset left outstanding at 1, the encoded payload
     * spans 1 to here - ten offsets, against the two the narrow fixture spans.
     */
    static final long WIDE_GAP_HIGHEST_POLLED = 10L;

    /** The offset the wide-payload fixture leaves outstanding, to be raced. */
    static final List<Long> WIDE_GAP_OUTSTANDING = Collections.singletonList(1L);

    /** Traffic rates swept against the wide payload - below, beside and above its width. */
    static final long[] WIDE_GAP_RECORDS_PRODUCED_PER_CYCLE = {1L, 5L, 9L, 10L, 11L};

    /** Long enough for a slow compounding run to show its shape, and to halt on its own if it is going to. */
    static final int WIDE_GAP_CYCLES = 15;

    /** Highest offset the single-incomplete fixture polls. */
    static final long SINGLE_INCOMPLETE_HIGHEST_POLLED = 2L;

    /** The offset the single-incomplete fixture leaves outstanding, to be raced. */
    static final List<Long> SINGLE_INCOMPLETE_OUTSTANDING = Collections.singletonList(1L);

    /**
     * Claim A, measured: the shift is a property of the fixture. The single-incomplete state shifts the commit two
     * above the base its payload was encoded against; the two-incomplete state shifts it exactly one. Both are the
     * same defect and the same window - only the state at commit time differs.
     * <p>
     * The assertion is the invariant, unchanged from the sibling test: a commit must carry the base its payload was
     * encoded against. It is asserted over both fixtures at once so the failure message reports both magnitudes
     * rather than stopping at the first.
     */
    @Test
    void theShiftMagnitudeIsAPropertyOfTheFixtureNotAConstant() throws OffsetDecodingError {
        long shiftWithOneIncomplete = measureShift(singleIncompleteState());
        long shiftWithTwoIncompletes = measureShift(twoIncompleteState());

        log.info("confluentinc#894 shift magnitudes: one incomplete -> {}, two incompletes -> {}",
                shiftWithOneIncomplete, shiftWithTwoIncompletes);

        assertWithMessage("confluentinc#894: a commit must carry the base its payload was encoded against, so every "
                + "shift here must be 0. Measured %s with a single incomplete and %s with two - the second read of "
                + "the offset to commit returns offsetHighestSucceeded + 1 when the set empties, and the next "
                + "incomplete when it does not. The reviewer's \"committing 11 not 10\" is this defect at the "
                + "two-incomplete fixture, not a separate one.",
                shiftWithOneIncomplete, shiftWithTwoIncompletes)
                .that(Arrays.asList(shiftWithOneIncomplete, shiftWithTwoIncompletes))
                .isEqualTo(Arrays.asList(0L, 0L));
    }

    /**
     * Claim B, measured: repeat the commit-decode-reassign cycle and watch what the numbers do. The invariant
     * asserted is that state restored from a commit must never name an offset the partition never produced; the
     * per-cycle ledger of committed offset, encode base, shift, and decoded state travels in the failure message so
     * the shape of the drift - compounding, plateauing or self-correcting - is legible from the failure alone.
     */
    @Test
    void repeatingTheRaceAcrossRebalances() throws OffsetDecodingError {
        List<String> ledger = new ArrayList<>();
        long worstDecodedHighestSeen = Long.MIN_VALUE;

        HighestOffsetAndIncompletes carriedOverRebalance = null;

        for (int cycle = 1; cycle <= CYCLES; cycle++) {
            RacingCommitCycleState state = (carriedOverRebalance == null)
                    ? twoIncompleteState()
                    : restoreAndRedeliver(carriedOverRebalance);

            List<Long> outstanding = sorted(state.getAllIncompleteOffsets());
            if (outstanding.isEmpty()) {
                ledger.add(String.format("cycle %d: nothing outstanding, the cycle cannot run again", cycle));
                break;
            }
            long lowestOutstanding = outstanding.get(0);
            if (lowestOutstanding >= LOG_END_OFFSET) {
                ledger.add(String.format("cycle %d: lowest outstanding offset %d was never produced (log end offset "
                                + "%d), so no record can arrive to complete it - the cycle stops here, parked below "
                                + "a phantom", cycle, lowestOutstanding, LOG_END_OFFSET));
                break;
            }

            state.armRaceOn(lowestOutstanding);
            OffsetAndMetadata committed = state.createOffsetAndMetadata();
            assertRaceFired(state, cycle);
            long encodeBase = state.firstOffsetToCommitRead();
            carriedOverRebalance = decode(committed);

            worstDecodedHighestSeen = Math.max(worstDecodedHighestSeen,
                    carriedOverRebalance.getHighestSeenOffset().orElse(Long.MIN_VALUE));
            ledger.add(String.format("cycle %d: raced %d, encoded at base %d, committed %d (shift %+d), decodes to "
                            + "highest-seen %s incompletes %s",
                    cycle, lowestOutstanding, encodeBase, committed.offset(), committed.offset() - encodeBase,
                    carriedOverRebalance.getHighestSeenOffset().orElse(null),
                    carriedOverRebalance.getIncompleteOffsets()));
        }

        String report = String.join("\n  ", ledger);
        log.info("confluentinc#894 repeated-rebalance ledger (highest offset ever produced {}, log end offset {}):"
                + "\n  {}", HIGHEST_OFFSET_EVER_PRODUCED, LOG_END_OFFSET, report);

        assertWithMessage("confluentinc#894: state restored from a commit must never name an offset the partition "
                + "never produced. Highest offset ever produced is %s. Per-cycle ledger:\n  %s",
                HIGHEST_OFFSET_EVER_PRODUCED, report)
                .that(worstDecodedHighestSeen)
                .isAtMost(HIGHEST_OFFSET_EVER_PRODUCED);
    }

    /**
     * The question the static loop could not reach: on a partition that <b>keeps producing</b>, does the overshoot
     * past the log end offset compound?
     * <p>
     * The static loop parked because the phantom offsets the shifted payload names never arrive, so the incomplete
     * set never empties and the commit never falls through to a fabricated {@code offsetHighestSucceeded + 1}. Once
     * records keep arriving those phantoms can become real, the set can empty again, and the fall-through is back in
     * reach. This runs the full cycle - encode, commit, decode, reassign, produce, poll, complete, commit - and
     * reports the committed offset against the partition's true log end offset each time.
     * <p>
     * Each cycle polls everything the partition holds from the resume position that the restored state dictates,
     * completes all of it, and lets the lowest of those completions land inside the commit rather than before it.
     * That is the maximal-progress reading: the consumer is not behind, and one of its completions is merely
     * unlucky about when it finishes.
     * <p>
     * The one free parameter is {@link #RECORDS_PRODUCED_PER_CYCLE}, swept rather than picked. The assertion is the
     * same invariant throughout - a committed offset above the log end offset is out of range on the next poll -
     * and the per-cycle ledger travels in the failure message so the shape of the answer is legible from the
     * failure alone.
     */
    @Test
    void repeatingTheRaceOnAPartitionThatKeepsProducing() throws OffsetDecodingError {
        List<String> ledger = new ArrayList<>();
        long worstOvershoot = Long.MIN_VALUE;
        int worstDropped = 0;

        for (long producedPerCycle : RECORDS_PRODUCED_PER_CYCLE) {
            SweepResult swept = runGrowingPartition(producedPerCycle, ledger);
            worstOvershoot = Math.max(worstOvershoot, swept.worstOvershoot);
            worstDropped = Math.max(worstDropped, swept.worstDropped);
        }

        String report = String.join("\n  ", ledger);
        log.info("confluentinc#894 growing-partition ledger:\n  {}", report);

        assertBothRegimes(worstOvershoot, worstDropped, report);
    }

    /**
     * One sweep point of {@link #repeatingTheRaceOnAPartitionThatKeepsProducing()}.
     * <p>
     * Skipped records are counted in two separate columns, because one is correct and the other is not. A record
     * this consumer genuinely processed in an earlier cycle is rightly recognised by
     * {@link PartitionState#isRecordPreviouslyCompleted} and skipped. A record no cycle ever ran, dismissed only
     * because a fabricated {@code offsetHighestSucceeded} sits above it, is silently dropped. Counting them together
     * would let the second hide inside the first. Both are measured and reported rather than asserted on - the
     * assertion here is about the committed offset - so the ledger carries them either way.
     *
     * @return the largest amount by which a committed offset exceeded the true log end offset, or
     *         {@link Long#MIN_VALUE} if no cycle ever committed
     */
    private SweepResult runGrowingPartition(long producedPerCycle, List<String> ledger) throws OffsetDecodingError {
        return runGrowingPartition(SINGLE_INCOMPLETE_HIGHEST_POLLED, SINGLE_INCOMPLETE_OUTSTANDING,
                producedPerCycle, CYCLES, ledger);
    }

    private SweepResult runGrowingPartition(long highestPolledAtStart,
                                            List<Long> outstandingAtStart,
                                            long producedPerCycle,
                                            int cycles,
                                            List<String> ledger) throws OffsetDecodingError {
        long logEndOffset = highestPolledAtStart + 1;
        long worstOvershoot = Long.MIN_VALUE;
        int worstDropped = 0;
        HighestOffsetAndIncompletes carriedOverRebalance = null;
        // Seeded with what the cycle-1 fixture itself completes before the loop starts. Without this the harness
        // counts those as never having run, and reports correct skips as data loss - which it did, until the
        // control arm showed a fixed build "losing" records it had in fact processed.
        Set<Long> everActuallyCompleted = completedByFixture(highestPolledAtStart, outstandingAtStart);

        for (int cycle = 1; cycle <= cycles; cycle++) {
            RacingCommitCycleState state;
            int skippedHavingActuallyRun = 0;
            int droppedWithoutEverRunning = 0;

            if (carriedOverRebalance == null) {
                state = polledState(highestPolledAtStart, outstandingAtStart);
            } else {
                logEndOffset += producedPerCycle;
                state = new RacingCommitCycleState(mu.getModule(), tp, carriedOverRebalance);
                for (long offset = state.getOffsetToCommit(); offset < logEndOffset; offset++) {
                    ConsumerRecord<String, String> polled = record(offset);
                    if (state.isRecordPreviouslyCompleted(polled)) {
                        // Two very different things look identical here, so they are counted apart: a record this
                        // consumer really did process in an earlier cycle (correct), and one no cycle ever ran,
                        // dismissed only because a fabricated offsetHighestSucceeded sits above it (silent loss).
                        if (everActuallyCompleted.contains(offset)) {
                            skippedHavingActuallyRun++;
                        } else {
                            droppedWithoutEverRunning++;
                        }
                    } else {
                        state.addNewIncompleteRecord(polled);
                    }
                }
            }

            List<Long> deliverable = new ArrayList<>();
            for (Long outstanding : sorted(state.getAllIncompleteOffsets())) {
                if (outstanding < logEndOffset) {
                    deliverable.add(outstanding);
                }
            }
            if (deliverable.isEmpty()) {
                ledger.add(String.format("K=%d cycle %d: log end offset %d, nothing outstanding that a record could "
                                + "arrive for - the cycle cannot run again",
                        producedPerCycle, cycle, logEndOffset));
                break;
            }

            long racing = deliverable.get(0);
            for (Long done : deliverable) {
                if (done.longValue() != racing) {
                    state.onSuccess(done);
                }
            }
            everActuallyCompleted.addAll(deliverable);
            state.armRaceOn(racing);

            OffsetAndMetadata committed = state.createOffsetAndMetadata();
            assertRaceFired(state, cycle);
            long overshoot = committed.offset() - logEndOffset;
            worstOvershoot = Math.max(worstOvershoot, overshoot);
            worstDropped = Math.max(worstDropped, droppedWithoutEverRunning);

            if (committed.metadata().isEmpty()) {
                ledger.add(String.format("K=%d cycle %d: log end offset %d, committed %d (overshoot %+d), no payload "
                                + "- nothing carries to the next assignment",
                        producedPerCycle, cycle, logEndOffset, committed.offset(), overshoot));
                break;
            }

            carriedOverRebalance = decode(committed);
            ledger.add(String.format("K=%d cycle %d: log end offset %d, raced %d, committed %d (overshoot %+d), "
                            + "%d skipped having actually run / %d DROPPED without ever running, decodes to "
                            + "highest-seen %s incompletes %s",
                    producedPerCycle, cycle, logEndOffset, racing, committed.offset(), overshoot,
                    skippedHavingActuallyRun, droppedWithoutEverRunning,
                    carriedOverRebalance.getHighestSeenOffset().orElse(null),
                    carriedOverRebalance.getIncompleteOffsets()));
        }

        return new SweepResult(worstOvershoot, worstDropped);
    }

    /**
     * What one sweep arm measured. The two travel together because they are the defect's <b>two mutually exclusive
     * regimes</b>: below the payload width the commit overshoots the log end and the run walks off the end of the
     * partition, and at or above it the overshoot stays exactly zero while real records are dismissed against a
     * fabricated high-water mark. Returning only the overshoot would leave the quiet regime measured, printed, and
     * unasserted - green while dropping records, which is the failure this class exists to catch.
     */
    private static final class SweepResult {

        /** How far the worst commit sat above the partition's log end offset. */
        final long worstOvershoot;

        /** The most records any one cycle dismissed that no cycle had ever actually run. */
        final int worstDropped;

        SweepResult(long worstOvershoot, int worstDropped) {
            this.worstOvershoot = worstOvershoot;
            this.worstDropped = worstDropped;
        }
    }

    /**
     * The seam guard. {@code armRaceOn} clears the fired flag, so this asserts the race armed for <em>this</em>
     * cycle actually landed - not that some earlier cycle's did.
     * <p>
     * Without it the whole class degrades silently the moment the encoder stops calling the overridden method,
     * which astubbs#344 changes it to do: the fix under test already produces zero shift and zero overshoot, so
     * every assertion here would still pass against a seam that never fires.
     */
    private static void assertRaceFired(RacingCommitCycleState state, int cycle) {
        assertWithMessage("cycle %s armed a racing completion that never fired - the seam "
                + "RacingCommitCycleState overrides is dead, so this cycle proved nothing. If the encoder stopped "
                + "calling getIncompleteOffsetsBelowHighestSucceeded (astubbs#344), re-hook the double to the "
                + "bounded getIncompleteOffsetsBelow(long) overload rather than deleting this guard.", cycle)
                .that(state.raceHasFired())
                .isTrue();
    }

    /**
     * The second free parameter, and the one that decides whether the overshoot is a single event or a compounding
     * one: the <b>width of the encoded payload</b>, {@code L = offsetHighestSucceeded - lowestIncomplete + 1} at the
     * moment of the commit.
     * <p>
     * {@link #repeatingTheRaceOnAPartitionThatKeepsProducing()} swept the traffic rate K but held the width at 2,
     * because its fixture polls only offsets 0 to 2. At that width the arithmetic allows exactly one growth cycle
     * and no more, so "single event" was a property of the fixture rather than of the defect - the same mistake
     * {@link #theShiftMagnitudeIsAPropertyOfTheFixtureNotAConstant()} caught for the shift magnitude one level up.
     * <p>
     * The committed offset advances by L per cycle and the partition's end by K, so the overshoot grows by
     * {@code L - K}; the run halts once the overshoot reaches K, because the resume position is then past the end of
     * the log. A wide payload with traffic just below it - large K, small {@code L - K} - therefore has room to
     * compound for many cycles before halting. This sweeps a width of {@value #WIDE_GAP_HIGHEST_POLLED} against
     * traffic rates on both sides of it to find out whether it does.
     */
    @Test
    void theOvershootCompoundsWhenThePayloadIsWiderThanTheTraffic() throws OffsetDecodingError {
        List<String> ledger = new ArrayList<>();
        long worstOvershoot = Long.MIN_VALUE;
        int worstDropped = 0;

        for (long producedPerCycle : WIDE_GAP_RECORDS_PRODUCED_PER_CYCLE) {
            SweepResult swept = runGrowingPartition(WIDE_GAP_HIGHEST_POLLED,
                    WIDE_GAP_OUTSTANDING, producedPerCycle, WIDE_GAP_CYCLES, ledger);
            worstOvershoot = Math.max(worstOvershoot, swept.worstOvershoot);
            worstDropped = Math.max(worstDropped, swept.worstDropped);
        }

        String report = String.join("\n  ", ledger);
        log.info("confluentinc#894 wide-payload ledger:\n  {}", report);

        assertBothRegimes(worstOvershoot, worstDropped, report);
    }

    /**
     * Assert the invariant for <b>both</b> regimes of the defect, over one sweep.
     * <p>
     * The loud one is a committed offset above the log end offset - out of range on the next poll, and
     * {@code auto.offset.reset}. The quiet one drops no offset above the end at all: the commit tracks the log end
     * exactly while real records are dismissed against a fabricated {@code offsetHighestSucceeded}. The two never
     * occur together, so asserting only the overshoot leaves every {@code K >= L} arm of the sweep green while it
     * loses records - and those arms are the ones the write-up calls the stronger argument for the fix.
     */
    private static void assertBothRegimes(long worstOvershoot, int worstDropped, String report) {
        // Asserted as ONE pair rather than two statements, so neither regime can hide behind the other. Two
        // sequential assertions short-circuit: the unfixed code overshoots in the K < L arms, so the loud
        // assertion fails first and the quiet one is never evaluated - leaving it unproven, which is the exact
        // shape of the gap this method was added to close. Overshoot is clamped because a commit BELOW the log
        // end is ordinary, while any drop at all is not.
        assertWithMessage("confluentinc#894, both regimes of the defect, neither of which may occur.\n"
                + "  LOUD  - worst overshoot %s: a committed offset above the partition's log end offset is out "
                + "of range on the next poll, and fires auto.offset.reset.\n"
                + "  QUIET - %s record(s) dropped without ever running: dismissed as already-processed against a "
                + "fabricated offsetHighestSucceeded, while the commit tracks the log end exactly so nothing "
                + "overshoots and no error surfaces.\n"
                + "Per-cycle ledger:\n  %s", worstOvershoot, worstDropped, report)
                .that(Arrays.asList(Math.max(worstOvershoot, 0L), (long) worstDropped))
                .isEqualTo(Arrays.asList(0L, 0L));
    }

    /**
     * Offsets 0 to 2 polled, 0 and 2 complete, 1 outstanding. Completing 1 empties the set, so the second read of
     * the offset to commit falls through to {@code offsetHighestSucceeded + 1}. This is the sibling test's fixture,
     * rebuilt here so the two magnitudes are measured side by side in one run.
     */
    private RacingCommitCycleState singleIncompleteState() {
        return polledState(SINGLE_INCOMPLETE_HIGHEST_POLLED, SINGLE_INCOMPLETE_OUTSTANDING);
    }

    /**
     * @return the offsets {@link #singleIncompleteState()} really processes when it is built - derived from the same
     *         two constants the fixture is built from, so the two cannot drift. The growing-partition loop needs
     *         this or it mistakes a correct skip for a dropped record.
     */
    private Set<Long> completedByFixture(long highestPolled, List<Long> outstanding) {
        Set<Long> completed = new TreeSet<>();
        for (long offset = 0; offset <= highestPolled; offset++) {
            if (!outstanding.contains(offset)) {
                completed.add(offset);
            }
        }
        return completed;
    }

    /**
     * Offsets 0 to 3 polled, 0 and 3 complete, 1 and 2 outstanding. Completing 1 leaves 2 outstanding, so the second
     * read returns 2 rather than {@code offsetHighestSucceeded + 1}.
     */
    private RacingCommitCycleState twoIncompleteState() {
        return polledState(HIGHEST_OFFSET_EVER_PRODUCED, Arrays.asList(1L, 2L));
    }

    /**
     * Builds the state by polling and completing records rather than handing it to the constructor, so the starting
     * point is one the running system demonstrably produces. Out-of-order completion is ordinary here - it is what
     * this library is for.
     */
    private RacingCommitCycleState polledState(long highestPolled, List<Long> leaveOutstanding) {
        RacingCommitCycleState state =
                new RacingCommitCycleState(mu.getModule(), tp, HighestOffsetAndIncompletes.of());
        for (long offset = 0; offset <= highestPolled; offset++) {
            state.addNewIncompleteRecord(record(offset));
        }
        for (long offset = 0; offset <= highestPolled; offset++) {
            if (!leaveOutstanding.contains(offset)) {
                state.onSuccess(offset);
            }
        }
        return state;
    }

    /**
     * The rebalance: a new owner decodes the committed data into fresh partition state, then polls. Only offsets the
     * partition actually holds can be delivered - a decoded incomplete above the log end offset names a record that
     * does not exist and simply never arrives.
     */
    private RacingCommitCycleState restoreAndRedeliver(HighestOffsetAndIncompletes restored) {
        RacingCommitCycleState state = new RacingCommitCycleState(mu.getModule(), tp, restored);
        for (Long outstanding : restored.getIncompleteOffsets()) {
            if (outstanding < LOG_END_OFFSET) {
                state.addNewIncompleteRecord(record(outstanding));
            }
        }
        return state;
    }

    /**
     * @return how far the committed offset sits above the base its payload was encoded against - 0 on correct
     *         behaviour, whatever the race produced otherwise
     */
    private long measureShift(RacingCommitCycleState state) {
        long racingOffset = sorted(state.getAllIncompleteOffsets()).get(0);
        state.armRaceOn(racingOffset);

        OffsetAndMetadata committed = state.createOffsetAndMetadata();
        assertRaceFired(state, 1);

        assertWithMessage("precondition: this commit cycle must take the encoding path, not the empty early-return, "
                + "or there is no payload whose base could disagree with the committed offset")
                .that(committed.metadata())
                .isNotEmpty();
        return committed.offset() - state.firstOffsetToCommitRead();
    }

    private ConsumerRecord<String, String> record(long offset) {
        return new ConsumerRecord<>(tp.topic(), tp.partition(), offset, "key", "value");
    }

    private List<Long> sorted(List<Long> offsets) {
        List<Long> copy = new ArrayList<>(offsets);
        Collections.sort(copy);
        return copy;
    }

    private HighestOffsetAndIncompletes decode(OffsetAndMetadata committed) throws OffsetDecodingError {
        return OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromBase64(committed.offset(), committed.metadata());
    }

}
