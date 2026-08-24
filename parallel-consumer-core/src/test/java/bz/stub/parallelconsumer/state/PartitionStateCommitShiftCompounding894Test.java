package bz.stub.parallelconsumer.state;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.PCModule;
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
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicLong;

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
        AtomicLong worstDecodedHighestSeen = new AtomicLong(Long.MIN_VALUE);

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
            long encodeBase = state.firstOffsetToCommitRead();
            carriedOverRebalance = decode(committed);

            worstDecodedHighestSeen.set(Math.max(worstDecodedHighestSeen.get(),
                    carriedOverRebalance.getHighestSeenOffset().orElse(Long.MIN_VALUE)));
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
                .that(worstDecodedHighestSeen.get())
                .isAtMost(HIGHEST_OFFSET_EVER_PRODUCED);
    }

    /**
     * Offsets 0 to 2 polled, 0 and 2 complete, 1 outstanding. Completing 1 empties the set, so the second read of
     * the offset to commit falls through to {@code offsetHighestSucceeded + 1}. This is the sibling test's fixture,
     * rebuilt here so the two magnitudes are measured side by side in one run.
     */
    private RacingCommitCycleState singleIncompleteState() {
        return polledState(2L, Collections.singletonList(1L));
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

    /**
     * A {@link PartitionState} that lands one work completion inside a commit cycle, at the moment the encoder has
     * finished reading the state it encodes, and can be re-armed for the next cycle.
     * <p>
     * Same seam and same reasoning as the sibling test's racing state - {@code encodeOffsetsCompressed} calls
     * {@link #getIncompleteOffsetsBelowHighestSucceeded()} exactly once to snapshot its input, and the snapshot is a
     * fresh copy, so a completion landing immediately after it can change neither the payload nor
     * {@code offsetHighestSucceeded}. It is a separate class rather than a shared helper because it carries state
     * the sibling does not need: an offset that varies per cycle and a re-arm, both required to run the cycle more
     * than once. Merging the two would push per-cycle machinery into a test that has no cycles.
     */
    private static class RacingCommitCycleState extends PartitionState<String, String> {

        private final List<Long> offsetToCommitReads = new ArrayList<>();

        /** The offset to complete when the encoder next takes its snapshot, or {@code null} when not armed. */
        private Long armedRacingOffset;

        RacingCommitCycleState(PCModule<String, String> module,
                               TopicPartition topicPartition,
                               HighestOffsetAndIncompletes offsetData) {
            super(0, module, topicPartition, offsetData);
        }

        void armRaceOn(long offset) {
            this.armedRacingOffset = offset;
        }

        @Override
        protected long getOffsetToCommit() {
            long read = super.getOffsetToCommit();
            offsetToCommitReads.add(read);
            return read;
        }

        @Override
        public SortedSet<Long> getIncompleteOffsetsBelowHighestSucceeded() {
            SortedSet<Long> snapshotTheEncoderWillUse = super.getIncompleteOffsetsBelowHighestSucceeded();
            if (armedRacingOffset != null) {
                long racing = armedRacingOffset;
                armedRacingOffset = null;
                log.debug("Racing completion of offset {} lands after the encoder snapshot {}",
                        racing, snapshotTheEncoderWillUse);
                onSuccess(racing);
            }
            return snapshotTheEncoderWillUse;
        }

        /**
         * @return the read that {@code tryToEncodeOffsets} used as the payload's base - the first on the unfixed
         *         code, which reads twice, and the only one on the fixed code, which reads once and threads it
         *         through a tuple
         */
        long firstOffsetToCommitRead() {
            assertWithMessage("the encode path must have read the offset to commit at least once")
                    .that(offsetToCommitReads)
                    .isNotEmpty();
            return offsetToCommitReads.get(0);
        }
    }
}
