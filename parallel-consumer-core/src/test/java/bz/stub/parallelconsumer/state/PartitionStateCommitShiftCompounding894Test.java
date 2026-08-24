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
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
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

        for (long producedPerCycle : RECORDS_PRODUCED_PER_CYCLE) {
            worstOvershoot = Math.max(worstOvershoot, runGrowingPartition(producedPerCycle, ledger));
        }

        String report = String.join("\n  ", ledger);
        log.info("confluentinc#894 growing-partition ledger:\n  {}", report);

        assertWithMessage("confluentinc#894: a committed offset above the partition's log end offset is out of range "
                + "on the next poll, and fires auto.offset.reset. Worst overshoot across the sweep was %s. "
                + "Per-cycle ledger:\n  %s", worstOvershoot, report)
                .that(worstOvershoot)
                .isAtMost(0L);
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
    private long runGrowingPartition(long producedPerCycle, List<String> ledger) throws OffsetDecodingError {
        long logEndOffset = SINGLE_INCOMPLETE_LOG_END_OFFSET;
        long worstOvershoot = Long.MIN_VALUE;
        HighestOffsetAndIncompletes carriedOverRebalance = null;
        // Seeded with what the cycle-1 fixture itself completes before the loop starts. Without this the harness
        // counts those as never having run, and reports correct skips as data loss - which it did, until the
        // control arm showed a fixed build "losing" records it had in fact processed.
        Set<Long> everActuallyCompleted = completedBySingleIncompleteFixture();

        for (int cycle = 1; cycle <= CYCLES; cycle++) {
            RacingCommitCycleState state;
            int skippedHavingActuallyRun = 0;
            int droppedWithoutEverRunning = 0;

            if (carriedOverRebalance == null) {
                state = singleIncompleteState();
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
            long overshoot = committed.offset() - logEndOffset;
            worstOvershoot = Math.max(worstOvershoot, overshoot);

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

        return worstOvershoot;
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
    private Set<Long> completedBySingleIncompleteFixture() {
        Set<Long> completed = new TreeSet<>();
        for (long offset = 0; offset <= SINGLE_INCOMPLETE_HIGHEST_POLLED; offset++) {
            if (!SINGLE_INCOMPLETE_OUTSTANDING.contains(offset)) {
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
