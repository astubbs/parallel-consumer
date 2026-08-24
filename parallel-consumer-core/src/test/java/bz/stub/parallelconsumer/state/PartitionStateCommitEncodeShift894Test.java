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
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Behavioural reproduction of
 * <a href="https://github.com/confluentinc/parallel-consumer/issues/894">confluentinc#894</a> - "Offset reset when
 * frequent rebalancing" - and of the fix carried as
 * <a href="https://github.com/confluentinc/parallel-consumer/pull/893">confluentinc#893</a>.
 * <p>
 * <b>The defect.</b> On the unfixed code {@link PartitionState#createOffsetAndMetadata()} reads the offset to
 * commit twice:
 * <pre>
 *     Optional&lt;String&gt; payloadOpt = tryToEncodeOffsets();   // internally calls getOffsetToCommit()  &lt;- FIRST read
 *     long nextOffset = getOffsetToCommit();                //                                       &lt;- SECOND read
 * </pre>
 * The incomplete-offsets payload is encoded <em>relative to</em> the first read. The offset actually committed comes
 * from the second. Both reads only happen when there are incompletes to encode - the empty case returns before the
 * first read. If a work item completes in between, the last incomplete disappears, the second read jumps to
 * {@code offsetHighestSucceeded + 1}, and the payload is stored against a base higher than the one it was encoded
 * with. On restore every decoded incomplete - and the decoded highest-seen - shifts up by the difference.
 * <p>
 * <b>Why that ends in {@code auto.offset.reset}.</b> The shifted state names offsets that were never produced. The
 * new owner completes the phantom incomplete, {@code offsetHighestSucceeded} is already above the real log end, and
 * the next commit lands past it. The following poll asks the broker for an offset that does not exist.
 * <p>
 * <b>The forced race.</b> {@link RacingPartitionState} fires one completion from inside
 * {@link PartitionState#getIncompleteOffsetsBelowHighestSucceeded()}, which the encoder calls exactly once (see
 * {@code OffsetMapCodecManager.encodeOffsetsCompressed}) to snapshot its input. The snapshot has already been taken
 * by the time the completion lands, so the payload content is identical with or without the race - only the second
 * read of the commit offset can see it. That is the same window the reporter's production logs show, and it is the
 * only thing this test changes.
 * <p>
 * These offsets are the reporter's 601266890-601266895 walkthrough scaled to 0-4; the shift is the same +2.
 *
 * @author Antony Stubbs
 * @see PartitionStateCommittedOffsetTest#offsetToCommitIsComputedOncePerCommit the shape-only companion, which pins
 *         the call count rather than the resulting state
 */
@Slf4j
class PartitionStateCommitEncodeShift894Test {

    ModelUtils mu = new ModelUtils(new PCModuleTestEnv());

    TopicPartition tp = new TopicPartition("topic", 0);

    /** Offsets 0, 1 and 2 have been polled when the commit cycle starts. */
    static final long HIGHEST_OFFSET_SEEN_AT_COMMIT = 2L;

    /** The one offset still outstanding when the commit cycle starts - and the one that completes mid-cycle. */
    static final long RACING_OFFSET = 1L;

    /**
     * One further record (offset 3) is produced after the rebalance, so the partition's log end offset is 4. A
     * committed offset above this is out of range and triggers {@code auto.offset.reset} on the next poll.
     */
    static final long LOG_END_OFFSET_AFTER_REBALANCE = 4L;

    /**
     * Priority 1, the tight invariant: <b>the offset committed must equal the base the payload was encoded
     * against</b>. The first read is the base handed to {@code makeOffsetMetadataPayload}; on the unfixed code the second read is
     * a different, higher number, and that number is what gets committed alongside the payload.
     */
    @Test
    void committedOffsetMustEqualTheBaseThePayloadWasEncodedAgainst() throws OffsetDecodingError {
        RacingPartitionState state = newStateWithRacingCompletion();

        OffsetAndMetadata committed = state.createOffsetAndMetadata();

        assertWithMessage("precondition: this commit cycle must take the encoding path, not the empty early-return")
                .that(committed.metadata())
                .isNotEmpty();

        long encodeBase = state.firstOffsetToCommitRead();
        assertWithMessage("precondition: the payload must describe the state as it stood when it was encoded - "
                + "decoded against its own base it must yield exactly the offset that was outstanding then, "
                + "which proves the forced completion changed the commit offset and not the payload")
                .that(OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromBase64(encodeBase, committed.metadata())
                        .getIncompleteOffsets())
                .containsExactly(RACING_OFFSET);

        assertWithMessage("confluentinc#894: the committed offset is the decode base for the metadata stored with "
                + "it, so it must be the same number the payload was encoded against. Committed %s against a "
                + "payload encoded at %s - every incomplete in that payload will decode %s too high.",
                committed.offset(), encodeBase, committed.offset() - encodeBase)
                .that(committed.offset())
                .isEqualTo(encodeBase);
    }

    /**
     * The same defect stated without reference to the internals: decoding the committed metadata against the
     * committed offset must not produce state describing offsets that were never produced. Offsets 0-2 exist; a
     * restore that reports 3 as incomplete and 4 as seen is describing a partition that does not exist.
     */
    @Test
    void restoringFromTheCommittedDataMustNotInventOffsetsThatWereNeverProduced() throws OffsetDecodingError {
        RacingPartitionState state = newStateWithRacingCompletion();

        OffsetAndMetadata committed = state.createOffsetAndMetadata();
        HighestOffsetAndIncompletes restored = decode(committed);

        assertWithMessage("confluentinc#894: restored highest-seen %s, but the highest offset ever polled was %s",
                restored.getHighestSeenOffset().orElse(null), HIGHEST_OFFSET_SEEN_AT_COMMIT)
                .that(restored.getHighestSeenOffset().orElseThrow(IllegalStateException::new))
                .isAtMost(HIGHEST_OFFSET_SEEN_AT_COMMIT);

        assertWithMessage("confluentinc#894: restored incompletes %s contain an offset above the highest ever "
                + "polled (%s) - those offsets were never produced", restored.getIncompleteOffsets(),
                HIGHEST_OFFSET_SEEN_AT_COMMIT)
                .that(restored.getIncompleteOffsets().stream().filter(o -> o > HIGHEST_OFFSET_SEEN_AT_COMMIT))
                .isEmpty();
    }

    /**
     * Priority 2 - the shift carried through a rebalance to the reported symptom. The committed data is decoded
     * into a fresh {@link PartitionState} (what a reassignment does), the new owner polls from where that state
     * says to resume, completes what it gets, and commits again. On the unfixed code that second commit is past the
     * partition's log end offset, so the next poll is
     * {@code Fetch position ... is out of range ... resetting offset} - the reporter's step 8.
     */
    @Test
    void theShiftedCommitDrivesTheNextCommitPastTheEndOfThePartition() throws OffsetDecodingError {
        RacingPartitionState state = newStateWithRacingCompletion();
        OffsetAndMetadata committed = state.createOffsetAndMetadata();

        // the rebalance: a new owner rebuilds partition state from what was committed
        PartitionState<String, String> afterRebalance =
                new PartitionState<>(1, mu.getModule(), tp, decode(committed));

        // it resumes where that state tells it to, and successfully processes everything the broker returns
        for (long offset = afterRebalance.getOffsetToCommit(); offset < LOG_END_OFFSET_AFTER_REBALANCE; offset++) {
            afterRebalance.addNewIncompleteRecord(
                    new ConsumerRecord<>(tp.topic(), tp.partition(), offset, "key", "value"));
            afterRebalance.onSuccess(offset);
        }

        long nextCommit = afterRebalance.createOffsetAndMetadata().offset();

        assertWithMessage("confluentinc#894: after the rebalance the next commit is %s, but the partition's log end "
                + "offset is %s - a poll from %s is out of range and fires auto.offset.reset",
                nextCommit, LOG_END_OFFSET_AFTER_REBALANCE, nextCommit)
                .that(nextCommit)
                .isAtMost(LOG_END_OFFSET_AFTER_REBALANCE);
    }

    /**
     * Builds the state at the start of the commit cycle by <em>polling and completing records</em>, not by handing
     * the constructor a state object - so nothing about the starting point can be dismissed as a fixture that the
     * running system would never produce. Offsets 0, 1 and 2 are polled; 0 and 2 complete and 1 does not, which is
     * ordinary out-of-order completion and the whole point of this library.
     */
    private RacingPartitionState newStateWithRacingCompletion() {
        RacingPartitionState state = new RacingPartitionState(mu.getModule(), tp, HighestOffsetAndIncompletes.of());
        for (long offset = 0; offset <= HIGHEST_OFFSET_SEEN_AT_COMMIT; offset++) {
            state.addNewIncompleteRecord(new ConsumerRecord<>(tp.topic(), tp.partition(), offset, "key", "value"));
        }
        for (long offset = 0; offset <= HIGHEST_OFFSET_SEEN_AT_COMMIT; offset++) {
            if (offset != RACING_OFFSET) {
                state.onSuccess(offset);
            }
        }
        return state;
    }

    private HighestOffsetAndIncompletes decode(OffsetAndMetadata committed) throws OffsetDecodingError {
        HighestOffsetAndIncompletes restored =
                OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromBase64(committed.offset(), committed.metadata());
        log.debug("Committed {} -> restored {}", committed, restored);
        return restored;
    }

    /**
     * A {@link PartitionState} that lands exactly one work completion inside a single commit cycle, at the moment
     * the encoder has finished reading the state it encodes.
     * <p>
     * The seam is {@link #getIncompleteOffsetsBelowHighestSucceeded()}: {@code encodeOffsetsCompressed} calls it
     * once, to take the snapshot it encodes, and the returned set is a fresh copy - so completing the offset
     * immediately afterwards cannot alter the payload. It also cannot alter {@code offsetHighestSucceeded}, which
     * already sits at {@value #HIGHEST_OFFSET_SEEN_AT_COMMIT}. The only value the completion can move is a
     * <em>subsequent</em> read of the offset to commit, which is the whole of the defect under test.
     */
    private class RacingPartitionState extends PartitionState<String, String> {

        private final List<Long> offsetToCommitReads = new ArrayList<>();

        private final AtomicBoolean completionFired = new AtomicBoolean(false);

        RacingPartitionState(bz.stub.parallelconsumer.internal.PCModule<String, String> module,
                             TopicPartition topicPartition,
                             HighestOffsetAndIncompletes offsetData) {
            super(0, module, topicPartition, offsetData);
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
            if (completionFired.compareAndSet(false, true)) {
                log.debug("Racing completion of offset {} lands after the encoder snapshot {}",
                        RACING_OFFSET, snapshotTheEncoderWillUse);
                onSuccess(RACING_OFFSET);
            }
            return snapshotTheEncoderWillUse;
        }

        /**
         * @return the value of the read that {@code tryToEncodeOffsets} used as the payload's base - the first read
         *         on both the unfixed code (which reads twice) and the fixed code (which reads once and threads it
         *         through a tuple)
         */
        long firstOffsetToCommitRead() {
            assertWithMessage("the encode path must have read the offset to commit at least once")
                    .that(offsetToCommitReads)
                    .isNotEmpty();
            return offsetToCommitReads.get(0);
        }
    }
}
