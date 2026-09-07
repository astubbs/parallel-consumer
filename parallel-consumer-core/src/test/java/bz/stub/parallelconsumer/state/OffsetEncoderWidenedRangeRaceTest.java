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

import java.util.List;
import pl.tlinkowski.unij.api.UniLists;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Behavioural reproduction of the widened-range encode race: {@code OffsetMapCodecManager.encodeOffsetsCompressed}
 * snapshots the incomplete offsets (filtered on the high-water mark as it stood at that instant), then takes a
 * <em>second</em> read of {@code getOffsetHighestSucceeded()} for the encoder's range top. A completion landing
 * between the two reads, of an offset <b>above</b> the high-water mark, widens the range against the stale
 * snapshot - and {@code OffsetSimultaneousEncoder} encodes every offset in the range that is absent from the
 * snapshot as <em>completed</em>, including offsets that are still incomplete.
 * <p>
 * <b>Why the window is reachable.</b> In {@code PERIODIC_CONSUMER_SYNC}/{@code ASYNCHRONOUS} commit modes the
 * encode runs on the broker-poll thread ({@code BrokerPollSystem} → {@code ConsumerOffsetCommitter.maybeDoCommit}
 * → {@code AbstractOffsetCommitter.retrieveOffsetsAndCommit}), while completions land from the control thread's
 * mailbox processing ({@code AbstractParallelEoSStreamProcessor} → {@code WorkManager.handleFutureResult} →
 * {@code PartitionState.onSuccess}). In ASYNCHRONOUS mode - the default - the control thread does not wait for the
 * commit, so the two run concurrently with no shared lock.
 * <p>
 * <b>Why the consequence is silent record loss.</b> On restore after a rebalance, the decoded payload sets
 * {@code offsetHighestSucceeded} to the widened range top. The genuinely-incomplete offsets inside the widened
 * stretch are not in the decoded incomplete set, so {@link PartitionState#isRecordPreviouslyCompleted} dismisses
 * them ({@code recOffset <= offsetHighestSucceeded}) - they never run and are never retried, with no error and no
 * lag anomaly.
 * <p>
 * <b>The forced race.</b> {@link RacingEncodeWindowState} fires one completion at the instant the encoder's
 * snapshot has been taken - the same deterministic seam as the confluentinc#894 reproduction
 * ({@code RacingCommitCycleState}, which arrives with astubbs#337), one read later. The racing offset (10) is
 * above the high-water mark (6), the interleaving that reproduction's javadoc explicitly records as unreached
 * there.
 * <p>
 * The racing offset is deliberately <em>not</em> the lowest incomplete, so the commit's base offset
 * ({@code getOffsetToCommit}) is identical before and after the race - this test cannot trip, and is not perturbed
 * by, the sibling confluentinc#894 base-offset defect.
 * <p>
 * <b>What the controls actually discriminate, for anyone mutation-checking this.</b> These tests pin the read
 * <em>ordering</em>, not the presence of two reads. Reinstating the defect order - the incomplete-offsets snapshot
 * taken before the range-top read - goes red; the two reads present but <em>swapped</em> stays green, because that
 * direction is conservative and cannot widen the range. A whole-file revert of the fix does not compile at all
 * (the test double calls the bounded method), so it is a compile failure rather than a behavioural control - do not
 * record it as one, and do not "strengthen" these tests into asserting that two reads exist.
 *
 * @author Antony Stubbs
 */
@Slf4j
class OffsetEncoderWidenedRangeRaceTest {

    ModelUtils mu = new ModelUtils(new PCModuleTestEnv());

    TopicPartition tp = new TopicPartition("topic", 0);

    /** Offsets 0..10 have been polled when the commit cycle starts. */
    static final long HIGHEST_OFFSET_POLLED = 10L;

    /** Completed before the commit cycle: 0-4 and 6 - so the high-water mark is 6 when the encoder snapshots. */
    static final long HIGH_WATER_MARK_AT_SNAPSHOT = 6L;

    /** The offset whose completion lands mid-encode - ABOVE the high-water mark, so it moves it to 10. */
    static final long RACING_OFFSET = 10L;

    /** Still incomplete throughout the whole cycle: 5 (below the mark) and 7, 8, 9 (the widened stretch). */
    static final List<Long> NEVER_COMPLETED_IN_WIDENED_STRETCH = UniLists.of(7L, 8L, 9L);

    /** The base the commit cycle uses: the lowest incomplete, unmoved by the race. */
    static final long EXPECTED_COMMIT_OFFSET = 5L;

    /**
     * The core invariant: <b>an offset that never completed must not decode as completed</b>. Decoded against the
     * committed offset, an offset is claimed completed when it is inside the payload's range and absent from the
     * incomplete set. On the unfixed code the race widens the range to 10 around the stale snapshot {5}, so 7, 8
     * and 9 - incomplete before, during and after the commit - decode as completed.
     */
    @Test
    void offsetsThatNeverCompletedMustNotDecodeAsCompleted() throws OffsetDecodingError {
        RacingEncodeWindowState state = newStateWithRacingCompletion();

        OffsetAndMetadata committed = state.createOffsetAndMetadata();

        assertRaceFiredAndEncodingPathTaken(state, committed);

        HighestOffsetAndIncompletes restored = decode(committed);
        long restoredHighestSeen = restored.getHighestSeenOffset().orElseThrow(IllegalStateException::new);

        assertWithMessage("fixture: offset 5 stayed incomplete and sits below every read of the high-water mark, "
                + "so it must decode as incomplete in both the racy and the clean interleaving")
                .that(restored.getIncompleteOffsets())
                .contains(EXPECTED_COMMIT_OFFSET);

        for (long neverCompleted : NEVER_COMPLETED_IN_WIDENED_STRETCH) {
            boolean decodedAsCompleted = neverCompleted <= restoredHighestSeen
                    && !restored.getIncompleteOffsets().contains(neverCompleted);
            assertWithMessage("offset %s never completed, but the committed payload (range top %s, incompletes %s) "
                            + "decodes it as completed - the encoder's range was widened by a completion that "
                            + "landed after the incomplete-offsets snapshot was taken",
                    neverCompleted, restoredHighestSeen, restored.getIncompleteOffsets())
                    .that(decodedAsCompleted)
                    .isFalse();
        }
    }

    /**
     * The same defect carried through to the user-visible consequence: a new owner restores from the committed
     * data, the genuinely-unprocessed records arrive from the broker, and
     * {@link PartitionState#isRecordPreviouslyCompleted} must not dismiss them - on the unfixed code it does,
     * silently, and they never run.
     */
    @Test
    void afterRestoreTheUnprocessedRecordsMustStillBeRunnable() throws OffsetDecodingError {
        RacingEncodeWindowState state = newStateWithRacingCompletion();

        OffsetAndMetadata committed = state.createOffsetAndMetadata();

        assertRaceFiredAndEncodingPathTaken(state, committed);

        // the rebalance: a new owner rebuilds partition state from what was committed
        PartitionState<String, String> afterRebalance =
                new PartitionState<>(1, mu.getModule(), tp, decode(committed));

        for (long neverCompleted : NEVER_COMPLETED_IN_WIDENED_STRETCH) {
            assertWithMessage("offset %s never ran, but the state restored from the committed data dismisses it as "
                            + "previously completed - it will never be processed and never retried, silently",
                    neverCompleted)
                    .that(afterRebalance.isRecordPreviouslyCompleted(recordAt(neverCompleted)))
                    .isFalse();
        }
    }

    /**
     * Control arm for the fixture: with the race disarmed, the very same fixture must encode faithfully - on the
     * unfixed code too. This pins the failures above on the injected completion, not on the fixture.
     */
    @Test
    void withoutTheRaceTheSameFixtureEncodesFaithfully() throws OffsetDecodingError {
        RacingEncodeWindowState state = newState();
        // deliberately not armed

        OffsetAndMetadata committed = state.createOffsetAndMetadata();

        assertWithMessage("control: no race was armed, so none may fire")
                .that(state.raceHasFired())
                .isFalse();

        assertWithMessage("control: the clean commit must use the lowest incomplete as its base")
                .that(committed.offset())
                .isEqualTo(EXPECTED_COMMIT_OFFSET);

        HighestOffsetAndIncompletes restored = decode(committed);

        assertWithMessage("control: with no mid-encode completion the payload's range top is the high-water mark "
                + "the snapshot was filtered on")
                .that(restored.getHighestSeenOffset().orElseThrow(IllegalStateException::new))
                .isEqualTo(HIGH_WATER_MARK_AT_SNAPSHOT);

        PartitionState<String, String> afterRebalance =
                new PartitionState<>(1, mu.getModule(), tp, restored);
        for (long neverCompleted : NEVER_COMPLETED_IN_WIDENED_STRETCH) {
            assertWithMessage("control: offset %s is above the payload's range, so the restored state must treat "
                            + "it as new work", neverCompleted)
                    .that(afterRebalance.isRecordPreviouslyCompleted(recordAt(neverCompleted)))
                    .isFalse();
        }
    }

    private void assertRaceFiredAndEncodingPathTaken(RacingEncodeWindowState state, OffsetAndMetadata committed) {
        assertWithMessage("precondition: the injected race must actually have fired - without this the test "
                + "passes trivially if the seam silently stops being called, or if the arm was forgotten")
                .that(state.raceHasFired())
                .isTrue();

        assertWithMessage("precondition: the racing completion must actually have ADVANCED the high-water mark "
                        + "(before %s, after %s). Firing alone is not the property under test: an armed offset at or "
                        + "below the mark would still fire and still flip the flag, but both reads would then return "
                        + "the same value and these tests could stay green against the defective ordering",
                state.markBeforeRace(), state.markAfterRace())
                .that(state.markAfterRace())
                .isGreaterThan(state.markBeforeRace());

        assertWithMessage("precondition: the race must move the mark to the racing offset, so the widened range is "
                        + "the one the defect would encode")
                .that(state.markAfterRace())
                .isEqualTo(RACING_OFFSET);

        assertWithMessage("precondition: this commit cycle must take the encoding path, not the empty early-return")
                .that(committed.metadata())
                .isNotEmpty();

        assertWithMessage("precondition: the racing completion is not the lowest incomplete, so the committed "
                + "offset must be unmoved by it - anything else means this fixture strayed into the sibling "
                + "confluentinc#894 base-offset defect")
                .that(committed.offset())
                .isEqualTo(EXPECTED_COMMIT_OFFSET);
    }

    /**
     * Builds the state at the start of the commit cycle by polling and completing records, not by handing the
     * constructor a state object. Offsets 0..10 are polled; 0-4 and 6 complete - ordinary out-of-order completion,
     * the whole point of this library - leaving the high-water mark at 6 and {5, 7, 8, 9, 10} incomplete.
     */
    private RacingEncodeWindowState newState() {
        RacingEncodeWindowState state =
                new RacingEncodeWindowState(mu.getModule(), tp, HighestOffsetAndIncompletes.of());
        for (long offset = 0; offset <= HIGHEST_OFFSET_POLLED; offset++) {
            state.addNewIncompleteRecord(recordAt(offset));
        }
        for (long offset = 0; offset <= HIGH_WATER_MARK_AT_SNAPSHOT; offset++) {
            if (offset != EXPECTED_COMMIT_OFFSET) {
                state.onSuccess(offset);
            }
        }
        return state;
    }

    private RacingEncodeWindowState newStateWithRacingCompletion() {
        RacingEncodeWindowState state = newState();
        // Arm last, so the fixture's own completions above do not consume the one shot.
        state.armRaceOn(RACING_OFFSET);
        return state;
    }

    private ConsumerRecord<String, String> recordAt(long offset) {
        return new ConsumerRecord<>(tp.topic(), tp.partition(), offset, "key", "value");
    }

    private HighestOffsetAndIncompletes decode(OffsetAndMetadata committed) throws OffsetDecodingError {
        HighestOffsetAndIncompletes restored = OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromBase64(
                committed.offset(), committed.metadata());
        log.debug("Committed {} -> restored {}", committed, restored);
        return restored;
    }
}
