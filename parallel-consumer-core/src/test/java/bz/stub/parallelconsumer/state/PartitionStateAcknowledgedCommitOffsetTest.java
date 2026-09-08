package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.TreeSet;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * The rule that decides what an acknowledged commit does to a {@link PartitionState}: <b>record the offset always,
 * mark the partition clean only when the offset acknowledged is the one this partition last OFFERED for commit.</b>
 * <p>
 * The rule lives here because the offer does. {@code getCommitDataIfDirty} is what hands an offset to the committer,
 * so the partition is the thing that can recognise the answer to its own latest offer - and
 * {@code ConsumerOffsetCommitter} therefore keeps no record of what it has in flight. Under
 * {@code PERIODIC_CONSUMER_ASYNCHRONOUS} two commits can be in flight at once and their answers can arrive in either
 * order, which is the only reason any of this is needed; the committer's end of the seam is
 * {@code ConsumerOffsetCommitterOverlappingAsyncCommitTest}, which drives the same three orderings through a real
 * {@code WorkManager}.
 * <p>
 * <b>Every test reads the offer back rather than naming an offset.</b> The offered offset is
 * {@code getOffsetHighestSequentialSucceeded() + 1}, derived from the incomplete set - so a test that hard-coded it
 * would pass against a partition that offered something else entirely, which is the shape of assertion the rule is
 * most easily broken under.
 *
 * @author Antony Stubbs
 */
class PartitionStateAcknowledgedCommitOffsetTest {

    private static final long FIRST_RECORD = 100L;

    private static final long SECOND_RECORD = 200L;

    /**
     * Never completed by the tests that only need two offers. It exists so
     * {@link #anAcknowledgementOfAnOfferWithSupersededMetadataAtTheSameOffsetDoesNotCleanThePartition()} has a record
     * ABOVE the lowest incomplete one to complete - the only way to change an offer's metadata without moving its
     * offset.
     */
    private static final long THIRD_RECORD = 300L;

    private final ModelUtils mu = new ModelUtils(new PCModuleTestEnv());

    private final PartitionState<String, String> state = freshPartition(0);

    /**
     * The acknowledgement of an offer a later one has already passed. Its offset is TRUE - the broker committed up to
     * it - so it is recorded; what it may not do is end the story, because the offsets between it and the newer offer
     * would then have nothing dirty left to re-send them if that newer request failed or was dropped.
     */
    @Test
    void anOlderAcknowledgementAfterAHigherOfferRecordsTheOffsetAndLeavesThePartitionDirty() {
        OffsetAndMetadata olderOffer = completeAndOffer(state, FIRST_RECORD);
        OffsetAndMetadata newerOffer = completeAndOffer(state, SECOND_RECORD);
        assertWithMessage("the second offer must be higher, or this test asserts nothing")
                .that(newerOffer.offset()).isGreaterThan(olderOffer.offset());

        state.onOffsetCommitSuccess(olderOffer);

        assertThat(state.getLastCommittedOffset()).isEqualTo(olderOffer.offset());
        assertWithMessage("offer %s is still unanswered, so the partition must stay dirty and be re-committed if "
                + "that request fails or is dropped", newerOffer.offset())
                .that(state.isDirty()).isTrue();
    }

    /**
     * The ordinary case, and the only one the synchronous and transactional modes ever produce: the answer is to the
     * partition's latest offer, so it ends the story.
     */
    @Test
    void anAcknowledgementOfTheOfferedOffsetMarksThePartitionClean() {
        OffsetAndMetadata offered = completeAndOffer(state, FIRST_RECORD);

        state.onOffsetCommitSuccess(offered);

        assertThat(state.getLastCommittedOffset()).isEqualTo(offered.offset());
        assertThat(state.isDirty()).isFalse();
    }

    /**
     * The out-of-order pair: the newest answer arrives first and cleans, and the late lower one must move nothing.
     * The recorded offset is what {@code pc.partition.latest.committed.offset} reads, so walking it backwards would
     * walk the gauge backwards.
     */
    @Test
    void aLowerAcknowledgementAfterAHigherRecordedOffsetMovesNothingBackwards() {
        OffsetAndMetadata olderOffer = completeAndOffer(state, FIRST_RECORD);
        OffsetAndMetadata newerOffer = completeAndOffer(state, SECOND_RECORD);

        state.onOffsetCommitSuccess(newerOffer);
        assertThat(state.isDirty()).isFalse();

        state.onOffsetCommitSuccess(olderOffer);

        assertWithMessage("pc.partition.latest.committed.offset reads this field - a lower answer arriving after a "
                + "higher one must not move it")
                .that(state.getLastCommittedOffset()).isEqualTo(newerOffer.offset());
        assertWithMessage("the late answer is not the answer to the latest offer, so it cannot clean - but there is "
                + "nothing left to clean either, and it must not make the partition dirty again")
                .that(state.isDirty()).isFalse();
    }

    /**
     * The case the previous design carried a per-partition map in the committer to reach, and which now falls out of
     * the rule with no machinery at all: one commit request carries every dirty partition, so it is routinely the
     * newest word on one and superseded on another.
     * <p>
     * Here the second round raises partition 0's offer and re-offers partition 1 at exactly the same offset, because
     * nothing new completed there. The answer to the FIRST round therefore cleans partition 1 - it is that
     * partition's latest offer - and leaves partition 0 dirty. Each partition decides for itself, because each one
     * made its own offer.
     */
    @Test
    void oneAnswerCleansThePartitionItIsTheLatestOfferForAndLeavesTheOtherDirty() {
        PartitionState<String, String> movedOn = state;
        PartitionState<String, String> stoodStill = freshPartition(1);

        OffsetAndMetadata movedOnFirstOffer = completeAndOffer(movedOn, FIRST_RECORD);
        OffsetAndMetadata stoodStillOffer = completeAndOffer(stoodStill, FIRST_RECORD);

        // the second commit round: only one partition completed more work, and the other is re-offered unchanged
        // because it is still dirty
        OffsetAndMetadata movedOnSecondOffer = completeAndOffer(movedOn, SECOND_RECORD);
        assertWithMessage("the partition that stood still must be re-offered identically - same offset AND same "
                + "metadata - or this is not the mixed case")
                .that(offer(stoodStill)).isEqualTo(stoodStillOffer);
        assertWithMessage("the partition that moved on must be re-offered higher, or this is not the mixed case")
                .that(movedOnSecondOffer.offset()).isGreaterThan(movedOnFirstOffer.offset());

        // one answer, to the first round, carrying both partitions
        movedOn.onOffsetCommitSuccess(movedOnFirstOffer);
        stoodStill.onOffsetCommitSuccess(stoodStillOffer);

        assertWithMessage("a newer offer for this partition is still unanswered")
                .that(movedOn.isDirty()).isTrue();
        assertWithMessage("this partition's latest offer is the one just answered, so nothing is outstanding for it "
                + "and it must not wait for a re-commit of an offset the broker has already acknowledged")
                .that(stoodStill.isDirty()).isFalse();
    }

    /**
     * The case an offset-only comparison cannot see, and the reason the offer is remembered whole.
     * <p>
     * The offered offset is {@code getOffsetHighestSequentialSucceeded() + 1}, so completing a record ABOVE the
     * lowest incomplete one leaves that offset exactly where it was while changing the encoded incomplete set that
     * rides with it as metadata. Two requests are then in flight carrying the SAME offset and DIFFERENT metadata.
     * Comparing offsets alone, the answer to the older one matches and cleans the partition - and the newer
     * request's metadata, the only record that the higher record is done, is never re-sent if that request then
     * fails or is dropped. Those records are re-delivered after a reassignment: not lost offsets, but precisely the
     * replay the encoded offset map exists to prevent.
     * <p>
     * Found by the Codex review on astubbs/parallel-consumer#470.
     */
    @Test
    void anAcknowledgementOfAnOfferWithSupersededMetadataAtTheSameOffsetDoesNotCleanThePartition() {
        state.onSuccess(THIRD_RECORD);
        OffsetAndMetadata olderOffer = offer(state);

        // completing a record above the lowest incomplete one cannot move the offered offset, only the metadata
        state.onSuccess(SECOND_RECORD);
        OffsetAndMetadata newerOffer = offer(state);

        assertWithMessage("the two offers must carry the same offset, or this test is the ordinary superseded case "
                + "and asserts nothing new")
                .that(newerOffer.offset()).isEqualTo(olderOffer.offset());
        assertWithMessage("the two offers must carry different metadata, or there is nothing for an offset-only "
                + "comparison to miss and this test asserts nothing")
                .that(newerOffer.metadata()).isNotEqualTo(olderOffer.metadata());

        state.onOffsetCommitSuccess(olderOffer);

        assertWithMessage("the newer offer is still unanswered and is the only thing carrying the completion of "
                + "record %s - cleaning here would leave nothing dirty to re-send it", THIRD_RECORD)
                .that(state.isDirty()).isTrue();
    }

    private PartitionState<String, String> freshPartition(int partition) {
        return new PartitionState<>(0, mu.getModule(), new TopicPartition("acknowledged-commit-offset", partition),
                new HighestOffsetAndIncompletes(Optional.of(THIRD_RECORD),
                        new TreeSet<>(of(FIRST_RECORD, SECOND_RECORD, THIRD_RECORD))));
    }

    /**
     * Completes one record and opens a commit window on the partition, which is what a commit cycle does: the offer
     * is what {@code onOffsetCommitSuccess} then measures an acknowledgement against, and
     * {@code getCommitDataIfDirty} is also what clears {@code stateChangedSinceCommitStart} - without it the clean
     * mark would be refused for the unrelated reason that state changed during the commit, and every
     * clean-versus-dirty assertion here would pass whatever the code did.
     *
     * @return the offer made for commit, WHOLE - offset and encoded metadata together
     */
    private static OffsetAndMetadata completeAndOffer(PartitionState<String, String> state, long recordOffset) {
        state.onSuccess(recordOffset);
        return offer(state);
    }

    /**
     * Returns the offer whole, and every test acknowledges it by handing that same object back. That is what the
     * real callback does - {@code Consumer#commitAsync} passes the very map it was given to the
     * {@code OffsetCommitCallback}, and the transactional path calls {@code onOffsetCommitSuccess} inline with the
     * map it just collected - so reconstructing an {@code OffsetAndMetadata} from the offset alone would test a
     * request shape that never occurs, and would hide any mismatch in the metadata half of the offer.
     *
     * @return the offer this partition makes for commit, asserted present so a partition that is unexpectedly clean
     * reports that rather than an empty {@code Optional}
     */
    private static OffsetAndMetadata offer(PartitionState<String, String> state) {
        Optional<OffsetAndMetadata> commitData = state.getCommitDataIfDirty();
        assertWithMessage("the partition must be dirty to offer a commit, or the assertions below are vacuous")
                .that(commitData.isPresent()).isTrue();
        return commitData.get();
    }

}
