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

    private final ModelUtils mu = new ModelUtils(new PCModuleTestEnv());

    private final PartitionState<String, String> state = freshPartition(0);

    /**
     * The acknowledgement of an offer a later one has already passed. Its offset is TRUE - the broker committed up to
     * it - so it is recorded; what it may not do is end the story, because the offsets between it and the newer offer
     * would then have nothing dirty left to re-send them if that newer request failed or was dropped.
     */
    @Test
    void anOlderAcknowledgementAfterAHigherOfferRecordsTheOffsetAndLeavesThePartitionDirty() {
        long olderOffer = completeAndOffer(state, FIRST_RECORD);
        long newerOffer = completeAndOffer(state, SECOND_RECORD);
        assertWithMessage("the second offer must be higher, or this test asserts nothing")
                .that(newerOffer).isGreaterThan(olderOffer);

        state.onOffsetCommitSuccess(new OffsetAndMetadata(olderOffer));

        assertThat(state.getLastCommittedOffset()).isEqualTo(olderOffer);
        assertWithMessage("offer %s is still unanswered, so the partition must stay dirty and be re-committed if "
                + "that request fails or is dropped", newerOffer)
                .that(state.isDirty()).isTrue();
    }

    /**
     * The ordinary case, and the only one the synchronous and transactional modes ever produce: the answer is to the
     * partition's latest offer, so it ends the story.
     */
    @Test
    void anAcknowledgementOfTheOfferedOffsetMarksThePartitionClean() {
        long offered = completeAndOffer(state, FIRST_RECORD);

        state.onOffsetCommitSuccess(new OffsetAndMetadata(offered));

        assertThat(state.getLastCommittedOffset()).isEqualTo(offered);
        assertThat(state.isDirty()).isFalse();
    }

    /**
     * The out-of-order pair: the newest answer arrives first and cleans, and the late lower one must move nothing.
     * The recorded offset is what {@code pc.partition.latest.committed.offset} reads, so walking it backwards would
     * walk the gauge backwards.
     */
    @Test
    void aLowerAcknowledgementAfterAHigherRecordedOffsetMovesNothingBackwards() {
        long olderOffer = completeAndOffer(state, FIRST_RECORD);
        long newerOffer = completeAndOffer(state, SECOND_RECORD);

        state.onOffsetCommitSuccess(new OffsetAndMetadata(newerOffer));
        assertThat(state.isDirty()).isFalse();

        state.onOffsetCommitSuccess(new OffsetAndMetadata(olderOffer));

        assertWithMessage("pc.partition.latest.committed.offset reads this field - a lower answer arriving after a "
                + "higher one must not move it")
                .that(state.getLastCommittedOffset()).isEqualTo(newerOffer);
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

        long movedOnFirstOffer = completeAndOffer(movedOn, FIRST_RECORD);
        long stoodStillOffer = completeAndOffer(stoodStill, FIRST_RECORD);

        // the second commit round: only one partition completed more work, and the other is re-offered unchanged
        // because it is still dirty
        long movedOnSecondOffer = completeAndOffer(movedOn, SECOND_RECORD);
        assertWithMessage("the partition that stood still must be re-offered at the same offset, or this is not the "
                + "mixed case")
                .that(offer(stoodStill)).isEqualTo(stoodStillOffer);
        assertWithMessage("the partition that moved on must be re-offered higher, or this is not the mixed case")
                .that(movedOnSecondOffer).isGreaterThan(movedOnFirstOffer);

        // one answer, to the first round, carrying both partitions
        movedOn.onOffsetCommitSuccess(new OffsetAndMetadata(movedOnFirstOffer));
        stoodStill.onOffsetCommitSuccess(new OffsetAndMetadata(stoodStillOffer));

        assertWithMessage("a newer offer for this partition is still unanswered")
                .that(movedOn.isDirty()).isTrue();
        assertWithMessage("this partition's latest offer is the one just answered, so nothing is outstanding for it "
                + "and it must not wait for a re-commit of an offset the broker has already acknowledged")
                .that(stoodStill.isDirty()).isFalse();
    }

    private PartitionState<String, String> freshPartition(int partition) {
        return new PartitionState<>(0, mu.getModule(), new TopicPartition("acknowledged-commit-offset", partition),
                new HighestOffsetAndIncompletes(Optional.of(SECOND_RECORD),
                        new TreeSet<>(of(FIRST_RECORD, SECOND_RECORD))));
    }

    /**
     * Completes one record and opens a commit window on the partition, which is what a commit cycle does: the offer
     * is what {@code onOffsetCommitSuccess} then measures an acknowledgement against, and
     * {@code getCommitDataIfDirty} is also what clears {@code stateChangedSinceCommitStart} - without it the clean
     * mark would be refused for the unrelated reason that state changed during the commit, and every
     * clean-versus-dirty assertion here would pass whatever the code did.
     *
     * @return the offset offered for commit
     */
    private static long completeAndOffer(PartitionState<String, String> state, long recordOffset) {
        state.onSuccess(recordOffset);
        return offer(state);
    }

    /**
     * @return the offset this partition offers for commit, asserted present so a partition that is unexpectedly clean
     * reports that rather than an empty {@code Optional}
     */
    private static long offer(PartitionState<String, String> state) {
        Optional<OffsetAndMetadata> commitData = state.getCommitDataIfDirty();
        assertWithMessage("the partition must be dirty to offer a commit, or the assertions below are vacuous")
                .that(commitData.isPresent()).isTrue();
        return commitData.get().offset();
    }

}
