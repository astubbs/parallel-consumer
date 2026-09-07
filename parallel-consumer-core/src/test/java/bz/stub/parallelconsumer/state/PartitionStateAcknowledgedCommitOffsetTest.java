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
 * The two things a {@link PartitionState} does with an acknowledged commit, which under
 * {@code PERIODIC_CONSUMER_ASYNCHRONOUS} are decided separately and per partition: the committed offset it RECORDS,
 * and whether it marks the partition CLEAN.
 * <p>
 * Both halves exist because two {@code commitAsync} requests can be in flight at once, and their answers can arrive
 * in either order - {@code ConsumerOffsetCommitterOverlappingAsyncCommitTest} is where the committer's routing
 * decision is asserted. This class is the other side of that seam: what each of the two entry points it routes to
 * actually does, on a real partition state.
 * <p>
 * <b>The recorded offset only ever rises.</b> It is what {@code pc.partition.latest.committed.offset} reads, and an
 * answer carrying an offset a later request has already passed arrives, by definition, after the higher one -
 * assigning it would walk the gauge and the commit watermark backwards. The guard lives here rather than in the
 * committer because this is where the field lives.
 *
 * @author Antony Stubbs
 */
class PartitionStateAcknowledgedCommitOffsetTest {

    private static final long OLDER_OFFSET = 100L;

    private static final long NEWER_OFFSET = 200L;

    private final ModelUtils mu = new ModelUtils(new PCModuleTestEnv());

    private final TopicPartition tp = new TopicPartition("acknowledged-commit-offset", 0);

    private final PartitionState<String, String> state = new PartitionState<>(0, mu.getModule(), tp,
            new HighestOffsetAndIncompletes(Optional.of(NEWER_OFFSET), new TreeSet<>(of(OLDER_OFFSET, NEWER_OFFSET))));

    /**
     * The acknowledgement that a later request has already passed for this partition. Its offset is TRUE - the
     * broker committed up to it - so it is recorded; what it may not do is end the story, because the offsets
     * between it and the newer request would then have nothing dirty left to re-send them.
     */
    @Test
    void aSupersededAcknowledgementRecordsItsOffsetAndLeavesThePartitionDirty() {
        openACommitWindowOnADirtyPartition();

        state.onSupersededOffsetCommitSuccess(new OffsetAndMetadata(OLDER_OFFSET));

        assertThat(state.getLastCommittedOffset()).isEqualTo(OLDER_OFFSET);
        assertWithMessage("the newer request's offsets are still unanswered, so the partition must stay dirty and "
                + "be re-committed if that request fails or is dropped")
                .that(state.isDirty()).isTrue();
    }

    /**
     * The acknowledgement carrying the highest offset in flight, which is the ordinary case and the one the
     * synchronous mode only ever produces: record, and mark clean.
     */
    @Test
    void anAcknowledgementOfTheHighestOffsetInFlightMarksThePartitionClean() {
        openACommitWindowOnADirtyPartition();

        state.onOffsetCommitSuccess(new OffsetAndMetadata(NEWER_OFFSET));

        assertThat(state.getLastCommittedOffset()).isEqualTo(NEWER_OFFSET);
        assertThat(state.isDirty()).isFalse();
    }

    /**
     * Out-of-order answers: the higher offset is recorded first and the late, lower one must apply nothing. Asserted
     * on both entry points, because they share one recording step and a future caller could reach either.
     */
    @Test
    void aLateAcknowledgementCannotWalkTheCommittedOffsetBackwards() {
        state.onOffsetCommitSuccess(new OffsetAndMetadata(NEWER_OFFSET));

        state.onSupersededOffsetCommitSuccess(new OffsetAndMetadata(OLDER_OFFSET));

        assertWithMessage("pc.partition.latest.committed.offset reads this field - a lower answer arriving after a "
                + "higher one must not move it")
                .that(state.getLastCommittedOffset()).isEqualTo(NEWER_OFFSET);

        state.onOffsetCommitSuccess(new OffsetAndMetadata(OLDER_OFFSET));

        assertThat(state.getLastCommittedOffset()).isEqualTo(NEWER_OFFSET);
    }

    /**
     * Puts the partition in the state a commit is answered in: dirty from a completed offset, and with the commit
     * window opened - {@code getCommitDataIfDirty} is what clears {@code stateChangedSinceCommitStart}, so without
     * it the clean mark is refused for the unrelated reason that state changed during the commit, and the
     * clean-versus-dirty assertions here would pass whatever the code did.
     */
    private void openACommitWindowOnADirtyPartition() {
        state.onSuccess(OLDER_OFFSET);
        Optional<OffsetAndMetadata> ignoredCommitData = state.getCommitDataIfDirty();
        assertWithMessage("the partition must be dirty for the commit window to open, or the assertions below are "
                + "vacuous")
                .that(ignoredCommitData.isPresent()).isTrue();
    }

}
