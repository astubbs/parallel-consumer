package bz.stub.parallelconsumer.state;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * A {@link PartitionState} that lands one work completion inside a single commit cycle, at the moment the encoder
 * has finished reading the state it encodes. Shared by the two
 * <a href="https://github.com/confluentinc/parallel-consumer/issues/894">confluentinc#894</a> reproductions.
 * <p>
 * <b>The seam.</b> {@code OffsetMapCodecManager.encodeOffsetsCompressed} calls
 * {@link #getIncompleteOffsetsBelowHighestSucceeded()} exactly once, to snapshot the set it encodes, and that
 * snapshot is a fresh copy. Completing an offset immediately afterwards therefore cannot alter the payload's
 * contents - only a <em>subsequent</em> read of the offset to commit can see it, which is the defect under test.
 * <p>
 * <b>What this does NOT establish, stated because an earlier draft claimed it did.</b> The completion cannot move
 * {@code offsetHighestSucceeded} <em>in these fixtures</em>, because every offset they race is the lowest
 * outstanding one and so already sits below it. That is a property of the fixtures, not a general property of the
 * seam: {@code encodeOffsetsCompressed} takes its own second read of {@code getOffsetHighestSucceeded()} after the
 * snapshot, so a completion ABOVE the current high-water mark would widen the encoder's range against a stale set.
 * These tests do not reach that interleaving, and nothing here should be read as evidence it is safe.
 *
 * @author Antony Stubbs
 */
@Slf4j
class RacingCommitCycleState extends PartitionState<String, String> {

    private final List<Long> offsetToCommitReads = new ArrayList<>();

    /** The offset to complete when the encoder next takes its snapshot, or {@code null} when not armed. */
    private Long armedRacingOffset;

    /**
     * Set when an armed race actually fires. Tracked separately rather than inferred from
     * {@code armedRacingOffset == null}, because that cannot tell "armed, then fired" from "never armed" - a
     * guard built on it passes on a test that forgot to arm, which is the failure it exists to catch.
     */
    private boolean raceFired;

    RacingCommitCycleState(PCModule<String, String> module,
                           TopicPartition topicPartition,
                           HighestOffsetAndIncompletes offsetData) {
        super(0, module, topicPartition, offsetData);
    }

    /**
     * Arm one completion for the next encoder snapshot. Disarms itself after firing, so a single call gives the
     * one-shot behaviour a single-hop test wants, and re-arming each cycle drives a repeating one.
     */
    void armRaceOn(long offset) {
        this.armedRacingOffset = offset;
    }

    /** @return whether a race has actually fired - the guard against a silently dead seam, or a forgotten arm. */
    boolean raceHasFired() {
        return raceFired;
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
            raceFired = true;
            log.debug("Racing completion of offset {} lands after the encoder snapshot {}",
                    racing, snapshotTheEncoderWillUse);
            onSuccess(racing);
        }
        return snapshotTheEncoderWillUse;
    }

    /**
     * @return the read that {@code tryToEncodeOffsets} used as the payload's base - the first on the unfixed code,
     *         which reads twice, and the only one on the fixed code, which reads once and threads it through a
     *         tuple
     */
    long firstOffsetToCommitRead() {
        assertWithMessage("the encode path must have read the offset to commit at least once")
                .that(offsetToCommitReads)
                .isNotEmpty();
        return offsetToCommitReads.get(0);
    }
}
