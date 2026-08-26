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
 * <b>The seam.</b> {@code OffsetMapCodecManager.encodeOffsetsCompressed} snapshots the set it encodes exactly
 * once, through the bounded filter {@link #getIncompleteOffsetsBelow(long)}, and that snapshot is a fresh copy.
 * Completing an offset immediately afterwards therefore cannot alter the payload's contents - only a
 * <em>subsequent</em> read of the offset to commit can see it, which is the defect under test.
 * <p>
 * <b>The override is on the bounded overload, and that is load-bearing.</b> It used to sit on
 * {@link #getIncompleteOffsetsBelowHighestSucceeded()}, which the encoder no longer calls. Overriding the bounded
 * method catches both entry points, because the no-arg convenience method delegates through it in the base class -
 * so this double keeps working whichever one a future caller reaches for. Do not move it back: a seam the encoder
 * does not call is a dead seam, and these tests would then arm races that never fire.
 * <p>
 * <b>What this does NOT establish.</b> The completion cannot move
 * {@code offsetHighestSucceeded} <em>in these fixtures</em>, because every offset they race is the lowest
 * outstanding one and so already sits below it. That is a property of the fixtures, not a general property of the
 * seam.
 * <!-- post-merge: checked-begin -->
 * This paragraph used to continue: that {@code encodeOffsetsCompressed} took a <em>second</em> read of
 * {@code getOffsetHighestSucceeded()} after the snapshot, so a completion above the high-water mark would widen the
 * encoder's range against a stale set, and that these tests never reach that interleaving. That second read is gone -
 * astubbs#344 samples the mark once and derives both the snapshot and the range top from it - and the interleaving
 * it warned about has its own reproduction in {@code OffsetEncoderWidenedRangeRaceTest}. The caveat is kept rather
 * than deleted because it is what a reader of these fixtures still needs: they establish the base/payload tear, not
 * the widened-range one.
 * <!-- post-merge: checked-end -->
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
     * <p>
     * <b>Clears {@link #raceHasFired()}, so the guard is per-arm rather than per-instance.</b> A repeating test
     * arms once per cycle on one state object; a latched flag would stay true after the first cycle fired and
     * report a seam that had since gone dead as healthy - which is the one thing the guard exists to catch.
     */
    void armRaceOn(long offset) {
        this.armedRacingOffset = offset;
        this.raceFired = false;
    }

    /**
     * @return whether the <em>most recent</em> arm actually fired - the guard against a silently dead seam, or a
     *         forgotten arm. Assert it after every commit that armed one, not once at the end of a run.
     */
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
    public SortedSet<Long> getIncompleteOffsetsBelow(long highestSucceededBound) {
        SortedSet<Long> snapshotTheEncoderWillUse = super.getIncompleteOffsetsBelow(highestSucceededBound);
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
