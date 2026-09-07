package bz.stub.parallelconsumer.state;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.util.SortedSet;

/**
 * A {@link PartitionState} that lands one work completion inside a single commit cycle, at the moment the encoder
 * has taken its snapshot of the incomplete offsets - the same seam as {@code RacingCommitCycleState} (the
 * confluentinc#894 reproduction, which arrives with astubbs#337), one read later.
 * <p>
 * <b>The seam.</b> {@code OffsetMapCodecManager.encodeOffsetsCompressed} snapshots the incomplete offsets through
 * the bounded filter {@link #getIncompleteOffsetsBelow(long)} (reached via
 * {@link #getIncompleteOffsetsBelowHighestSucceeded()} on the unfixed shape). This double fires one
 * {@link #onSuccess(long)} the moment that snapshot has been taken, before it is returned. Unlike the
 * confluentinc#894 double, the racing offset here is <em>above</em> the current high-water mark, so the completion
 * moves {@code offsetHighestSucceeded} itself - on the unfixed code, which re-read the mark after the snapshot for
 * the encoder's range top, the range widens past the bound the snapshot was filtered on, and every incomplete
 * offset inside the widened stretch is encoded as completed. On the fixed code the mark is sampled once, before
 * the snapshot, so the completion this double fires can no longer reach the encoder's range.
 * <p>
 * The firing state is tracked in an explicit {@code raceFired} boolean rather than inferred from the armed slot
 * being emptied, because a nulled slot cannot tell "armed, then fired" from "never armed" - a guard built on it
 * passes on a test that forgot to arm, which is exactly the failure it exists to catch.
 * <p>
 * <b>Firing is necessary but not sufficient, so the mark either side of the completion is recorded too.</b>
 * {@code raceFired} alone proves the callback was <em>attempted</em>, not that the widened-range race was
 * actually injected: if fixture drift ever left the armed offset at or below the current high-water mark,
 * {@link #onSuccess(long)} would still run and still set the flag, but the mark would not move - both reads
 * would then return the same value and the behavioural tests could stay green against the defective ordering.
 * {@link #markBeforeRace()} and {@link #markAfterRace()} let a caller assert the mark genuinely advanced, which
 * is the property the reproduction actually depends on.
 *
 * @author Antony Stubbs
 */
@Slf4j
class RacingEncodeWindowState extends PartitionState<String, String> {

    /** The offset to complete when the encoder next takes its snapshot, or {@code null} when not armed. */
    private Long armedRacingOffset;

    /** Set when an armed race actually fires - see class javadoc for why this is tracked explicitly. */
    private boolean raceFired;

    /** The high-water mark immediately before the racing completion, or {@code -1} if no race has fired. */
    private long markBeforeRace = -1;

    /** The high-water mark immediately after the racing completion, or {@code -1} if no race has fired. */
    private long markAfterRace = -1;

    RacingEncodeWindowState(PCModule<String, String> module,
                            TopicPartition topicPartition,
                            HighestOffsetAndIncompletes offsetData) {
        super(0, module, topicPartition, offsetData);
    }

    /** Arm one completion for the next encoder snapshot. One-shot: disarms itself after firing. */
    void armRaceOn(long offset) {
        this.armedRacingOffset = offset;
    }

    /** @return whether a race has actually fired - the guard against a silently dead seam, or a forgotten arm. */
    boolean raceHasFired() {
        return raceFired;
    }

    /** @return the high-water mark sampled immediately before the racing completion, or {@code -1} if none fired. */
    long markBeforeRace() {
        return markBeforeRace;
    }

    /** @return the high-water mark sampled immediately after the racing completion, or {@code -1} if none fired. */
    long markAfterRace() {
        return markAfterRace;
    }

    @Override
    public SortedSet<Long> getIncompleteOffsetsBelow(long highestSucceededBound) {
        SortedSet<Long> snapshotTheEncoderWillUse = super.getIncompleteOffsetsBelow(highestSucceededBound);
        fireIfArmed(snapshotTheEncoderWillUse);
        return snapshotTheEncoderWillUse;
    }

    private void fireIfArmed(SortedSet<Long> snapshotTheEncoderWillUse) {
        if (armedRacingOffset != null) {
            long racing = armedRacingOffset;
            armedRacingOffset = null;
            raceFired = true;
            markBeforeRace = getOffsetHighestSucceeded();
            log.debug("Racing completion of offset {} lands after the encoder snapshot {}",
                    racing, snapshotTheEncoderWillUse);
            onSuccess(racing);
            markAfterRace = getOffsetHighestSucceeded();
        }
    }
}
