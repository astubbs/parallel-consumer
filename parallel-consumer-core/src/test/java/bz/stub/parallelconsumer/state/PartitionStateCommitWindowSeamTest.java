package bz.stub.parallelconsumer.state;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Optional;
import java.util.TreeSet;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Drives the one interleaving the commit window ever lost a completion to, deterministically, and pins the
 * protocol that closes it - {@link PartitionState#completionCount} against
 * {@code completionCountCommitted}, replacing the {@code dirty} / {@code stateChangedSinceCommitStart}
 * pair of booleans (astubbs/parallel-consumer#469).
 *
 * <h2>Why a seam was needed at all</h2>
 *
 * Every <em>other</em> position a completion can take inside a commit window is already reachable through
 * public calls, and the old protocol handled all of them - {@code PartitionStateCommittedOffsetTest}'s
 * {@code workCompletedDuringAsyncCommitShouldKeepStateAsDirty} is the completion landing between collect
 * and commit-success, and it passed before this change. The one position it lost was <em>inside</em> the
 * clean-marking step, between its read of the flag and its write of {@code dirty}: a plain check-then-act,
 * reachable on sequentially consistent hardware, and the old code offered no override point between those
 * two instructions. {@link PartitionState#onCommitWindowClosing()} is that override point.
 *
 * <h2>The two arms, and what each proves</h2>
 *
 * Both drive the identical scenario through the identical seam; the <b>only</b> term that differs is the
 * protocol.
 * <ul>
 *   <li>{@link #aCompletionInsideTheCleanMarkingStepLeavesThePartitionDirty()} - the shipped protocol.
 *       The partition stays dirty, so the completion is committed on the next cycle.</li>
 *   <li>{@link #controlArm_theOldFlagProtocolLosesThatCompletion()} - the control arm: the same seam, the
 *       protocol reverted to the pair of booleans. The partition is marked <b>clean</b> over a completion
 *       the commit did not cover. That is the defect, expressed as an assertion that passes, so it stays
 *       runnable rather than being a claim about a build nobody can re-run.</li>
 * </ul>
 *
 * <h2>What the control arm is and is not</h2>
 *
 * It is a replica of the old protocol, not the old class - nothing binds it to what actually shipped
 * except a human having copied nine lines, exactly the limitation the {@code jcstress-poc/} probes carry
 * and record. Its independent corroboration is
 * {@code CommitWindowLostUpdateProbes.PlainStateChangedFlagAcrossTheCommitWindow}, which measured the same
 * loss on the same shape at 1.6e-3 per raced pair - and the arm beside it,
 * {@code .VolatileStateChangedFlagAcrossTheCommitWindow}, which measured that making the second flag
 * {@code volatile} moves that rate by nothing. Re-run that one before proposing the modifier again.
 *
 * @author Antony Stubbs
 * @see PartitionState#onCommitWindowClosing()
 * @see RacingSeamWorkManager the same one-shot-armed-seam shape, for windows inside {@code WorkManager}
 */
@Slf4j
class PartitionStateCommitWindowSeamTest {

    static final long COMPLETES_BEFORE_THE_COMMIT = 1L;

    static final long COMPLETES_INSIDE_THE_WINDOW = 2L;

    final ModelUtils mu = new ModelUtils(new PCModuleTestEnv());

    final TopicPartition tp = new TopicPartition("topic", 0);

    HighestOffsetAndIncompletes bothIncomplete() {
        return new HighestOffsetAndIncompletes(Optional.of(COMPLETES_INSIDE_THE_WINDOW),
                new TreeSet<>(Arrays.asList(COMPLETES_BEFORE_THE_COMMIT, COMPLETES_INSIDE_THE_WINDOW)));
    }

    /**
     * The shipped protocol. The clean-marking step publishes a count decided at commit start, so a
     * completion landing inside it cannot be marked clean by it.
     */
    @Test
    void aCompletionInsideTheCleanMarkingStepLeavesThePartitionDirty() {
        var state = new SeamedPartitionState(mu, tp, bothIncomplete());

        OffsetAndMetadata committed = driveTheWindow(state);

        assertWithMessage("non-vacuity: the seam must actually have fired, or this test asserts nothing")
                .that(state.seamHasFired())
                .isTrue();
        assertWithMessage("the commit cannot have covered offset %s - it committed next-offset-to-read %s, "
                        + "so if this fails the scenario has stopped exercising the window",
                COMPLETES_INSIDE_THE_WINDOW, committed.offset())
                .that(committed.offset())
                .isLessThan(COMPLETES_INSIDE_THE_WINDOW + 1);
        assertWithMessage("a completion landing inside the clean-marking step must leave the partition dirty - "
                        + "the commit did not cover it, so nothing else will commit it until the next completion, "
                        + "and on a partition that then goes idle the offset waits for the next rebalance")
                .that(state.isDirty())
                .isTrue();
    }

    /**
     * The same window with nothing landing in it: the partition must go clean, or the protocol would simply
     * never mark anything committed and the arm above would pass vacuously.
     */
    @Test
    void aCommitWithNoCompletionInsideItMarksThePartitionClean() {
        var state = new SeamedPartitionState(mu, tp, bothIncomplete());

        state.onSuccess(COMPLETES_BEFORE_THE_COMMIT);
        OffsetAndMetadata committed = state.getCommitDataIfDirty().get();
        state.onOffsetCommitSuccess(committed);

        assertWithMessage("the seam was never armed, so nothing should have fired")
                .that(state.seamHasFired())
                .isFalse();
        assertWithMessage("everything this cycle collected has been committed, so the partition is clean")
                .that(state.isDirty())
                .isFalse();
    }

    /**
     * <b>The control arm - this is the red proof.</b> Identical scenario, identical seam, the protocol
     * reverted to the two booleans this change removes. It asserts the loss, so the defect stays
     * demonstrable after the fix has removed it.
     */
    @Test
    void controlArm_theOldFlagProtocolLosesThatCompletion() {
        var state = new OldFlagProtocolPartitionState(mu, tp, bothIncomplete());

        OffsetAndMetadata committed = driveTheWindow(state);

        assertWithMessage("non-vacuity: the seam must actually have fired")
                .that(state.seamHasFired())
                .isTrue();
        assertWithMessage("the commit did not cover offset %s", COMPLETES_INSIDE_THE_WINDOW)
                .that(committed.offset())
                .isLessThan(COMPLETES_INSIDE_THE_WINDOW + 1);
        assertWithMessage("the old check-then-act reads the flag, the completion sets it, and the write "
                        + "lands anyway - so the partition is marked clean over a completion nobody committed. "
                        + "If THIS assertion fails, the replica has stopped modelling the old protocol; fix the "
                        + "replica rather than deleting the arm, because it is the only executable record of "
                        + "what the shipped protocol is for")
                .that(state.isDirty())
                .isFalse();
    }

    /**
     * One completion, a whole commit cycle, and a second completion landing at the seam inside the
     * clean-marking step.
     *
     * @return the offsets the cycle actually committed
     */
    private OffsetAndMetadata driveTheWindow(SeamedPartitionState state) {
        state.onSuccess(COMPLETES_BEFORE_THE_COMMIT);

        OffsetAndMetadata committed = state.getCommitDataIfDirty().get();

        state.armCommitWindowSeam(() -> state.onSuccess(COMPLETES_INSIDE_THE_WINDOW));
        state.onOffsetCommitSuccess(committed);

        return committed;
    }

    /**
     * A {@link PartitionState} that runs another thread's action at one exact instruction inside the
     * clean-marking step, so the window is driven rather than raced for. One shot, and firing is tracked
     * separately from arming so a test that forgot to arm cannot pass while asserting nothing - the same
     * reasoning as {@link RacingSeamWorkManager}, which owns the pattern for {@code WorkManager} seams.
     */
    static class SeamedPartitionState extends PartitionState<String, String> {

        private Runnable interference = () -> {
            // not armed
        };

        private boolean armed;

        private boolean fired;

        SeamedPartitionState(ModelUtils mu, TopicPartition tp, HighestOffsetAndIncompletes offsetData) {
            super(0, mu.getModule(), tp, offsetData);
        }

        void armCommitWindowSeam(Runnable interference) {
            this.interference = interference;
            this.armed = true;
        }

        boolean seamHasFired() {
            return fired;
        }

        @Override
        protected void onCommitWindowClosing() {
            if (armed) {
                armed = false;
                fired = true;
                interference.run();
            }
        }
    }

    /**
     * The protocol as it stood before astubbs/parallel-consumer#469, over the same seam: a {@code dirty}
     * flag cleared by a check-then-act, and a second flag cleared at commit start by the committer thread
     * and set by the completing thread.
     * <p>
     * Only the protocol is replaced - the real {@link PartitionState#onSuccess(long)} bookkeeping and the
     * real {@link PartitionState#createOffsetAndMetadata()} still run, so the offsets this arm commits are
     * the ones production would commit.
     */
    static class OldFlagProtocolPartitionState extends SeamedPartitionState {

        private boolean dirty;

        private boolean stateChangedSinceCommitStart;

        OldFlagProtocolPartitionState(ModelUtils mu, TopicPartition tp, HighestOffsetAndIncompletes offsetData) {
            super(mu, tp, offsetData);
        }

        @Override
        public void onSuccess(long offset) {
            super.onSuccess(offset);
            stateChangedSinceCommitStart = true;
            dirty = true;
        }

        @Override
        public Optional<OffsetAndMetadata> getCommitDataIfDirty() {
            if (dirty) {
                stateChangedSinceCommitStart = false;
                return Optional.of(createOffsetAndMetadata());
            }
            return Optional.empty();
        }

        @Override
        public void onOffsetCommitSuccess(OffsetAndMetadata committed) {
            // the old setClean(), with its check and its act separated by the seam
            boolean changedDuringTheWindow = stateChangedSinceCommitStart;
            onCommitWindowClosing();
            if (!changedDuringTheWindow) {
                dirty = false;
            }
        }

        @Override
        boolean isDirty() {
            return dirty;
        }
    }
}
