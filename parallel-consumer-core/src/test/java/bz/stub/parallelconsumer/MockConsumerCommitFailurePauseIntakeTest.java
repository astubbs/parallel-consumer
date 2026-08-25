package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitFailureContinueMode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * The {@link CommitFailureContinueMode#PAUSE_INTAKE} half of a CONTINUE decision (astubbs#317,
 * confluentinc#833): while commits are failing, in-flight work still completes but no NEW work is drawn, and the
 * pause releases on the next successful commit with no user action.
 * <p>
 * Beside the pause itself, the scenarios pin what it composes with: the {@link
 * CommitFailureContinueMode#KEEP_PROCESSING} control arm that gates nothing, both directions of composition with
 * the user's own {@code pauseIfRunning()}/{@code resumeIfPaused()} (neither axis may clear the other), and the
 * close path winning over an active pause during DRAINING.
 * <p>
 * The fixture - the failing {@link MockConsumer}, the recording handler, the waits - is
 * {@link MockConsumerCommitFailureSeamTestBase}, which also names the other slices of the seam.
 *
 * @author Antony Stubbs
 * @see CommitFailureHandler
 */
class MockConsumerCommitFailurePauseIntakeTest extends MockConsumerCommitFailureSeamTestBase {

    /**
     * The {@link CommitFailureContinueMode#PAUSE_INTAKE} half of a CONTINUE decision: while
     * commits are failing, in-flight work still completes, but no NEW work is drawn - and the pause releases on the
     * next successful commit, without any user action.
     * <p>
     * The gated records are asserted to have REACHED the work manager ({@code workRemaining}) before asserting they
     * are not processed: the seam's pause gates work distribution, not broker polling, so the records must be aboard
     * and waiting - otherwise "not processed" would also pass for a poller that simply stopped.
     * <p>
     * Structure shared with the other PAUSE_INTAKE scenarios: the commit path starts HEALTHY, the opening batch is
     * processed and cleanly committed, and only then do commits break. The first exhaustion (and so the pause)
     * otherwise lands mid-batch - the first commit fires as soon as the first record completes - and gates the
     * remainder of the batch, making every subsequent count non-deterministic.
     */
    @Test
    void pauseIntakeStopsNewWorkCompletesInFlightAndResumesAfterCommitSuccess() throws InterruptedException {
        var commitsHealthy = new AtomicBoolean(true);
        useCommitsTimingOut(commitsHealthy);
        var handler = continuingHandler();
        startPc(SMALL_BUDGET, handler, CommitFailureContinueMode.PAUSE_INTAKE);

        // the in-flight probe is a record on its OWN key - a parallel lane beside the serial single-key lane, so
        // it can sit mid-flight while later single-key records complete around it
        var inFlightEntered = new CountDownLatch(1);
        var inFlightHold = new CountDownLatch(1);
        final long heldOffset = RECORDS; // offset 5, key "held-key"
        addRecords(0, RECORDS);
        startProcessingHoldingAt(heldOffset, inFlightEntered, inFlightHold);
        awaitCommittedOffset(RECORDS); // the opening batch is processed and cleanly committed - nothing is dirty

        // the held record enters processing while the commit path is still healthy - guaranteed in-flight from
        // here on, whatever the commit timing
        mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, TOPIC_PARTITION.partition(), heldOffset, "held-key",
                "value-held"));
        assertWithMessage("the in-flight probe record never started processing")
                .that(inFlightEntered.await(30, SECONDS)).isTrue();

        // break commits, then complete one ordinary record: the partition turns dirty, the next cadence's commit
        // exhausts its budget, and CONTINUE under PAUSE_INTAKE engages the seam pause
        commitsHealthy.set(false);
        addRecords(heldOffset + 1, 1); // offset 6, the dirty-driver
        awaitAsserted(() -> assertThat(handler.contexts).isNotEmpty());

        // in-flight work is NOT gated: the held record completes while commits are still failing
        inFlightHold.countDown();
        awaitAsserted(() -> assertThat(processedRecords).hasSize(RECORDS + 2));

        // new work IS gated: these records arrive in the work manager but are never drawn
        addRecords(heldOffset + 2, 3); // offsets 7..9
        awaitAsserted(() -> assertThat(parallelConsumer.workRemaining()).isEqualTo(3));
        int exhaustionsBeforeProbe = handler.contexts.size();
        // a further exhaustion is the positive signal that full control-loop cycles passed with the pause active
        awaitAsserted(() -> assertThat(handler.contexts.size()).isAtLeast(exhaustionsBeforeProbe + 1));
        assertWithMessage("no NEW work may be drawn while the seam pause is active")
                .that(processedRecords).hasSize(RECORDS + 2);

        // the next successful commit releases the pause: intake resumes with no user action
        commitsHealthy.set(true);
        awaitAsserted(() -> assertThat(processedRecords).hasSize(RECORDS + 5));
        awaitCommittedOffset(RECORDS + 5);
        assertThat(parallelConsumer.isClosedOrFailed()).isFalse();
        assertThat(parallelConsumer.getFailureCause()).isNull();
    }

    /**
     * The control arm for {@link #pauseIntakeStopsNewWorkCompletesInFlightAndResumesAfterCommitSuccess}: under the
     * default {@link CommitFailureContinueMode#KEEP_PROCESSING}, a CONTINUE decision gates nothing - new work keeps
     * flowing while commits fail.
     */
    @Test
    void keepProcessingModeKeepsDrawingNewWorkWhileCommitsFail() {
        var healed = new AtomicBoolean(false);
        var handler = startContinuingPc(healed); // KEEP_PROCESSING is the builder default

        awaitAsserted(() -> assertThat(handler.contexts).isNotEmpty());

        // while commits are still failing (healed is untouched), new work is drawn and processed
        addRecords(RECORDS, 3);
        awaitAsserted(() -> assertThat(processedRecords).hasSize(RECORDS + 3));
        assertThat(parallelConsumer.isClosedOrFailed()).isFalse();

        healed.set(true);
        awaitCommittedOffset(RECORDS + 3);
    }

    /**
     * Composition with the user's own pause, direction 1: the seam's release must never resume a user
     * {@code pauseIfRunning()}. After the broker heals and the seam pause releases, intake stays stopped until the
     * user's own {@code resumeIfPaused()} - which then restores flow, proving the seam flag really was released
     * rather than merely masked by the user pause.
     */
    @Test
    void seamReleaseNeverClearsUserPause() {
        var commitsHealthy = new AtomicBoolean(true);
        var handler = startPcAndEngageTheSeamPause(commitsHealthy);

        // both pause axes now active: the seam's (from the exhaustion) and the user's
        parallelConsumer.pauseIfRunning();
        addRecords(RECORDS + 1, 3);
        awaitAsserted(() -> assertThat(parallelConsumer.workRemaining()).isEqualTo(3));

        // healing lets the next cadence commit succeed, which releases the SEAM pause only
        commitsHealthy.set(true);
        awaitCommittedOffset(RECORDS + 1);
        assertWithMessage("the seam's release must not resume intake while the user's own pause holds")
                .that(processedRecords).hasSize(RECORDS + 1);

        // the user's resume restores flow - which also proves the seam flag was genuinely released above
        parallelConsumer.resumeIfPaused();
        awaitAsserted(() -> assertThat(processedRecords).hasSize(RECORDS + 4));
        awaitCommittedOffset(RECORDS + 4);
        assertThat(parallelConsumer.isClosedOrFailed()).isFalse();
    }

    /**
     * Composition with the user's own pause, direction 2: the user's {@code resumeIfPaused()} must never
     * clear the seam's pause. With the seam pause active and no user pause set, the resume call is a no-op - intake
     * stays stopped until a commit actually succeeds.
     */
    @Test
    void userResumeIsANoOpOnTheSeamPause() {
        var commitsHealthy = new AtomicBoolean(true);
        var handler = startPcAndEngageTheSeamPause(commitsHealthy);

        // the state is RUNNING, not PAUSED, so this is a no-op - and it must not touch the seam's flag
        parallelConsumer.resumeIfPaused();

        addRecords(RECORDS + 1, 3);
        awaitAsserted(() -> assertThat(parallelConsumer.workRemaining()).isEqualTo(3));
        int exhaustionsBeforeProbe = handler.contexts.size();
        awaitAsserted(() -> assertThat(handler.contexts.size()).isAtLeast(exhaustionsBeforeProbe + 1));
        assertWithMessage("intake must stay stopped after a user resume that had no user pause to clear")
                .that(processedRecords).hasSize(RECORDS + 1);

        // only a successful commit releases the seam pause
        commitsHealthy.set(true);
        awaitAsserted(() -> assertThat(processedRecords).hasSize(RECORDS + 4));
        awaitCommittedOffset(RECORDS + 4);
        assertThat(parallelConsumer.isClosedOrFailed()).isFalse();
    }

    /**
     * During DRAINING the close path wins: a seam pause that is active when {@code closeDrainFirst()} is
     * called does not gate the drain - the records it was holding back are drawn, processed and the close completes,
     * rather than the drain deadlocking behind a pause whose release condition (a successful commit) never arrives
     * (commits never heal here). Without the gate's DRAINING exemption this scenario hangs the drain and the close
     * times out red.
     * <p>
     * The 5s commit interval (the {@code closeBegunStaysHandlerFree} device, in
     * {@link MockConsumerCommitFailureHandlerFreeExitsTest}) keeps the drain window commit-free: the CONTINUE
     * decision resets the cadence, so for seconds after the exhaustion no scheduled commit can race the drain -
     * neither blurring what released the records, nor aborting the drain early through the shutdown guard's fatal
     * route. The close sequence's own final commit still exhausts, and stays handler-free per that test - the
     * invocation count ends where it was before the close.
     */
    @Test
    void activeSeamPauseDoesNotGateTheDrain() {
        var commitsHealthy = new AtomicBoolean(true);
        useCommitsTimingOut(commitsHealthy);
        var handler = continuingHandler();
        startPc(SMALL_BUDGET, Duration.ofSeconds(5), handler, CommitFailureContinueMode.PAUSE_INTAKE);
        addRecordsAndProcess();
        awaitCommittedOffset(RECORDS); // the first commit fires immediately (no previous commit) and is clean

        // break commits for good and drive one record through: the partition turns dirty, and the next cadence
        // (one interval after the clean commit) exhausts and engages the seam pause
        commitsHealthy.set(false);
        addRecords(RECORDS, 1);
        awaitAsserted(() -> assertThat(handler.contexts).isNotEmpty());
        int exhaustionsBeforeClose = handler.contexts.size();

        // gated records must be aboard BEFORE the close: DRAINING pauses the subscription, so anything not yet
        // polled would never arrive - and "drained" would be indistinguishable from "never fetched"
        addRecords(RECORDS + 1, 3);
        awaitAsserted(() -> assertThat(parallelConsumer.workRemaining()).isEqualTo(3));
        assertWithMessage("the seam pause must be holding the new records back before the close begins")
                .that(processedRecords).hasSize(RECORDS + 1);

        parallelConsumer.closeDrainFirst();

        // close wins over the seam pause: the drain drew and processed the gated records
        assertWithMessage("the drain must draw the records the seam pause was holding back")
                .that(processedRecords).hasSize(RECORDS + 4);
        assertWithMessage("close-time commit failures stay handler-free - no decision during the close")
                .that(handler.contexts).hasSize(exhaustionsBeforeClose);
        assertThat(parallelConsumer.isClosedOrFailed()).isTrue();
    }

    /**
     * The opening the two pause-composition scenarios share: PC under
     * {@link CommitFailureContinueMode#PAUSE_INTAKE} with the opening batch processed and cleanly committed (per
     * the structure note on {@link #pauseIntakeStopsNewWorkCompletesInFlightAndResumesAfterCommitSuccess()}), then
     * commits broken and one record driven through, so the exhaustion's CONTINUE engages the seam pause.
     *
     * @param commitsHealthy starts true and is left FALSE - the scenario flips it back to release the pause
     * @return the handler, whose recorded decisions are the evidence the pause is engaged
     */
    private RecordingHandler startPcAndEngageTheSeamPause(AtomicBoolean commitsHealthy) {
        useCommitsTimingOut(commitsHealthy);
        var handler = continuingHandler();
        startPc(SMALL_BUDGET, handler, CommitFailureContinueMode.PAUSE_INTAKE);
        addRecordsAndProcess();
        awaitCommittedOffset(RECORDS); // opening batch processed and cleanly committed - nothing is dirty

        commitsHealthy.set(false);
        addRecords(RECORDS, 1);
        awaitAsserted(() -> assertThat(handler.contexts).isNotEmpty());
        return handler;
    }
}
