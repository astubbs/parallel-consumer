package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.LogCapture;
import bz.stub.parallelconsumer.state.WorkManager;
import ch.qos.logback.classic.Level;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static bz.stub.parallelconsumer.internal.AsyncCommitterFixture.GROUP;
import static bz.stub.parallelconsumer.internal.AsyncCommitterFixture.asyncCommitter;
import static bz.stub.parallelconsumer.internal.AsyncCommitterFixture.callbacksHandedToTheClient;
import static bz.stub.parallelconsumer.internal.AsyncCommitterFixture.commitOf;
import static bz.stub.parallelconsumer.internal.AsyncCommitterFixture.consumerManagerMock;
import static bz.stub.parallelconsumer.internal.AsyncCommitterFixture.partitionOf;
import static bz.stub.parallelconsumer.internal.AsyncCommitterFixture.workManagerMock;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.util.Collections.nCopies;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * What {@link ConsumerOffsetCommitter}'s {@code onAsyncCommitAnswered} does with the answer to a commit request that
 * a later request has partly or wholly overtaken - the case that exists at all only because deferring the
 * clean-marking is what lets two {@code commitAsync} requests be in flight at once.
 * <p>
 * <b>The rule is per partition, and it splits the two things an acknowledgement carries.</b> A late acknowledgement
 * is TRUE - the broker did commit up to the offsets it names - so its offsets are always recorded. What must wait is
 * the CLEAN mark, and only for a partition whose offset a newer, still-unanswered request has passed: marking that
 * one clean is what would leave nothing dirty to re-send if the newer request then failed or was dropped, which is
 * the defect
 * {@code docs/solutions/logic-errors/an-async-commit-was-recorded-on-send-not-on-acknowledgement-2026-09-07.md}
 * removed. Ignoring the whole answer - what the sequence guard this replaces did - was the other error: it threw
 * away a fact the broker had established, for every partition, including the ones nothing had superseded.
 * <p>
 * <b>Why two overlapping in-flight commits cannot be reached from the {@code MockConsumer} tests.</b>
 * {@code MockConsumer.commitAsync} answers inline, on the calling thread, before it returns - so a request is
 * always already answered by the time the next one is sent and the overlap never exists. Reaching it needs the
 * callback held, which means capturing it, which means these mocks.
 * <p>
 * The decisions are asserted <b>at the {@link WorkManager} boundary</b>: which partitions the committer routed to
 * {@code onOffsetCommitSuccess} (record and mark clean) and which to {@code onSupersededOffsetCommitSuccess}
 * (record only). That is the seam the committer's decision is visible at without standing up a partition-assigned
 * engine. What each of those two calls then does to a partition - the offset rising monotonically, the dirty flag
 * left alone - is {@code PartitionStateAcknowledgedCommitOffsetTest}'s subject, on a real {@code PartitionState}.
 *
 * @author Antony Stubbs
 */
// SAME_THREAD for the reason ConsumerOffsetCommitterAsyncFailureLoggingTest states at length: this module runs a
// class's METHODS concurrently, LogCapture's raised level is shared state on a JVM-wide logger, and the methods
// here all log from ConsumerOffsetCommitter.
@Execution(ExecutionMode.SAME_THREAD)
class ConsumerOffsetCommitterOverlappingAsyncCommitTest {

    /**
     * The logger is shared JVM-wide, so every log read is filtered on a string unique to this test.
     */
    private static final String TOPIC = ConsumerOffsetCommitterOverlappingAsyncCommitTest.class.getSimpleName();

    private static final long OLDER_OFFSET = 100L;

    private static final long NEWER_OFFSET = 200L;

    /**
     * The offset the second request does NOT move: partition 1 is complete at it in both requests, so the older
     * answer is the newest word on that partition even though the request as a whole is superseded.
     */
    private static final long UNSUPERSEDED_OFFSET = 50L;

    /**
     * Stands in for PC's encoded offset map on the older commit - what must never be interpolated into a log line,
     * astubbs#168 (confluentinc#629). The real per-partition BOUND is
     * {@code ConsumerOffsetCommitterAsyncFailureLoggingTest}'s to own and measure; all this class asks of the
     * superseded-failure line is that the map is summarised rather than printed.
     */
    private static final String METADATA = String.join("", nCopies(256, "x"));

    private final ConsumerManager<String, String> consumerMgr = consumerManagerMock();

    private final WorkManager<String, String> wm = workManagerMock();

    private final ConsumerOffsetCommitter<String, String> committer = asyncCommitter(consumerMgr, wm);

    /**
     * The scenario that actually happens: the client answers in send order, but the older answer now arrives while a
     * NEWER request is already outstanding.
     * <p>
     * Marking the partition clean here would end the story at {@link #OLDER_OFFSET} while the offsets up to
     * {@link #NEWER_OFFSET} are still unanswered - so if the newer request then failed, nothing would be dirty and
     * nothing would re-commit them. Recording the offset costs nothing and is simply true, so both happen: the
     * offset moves, the partition stays dirty.
     */
    @Test
    void anOlderAnswerIsRecordedAndLeavesThePartitionDirtyForTheHigherOffsetStillInFlight() {
        var older = commitOf(TOPIC, OLDER_OFFSET, METADATA);
        var newer = commitOf(TOPIC, NEWER_OFFSET, METADATA);

        committer.commitOffsets(older, GROUP);
        committer.commitOffsets(newer, GROUP);
        List<OffsetCommitCallback> callbacks = callbacksHandedToTheClient(consumerMgr, 2);

        // the older request is answered only AFTER the newer one has been sent - the whole point
        callbacks.get(0).onComplete(older, null);

        assertWithMessage("the broker committed up to %s, so that offset is recorded whether or not it is the "
                + "newest word on the partition", OLDER_OFFSET)
                .that(offsetsRecordedWithoutMarkingClean()).containsExactly(OLDER_OFFSET);
        assertWithMessage("marking clean at %s would leave nothing dirty to re-send the offsets up to %s if the "
                + "newer request failed", OLDER_OFFSET, NEWER_OFFSET)
                .that(offsetsMarkedClean()).isEmpty();

        // and the newest request's own acknowledgement is what marks clean
        callbacks.get(1).onComplete(newer, null);

        verify(wm, times(1)).onOffsetCommitSuccess(newer);
        assertThat(offsetsMarkedClean()).containsExactly(NEWER_OFFSET);
    }

    /**
     * The out-of-order case: the newest request is acknowledged first and the superseded one answers after it.
     * <p>
     * Nothing is in flight above {@link #NEWER_OFFSET} by then, so the newer answer marks clean - and the older
     * answer that follows must apply nothing at all. It cannot mark clean (there is nothing to clean; the newer
     * answer already did), and the offset it carries is below the one recorded, so
     * {@code PartitionState.recordCommittedOffset} keeps the higher one. Asserting the offsets rather than only the
     * call counts is what puts the DIRECTION of a regression in the failure message.
     */
    @Test
    void anAnswerThatArrivesAfterAHigherOneAppliesNothing() {
        var older = commitOf(TOPIC, OLDER_OFFSET, METADATA);
        var newer = commitOf(TOPIC, NEWER_OFFSET, METADATA);

        committer.commitOffsets(older, GROUP);
        committer.commitOffsets(newer, GROUP);
        List<OffsetCommitCallback> callbacks = callbacksHandedToTheClient(consumerMgr, 2);

        callbacks.get(1).onComplete(newer, null);
        callbacks.get(0).onComplete(older, null);

        assertWithMessage("only the answer carrying the highest offset in flight may mark the partition clean")
                .that(offsetsMarkedClean()).containsExactly(NEWER_OFFSET);
        assertWithMessage("the late answer's own offset is below the one already recorded, so it is routed to the "
                + "record-only path, where PartitionState keeps the higher one")
                .that(offsetsRecordedWithoutMarkingClean()).containsExactly(OLDER_OFFSET);
    }

    /**
     * The case that separates a per-partition rule from a whole-request one, and the reason the rule is per
     * partition at all: the newer request raises partition 0's offset and carries partition 1 at exactly the offset
     * the older request did, because nothing new completed there.
     * <p>
     * Partition 1's answer is therefore the newest word on partition 1 and marks it clean; partition 0's is
     * superseded and does not. Under the sequence guard this replaces, BOTH stayed dirty - partition 1 waited for a
     * re-commit of an offset the broker had already acknowledged, every cycle, for as long as one partition of the
     * assignment kept moving faster than another.
     */
    @Test
    void theOlderAnswerMarksCleanTheOnePartitionTheNewerRequestDidNotSupersede() {
        var older = commitOf(TOPIC, OLDER_OFFSET, UNSUPERSEDED_OFFSET, METADATA);
        var newer = commitOf(TOPIC, NEWER_OFFSET, UNSUPERSEDED_OFFSET, METADATA);

        committer.commitOffsets(older, GROUP);
        committer.commitOffsets(newer, GROUP);
        List<OffsetCommitCallback> callbacks = callbacksHandedToTheClient(consumerMgr, 2);

        callbacks.get(0).onComplete(older, null);

        verify(wm, times(1)).onOffsetCommitSuccess(
                Collections.singletonMap(partitionOf(TOPIC, 1), new OffsetAndMetadata(UNSUPERSEDED_OFFSET, METADATA)));
        verify(wm, times(1)).onSupersededOffsetCommitSuccess(
                Collections.singletonMap(partitionOf(TOPIC, 0), new OffsetAndMetadata(OLDER_OFFSET, METADATA)));
    }

    /**
     * The ERROR line is a contract with whoever is on call: it says these offsets stay dirty and THIS cycle
     * re-commits them. When every partition the failed request carried has a higher offset already in flight, that
     * promise is not this answer's to make - the newer request's answer is what decides those offsets - so the
     * failure is reported as what it is.
     * <p>
     * Still a WARN rather than a DEBUG, because a commit request really did fail. The astubbs#168 bound applies to
     * this line as much as to the ERROR one, hence the last assertion: a failure line that interpolated the offset
     * map would be just as unreadable for being at WARN.
     */
    @Test
    void aFailureSupersededOnEveryPartitionSaysTheNewerRequestDecidesTheOutcome() {
        var older = commitOf(TOPIC, OLDER_OFFSET, METADATA);
        var newer = commitOf(TOPIC, NEWER_OFFSET, METADATA);

        committer.commitOffsets(older, GROUP);
        committer.commitOffsets(newer, GROUP);
        List<OffsetCommitCallback> callbacks = callbacksHandedToTheClient(consumerMgr, 2);

        try (var logs = LogCapture.of(ConsumerOffsetCommitter.class, Level.DEBUG)) {
            callbacks.get(0).onComplete(older, new RetriableCommitFailedException(
                    "Offset commit failed: coordinator unavailable (mocked)"));

            assertWithMessage("a wholly superseded failure must not claim these offsets are being re-committed "
                    + "this cycle")
                    .that(logs.messagesAt(Level.ERROR, TOPIC))
                    .isEmpty();
            String warning = logs.onlyMessageAt(Level.WARN, TOPIC);
            assertThat(warning).contains("superseded");
            assertThat(warning).contains(TOPIC + "-0: offset " + OLDER_OFFSET);
            assertThat(warning).doesNotContain(METADATA);
        }

        // a failure moves no state, superseded or not
        verify(wm, never()).onOffsetCommitSuccess(anyMap());
        verify(wm, never()).onSupersededOffsetCommitSuccess(anyMap());
    }

    /**
     * The other half of the failure rule, and the one the per-partition change adds: the failed request still
     * carries the highest offset in flight for partition 1, so for that partition the ERROR line's promise is
     * exactly true - it stays dirty and the next cycle re-commits it. A request is reported at ERROR when it is
     * true of ANY partition it carried, because the line names them all.
     */
    @Test
    void aFailureStillCarryingTheHighestOffsetForOnePartitionKeepsTheReCommitPromise() {
        var older = commitOf(TOPIC, OLDER_OFFSET, UNSUPERSEDED_OFFSET, METADATA);
        var newer = commitOf(TOPIC, NEWER_OFFSET, UNSUPERSEDED_OFFSET, METADATA);

        committer.commitOffsets(older, GROUP);
        committer.commitOffsets(newer, GROUP);
        List<OffsetCommitCallback> callbacks = callbacksHandedToTheClient(consumerMgr, 2);

        try (var logs = LogCapture.of(ConsumerOffsetCommitter.class, Level.DEBUG)) {
            callbacks.get(0).onComplete(older, new RetriableCommitFailedException(
                    "Offset commit failed: coordinator unavailable (mocked)"));

            String error = logs.onlyMessageAt(Level.ERROR, TOPIC);
            assertThat(error).contains(TOPIC + "-1: offset " + UNSUPERSEDED_OFFSET);
            assertThat(error).doesNotContain(METADATA);
            assertWithMessage("the request is not wholly superseded, so it must not be reported as if a newer "
                    + "request decided every offset on it")
                    .that(logs.messagesAt(Level.WARN, TOPIC))
                    .isEmpty();
        }
    }

    /**
     * @return every offset the committer told the {@link WorkManager} to record AND mark clean, in the order it did
     * so
     */
    private List<Long> offsetsMarkedClean() {
        ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> applied = offsetMapCaptor();
        // atLeast(0) rather than a count: it captures every invocation without asserting how many, so the failure
        // reported is the OFFSETS assertion - which says what was applied and in what order - rather than a
        // verification count that fails first and never shows them
        verify(wm, atLeast(0)).onOffsetCommitSuccess(applied.capture());
        return offsetsOf(applied);
    }

    /**
     * @return every offset the committer told the {@link WorkManager} to record while leaving the partition dirty
     */
    private List<Long> offsetsRecordedWithoutMarkingClean() {
        ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> applied = offsetMapCaptor();
        verify(wm, atLeast(0)).onSupersededOffsetCommitSuccess(applied.capture());
        return offsetsOf(applied);
    }

    @SuppressWarnings("unchecked")
    private static ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> offsetMapCaptor() {
        return ArgumentCaptor.forClass((Class<Map<TopicPartition, OffsetAndMetadata>>) (Class<?>) Map.class);
    }

    private static List<Long> offsetsOf(ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> applied) {
        return applied.getAllValues().stream()
                .flatMap(commit -> commit.values().stream())
                .map(OffsetAndMetadata::offset)
                .collect(Collectors.toList());
    }

}
