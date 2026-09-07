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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static bz.stub.parallelconsumer.internal.AsyncCommitterFixture.GROUP;
import static bz.stub.parallelconsumer.internal.AsyncCommitterFixture.asyncCommitter;
import static bz.stub.parallelconsumer.internal.AsyncCommitterFixture.callbacksHandedToTheClient;
import static bz.stub.parallelconsumer.internal.AsyncCommitterFixture.commitOf;
import static bz.stub.parallelconsumer.internal.AsyncCommitterFixture.consumerManagerMock;
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
 * The sequence guard in {@link ConsumerOffsetCommitter}'s {@code onAsyncCommitAnswered}: an answer to a
 * <em>superseded</em> {@code commitAsync} request must not be treated as an outcome for the offsets a later request
 * is still carrying.
 * <p>
 * <b>Nothing downstream is a second line of defence, which is why this is a test and not a comment.</b>
 * {@code PartitionState.onOffsetCommitSuccess} assigns {@code lastCommittedOffset = committed.offset()}
 * unconditionally and then marks the partition clean - it neither compares against what it already holds nor knows
 * that another request is outstanding. So a stale success reaching it walks the recorded offset BACKWARDS and ends
 * the story, exactly the way marking clean on send did before
 * {@code docs/solutions/logic-errors/an-async-commit-was-recorded-on-send-not-on-acknowledgement-2026-09-07.md}.
 * The guard in the committer is the whole of the protection, and these tests are the whole of the evidence that it
 * works: removing it turns all three red.
 * <p>
 * <b>Why two overlapping in-flight commits cannot be reached from the {@code MockConsumer} tests.</b>
 * {@code MockConsumer.commitAsync} answers inline, on the calling thread, before it returns - so a request is
 * always already answered by the time the next one is sent and the overlap never exists. Reaching it needs the
 * callback held, which means capturing it, which means these mocks.
 * <p>
 * The offsets are asserted <b>at the {@link WorkManager} boundary</b> rather than on a real {@code PartitionState}.
 * That is the seam the committer's decision is visible at, and it is the one this class can observe without
 * standing up a partition-assigned engine; what happens past it is `PartitionState`'s unconditional assignment,
 * quoted above.
 *
 * @author Antony Stubbs
 */
// SAME_THREAD for the reason ConsumerOffsetCommitterAsyncFailureLoggingTest states at length: this module runs a
// class's METHODS concurrently, LogCapture's raised level is shared state on a JVM-wide logger, and the methods
// here all log from ConsumerOffsetCommitter. Only one of them reads the log today, so this is what keeps the next
// one that does from being flaky rather than a fix for an observed failure.
@Execution(ExecutionMode.SAME_THREAD)
class ConsumerOffsetCommitterSupersededAsyncCommitTest {

    /**
     * The logger is shared JVM-wide, so every log read is filtered on a string unique to this test.
     */
    private static final String TOPIC = ConsumerOffsetCommitterSupersededAsyncCommitTest.class.getSimpleName();

    private static final long OLDER_OFFSET = 100L;

    private static final long NEWER_OFFSET = 200L;

    /**
     * Stands in for PC's encoded offset map on the older commit - what must never be interpolated into a log line,
     * astubbs#168 (confluentinc#629). The real per-partition BOUND is
     * {@code ConsumerOffsetCommitterAsyncFailureLoggingTest}'s to own and measure; all this class asks of the
     * <em>new</em> superseded-failure line is that the map is summarised rather than printed.
     */
    private static final String METADATA = String.join("", nCopies(256, "x"));

    private final ConsumerManager<String, String> consumerMgr = consumerManagerMock();

    private final WorkManager<String, String> wm = workManagerMock();

    private final ConsumerOffsetCommitter<String, String> committer = asyncCommitter(consumerMgr, wm);

    /**
     * The review's scenario, and the one that actually happens: the client answers in send order, but the older
     * answer now arrives when a NEWER request is already outstanding, because deferring the clean-marking is what
     * made a second request possible at all.
     * <p>
     * Acknowledging the older one would mark the partition CLEAN while the offsets up to {@link #NEWER_OFFSET} are
     * still unanswered - so if the newer request then failed, nothing would be dirty and nothing would re-commit
     * it. That is the original defect, re-entered through the door the fix opened.
     */
    @Test
    void aSupersededAcknowledgementIsIgnoredAndTheStillOutstandingOneIsNot() {
        var older = commitOf(TOPIC, OLDER_OFFSET, METADATA);
        var newer = commitOf(TOPIC, NEWER_OFFSET, METADATA);

        committer.commitOffsets(older, GROUP);
        committer.commitOffsets(newer, GROUP);
        List<OffsetCommitCallback> callbacks = callbacksHandedToTheClient(consumerMgr, 2);

        // the older request is answered only AFTER the newer one has been sent - the whole point
        callbacks.get(0).onComplete(older, null);

        verify(wm, never()).onOffsetCommitSuccess(anyMap());

        // and the newest request's own acknowledgement is still applied, so the guard defers rather than drops
        callbacks.get(1).onComplete(newer, null);

        verify(wm, times(1)).onOffsetCommitSuccess(newer);
    }

    /**
     * The out-of-order case, which is what the sequence number exists for rather than merely helps with: the
     * newest request is acknowledged first and the superseded one answers after it.
     * <p>
     * Without the guard the {@link WorkManager} is told twice, second time with the LOWER offset, and
     * {@code PartitionState.onOffsetCommitSuccess} takes it - the commit watermark and the
     * {@code pc.partition.latest.committed.offset} gauge both go backwards, and the partition is left clean at an
     * offset the newer commit had already passed. Asserting the single applied offset, rather than only the call
     * count, is what makes the direction of the regression part of the failure message.
     */
    @Test
    void anAnswerThatArrivesAfterAHigherOneCannotWalkTheCommittedOffsetBackwards() {
        var older = commitOf(TOPIC, OLDER_OFFSET, METADATA);
        var newer = commitOf(TOPIC, NEWER_OFFSET, METADATA);

        committer.commitOffsets(older, GROUP);
        committer.commitOffsets(newer, GROUP);
        List<OffsetCommitCallback> callbacks = callbacksHandedToTheClient(consumerMgr, 2);

        callbacks.get(1).onComplete(newer, null);
        callbacks.get(0).onComplete(older, null);

        assertThat(offsetsAppliedToTheWorkManager()).containsExactly(NEWER_OFFSET);
    }

    /**
     * The ERROR line is a contract with whoever is on call: it says the offsets stay dirty and THIS cycle
     * re-commits them. For a superseded request that claim is not the committer's to make - a later request has
     * already been sent, and its answer is what decides those offsets - so supersession is checked before the
     * exception and this failure is reported as what it is.
     * <p>
     * Still a WARN rather than a DEBUG, because a commit request really did fail; what it must not do is claim an
     * outcome. The astubbs#168 bound applies to the new line as much as to the old one, hence the last assertion:
     * a failure line that interpolated the offset map would be just as unreadable for being at WARN.
     */
    @Test
    void aSupersededFailureSaysItWasSupersededInsteadOfClaimingAReCommit() {
        var older = commitOf(TOPIC, OLDER_OFFSET, METADATA);
        var newer = commitOf(TOPIC, NEWER_OFFSET, METADATA);

        committer.commitOffsets(older, GROUP);
        committer.commitOffsets(newer, GROUP);
        List<OffsetCommitCallback> callbacks = callbacksHandedToTheClient(consumerMgr, 2);

        try (var logs = LogCapture.of(ConsumerOffsetCommitter.class, Level.DEBUG)) {
            callbacks.get(0).onComplete(older, new RetriableCommitFailedException(
                    "Offset commit failed: coordinator unavailable (mocked)"));

            assertWithMessage("a superseded failure must not claim these offsets are being re-committed this cycle")
                    .that(logs.messagesAt(Level.ERROR, TOPIC))
                    .isEmpty();
            String warning = logs.onlyMessageAt(Level.WARN, TOPIC);
            assertThat(warning).contains("superseded");
            assertThat(warning).contains(TOPIC + "-0: offset " + OLDER_OFFSET);
            assertThat(warning).doesNotContain(METADATA);
        }

        // a failure marks nothing clean whether it was superseded or not
        verify(wm, never()).onOffsetCommitSuccess(anyMap());
    }

    /**
     * @return every offset the committer told the {@link WorkManager} the broker had acknowledged, in the order it
     * did so - one entry per commit, since each is a single-partition map
     */
    private List<Long> offsetsAppliedToTheWorkManager() {
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> applied =
                ArgumentCaptor.forClass((Class<Map<TopicPartition, OffsetAndMetadata>>) (Class<?>) Map.class);
        // atLeast(0) rather than a count: it captures every invocation without asserting how many, so the failure
        // reported is the OFFSETS assertion - which says what was applied and in what order - rather than a
        // verification count that fails first and never shows them
        verify(wm, atLeast(0)).onOffsetCommitSuccess(applied.capture());
        return applied.getAllValues().stream()
                .flatMap(commit -> commit.values().stream())
                .map(OffsetAndMetadata::offset)
                .collect(Collectors.toList());
    }

}
