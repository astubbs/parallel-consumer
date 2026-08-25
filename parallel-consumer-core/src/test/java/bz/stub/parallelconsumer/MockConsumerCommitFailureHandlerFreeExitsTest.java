package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * The four exits of the commit-failure seam (astubbs#317, confluentinc#833) that stay <em>handler-free</em>: a
 * genuine poller death, a non-retriable commit failure, a failure once close has begun, and an exhaustion inside
 * the revocation callback. None of them has a waiter that could act on a decision, so none of them may consult the
 * {@link CommitFailureHandler} - and each keeps its historical disposition.
 * <p>
 * The fixture - the failing {@link MockConsumer}, the recording handler, the waits - is
 * {@link MockConsumerCommitFailureSeamTestBase}, which also names the other slices of the seam.
 *
 * @author Antony Stubbs
 * @see CommitFailureHandler
 */
class MockConsumerCommitFailureHandlerFreeExitsTest extends MockConsumerCommitFailureSeamTestBase {

    /**
     * A genuine poller death - the broker-poll thread dying of something that is not budget exhaustion -
     * stays fatal and handler-free. No decision can revive the only producer of commit responses.
     */
    @Test
    void genuinePollerDeathStaysFatalAndHandlerFree() {
        final String pollerFailureMessage = "simulated poller death (mocking)";
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized ConsumerRecords<String, String> poll(Duration timeout) {
                throw new FakeRuntimeException(pollerFailureMessage);
            }
        };
        var handler = continuingHandler();
        startPc(SMALL_BUDGET, handler);
        addRecordsAndProcess();

        awaitAsserted(() -> assertThat(parallelConsumer.isClosedOrFailed()).isTrue());

        assertThat(handler.contexts).isEmpty();
        Exception failureCause = parallelConsumer.getFailureCause();
        assertThat(failureCause).isNotNull();
        assertThat(chainWithSuppressed(failureCause).stream()
                .anyMatch(t -> String.valueOf(t.getMessage()).contains(pollerFailureMessage))).isTrue();
    }

    /**
     * A non-retriable commit failure (authorization) stays immediately fatal and handler-free - the
     * seam intercepts only the exhaustion of a retriable budget, never failure classes continuing cannot answer.
     */
    @Test
    void authorizationFailureStaysFatalAndHandlerFree() {
        final String authorizationFailureMessage = "Not authorized to commit (mocking)";
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
                throw new TopicAuthorizationException(authorizationFailureMessage);
            }
        };
        var handler = continuingHandler();
        startPc(SMALL_BUDGET, handler);
        addRecordsAndProcess();

        awaitAsserted(() -> assertThat(parallelConsumer.isClosedOrFailed()).isTrue());

        assertThat(handler.contexts).isEmpty();
        Exception failureCause = parallelConsumer.getFailureCause();
        assertThat(failureCause).isNotNull();
        assertThat(chainWithSuppressed(failureCause).stream()
                .anyMatch(t -> t instanceof TopicAuthorizationException)).isTrue();
    }

    /**
     * Once close has begun the handler is never consulted: a commit failing during the close sequence keeps
     * its historical handler-free disposition, and the close itself completes rather than wedging behind a decision
     * nobody can act on.
     */
    @Test
    void closeBegunStaysHandlerFree() {
        useCommitsTimingOut(null);
        var handler = continuingHandler();
        // commit interval much longer than the test's close step, so no second scheduled exhaustion can race the
        // close and blur the count below
        startPc(Duration.ofMillis(500), Duration.ofSeconds(5), handler);
        addRecordsAndProcess();

        awaitAsserted(() -> assertThat(handler.contexts).hasSize(1));

        parallelConsumer.closeDontDrainFirst();

        // the close sequence's own final commit also exhausted its budget (commits never heal here), and it did so
        // handler-free: the invocation count is unchanged
        assertThat(handler.contexts).hasSize(1);
        assertThat(parallelConsumer.isClosedOrFailed()).isTrue();
        assertThat(parallelConsumer.getFailureCause()).isNull();
    }

    /**
     * The fourth handler-free exit, pinned in isolation: a commit whose budget exhausts DURING partition revocation -
     * inside the rebalance callback, where there is no waiter to hand a decision to - is a DEFERRAL. The poller
     * stays alive, the instance stays open, the handler is not consulted, and the offsets are not recorded as
     * committed; they are the new assignee's to resolve by reprocessing.
     * <p>
     * The long commit interval keeps the scheduled-commit lane quiet, so the ONLY commit that can exhaust here is
     * the revocation-time one - otherwise "handler not consulted" could pass or fail on an unrelated scheduled
     * exhaustion.
     */
    @Test
    void revocationTimeBudgetExhaustionDefersWithoutKillingOrConsultingTheHandler() {
        var commitsHealthy = new AtomicBoolean(true);
        useCommitsTimingOut(commitsHealthy);
        var handler = continuingHandler();
        startPc(SMALL_BUDGET, Duration.ofSeconds(30), handler);
        addRecordsAndProcess();
        // the first commit fires immediately; requesting one explicitly makes the whole batch land regardless of
        // how it interleaved with processing, before the 30s cadence takes over
        awaitAsserted(() -> assertThat(processedRecords).hasSize(RECORDS));
        parallelConsumer.requestCommitAsap();
        awaitCommittedOffset(RECORDS);

        // break commits, then make the partition dirty again - no scheduled commit will touch it for 30s
        commitsHealthy.set(false);
        addRecords(RECORDS, 1); // offset 5
        awaitAsserted(() -> assertThat(processedRecords).hasSize(RECORDS + 1));

        // the revocation-time commit spends its whole budget and exhausts - and that must NOT escape the callback
        parallelConsumer.onPartitionsRevoked(of(TOPIC_PARTITION));

        assertWithMessage("a revocation-time exhaustion has no waiter, so the handler must not be consulted")
                .that(handler.contexts).isEmpty();
        assertThat(parallelConsumer.isClosedOrFailed()).isFalse();
        assertThat(parallelConsumer.getFailureCause()).isNull();
        // not recorded as committed: the broker still holds the pre-revocation offset
        var committed = mockConsumer.committed(Collections.singleton(TOPIC_PARTITION)).get(TOPIC_PARTITION);
        assertThat(committed.offset()).isEqualTo(RECORDS);

        // and the instance is genuinely alive: reassigned, healed, it processes and commits new work
        mockConsumer.rebalance(of(TOPIC_PARTITION));
        parallelConsumer.onPartitionsAssigned(of(TOPIC_PARTITION));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(TOPIC_PARTITION, 0L));
        commitsHealthy.set(true);
        addRecords(RECORDS + 1, 3); // offsets 6..8
        awaitAsserted(() -> assertThat(processedRecords).hasSize(RECORDS + 4));
        parallelConsumer.requestCommitAsap();
        awaitCommittedOffset(RECORDS + 4);
    }
}
