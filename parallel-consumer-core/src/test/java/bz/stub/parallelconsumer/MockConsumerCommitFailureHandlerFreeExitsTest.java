package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.internal.ConsumerOffsetCommitter;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.util.concurrent.TimeUnit.SECONDS;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * The exits of the commit-failure seam (astubbs#317, confluentinc#833) that stay <em>handler-free</em>: a
 * genuine poller death, a non-retriable commit failure, a failure once close has begun, and the two ways a
 * revocation-time commit gives up - its budget exhausting, and the commit lock being held by a commit already in
 * flight. None of them has a waiter that could act on a decision, so none of them may consult the
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

    /**
     * The revocation-time commit <b>declines rather than waits</b> when a commit is already in flight - the
     * regression test for the AB-BA deadlock of astubbs#29 / confluentinc#857
     * (docs/solutions/runtime-errors/revoke-path-commit-deadlock-between-poll-and-control-threads.md owns the
     * diagnosis), which this branch reopened by removing the waiter's local deadline.
     * <p>
     * <b>Pre-fix, this interleaving hung permanently</b>, and nothing timed out to say so. The control thread
     * entered {@code commitOffsetsThatAreReady()}, took the {@code commitCommand} monitor, and blocked in
     * {@code ConsumerOffsetCommitter.commitAndWait()} - which only the broker-poll thread can answer. The
     * revocation callback then tried to enter that same monitor. The waiter's old {@code offsetCommitTimeout}
     * deadline used to break the cycle fatally after ~10s; with every exit now an affirmative event published
     * from the commit path, and the poll thread wedged outside it, no exit existed at all.
     * <p>
     * The interleaving is driven at production hooks rather than by sleeping: the mock consumer's {@code poll}
     * parks (that IS the wedged poll thread - the only producer of commit responses, stuck outside the commit
     * path), and the control thread's arrival in {@code commitAndWait} is awaited through its own heartbeat WARN.
     * The revocation runs on a separate thread with a bounded join, so the pre-fix behaviour surfaces as a named
     * assertion failure rather than a test that hangs until its {@code @Timeout}.
     * <p>
     * With the fix it resolves as a deferral: WARN naming the contention, the offsets left dirty for the new
     * assignee, no handler consulted - and both threads then make progress, which the probe at the end proves
     * from the control thread's side.
     */
    @Test
    void aRevocationCommitDeclinesRatherThanBlockingBehindACommitInFlight() throws InterruptedException {
        var pollShouldPark = new AtomicBoolean(false);
        var pollParked = new CountDownLatch(1);
        var pollRelease = new CountDownLatch(1);
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
            @Override
            // deliberately NOT synchronized: this parks, and MockConsumer guards poll, addRecord, commitSync and
            // close with one monitor - see CommitResponseTimeoutSymptomTest for the measured cost of holding it
            public ConsumerRecords<String, String> poll(Duration timeout) {
                if (pollShouldPark.get()) {
                    pollParked.countDown();
                    try {
                        if (!pollRelease.await(60, SECONDS)) {
                            throw new FakeRuntimeException("the parked poll was never released - test failure");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    pollShouldPark.set(false);
                }
                return super.poll(timeout);
            }
        };
        var handler = continuingHandler();
        var committerLogger = (Logger) LoggerFactory.getLogger(ConsumerOffsetCommitter.class);
        var processorLogger = (Logger) LoggerFactory.getLogger(AbstractParallelEoSStreamProcessor.class);
        var appender = new ListAppender<ILoggingEvent>();
        appender.start();
        committerLogger.addAppender(appender);
        processorLogger.addAppender(appender);
        try {
            // healthy baseline: commits work, so a red later is about the interleaving and not the fixture. One
            // record is held in flight, because a commit cycle only runs when something is dirty - that held
            // record is what makes the control thread want to commit AFTER the poll thread is wedged.
            var heldRecordEntered = new CountDownLatch(1);
            var heldRecordRelease = new CountDownLatch(1);
            startPc(SMALL_BUDGET, handler);
            addRecords(0, RECORDS);
            startProcessingHoldingAt(RECORDS - 1, heldRecordEntered, heldRecordRelease);
            assertWithMessage("the held record was never reached")
                    .that(heldRecordEntered.await(30, SECONDS)).isTrue();
            awaitCommittedOffset(RECORDS - 1);

            // wedge the poll thread OUTSIDE the commit path - it can no longer service any commit request
            pollShouldPark.set(true);
            assertWithMessage("the poll thread never parked").that(pollParked.await(30, SECONDS)).isTrue();

            // now make the partition dirty, which is what sends the control thread into a commit it cannot have
            // answered
            heldRecordRelease.countDown();

            // ...and wait until the control thread is demonstrably blocked in commitAndWait, holding the commit
            // lock. Its heartbeat WARN is the affirmative signal that it is (the cadence commits every 100ms,
            // and the heartbeat follows one offsetCommitTimeout later)
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                    assertWithMessage("the control thread never reached the commit wait")
                            .that(loggedContaining(appender, "Commit response still pending")).isAtLeast(1L));

            // the revocation callback's turn - on another thread, so a pre-fix block is a failed assertion
            // rather than a hung test
            var revocation = new Thread(() -> parallelConsumer.onPartitionsRevoked(of(TOPIC_PARTITION)),
                    TOPIC + "-revocation");
            revocation.start();
            revocation.join(Duration.ofSeconds(20).toMillis());
            assertWithMessage("the revocation callback blocked on the commit lock - this is the astubbs#29 "
                    + "deadlock: it can only be released by the poll thread it is itself running on")
                    .that(revocation.isAlive()).isFalse();

            // it declined, and said so, naming the contention
            assertWithMessage("declining must be loud - it is the only evidence the contended branch ran")
                    .that(loggedContaining(appender, "Offset commit SKIPPED during partition revocation"))
                    .isAtLeast(1L);
            assertWithMessage("declining is a deferral, not a decision point - no waiter, so no handler")
                    .that(handler.contexts).isEmpty();
            // and nothing was recorded as committed that never reached the broker - the held record's offset is
            // still dirty, for the new assignee to reprocess
            var committed = mockConsumer.committed(Collections.singleton(TOPIC_PARTITION)).get(TOPIC_PARTITION);
            assertThat(committed.offset()).isEqualTo(RECORDS - 1);

            // both threads make progress: unwedge the poller, and the control thread's blocked commit is
            // answered - proven by the commit lock becoming acquirable again from a third thread
            pollRelease.countDown();
            var lockProbe = new Thread(() -> parallelConsumer.requestCommitAsap(), TOPIC + "-lock-probe");
            lockProbe.start();
            lockProbe.join(Duration.ofSeconds(30).toMillis());
            assertWithMessage("the control thread never left its commit cycle, so the pair did not recover")
                    .that(lockProbe.isAlive()).isFalse();
            assertThat(parallelConsumer.isClosedOrFailed()).isFalse();
            assertThat(parallelConsumer.getFailureCause()).isNull();
        } finally {
            pollRelease.countDown();
            committerLogger.detachAppender(appender);
            processorLogger.detachAppender(appender);
            appender.stop();
        }
    }

    /** How many WARNs so far contain {@code fragment} - read repeatedly, so a copy of the list is taken. */
    private static long loggedContaining(ListAppender<ILoggingEvent> appender, String fragment) {
        return new java.util.ArrayList<>(appender.list).stream()
                .filter(event -> event.getLevel() == Level.WARN)
                .filter(event -> String.valueOf(event.getFormattedMessage()).contains(fragment))
                .count();
    }
}
