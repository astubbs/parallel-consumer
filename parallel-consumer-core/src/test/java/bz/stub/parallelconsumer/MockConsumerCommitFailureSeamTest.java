package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.CommitFailureHandler.CommitFailureDecision;
import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitFailureContinueMode;
import bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static bz.stub.parallelconsumer.internal.utils.ThreadUtils.sleepOrFail;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.parallel.ResourceAccessMode.READ_WRITE;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * The commit-failure seam end to end on the sync consumer path (astubbs#317, confluentinc#833): a commit that
 * exhausts its retry budget reaches the configured {@link CommitFailureHandler} as a decision, instead of
 * unconditionally killing the broker-poll thread and with it the instance.
 * <p>
 * The scenarios pin, in order: the default is byte-compatible with the pre-seam world (shut down, cause recorded -
 * AE1); the handler sees an accurate history (attempts, elapsed, consecutive exhaustions); both decisions work end
 * to end, including recovery after CONTINUE (AE2); the handler is fail-safe - throwing (AE5) or hanging converts to
 * shut-down rather than a wedged instance; a deciding handler holds no PC monitor, so rebalance-path callers of the
 * {@code commitCommand} monitor are never blocked by user code; and the four handler-free exits stay handler-free:
 * genuine poller death (AE7), non-retriable failures (AE6), and close-time failures. The SASL budget lane feeds the
 * same event, and the waiter in {@code ConsumerOffsetCommitter#commitAndWait} no longer carries its own
 * {@code offsetCommitTimeout} deadline - it outlives a commit attempt held open past it, which is the precondition
 * for the seam being reachable at all (KTD2 in the plan).
 * <p>
 * Deliberately not a subclass of {@link MockConsumerTestBase}, for {@link CommitResponseTimeoutSymptomTest}'s
 * reasons: these scenarios need a different consumer, different options and a different handler per test, and
 * several deliberately end with a non-null failure cause, which that base's teardown asserts against.
 *
 * @author Antony Stubbs
 * @see CommitFailureHandler
 * @see CommitResponseTimeoutSymptomTest
 */
@Slf4j
@Timeout(120)
class MockConsumerCommitFailureSeamTest {

    private static final String TOPIC = MockConsumerCommitFailureSeamTest.class.getSimpleName();

    private static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, 0);

    private static final int RECORDS = 5;

    /**
     * Pacing sleep inside a failing mock {@code commitSync}, so a budget's retry loop makes a handful of attempts
     * rather than hot-spinning thousands of log lines. Never held under the {@link MockConsumer} monitor.
     */
    private static final Duration FAILING_COMMIT_PACING = Duration.ofMillis(20);

    /** Small budget so exhaustion happens quickly; big enough that the paced retries make several attempts. */
    private static final Duration SMALL_BUDGET = Duration.ofMillis(300);

    private MockConsumer<String, String> mockConsumer;

    private ParallelEoSStreamProcessor<String, String> parallelConsumer;

    /** Records handed to the user function. Concurrent because PC's worker threads write it. */
    private final ConcurrentLinkedQueue<RecordContext<String, String>> processedRecords = new ConcurrentLinkedQueue<>();

    @AfterEach
    void closePc() {
        Awaitility.reset();
        if (parallelConsumer != null && !parallelConsumer.isClosedOrFailed()) {
            parallelConsumer.closeDontDrainFirst();
        }
    }

    /**
     * Covers AE1, and is the characterization of the pre-seam behaviour: with default configuration (the canned
     * {@link CommitFailurePolicies#shutDown()} handler), budget exhaustion still closes the instance and
     * {@link ParallelEoSStreamProcessor#getFailureCause()} still carries the commit failure - byte-compatible with
     * the world before the seam. What has changed is only the message: it used to say the decision could not be
     * handed to the application ("no way yet to hand"), which the seam makes false.
     */
    @Test
    void defaultConfigurationStillClosesOnExhaustionWithTheCommitFailureAsCause() {
        mockConsumer = consumerWithFailingCommits(() -> new TimeoutException("mock commit timeout"), null);
        startPc(SMALL_BUDGET, CommitFailurePolicies.shutDown());
        addRecordsAndProcess();

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(parallelConsumer.isClosedOrFailed()).isTrue());

        Exception failureCause = parallelConsumer.getFailureCause();
        assertThat(failureCause).isNotNull();
        var chain = causeChain(failureCause);
        var budgetFailure = chain.stream()
                .filter(t -> t instanceof OffsetCommitBudgetExceededException)
                .findFirst();
        assertWithMessage("the commit failure must be reachable from getFailureCause()")
                .that(budgetFailure.isPresent()).isTrue();

        String message = budgetFailure.get().getMessage();
        assertThat(message).contains("offsetCommitTimeout");
        // the seam exists now, so the message must not claim the opposite (the pre-seam text pointed at
        // astubbs/parallel-consumer#317 as an open limit)
        assertThat(message).doesNotContain("no way yet to hand");
        assertThat(message).contains("commitFailureHandler");
    }

    /**
     * The handler's history is accurate: each exhaustion arrives with the attempts and elapsed time the budget loop
     * actually spent, and a consecutive count that grows while no commit succeeds.
     */
    @Test
    void handlerReceivesAttemptsElapsedAndConsecutiveHistoryOnEachExhaustion() {
        var healed = new AtomicBoolean(false);
        mockConsumer = consumerWithFailingCommits(() -> new TimeoutException("mock commit timeout"), healed);
        var handler = new RecordingHandler(CommitFailureDecision.CONTINUE);
        startPc(SMALL_BUDGET, handler);
        addRecordsAndProcess();

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(handler.contexts.size()).isAtLeast(2));

        var contexts = new ArrayList<>(handler.contexts);
        var first = contexts.get(0);
        assertThat(first.getFailure()).isInstanceOf(OffsetCommitBudgetExceededException.class);
        assertThat(first.getFailure().getCause()).isInstanceOf(TimeoutException.class);
        assertThat(first.getOffsets()).containsKey(TOPIC_PARTITION);
        assertThat(first.getAttemptsMade()).isAtLeast(1);
        // exhaustion requires the elapsed time to have exceeded the budget
        assertThat(first.getElapsed().toMillis()).isAtLeast(SMALL_BUDGET.toMillis());
        assertThat(first.getConsecutiveExhaustedBudgets()).isEqualTo(1);
        assertThat(first.getTimeSinceLastSuccessfulCommit().isNegative()).isFalse();
        assertThat(first.getCommitMode()).isEqualTo(ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC);

        var second = contexts.get(1);
        assertThat(second.getConsecutiveExhaustedBudgets()).isEqualTo(2);

        assertThat(parallelConsumer.isClosedOrFailed()).isFalse();

        settleBeforeTeardown(healed);
    }

    /**
     * Covers AE2 (the mock half): a CONTINUE decision leaves the offsets dirty, and when the broker heals, the next
     * commit cadence commits them with a fresh budget - nothing was lost and nothing was wrongly marked done.
     */
    @Test
    void continueThenBrokerHealsCommitsDirtyOffsetsOnNextCadenceWithAFreshBudget() {
        var healed = new AtomicBoolean(false);
        mockConsumer = consumerWithFailingCommits(() -> new TimeoutException("mock commit timeout"), healed);
        var handler = new RecordingHandler(CommitFailureDecision.CONTINUE);
        startPc(SMALL_BUDGET, handler);
        addRecordsAndProcess();

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(handler.contexts).isNotEmpty());

        healed.set(true);

        // the offsets stayed dirty through the CONTINUE, so the healed broker receives them on the next cadence
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var committed = mockConsumer.committed(Collections.singleton(TOPIC_PARTITION)).get(TOPIC_PARTITION);
            assertThat(committed).isNotNull();
            assertThat(committed.offset()).isEqualTo(RECORDS);
        });

        assertThat(parallelConsumer.getFailureCause()).isNull();
        assertThat(parallelConsumer.isClosedOrFailed()).isFalse();
    }

    /**
     * Covers AE5: a handler that throws decides nothing - fail-safe SHUT_DOWN - and the reported failure names both
     * the commit failure (as the primary cause chain) and the handler's own exception (travelling with it).
     */
    @Test
    void handlerThatThrowsShutsDownNamingBothExceptions() {
        final String handlerFailureMessage = "handler blew up (mocking)";
        mockConsumer = consumerWithFailingCommits(() -> new TimeoutException("mock commit timeout"), null);
        startPc(SMALL_BUDGET, context -> {
            throw new FakeRuntimeException(handlerFailureMessage);
        });
        addRecordsAndProcess();

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(parallelConsumer.isClosedOrFailed()).isTrue());

        Exception failureCause = parallelConsumer.getFailureCause();
        assertThat(failureCause).isNotNull();
        var everyThrowable = chainWithSuppressed(failureCause);
        assertWithMessage("the commit failure must be named")
                .that(everyThrowable.stream().anyMatch(t -> t instanceof OffsetCommitBudgetExceededException))
                .isTrue();
        assertWithMessage("the handler's own exception must be named too")
                .that(everyThrowable.stream()
                        .anyMatch(t -> String.valueOf(t.getMessage()).contains(handlerFailureMessage)))
                .isTrue();
    }

    /**
     * A handler that exceeds its time bound is treated as SHUT_DOWN: the instance closes with the commit failure as
     * cause rather than wedging behind user code that never answers.
     * <p>
     * The bound is an internal constant (default 30s, deliberately not a user option); it is shortened here through
     * the same static test seam {@code BrokerPollSystem.longPollTimeout} uses. The {@link ResourceLock} serialises
     * this test against {@link #aSlowHandlerDoesNotBlockTheCommitCommandMonitor}, the other test whose handler is
     * deliberately in flight for a while - a shortened bound leaking into that test would convert its held-open
     * decision into a spurious timeout.
     */
    @Test
    @ResourceLock(value = "commitFailureHandlerTimeBound", mode = READ_WRITE)
    void handlerExceedingItsTimeBoundShutsDownRatherThanWedging() {
        Duration originalBound = AbstractParallelEoSStreamProcessor.getCommitFailureHandlerTimeBound();
        AbstractParallelEoSStreamProcessor.setCommitFailureHandlerTimeBound(Duration.ofSeconds(1));
        try {
            var handlerHold = new CountDownLatch(1);
            mockConsumer = consumerWithFailingCommits(() -> new TimeoutException("mock commit timeout"), null);
            startPc(SMALL_BUDGET, context -> {
                try {
                    // never released - only the fail-safe time bound (or its cancel interrupt) ends this
                    handlerHold.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return CommitFailureDecision.CONTINUE;
            });
            addRecordsAndProcess();

            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                    assertThat(parallelConsumer.isClosedOrFailed()).isTrue());

            Exception failureCause = parallelConsumer.getFailureCause();
            assertThat(failureCause).isNotNull();
            assertThat(causeChain(failureCause).stream()
                    .anyMatch(t -> t instanceof OffsetCommitBudgetExceededException)).isTrue();
        } finally {
            AbstractParallelEoSStreamProcessor.setCommitFailureHandlerTimeBound(originalBound);
        }
    }

    /**
     * KTD3's monitor-free invocation: while a slow handler is still deciding, the {@code commitCommand} monitor is
     * free - so a rebalance-path caller (the revocation commit synchronizes on it) is never blocked behind user
     * code. Probed directly through {@link AbstractParallelEoSStreamProcessor#requestCommitAsap()}, which takes
     * exactly that monitor.
     */
    @Test
    @ResourceLock(value = "commitFailureHandlerTimeBound", mode = READ_WRITE)
    void aSlowHandlerDoesNotBlockTheCommitCommandMonitor() throws InterruptedException {
        var handlerEntered = new CountDownLatch(1);
        var handlerRelease = new CountDownLatch(1);
        var healed = new AtomicBoolean(false);
        mockConsumer = consumerWithFailingCommits(() -> new TimeoutException("mock commit timeout"), healed);
        var handler = new RecordingHandler(CommitFailureDecision.CONTINUE) {
            @Override
            public CommitFailureDecision onCommitFailure(CommitFailureContext context) {
                handlerEntered.countDown();
                try {
                    // held open only while the monitor probe below runs; bounded so a failure cannot wedge
                    handlerRelease.await(20, SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return super.onCommitFailure(context);
            }
        };
        startPc(SMALL_BUDGET, handler);
        addRecordsAndProcess();

        assertWithMessage("the handler was never invoked")
                .that(handlerEntered.await(30, SECONDS)).isTrue();

        // while the handler is deciding, the commitCommand monitor must be acquirable by another thread
        var monitorProbe = new Thread(() -> parallelConsumer.requestCommitAsap(), TOPIC + "-monitor-probe");
        monitorProbe.start();
        monitorProbe.join(Duration.ofSeconds(10).toMillis());
        boolean probeCompleted = !monitorProbe.isAlive();
        handlerRelease.countDown();
        assertWithMessage("acquiring the commitCommand monitor must not block while the handler is deciding "
                + "- the decision must run monitor-free")
                .that(probeCompleted).isTrue();

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(handler.contexts).isNotEmpty());
        assertThat(parallelConsumer.isClosedOrFailed()).isFalse();

        settleBeforeTeardown(healed);
    }

    /**
     * Covers AE7: a genuine poller death - the broker-poll thread dying of something that is not budget exhaustion -
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
        var handler = new RecordingHandler(CommitFailureDecision.CONTINUE);
        startPc(SMALL_BUDGET, handler);
        addRecordsAndProcess();

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(parallelConsumer.isClosedOrFailed()).isTrue());

        assertThat(handler.contexts).isEmpty();
        Exception failureCause = parallelConsumer.getFailureCause();
        assertThat(failureCause).isNotNull();
        assertThat(chainWithSuppressed(failureCause).stream()
                .anyMatch(t -> String.valueOf(t.getMessage()).contains(pollerFailureMessage))).isTrue();
    }

    /**
     * Covers AE6: a non-retriable commit failure (authorization) stays immediately fatal and handler-free - the
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
        var handler = new RecordingHandler(CommitFailureDecision.CONTINUE);
        startPc(SMALL_BUDGET, handler);
        addRecordsAndProcess();

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(parallelConsumer.isClosedOrFailed()).isTrue());

        assertThat(handler.contexts).isEmpty();
        Exception failureCause = parallelConsumer.getFailureCause();
        assertThat(failureCause).isNotNull();
        assertThat(chainWithSuppressed(failureCause).stream()
                .anyMatch(t -> t instanceof TopicAuthorizationException)).isTrue();
    }

    /**
     * Both budget lanes are ONE exhaustion event (KTD7): the SASL authentication budget
     * ({@code saslAuthenticationRetryTimeout}, zero by default so it exhausts on the first failure) reaches the
     * handler exactly as the offset-commit budget does.
     */
    @Test
    void saslBudgetExhaustionReachesTheHandlerAsTheSameEvent() {
        var healed = new AtomicBoolean(false);
        // polls stay healthy - only committing hits the auth failure, so only the commit-side SASL budget is in play
        mockConsumer = consumerWithFailingCommits(
                () -> new SaslAuthenticationException("Invalid username or password (mocking)"), healed);
        var handler = new RecordingHandler(CommitFailureDecision.CONTINUE);
        startPc(Duration.ofSeconds(30), handler); // generous offset-commit budget: it must NOT be the one that fires
        addRecordsAndProcess();

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(handler.contexts).isNotEmpty());

        var context = handler.contexts.peek();
        assertThat(context.getFailure()).isInstanceOf(OffsetCommitBudgetExceededException.class);
        assertThat(context.getFailure().getCause()).isInstanceOf(SaslAuthenticationException.class);
        assertThat(context.getFailure().getMessage()).contains("saslAuthenticationRetryTimeout");
        assertThat(parallelConsumer.isClosedOrFailed()).isFalse();

        settleBeforeTeardown(healed);
    }

    /**
     * KTD2's affirmative wait, the precondition for the whole seam: the waiter in {@code commitAndWait} no longer
     * dies on its own {@code offsetCommitTimeout} clock while the poll side is still legitimately spending the same
     * budget on a later clock. A commit attempt held open well past the budget still ends in the handler firing -
     * never in a {@code "Timeout waiting for commit response"} killing the instance first.
     */
    @Test
    void theWaiterOutlivesACommitAttemptHeldOpenPastTheOldDeadline() throws InterruptedException {
        final Duration budget = Duration.ofSeconds(1);
        var commitEntered = new CountDownLatch(1);
        var commitHold = new CountDownLatch(1);
        var healed = new AtomicBoolean(false);
        var commitCalls = new AtomicInteger();
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
            @Override
            // deliberately NOT synchronized: this blocks, and holding the MockConsumer monitor while blocked
            // would park the poll and teardown paths too - see MockConsumerTestBase#addRecordsInBackground
            public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
                if (commitCalls.incrementAndGet() == 1) {
                    commitEntered.countDown();
                    try {
                        // the held-open attempt - released by the test, bounded so a failure cannot wedge
                        commitHold.await(30, SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    throw new TimeoutException("mock commit timeout after being held open");
                }
                if (!healed.get()) {
                    sleepOrFail(FAILING_COMMIT_PACING, "Interrupted while pacing a failing commit");
                    throw new TimeoutException("mock commit timeout");
                }
                super.commitSync(offsets);
            }
        };
        var handler = new RecordingHandler(CommitFailureDecision.CONTINUE);
        startPc(budget, handler);
        addRecordsAndProcess();

        assertWithMessage("the commit attempt never started")
                .that(commitEntered.await(30, SECONDS)).isTrue();

        // Hold the attempt open for several times the budget. Under the old code the WAITER's own deadline (the
        // same offsetCommitTimeout, on an earlier clock) fired during this window and killed the instance with
        // "Timeout waiting for commit response". A lower-bound wait, not timing arithmetic: holding LONGER only
        // strengthens the scenario, and the elapsed assertion below verifies the window was actually crossed.
        sleepOrFail(budget.multipliedBy(3), "Interrupted while holding the commit attempt open");
        healed.set(true);
        commitHold.countDown();

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(handler.contexts).isNotEmpty());

        // the exhaustion the handler saw really is the held-open one: its elapsed spans the hold window,
        // which is far past the old waiter deadline of one offsetCommitTimeout
        assertThat(handler.contexts.peek().getElapsed().toMillis()).isAtLeast(budget.multipliedBy(3).toMillis());

        // and the healed broker then receives the still-dirty offsets - the instance survived the whole episode
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var committed = mockConsumer.committed(Collections.singleton(TOPIC_PARTITION)).get(TOPIC_PARTITION);
            assertThat(committed).isNotNull();
            assertThat(committed.offset()).isEqualTo(RECORDS);
        });

        assertThat(parallelConsumer.getFailureCause()).isNull();
        assertThat(parallelConsumer.isClosedOrFailed()).isFalse();
    }

    /**
     * Once close has begun the handler is never consulted (KTD7): a commit failing during the close sequence keeps
     * its historical handler-free disposition, and the close itself completes rather than wedging behind a decision
     * nobody can act on.
     */
    @Test
    void closeBegunStaysHandlerFree() {
        mockConsumer = consumerWithFailingCommits(() -> new TimeoutException("mock commit timeout"), null);
        var handler = new RecordingHandler(CommitFailureDecision.CONTINUE);
        // commit interval much longer than the test's close step, so no second scheduled exhaustion can race the
        // close and blur the count below
        startPc(Duration.ofMillis(500), Duration.ofSeconds(5), handler);
        addRecordsAndProcess();

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(handler.contexts).hasSize(1));

        parallelConsumer.closeDontDrainFirst();

        // the close sequence's own final commit also exhausted its budget (commits never heal here), and it did so
        // handler-free: the invocation count is unchanged
        assertThat(handler.contexts).hasSize(1);
        assertThat(parallelConsumer.isClosedOrFailed()).isTrue();
        assertThat(parallelConsumer.getFailureCause()).isNull();
    }

    /**
     * Covers AE8, the {@link CommitFailureContinueMode#PAUSE_INTAKE} half of a CONTINUE decision (KTD5): while
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
        mockConsumer = consumerWithFailingCommits(() -> new TimeoutException("mock commit timeout"), commitsHealthy);
        var handler = new RecordingHandler(CommitFailureDecision.CONTINUE);
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
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(handler.contexts).isNotEmpty());

        // in-flight work is NOT gated: the held record completes while commits are still failing
        inFlightHold.countDown();
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(processedRecords).hasSize(RECORDS + 2));

        // new work IS gated: these records arrive in the work manager but are never drawn
        addRecords(heldOffset + 2, 3); // offsets 7..9
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(parallelConsumer.workRemaining()).isEqualTo(3));
        int exhaustionsBeforeProbe = handler.contexts.size();
        // a further exhaustion is the positive signal that full control-loop cycles passed with the pause active
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(handler.contexts.size()).isAtLeast(exhaustionsBeforeProbe + 1));
        assertWithMessage("no NEW work may be drawn while the seam pause is active")
                .that(processedRecords).hasSize(RECORDS + 2);

        // the next successful commit releases the pause: intake resumes with no user action
        commitsHealthy.set(true);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(processedRecords).hasSize(RECORDS + 5));
        awaitCommittedOffset(RECORDS + 5);
        assertThat(parallelConsumer.isClosedOrFailed()).isFalse();
        assertThat(parallelConsumer.getFailureCause()).isNull();
    }

    /**
     * The control arm for {@link #pauseIntakeStopsNewWorkCompletesInFlightAndResumesAfterCommitSuccess}: under the
     * default {@link CommitFailureContinueMode#KEEP_PROCESSING}, a CONTINUE decision gates nothing - new work keeps
     * flowing while commits fail, exactly as U2 left it.
     */
    @Test
    void keepProcessingModeKeepsDrawingNewWorkWhileCommitsFail() {
        var healed = new AtomicBoolean(false);
        mockConsumer = consumerWithFailingCommits(() -> new TimeoutException("mock commit timeout"), healed);
        var handler = new RecordingHandler(CommitFailureDecision.CONTINUE);
        startPc(SMALL_BUDGET, handler); // KEEP_PROCESSING is the builder default
        addRecordsAndProcess();

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(handler.contexts).isNotEmpty());

        // while commits are still failing (healed is untouched), new work is drawn and processed
        addRecords(RECORDS, 3);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(processedRecords).hasSize(RECORDS + 3));
        assertThat(parallelConsumer.isClosedOrFailed()).isFalse();

        healed.set(true);
        awaitCommittedOffset(RECORDS + 3);
    }

    /**
     * Composition with the user's own pause, direction 1 (KTD5): the seam's release must never resume a user
     * {@code pauseIfRunning()}. After the broker heals and the seam pause releases, intake stays stopped until the
     * user's own {@code resumeIfPaused()} - which then restores flow, proving the seam flag really was released
     * rather than merely masked by the user pause.
     */
    @Test
    void seamReleaseNeverClearsUserPause() {
        var commitsHealthy = new AtomicBoolean(true);
        mockConsumer = consumerWithFailingCommits(() -> new TimeoutException("mock commit timeout"), commitsHealthy);
        var handler = new RecordingHandler(CommitFailureDecision.CONTINUE);
        startPc(SMALL_BUDGET, handler, CommitFailureContinueMode.PAUSE_INTAKE);
        addRecordsAndProcess();
        awaitCommittedOffset(RECORDS); // opening batch processed and cleanly committed (see the AE8 scenario)

        // break commits and drive one record through: the exhaustion engages the seam pause
        commitsHealthy.set(false);
        addRecords(RECORDS, 1);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(handler.contexts).isNotEmpty());

        // both pause axes now active: the seam's (from the exhaustion) and the user's
        parallelConsumer.pauseIfRunning();
        addRecords(RECORDS + 1, 3);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(parallelConsumer.workRemaining()).isEqualTo(3));

        // healing lets the next cadence commit succeed, which releases the SEAM pause only
        commitsHealthy.set(true);
        awaitCommittedOffset(RECORDS + 1);
        assertWithMessage("the seam's release must not resume intake while the user's own pause holds")
                .that(processedRecords).hasSize(RECORDS + 1);

        // the user's resume restores flow - which also proves the seam flag was genuinely released above
        parallelConsumer.resumeIfPaused();
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(processedRecords).hasSize(RECORDS + 4));
        awaitCommittedOffset(RECORDS + 4);
        assertThat(parallelConsumer.isClosedOrFailed()).isFalse();
    }

    /**
     * Composition with the user's own pause, direction 2 (KTD5): the user's {@code resumeIfPaused()} must never
     * clear the seam's pause. With the seam pause active and no user pause set, the resume call is a no-op - intake
     * stays stopped until a commit actually succeeds.
     */
    @Test
    void userResumeIsANoOpOnTheSeamPause() {
        var commitsHealthy = new AtomicBoolean(true);
        mockConsumer = consumerWithFailingCommits(() -> new TimeoutException("mock commit timeout"), commitsHealthy);
        var handler = new RecordingHandler(CommitFailureDecision.CONTINUE);
        startPc(SMALL_BUDGET, handler, CommitFailureContinueMode.PAUSE_INTAKE);
        addRecordsAndProcess();
        awaitCommittedOffset(RECORDS); // opening batch processed and cleanly committed (see the AE8 scenario)

        // break commits and drive one record through: the exhaustion engages the seam pause
        commitsHealthy.set(false);
        addRecords(RECORDS, 1);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(handler.contexts).isNotEmpty());

        // the state is RUNNING, not PAUSED, so this is a no-op - and it must not touch the seam's flag
        parallelConsumer.resumeIfPaused();

        addRecords(RECORDS + 1, 3);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(parallelConsumer.workRemaining()).isEqualTo(3));
        int exhaustionsBeforeProbe = handler.contexts.size();
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(handler.contexts.size()).isAtLeast(exhaustionsBeforeProbe + 1));
        assertWithMessage("intake must stay stopped after a user resume that had no user pause to clear")
                .that(processedRecords).hasSize(RECORDS + 1);

        // only a successful commit releases the seam pause
        commitsHealthy.set(true);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(processedRecords).hasSize(RECORDS + 4));
        awaitCommittedOffset(RECORDS + 4);
        assertThat(parallelConsumer.isClosedOrFailed()).isFalse();
    }

    /**
     * During DRAINING the close path wins (KTD5): a seam pause that is active when {@code closeDrainFirst()} is
     * called does not gate the drain - the records it was holding back are drawn, processed and the close completes,
     * rather than the drain deadlocking behind a pause whose release condition (a successful commit) never arrives
     * (commits never heal here). Without the gate's DRAINING exemption this scenario hangs the drain and the close
     * times out red.
     * <p>
     * The 5s commit interval (the {@link #closeBegunStaysHandlerFree()} device) keeps the drain window commit-free:
     * the CONTINUE decision resets the cadence, so for seconds after the exhaustion no scheduled commit can race the
     * drain - neither blurring what released the records, nor aborting the drain early through the shutdown guard's
     * fatal route. The close sequence's own final commit still exhausts, and stays handler-free per
     * {@link #closeBegunStaysHandlerFree()} - the invocation count ends where it was before the close.
     */
    @Test
    void activeSeamPauseDoesNotGateTheDrain() {
        var commitsHealthy = new AtomicBoolean(true);
        mockConsumer = consumerWithFailingCommits(() -> new TimeoutException("mock commit timeout"), commitsHealthy);
        var handler = new RecordingHandler(CommitFailureDecision.CONTINUE);
        startPc(SMALL_BUDGET, Duration.ofSeconds(5), handler, CommitFailureContinueMode.PAUSE_INTAKE);
        addRecordsAndProcess();
        awaitCommittedOffset(RECORDS); // the first commit fires immediately (no previous commit) and is clean

        // break commits for good and drive one record through: the partition turns dirty, and the next cadence
        // (one interval after the clean commit) exhausts and engages the seam pause
        commitsHealthy.set(false);
        addRecords(RECORDS, 1);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(handler.contexts).isNotEmpty());
        int exhaustionsBeforeClose = handler.contexts.size();

        // gated records must be aboard BEFORE the close: DRAINING pauses the subscription, so anything not yet
        // polled would never arrive - and "drained" would be indistinguishable from "never fetched"
        addRecords(RECORDS + 1, 3);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(parallelConsumer.workRemaining()).isEqualTo(3));
        assertWithMessage("the seam pause must be holding the new records back before the close begins")
                .that(processedRecords).hasSize(RECORDS + 1);

        parallelConsumer.closeDrainFirst();

        // close wins over the seam pause: the drain drew and processed the gated records
        assertWithMessage("the drain must draw the records the seam pause was holding back")
                .that(processedRecords).hasSize(RECORDS + 4);
        assertWithMessage("close-time commit failures stay handler-free (KTD7) - no decision during the close")
                .that(handler.contexts).hasSize(exhaustionsBeforeClose);
        assertThat(parallelConsumer.isClosedOrFailed()).isTrue();
    }

    // ---------------------------------------------------------------------------------------------------------
    // harness
    // ---------------------------------------------------------------------------------------------------------

    /**
     * Heals the broker and waits for the dirty offsets to land, so a scenario that leaves commits failing forever
     * does not hand teardown a close that races an in-flight exhaustion. That race is not a defect - once close has
     * begun, an exhaustion keeps its historical fatal route, handler-free (see
     * {@link #closeBegunStaysHandlerFree()}) - but for these scenarios it is noise: their subject ends before the
     * close. Once the committed offset is observed, no further exhaustion can be in flight (decisions and commits
     * are serialized on the control thread, and healed commits cannot exhaust), so the teardown close is clean.
     */
    private void settleBeforeTeardown(AtomicBoolean healed) {
        healed.set(true);
        awaitCommittedOffset(RECORDS);
    }

    /** Awaits the mock broker having the given committed offset for the test partition. */
    private void awaitCommittedOffset(long expectedOffset) {
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var committed = mockConsumer.committed(Collections.singleton(TOPIC_PARTITION)).get(TOPIC_PARTITION);
            assertThat(committed).isNotNull();
            assertThat(committed.offset()).isEqualTo(expectedOffset);
        });
    }

    /**
     * A handler that records every context it is given (write-order: the context is added BEFORE returning, so a
     * test awaiting {@code contexts} reads fully published values) and returns a fixed decision.
     */
    private static class RecordingHandler implements CommitFailureHandler {

        final ConcurrentLinkedQueue<CommitFailureContext> contexts = new ConcurrentLinkedQueue<>();

        private final CommitFailureDecision decision;

        private RecordingHandler(CommitFailureDecision decision) {
            this.decision = decision;
        }

        @Override
        public CommitFailureDecision onCommitFailure(CommitFailureContext context) {
            log.info("Handler invoked: consecutive={}, attempts={}, elapsed={}",
                    context.getConsecutiveExhaustedBudgets(), context.getAttemptsMade(), context.getElapsed());
            contexts.add(context);
            return decision;
        }
    }

    /**
     * A consumer whose {@code commitSync} fails with the supplied exception (paced, so budget loops retry a
     * handful of times rather than hot-spinning) until {@code healed} flips true; polls stay healthy throughout.
     *
     * @param healed when non-null and set, commits succeed again; when null, commits fail forever
     */
    private MockConsumer<String, String> consumerWithFailingCommits(Supplier<RuntimeException> failure,
                                                                    AtomicBoolean healed) {
        return new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
            @Override
            // deliberately NOT synchronized on the failing path: it sleeps, and holding the MockConsumer monitor
            // while sleeping parks the poll and teardown paths - see MockConsumerTestBase#addRecordsInBackground
            public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
                if (healed == null || !healed.get()) {
                    sleepOrFail(FAILING_COMMIT_PACING, "Interrupted while pacing a failing commit");
                    throw failure.get();
                }
                super.commitSync(offsets); // synchronized in the superclass
            }
        };
    }

    private void startPc(Duration offsetCommitTimeout, CommitFailureHandler handler) {
        startPc(offsetCommitTimeout, Duration.ofMillis(100), handler);
    }

    private void startPc(Duration offsetCommitTimeout, CommitFailureHandler handler, CommitFailureContinueMode mode) {
        startPc(offsetCommitTimeout, Duration.ofMillis(100), handler, mode);
    }

    private void startPc(Duration offsetCommitTimeout, Duration commitInterval, CommitFailureHandler handler) {
        startPc(offsetCommitTimeout, commitInterval, handler, CommitFailureContinueMode.KEEP_PROCESSING);
    }

    private void startPc(Duration offsetCommitTimeout, Duration commitInterval, CommitFailureHandler handler,
                         CommitFailureContinueMode mode) {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(mockConsumer)
                .commitMode(ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC) // the only mode with the budget today
                .commitInterval(commitInterval)
                .offsetCommitTimeout(offsetCommitTimeout)
                .commitFailureHandler(handler)
                .commitFailureContinueMode(mode)
                .build();
        parallelConsumer = new ParallelEoSStreamProcessor<>(options);
        parallelConsumer.subscribe(of(TOPIC));

        // MockConsumer is not a correct implementation of the Consumer contract - the partition must be rebalanced
        // in by hand and PC told separately, per MockConsumerTestBase
        mockConsumer.rebalance(of(TOPIC_PARTITION));
        parallelConsumer.onPartitionsAssigned(of(TOPIC_PARTITION));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(TOPIC_PARTITION, 0L));
    }

    private void addRecordsAndProcess() {
        addRecords(0, RECORDS);
        startProcessing();
    }

    /** Adds {@code count} single-key records starting at {@code fromOffset}. */
    private void addRecords(long fromOffset, int count) {
        for (long offset = fromOffset; offset < fromOffset + count; offset++) {
            mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, TOPIC_PARTITION.partition(), offset, "key",
                    "value-" + offset));
        }
    }

    private void startProcessing() {
        parallelConsumer.poll(recordContexts -> recordContexts.forEach(recordContext -> {
            log.info("Processing: {}", recordContext);
            processedRecords.add(recordContext);
        }));
    }

    /**
     * Like {@link #startProcessing()}, but the record at {@code heldOffset} signals {@code entered} and then awaits
     * {@code hold} before completing - a controllable, observable in-flight record. Bounded, so a test failure
     * cannot wedge a worker thread forever.
     */
    private void startProcessingHoldingAt(long heldOffset, CountDownLatch entered, CountDownLatch hold) {
        parallelConsumer.poll(recordContexts -> recordContexts.forEach(recordContext -> {
            if (recordContext.offset() == heldOffset) {
                entered.countDown();
                try {
                    boolean released = hold.await(30, SECONDS);
                    if (!released) {
                        throw new FakeRuntimeException("the held record was never released - test failure");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            log.info("Processing: {}", recordContext);
            processedRecords.add(recordContext);
        }));
    }

    private static List<Throwable> causeChain(Throwable throwable) {
        List<Throwable> chain = new ArrayList<>();
        for (Throwable t = throwable; t != null && !chain.contains(t); t = t.getCause()) {
            chain.add(t);
        }
        return chain;
    }

    /** The cause chain plus, flattened in, every element's suppressed exceptions and their causes. */
    private static List<Throwable> chainWithSuppressed(Throwable throwable) {
        return causeChain(throwable).stream()
                .flatMap(t -> Stream.concat(Stream.of(t),
                        java.util.Arrays.stream(t.getSuppressed()).flatMap(s -> causeChain(s).stream())))
                .collect(java.util.stream.Collectors.toList());
    }
}
