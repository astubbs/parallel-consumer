package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.CommitFailureHandler.CommitFailureDecision;
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
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var committed = mockConsumer.committed(Collections.singleton(TOPIC_PARTITION)).get(TOPIC_PARTITION);
            assertThat(committed).isNotNull();
            assertThat(committed.offset()).isEqualTo(RECORDS);
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

    private void startPc(Duration offsetCommitTimeout, Duration commitInterval, CommitFailureHandler handler) {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(mockConsumer)
                .commitMode(ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC) // the only mode with the budget today
                .commitInterval(commitInterval)
                .offsetCommitTimeout(offsetCommitTimeout)
                .commitFailureHandler(handler)
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
        for (int offset = 0; offset < RECORDS; offset++) {
            mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, TOPIC_PARTITION.partition(), offset, "key",
                    "value-" + offset));
        }
        parallelConsumer.poll(recordContexts -> recordContexts.forEach(recordContext -> {
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
