package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.CommitFailureHandler.CommitFailureDecision;
import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitFailureContinueMode;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.awaitility.Awaitility;
import org.awaitility.core.ThrowingRunnable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static bz.stub.parallelconsumer.internal.utils.ThreadUtils.sleepOrFail;
import static com.google.common.truth.Truth.assertThat;
import static java.util.concurrent.TimeUnit.SECONDS;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * The mechanics shared by the commit-failure seam scenarios (astubbs#317, confluentinc#833): a
 * {@link MockConsumer} whose {@code commitSync} fails on demand, a PC built around it with the seam configured, a
 * recording {@link CommitFailureHandler}, and the waits the scenarios assert through.
 * <p>
 * The seam itself is split across the subclasses by what each one pins, and each states its own slice:
 * <ul>
 *     <li>{@link MockConsumerCommitFailureDecisionTest} - what a decision does end to end, and the contract
 *     around the decision call</li>
 *     <li>{@link MockConsumerCommitFailureHandlerFreeExitsTest} - the four exits that never consult the handler</li>
 *     <li>{@link MockConsumerCommitFailurePauseIntakeTest} - the {@link CommitFailureContinueMode#PAUSE_INTAKE}
 *     half of CONTINUE, and its composition with the user's own pause and with close</li>
 *     <li>{@link MockConsumerCommitFailureDeferralEscalationTest} - the rebalance-deferral lane joining the
 *     seam</li>
 *     <li>{@link MockConsumerCommitFailureMetricsTest} - the meters and the log loudness</li>
 * </ul>
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
// See MockConsumerTestBase's own @Timeout note: the value is SECONDS, and @Timeout is @Inherited, so every
// subclass here gets this bound unless it declares its own.
@Timeout(120)
abstract class MockConsumerCommitFailureSeamTestBase {

    /**
     * One topic name for the whole seam suite. Not a correctness measure - each test builds its own
     * {@link MockConsumer}, so there is no shared broker and no cross-scenario delivery to prevent.
     */
    protected static final String TOPIC = "MockConsumerCommitFailureSeam";

    protected static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, 0);

    protected static final int RECORDS = 5;

    /**
     * Pacing sleep inside a failing mock {@code commitSync}, so a budget's retry loop makes a handful of attempts
     * rather than hot-spinning thousands of log lines. Never held under the {@link MockConsumer} monitor.
     */
    protected static final Duration FAILING_COMMIT_PACING = Duration.ofMillis(20);

    /** Small budget so exhaustion happens quickly; big enough that the paced retries make several attempts. */
    protected static final Duration SMALL_BUDGET = Duration.ofMillis(300);

    /**
     * The deadline every scenario's polling assertion shares. Generous on purpose: these are event waits, not
     * timing measurements - a scenario that needs to prove a window was crossed asserts the elapsed time itself.
     */
    protected static final Duration AWAIT_DEADLINE = Duration.ofSeconds(30);

    /** The retriable commit failure used wherever the scenario does not need a specific failure class. */
    protected static final Supplier<RuntimeException> COMMIT_TIMES_OUT =
            () -> new TimeoutException("mock commit timeout");

    /** The rebalance-deferral failure: this consumer is no longer a member of the group (usually eviction). */
    protected static final Supplier<RuntimeException> COMMIT_IS_DEFERRED =
            MockConsumerCommitFailureSeamTestBase::noLongerAGroupMember;

    protected MockConsumer<String, String> mockConsumer;

    protected ParallelEoSStreamProcessor<String, String> parallelConsumer;

    /**
     * Set by the metrics scenarios BEFORE {@link #startPc}; when null (every other scenario), the options carry no
     * registry and PC's metrics run in their no-op mode, exactly as before the meters existed.
     */
    protected SimpleMeterRegistry meterRegistry;

    /** Records handed to the user function. Concurrent because PC's worker threads write it. */
    protected final ConcurrentLinkedQueue<RecordContext<String, String>> processedRecords =
            new ConcurrentLinkedQueue<>();

    @AfterEach
    void closePc() {
        Awaitility.reset();
        if (parallelConsumer != null && !parallelConsumer.isClosedOrFailed()) {
            parallelConsumer.closeDontDrainFirst();
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // waits
    // ---------------------------------------------------------------------------------------------------------

    /**
     * Polls {@code assertion} until it passes, within {@link #AWAIT_DEADLINE}. The assertion stays in the
     * scenario - it is the point of the test - and only the wait around it is shared.
     */
    protected void awaitAsserted(ThrowingRunnable assertion) {
        Awaitility.await().atMost(AWAIT_DEADLINE).untilAsserted(assertion);
    }

    /** Awaits the mock broker having the given committed offset for the test partition. */
    protected void awaitCommittedOffset(long expectedOffset) {
        awaitAsserted(() -> {
            var committed = mockConsumer.committed(Collections.singleton(TOPIC_PARTITION)).get(TOPIC_PARTITION);
            assertThat(committed).isNotNull();
            assertThat(committed.offset()).isEqualTo(expectedOffset);
        });
    }

    /**
     * Heals the broker and waits for the dirty offsets to land, so a scenario that leaves commits failing forever
     * does not hand teardown a close that races an in-flight exhaustion. That race is not a defect - once close has
     * begun, an exhaustion keeps its historical fatal route, handler-free (see
     * {@link MockConsumerCommitFailureHandlerFreeExitsTest#closeBegunStaysHandlerFree()}) - but for these scenarios
     * it is noise: their subject ends before the close. Once the committed offset is observed, no further
     * exhaustion can be in flight (decisions and commits are serialized on the control thread, and healed commits
     * cannot exhaust), so the teardown close is clean.
     */
    protected void settleBeforeTeardown(AtomicBoolean healed) {
        healed.set(true);
        awaitCommittedOffset(RECORDS);
    }

    // ---------------------------------------------------------------------------------------------------------
    // the failing consumer, and the handler
    // ---------------------------------------------------------------------------------------------------------

    /** A fresh instance of the deferral failure - a new one per attempt, exactly as a broker would raise it. */
    protected static CommitFailedException noLongerAGroupMember() {
        return new CommitFailedException("Commit cannot be completed since the consumer is not part of an "
                + "active group (mocking)");
    }

    /**
     * A consumer whose {@code commitSync} fails with the supplied exception (paced, so budget loops retry a
     * handful of times rather than hot-spinning) until {@code healed} flips true; polls stay healthy throughout.
     *
     * @param healed when non-null and set, commits succeed again; when null, commits fail forever
     */
    protected MockConsumer<String, String> consumerWithFailingCommits(Supplier<RuntimeException> failure,
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

    /** Installs {@link #consumerWithFailingCommits} for the commonest failure, the mock commit timeout. */
    protected void useCommitsTimingOut(AtomicBoolean healed) {
        mockConsumer = consumerWithFailingCommits(COMMIT_TIMES_OUT, healed);
    }

    /** The handler almost every scenario wants: records what it is given, and decides CONTINUE. */
    protected static RecordingHandler continuingHandler() {
        return new RecordingHandler(CommitFailureDecision.CONTINUE);
    }

    /**
     * The opening most CONTINUE scenarios share: a consumer whose commits fail with {@code failure} until
     * {@code healed} flips, a CONTINUE-deciding {@link RecordingHandler}, PC started on the given budget, and the
     * opening batch of {@link #RECORDS} records processing.
     *
     * @return the handler, so the scenario can assert on the decisions it was asked for
     */
    protected RecordingHandler startContinuingPc(Supplier<RuntimeException> failure, AtomicBoolean healed,
                                                 Duration offsetCommitTimeout) {
        mockConsumer = consumerWithFailingCommits(failure, healed);
        var handler = continuingHandler();
        startPc(offsetCommitTimeout, handler);
        addRecordsAndProcess();
        return handler;
    }

    /** {@link #startContinuingPc} on the mock commit timeout and the {@link #SMALL_BUDGET}. */
    protected RecordingHandler startContinuingPc(AtomicBoolean healed) {
        return startContinuingPc(COMMIT_TIMES_OUT, healed, SMALL_BUDGET);
    }

    /**
     * A handler that records every context it is given (write-order: the context is added BEFORE returning, so a
     * test awaiting {@code contexts} reads fully published values) and returns a fixed decision.
     */
    protected static class RecordingHandler implements CommitFailureHandler {

        final ConcurrentLinkedQueue<CommitFailureContext> contexts = new ConcurrentLinkedQueue<>();

        private final CommitFailureDecision decision;

        protected RecordingHandler(CommitFailureDecision decision) {
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

    // ---------------------------------------------------------------------------------------------------------
    // PC lifecycle and the record feed
    // ---------------------------------------------------------------------------------------------------------

    protected void startPc(Duration offsetCommitTimeout, CommitFailureHandler handler) {
        startPc(offsetCommitTimeout, Duration.ofMillis(100), handler);
    }

    protected void startPc(Duration offsetCommitTimeout, CommitFailureHandler handler,
                           CommitFailureContinueMode mode) {
        startPc(offsetCommitTimeout, Duration.ofMillis(100), handler, mode);
    }

    protected void startPc(Duration offsetCommitTimeout, Duration commitInterval, CommitFailureHandler handler) {
        startPc(offsetCommitTimeout, commitInterval, handler, CommitFailureContinueMode.KEEP_PROCESSING);
    }

    protected void startPc(Duration offsetCommitTimeout, Duration commitInterval, CommitFailureHandler handler,
                           CommitFailureContinueMode mode) {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(mockConsumer)
                // the seam's consumer-side lane; the transactional lane has its own budget tests
                .commitMode(ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC)
                .commitInterval(commitInterval)
                .offsetCommitTimeout(offsetCommitTimeout)
                .commitFailureHandler(handler)
                .commitFailureContinueMode(mode)
                .meterRegistry(meterRegistry) // null for every non-metrics scenario - the builder default
                .build();
        parallelConsumer = new ParallelEoSStreamProcessor<>(options);
        parallelConsumer.subscribe(of(TOPIC));

        // MockConsumer is not a correct implementation of the Consumer contract - the partition must be rebalanced
        // in by hand and PC told separately, per MockConsumerTestBase
        mockConsumer.rebalance(of(TOPIC_PARTITION));
        parallelConsumer.onPartitionsAssigned(of(TOPIC_PARTITION));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(TOPIC_PARTITION, 0L));
    }

    protected void addRecordsAndProcess() {
        addRecords(0, RECORDS);
        startProcessing();
    }

    /** Adds {@code count} single-key records starting at {@code fromOffset}. */
    protected void addRecords(long fromOffset, int count) {
        for (long offset = fromOffset; offset < fromOffset + count; offset++) {
            mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, TOPIC_PARTITION.partition(), offset, "key",
                    "value-" + offset));
        }
    }

    protected void startProcessing() {
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
    protected void startProcessingHoldingAt(long heldOffset, CountDownLatch entered, CountDownLatch hold) {
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

    // ---------------------------------------------------------------------------------------------------------
    // exception-chain inspection
    // ---------------------------------------------------------------------------------------------------------

    protected static List<Throwable> causeChain(Throwable throwable) {
        List<Throwable> chain = new ArrayList<>();
        for (Throwable t = throwable; t != null && !chain.contains(t); t = t.getCause()) {
            chain.add(t);
        }
        return chain;
    }

    /** The cause chain plus, flattened in, every element's suppressed exceptions and their causes. */
    protected static List<Throwable> chainWithSuppressed(Throwable throwable) {
        return causeChain(throwable).stream()
                .flatMap(t -> Stream.concat(Stream.of(t),
                        Arrays.stream(t.getSuppressed()).flatMap(s -> causeChain(s).stream())))
                .collect(Collectors.toList());
    }
}
