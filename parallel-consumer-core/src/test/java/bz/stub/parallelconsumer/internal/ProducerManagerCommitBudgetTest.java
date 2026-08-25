package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.OffsetCommitBudgetExceededException;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.PollContextInternal;
import bz.stub.parallelconsumer.state.WorkManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static bz.stub.parallelconsumer.internal.utils.ThreadUtils.sleepOrFail;
import static com.google.common.truth.Truth.assertThat;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static pl.tlinkowski.unij.api.UniMaps.of;

/**
 * The transactional commit loop must be bounded by the {@code offsetCommitTimeout} <b>budget</b>, not by an
 * attempt count - and on exhaustion it must throw the seam's typed event, {@link
 * OffsetCommitBudgetExceededException}, so the commit-failure seam (astubbs#317, confluentinc#833) can consult
 * the configured handler instead of the control thread dying on a bare {@code InternalRuntimeException}.
 * <p>
 * Before this, {@link ProducerManager#commitOffsets} retried retriable {@code commitTransaction} failures up to
 * a fixed {@code arbitrarilyChosenLimitForArbitraryErrorSituation = 200} attempts - a bound with no time
 * semantics at all (each real attempt blocks up to {@code max.block.ms}, so 200 attempts is over three hours at
 * the default), and a give-up type ({@code InternalRuntimeException}) the seam deliberately does not intercept.
 * The consumer-side committer got the whole-operation budget in astubbs#177 ({@code ConsumerManager#commitSync},
 * pinned by {@link ConsumerManagerCommitRetryBudgetTest}, which this class is modelled on); this is the same
 * semantics arriving on the transactional side.
 * <p>
 * Also pinned here (green before AND after the change - the classifier is characterised, not redefined):
 * non-retriable failures stay fatal and handler-free, {@code sendOffsetsToTransaction} failures stay fatal, and
 * the commit-lock acquisition timeout keeps its own fatal {@code java.util.concurrent.TimeoutException} path.
 * <p>
 * The recovery tests pin KTD8 (complete-else-abort): a budget exhausted mid-{@code commitTransaction} leaves
 * the producer holding an in-flight transaction ({@code sendOffsetsToTransaction} already succeeded), and
 * without recovery the next cycle's {@code beginTransaction}/{@code sendOffsetsToTransaction} meets
 * KafkaProducer's "previous fatal or abortable error" - making the cycle after a CONTINUE decision terminally
 * fatal, one grace cycle instead of a pause window.
 *
 * @see ProducerManager#commitOffsets
 * @see ConsumerManagerCommitRetryBudgetTest
 */
@Slf4j
@Timeout(60)
class ProducerManagerCommitBudgetTest {

    private static final TopicPartition TP = new TopicPartition("ProducerManagerCommitBudgetTest", 0);

    private static final ConsumerGroupMetadata GROUP_METADATA = new ConsumerGroupMetadata("pm-commit-budget-test");

    /**
     * Deliberately 20x {@link #ATTEMPT_DURATION} rather than the 5x a smaller budget would give - the same
     * load-dilation reasoning as {@link ConsumerManagerCommitRetryBudgetTest}'s identical constant: the lower
     * bound (at least 2 attempts) only fails if the FIRST attempt stretches past the whole budget, so 20x means
     * that needs a 20-fold dilation rather than a five-fold one.
     */
    private static final Duration COMMIT_BUDGET = ofSeconds(2);

    /** Each failing attempt burns this much of the budget. */
    private static final Duration ATTEMPT_DURATION = ofMillis(100);

    /**
     * Escape hatch so an unbounded loop <b>fails</b> rather than hangs: past this, the mock starts succeeding.
     * Far more attempts than the budget can pay for, so reaching it means the budget was never enforced - and
     * with the pre-change 200-attempt loop, the mock relenting turns the expected exception into a pass-through
     * commit, which is exactly how this test was red before the change (the control arm).
     */
    private static final int ATTEMPTS_BEFORE_MOCK_RELENTS = 50;

    /**
     * Generous ceiling on {@code COMMIT_BUDGET / ATTEMPT_DURATION} (= 20). Only an upper bound, so a loaded
     * machine taking longer per attempt just uses fewer of them - it cannot flake THIS bound.
     */
    private static final int ATTEMPT_LIMIT = 40;

    private static final Map<TopicPartition, OffsetAndMetadata> OFFSETS = of(TP, new OffsetAndMetadata(1L));

    private Producer<String, String> producer;

    private ProducerWrapper<String, String> producerWrapper;

    private ProducerManager<String, String> producerManager;

    /**
     * A real {@link ProducerManager} over a Mockito producer.
     * <p>
     * {@code isTransactionCompleting} is pinned {@code true} by default because it is what a REAL timed-out
     * {@code commitTransaction} looks like: the producer's {@code TransactionManager} stays in its committing
     * state, so the loop's retry branch actually retries. (Reflection cannot answer it for a Mockito mock, and
     * with {@code false} the legacy loop's other branch declares the commit successful without attempting it -
     * the "tx completed between interrupt and retry" assumption.)
     */
    private void build(ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> optionsBuilder) {
        producer = mock(Producer.class);
        var options = optionsBuilder
                .consumer(mock(Consumer.class))
                .producer(producer)
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                .build();
        producerWrapper = spy(new ProducerWrapper<>(options, true, producer));
        doReturn(true).when(producerWrapper).isTransactionCompleting();
        producerManager = new ProducerManager<>(producerWrapper, mock(ConsumerManager.class),
                mock(WorkManager.class), options);
    }

    private void buildWithBudget(Duration commitBudget) {
        build(ParallelConsumerOptions.<String, String>builder().offsetCommitTimeout(commitBudget));
    }

    /** A failing-attempt answer: burn {@link #ATTEMPT_DURATION}, then fail retriably - until told to relent. */
    private void commitTransactionFailsRetriablyWhile(AtomicBoolean failing, AtomicInteger attempts) {
        doAnswer(invocation -> {
            if (!failing.get() || attempts.incrementAndGet() > ATTEMPTS_BEFORE_MOCK_RELENTS) {
                if (failing.get()) {
                    log.warn("Mock relenting after {} attempts - the retry budget was never enforced",
                            attempts.get());
                }
                return null; // commit succeeds
            }
            sleepOrFail(ATTEMPT_DURATION, "Interrupted while mocking a slow transactional commit");
            throw new TimeoutException("Broker unreachable (mocking)");
        }).when(producer).commitTransaction();
    }

    @Test
    void commitTransactionGivesUpOnceTheWholeOffsetCommitTimeoutIsSpent() throws Exception {
        buildWithBudget(COMMIT_BUDGET);
        var attempts = new AtomicInteger();
        commitTransactionFailsRetriablyWhile(new AtomicBoolean(true), attempts);

        producerManager.preAcquireOffsetsToCommit();
        var thrown = assertThrows(OffsetCommitBudgetExceededException.class,
                () -> producerManager.commitOffsets(OFFSETS, GROUP_METADATA),
                "a permanently timing-out transactional commit must surface the seam's typed failure once the "
                        + "budget is spent - not retry to a fixed attempt count, and not throw the seam-invisible "
                        + "InternalRuntimeException");

        // the broker's own exception is never discarded - it is the cause
        assertThat(thrown).hasCauseThat().isInstanceOf(TimeoutException.class);

        // and the message has to be actionable: which budget ran out, and whose decision comes next
        assertThat(thrown).hasMessageThat().contains("offsetCommitTimeout");
        assertThat(thrown).hasMessageThat().contains(COMMIT_BUDGET.toString());
        assertThat(thrown).hasMessageThat().contains("commitFailureHandler");
        assertThat(thrown).hasMessageThat().contains("astubbs/parallel-consumer#317");

        // the seam's context fields are carried
        assertThat(thrown.getAttemptsMade()).isAtLeast(2L); // it must still RETRY - one try is the other failure
        assertThat(thrown.getAttemptsMade()).isAtMost((long) ATTEMPT_LIMIT);
        assertThat(thrown.getElapsed()).isAtLeast(COMMIT_BUDGET);
        assertThat(thrown.getOffsets()).containsExactly(TP, new OffsetAndMetadata(1L));

        // the control arm on the old fixed-count loop: 200 attempts would have sailed past the mock's relent
        // point and committed, and past ATTEMPT_LIMIT long before that
        assertThat(attempts.get()).isAtMost(ATTEMPT_LIMIT);
    }

    @Test
    void nonRetriableCommitTransactionFailureStaysFatalAndHandlerFree() throws Exception {
        buildWithBudget(COMMIT_BUDGET);
        doThrow(new KafkaException("Cannot execute transactional method because we are in an error state (mocking)"))
                .when(producer).commitTransaction();

        producerManager.preAcquireOffsetsToCommit();
        var thrown = assertThrows(KafkaException.class,
                () -> producerManager.commitOffsets(OFFSETS, GROUP_METADATA));

        // NOT the budget event: the classifier ("Only catch and retry the retriable ones") is unchanged
        assertThat(thrown).isNotInstanceOf(OffsetCommitBudgetExceededException.class);
        verify(producer, times(1)).commitTransaction(); // fail fast - no retry burnt on a terminal failure
    }

    @Test
    void producerFencedOnSendOffsetsStaysFatalAndNeverReachesTheCommitLoop() throws Exception {
        buildWithBudget(COMMIT_BUDGET);
        doThrow(new ProducerFencedException("another producer with the same transactional.id is active (mocking)"))
                .when(producer).sendOffsetsToTransaction(anyMap(), any(ConsumerGroupMetadata.class));

        producerManager.preAcquireOffsetsToCommit();
        assertThrows(InternalRuntimeException.class,
                () -> producerManager.commitOffsets(OFFSETS, GROUP_METADATA));

        verify(producer, never()).commitTransaction();
    }

    @Test
    void commitLockAcquisitionTimeoutKeepsItsOwnFatalTimeoutPath() throws Exception {
        build(ParallelConsumerOptions.<String, String>builder()
                .offsetCommitTimeout(COMMIT_BUDGET)
                .commitLockAcquisitionTimeout(ofMillis(200)));

        // hold the produce (read) side of the transaction lock on another thread, so the commit (write) side
        // cannot be acquired - the deliberately fatal path the budget must NOT absorb
        ExecutorService lockHolder = Executors.newSingleThreadExecutor();
        try {
            lockHolder.submit(() -> producerManager.beginProducing(mock(PollContextInternal.class))).get();

            var thrown = assertThrows(java.util.concurrent.TimeoutException.class,
                    () -> producerManager.preAcquireOffsetsToCommit());
            assertThat(thrown).hasMessageThat().contains("commit lock");
        } finally {
            lockHolder.shutdownNow();
        }
    }

    @Test
    void recoveryCompletesAStillCommittingTransactionInsteadOfAborting() throws Exception {
        buildWithBudget(COMMIT_BUDGET);
        var failing = new AtomicBoolean(true);
        commitTransactionFailsRetriablyWhile(failing, new AtomicInteger());

        // cycle 1: the budget exhausts mid-commitTransaction, transaction left in flight
        producerManager.preAcquireOffsetsToCommit();
        assertThrows(OffsetCommitBudgetExceededException.class,
                () -> producerManager.commitOffsets(OFFSETS, GROUP_METADATA));
        producerManager.postCommit(); // what AbstractOffsetCommitter's finally does after the failure

        // between cycles the outage heals; the transaction is still reported as completing
        failing.set(false);
        clearInvocations(producer);

        // cycle 2 must recover by COMPLETING the stuck transaction, then run an ordinary fresh-transaction commit
        producerManager.preAcquireOffsetsToCommit();
        producerManager.commitOffsets(OFFSETS, GROUP_METADATA);
        producerManager.postCommit();

        verify(producer, never()).abortTransaction(); // completion is preferred - it loses nothing
        var inOrder = inOrder(producer);
        inOrder.verify(producer).commitTransaction(); // the recovery: the exhausted transaction finally lands
        inOrder.verify(producer).beginTransaction(); // then a FRESH transaction for this cycle's commit
        inOrder.verify(producer).sendOffsetsToTransaction(anyMap(), any(ConsumerGroupMetadata.class));
        inOrder.verify(producer).commitTransaction();
    }

    @Test
    void recoveryAbortsAnUnfinishableTransactionThenBeginsFresh() throws Exception {
        buildWithBudget(COMMIT_BUDGET);
        var completing = new AtomicBoolean(true);
        doAnswer(invocation -> completing.get()).when(producerWrapper).isTransactionCompleting();
        var failing = new AtomicBoolean(true);
        commitTransactionFailsRetriablyWhile(failing, new AtomicInteger());

        // cycle 1: exhaust the budget mid-commitTransaction
        producerManager.preAcquireOffsetsToCommit();
        assertThrows(OffsetCommitBudgetExceededException.class,
                () -> producerManager.commitOffsets(OFFSETS, GROUP_METADATA));
        producerManager.postCommit();

        // the producer no longer reports the transaction as completing - it is unfinishable, only abort remains
        completing.set(false);
        failing.set(false); // a fresh transaction's commit would succeed
        clearInvocations(producer);

        // cycle 2 must recover by ABORTING, then run an ordinary fresh-transaction commit - the exhausted
        // commit's offsets were never marked committed, so they stay dirty and recommit with this cycle
        producerManager.preAcquireOffsetsToCommit();
        producerManager.commitOffsets(OFFSETS, GROUP_METADATA);
        producerManager.postCommit();

        var inOrder = inOrder(producer);
        inOrder.verify(producer).abortTransaction();
        inOrder.verify(producer).beginTransaction();
        inOrder.verify(producer).sendOffsetsToTransaction(anyMap(), any(ConsumerGroupMetadata.class));
        inOrder.verify(producer).commitTransaction(); // exactly one: no completion attempt on an unfinishable tx
        verify(producer, times(1)).abortTransaction();
        verify(producer, times(1)).commitTransaction();
    }

    /**
     * An outage outlasting many budgets: recovery that cannot finish within its cycle's budget is ITSELF the
     * seam's budget event - so the handler is re-consulted and a CONTINUE decision keeps the recovery pending
     * for the next cycle, rather than the second cycle turning fatal (AE3's survival property, at mock level).
     */
    @Test
    void recoveryThatExhaustsItsOwnBudgetStaysPendingAndResumesNextCycle() throws Exception {
        buildWithBudget(COMMIT_BUDGET);
        var failing = new AtomicBoolean(true);
        commitTransactionFailsRetriablyWhile(failing, new AtomicInteger());

        // cycle 1: exhaust
        producerManager.preAcquireOffsetsToCommit();
        assertThrows(OffsetCommitBudgetExceededException.class,
                () -> producerManager.commitOffsets(OFFSETS, GROUP_METADATA));
        producerManager.postCommit();

        clearInvocations(producer);

        // cycle 2: the outage persists - recovery's completion attempts exhaust this cycle's budget too
        producerManager.preAcquireOffsetsToCommit();
        var secondExhaustion = assertThrows(OffsetCommitBudgetExceededException.class,
                () -> producerManager.commitOffsets(OFFSETS, GROUP_METADATA),
                "an unrecoverable-so-far transaction must re-surface as the seam's typed event, not fatality");
        producerManager.postCommit();

        assertThat(secondExhaustion).hasCauseThat().isInstanceOf(TimeoutException.class);
        assertThat(secondExhaustion).hasMessageThat().contains("offsetCommitTimeout");
        // recovery never finished, so cycle 2 must not have begun a fresh transaction or re-sent offsets
        verify(producer, never()).beginTransaction();
        verify(producer, never()).sendOffsetsToTransaction(anyMap(), any(ConsumerGroupMetadata.class));
        verify(producer, never()).abortTransaction(); // still reported completing, so abort was never right

        // cycle 3: the outage heals - the pending recovery resumes and the cycle commits normally
        failing.set(false);
        clearInvocations(producer);
        producerManager.preAcquireOffsetsToCommit();
        producerManager.commitOffsets(OFFSETS, GROUP_METADATA);
        producerManager.postCommit();
        var inOrder = inOrder(producer);
        inOrder.verify(producer).commitTransaction(); // recovery completes the long-stuck transaction
        inOrder.verify(producer).beginTransaction();
        inOrder.verify(producer).commitTransaction(); // and this cycle's own commit lands
    }

    /** R6: a transaction PC can neither complete nor abort is terminal, and deliberately handler-free. */
    @Test
    void recoveryAbortFailureStaysFatalAndHandlerFree() throws Exception {
        buildWithBudget(COMMIT_BUDGET);
        var completing = new AtomicBoolean(true);
        doAnswer(invocation -> completing.get()).when(producerWrapper).isTransactionCompleting();
        commitTransactionFailsRetriablyWhile(new AtomicBoolean(true), new AtomicInteger());
        doThrow(new ProducerFencedException("fenced during abort (mocking)")).when(producer).abortTransaction();

        // cycle 1: exhaust
        producerManager.preAcquireOffsetsToCommit();
        assertThrows(OffsetCommitBudgetExceededException.class,
                () -> producerManager.commitOffsets(OFFSETS, GROUP_METADATA));
        producerManager.postCommit();

        completing.set(false); // unfinishable - recovery goes to abort, which is fenced

        producerManager.preAcquireOffsetsToCommit();
        var thrown = assertThrows(ProducerFencedException.class,
                () -> producerManager.commitOffsets(OFFSETS, GROUP_METADATA));
        producerManager.postCommit();

        assertThat(thrown).isNotInstanceOf(OffsetCommitBudgetExceededException.class);
    }
}
