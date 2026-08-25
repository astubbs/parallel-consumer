package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.CommitFailureHandler.CommitFailureDecision;
import bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static bz.stub.parallelconsumer.internal.utils.ThreadUtils.sleepOrFail;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.parallel.ResourceAccessMode.READ_WRITE;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * The decision itself, on the sync consumer path (astubbs#317, confluentinc#833): a commit that exhausts its retry
 * budget reaches the configured {@link CommitFailureHandler} as a decision, instead of unconditionally killing the
 * broker-poll thread and with it the instance.
 * <p>
 * The scenarios pin, in order: the default is byte-compatible with the pre-seam world (shut down, cause recorded);
 * the handler sees an accurate history (attempts, elapsed, consecutive exhaustions); both decisions work end to
 * end, including recovery after CONTINUE; the handler is fail-safe - throwing or hanging converts to shut-down
 * rather than a wedged instance; a deciding handler holds no PC monitor, so rebalance-path callers of the
 * {@code commitCommand} monitor are never blocked by user code; the SASL budget lane feeds the same event; the
 * waiter in {@code ConsumerOffsetCommitter#commitAndWait} no longer carries its own {@code offsetCommitTimeout}
 * deadline, which is the precondition for the seam being reachable at all; a CONTINUE consumes any pending commit
 * command rather than hot-looping on it; and a reassignment scopes the history it reports.
 * <p>
 * The fixture - the failing {@link MockConsumer}, the recording handler, the waits - is
 * {@link MockConsumerCommitFailureSeamTestBase}, which also names the other slices of the seam.
 *
 * @author Antony Stubbs
 * @see CommitFailureHandler
 */
class MockConsumerCommitFailureDecisionTest extends MockConsumerCommitFailureSeamTestBase {

    /**
     * The characterization of the pre-seam behaviour: with default configuration (the canned
     * {@link CommitFailurePolicies#shutDown()} handler), budget exhaustion still closes the instance and
     * {@link ParallelEoSStreamProcessor#getFailureCause()} still carries the commit failure - byte-compatible with
     * the world before the seam. What has changed is only the message: it used to say the decision could not be
     * handed to the application ("no way yet to hand"), which the seam makes false.
     */
    @Test
    void defaultConfigurationStillClosesOnExhaustionWithTheCommitFailureAsCause() {
        useCommitsTimingOut(null);
        startPc(SMALL_BUDGET, CommitFailurePolicies.shutDown());
        addRecordsAndProcess();

        awaitAsserted(() -> assertThat(parallelConsumer.isClosedOrFailed()).isTrue());

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
        var handler = startContinuingPc(healed);

        awaitAsserted(() -> assertThat(handler.contexts.size()).isAtLeast(2));

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
     * CONTINUE recovery, the mock half: a CONTINUE decision leaves the offsets dirty, and when the broker heals, the
     * next
     * commit cadence commits them with a fresh budget - nothing was lost and nothing was wrongly marked done.
     */
    @Test
    void continueThenBrokerHealsCommitsDirtyOffsetsOnNextCadenceWithAFreshBudget() {
        var healed = new AtomicBoolean(false);
        var handler = startContinuingPc(healed);

        awaitAsserted(() -> assertThat(handler.contexts).isNotEmpty());

        healed.set(true);

        // the offsets stayed dirty through the CONTINUE, so the healed broker receives them on the next cadence
        awaitAsserted(() -> {
            var committed = mockConsumer.committed(Collections.singleton(TOPIC_PARTITION)).get(TOPIC_PARTITION);
            assertThat(committed).isNotNull();
            assertThat(committed.offset()).isEqualTo(RECORDS);
        });

        assertThat(parallelConsumer.getFailureCause()).isNull();
        assertThat(parallelConsumer.isClosedOrFailed()).isFalse();
    }

    /**
     * A handler that throws decides nothing - fail-safe SHUT_DOWN - and the reported failure names both
     * the commit failure (as the primary cause chain) and the handler's own exception (travelling with it).
     */
    @Test
    void handlerThatThrowsShutsDownNamingBothExceptions() {
        final String handlerFailureMessage = "handler blew up (mocking)";
        useCommitsTimingOut(null);
        startPc(SMALL_BUDGET, context -> {
            throw new FakeRuntimeException(handlerFailureMessage);
        });
        addRecordsAndProcess();

        awaitAsserted(() -> assertThat(parallelConsumer.isClosedOrFailed()).isTrue());

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
            useCommitsTimingOut(null);
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

            awaitAsserted(() -> assertThat(parallelConsumer.isClosedOrFailed()).isTrue());

            Exception failureCause = parallelConsumer.getFailureCause();
            assertThat(failureCause).isNotNull();
            assertThat(causeChain(failureCause).stream()
                    .anyMatch(t -> t instanceof OffsetCommitBudgetExceededException)).isTrue();
        } finally {
            AbstractParallelEoSStreamProcessor.setCommitFailureHandlerTimeBound(originalBound);
        }
    }

    /**
     * Monitor-free invocation: while a slow handler is still deciding, the {@code commitCommand} monitor is
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
        useCommitsTimingOut(healed);
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

        awaitAsserted(() -> assertThat(handler.contexts).isNotEmpty());
        assertThat(parallelConsumer.isClosedOrFailed()).isFalse();

        settleBeforeTeardown(healed);
    }

    /**
     * Both budget lanes are ONE exhaustion event: the SASL authentication budget
     * ({@code saslAuthenticationRetryTimeout}, zero by default so it exhausts on the first failure) reaches the
     * handler exactly as the offset-commit budget does.
     */
    @Test
    void saslBudgetExhaustionReachesTheHandlerAsTheSameEvent() {
        var healed = new AtomicBoolean(false);
        // polls stay healthy - only committing hits the auth failure, so only the commit-side SASL budget is in
        // play; the generous offset-commit budget must NOT be the one that fires
        var handler = startContinuingPc(
                () -> new SaslAuthenticationException("Invalid username or password (mocking)"), healed,
                Duration.ofSeconds(30));

        awaitAsserted(() -> assertThat(handler.contexts).isNotEmpty());

        var context = handler.contexts.peek();
        assertThat(context.getFailure()).isInstanceOf(OffsetCommitBudgetExceededException.class);
        assertThat(context.getFailure().getCause()).isInstanceOf(SaslAuthenticationException.class);
        assertThat(context.getFailure().getMessage()).contains("saslAuthenticationRetryTimeout");
        assertThat(parallelConsumer.isClosedOrFailed()).isFalse();

        settleBeforeTeardown(healed);
    }

    /**
     * The affirmative wait, the precondition for the whole seam: the waiter in {@code commitAndWait} no longer
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
        var handler = continuingHandler();
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

        awaitAsserted(() -> assertThat(handler.contexts).isNotEmpty());

        // the exhaustion the handler saw really is the held-open one: its elapsed spans the hold window,
        // which is far past the old waiter deadline of one offsetCommitTimeout
        assertThat(handler.contexts.peek().getElapsed().toMillis()).isAtLeast(budget.multipliedBy(3).toMillis());

        // and the healed broker then receives the still-dirty offsets - the instance survived the whole episode
        awaitAsserted(() -> {
            var committed = mockConsumer.committed(Collections.singleton(TOPIC_PARTITION)).get(TOPIC_PARTITION);
            assertThat(committed).isNotNull();
            assertThat(committed.offset()).isEqualTo(RECORDS);
        });

        assertThat(parallelConsumer.getFailureCause()).isNull();
        assertThat(parallelConsumer.isClosedOrFailed()).isFalse();
    }

    /**
     * A commit command pending at the moment a budget exhausts must not defeat CONTINUE's cadence reset. The
     * command is only cleared on the success path, so it survives the failure's throw - and because the very
     * command that <em>initiates</em> a failing commit is that leftover, one {@code requestCommitAsap} before a
     * terminal failure would otherwise re-fire the commit every control-loop pass forever: back-to-back
     * budget-length attempts, each re-consulting the handler, with the cadence reset never taking effect. The
     * CONTINUE decision therefore consumes the command; the offsets it asked to commit stay dirty and travel on
     * the cadence retry.
     * <p>
     * Determinism note: the command cannot be placed while a commit is in flight - the control thread holds the
     * {@code commitCommand} monitor for the whole commit, so a concurrent {@code requestCommitAsap} just blocks
     * until the decision is done (the monitor-free guarantee covers the handler <em>decision</em>, not the
     * commit itself). Between attempts, with a 30s cadence, the control thread is idle and the placement is
     * race-free.
     */
    @Test
    void pendingCommitCommandDoesNotHotLoopAfterContinue() {
        var healed = new AtomicBoolean(false);
        useCommitsTimingOut(healed);
        var handler = continuingHandler();
        // cadence far beyond the observation window: any re-attempt inside it can only come from a leftover command
        startPc(SMALL_BUDGET, Duration.ofSeconds(30), handler);
        addRecordsAndProcess();

        // the first, cadence-sentinel commit exhausts and CONTINUE restores the (30s) cadence
        awaitAsserted(() -> assertThat(handler.contexts).hasSize(1));

        // place the command while the control thread is idle between attempts; the commanded attempt fires
        // promptly and exhausts too
        parallelConsumer.requestCommitAsap();
        awaitAsserted(() -> assertThat(handler.contexts).hasSize(2));

        // the negative half: the command was consumed by that decision, so while the 30s cadence owns the retry
        // no third decision may arrive - a leftover command would produce one within roughly a budget-length
        Awaitility.await()
                .during(SMALL_BUDGET.multipliedBy(5))
                .atMost(Duration.ofSeconds(10))
                .until(() -> handler.contexts.size() == 2);
        assertThat(parallelConsumer.isClosedOrFailed()).isFalse();

        // heal and prove the command mechanism itself still works: a fresh request lands the dirty offsets
        // without waiting out the 30s cadence
        healed.set(true);
        parallelConsumer.requestCommitAsap();
        awaitCommittedOffset(RECORDS);
    }

    /**
     * History scoping across reassignment: a CONTINUE period that ends in revocation and reassignment starts a
     * fresh handler history - the next exhaustion in the new assignment reports {@code consecutive == 1}, a bumped
     * assignment epoch, and a time-since-last-successful-commit measured from the new assignment, not the old
     * failing one. The dirty offsets of the old assignment resolve by the new assignee's reprocessing (here: they
     * are simply gone with the truncation - MockConsumer does not redeliver - and the new assignment's records
     * commit cleanly once healed).
     * <p>
     * The revocation happens while commits are still failing, so the revocation-time commit exhausts its budget
     * mid-callback - covering that this defers (no kill, see
     * {@link MockConsumerCommitFailureHandlerFreeExitsTest}'s
     * {@code revocationTimeBudgetExhaustionDefersWithoutKillingOrConsultingTheHandler} for the focused pin)
     * rather than aborting the rebalance.
     * <p>
     * The new assignment's genuine exhaustion is identified by its OFFSETS (only new-assignment commits carry
     * offsets past the old batch), not by arrival order: a decision already in flight on the control thread when
     * the test thread reassigns may straddle the epoch bump, and must be ignored rather than raced against.
     */
    @Test
    void revocationAndReassignmentResetTheHandlerHistoryForTheNewAssignment() {
        var healed = new AtomicBoolean(false);
        var handler = startContinuingPc(healed);

        // a real CONTINUE period first: at least two consecutive exhaustions, serialized on the control thread
        awaitAsserted(() -> assertThat(handler.contexts.size()).isAtLeast(2));
        var contexts = new ArrayList<>(handler.contexts);
        assertThat(contexts.get(0).getConsecutiveExhaustedBudgets()).isEqualTo(1);
        assertThat(contexts.get(1).getConsecutiveExhaustedBudgets()).isEqualTo(2);
        long oldEpoch = contexts.get(0).getAssignmentEpoch();

        // revoke mid-failure, then reassign - the harness pattern: MockConsumer is told and PC is told by hand
        Instant beforeRevocation = Instant.now();
        parallelConsumer.onPartitionsRevoked(of(TOPIC_PARTITION));
        mockConsumer.rebalance(of(TOPIC_PARTITION));
        parallelConsumer.onPartitionsAssigned(of(TOPIC_PARTITION));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(TOPIC_PARTITION, 0L));

        // fresh work in the new assignment, commits still failing: the next exhaustion must carry a FRESH history
        addRecords(RECORDS, 3); // offsets 5..7, so this exhaustion's offsets (8) are distinguishable from the old (5)
        awaitAsserted(() -> {
            var freshContext = firstContextCommittingPast(handler, RECORDS);
            assertThat(freshContext.isPresent()).isTrue();
            var fresh = freshContext.get();
            assertWithMessage("the first exhaustion of the new assignment must start a fresh consecutive count")
                    .that(fresh.getConsecutiveExhaustedBudgets()).isEqualTo(1);
            assertThat(fresh.getAssignmentEpoch()).isGreaterThan(oldEpoch);
            // measured from the new assignment: spans at least its own budget...
            assertThat(fresh.getTimeSinceLastSuccessfulCommit().toMillis()).isAtLeast(SMALL_BUDGET.toMillis());
            // ...but never the old assignment's failing period, which began well before the revocation
            assertThat(fresh.getTimeSinceLastSuccessfulCommit().toMillis())
                    .isAtMost(Duration.between(beforeRevocation, Instant.now()).toMillis());
        });

        // the new assignment recovers cleanly once the broker heals
        healed.set(true);
        awaitCommittedOffset(RECORDS + 3);
        assertThat(parallelConsumer.isClosedOrFailed()).isFalse();
        assertThat(parallelConsumer.getFailureCause()).isNull();
    }

    /**
     * The first recorded context whose failed commit was for offsets PAST the given one - how a test identifies a
     * decision that can only belong to the new assignment, immune to decisions that straddled the reassignment.
     */
    private static Optional<CommitFailureContext> firstContextCommittingPast(RecordingHandler handler, long offset) {
        return handler.contexts.stream()
                .filter(context -> {
                    OffsetAndMetadata attempted = context.getOffsets().get(TOPIC_PARTITION);
                    return attempted != null && attempted.offset() > offset;
                })
                .findFirst();
    }
}
