package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.CommitFailureHandler.CommitFailureDecision;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static bz.stub.parallelconsumer.internal.utils.ThreadUtils.sleepOrFail;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * The rebalance-deferral lane joining the commit-failure seam (astubbs#317, confluentinc#833): a commit deferred
 * because this consumer is no longer a group member ({@link CommitFailedException} - usually eviction) used to loop
 * at WARN forever, uncounted. Once consecutive deferrals have persisted longer than {@code offsetCommitTimeout} -
 * the same quantum the budget lane uses - the streak escalates to the handler as the seam's one typed event.
 * <p>
 * The scenarios pin both decisions on an escalated streak, and the escalation bound itself from three sides: the
 * control arm that a short streak never escalates, and the two events that clear the accounting (a successful
 * commit, and a completed rebalance) - each with the stale-clock trap that a non-clearing implementation falls
 * into.
 * <p>
 * The fixture - the failing {@link MockConsumer}, the recording handler, the waits - is
 * {@link MockConsumerCommitFailureSeamTestBase}, which also names the other slices of the seam.
 *
 * @author Antony Stubbs
 * @see CommitFailureHandler
 */
class MockConsumerCommitFailureDeferralEscalationTest extends MockConsumerCommitFailureSeamTestBase {

    /**
     * The rebalance-deferral lane joins the seam: a commit deferred because this consumer is no longer a group
     * member ({@link CommitFailedException} - usually eviction) used to loop at WARN forever, uncounted. Once
     * consecutive deferrals have persisted longer than {@code offsetCommitTimeout} - the same quantum the budget
     * lane uses - the streak escalates to the handler as the seam's one typed event, and a CONTINUE decision keeps
     * the instance alive exactly as it does for a budget exhaustion.
     * <p>
     * Also pins the deferral sharpening the escalation depends on: deferred cycles must NOT advance
     * {@code lastSuccessfulCommitTime} (they reach no broker), so the context's time-since-last-successful-commit
     * spans the whole streak - at least the escalation bound - rather than one commit interval.
     */
    @Test
    void persistentDeferralsEscalateToTheHandlerAndContinueKeepsTheInstanceAlive() {
        var healed = new AtomicBoolean(false);
        var handler = startContinuingPc(COMMIT_IS_DEFERRED, healed, SMALL_BUDGET);

        awaitAsserted(() -> assertThat(handler.contexts).isNotEmpty());

        var context = handler.contexts.peek();
        assertThat(context.getFailure()).isInstanceOf(OffsetCommitBudgetExceededException.class);
        var failure = (OffsetCommitBudgetExceededException) context.getFailure();
        // the escalation names the deferral cause, not a generic timeout: the operator must learn the consumer
        // was thrown out of the group, because the remedy (fix max.poll.interval.ms / processing time) is different
        assertThat(failure.getCause()).isInstanceOf(CommitFailedException.class);
        assertThat(failure.getMessage()).contains("offsetCommitTimeout");
        assertThat(failure.getMessage()).contains("no longer a member");
        assertThat(failure.getMessage()).contains("commitFailureHandler");
        assertThat(context.getOffsets()).containsKey(TOPIC_PARTITION);
        // the streak: the bound cannot be crossed by the first deferral, whose elapsed is zero
        assertThat(failure.getAttemptsMade()).isAtLeast(2);
        assertThat(failure.getElapsed().toMillis()).isAtLeast(SMALL_BUDGET.toMillis());
        assertThat(context.getConsecutiveExhaustedBudgets()).isEqualTo(1);
        // the sharpening: deferred cycles are not successes, so the whole streak is visible here
        assertThat(context.getTimeSinceLastSuccessfulCommit().toMillis()).isAtLeast(SMALL_BUDGET.toMillis());

        // CONTINUE keeps the instance alive
        assertThat(parallelConsumer.isClosedOrFailed()).isFalse();
        assertThat(parallelConsumer.getFailureCause()).isNull();

        settleBeforeTeardown(healed);
    }

    /**
     * The SHUT_DOWN half of {@link #persistentDeferralsEscalateToTheHandlerAndContinueKeepsTheInstanceAlive()}: the
     * escalated deferral rides the ordinary decision loop, so a SHUT_DOWN decision closes the instance with the
     * escalation - deferral cause attached - recorded as the failure.
     */
    @Test
    void persistentDeferralsEscalationShutDownClosesTheInstance() {
        mockConsumer = consumerWithFailingCommits(COMMIT_IS_DEFERRED, null);
        var handler = new RecordingHandler(CommitFailureDecision.SHUT_DOWN);
        startPc(SMALL_BUDGET, handler);
        addRecordsAndProcess();

        awaitAsserted(() -> assertThat(parallelConsumer.isClosedOrFailed()).isTrue());

        assertThat(handler.contexts).isNotEmpty();
        Exception failureCause = parallelConsumer.getFailureCause();
        assertThat(failureCause).isNotNull();
        var budgetFailure = causeChain(failureCause).stream()
                .filter(t -> t instanceof OffsetCommitBudgetExceededException)
                .findFirst();
        assertWithMessage("the escalated deferral must be reachable from getFailureCause()")
                .that(budgetFailure.isPresent()).isTrue();
        assertThat(budgetFailure.get().getCause()).isInstanceOf(CommitFailedException.class);
    }

    /**
     * The control arm for the escalation bound: a deferral streak that heals well inside {@code offsetCommitTimeout}
     * - the normal one-or-two-cycle deferral of a healthy rebalance - never reaches the handler. Without this arm,
     * the escalation tests could pass with a bound of zero, which would consult the handler for every routine
     * rebalance.
     */
    @Test
    void aShortDeferralStreakHealsWithoutConsultingTheHandler() {
        var commitAttempts = new AtomicInteger();
        final int deferredCommits = 2;
        mockConsumer = consumerDeferringWhile(() -> commitAttempts.incrementAndGet() <= deferredCommits);
        var handler = continuingHandler();
        // a bound the short streak cannot plausibly cross, so a red here means the bound logic broke, not the clock
        startPc(Duration.ofSeconds(10), handler);
        addRecordsAndProcess();

        // the deferrals heal (attempt 3 commits) and the offsets land - with no decision ever asked for
        awaitCommittedOffset(RECORDS);
        assertThat(commitAttempts.get()).isAtLeast(deferredCommits + 1);
        assertWithMessage("a deferral streak that heals inside the bound must never consult the handler")
                .that(handler.contexts).isEmpty();
        assertThat(parallelConsumer.isClosedOrFailed()).isFalse();
        assertThat(parallelConsumer.getFailureCause()).isNull();
    }

    /**
     * The deferral accounting clears on a successful commit: after a short streak heals and commits land, a LATER
     * streak gets the full escalation bound on a fresh clock. Stale accounting would escalate the later streak's
     * first deferral instantly - its clock would still be running from the first streak, which by then is long
     * past the bound - which is exactly what the no-escalation window detects.
     */
    @Test
    void deferralAccountingClearsOnASuccessfulCommit() {
        final Duration bound = Duration.ofSeconds(2);
        var commitAttempts = new AtomicInteger();
        var brokenAgain = new AtomicBoolean(false);
        final int initiallyDeferredCommits = 3;
        mockConsumer = consumerDeferringWhile(
                () -> commitAttempts.incrementAndGet() <= initiallyDeferredCommits || brokenAgain.get());
        var handler = continuingHandler();
        startPc(bound, handler);
        addRecordsAndProcess();

        // first streak: a handful of deferrals well inside the bound, then a clean commit
        awaitCommittedOffset(RECORDS);
        assertThat(commitAttempts.get()).isAtLeast(initiallyDeferredCommits + 1);
        assertThat(handler.contexts).isEmpty();

        // park until the FIRST streak's clock, were it still running, would be past the bound
        sleepOrFail(bound.plusMillis(500), "Interrupted while outliving the first streak's would-be clock");

        // second streak: deferrals resume - on a fresh clock, so nothing may escalate inside the bound
        brokenAgain.set(true);
        addRecords(RECORDS, 1); // offset 5 - the dirty-driver for the second streak
        int attemptsAtSecondStreak = commitAttempts.get();
        Awaitility.await().during(Duration.ofMillis(700)).atMost(Duration.ofSeconds(10))
                .until(() -> handler.contexts.isEmpty());
        assertWithMessage("the no-escalation window only means something if commits were being deferred in it")
                .that(commitAttempts.get()).isGreaterThan(attemptsAtSecondStreak);

        // and the fresh clock does its job: left broken, the second streak escalates once ITS bound is crossed
        awaitAsserted(() -> assertThat(handler.contexts).isNotEmpty());
        assertThat(handler.contexts.peek().getFailure().getCause()).isInstanceOf(CommitFailedException.class);
        assertThat(parallelConsumer.isClosedOrFailed()).isFalse();

        brokenAgain.set(false);
        awaitCommittedOffset(RECORDS + 1);
    }

    /**
     * The deferral accounting clears on a completed rebalance: deferrals accumulated before a revocation
     * belong to the OLD assignment, so after reassignment a new streak gets the full bound on a fresh clock -
     * pinned with the same stale-clock trap as {@link #deferralAccountingClearsOnASuccessfulCommit()}, but with the
     * clearing done by the reassignment (no commit ever succeeds here until the very end).
     */
    @Test
    void deferralAccountingClearsOnACompletedRebalance() {
        final Duration bound = Duration.ofSeconds(2);
        var commitAttempts = new AtomicInteger();
        var broken = new AtomicBoolean(true);
        // counts only the DEFERRED attempts - the healed ones at the end are not part of any streak
        mockConsumer = consumerDeferringWhile(() -> {
            if (!broken.get()) {
                return false;
            }
            commitAttempts.incrementAndGet();
            return true;
        });
        var handler = continuingHandler();
        startPc(bound, handler);
        addRecordsAndProcess();

        // deferrals accumulate in the old assignment - the streak's clock starts here
        awaitAsserted(() -> assertThat(commitAttempts.get()).isAtLeast(2));

        // the rebalance completes: revocation (whose own commit defers too - CommitFailedException is a deferral,
        // not an exhaustion, so the callback just carries on) and reassignment, which scopes the history
        parallelConsumer.onPartitionsRevoked(of(TOPIC_PARTITION));
        mockConsumer.rebalance(of(TOPIC_PARTITION));
        parallelConsumer.onPartitionsAssigned(of(TOPIC_PARTITION));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(TOPIC_PARTITION, 0L));

        // park until the OLD streak's clock, were it still running, would be well past the bound
        sleepOrFail(bound.plusMillis(500), "Interrupted while outliving the old assignment's would-be clock");

        // new assignment, commits still broken: the new streak must get the whole bound on a fresh clock
        addRecords(RECORDS, 3); // offsets 5..7
        int attemptsAtNewAssignment = commitAttempts.get();
        Awaitility.await().during(Duration.ofMillis(700)).atMost(Duration.ofSeconds(10))
                .until(() -> handler.contexts.isEmpty());
        assertWithMessage("the no-escalation window only means something if commits were being deferred in it")
                .that(commitAttempts.get()).isGreaterThan(attemptsAtNewAssignment);

        // and left broken, the new streak escalates once ITS OWN bound is crossed
        awaitAsserted(() -> assertThat(handler.contexts).isNotEmpty());
        assertThat(parallelConsumer.isClosedOrFailed()).isFalse();

        broken.set(false);
        awaitCommittedOffset(RECORDS + 3);
    }

    /**
     * A consumer whose {@code commitSync} DEFERS - {@link CommitFailedException}, paced like every other failing
     * commit here - whenever {@code deferring} says so, and commits normally otherwise. The predicate owns any
     * attempt counting, because the scenarios disagree about whether a healed commit counts as an attempt.
     * <p>
     * Not {@link #consumerWithFailingCommits}: these streaks switch on state the scenario re-evaluates per attempt
     * (an attempt count, a second outage), not on the single healed flag that helper takes.
     */
    private MockConsumer<String, String> consumerDeferringWhile(BooleanSupplier deferring) {
        return new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
            @Override
            // deliberately NOT synchronized on the failing path - see consumerWithFailingCommits
            public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
                if (deferring.getAsBoolean()) {
                    sleepOrFail(FAILING_COMMIT_PACING, "Interrupted while pacing a failing commit");
                    throw noLongerAGroupMember();
                }
                super.commitSync(offsets);
            }
        };
    }
}
