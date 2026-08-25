package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.CommitFailureHandler.CommitFailureDecision;
import bz.stub.parallelconsumer.internal.ConsumerOffsetCommitter;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static bz.stub.parallelconsumer.internal.utils.ThreadUtils.sleepOrFail;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.parallel.ResourceAccessMode.READ_WRITE;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * The rebalance-deferral lane joining the commit-failure seam (astubbs#317, confluentinc#833): a commit deferred
 * because this consumer is no longer a group member ({@link CommitFailedException} - usually eviction) used to loop
 * at WARN forever, uncounted. Once consecutive deferrals have persisted longer than
 * {@link ConsumerOffsetCommitter#getDeferralEscalationBound()} - <b>rebalance-scale</b>, five minutes by default -
 * the streak escalates to the handler as the seam's one typed event.
 * <p>
 * The scenarios pin both decisions on an escalated streak, and the escalation bound itself from four sides: the
 * control arm that a short streak never escalates, the regression arm that a streak outliving the far shorter
 * {@code offsetCommitTimeout} still does not
 * ({@link #aStreakOutlivingTheCommitBudgetButNotTheBoundNeverEscalates()}), and the two events that clear the
 * accounting (a successful commit, and a completed rebalance) - each with the stale-clock trap that a
 * non-clearing implementation falls into.
 * <p>
 * <b>Every scenario drives the bound through its test seam</b>, because the real one is five minutes and no unit
 * test can outlive it. The seam is restored after each test; a scenario that needs a bound the streak cannot
 * plausibly cross says so where it sets one.
 * <p>
 * The fixture - the failing {@link MockConsumer}, the recording handler, the waits - is
 * {@link MockConsumerCommitFailureSeamTestBase}, which also names the other slices of the seam.
 *
 * @author Antony Stubbs
 * @see CommitFailureHandler
 */
// GLOBAL, not a named resource: the escalation bound is a JVM-wide static, and shortening it changes the
// behaviour of any concurrently running test whose commits defer - CommitRejectionTestBase's subclasses reject
// commits for a while and assert PC survives, which a 300ms bound turns into a shutdown. A named lock only
// excludes tests that name the same resource, and no other class knows to. Measured, not theorised: the sibling
// static in CommitResponseTimeoutSymptomTest was first guarded by a named lock and took an unrelated test in
// its own class down.
@ResourceLock(value = Resources.GLOBAL, mode = READ_WRITE)
class MockConsumerCommitFailureDeferralEscalationTest extends MockConsumerCommitFailureSeamTestBase {

    private Duration originalEscalationBound;

    @BeforeEach
    void rememberTheEscalationBound() {
        originalEscalationBound = ConsumerOffsetCommitter.getDeferralEscalationBound();
    }

    @AfterEach
    void restoreTheEscalationBound() {
        ConsumerOffsetCommitter.setDeferralEscalationBound(originalEscalationBound);
    }

    /**
     * The rebalance-deferral lane joins the seam: a commit deferred because this consumer is no longer a group
     * member ({@link CommitFailedException} - usually eviction) used to loop at WARN forever, uncounted. Once
     * consecutive deferrals have persisted longer than the rebalance-scale escalation bound, the streak escalates
     * to the handler as the seam's one typed event, and a CONTINUE decision keeps the instance alive exactly as it
     * does for a budget exhaustion.
     * <p>
     * Also pins the deferral sharpening the escalation depends on: deferred cycles must NOT advance
     * {@code lastSuccessfulCommitTime} (they reach no broker), so the context's time-since-last-successful-commit
     * spans the whole streak - at least the escalation bound - rather than one commit interval.
     */
    @Test
    void persistentDeferralsEscalateToTheHandlerAndContinueKeepsTheInstanceAlive() {
        final Duration bound = SMALL_BUDGET;
        ConsumerOffsetCommitter.setDeferralEscalationBound(bound);
        var healed = new AtomicBoolean(false);
        var handler = startContinuingPc(COMMIT_IS_DEFERRED, healed, SMALL_BUDGET);

        awaitAsserted(() -> assertThat(handler.contexts).isNotEmpty());

        var context = handler.contexts.peek();
        assertThat(context.getFailure()).isInstanceOf(OffsetCommitBudgetExceededException.class);
        var failure = (OffsetCommitBudgetExceededException) context.getFailure();
        // the escalation names the deferral cause, not a generic timeout: the operator must learn the consumer
        // was thrown out of the group, because the remedy (fix max.poll.interval.ms / processing time) is different
        assertThat(failure.getCause()).isInstanceOf(CommitFailedException.class);
        // and it names the clock it actually ran on - a rebalance-scale bound, explicitly not the commit budget
        assertThat(failure.getMessage()).contains("deferral escalation bound");
        assertThat(failure.getMessage()).contains("max.poll.interval.ms");
        assertThat(failure.getMessage()).contains("no longer a member");
        assertThat(failure.getMessage()).contains("commitFailureHandler");
        assertThat(context.getOffsets()).containsKey(TOPIC_PARTITION);
        // the streak: the bound cannot be crossed by the first deferral, whose elapsed is zero
        assertThat(failure.getAttemptsMade()).isAtLeast(2);
        assertThat(failure.getElapsed().toMillis()).isAtLeast(bound.toMillis());
        assertThat(context.getConsecutiveExhaustedBudgets()).isEqualTo(1);
        // the sharpening: deferred cycles are not successes, so the whole streak is visible here
        assertThat(context.getTimeSinceLastSuccessfulCommit().toMillis()).isAtLeast(bound.toMillis());

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
        ConsumerOffsetCommitter.setDeferralEscalationBound(SMALL_BUDGET);
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
     * The control arm for the escalation bound: a deferral streak that heals well inside it - the normal
     * one-or-two-cycle deferral of a healthy rebalance - never reaches the handler. Without this arm, the
     * escalation tests could pass with a bound of zero, which would consult the handler for every routine
     * rebalance.
     */
    @Test
    void aShortDeferralStreakHealsWithoutConsultingTheHandler() {
        // a bound the short streak cannot plausibly cross, so a red here means the bound logic broke, not the clock
        ConsumerOffsetCommitter.setDeferralEscalationBound(Duration.ofSeconds(10));
        var commitAttempts = new AtomicInteger();
        final int deferredCommits = 2;
        mockConsumer = consumerDeferringWhile(() -> commitAttempts.incrementAndGet() <= deferredCommits);
        var handler = continuingHandler();
        startPc(SMALL_BUDGET, handler);
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
        ConsumerOffsetCommitter.setDeferralEscalationBound(bound);
        var commitAttempts = new AtomicInteger();
        var brokenAgain = new AtomicBoolean(false);
        final int initiallyDeferredCommits = 3;
        mockConsumer = consumerDeferringWhile(
                () -> commitAttempts.incrementAndGet() <= initiallyDeferredCommits || brokenAgain.get());
        var handler = continuingHandler();
        startPc(SMALL_BUDGET, handler);
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
     * the same stale-clock trap as {@link #deferralAccountingClearsOnASuccessfulCommit()}, but with the clearing
     * done by the reassignment (no commit ever succeeds here until the very end).
     * <p>
     * <b>The old streak is deliberately aged to part of the bound before the rebalance</b>, and the
     * no-escalation window after it is longer than the bound's remainder. That is what makes the arm
     * discriminating: a stale clock escalates {@code bound - age} after the rebalance, inside the window; a
     * fresh one has the whole bound and cannot. Sleeping out the WHOLE bound first - the shape this had - only
     * looked equivalent: a deferral landing between the clear and the sleep starts the new streak's clock before
     * the sleep, so the sleep spends the fresh bound too and both implementations escalate. It passed by
     * timing, and failed the moment two test classes shared a machine.
     */
    @Test
    void deferralAccountingClearsOnACompletedRebalance() {
        final Duration bound = Duration.ofSeconds(6);
        // how far into the bound the OLD streak is taken before the rebalance
        final Duration ageBeforeRebalance = Duration.ofMillis(2500);
        // longer than the bound's remainder (3.5s), so a stale clock escalates inside it; shorter than a whole
        // fresh bound, so a cleared one cannot
        final Duration noEscalationWindow = Duration.ofSeconds(4);
        ConsumerOffsetCommitter.setDeferralEscalationBound(bound);
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
        startPc(SMALL_BUDGET, handler);
        addRecordsAndProcess();

        // deferrals accumulate in the old assignment - the streak's clock starts here
        awaitAsserted(() -> assertThat(commitAttempts.get()).isAtLeast(2));
        sleepOrFail(ageBeforeRebalance, "Interrupted while ageing the old assignment's streak");
        assertWithMessage("the old streak must still be inside the bound when the rebalance happens, or this "
                + "arm is measuring an escalation that already happened")
                .that(handler.contexts).isEmpty();

        // the rebalance completes: revocation (whose own commit defers too - CommitFailedException is a deferral,
        // not an exhaustion, so the callback just carries on) and reassignment, which scopes the history
        parallelConsumer.onPartitionsRevoked(of(TOPIC_PARTITION));
        mockConsumer.rebalance(of(TOPIC_PARTITION));
        parallelConsumer.onPartitionsAssigned(of(TOPIC_PARTITION));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(TOPIC_PARTITION, 0L));
        Instant reassignedAt = Instant.now();

        // new assignment, commits still broken: the new streak must get the whole bound on a fresh clock
        addRecords(RECORDS, 3); // offsets 5..7
        int attemptsAtNewAssignment = commitAttempts.get();
        Awaitility.await().during(noEscalationWindow).atMost(noEscalationWindow.plusSeconds(10))
                .until(() -> handler.contexts.isEmpty());
        assertWithMessage("the no-escalation window only means something if commits were being deferred in it")
                .that(commitAttempts.get()).isGreaterThan(attemptsAtNewAssignment);

        // and left broken, the new streak escalates once ITS OWN bound is crossed
        awaitAsserted(() -> assertThat(handler.contexts).isNotEmpty());
        var failure = (OffsetCommitBudgetExceededException) handler.contexts.peek().getFailure();
        assertWithMessage("the escalation must report the NEW streak's age - an age reaching back past the "
                + "reassignment is the stale clock this arm exists to catch")
                .that(failure.getElapsed().toMillis())
                .isLessThan(Duration.between(reassignedAt, Instant.now()).toMillis());
        assertThat(parallelConsumer.isClosedOrFailed()).isFalse();

        broken.set(false);
        awaitCommittedOffset(RECORDS + 3);
    }

    /**
     * The regression that the escalation clock is <b>rebalance-scale</b> and not the commit budget: a deferral
     * streak that runs for many times {@code offsetCommitTimeout}, but stays inside the escalation bound, must
     * NOT escalate. The handler is never consulted, PC survives, and the deferrals go on WARNing - exactly what
     * a pre-seam PC did with the identical sequence.
     * <p>
     * <b>Why this arm exists.</b> The escalation clock was originally the {@code offsetCommitTimeout} quantum,
     * whose default is 10 seconds. An eager rebalance legitimately waits on its slowest member for up to
     * {@code max.poll.interval.ms} - five minutes by default - and a consumer waiting that out defers every
     * commit it attempts. With a 10-second clock the resulting streak escalates, and the DEFAULT handler is
     * {@code shutDown()}: PC would close instances during ordinary healthy rebalances that it previously
     * survived, and every close triggers another rebalance. That is a break of the seam's own
     * default-behaviour-unchanged promise, which no other test here could catch - each of them shortens the
     * bound to make escalation happen, so all of them would stay green with the wrong clock.
     * <p>
     * Discriminating: point the escalation at {@code offsetCommitTimeout} again and this fails - the streak
     * outlives that budget several times over inside the observation window, so a decision arrives and the
     * during-window below goes red.
     */
    @Test
    void aStreakOutlivingTheCommitBudgetButNotTheBoundNeverEscalates() {
        // ten commit budgets' worth of headroom in the bound: the streak below crosses the budget many times over
        // and cannot come close to the bound
        final Duration commitBudget = SMALL_BUDGET;
        ConsumerOffsetCommitter.setDeferralEscalationBound(Duration.ofSeconds(30));
        var committerLogger = (Logger) LoggerFactory.getLogger(ConsumerOffsetCommitter.class);
        var appender = new ListAppender<ILoggingEvent>();
        appender.start();
        committerLogger.addAppender(appender);
        try {
            var commitAttempts = new AtomicInteger();
            var broken = new AtomicBoolean(true);
            mockConsumer = consumerDeferringWhile(() -> {
                if (!broken.get()) {
                    return false;
                }
                commitAttempts.incrementAndGet();
                return true;
            });
            var handler = continuingHandler();
            startPc(commitBudget, handler);
            addRecordsAndProcess();

            // deferrals run for ten budgets without a single successful commit to clear the streak
            Duration wellPastTheBudget = commitBudget.multipliedBy(10);
            Awaitility.await().during(wellPastTheBudget).atMost(Duration.ofSeconds(20))
                    .until(() -> handler.contexts.isEmpty());

            assertWithMessage("the window only means something if commits were genuinely being deferred throughout")
                    .that(commitAttempts.get()).isAtLeast(3);
            assertWithMessage("a streak longer than offsetCommitTimeout, but inside the rebalance-scale bound, "
                    + "must not consult the handler - the default handler shuts PC down")
                    .that(handler.contexts).isEmpty();
            assertThat(parallelConsumer.isClosedOrFailed()).isFalse();
            assertThat(parallelConsumer.getFailureCause()).isNull();

            // ...and the deferrals were not silent either: each cycle still WARNs, which is the behaviour the
            // escalation exists to eventually escape, not to replace
            long deferralWarnings = appender.list.stream()
                    .filter(event -> event.getLevel() == Level.WARN)
                    .filter(event -> String.valueOf(event.getFormattedMessage()).contains("Offset commit deferred"))
                    .count();
            assertWithMessage("every deferred cycle must still WARN")
                    .that(deferralWarnings).isAtLeast(2L);

            // and the instance is genuinely healthy: healed, the dirty offsets land
            broken.set(false);
            awaitCommittedOffset(RECORDS);
        } finally {
            committerLogger.detachAppender(appender);
            appender.stop();
        }
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
