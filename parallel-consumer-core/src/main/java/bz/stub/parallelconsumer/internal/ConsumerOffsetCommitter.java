package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.OffsetCommitBudgetExceededException;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.state.WorkManager;
import lombok.Getter;
import lombok.Setter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RebalanceInProgressException;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static bz.stub.parallelconsumer.internal.utils.JavaUtils.isGreaterThan;
import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * Committer that uses the Kafka Consumer to commit either synchronously or asynchronously
 *
 * @see CommitMode
 */
@Slf4j
public class ConsumerOffsetCommitter<K, V> extends AbstractOffsetCommitter<K, V> implements OffsetCommitter {

    /**
     * Chosen arbitrarily - retries should never be needed, if they are it's an invalid state
     */
    private static final int ARBITRARY_RETRY_LIMIT = 50;

    /**
     * How long an uncleared deferral streak may run before it escalates to the commit-failure seam
     * (astubbs#317) - see {@link #recordDeferralAndMaybeEscalate}.
     * <p>
     * <b>Rebalance-scale, deliberately: five minutes is parity with Kafka's own
     * {@code max.poll.interval.ms} default</b>, which is the longest an eager rebalance may legitimately wait
     * for a slow member before the group evicts it. A deferral streak is precisely what a healthy consumer
     * shows while that wait runs, so any bound shorter than one full rebalance calls a healthy consumer broken.
     * <p>
     * <b>This overrides the plan document</b>
     * ({@code docs/plans/2026-08-24-001-feat-commit-failure-seam-plan.md}, KTD9 / U5 - "deferrals persisting
     * past the bound clock"), which took that bound to be the {@code offsetCommitTimeout} quantum the retry
     * budget uses. Code review found that quantum breaks the seam's own default-behaviour-unchanged
     * invariant: at its 10s default, an ordinary slow-but-healthy rebalance escalates, and the DEFAULT
     * handler is {@code shutDown()} - so PC would close instances that pre-seam merely WARNed and survived,
     * and each close triggers another group rebalance, cascading. A rebalance-scale clock keeps the feature
     * the escalation exists for (a deferral streak can no longer loop silently at WARN forever) while an
     * ordinary rebalance stays inside it.
     * <p>
     * Static and settable only as a test seam - a streak this long cannot be produced inside a unit test's
     * lifetime - the same shape as {@link BrokerPollSystem#getLongPollTimeout()}.
     */
    @Setter
    @Getter
    private static Duration deferralEscalationBound = Duration.ofMinutes(5);

    /**
     * The fixed part of {@link #commitAndWait()}'s poller-wedged backstop; the whole bound is this plus
     * {@link #POLLER_WEDGED_BACKSTOP_BUDGET_MULTIPLE} times the configured {@code offsetCommitTimeout}.
     * <p>
     * Generous on purpose, because it is a <em>last resort</em> and not a deadline: the affirmative events
     * (a typed response, or the poller's death) are what normally end that wait, and this exists only for
     * the case where neither can arrive because the poll thread is wedged somewhere outside the commit path.
     * Deriving it from the budget keeps it above any legitimately slow commit - the poll side cannot spend
     * more than its own budget plus one broker call - and the floor keeps it well clear of a small budget.
     * <p>
     * Static and settable only as a test seam, the same shape as {@link BrokerPollSystem#getLongPollTimeout()}.
     */
    @Setter
    @Getter
    private static Duration pollerWedgedBackstopFloor = Duration.ofSeconds(60);

    /**
     * @see #pollerWedgedBackstopFloor
     */
    private static final int POLLER_WEDGED_BACKSTOP_BUDGET_MULTIPLE = 4;

    private final CommitMode commitMode;

    private final Duration commitTimeout;

    private Optional<Thread> owningThread = Optional.empty();

    /**
     * Queue of commit requests from other threads
     */
    private final Queue<CommitRequest> commitRequestQueue = new ConcurrentLinkedQueue<>();

    /**
     * Queue of commit responses, for other threads to block on
     */
    private final BlockingQueue<CommitResponse> commitResponseQueue = new LinkedBlockingQueue<>();

    /**
     * The exception that killed the broker-poll thread, published by that thread as it dies.
     * <p>
     * The poll thread is the <em>only</em> producer of commit responses, so a waiter can never learn
     * of its death by waiting - waiting is precisely the thing that cannot work. Deriving the
     * waiter's deadline from the poller's budget so that one expires first would only make the race
     * usually resolve the right way; being told is the version that is always right. This is the same
     * move {@link #maybeDoCommit()} already makes for a deferred commit, and the same shape as
     * {@link ConsumerManager#setCloseInProgressSignal}.
     */
    private final AtomicReference<Throwable> pollerDeath = new AtomicReference<>();

    /**
     * Wake-up token, published once alongside {@link #pollerDeath}. Its request id matches nobody, so
     * a waiter can only ever act on it through {@code pollerDeath} - its job is to end the blocking
     * {@code poll()} at the moment of death, not to answer a request.
     */
    private static final CommitResponse POLLER_DIED = new CommitResponse(new CommitRequest());

    // --- rebalance-deferral accounting (astubbs#317) ------------------------------------------------------------

    /**
     * The current streak of consecutive deferrals, held as ONE immutable value swapped atomically (astubbs#317).
     * <p>
     * Every deferral is recorded by the broker-poll thread (both {@link #commitDeferringOnRebalance()} call paths
     * execute there in production), but <b>the clearing side is not always that thread</b>: the controller's
     * {@code onPartitionsAssigned} callback forwards a completed rebalance to {@link #onPartitionsAssigned()}, and
     * in test harnesses the rebalance callbacks are driven by the test thread. So there is no single writer, and
     * the streak's start and its count may not be written one at a time - a clear landing between the two leaves a
     * count belonging to a streak that has been cleared, which is what decides whether the escalation fires and
     * with what {@link OffsetCommitBudgetExceededException#getAttemptsMade()}.
     */
    private final AtomicReference<DeferralStreak> deferralStreak = new AtomicReference<>(DeferralStreak.cleared());

    /**
     * One coherent generation of {@link #deferralStreak}: when the streak began - {@code null} while no deferral is
     * outstanding - and how many deferrals it has taken. The start is the escalation's bound clock: once
     * {@code Instant.now() - firstDeferral} crosses {@link ConsumerOffsetCommitter#getDeferralEscalationBound()},
     * the streak stops being silent - see {@link ConsumerOffsetCommitter#commitDeferringOnRebalance()}.
     */
    @Value
    static class DeferralStreak {

        Instant firstDeferral;

        int deferrals;

        static DeferralStreak cleared() {
            return new DeferralStreak(null, 0);
        }

        /**
         * A fresh window on the same running streak: the clock restarts, so a CONTINUE decision buys another
         * whole {@link ConsumerOffsetCommitter#getDeferralEscalationBound()} rather than an escalation every
         * cycle, and the count starts again from the deferrals that follow.
         */
        static DeferralStreak restartedAt(Instant now) {
            return new DeferralStreak(now, 0);
        }

        DeferralStreak plusDeferral(Instant now) {
            return isRunning()
                    ? new DeferralStreak(firstDeferral, deferrals + 1)
                    : new DeferralStreak(now, 1);
        }

        boolean isRunning() {
            return firstDeferral != null;
        }

        Duration age(Instant now) {
            return Duration.between(firstDeferral, now);
        }
    }

    /**
     * Whether the commit cycle that most recently completed normally was a DEFERRAL rather than a commit - the
     * committed-vs-deferred outcome {@link OffsetCommitter#lastCommitWasDeferred()} reports to the controller, whose
     * success accounting must not treat a deferred cycle as a success. Read by the waiting thread after its typed
     * response arrives (the response queue provides the happens-before edge); commit cycles themselves are
     * serialized by the controller's {@code commitCommand} monitor, so the flag cannot be overwritten by another
     * cycle between the response and the read. {@link #onPartitionsAssigned()} clears it from whichever thread
     * delivers the rebalance - a whole-value write, so unlike the streak beside it there is nothing here to tear.
     */
    private volatile boolean lastCommitCycleDeferred;

    /**
     * The offsets the most recent commit attempt carried, kept only so an escalated deferral can report which
     * offsets are stuck - the deferral exceptions are thrown by the broker before this class sees the offsets map
     * again. Only ever touched by the broker-poll thread, inside a commit cycle.
     */
    private Map<TopicPartition, OffsetAndMetadata> lastAttemptedOffsets = Collections.emptyMap();

    public ConsumerOffsetCommitter(final ConsumerManager<K, V> newConsumer, final WorkManager<K, V> newWorkManager, final ParallelConsumerOptions options) {
        super(newConsumer, newWorkManager);
        commitMode = options.getCommitMode();
        commitTimeout = options.getOffsetCommitTimeout();
        if (commitMode.equals(PERIODIC_TRANSACTIONAL_PRODUCER)) {
            throw new IllegalArgumentException("Cannot use " + commitMode + " when using " + this.getClass().getSimpleName());
        }
    }

    /**
     * Might block if using {@link CommitMode#PERIODIC_CONSUMER_SYNC}
     *
     * @see CommitMode
     */
    void commit() throws TimeoutException, InterruptedException {
        if (isOwner()) {
            commitDeferringOnRebalance();
        } else if (isSync()) {
            log.debug("Sync commit");
            commitAndWait();
            log.debug("Finished waiting");
        } else {
            // async
            // we just request the commit and hope
            log.debug("Async commit to be requested");
            requestCommitInternal();
        }
    }

    @Override
    protected void commitOffsets(final Map<TopicPartition, OffsetAndMetadata> offsetsToSend, final ConsumerGroupMetadata groupMetadata) {
        if (offsetsToSend.isEmpty()) {
            log.trace("Nothing to commit");
            return;
        }
        // stashed for the deferral accounting: if this attempt is deferred, the escalation path reports these
        lastAttemptedOffsets = offsetsToSend;
        switch (commitMode) {
            case PERIODIC_CONSUMER_SYNC -> {
                log.debug("Committing offsets Sync");
                consumerMgr.commitSync(offsetsToSend);
            }
            case PERIODIC_CONSUMER_ASYNCHRONOUS -> {
                //
                log.debug("Committing offsets Async");
                consumerMgr.commitAsync(offsetsToSend, (offsets, exception) -> {
                    if (exception != null) {
                        log.error("Error committing offsets: {}, exception: ", offsets, exception);
                        // todo keep work in limbo until async response is received?
                    }
                });
            }
            default ->
                    throw new IllegalArgumentException("Cannot use " + commitMode + " when using " + this.getClass().getSimpleName());
        }
    }

    /**
     * @see #commit()
     */
    @Override
    protected void postCommit() {
    }

    private boolean isOwner() {
        return Thread.currentThread().equals(owningThread.orElse(null));
    }

    /**
     * Commit request message
     */
    @Value
    public static class CommitRequest {
        UUID id = UUID.randomUUID();
        long requestedAtMs = System.currentTimeMillis();
    }

    /**
     * Commit response message, linked to a {@link CommitRequest}.
     * <p>
     * Three outcomes travel on this one type: committed, deferred (see {@link #commitDeferringOnRebalance}), and -
     * carrying a non-null {@link #commitFailure} - terminally failed, the commit-failure seam's typed re-route
     * (astubbs#317; a spent retry budget, or a deferral streak that outlived one). The failure rides the
     * existing response channel deliberately: a typed message on the queue the waiter already blocks on,
     * never a cross-thread interrupt or a bare flag (the interrupt bit here is already
     * overloaded - see docs/solutions/workflow-issues/waking-a-thread-by-interrupting-it-2026-08-17.md).
     */
    @Value
    public static class CommitResponse {
        CommitRequest request;

        /**
         * Non-null when the commit failed terminally - its retry budget was exhausted. The waiter rethrows it on
         * its own thread, where the {@link bz.stub.parallelconsumer.CommitFailureHandler} decision runs.
         */
        OffsetCommitBudgetExceededException commitFailure;

        public CommitResponse(CommitRequest request) {
            this(request, null);
        }

        /**
         * Stores {@code commitFailure} directly rather than copying it, which SpotBugs reports as
         * {@code EI_EXPOSE_REP2}. Intentional and safe here on both counts: the payload is effectively
         * immutable - {@link OffsetCommitBudgetExceededException}'s attempts, elapsed and message are final,
         * and its offsets map is an unmodifiable copy made in its own constructor - and the type is a
         * package-internal message on a queue with exactly one producer and one consumer, so there is no
         * caller to alias it from. An exception is not copyable in any case; the alternative to storing it is
         * losing its stack trace. Left as a comment rather than {@code @SuppressFBWarnings} because
         * {@code spotbugs-annotations} is not a dependency of this project (only the reporting plugin is) and
         * this finding is not worth adding one - the whole {@code EI_EXPOSE_REP2} group is already recorded
         * as noise in this codebase's composition style, in
         * {@code docs/inflight/static-spotbugs-latent-findings.md}.
         */
        public CommitResponse(CommitRequest request, OffsetCommitBudgetExceededException commitFailure) {
            this.request = request;
            this.commitFailure = commitFailure;
        }
    }

    /**
     * Waits for the broker-poll thread to answer a commit request.
     * <p>
     * Every exit is an affirmative <b>event</b>, never a guessed deadline. The poll thread answers with a typed
     * {@link CommitResponse} - committed, deferred, or terminally failed ({@link CommitResponse#getCommitFailure()},
     * rethrown here on the waiter's thread) - or publishes its own death through {@link #notifyPollerDied}, which
     * releases this immediately with that as the cause (the astubbs#177 / confluentinc#833 half of the story).
     * <p>
     * <b>Why there is no longer a local timeout.</b> This used to also give up on its own clock after
     * {@code offsetCommitTimeout} ("Timeout waiting for commit response"). But the poll side spends the <em>same</em>
     * option as its retry budget, on a clock that starts later (when it dequeues the request) and whose final
     * attempt may overrun the budget by up to one whole broker call - so whenever a commit was genuinely failing,
     * the waiter's deadline deterministically fired first, and the budget's terminal outcome (the event the
     * commit-failure seam, astubbs#317, exists to intercept) could never be delivered. With every real outcome
     * published affirmatively, a bare deadline asserts nothing: a poller that dies says so, a commit that fails
     * terminally says so, and a poller that is merely slow is <em>waited on</em>, bounded by its own budget rather
     * than by a second copy of the same number. The {@code commitTimeout} poll interval below is only a heartbeat
     * for logging that the wait continues. See docs/inflight/bug-offset-commit-timeout-does-two-jobs.md - this
     * settles that note's waiter half.
     * <p>
     * The {@link #ARBITRARY_RETRY_LIMIT} backstop remains what it always was: a bound on <em>draining</em> foreign
     * responses, which should never be needed.
     * <p>
     * <b>The one bound that remains, and why it is not the deadline that was removed.</b> Both affirmative events
     * are published by the poll thread <em>from the commit path</em>, so neither can arrive while that thread is
     * alive but wedged somewhere else entirely - a rebalance callback that blocks, say, which is the AB-BA cycle
     * of astubbs#29 (docs/solutions/runtime-errors/revoke-path-commit-deadlock-between-poll-and-control-threads.md;
     * the callback half is cut in {@code AbstractParallelEoSStreamProcessor#tryCommitOffsetsOnRevoke}). With
     * nothing to wait for and nobody to say so, this loop would otherwise wait forever, and PC would neither
     * commit nor report. So a last-resort backstop - {@link #pollerWedgedBackstopFloor} plus
     * {@link #POLLER_WEDGED_BACKSTOP_BUDGET_MULTIPLE} budgets, minutes rather than seconds - ends it fatally.
     * It asserts only what it can: that nothing serviced this request for the whole window. A commit that is
     * merely slow answers long before it, and budget exhaustion answers as a typed response, so this firing
     * means <em>no commit cycle is running at all</em>.
     */
    private void commitAndWait() {
        throwIfPollerDied(null);

        // request
        CommitRequest commitRequest = requestCommitInternal();

        // wait - the only ways out are our own typed response (committed, deferred, or terminally failed), the
        // poller's death, and the wedged-poller backstop below
        int foreignResponsesDrained = 0;
        final long startedWaitingAtNanos = System.nanoTime();
        final Duration backstop = pollerWedgedBackstop();
        while (true) {
            // checked at the top of every pass, so an interrupt storm (a routine wake-up here, see below)
            // cannot starve it
            Duration waited = waitedSince(startedWaitingAtNanos);
            if (isGreaterThan(waited, backstop)) {
                throw new InternalRuntimeException(msg(
                        "The broker poll thread is ALIVE but has not serviced or answered commit request {} for " +
                                "{} - the whole wedged-poller backstop window ({}). That thread is the only " +
                                "producer of commit responses, so it is stuck somewhere OUTSIDE the commit path " +
                                "(a blocked rebalance callback, or a poll that never returns) and no affirmative " +
                                "outcome can ever be published for this request. PC cannot establish whether " +
                                "these offsets committed, so this is fatal. NOTE: this is NOT the commit-failure " +
                                "seam's budget exhaustion (astubbs/parallel-consumer#317) - an exhausted budget " +
                                "arrives as a typed response and consults the configured commitFailureHandler. " +
                                "This backstop firing means nothing is being serviced at all, which no handler " +
                                "decision can answer.",
                        commitRequest, waited, backstop));
            }
            try {
                log.debug("Waiting on a commit response");
                CommitResponse take = commitResponseQueue.poll(commitTimeout.toMillis(), TimeUnit.MILLISECONDS); // blocks, drain until we find our response
                if (take != null && commitRequest.getId().equals(take.getRequest().getId())) {
                    OffsetCommitBudgetExceededException commitFailure = take.getCommitFailure();
                    if (commitFailure != null) {
                        // the commit-failure seam's typed outcome (astubbs#317): the budget was exhausted on the
                        // poll thread, which stayed alive; rethrow on this (the waiting) thread, where the
                        // CommitFailureHandler decision runs - monitor-free, once the exception has propagated
                        // out of the commitCommand block
                        throw commitFailure;
                    }
                    // Our answer arrived, so this commit HAPPENED - report it as such even if the
                    // poller died immediately afterwards of something unrelated. Checking the death
                    // first would report "request X can never be answered" about a request that was
                    // answered, which is the same kind of unestablished claim this whole change
                    // exists to remove. The death is not lost: the next commit fails fast on it, and
                    // the poller's exception still reaches the control thread through
                    // AbstractParallelEoSStreamProcessor's supervise() backstop.
                    return;
                }
                throwIfPollerDied(commitRequest);
                if (take == null) {
                    // a heartbeat, not a deadline - see the method javadoc for why waiting longer than
                    // offsetCommitTimeout here is correct
                    // recomputed: `waited` above was read before this pass blocked for a whole heartbeat interval
                    log.warn("Commit response still pending after {} waited - the broker poll thread has not died " +
                                    "with an exception, so it is still working on (or towards) this commit; " +
                                    "continuing to wait on it, bounded by its own commit retry budget rather than " +
                                    "a local deadline (last-resort wedged-poller backstop at {})",
                            waitedSince(startedWaitingAtNanos), backstop);
                } else {
                    // an older request's response, or the wake-up token: keep draining until ours arrives
                    foreignResponsesDrained++;
                    if (foreignResponsesDrained > ARBITRARY_RETRY_LIMIT) {
                        throw new InternalRuntimeException("Too many attempts taking commit responses");
                    }
                }
            } catch (InterruptedException e) {
                log.debug("Interrupted waiting for commit response", e);
            }
        }
    }

    /**
     * The whole wedged-poller backstop for one wait: {@link #POLLER_WEDGED_BACKSTOP_BUDGET_MULTIPLE} of the
     * configured budget, plus {@link #pollerWedgedBackstopFloor}. Derived per wait rather than cached, so a test
     * that shrinks the floor takes effect on the next commit.
     */
    private static Duration waitedSince(long startedWaitingAtNanos) {
        return Duration.ofNanos(System.nanoTime() - startedWaitingAtNanos);
    }

    private Duration pollerWedgedBackstop() {
        return commitTimeout.multipliedBy(POLLER_WEDGED_BACKSTOP_BUDGET_MULTIPLE)
                .plus(getPollerWedgedBackstopFloor());
    }

    /**
     * Published by the broker-poll thread from its own exit path as it dies, so that a waiter is
     * released <em>at that moment</em> rather than after {@code offsetCommitTimeout}.
     * <p>
     * Idempotent: only the first death is recorded, and the wake-up token is published only with it.
     *
     * @param cause what killed the poll thread - becomes the cause every stranded committer reports
     */
    void notifyPollerDied(Throwable cause) {
        if (pollerDeath.compareAndSet(null, cause)) {
            log.debug("Broker poll thread died - releasing any waiting committer now, and failing later ones fast", cause);
            commitResponseQueue.add(POLLER_DIED);
        }
    }

    /**
     * @param commitRequest the request that can no longer be answered, or {@code null} when checking
     *                      before one has been made
     */
    private void throwIfPollerDied(CommitRequest commitRequest) {
        Throwable death = pollerDeath.get();
        if (death == null) {
            return;
        }
        String context = commitRequest == null
                ? "no commit can be requested"
                : "request " + commitRequest + " can never be answered";
        // TODO(refactor): a user-facing failure wants a PC-named type - see
        // docs/inflight/core-exception-hierarchy-cleanup.md
        throw new InternalRuntimeException(
                "The broker poll thread has died, so {} - its own error is the cause of this one",
                death, context);
    }

    private CommitRequest requestCommitInternal() {
        CommitRequest request = new CommitRequest();
        commitRequestQueue.add(request);
        consumerMgr.wakeup();
        return request;
    }

    void maybeDoCommit() throws TimeoutException, InterruptedException {
        CommitRequest poll = commitRequestQueue.poll();
        if (poll != null) {
            log.debug("Commit requested, performing...");
            CommitResponse response;
            try {
                commitDeferringOnRebalance();
                response = new CommitResponse(poll);
            } catch (OffsetCommitBudgetExceededException budgetExhausted) {
                // The commit-failure seam's re-route (astubbs#317, confluentinc#833): budget exhaustion is a
                // commit OUTCOME for the waiter's CommitFailureHandler to decide about, not a reason for this
                // thread to die. Letting it escape here kills the broker-poll thread - the only producer of
                // commit responses - turning "this commit failed" into "the whole instance died" before any
                // handler could be consulted. So answer the waiter with the failure as a typed response on the
                // existing channel (the same move the DEFERRED case below makes; never an interrupt or a bare
                // flag) and keep polling: on a CONTINUE decision the offsets are still dirty, and the next
                // cycle re-commits them with a fresh budget.
                if (!isSync()) {
                    // no waiter to hand the decision to - the async commit mode is outside the seam (and its
                    // commitAsync path cannot throw this anyway), so preserve the historical fatal route
                    throw budgetExhausted;
                }
                response = new CommitResponse(poll, budgetExhausted);
            }
            // Only need to send a response if someone will be waiting - and send it even when the
            // commit was DEFERRED (postponed to the next cycle, not dropped - see
            // #commitDeferringOnRebalance), otherwise the requesting thread blocks waiting on a
            // commit that is not coming. It re-requests next cycle.
            if (isSync()) {
                log.debug("Adding commit response to queue...");
                commitResponseQueue.add(response);
            }
        }
    }

    /**
     * Commit, <b>deferring</b> rather than failing when the group will not accept it right now.
     * <p>
     * Two exceptions mean "this commit cannot happen", not "this consumer is broken".
     * {@link RebalanceInProgressException}: a commit landed during a rebalance, which Kafka resolves
     * by completing that rebalance on the next {@code poll()} - so it means "not yet".
     * {@link CommitFailedException}: this consumer is no longer a member of the group, so the commit
     * was rejected outright - "not by you". There are three things this code could do about either,
     * and only the third is correct:
     * <ol>
     *     <li><b>Throw</b> - let it escape. Fatal: this runs on the broker-poll thread, the only
     *         producer of commit responses, so killing it releases every waiting committer with a
     *         poller-death report (see {@link #notifyPollerDied}) and takes the whole PC instance
     *         down. Historically - before the death notification - the waiter instead sat out
     *         {@code offsetCommitTimeout} and reported "Timeout waiting for commit response", a
     *         symptom whose cause looked nothing like it.</li>
     *     <li><b>Swallow</b> - catch it and carry on. Silently wrong: it would leave
     *         {@link AbstractOffsetCommitter#retrieveOffsetsAndCommit()} free to call
     *         {@code onOffsetCommitSuccess()}, marking offsets that never reached the broker as
     *         committed. PC's bookkeeping would then disagree with the broker, and nothing would
     *         ever retry. Not hypothetical: {@link ConsumerManager} handled
     *         {@link CommitFailedException} exactly this way, its comment promising the poller would
     *         "seek commit later" while the success marking guaranteed it never would.</li>
     *     <li><b>Defer</b> - what this does. The commit is <em>postponed, not dropped</em>: the
     *         exception still aborts {@code retrieveOffsetsAndCommit()} before the success marking,
     *         so the offsets stay dirty and the next commit cycle genuinely re-commits them, by
     *         which point {@code poll()} has completed the rebalance.</li>
     * </ol>
     * That choice is why this is caught <em>here</em> and not inside
     * {@link ConsumerManager#commitSync(Map)}: one layer lower is option 2, because the success
     * marking has already happened by the time the exception would be handled.
     * <p>
     * The other half of deferring is in {@link #maybeDoCommit()}, which still sends the commit
     * response, so a waiting committer is released immediately instead of blocking for a commit that
     * is not coming. It simply asks again on the next cycle.
     * <p>
     * <b>Deferring is accounted and bounded, not free forever</b> (astubbs#317). Each deferral joins a streak
     * ({@link #deferralStreak}), cleared by a commit cycle that completes - commit or nothing-to-commit - and by a
     * completed rebalance ({@link #onPartitionsAssigned()}: after reassignment the streak belonged to an assignment
     * this consumer may no longer hold). While the streak is
     * younger than {@link #getDeferralEscalationBound()} - rebalance-scale, and deliberately NOT the shorter
     * {@code offsetCommitTimeout} the retry budget spends; that field states why -
     * deferral stays a WARN, so the deferrals of a healthy rebalance never escalate however long that
     * rebalance legitimately takes. A streak that outlives the bound has stopped being "not yet": it escalates as an
     * {@link OffsetCommitBudgetExceededException} naming the deferral cause, thrown here so it travels the seam's
     * ordinary route - {@link #maybeDoCommit()} answers the waiter with it as a typed response, and the control
     * thread's {@code CommitFailureHandler} decision loop runs (never a new interrupt or flag). Escalating starts
     * a fresh window, so a CONTINUE decision is re-consulted once per bound rather than on every deferred
     * cycle. A revocation-time cycle can escalate too - on that path the exception surfaces inside
     * the rebalance callback, where the controller's revocation catch treats it as this same deferral (no
     * waiter there, no handler, poller stays alive).
     */
    private void commitDeferringOnRebalance() throws TimeoutException, InterruptedException {
        try {
            retrieveOffsetsAndCommit();
            lastCommitCycleDeferred = false;
            clearDeferralAccounting("a commit cycle completed");
        } catch (RebalanceInProgressException e) {
            log.warn("Offset commit deferred (postponed, not dropped) - the group is rebalancing. " +
                    "These offsets are still marked as needing a commit and will be re-committed on " +
                    "the next commit cycle, once poll() has completed the rebalance.", e);
            recordDeferralAndMaybeEscalate(e, "the group is rebalancing (RebalanceInProgressException - " +
                    "resolved by poll() completing the rebalance)");
        } catch (CommitFailedException e) {
            log.warn("Offset commit deferred (postponed, not dropped) - this consumer is no longer a " +
                    "member of the group, so the commit was rejected. These offsets stay marked as " +
                    "needing a commit rather than being recorded as done, so whoever ends up owning " +
                    "the partitions resumes from where the broker actually is.", e);
            recordDeferralAndMaybeEscalate(e, "this consumer is no longer a member of the group " +
                    "(CommitFailedException - usually the consumer was evicted, e.g. it exceeded " +
                    "max.poll.interval.ms, or a rebalance completed without it)");
        }
    }

    /**
     * The accounting half of {@link #commitDeferringOnRebalance()}'s deferral bound: joins this deferral to the
     * streak, and escalates the streak once it has outlived {@link #getDeferralEscalationBound()}.
     */
    private void recordDeferralAndMaybeEscalate(RuntimeException deferralCause, String causeDescription) {
        lastCommitCycleDeferred = true;
        Instant now = Instant.now();
        // one swap, and everything below reads the generation it produced - a clear racing this from another
        // thread can no longer leave the start and the count disagreeing (see the field's javadoc)
        DeferralStreak streak = deferralStreak.updateAndGet(previous -> previous.plusDeferral(now));
        Duration elapsed = streak.age(now);
        Duration escalationBound = getDeferralEscalationBound();
        if (!isGreaterThan(elapsed, escalationBound)) {
            // inside the bound: the healthy-rebalance case - which legitimately lasts as long as a rebalance -
            // so deferral stays a WARN
            return;
        }
        if (!isSync()) {
            // no waiter to hand a decision to - the async commit mode is outside the seam (and in practice its
            // commitAsync path never throws the deferral exceptions synchronously), so keep its historical
            // WARN-and-carry-on disposition rather than inventing a new fatal route for it
            return;
        }
        // one whole bound of uninterrupted deferrals: surface it to the decision loop, and restart the clock so
        // a CONTINUE decision buys another full bound rather than an escalation every cycle
        int deferralsThisWindow = streak.getDeferrals();
        deferralStreak.set(DeferralStreak.restartedAt(now));
        throw new OffsetCommitBudgetExceededException(msg(
                "Offset commit DEFERRED continuously for {} - longer than the rebalance-scale deferral " +
                        "escalation bound of {} - across {} consecutive deferral(s), because {}. A deferral is " +
                        "postponement, not failure, and a rebalance's worth of them is normal, which is why the " +
                        "bound is a whole rebalance long (the default matches Kafka's max.poll.interval.ms " +
                        "default, NOT the shorter offsetCommitTimeout the retry budget spends) - but a streak " +
                        "outliving even that is not healing on its own, so it stops being silent. The offsets " +
                        "are still marked as needing a commit. What happens next is the configured " +
                        "commitFailureHandler's decision (astubbs/parallel-consumer#317) - the default policy " +
                        "shuts PC down (fail fast), and CommitFailurePolicies has canned alternatives.",
                elapsed, escalationBound, deferralsThisWindow, causeDescription),
                deferralCause, deferralsThisWindow, elapsed, lastAttemptedOffsets);
    }

    private void clearDeferralAccounting(String reason) {
        // clear first, then report what was cleared: the captured generation is the streak that actually ended,
        // where re-reading the field could report a count another thread has since moved
        DeferralStreak ended = deferralStreak.getAndSet(DeferralStreak.cleared());
        if (ended.isRunning()) {
            log.debug("Rebalance-deferral accounting cleared after {} deferral(s): {}", ended.getDeferrals(), reason);
        }
    }

    /**
     * A completed rebalance - partitions reassigned - scopes the deferral streak to the assignment it belonged to
     * (astubbs#317): whatever was blocking commits for the OLD assignment is history the new one must not
     * inherit, so the next streak gets the full escalation quantum on a fresh clock. Forwarded by the controller's
     * own {@code onPartitionsAssigned} through {@link BrokerPollSystem#onPartitionsAssigned()}.
     */
    void onPartitionsAssigned() {
        lastCommitCycleDeferred = false;
        clearDeferralAccounting("the rebalance completed - partitions were reassigned");
    }

    /**
     * @see OffsetCommitter#lastCommitWasDeferred() - reported to the controller through
     *         {@link BrokerPollSystem#lastCommitWasDeferred()}
     */
    @Override
    public boolean lastCommitWasDeferred() {
        return lastCommitCycleDeferred;
    }

    public boolean isSync() {
        return commitMode.equals(PERIODIC_CONSUMER_SYNC);
    }

    public void claim() {
        owningThread = Optional.of(Thread.currentThread());
    }
}