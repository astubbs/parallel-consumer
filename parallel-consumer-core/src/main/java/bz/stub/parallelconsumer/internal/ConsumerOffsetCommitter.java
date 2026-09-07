package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.internal.utils.RecordBatchSummary;
import bz.stub.parallelconsumer.state.WorkManager;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RebalanceInProgressException;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;

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

    private final CommitMode commitMode;

    private final Duration commitTimeout;

    /** Written by the broker-poll thread in {@link #claim()}, read by the CONTROL thread - by
     * {@code isOwner()} and, on the timeout path, to diagnose why the poll thread is not answering.
     * Volatile so that read cannot see a stale empty and report "no poll thread has claimed this
     * committer" about a committer that was claimed. */
    private volatile Optional<Thread> owningThread = Optional.empty();

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
        switch (commitMode) {
            case PERIODIC_CONSUMER_SYNC -> {
                log.debug("Committing offsets Sync");
                consumerMgr.commitSync(offsetsToSend);
            }
            case PERIODIC_CONSUMER_ASYNCHRONOUS -> {
                log.debug("Committing offsets Async");
                consumerMgr.commitAsync(offsetsToSend, this::onAsyncCommitAnswered);
            }
            default ->
                    throw new IllegalArgumentException("Cannot use " + commitMode + " when using " + this.getClass().getSimpleName());
        }
    }

    /**
     * {@code commitAsync} returns as soon as the request is handed to the client, so in that mode - and only that
     * mode - the offsets are recorded as committed by {@link #onAsyncCommitAnswered}, when the broker answers.
     * {@code commitSync} blocks until the broker has answered, so the base class's inline marking is correct
     * there and is left untouched.
     *
     * @see AbstractOffsetCommitter#commitOffsetsReturnsOnlyOnceAcknowledged()
     */
    @Override
    protected boolean commitOffsetsReturnsOnlyOnceAcknowledged() {
        return isSync();
    }

    /**
     * The broker's answer to one {@code commitAsync} request: the moment the offsets it carried become durable,
     * or fail to.
     * <p>
     * <b>Success is recorded here rather than at the send</b>, which is the whole point. Previously
     * {@link AbstractOffsetCommitter#retrieveOffsetsAndCommit()} marked the partition clean as soon as
     * {@code commitAsync} returned, so a failure arriving in this callback - or no callback at all - had no
     * dirty state left to retry: nothing was re-committed and the broker's position silently stayed behind
     * PC's. Now every route out of a commit that is not an acknowledged success leaves the offsets dirty, and
     * the next commit cycle re-sends them.
     * <p>
     * <b>A failure is a DEFERRAL, not a swallow</b>, and lands in the same handling the synchronous path's
     * rejections do (see {@link #commitDeferringOnRebalance()}, option 3): logged, not fatal, and the offsets
     * stay marked as needing a commit. That is right for the failure {@code commitAsync} is specified to
     * report - a {@link org.apache.kafka.clients.consumer.RetriableCommitFailedException} for a coordinator
     * that was unavailable, timed out or was mid-rebalance - and it is also the conservative answer for any
     * other exception: re-committing an offset the broker already has costs one request, whereas recording a
     * commit that did not happen loses records. A non-retriable failure is not escalated any further because
     * the async mode has no commit budget and so cannot reach the commit-failure seam (astubbs#317) at all -
     * see below.
     * <p>
     * <b>This committer keeps no record of what it has in flight, deliberately.</b> Deferring the clean-marking is
     * what makes two async commits able to be in flight at once - before this change the first send marked the
     * partition clean, so there was never a second - and an answer can therefore arrive for a request a later one
     * has partly overtaken. Deciding that here would mean tracking, per partition, an offset the partition already
     * knows: {@code PartitionState} hands out the offset it wants committed and can recognise the answer to its own
     * latest offer. So an acknowledgement is passed straight through, whole, and the partition decides whether it
     * ends the story - {@code PartitionState}'s {@code offsetLastOfferedForCommit} owns that rule. A request that is
     * the newest word on one partition and superseded on another needs no special case here, because nothing here
     * is deciding per request.
     *
     * @param offsets   the offsets the answered request carried
     * @param exception {@code null} if and only if the broker acknowledged the commit
     */
    private void onAsyncCommitAnswered(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        if (exception != null) {
            // WARN rather than ERROR: a request really did fail, which is worth seeing, but nothing is lost and
            // nothing needs an operator tonight - the partitions were never marked clean, so they are still dirty
            // and a later request carries the same offsets.
            //
            // Every partition and offset stays on both of these lines - astubbs#168 (confluentinc#629) asked for
            // exactly them - and only the metadata string is reduced, to its length: it is PC's encoded
            // offset map, up to OffsetMapCodecManager.DefaultMaxMetadataSize of base64 PER PARTITION, and
            // interpolating the map rendered all of it on the one line that most needs to survive log
            // truncation. The map in full is one level down, where it has to be asked for.
            log.warn("Async offset commit failed - these partitions stay dirty and are committed when a later " +
                            "request is acknowledged. Offsets: {}, exception: ",
                    RecordBatchSummary.summariseCommit(offsets), exception);
            log.debug("Failed commit in full: {}", offsets);
            return;
        }

        log.debug("Async commit acknowledged by the broker: {}", RecordBatchSummary.summariseCommit(offsets));
        onOffsetCommitSuccess(offsets);
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
     * Commit response message, linked to a {@link CommitRequest}
     */
    @Value
    public static class CommitResponse {
        CommitRequest request;
    }

    /**
     * Waits for the broker-poll thread to answer a commit request.
     * <p>
     * The two ways this does not return normally are now genuinely different things, which is the
     * point of astubbs#177 / confluentinc#833. A <b>dead</b> poller is an event: it publishes its own
     * exception through {@link #notifyPollerDied} as it dies and this returns immediately with that
     * as the cause - it is never waited out. A <b>timeout</b> therefore means what it says: the poller
     * is alive and has not answered within {@code offsetCommitTimeout}. Neither message guesses at the
     * other's cause; guessing is what sent users looking in the wrong place for years.
     */
    private void commitAndWait() {
        throwIfPollerDied(null);

        // request
        CommitRequest commitRequest = requestCommitInternal();

        // wait - the only way out is our own response arriving, a death, or a timeout
        for (int attempts = 0; attempts <= ARBITRARY_RETRY_LIMIT; attempts++) {
            try {
                log.debug("Waiting on a commit response");
                CommitResponse take = commitResponseQueue.poll(commitTimeout.toMillis(), TimeUnit.MILLISECONDS); // blocks, drain until we find our response
                if (take != null && commitRequest.getId().equals(take.getRequest().getId())) {
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
                    // report the timeout actually waited (offsetCommitTimeout). This used to
                    // interpolate the unrelated constant DEFAULT_TIMEOUT, so every one of these
                    // errors claimed PT30S no matter what the option was set to - overstating the
                    // default by 3x and making the number useless as a diagnostic.
                    // TODO(refactor): a user-facing failure wants a PC-named type, not "internal runtime" -
                    // see docs/inflight/core-exception-hierarchy-cleanup.md
                    // "blocked or slower" is two opposite defects with opposite responses, and the
                    // message alone could never say which - so every such failure was triaged by
                    // arguing from preconditions. Look, now, while the thread is still parked: after
                    // the throw the evidence is gone. See PollThreadStallDiagnosis for the incident.
                    String diagnosis = owningThread
                            .map(PollThreadStallDiagnosis::diagnose)
                            .orElse("UNAVAILABLE - no poll thread has claimed this committer yet");
                    throw PCInternalRuntimeException.msg(
                            "Timeout waiting for commit response {} to request {} - the broker poll thread is the " +
                                    "only producer of commit responses, and it has not died with an exception, so it is " +
                                    "not answering: it is blocked or slower than the configured offsetCommitTimeout. Had " +
                                    "it thrown, that would have been reported here immediately, with its own error as the " +
                                    "cause. An Error rather than an Exception escapes that path and is reported by " +
                                    "AbstractParallelEoSStreamProcessor's supervise() backstop instead." +
                                    " POLL THREAD AT TIMEOUT: {}",
                            commitTimeout, commitRequest, diagnosis);
                }
                // an older request's response, or the wake-up token: keep draining until ours arrives
            } catch (InterruptedException e) {
                log.debug("Interrupted waiting for commit response", e);
            }
        }
        throw new PCInternalRuntimeException("Too many attempts taking commit responses");
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
        throw new PCInternalRuntimeException(
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
            commitDeferringOnRebalance();
            // Only need to send a response if someone will be waiting - and send it even when the
            // commit was DEFERRED (postponed to the next cycle, not dropped - see
            // #commitDeferringOnRebalance), otherwise the requesting thread blocks for the full
            // offsetCommitTimeout waiting on a commit that is not coming. It re-requests next cycle.
            if (isSync()) {
                log.debug("Adding commit response to queue...");
                commitResponseQueue.add(new CommitResponse(poll));
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
     *         producer of commit responses, so killing it strands every waiting committer until
     *         {@code offsetCommitTimeout} and then takes the whole PC instance down. This is the
     *         "Timeout waiting for commit response" symptom, whose cause looks nothing like it.</li>
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
     */
    private void commitDeferringOnRebalance() throws TimeoutException, InterruptedException {
        try {
            retrieveOffsetsAndCommit();
        } catch (RebalanceInProgressException e) {
            log.warn("Offset commit deferred (postponed, not dropped) - the group is rebalancing. " +
                    "These offsets are still marked as needing a commit and will be re-committed on " +
                    "the next commit cycle, once poll() has completed the rebalance.", e);
        } catch (CommitFailedException e) {
            log.warn("Offset commit deferred (postponed, not dropped) - this consumer is no longer a " +
                    "member of the group, so the commit was rejected. These offsets stay marked as " +
                    "needing a commit rather than being recorded as done, so whoever ends up owning " +
                    "the partitions resumes from where the broker actually is.", e);
        }
    }

    public boolean isSync() {
        return commitMode.equals(PERIODIC_CONSUMER_SYNC);
    }

    /**
     * Records the broker-poll thread as this committer's owner.
     * <p>
     * <b>Called exactly once, as a METHOD REFERENCE</b> - {@code committer.ifPresent(ConsumerOffsetCommitter::claim)}
     * in {@code BrokerPollSystem}'s control loop, on the poll thread itself, before the loop starts.
     * That spelling is invisible to a grep for {@code .claim(}, which returns zero hits: a reviewer
     * reading this file can conclude the owner is never set and that everything depending on it -
     * {@code isOwner()}, and the timeout-path diagnosis - is dead code. It is not. Named here because
     * the same wrong conclusion has now been reached once.
     */
    public void claim() {
        owningThread = Optional.of(Thread.currentThread());
    }
}