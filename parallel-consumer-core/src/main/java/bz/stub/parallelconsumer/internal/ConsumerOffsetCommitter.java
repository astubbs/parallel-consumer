package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
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

    private Optional<Thread> owningThread = Optional.empty();

    /**
     * Queue of commit requests from other threads
     */
    private final Queue<CommitRequest> commitRequestQueue = new ConcurrentLinkedQueue<>();

    /**
     * Queue of commit responses, for other threads to block on
     */
    private final BlockingQueue<CommitResponse> commitResponseQueue = new LinkedBlockingQueue<>();

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
     * Commit response message, linked to a {@link CommitRequest}
     */
    @Value
    public static class CommitResponse {
        CommitRequest request;
    }

    private void commitAndWait() {
        // request
        CommitRequest commitRequest = requestCommitInternal();

        // wait
        boolean waitingOnCommitResponse = true;
        int attempts = 0;
        while (waitingOnCommitResponse) {
            if (attempts > ARBITRARY_RETRY_LIMIT)
                throw new InternalRuntimeException("Too many attempts taking commit responses");

            try {
                log.debug("Waiting on a commit response");
                Duration timeout = AbstractParallelEoSStreamProcessor.DEFAULT_TIMEOUT;
                CommitResponse take = commitResponseQueue.poll(commitTimeout.toMillis(), TimeUnit.MILLISECONDS); // blocks, drain until we find our response
                if (take == null) {
                    throw InternalRuntimeException.msg("Timeout waiting for commit response {} to request {}", timeout, commitRequest);
                }
                waitingOnCommitResponse = take.getRequest().getId() != commitRequest.getId();
            } catch (InterruptedException e) {
                log.debug("Interrupted waiting for commit response", e);
            }
            attempts++;
        }
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

    public void claim() {
        owningThread = Optional.of(Thread.currentThread());
    }
}