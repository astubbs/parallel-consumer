package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.state.WorkManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.concurrent.TimeoutException;

@Slf4j
@RequiredArgsConstructor
public abstract class AbstractOffsetCommitter<K, V> implements OffsetCommitter {

    protected final ConsumerManager<K, V> consumerMgr;
    protected final WorkManager<K, V> wm;

    /**
     * Get offsets from {@link WorkManager} that are ready to commit
     */
    @Override
    public void retrieveOffsetsAndCommit() throws TimeoutException, InterruptedException {
        log.debug("Find completed work to commit offsets");
        preAcquireOffsetsToCommit();
        try {
            var offsetsToCommit = wm.collectCommitDataForDirtyPartitions();
            if (offsetsToCommit.isEmpty()) {
                log.debug("No offsets ready");
            } else {
                log.debug("Will commit offsets for {} partition(s): {}", offsetsToCommit.size(), offsetsToCommit);
                ConsumerGroupMetadata groupMetadata = consumerMgr.groupMetadata();

                log.debug("Begin commit offsets");
                commitOffsets(offsetsToCommit, groupMetadata);

                if (commitOffsetsReturnsOnlyOnceAcknowledged()) {
                    log.debug("On commit success");
                    onOffsetCommitSuccess(offsetsToCommit);
                } else {
                    log.debug("Commit request sent; it is recorded as successful when the broker acknowledges it, " +
                            "not here - the offsets stay dirty until then");
                }
            }
        } finally {
            postCommit();
        }
    }

    protected void postCommit() {
        // default noop
    }

    protected void preAcquireOffsetsToCommit() throws TimeoutException, InterruptedException {
        // default noop
    }

    /**
     * Whether {@link #commitOffsets} returning normally means the broker has <b>acknowledged</b> the commit.
     * <p>
     * True for every blocking committer - {@code commitSync} and the transactional producer's
     * {@code sendOffsetsToTransaction}/{@code commitTransaction} pair both return only once the broker has
     * answered, so the success marking directly below the call is a statement about something that has already
     * happened. It is false for exactly one path, {@code Consumer#commitAsync}, which returns as soon as the
     * request is handed to the client and answers later through an {@code OffsetCommitCallback}. A committer
     * returning false takes on the obligation to call {@link #onOffsetCommitSuccess} itself, from whatever
     * acknowledgement it does get.
     * <p>
     * Overriding this is the whole of the fix for
     * {@code docs/solutions/logic-errors/an-async-commit-was-recorded-on-send-not-on-acknowledgement-2026-09-07.md}:
     * marking clean on send left a failed or dropped acknowledgement with no dirty state to retry, so the
     * offsets were never re-committed and the broker's position silently stayed behind PC's.
     */
    protected boolean commitOffsetsReturnsOnlyOnceAcknowledged() {
        return true;
    }

    /**
     * Records offsets the broker has acknowledged: each partition's last committed offset advances, and the
     * partition is marked clean if the offset acknowledged is the one it last offered for commit.
     * <p>
     * Protected rather than private so an asynchronous committer - one whose
     * {@link #commitOffsetsReturnsOnlyOnceAcknowledged()} is false - can call it at the moment the
     * acknowledgement actually arrives. Such a committer can have two requests in flight at once, and needs to know
     * nothing about that: each partition offered the offset, so each partition recognises the answer to its own
     * latest offer and declines to clean on an older one. {@code PartitionState}'s
     * {@code offsetLastOfferedForCommit} owns that rule.
     */
    protected void onOffsetCommitSuccess(final Map<TopicPartition, OffsetAndMetadata> committed) {
        wm.onOffsetCommitSuccess(committed);
    }

    protected abstract void commitOffsets(final Map<TopicPartition, OffsetAndMetadata> offsetsToSend, final ConsumerGroupMetadata groupMetadata);

}
