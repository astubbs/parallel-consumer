package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.clients.consumer.CommitFailedException;

/**
 * A rejected {@link CommitFailedException} commit must not be recorded as a successful one.
 * <p>
 * Kafka throws it when this consumer is no longer a member of the group - typically because a
 * rebalance completed while work was in flight - so the commit was rejected outright. Unlike its
 * sibling {@link org.apache.kafka.common.errors.RebalanceInProgressException} (see
 * {@link MockConsumerRebalanceInProgressTest}) this one was never fatal, which is what let it hide:
 * {@code ConsumerManager.commitSync} caught it, logged, and returned normally. Its own comment said
 * the poller would "seek commit later", but returning normally meant
 * {@code AbstractOffsetCommitter.retrieveOffsetsAndCommit()} carried straight on to
 * {@code onOffsetCommitSuccess()} and marked the offsets clean. Nothing was ever going to re-commit
 * them, because as far as PC was concerned they already had been.
 * <p>
 * So the failure mode is the opposite shape to #100's: no exception, no dead thread, no stall - just
 * offsets quietly recorded at a position the broker never accepted. Whoever ends up owning those
 * partitions resumes from the broker's older offset, and the records in between are re-delivered
 * having already been marked done. Handling it beside the rebalance case in
 * {@code ConsumerOffsetCommitter} leaves the offsets dirty, so the next commit cycle retries them.
 * <p>
 * The scenario lives in {@link CommitRejectionTestBase}; only the exception differs.
 */
class MockConsumerCommitFailedTest extends CommitRejectionTestBase {

    @Override
    protected RuntimeException rejection() {
        return new CommitFailedException();
    }
}
