package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.concurrent.TimeoutException;

/**
 * Contract for committing offsets. As there are two ways to commit offsets - through the Consumer or Producer, and
 * several systems involved, we need a contract.
 *
 * @author Antony Stubbs
 */
public interface OffsetCommitter {
    void retrieveOffsetsAndCommit() throws TimeoutException, InterruptedException;

    /**
     * Whether the commit cycle that just returned on this thread ended in a rebalance-class DEFERRAL - postponed to
     * the next cycle, offsets still dirty - rather than reaching the broker (astubbs#317, R8). A caller's success
     * accounting must not treat such a cycle as a success: the commit-failure seam's
     * time-since-last-successful-commit story, consecutive-failure count and pause release all key off genuine
     * successes only.
     * <p>
     * Meaningful for callers that just drove a commit cycle to completion on this thread (the controller does,
     * under its commit serialization); in modes with no waiter (async) it is best-effort. Default {@code false}:
     * only the consumer-side committer defers - see
     * {@code ConsumerOffsetCommitter#commitDeferringOnRebalance} for what qualifies and why.
     */
    default boolean lastCommitWasDeferred() {
        return false;
    }
}
