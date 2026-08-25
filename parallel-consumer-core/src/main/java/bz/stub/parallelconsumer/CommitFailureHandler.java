package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * The seam for deciding what Parallel Consumer does when an offset commit has failed terminally - its configured
 * retry budget was spent without the commit succeeding (the situation
 * {@link OffsetCommitBudgetExceededException} describes).
 * <p>
 * Historically that always shut PC down; Kafka's own client throws a retriable exception and lets the caller choose.
 * This handler closes that gap (astubbs#317, confluentinc#833): PC hands the decision to the application, with a
 * {@link CommitFailureContext} carrying the history needed to make it.
 * <p>
 * Canned implementations live in {@link CommitFailurePolicies}; the default is
 * {@link CommitFailurePolicies#shutDown()}. Configure via
 * {@link ParallelConsumerOptions.ParallelConsumerOptionsBuilder#commitFailureHandler}.
 * <p>
 * The handler is invoked from PC's control thread only - one invocation at a time, in failure order - so stateful
 * implementations (like the bounded canned policy) need no synchronisation of their own beyond that guarantee.
 *
 * @author Antony Stubbs
 * @see CommitFailurePolicies
 * @see CommitFailureContext
 */
@InterfaceStability.Evolving
@FunctionalInterface
public interface CommitFailureHandler {

    /**
     * Decide how PC reacts to a terminally failed commit.
     *
     * @param context the failure and its history - see {@link CommitFailureContext}
     * @return {@link CommitFailureDecision#SHUT_DOWN} to fail fast, {@link CommitFailureDecision#CONTINUE} to carry
     *         on and let PC retry the commit on its next commit cycle
     */
    CommitFailureDecision onCommitFailure(CommitFailureContext context);

    /**
     * The decision a {@link CommitFailureHandler} returns.
     */
    enum CommitFailureDecision {

        /**
         * Fail fast: shut PC down, surfacing the commit failure as the cause. The historical (and default)
         * behaviour.
         */
        SHUT_DOWN,

        /**
         * Keep running: PC continues and retries the commit on its next commit cycle. What happens to record intake
         * meanwhile is governed by
         * {@link ParallelConsumerOptions.ParallelConsumerOptionsBuilder#commitFailureContinueMode}.
         */
        CONTINUE
    }
}
