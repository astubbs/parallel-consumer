package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Builder;
import lombok.Value;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.time.Duration;
import java.util.Map;

/**
 * Everything a {@link CommitFailureHandler} gets to see about a terminally failed commit: the failure itself, the
 * offsets that could not be committed, and the history clocks a policy graduates on.
 * <p>
 * Deliberately a pure value: the canned {@link CommitFailurePolicies} are driven entirely by these fields (no wall
 * clock), which is what makes them - and user handlers - unit-testable without threads.
 *
 * @author Antony Stubbs
 * @see CommitFailureHandler
 * @see CommitFailurePolicies
 */
@Value
@Builder
@InterfaceStability.Evolving
public class CommitFailureContext {

    /**
     * The terminal failure of this commit - typically an {@link OffsetCommitBudgetExceededException} wrapping the
     * broker's own exception as its cause.
     */
    Throwable failure;

    /**
     * The offsets that were in play - what the failed commit was trying to commit.
     */
    Map<TopicPartition, OffsetAndMetadata> offsets;

    /**
     * How many commit attempts were made within the budget that this failure exhausted.
     */
    int attemptsMade;

    /**
     * How long was spent inside the failed commit cycle - from the first attempt of the exhausted budget to giving
     * up.
     */
    Duration elapsed;

    /**
     * How many budgets in a row have now been exhausted without an intervening successful commit, <b>including this
     * one</b> - so the first failure after a success (or after assignment) is {@code 1}. Resets to zero on a
     * successful commit.
     */
    int consecutiveExhaustedBudgets;

    /**
     * How long since a commit last succeeded in the current assignment.
     * <p>
     * <b>The epoch rule:</b> when no commit has <em>ever</em> succeeded in this assignment, this is measured from
     * assignment start instead - there is no immortal "never succeeded" state that time-based bounds cannot reach.
     */
    Duration timeSinceLastSuccessfulCommit;

    /**
     * The {@link ParallelConsumerOptions.CommitMode} in force.
     */
    ParallelConsumerOptions.CommitMode commitMode;

    /**
     * Which assignment this failure belongs to - incremented on each rebalance-driven assignment change. Lets a
     * stateful handler notice that history predating the current assignment no longer applies (the canned bounded
     * policy resets its rolling window on a change).
     */
    long assignmentEpoch;
}
