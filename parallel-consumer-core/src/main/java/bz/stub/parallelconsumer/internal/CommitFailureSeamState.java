package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Getter;

/**
 * The commit-failure seam's observable state (astubbs#317, confluentinc#833), reported by the
 * {@code pc.commit.failure.seam.state} gauge so a dashboard can tell a healthy instance from one that is alive only
 * because a CONTINUE decision keeps it so. Derived on observation from the seam's accounting in
 * {@link AbstractParallelEoSStreamProcessor} - never stored - so it cannot drift from the behaviour it reports.
 */
public enum CommitFailureSeamState {

    /**
     * No commit-failure streak is active: the last commit cycle completed without a terminal failure (or none has
     * run yet this assignment).
     */
    HEALTHY(0),

    /**
     * At least one commit retry budget has been exhausted with no successful commit since, and a CONTINUE decision
     * is keeping the instance processing - completed work is NOT being committed, so a restart or rebalance in this
     * state reprocesses it.
     */
    FAILING_CONTINUING(1),

    /**
     * As {@link #FAILING_CONTINUING}, and the seam's pause
     * ({@link bz.stub.parallelconsumer.ParallelConsumerOptions.CommitFailureContinueMode#PAUSE_INTAKE}) is holding
     * back new work until a commit succeeds; in-flight work still completes.
     */
    FAILING_PAUSED(2);

    // Enum value used for metrics - deterministic as opposed to ordinal to prevent change on adding / removing enum constants
    @Getter
    private final int value;

    CommitFailureSeamState(int value) {
        this.value = value;
    }
}
