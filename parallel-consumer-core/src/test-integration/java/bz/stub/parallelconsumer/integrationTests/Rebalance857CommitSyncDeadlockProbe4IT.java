package bz.stub.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.RepeatedTest;

/**
 * Repetitions 16-20 of the twenty that {@link Rebalance857CommitSyncDeadlockProbeBase}
 * documents. One of four sibling classes carrying five each, so failsafe's forks can run them in
 * parallel - forks pull whole classes from one queue, so a single class is never split across
 * them, and at twenty repetitions in one class this instrument was the hard floor under the whole
 * integration gate. Read the base class for the mechanism, the calibration status, and what a
 * green cell means.
 */
class Rebalance857CommitSyncDeadlockProbe4IT extends Rebalance857CommitSyncDeadlockProbeBase {

    @RepeatedTest(5)
    void revokeWhileControlThreadMidCommitMustNotDeadlockOrKillTheConsumer() {
        runOneProbeIteration();
    }
}
