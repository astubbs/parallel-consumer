package bz.stub.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * The twenty repetitions of {@link Rebalance857CommitSyncDeadlockProbeBase}, as four classes of
 * five so failsafe's forks can run them in parallel. Each inherits the parent's
 * {@code @RepeatedTest(5)} method and adds nothing - the body exists once, in the parent.
 * <p>
 * <b>Why four classes and not {@code @RepeatedTest(20)}.</b> Surefire and failsafe schedule whole
 * CLASSES from one queue, so a class is never split across forks. At twenty repetitions in one
 * class this instrument measured ~356s and set the floor for whichever shard held it - the single
 * largest term in the integration gate, and one no fork count could lower. Split, each is ~140s.
 * <p>
 * <b>Why one file.</b> Java allows several package-private top-level classes per file, and failsafe
 * collects compiled CLASSES rather than source filenames, so all four schedule independently
 * exactly as if they were separate files. They are one idea, and four files holding one line each
 * would be duplication for the filesystem's benefit rather than a reader's.
 * <p>
 * Read the base class for the mechanism, the calibration status, and what a green cell means.
 */
class Rebalance857CommitSyncDeadlockProbeIT extends Rebalance857CommitSyncDeadlockProbeBase {}

class Rebalance857CommitSyncDeadlockProbe2IT extends Rebalance857CommitSyncDeadlockProbeBase {}

class Rebalance857CommitSyncDeadlockProbe3IT extends Rebalance857CommitSyncDeadlockProbeBase {}

class Rebalance857CommitSyncDeadlockProbe4IT extends Rebalance857CommitSyncDeadlockProbeBase {}
