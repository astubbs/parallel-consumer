package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * What the instance does when a commit exhausts its retry budget: the one instance-wide setting beside the commit
 * mode, by the same line - it is a property of the one commit, not of the work (KD11, R6).
 * <p>
 * <b>Not yet wired.</b> The seam it needs is astubbs#352, which has not landed, so
 * {@link ParallelConsumerDefinition#commitFailure} refuses rather than storing a value that would do nothing. The
 * type is here so that refusal can name the policy the caller asked for.
 */
@InterfaceStability.Unstable
public enum CommitFailurePolicy {

    /**
     * Stop the instance - today's behaviour.
     */
    SHUT_DOWN,

    /**
     * Log and keep processing, accepting that the offsets will be committed later or replayed.
     */
    KEEP_GOING
}
