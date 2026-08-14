package bz.stub.parallelconsumer.client;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * The ordering guarantee the user asks Parallel Consumer for - the wrapper-level mirror of core's
 * {@code ParallelConsumerOptions.ProcessingOrder}, redeclared here because this module depends on nothing (the
 * dependency arrows all point <em>into</em> the shared API, per the language-proxy plan's Output Structure).
 * Each transport maps it onto its own lower layer; ordering is enforced by the engine, never by the wrapper
 * (R2).
 *
 * @author Antony Stubbs
 */
public enum ProcessingOrder {
    /** No ordering: any record may process concurrently with any other. */
    UNORDERED,
    /** Records of one partition process one at a time, in offset order. */
    PARTITION,
    /** Records sharing a key process one at a time, in offset order; distinct keys proceed concurrently. */
    KEY
}
