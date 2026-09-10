package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * The two things the dispatch wrapper needs to say about the <em>instance</em> rather than about a record, and the
 * one seam the lifecycle unit replaces.
 * <p>
 * A worker cannot close the engine it runs in - the close awaits the worker pool, so a function that closed from
 * inside itself would wait for itself - which is why both of these are a request to somebody else's thread rather
 * than a call the wrapper makes directly (KTD6).
 *
 * @see ConsumerHandle the implementation the definition wires in at start
 */
interface InstanceControl {

    /**
     * This instance cannot run: something is wrong with the definition itself, not with a record, and every
     * subsequent record would fail identically. Today the only one is a pre-built consumer that is not configured
     * for raw bytes (KTD3, R1).
     * <p>
     * The implementation must not block the calling thread - it is a worker thread, inside the user function's
     * failure path.
     */
    void fatal(Throwable definitionFault);

    /**
     * A route's function reported the stop outcome for this record (R24).
     * <p>
     * <b>Seam for the lifecycle unit.</b> The full behaviour is: mark the instance stopping so no further record is
     * run, call the engine's non-blocking pause so no further record is dispatched, then close on the declared
     * close path from the handle's own thread and record the reason so that awaiting caller can tell a stop from a
     * close from a failure (KTD6). Until that lands the wrapper still hands the record back incomplete, so nothing
     * is silently completed, and the record is delivered again after a restart.
     */
    void stopRequested(ConsumerRecord<byte[], byte[]> record, String reason);
}
