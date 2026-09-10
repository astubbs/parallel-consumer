package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * The two things the dispatch wrapper needs to say about the <em>instance</em> rather than about a record.
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
     * A route asked the instance to stop for this record - from its function's {@link Outcome#stop(String)}, or
     * because the record ran out of attempts on a route whose reaction is {@link AfterRetries#stop()} (R24, R27).
     * <p>
     * The caller has already handed the record back never-due, so a drain will not re-invoke it. What is left is
     * the part a worker thread cannot do: record the reason, pause the engine - which stops both the controller
     * handing out work and the batches already queued in the pool - and close on the declared close path from a
     * thread of the implementation's own (KTD6).
     * <p>
     * The implementation must not block the calling thread - it is a worker thread, and the close it starts awaits
     * the worker pool this thread belongs to.
     */
    void stopRequested(ConsumerRecord<byte[], byte[]> record, String reason);
}
