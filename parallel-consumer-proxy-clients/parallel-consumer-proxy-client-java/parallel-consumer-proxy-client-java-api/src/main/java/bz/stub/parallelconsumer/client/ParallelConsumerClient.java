package bz.stub.parallelconsumer.client;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * The client wrapper: Parallel Consumer's user-facing surface, one layer for every language (the language-proxy
 * plan's KTD1, astubbs#242). A user builds {@link ClientOptions}, obtains a transport-bound client, and hands
 * {@link #poll} a {@link RecordProcessor}; the wrapper delivers each record to the processor exactly as an
 * in-process Parallel Consumer user function would see it, and applies the processor's per-record
 * {@link Outcome}.
 * <p>
 * <b>This interface is transport-free, deliberately.</b> Two Java transports implement it - the direct one
 * binds {@code parallel-consumer-core} in-process with no protocol underneath, the gRPC one speaks to the
 * sidecar proxy over the wire - and the same conformance suite drives both (KTD20). Anything only one
 * transport can express is a leak and must not appear here: no epochs, no tokens, no connection state. Nine
 * other languages mirror this surface, so it stays small enough for a language with no generics, no
 * exceptions, or no closures.
 * <p>
 * Keys and values are {@code byte[]}: the proxy never deserializes, so deserialization belongs to the user's
 * own code in the user's own language - and the direct transport, the degenerate case, keeps the same shape so
 * the surfaces stay identical.
 *
 * @author Antony Stubbs
 * @see ClientOptions
 * @see RecordProcessor
 */
public interface ParallelConsumerClient extends AutoCloseable {

    /**
     * Starts consumption: subscribes per the client's {@link ClientOptions} and hands every delivered record to
     * the given processor, honouring the configured concurrency and ordering. Non-blocking - it returns once
     * consumption is running, mirroring core's own poll-with-a-function shape.
     * <p>
     * May be called at most once per client instance, and never together with {@link #pollAsync}.
     */
    void poll(RecordProcessor processor);

    /**
     * The same as {@link #poll}, with the processor's verdict arriving through a {@link java.util.concurrent
     * .CompletionStage} instead of a return - for a processor whose work is a remote call, a reactive
     * pipeline, or a coroutine, where the synchronous form would park a wrapper-owned thread that has nothing
     * to do.
     * <p>
     * <b>It is on this interface, and not only on a transport, because it is what makes a thin client in
     * another language possible.</b> A wrapper over a Java transport can bridge a {@code CompletionStage} to
     * its own idiom - {@code suspend}, {@code Future}, {@code IO} - without a thread parked per record; over
     * the synchronous form it cannot, and the language then reimplements the whole session to avoid the cost.
     * Three JVM session implementations mean fixing every session defect three times, so the shape that
     * removes the reason to write the second one belongs here rather than in one transport's corner.
     * <p>
     * The two forms are the same session with the same conformance contract - see
     * {@link AsyncRecordProcessor} for the two places their behaviour genuinely differs, both consequences of
     * nothing blocking: concurrency is bounded by the engine's in-flight ceiling rather than by the executor
     * count, and a stage that never completes is how a client says it has no verdict to give.
     * <p>
     * May be called at most once per client instance, and never together with {@link #poll}.
     */
    void pollAsync(AsyncRecordProcessor processor);

    /** Stops consumption and releases the transport's resources. Idempotent. */
    @Override
    void close();
}
