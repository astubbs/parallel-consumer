package bz.stub.parallelconsumer.client;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.concurrent.CompletionStage;

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

    /**
     * <b>How a caller learns the session ended, and why - without polling for it.</b> The returned stage
     * completes when consumption has stopped for any reason: normally when the session ended cleanly (the
     * application closed the client, or the engine completed the stream), and exceptionally with the cause when
     * it did not - a broken stream, a refused handshake, a protocol violation.
     * <p>
     * <b>It exists because a client whose session dies silently is the worst failure this surface can have.</b>
     * {@link #poll} has already returned by then, so without this the application goes on believing it is
     * consuming while nothing is: the exact defect this method closes (astubbs#242, and
     * {@code client-authoring-guide.md} §1 makes "the caller can learn the session died, and why" normative for
     * every language).
     * <p>
     * <b>The shape is settled across the fan-out, not invented here.</b> Six clients had already answered:
     * Go's {@code Done()}/{@code Err()}, TypeScript's {@code done()}, Rust's {@code closed()}, Ruby's and
     * Python's {@code wait()}, and C#'s {@code Task} that <em>is</em> the session. Two properties separate the
     * good answers from the rest, and both are held here: the end is observable <em>without</em> ending the
     * client (Python and Rust make you close to learn why), and the <em>cause</em> arrives from the same call
     * (TypeScript and Ruby, the two the divergence review names as right). A {@link CompletionStage} is C#'s
     * answer in Java's vocabulary, and it is already this module's vocabulary - {@code connect} returns one and
     * {@link #pollAsync} consumes one.
     * <p>
     * It is an accessor rather than a return value from {@code poll} - the one place this differs from C# -
     * because a session can die before or without a poll: a client that only connected still has an end to
     * observe, and nine languages already mirror {@code poll}'s existing shape.
     * <p>
     * Every transport implements it, including the degenerate in-process one: two transports that disagree
     * about how a session ends would be a divergence on the one API they share.
     *
     * @return a stage completing when the session ends. Each call returns an independent view of the one
     * session end, so any number of callers may hold, chain or await it, and completing a view by hand cannot
     * reach back into the session.
     */
    CompletionStage<Void> sessionEnd();

    /**
     * Stops consumption and releases the transport's resources. Idempotent.
     * <p>
     * Ends the session: hand-out stops, records already executing finish and report, and only then is the
     * engine told the session is over. A record that was queued but never run is <b>not</b> reported - a client
     * never invents a verdict for work it did not do, and the engine redelivers what it was not told about.
     */
    @Override
    void close();
}
