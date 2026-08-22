package bz.stub.parallelconsumer.client;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.concurrent.CompletionStage;

/**
 * The user's function when the answer is not ready yet: one record in, an eventual {@link Outcome} out. The
 * same contract as {@link RecordProcessor} with one difference - the verdict arrives through a
 * {@link CompletionStage} rather than a return - and the transports treat the two identically once the stage
 * completes.
 * <p>
 * <b>It exists because a thread that is only waiting is a thread the wrapper should not be holding.</b> A
 * processor whose work is a remote call, a reactive pipeline, or a coroutine has nothing for a wrapper-owned
 * thread to do between handing the record over and the answer coming back; the synchronous form makes it park
 * one anyway. This form is also what lets another language's client <em>wrap</em> a Java transport instead of
 * reimplementing the session: a coroutine, a {@code Future}, an {@code IO} or a {@code Task} bridges to a
 * {@link CompletionStage} without blocking, so the wrapper adds a shape and not a thread. Every session defect
 * fixed in a transport is then fixed once for every language above it.
 * <p>
 * <b>The contract, and it is where this form differs from the synchronous one:</b>
 * <ul>
 *   <li><b>Return promptly.</b> This method is called on a wrapper-owned thread and should start the work and
 *       hand back the stage; work done <em>before</em> the return is work done on that thread, which is the
 *       cost the form exists to avoid.</li>
 *   <li><b>Concurrency is bounded by the engine's in-flight ceiling, not by the executor count.</b> Nothing
 *       blocks while a stage is outstanding, so as many records may be in flight as the engine is willing to
 *       have out at once - which is the number it chose. The executor count bounds threads, and in this form
 *       threads are not the scarce thing.</li>
 *   <li><b>A stage that never completes reports nothing, deliberately.</b> That is the only way to say "this
 *       client is not going to give a verdict for this record" - which is what a client draining at shutdown
 *       must say about a record it never ran, rather than inventing a success or a failure for work it did not
 *       do. The engine reclaims an unreported record when the session ends, exactly as it does after a
 *       connection loss. Use it for that and for nothing else: an ordinary record whose stage never completes
 *       is a stall, and it will look like one.</li>
 * </ul>
 * A throw from this method, a {@code null} stage, a stage that completes exceptionally, and a stage that
 * completes with {@code null} all become {@link Outcome#failure(String)} - see {@link Outcomes}, which is the
 * one place that translation happens.
 *
 * @author Antony Stubbs
 * @see RecordProcessor
 * @see ParallelConsumerClient#pollAsync(AsyncRecordProcessor)
 */
@FunctionalInterface
public interface AsyncRecordProcessor {

    /**
     * Starts processing one record.
     *
     * @return the eventual per-record outcome; never {@code null}, and never completing with {@code null}
     */
    CompletionStage<Outcome> processAsync(InboundRecord record);
}
