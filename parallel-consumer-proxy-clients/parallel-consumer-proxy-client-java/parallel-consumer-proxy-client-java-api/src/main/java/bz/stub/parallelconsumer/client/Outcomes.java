package bz.stub.parallelconsumer.client;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

/**
 * The one definition of how a processor invocation becomes an {@link Outcome} - shared by every transport so
 * the Java-convenience exception bridge behaves identically under both (KTD20: the transport is the only
 * variable). A throw becomes {@link Outcome#failure(String)} with the exception's message; a {@code null}
 * return is a processor bug reported as a failure rather than a wrapper crash, because one bad record must
 * not take down the client.
 * <p>
 * <b>The same rules govern both processor forms</b>, so a transport that runs {@link AsyncRecordProcessor}
 * and one that runs {@link RecordProcessor} cannot disagree about what a misbehaving user function means. The
 * asynchronous form has three failure moments where the synchronous one has two - the call itself, the stage
 * it returns, and the value that stage completes with - and all three land on the same failure text rules.
 *
 * @author Antony Stubbs
 */
public final class Outcomes {

    private Outcomes() {
    }

    /** Runs the processor on the record, translating a throw or a {@code null} return into a failure. */
    public static Outcome applyProcessor(RecordProcessor processor, InboundRecord record) {
        Outcome outcome;
        try {
            outcome = processor.process(record);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Outcome.failure("processing was interrupted");
        } catch (Exception e) {
            return Outcome.failure(failureText(e));
        }
        if (outcome == null) {
            return Outcome.failure("the RecordProcessor returned null instead of an Outcome");
        }
        return outcome;
    }

    /**
     * Starts the asynchronous processor on the record, returning a stage that never completes exceptionally
     * and never completes with {@code null}: everything that could go wrong on the way to a verdict has
     * already been turned into {@link Outcome#failure(String)} by the time a transport sees it.
     * <p>
     * The one case deliberately passed through untouched is a stage that <em>never completes</em>, which
     * {@link AsyncRecordProcessor} defines as "no verdict for this record" - see its contract. Nothing here
     * imposes a deadline: a record is held for as long as the user's work takes, and a wrapper that invented a
     * timeout would be inventing a verdict.
     */
    public static CompletionStage<Outcome> applyProcessorAsync(AsyncRecordProcessor processor,
                                                               InboundRecord record) {
        CompletionStage<Outcome> started;
        try {
            started = processor.processAsync(record);
        } catch (Exception e) {
            return CompletableFuture.completedFuture(Outcome.failure(failureText(e)));
        }
        if (started == null) {
            return CompletableFuture.completedFuture(
                    Outcome.failure("the AsyncRecordProcessor returned null instead of a CompletionStage"));
        }
        return started.handle((outcome, thrown) -> {
            if (thrown != null) {
                return Outcome.failure(failureText(unwrap(thrown)));
            }
            if (outcome == null) {
                return Outcome.failure("the AsyncRecordProcessor completed with null instead of an Outcome");
            }
            return outcome;
        });
    }

    /**
     * The synchronous processor seen as an asynchronous one: it runs on the calling thread and the stage is
     * already complete when it is returned.
     * <p>
     * This is what lets a transport implement the asynchronous path <b>only</b> and get the synchronous one
     * for free with its behaviour unchanged - the user function still runs on the transport's own executor
     * thread, and that thread still moves on only when the function returns. Two loops for the two forms would
     * be two places for a session bug to live.
     */
    public static AsyncRecordProcessor asAsync(RecordProcessor processor) {
        return record -> CompletableFuture.completedFuture(applyProcessor(processor, record));
    }

    /** A throwable as redelivery text: its message when it has one, otherwise its type. */
    private static String failureText(Throwable thrown) {
        return thrown.getMessage() != null ? thrown.getMessage() : thrown.toString();
    }

    /**
     * The cause behind a {@link CompletionException}, which is the wrapper the completion machinery adds
     * rather than anything the user's code threw - reporting it would name the plumbing instead of the
     * failure, and the redelivery carries that text to whoever handles the record next.
     */
    private static Throwable unwrap(Throwable thrown) {
        if (thrown instanceof CompletionException && thrown.getCause() != null) {
            return thrown.getCause();
        }
        return thrown;
    }
}
