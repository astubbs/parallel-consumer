package bz.stub.parallelconsumer.client;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The asynchronous processor's failure contract: every way a user's asynchronous function can misbehave,
 * turned into a verdict rather than into a broken session.
 * <p>
 * It matters more than the synchronous equivalent because the asynchronous form has <b>three</b> failure
 * moments where the synchronous one has two - the call itself, the stage it returns, and the value that stage
 * completes with - and a transport that meets an unhandled one does not fail one record, it fails a session
 * that ten languages are wrapping. The transports never see any of these: what reaches them is a stage that
 * completes normally with an {@link Outcome}, or one that never completes at all.
 *
 * @author Antony Stubbs
 */
class OutcomesAsyncTest {

    private final InboundRecord record = new InboundRecord("orders", 0, 7, null, "v".getBytes(), 1, null, null);

    @Test
    void aCompletedStageIsTheOutcome() {
        var outcome = await(Outcomes.applyProcessorAsync(
                r -> CompletableFuture.completedFuture(Outcome.success()), record));

        assertThat(outcome.isSuccess()).isTrue();
    }

    @Test
    void aThrowFromTheCallItselfIsAFailureCarryingItsMessage() {
        // the first failure moment: the processor never got as far as returning a stage
        var outcome = await(Outcomes.applyProcessorAsync(r -> {
            throw new IllegalStateException("the database said no");
        }, record));

        assertThat(outcome.isSuccess()).isFalse();
        assertThat(outcome.failureReason()).hasValue("the database said no");
    }

    @Test
    void aNullStageIsAProcessorBugReportedAsAFailure() {
        var outcome = await(Outcomes.applyProcessorAsync(r -> null, record));

        assertThat(outcome.isSuccess()).isFalse();
        assertThat(outcome.failureReason().get()).contains("CompletionStage");
    }

    @Test
    void anExceptionallyCompletedStageIsAFailureNamingTheCauseRatherThanTheWrapper() {
        // the message must be the user's, not "java.util.concurrent.CompletionException": the reason travels
        // with the redelivery to whoever handles the record next, and naming the plumbing wastes it
        var failed = new CompletableFuture<Outcome>();
        failed.completeExceptionally(new CompletionException(new IllegalStateException("the broker said no")));

        var outcome = await(Outcomes.applyProcessorAsync(r -> failed, record));

        assertThat(outcome.isSuccess()).isFalse();
        assertThat(outcome.failureReason()).hasValue("the broker said no");
    }

    @Test
    void aMessagelessExceptionStillNamesSomething() {
        var failed = new CompletableFuture<Outcome>();
        failed.completeExceptionally(new IllegalStateException());

        var outcome = await(Outcomes.applyProcessorAsync(r -> failed, record));

        assertThat(outcome.failureReason().get()).contains("IllegalStateException");
    }

    @Test
    void aStageCompletingWithNullIsAProcessorBugReportedAsAFailure() {
        var outcome = await(Outcomes.applyProcessorAsync(
                r -> CompletableFuture.completedFuture(null), record));

        assertThat(outcome.isSuccess()).isFalse();
        assertThat(outcome.failureReason().get()).contains("Outcome");
    }

    /**
     * The one case that must NOT be normalised: a stage that never completes is the contract's way of saying
     * "this client has no verdict for that record", which is what a client draining at shutdown says about a
     * record it never ran. Turning it into a failure - or imposing any deadline of its own - would put a
     * verdict on the wire for work nobody did.
     */
    @Test
    void aStageThatNeverCompletesIsPassedThroughUntouched() {
        var stage = Outcomes.applyProcessorAsync(r -> new CompletableFuture<>(), record);

        assertWithMessage("no deadline, no default verdict, no completion")
                .that(stage.toCompletableFuture().isDone()).isFalse();
    }

    @Test
    void theSynchronousFormIsTheAsynchronousOneAlreadyComplete() {
        // what lets a transport implement one loop instead of two: asAsync runs the function on the calling
        // thread and hands back a stage that is finished before it is returned
        var callingThread = Thread.currentThread().getName();
        var ranOn = new String[1];

        var stage = Outcomes.asAsync(r -> {
            ranOn[0] = Thread.currentThread().getName();
            return Outcome.success(Collections.singletonList(OutboundRecord.of("out", null, null)));
        }).processAsync(record);

        assertThat(stage.toCompletableFuture().isDone()).isTrue();
        assertThat(ranOn[0]).isEqualTo(callingThread);
        assertThat(await(stage).produce()).hasSize(1);
    }

    @Test
    void theSynchronousFormsThrowBecomesTheSameFailureThroughEitherRoute() {
        var direct = Outcomes.applyProcessor(r -> {
            throw new IllegalStateException("boom");
        }, record);
        var viaAsync = await(Outcomes.asAsync(r -> {
            throw new IllegalStateException("boom");
        }).processAsync(record));

        assertWithMessage("one exception bridge, so the two forms cannot drift apart")
                .that(viaAsync.failureReason()).isEqualTo(direct.failureReason());
    }

    private static Outcome await(java.util.concurrent.CompletionStage<Outcome> stage) {
        try {
            return stage.toCompletableFuture().get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError("the translated stage must always complete normally", e);
        }
    }
}
