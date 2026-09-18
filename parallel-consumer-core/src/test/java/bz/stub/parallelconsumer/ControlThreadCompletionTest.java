package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * {@code controlThreadCompletion()} is the engine saying, once, that its control thread has ended - so a caller
 * can WAIT for that instead of asking repeatedly whether it has happened yet.
 * <p>
 * The completion already existed as the control task's submitted {@link java.util.concurrent.Future}, private with
 * no accessor, which is the only reason a caller had to poll: the fluent API's {@code ConsumerHandle.awaitShutdown}
 * looped at 200ms on a latch and asked {@link AbstractParallelEoSStreamProcessor#isClosedOrFailed()} on each pass,
 * because an engine that ended without going through that handle counts no latch down. A composable future answers
 * both halves at once.
 * <p>
 * The three cases below are the contract, and the third is the one a reader gets wrong: an instance whose user
 * function throws has not ended, and must not report that it has.
 * <p>
 * <b>Proved by sabotage, not by assumption</b>, per the test-tree rules. Arming the clean completion two seconds
 * late, from a thread of its own, reddens {@link #itCompletesWhenTheInstanceIsClosed} at its {@code isDone()}
 * assertion and nothing else - which is the mutation the previous shape of that test could not see, because it
 * waited on {@code get()} first and then asked a question that cannot fail after one.
 * {@code ControlThreadCompletionOnLoggingFailureTest} owns the fourth case, where the task body's own clean-exit log
 * throws.
 *
 * @see AbstractParallelEoSStreamProcessor#controlThreadCompletion()
 */
@Slf4j
class ControlThreadCompletionTest extends ParallelEoSStreamProcessorTestBase {

    private static final long WAIT_SECONDS = 30;

    @BeforeEach
    void primeARecordToProcess() {
        super.primeFirstRecord();
    }

    @Timeout(60)
    @Test
    void itCompletesWhenTheInstanceIsClosed() throws Exception {
        parallelConsumer.poll(recordContexts -> log.debug("Processed {}", recordContexts));
        CompletableFuture<Void> completion = parallelConsumer.controlThreadCompletion();

        assertWithMessage("FIXTURE: a running instance has not ended, so nothing may have completed this yet")
                .that(completion.isDone())
                .isFalse();

        parallelConsumer.closeDrainFirst();

        // The ordering claim, asserted rather than described: the control task completes this BEFORE it returns, and
        // close() waits on the task's own future, so by the time close() has returned this is already done. Asked
        // before the get() on purpose - a get() first would wait for the completion and then find it done, which is
        // true of a completion armed a minute late as well.
        assertWithMessage("close() has returned, so the control task has ended and armed this already - waiting "
                + "here instead of asserting would hide an arming that happens too late")
                .that(completion.isDone())
                .isTrue();
        // What proves the completion is CLEAN: get() rethrows as an ExecutionException on an exceptional one, so
        // returning normally is the assertion. isCompletedExceptionally() after a successful get() cannot fail, and
        // said so for a while until the review of astubbs/parallel-consumer#506 traced it.
        Void ignoredResult = completion.get(WAIT_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * A control thread that dies on its own - nobody called close - is the case the polling loop existed for, and
     * the one where a bare completion would be a lie: a caller told only "it ended" would read that as a clean
     * shutdown. So it completes exceptionally, carrying the same throwable the engine records as its failure
     * cause.
     */
    @Timeout(60)
    @Test
    void itCompletesExceptionallyWhenTheControlThreadDies() {
        parallelConsumer.poll(recordContexts -> log.debug("Processed {}", recordContexts));
        CompletableFuture<Void> completion = parallelConsumer.controlThreadCompletion();

        // Killed from the inside, exactly as JStreamLiveResultStreamTest does it: nothing calls close, so the only
        // thing that can end this wait is the control thread reporting its own death.
        parallelConsumer.addLoopEndCallBack(() -> {
            throw new FakeRuntimeException("fake control loop error, so the control thread closes itself");
        });

        ExecutionException thrown = assertThrows(ExecutionException.class,
                () -> completion.get(WAIT_SECONDS, TimeUnit.SECONDS));

        assertWithMessage("the waiter must be handed the engine's own recorded cause, not a second description "
                + "of it - a caller that rethrows is rethrowing what actually killed the consumer")
                .that(thrown.getCause())
                .isSameInstanceAs(parallelConsumer.getFailureCause());
        // Two wrappers, both deliberate and both named here rather than walked past with a root-cause helper: the
        // loop-end callback is user code, so it arrives wrapped in ExceptionInUserFunctionException, and the
        // control loop wraps THAT to say which thread died. A caller diagnosing a dead consumer reads this chain.
        Throwable userCodeWrapper = thrown.getCause().getCause();
        assertWithMessage("the callback is user code, so the control loop's cause is the user-function wrapper")
                .that(userCodeWrapper)
                .isInstanceOf(ExceptionInUserFunctionException.class);
        assertWithMessage("and the throw itself is still reachable underneath both of them")
                .that(userCodeWrapper.getCause())
                .isInstanceOf(FakeRuntimeException.class);
    }

    /**
     * The confusion worth pinning: a user function that throws is one record failing, not an instance ending. The
     * record is retried, the control thread runs on, and anything waiting on the completion must keep waiting.
     */
    @Timeout(60)
    @Test
    void aUserFunctionThrowingDoesNotCompleteIt() {
        AtomicInteger attempts = new AtomicInteger();
        parallelConsumer.poll(recordContexts -> {
            attempts.incrementAndGet();
            throw new FakeRuntimeException("deliberate - one record failing is not the instance ending");
        });
        CompletableFuture<Void> completion = parallelConsumer.controlThreadCompletion();

        // Established, not raced for: wait until the function has actually thrown more than once, so the assertion
        // below is made against an instance that has genuinely met the failure and gone on retrying it.
        awaitUntilTrue(() -> attempts.get() > 1);
        awaitForSomeLoopCycles(3);

        assertWithMessage("the user's failure is the record's outcome; the control thread never ended, so nothing "
                + "may have completed this")
                .that(completion.isDone())
                .isFalse();
        assertThat(parallelConsumer.isClosedOrFailed()).isFalse();
    }
}
