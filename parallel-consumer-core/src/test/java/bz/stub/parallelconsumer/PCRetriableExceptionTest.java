package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.InternalRuntimeException;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;

/**
 * The decision that turns a failure into a DEBUG line or an ERROR line.
 * <p>
 * Tested here rather than through the engines because what matters is the classification itself - no module has a
 * log-capture harness, and asserting "which level did slf4j receive" would test the logger rather than the rule.
 * Each engine's job is only to hand this the right throwable.
 *
 * @author Antony Stubbs
 */
class PCRetriableExceptionTest {

    @Test
    void thrownDirectlyIsExpected() {
        assertThat(PCRetriableException.isPresentIn(new PCRetriableException("retry me"))).isTrue();
    }

    @Test
    void wrappedByPCsOwnUserFunctionWrapperIsStillExpected() {
        // the shape every user function failure arrives in - UserFunctions.carefullyRun wraps whatever was thrown
        var wrapped = new ExceptionInUserFunctionException("Error occurred in code supplied by user",
                new PCRetriableException("retry me"));

        assertThat(PCRetriableException.isPresentIn(wrapped)).isTrue();
    }

    /**
     * {@link InternalRuntimeException} reads like a pass-through wrapper and is not one.
     * <p>
     * Its message is how callers tell distinct internal failures apart - {@code "Error encoding offsets"},
     * {@code "Error producing result message"}, {@code "Too many attempts taking commit responses"}. Peeling it would
     * let the retriable underneath speak for a failure that is not retriable, so a genuine offset-encoding fault
     * carrying one would be logged at DEBUG - which is off in production, so the fault would simply be gone.
     * <p>
     * The user-code path never produces this shape anyway: {@code UserFunctions.carefullyRun} wraps in
     * {@link ExceptionInUserFunctionException} and nothing else.
     */
    @Test
    void anInternalFailureCarryingARetriableIsNotExpected() {
        var internal = new InternalRuntimeException("Error encoding offsets", new PCRetriableException("retry me"));

        assertThat(PCRetriableException.isPresentIn(internal)).isFalse();
    }

    /**
     * The same distinction one level down: peeling PC's genuine wrapper must not then peel a semantic one.
     */
    @Test
    void anInternalFailureBeneathTheUserWrapperIsNotExpected() {
        var wrapped = new ExceptionInUserFunctionException("Error occurred in code supplied by user",
                new InternalRuntimeException("Error producing result message", new PCRetriableException("retry me")));

        assertThat(PCRetriableException.isPresentIn(wrapped)).isFalse();
    }

    @Test
    void subclassesAreExpected() {
        class UserRetriable extends PCRetriableException {
            UserRetriable() {
                super("mine");
            }
        }

        assertThat(PCRetriableException.isPresentIn(new UserRetriable())).isTrue();
    }

    /**
     * The one that matters, and the reason this is not a whole-chain search.
     * <p>
     * A genuinely different failure that merely happens to carry a retriable further down its chain is NOT expected.
     * Classifying it as expected would log a real fault at debug - and debug is off in production, so the fault
     * would simply be gone.
     */
    @Test
    void aDifferentFailureCarryingARetriableBeneathItIsNotExpected() {
        var fatal = new IllegalStateException("the broker configuration is wrong",
                new PCRetriableException("an earlier, unrelated retry"));

        assertThat(PCRetriableException.isPresentIn(fatal)).isFalse();
    }

    @Test
    void anOrdinaryFailureIsNotExpected() {
        assertThat(PCRetriableException.isPresentIn(new IllegalStateException("boom"))).isFalse();
        assertThat(PCRetriableException.isPresentIn(new ExceptionInUserFunctionException("wrapped",
                new NullPointerException()))).isFalse();
    }

    @Test
    void nullIsNotExpected() {
        assertThat(PCRetriableException.isPresentIn(null)).isFalse();
    }
}
