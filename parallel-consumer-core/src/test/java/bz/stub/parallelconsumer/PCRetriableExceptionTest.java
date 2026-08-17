package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2020-2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.InternalRuntimeException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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

    @Test
    void wrappedTwiceByPCsOwnWrappersIsStillExpected() {
        var wrapped = new ExceptionInUserFunctionException("outer",
                new InternalRuntimeException("inner", new PCRetriableException("retry me")));

        assertThat(PCRetriableException.isPresentIn(wrapped)).isTrue();
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
