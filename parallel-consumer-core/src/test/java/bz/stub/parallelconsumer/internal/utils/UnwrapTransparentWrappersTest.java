package bz.stub.parallelconsumer.internal.utils;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ExceptionInUserFunctionException;
import bz.stub.parallelconsumer.FakeRuntimeException;
import bz.stub.parallelconsumer.PCRetriableException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static bz.stub.parallelconsumer.internal.utils.ThrowableUtils.unwrapTransparentWrappers;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The peel-and-test every engine's failure classification rests on: {@code PCRetriableException.handbackIn} and
 * {@code isPresentIn} both defer to it, so what it returns decides whether a failure is logged at error, whether a
 * record parks, and what delay it waits.
 * <p>
 * <b>It had no test of its own until the review of astubbs/parallel-consumer#506</b>, which is why this exists: that
 * review asked for the per-container allocation inside it to be removed, and a performance change to an untested
 * walk is a behaviour change nobody would notice. Its termination guards in particular - a cause chain that loops,
 * and the depth bound - were reachable only by reading the code.
 * <p>
 * <b>Proved by sabotage, and one result is worth recording.</b> Removing the depth bound reddens
 * {@link #aChainDeeperThanTheBoundStopsShortOfTheBottom} alone. Removing the identity guard reddens <b>nothing</b>,
 * and that is correct rather than a gap: the two guards are each other's backstop, so with either one present the
 * walk still terminates and still answers the same throwable. The identity set only bounds the WORK - two hops on a
 * two-cycle instead of a hundred - and no outcome can distinguish that. Removing both hangs, which is why the
 * cycle tests time out on a separate thread. Widening {@code isTransparentWrapper} to any exception reddens
 * {@link #anOpaqueWrapperIsNotPeeled}; narrowing it to nothing reddens {@link #oneTransparentWrapperIsPeeled}.
 *
 * @see ThrowableUtils#unwrapTransparentWrappers(Throwable)
 */
class UnwrapTransparentWrappersTest {

    @Test
    void aFailureThatIsNotWrappedIsItsOwnAnswer() {
        var bare = new PCRetriableException("thrown directly");

        assertThat(unwrapTransparentWrappers(bare)).isSameInstanceAs(bare);
    }

    /**
     * The case that actually happens: {@code UserFunctions.carefullyRun} wraps whatever the user threw, so the
     * classification has to see through exactly one layer.
     */
    @Test
    void oneTransparentWrapperIsPeeled() {
        var thrown = new PCRetriableException("busy");

        assertThat(unwrapTransparentWrappers(new ExceptionInUserFunctionException("ran user code", thrown)))
                .isSameInstanceAs(thrown);
    }

    /**
     * Only PC's own pass-through wrappers are peeled. An exception that merely carries a retriable further down is
     * not a retriable, which is what stops a real fault being silenced by something buried under it.
     */
    @Test
    void anOpaqueWrapperIsNotPeeled() {
        var buried = new PCRetriableException("buried");
        var opaque = new IllegalStateException("a genuine fault", buried);

        assertThat(unwrapTransparentWrappers(opaque)).isSameInstanceAs(opaque);
    }

    @Test
    void aWrapperWithNoCauseIsTheAnswerItself() {
        var emptyWrapper = new ExceptionInUserFunctionException("wrapped nothing", null);

        assertThat(unwrapTransparentWrappers(emptyWrapper)).isSameInstanceAs(emptyWrapper);
    }

    /**
     * <b>A wrapper that is its own cause must terminate.</b> {@code initCause} refuses self-causation, so this is
     * built the way it actually arises - an override, or a chain restored by deserialization, neither of which the
     * walk can tell from an ordinary one. The {@link Timeout} is the assertion that matters here, and it is on a
     * SEPARATE thread deliberately: a runaway walk cannot be interrupted, so an in-thread timeout would hang the
     * whole run instead of failing this test. Verified by removing both guards, which hangs with the default thread
     * mode and fails cleanly with this one.
     */
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void aSelfReferentialWrapperTerminates() {
        var itsOwnCause = new ExceptionInUserFunctionException("loops back", null) {
            @Override
            public synchronized Throwable getCause() {
                return this;
            }
        };

        assertWithMessage("it terminates, and answers with the wrapper it could not see past")
                .that(unwrapTransparentWrappers(itsOwnCause)).isSameInstanceAs(itsOwnCause);
    }

    /**
     * Two wrappers pointing at each other - the shape the identity guard exists for, and the one a plain
     * self-reference check would miss entirely.
     */
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void twoWrappersPointingAtEachOtherTerminate() {
        var pair = new MutuallyReferringWrappers();

        Throwable answer = unwrapTransparentWrappers(pair.first);

        assertWithMessage("it stops inside the cycle rather than walking it to the depth bound")
                .that(answer).isAnyOf(pair.first, pair.second);
    }

    /**
     * A chain longer than the depth bound stops at the bound rather than walking it all. Asserted through the
     * ANSWER - a wrapper rather than the throwable at the bottom - because that is the observable consequence: a
     * hostile chain cannot make the classification spend unbounded work, and it also cannot make it see the bottom.
     */
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    void aChainDeeperThanTheBoundStopsShortOfTheBottom() {
        Throwable bottom = new FakeRuntimeException("the real failure, further down than PC will look");
        Throwable chain = bottom;
        for (int i = 0; i < 500; i++) {
            chain = new ExceptionInUserFunctionException("layer " + i, chain);
        }

        Throwable answer = unwrapTransparentWrappers(chain);

        assertThat(answer).isNotSameInstanceAs(bottom);
        assertThat(answer).isInstanceOf(ExceptionInUserFunctionException.class);
    }

    /**
     * Two wrappers each naming the other as its cause - the shape {@code initCause} allows and a self-reference
     * check cannot see. Held in an array so each can name the other, which neither constructor can do.
     */
    private static final class MutuallyReferringWrappers {

        private final Throwable first;

        private final Throwable second;

        private MutuallyReferringWrappers() {
            var pair = new Throwable[2];
            pair[0] = new ExceptionInUserFunctionException("first", null) {
                @Override
                public synchronized Throwable getCause() {
                    return pair[1];
                }
            };
            pair[1] = new ExceptionInUserFunctionException("second", null) {
                @Override
                public synchronized Throwable getCause() {
                    return pair[0];
                }
            };
            first = pair[0];
            second = pair[1];
        }
    }
}
