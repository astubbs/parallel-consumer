package bz.stub.parallelconsumer.internal.utils;

/*-
 * Copyright (C) 2020-2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static bz.stub.parallelconsumer.internal.utils.ThrowableUtils.describeWithRootCause;
import static bz.stub.parallelconsumer.internal.utils.ThrowableUtils.hasCauseOfType;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The cases that matter here are the hostile ones. This runs on the failure path, describing a throwable that came
 * from user code - so it is handed objects whose author can have overridden anything, and an exception escaping the
 * <em>description</em> of a failure would prevent the caller from handling the failure itself.
 */
class ThrowableUtilsTest {

    @Test
    void namesTheRootCauseRatherThanTheWrapper() {
        var root = new IllegalStateException("the sentence a human needs");
        var wrapper = new RuntimeException("Error occurred in code supplied by user", root);

        assertThat(describeWithRootCause(wrapper))
                .contains("the sentence a human needs")
                .contains("IllegalStateException");
    }

    @Test
    void aNullMessageReportsTheTypeRatherThanNull() {
        // the original complaint: "Error from poll control thread: null", because an NPE from user code has no message
        assertThat(describeWithRootCause(new RuntimeException(new NullPointerException())))
                .contains("NullPointerException");
    }

    @Test
    void noCauseIsJustTheMessage() {
        assertThat(describeWithRootCause(new RuntimeException("alone"))).isEqualTo("alone");
    }

    /**
     * The original complaint - {@code "Error from poll control thread: null"} - surviving in the one branch that had
     * no cause to fall back to. A throwable with neither a message nor a cause is exactly what an NPE from user code
     * looks like when nothing wrapped it.
     */
    @Test
    void noMessageAndNoCauseReportsTheTypeRatherThanNull() {
        assertThat(describeWithRootCause(new NullPointerException()))
                .isEqualTo("NullPointerException")
                .isNotEqualTo("null");
    }

    @Test
    void aRootCauseWithNoMessageIsNamedOnceByType() {
        var wrapper = new RuntimeException("outer", new IllegalStateException());

        // not "IllegalStateException: null", and not the type repeated
        assertThat(describeWithRootCause(wrapper)).isEqualTo("outer - caused by IllegalStateException");
    }

    /**
     * {@code initCause} refuses self-causation, so {@code A -> A} cannot be built and a self-reference check reads as
     * sufficient. {@code A -> B -> A} can be, and defeats it - the walk never terminates.
     * <p>
     * The timeout is the assertion: a regression here does not fail with a message, it hangs.
     */
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void aCyclicCauseChainTerminates() {
        var head = new RuntimeException("head");
        var tail = new RuntimeException("tail", head);
        head.initCause(tail);

        assertThat(describeWithRootCause(head)).isNotNull();
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void aThrowingGetCauseFallsBackRatherThanEscaping() {
        var hostile = new RuntimeException("hostile") {
            @Override
            public synchronized Throwable getCause() {
                throw new UnsupportedOperationException("no cause for you");
            }
        };

        assertThat(describeWithRootCause(hostile))
                .as("falls back to something, rather than throwing out of the failure handler")
                .contains("ThrowableUtilsTest");
    }

    @Test
    void findsTheTypeWrappedAtAnyDepth() {
        var buried = new IllegalStateException("buried",
                new RuntimeException("outer wrapper",
                        new UnsupportedOperationException("inner wrapper")));

        // the depth is the point: an "is it retriable?" check that only reads the top answers no here, which is how
        // an expected failure gets logged as an error
        assertThat(hasCauseOfType(buried, UnsupportedOperationException.class)).isTrue();
        assertThat(hasCauseOfType(buried, IllegalStateException.class)).as("the throwable itself counts").isTrue();
        assertThat(hasCauseOfType(buried, NumberFormatException.class)).isFalse();
    }

    /**
     * A chain that never repeats and never ends.
     * <p>
     * The identity set stops a chain that loops <em>back</em>; it cannot stop one that never revisits anything.
     * {@code getCause()} is overridable, so each call here hands back a brand-new object with its own captured stack
     * trace - never "seen", so the walk runs until the heap is gone. Both guards are needed, and neither implies the
     * other: this input defeats the cycle guard, and the cyclic input above defeats a depth cap alone would not catch
     * it in time.
     * <p>
     * The timeout is the assertion. Without the depth bound this does not fail with a message - it allocates until
     * the JVM dies, taking the control thread's shutdown with it.
     */
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void anEndlesslyFreshCauseChainIsBounded() {
        EndlessCause.hops.set(0);
        assertThat(describeWithRootCause(new EndlessCause())).isNotNull();
        assertThat(EndlessCause.hops.get())
                .as("hops taken while describing an endless chain")
                .isLessThanOrEqualTo(HOP_CEILING);

        EndlessCause.hops.set(0);
        assertThat(hasCauseOfType(new EndlessCause(), NumberFormatException.class)).isFalse();
        assertThat(EndlessCause.hops.get())
                .as("hops taken while searching an endless chain")
                .isLessThanOrEqualTo(HOP_CEILING);
    }

    /**
     * Generously above the walk's own limit, because this asserts <em>bounded</em>, not the exact bound - pinning the
     * constant here would make the test fail on a deliberate tuning change rather than on the defect.
     */
    private static final int HOP_CEILING = 1_000;

    /**
     * Each call hands back a <b>new</b> instance of itself, so no identity is ever seen twice and the chain never
     * ends. The reviewer's example, made countable.
     * <p>
     * Counting hops rather than waiting for the heap to run out: an unbounded walk here would eventually throw
     * {@link OutOfMemoryError}, which the walk's own {@code catch (Throwable)} would swallow and return from
     * normally - so a "did it come back?" assertion passes either way, and the only thing distinguishing them would
     * be how long the JVM took to die. This asks the question directly.
     */
    private static class EndlessCause extends RuntimeException {

        static final AtomicInteger hops = new AtomicInteger();

        EndlessCause() {
            super("endless");
        }

        @Override
        public synchronized Throwable getCause() {
            hops.incrementAndGet();
            return new EndlessCause();
        }
    }

    /**
     * The one property every caller on the failure path relies on, tested against the input most likely to break it.
     * The fallback inside {@code describeWithRootCause} dereferences its argument, so it could not be the null
     * handler - a null used to produce a second NPE that escaped the method entirely.
     */
    @Test
    void aNullThrowableIsDescribedRatherThanThrowing() {
        assertThat(describeWithRootCause(null)).isEqualTo("null");
    }

    @Test
    void subtypesCount() {
        // isInstance, not equals: PCRetriableException is extended by users
        assertThat(hasCauseOfType(new IllegalStateException(), RuntimeException.class)).isTrue();
    }

    @Test
    void aNullThrowableIsNotAMatch() {
        assertThat(hasCauseOfType(null, RuntimeException.class)).isFalse();
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void searchingACyclicChainTerminates() {
        var head = new RuntimeException("head");
        var tail = new RuntimeException("tail", head);
        head.initCause(tail);

        assertThat(hasCauseOfType(head, RuntimeException.class)).isTrue();
        assertThat(hasCauseOfType(head, NumberFormatException.class)).as("absent, and the walk still ends").isFalse();
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void aThrowingGetCauseAnswersFalseRatherThanEscaping() {
        var hostile = new RuntimeException("hostile") {
            @Override
            public synchronized Throwable getCause() {
                throw new UnsupportedOperationException("no cause for you");
            }
        };

        assertThat(hasCauseOfType(hostile, NumberFormatException.class))
                .as("an unreadable chain answers false rather than replacing one failure with another")
                .isFalse();
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void aThrowingGetMessageFallsBackRatherThanEscaping() {
        var hostile = new RuntimeException() {
            @Override
            public String getMessage() {
                throw new IllegalStateException("no message for you");
            }
        };

        assertThat(describeWithRootCause(hostile))
                .as("falls back to something, rather than throwing out of the failure handler")
                .contains("ThrowableUtilsTest");
    }
}
