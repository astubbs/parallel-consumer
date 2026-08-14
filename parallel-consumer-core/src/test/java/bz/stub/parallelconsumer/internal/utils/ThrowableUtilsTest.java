package bz.stub.parallelconsumer.internal.utils;

/*-
 * Copyright (C) 2020-2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static bz.stub.parallelconsumer.internal.utils.ThrowableUtils.describeWithRootCause;
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
