package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;

/**
 * Guards the hand-written {@link PCInternalRuntimeException} constructors. These replaced Lombok's
 * {@code @StandardException} to remove a flaky annotation-processing race (see the class Javadoc); this test
 * pins the resulting public API - the four standard constructors, the {@code {}}-formatting varargs
 * constructor, and the {@link PCInternalRuntimeException#msg(String, Object...)} factory - so a future
 * refactor cannot silently drop one.
 *
 * @author Antony Stubbs
 */
class PCInternalRuntimeExceptionTest {

    @Test
    void noArgConstructor() {
        var e = new PCInternalRuntimeException();
        assertThat(e.getMessage()).isNull();
        assertThat(e.getCause()).isNull();
    }

    @Test
    void messageConstructor() {
        var e = new PCInternalRuntimeException("boom");
        assertThat(e.getMessage()).isEqualTo("boom");
        assertThat(e.getCause()).isNull();
    }

    @Test
    void messageAndCauseConstructor() {
        var cause = new IllegalStateException("root");
        var e = new PCInternalRuntimeException("boom", cause);
        assertThat(e.getMessage()).isEqualTo("boom");
        assertThat(e.getCause()).isSameInstanceAs(cause);
    }

    @Test
    void causeConstructor() {
        var cause = new IllegalStateException("root");
        var e = new PCInternalRuntimeException(cause);
        assertThat(e.getCause()).isSameInstanceAs(cause);
    }

    @Test
    void varargsConstructorFormatsMessageAndKeepsCause() {
        var cause = new IllegalStateException("root");
        var e = new PCInternalRuntimeException("timed out after {}ms on {}", cause, 500, "topic-a");
        assertThat(e.getMessage()).isEqualTo("timed out after 500ms on topic-a");
        assertThat(e.getCause()).isSameInstanceAs(cause);
    }

    @Test
    void msgFactoryFormatsMessage() {
        var e = PCInternalRuntimeException.msg("only {} of {} done", 3, 10);
        assertThat(e.getMessage()).isEqualTo("only 3 of 10 done");
        assertThat(e.getCause()).isNull();
    }
}
