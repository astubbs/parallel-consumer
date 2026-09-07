package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.Map;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Unit tests for the {@link MdcPropagation} primitive itself - capture, install, restore, and the disabled mode.
 * <p>
 * The end-to-end behaviour through the worker pool is covered by
 * {@code bz.stub.parallelconsumer.MdcContextPropagationTest}.
 *
 * @author Antony Stubbs
 */
@Slf4j
class MdcPropagationTest {

    private final MdcPropagation enabled = new MdcPropagation(true);
    private final MdcPropagation disabled = new MdcPropagation(false);

    @BeforeEach
    @AfterEach
    void clearContext() {
        // the JUnit runner thread is reused across tests, so the MDC must not be left dirty by (or for) any of them
        MDC.clear();
    }

    @Test
    void captureOfAnEmptyContextIsNullAndDoesNotThrow() {
        assertThat(enabled.capture()).isNull();
    }

    @Test
    void captureTakesASnapshotThatIsNotAffectedByLaterChanges() {
        MDC.put("trace_id", "abc");

        Map<String, String> captured = enabled.capture();

        MDC.put("trace_id", "changed");
        MDC.put("added_later", "nope");

        assertThat(captured).containsExactly("trace_id", "abc");
    }

    @Test
    void enterInstallsTheCapturedContextAndRestoresOnClose() {
        Map<String, String> captured = UniMaps.of("trace_id", "abc");

        try (var scope = enabled.enter(captured)) {
            assertMdc("trace_id", "abc");
        }

        assertMdc("trace_id", null);
        assertThat(enabled.capture()).isNull();
    }

    /**
     * The leak-on-reuse case, at the primitive level: whatever the body of the scope adds to the MDC must be gone when
     * the scope closes, or a pooled thread carries it into the next, unrelated, task.
     */
    @Test
    void keysAddedInsideTheScopeDoNotSurviveIt() {
        try (var scope = enabled.enter(UniMaps.of("trace_id", "abc"))) {
            MDC.put("order_id", "put-by-the-user-function");
        }

        assertMdc("order_id", null);
        assertMdc("trace_id", null);
    }

    /**
     * Even with nothing to install - the empty-context case - the scope must still clean up after the body, otherwise
     * the user function's own {@link MDC#put} calls leak onto the pooled thread.
     */
    @Test
    void aNullCapturedContextStillCleansUpAfterTheBody() {
        try (var scope = enabled.enter(null)) {
            MDC.put("order_id", "put-by-the-user-function");
            assertMdc("order_id", "put-by-the-user-function");
        }

        assertMdc("order_id", null);
    }

    /**
     * A pooled thread may legitimately have context of its own (e.g. a Jakarta EE managed thread factory seeds it).
     * Closing the scope must put that back, not blank the thread.
     */
    @Test
    void aPreExistingContextIsRestoredRatherThanCleared() {
        MDC.put("seeded_by_the_pool", "keep-me");

        try (var scope = enabled.enter(UniMaps.of("trace_id", "abc"))) {
            assertMdc("trace_id", "abc");
            // the installed snapshot replaces, rather than merges with, the thread's own context for the duration
            assertMdc("seeded_by_the_pool", null);
        }

        assertMdc("seeded_by_the_pool", "keep-me");
        assertMdc("trace_id", null);
    }

    /**
     * Precedence: PC's own keys are put on top of the installed caller context by the call sites, so they win a
     * collision.
     */
    @Test
    void keysPutAfterEnterOverrideTheCapturedOnes() {
        Map<String, String> callerContextThatCollides = UniMaps.of(
                AbstractParallelEoSStreamProcessor.MDC_INSTANCE_ID, "callers-value",
                "trace_id", "abc");

        try (var scope = enabled.enter(callerContextThatCollides)) {
            MDC.put(AbstractParallelEoSStreamProcessor.MDC_INSTANCE_ID, "pcs-value");

            assertMdc(AbstractParallelEoSStreamProcessor.MDC_INSTANCE_ID, "pcs-value");
            assertMdc("trace_id", "abc");
        }
    }

    @Test
    void whenDisabledNothingIsCapturedAndTheThreadContextIsLeftAlone() {
        MDC.put("trace_id", "abc");

        assertThat(disabled.capture()).isNull();

        try (var scope = disabled.enter(UniMaps.of("other", "value"))) {
            assertMdc("other", null);
            assertMdc("trace_id", "abc");
            MDC.put("order_id", "leaks-when-disabled");
        }

        // disabled means "behave exactly as before this feature existed" - including the leak
        assertMdc("order_id", "leaks-when-disabled");
        assertMdc("trace_id", "abc");
    }

    @Test
    void adoptInstallsWithoutAScopeAndToleratesNull() {
        enabled.adopt(null);
        assertThat(enabled.capture()).isNull();

        enabled.adopt(UniMaps.of("trace_id", "abc"));
        assertMdc("trace_id", "abc");

        disabled.adopt(UniMaps.of("ignored", "value"));
        assertMdc("ignored", null);
    }


    /**
     * Every MDC read in this class goes through here, for two reasons.
     *
     * <p>It names the key in the failure message. {@code assertThat(MDC.get(k)).isEqualTo(v)} fails with
     * "expected abc, but was null" and leaves you to work out which key it was talking about.
     *
     * <p>It is also what stops {@code PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS} reading these tests wrongly.
     * Several of them read the SAME key twice with the same constant argument - once inside the scope and
     * once after it - and the detector takes a repeated same-argument call for a cacheable one. Here it is
     * not: the scope under test is precisely the thing that changes the thread's context between the two
     * reads, so caching the first answer would delete the assertion. Reading through one place means each
     * test method calls this with a different expectation instead of calling {@link MDC#get} twice.
     */
    private static void assertMdc(String key, String expected) {
        assertWithMessage("MDC[%s]", key).that(MDC.get(key)).isEqualTo(expected);
    }

}
