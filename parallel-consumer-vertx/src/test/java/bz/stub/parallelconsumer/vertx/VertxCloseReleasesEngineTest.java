package bz.stub.parallelconsumer.vertx;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.DrainingCloseable.DrainingMode;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.time.Instant;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeoutException;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The owning half of the contract {@link VertxCloseTestBase} describes: an engine the processor built itself is
 * released by every close entry point, a teardown that fails cannot replace the failure {@code super.close(...)}
 * was already throwing, and the wait for the engine honours the caller's {@link Duration}.
 * {@link VertxCloseLeavesACallerSuppliedEngineRunningTest} pins the other half.
 */
class VertxCloseReleasesEngineTest extends VertxCloseTestBase {

    /**
     * How long the caller gives the close in the tests below. Well short of the configured default (10s), so a wait
     * bounded by the default rather than by this is unmistakable in the elapsed time.
     */
    private static final Duration CALLERS_TIMEOUT = Duration.ofMillis(300);

    /**
     * Anything near the configured 10s default means the caller's {@link Duration} was ignored; anything near
     * {@link #CALLERS_TIMEOUT} means it was honoured. The bound sits far from both, so neither side is timing-sensitive.
     */
    private static final Duration FAR_SHORT_OF_THE_DEFAULT = Duration.ofSeconds(5);

    /**
     * Hands the processor nothing, so it builds and owns the engine - the case under test.
     */
    @Override
    protected Vertx createVertx(VertxOptions vertxOptions) {
        return null;
    }

    @ParameterizedTest
    @EnumSource(CloseEntryPoint.class)
    void everyCloseEntryPointReleasesTheEngineTheProcessorBuilt(CloseEntryPoint entryPoint) {
        var probe = EngineProbeVerticle.deployOn(vertxAsync.getVertx());

        entryPoint.closeVia(vertxAsync);

        assertThat(probe.engineBeganClosing()).isTrue();
    }

    @Test
    void aFailingTeardownIsSuppressedOnTheCloseFailureRatherThanReplacingIt() {
        var heldOpen = EngineProbeVerticle.deployHoldingOpen(vertxAsync.getVertx());
        try {
            // DRAIN on a never-started instance makes super.close throw (see the base class javadoc); the held-open
            // engine makes the Vert.x teardown time out. Both fail - the caller must still see the first.
            var closeFailure = assertThrows(NoSuchElementException.class,
                    () -> vertxAsync.close(CALLERS_TIMEOUT, DrainingMode.DRAIN));

            assertThat(heldOpen.engineBeganClosing()).isTrue();
            assertThat(closeFailure.getSuppressed()).hasLength(1);
            assertThat(closeFailure.getSuppressed()[0]).isInstanceOf(TimeoutException.class);
        } finally {
            heldOpen.releaseAndAwaitShutdown();
        }
    }

    @Test
    void closeWithADurationBoundsTheWaitForTheEngineByThatDuration() {
        var heldOpen = EngineProbeVerticle.deployHoldingOpen(vertxAsync.getVertx());
        try {
            Instant started = Instant.now();
            assertThrows(TimeoutException.class, () -> vertxAsync.close(CALLERS_TIMEOUT, DrainingMode.DONT_DRAIN));
            Duration elapsed = Duration.between(started, Instant.now());

            assertThat(elapsed).isLessThan(FAR_SHORT_OF_THE_DEFAULT);
        } finally {
            heldOpen.releaseAndAwaitShutdown();
        }
    }
}
