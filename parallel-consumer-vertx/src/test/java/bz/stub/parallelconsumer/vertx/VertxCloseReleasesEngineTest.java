package bz.stub.parallelconsumer.vertx;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.web.client.WebClient;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * Pins a fix to {@link VertxParallelEoSStreamProcessor}: every
 * {@link bz.stub.parallelconsumer.internal.DrainingCloseable} entry point must release the Vert.x
 * {@link Vertx} engine and {@link WebClient}, not only the {@link java.time.Duration}-taking overload
 * that used to be the sole place they were released.
 * <p>
 * None of these tests ever call {@code poll(...)}, so {@code vertxAsync} stays in {@code State.UNUSED}
 * until the test closes it - the cheapest way to reach every {@code close} entry point without a
 * running control thread.
 */
@Slf4j
class VertxCloseReleasesEngineTest extends VertxBaseUnitTest {

    private Vertx vertxSpy;
    private WebClient webClientSpy;

    @Override
    protected AbstractParallelEoSStreamProcessor initAsyncConsumer(ParallelConsumerOptions parallelConsumerOptions) {
        VertxOptions vertxOptions = new VertxOptions();
        vertxSpy = spy(Vertx.vertx(vertxOptions));
        webClientSpy = spy(WebClient.create(vertxSpy));
        var build = parallelConsumerOptions.toBuilder()
                .maxConcurrency(10)
                .build();
        vertxAsync = new JStreamVertxParallelEoSStreamProcessor<>(vertxSpy, webClientSpy, build);
        return vertxAsync;
    }

    /**
     * Replaces the inherited {@code @AfterEach close()} entirely - JUnit 5 drops an inherited lifecycle
     * method once a subclass overrides it, unless the override repeats the annotation, so this fully
     * takes over teardown for this class rather than running alongside the original.
     * <p>
     * That is deliberate: {@link #aThrowingCloseStillReleasesTheVertxEngine()} drives
     * {@code closeDrainFirst()} on a never-started ({@code State.UNUSED}) instance, which - a separate,
     * pre-existing defect this PR does not fix - leaves {@code state} stuck at {@code DRAINING}/
     * {@code CLOSING} forever: {@code transitionToDraining} sets {@code DRAINING} unconditionally, unlike
     * {@code transitionToClosing}'s {@code State.UNUSED} guard, so {@code waitForClose} then dereferences
     * an empty {@code controlThreadFuture} on an instance that never started. The inherited teardown's own
     * unguarded {@code parentParallelConsumer.close()} would hit that same defect a second time and fail
     * the {@code @AfterEach} itself, reporting an otherwise-passing test as an error.
     */
    @Override
    @AfterEach
    public void close() {
        try {
            super.close();
        } catch (Exception expectedOnATestThatDrainsANeverStartedInstance) {
            log.debug("Ignoring the never-started-drain defect's re-throw while releasing test resources",
                    expectedOnATestThatDrainsANeverStartedInstance);
        }
    }

    @Test
    void noArgCloseReleasesTheVertxEngine() {
        vertxAsync.close();

        verify(webClientSpy).close();
        verify(vertxSpy).close();
    }

    @Test
    void closeDontDrainFirstReleasesTheVertxEngine() {
        vertxAsync.closeDontDrainFirst();

        verify(webClientSpy).close();
        verify(vertxSpy).close();
    }

    @Test
    void aThrowingCloseStillReleasesTheVertxEngine() {
        // DrainingMode.DRAIN on a never-started instance throws NoSuchElementException (see this class's
        // javadoc) - real, deterministic, and exactly the shape close(DrainingMode) must survive. If this
        // assertion ever starts failing because that other defect got fixed, swap in another throwing
        // mechanism rather than deleting the exception-safety coverage below.
        assertThrows(NoSuchElementException.class, () -> vertxAsync.closeDrainFirst());

        verify(webClientSpy).close();
        verify(vertxSpy).close();
    }
}
