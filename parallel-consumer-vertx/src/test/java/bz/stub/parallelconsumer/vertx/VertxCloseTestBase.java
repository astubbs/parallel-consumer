package bz.stub.parallelconsumer.vertx;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.DrainingCloseable;
import bz.stub.parallelconsumer.internal.DrainingCloseable.DrainingMode;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;

import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Shared shape of the tests pinning the ownership contract on {@link VertxParallelEoSStreamProcessor}'s
 * constructors - what the processor built, it closes; what the caller supplied, it leaves running. Each subclass
 * fixes who built the engine through {@link #createVertx} and {@link #createWebClient}, drives every
 * {@link DrainingCloseable} entry point through {@link CloseEntryPoint}, and reads the engine's fate off an
 * {@link EngineProbeVerticle}.
 * <p>
 * None of these tests ever call {@code poll(...)}, so the processor stays in {@code State.UNUSED} until the test
 * closes it - the cheapest way to reach every close entry point without a running control thread. Draining a
 * never-started instance throws a {@link NoSuchElementException} out of the base class: a core defect this branch
 * does not fix, recorded in {@code docs/inflight/bug-close-drain-first-on-a-never-started-instance.md}, which arrives
 * with branch {@code fix/close-shuts-down-worker-pool} (until that merges:
 * {@code git show de616c41e:docs/inflight/bug-close-drain-first-on-a-never-started-instance.md}). Here it doubles
 * as a real, deterministic way to make
 * {@code super.close(...)} throw; if that defect is fixed and these assertions start failing, swap in another
 * throwing mechanism rather than deleting the coverage.
 */
@Slf4j
abstract class VertxCloseTestBase extends VertxBaseUnitTest {

    /**
     * Every way a caller can close a processor. The {@code DRAIN} members assert the never-started defect's
     * {@link NoSuchElementException} rather than tolerating any exception, so a different failure still fails the
     * test.
     */
    enum CloseEntryPoint {
        NO_ARG {
            @Override
            void closeVia(DrainingCloseable processor) {
                processor.close();
            }
        },
        DONT_DRAIN_FIRST {
            @Override
            void closeVia(DrainingCloseable processor) {
                processor.closeDontDrainFirst();
            }
        },
        DONT_DRAIN_FIRST_WITH_TIMEOUT {
            @Override
            void closeVia(DrainingCloseable processor) {
                processor.closeDontDrainFirst(Duration.ofSeconds(5));
            }
        },
        DURATION_AND_MODE {
            @Override
            void closeVia(DrainingCloseable processor) {
                processor.close(Duration.ofSeconds(5), DrainingMode.DONT_DRAIN);
            }
        },
        DRAIN_FIRST {
            @Override
            void closeVia(DrainingCloseable processor) {
                assertThrows(NoSuchElementException.class, processor::closeDrainFirst);
            }
        },
        DRAIN_FIRST_WITH_TIMEOUT {
            @Override
            void closeVia(DrainingCloseable processor) {
                assertThrows(NoSuchElementException.class, () -> processor.closeDrainFirst(Duration.ofSeconds(5)));
            }
        };

        abstract void closeVia(DrainingCloseable processor);
    }

    /**
     * A verticle deployed on the engine under test as a witness. Vert.x undeploys every verticle before it shuts an
     * engine down, so this verticle's {@code stop} having run is proof - through public API, with no spy in front of
     * an instance the processor may have built for itself - that the engine began closing; a probe whose
     * {@code stop} never ran sat on an engine that was left running.
     * <p>
     * Deployed with {@link #deployHoldingOpen}, it withholds its stop promise until {@link #releaseAndAwaitShutdown},
     * and while it does {@code vertx.close()} cannot complete - a real, sleep-free way to make the processor's wait
     * for the engine run out.
     */
    static final class EngineProbeVerticle extends AbstractVerticle {

        private final boolean holdsTheEngineOpen;
        private final CompletableFuture<Promise<Void>> stopRequested = new CompletableFuture<>();

        private EngineProbeVerticle(boolean holdsTheEngineOpen) {
            this.holdsTheEngineOpen = holdsTheEngineOpen;
        }

        static EngineProbeVerticle deployOn(Vertx engine) {
            return deploy(engine, new EngineProbeVerticle(false));
        }

        static EngineProbeVerticle deployHoldingOpen(Vertx engine) {
            return deploy(engine, new EngineProbeVerticle(true));
        }

        @SneakyThrows
        private static EngineProbeVerticle deploy(Vertx engine, EngineProbeVerticle probe) {
            engine.deployVerticle(probe).toCompletionStage().toCompletableFuture().get(10, SECONDS);
            return probe;
        }

        @Override
        public void stop(Promise<Void> stopPromise) {
            stopRequested.complete(stopPromise);
            if (!holdsTheEngineOpen) {
                stopPromise.complete();
            }
        }

        boolean engineBeganClosing() {
            return stopRequested.isDone();
        }

        /**
         * Lets a held-open close finish and waits for the engine to be gone, so a test that timed out the processor's
         * wait does not leak a still-closing engine into the next test.
         */
        @SneakyThrows
        void releaseAndAwaitShutdown() {
            stopRequested.thenAccept(Promise::complete);
            getVertx().close().toCompletionStage().toCompletableFuture().get(10, SECONDS);
        }
    }

    /**
     * Replaces the inherited teardown, repeating the annotation because JUnit 5 drops an inherited lifecycle method a
     * subclass overrides without it. The {@code DRAIN} entry points leave a never-started instance stuck short of
     * {@code CLOSED} (the defect in the class javadoc), so the inherited teardown's own close hits the same
     * {@link NoSuchElementException} a second time and would report a passing test as an error. Only that exception
     * is tolerated; anything else still fails the test.
     */
    @Override
    @AfterEach
    public void close() {
        try {
            super.close();
        } catch (NoSuchElementException expectedFromDrainingANeverStartedInstance) {
            log.debug("Ignoring the never-started-drain defect's re-throw while releasing test resources",
                    expectedFromDrainingANeverStartedInstance);
        }
    }
}
