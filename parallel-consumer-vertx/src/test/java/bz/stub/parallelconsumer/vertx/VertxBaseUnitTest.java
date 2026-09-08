package bz.stub.parallelconsumer.vertx;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessorTestBase;
import bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.web.client.WebClient;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
public abstract class VertxBaseUnitTest extends ParallelEoSStreamProcessorTestBase {

    JStreamVertxParallelEoSStreamProcessor<String, String> vertxAsync;

    /**
     * The options {@link #initAsyncConsumer} built {@link #vertxAsync} with, kept so {@link #newProcessorOn} can build
     * a second processor the same way.
     */
    private ParallelConsumerOptions<String, String> processorOptions;

    /**
     * The engine and client this test built and handed to the processor - {@code null} where {@link #createVertx} or
     * {@link #createWebClient} chose to let the processor build its own. The processor closes only what it built
     * (the ownership contract on its constructors), so what this test built is this test's to release, in
     * {@link #close()}.
     */
    private Vertx suppliedVertx;
    private WebClient suppliedWebClient;

    @Override
    protected AbstractParallelEoSStreamProcessor<String, String> initAsyncConsumer(ParallelConsumerOptions<String, String> parallelConsumerOptions) {
        suppliedVertx = createVertx(new VertxOptions());
        suppliedWebClient = suppliedVertx == null ? null : createWebClient(suppliedVertx);
        processorOptions = parallelConsumerOptions.toBuilder()
                .maxConcurrency(10)
                .build();
        vertxAsync = newProcessorOn(suppliedVertx, suppliedWebClient);

        return vertxAsync;
    }

    /**
     * Builds a processor on the given engine and client with the options {@link #vertxAsync} was built with, so a
     * test that needs a second processor - one that shares an engine, say - constructs it the one way rather than
     * restating the options.
     */
    protected JStreamVertxParallelEoSStreamProcessor<String, String> newProcessorOn(Vertx vertx, WebClient webClient) {
        return new JStreamVertxParallelEoSStreamProcessor<>(vertx, webClient, processorOptions);
    }

    /**
     * The engine instance {@link #initAsyncConsumer} hands to the processor.
     * <p>
     * A seam, not a preference: a subclass that needs to observe the engine - a Mockito spy, say - can substitute the
     * instance here without restating {@link #initAsyncConsumer}'s options-builder and construction, which is the
     * copy that would otherwise drift the next time either changes. Return {@code null} to hand the processor
     * nothing, so it builds and owns its own engine and client; {@link #createWebClient} is then not called.
     */
    protected Vertx createVertx(VertxOptions vertxOptions) {
        return Vertx.vertx(vertxOptions);
    }

    /**
     * The web client {@link #initAsyncConsumer} hands to the processor, built on whatever {@link #createVertx}
     * returned. The same seam, for the same reason; return {@code null} to have the processor build and own the
     * client on the supplied engine.
     */
    protected WebClient createWebClient(Vertx vertx) {
        return WebClient.create(vertx);
    }

    @BeforeEach
    public void setupData() {
        super.primeFirstRecord();
    }

    /**
     * Closes the processor, then releases the engine this test supplied it. The engine goes second so nothing still
     * in flight is cut off from under a processor that is still closing, and in a {@code finally} so a processor
     * whose close throws does not leak the engine's threads into the next test.
     */
    @Override
    @AfterEach
    public void close() {
        try {
            super.close();
        } finally {
            releaseTheEngineThisTestSupplied();
        }
    }

    @SneakyThrows
    private void releaseTheEngineThisTestSupplied() {
        if (suppliedWebClient != null) {
            suppliedWebClient.close();
        }
        if (suppliedVertx != null) {
            suppliedVertx.close().toCompletionStage().toCompletableFuture().get(10, SECONDS);
        }
    }

}
