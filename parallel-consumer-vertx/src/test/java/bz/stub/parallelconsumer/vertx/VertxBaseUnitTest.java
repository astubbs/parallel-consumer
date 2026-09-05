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
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;

@Slf4j
public abstract class VertxBaseUnitTest extends ParallelEoSStreamProcessorTestBase {

    JStreamVertxParallelEoSStreamProcessor<String, String> vertxAsync;

    @Override
    protected AbstractParallelEoSStreamProcessor initAsyncConsumer(ParallelConsumerOptions parallelConsumerOptions) {
        VertxOptions vertxOptions = new VertxOptions();
        Vertx vertx = createVertx(vertxOptions);
        WebClient wc = createWebClient(vertx);
        var build = parallelConsumerOptions.toBuilder()
                .maxConcurrency(10)
                .build();
        vertxAsync = new JStreamVertxParallelEoSStreamProcessor<>(vertx, wc, build);

        return vertxAsync;
    }

    /**
     * The engine instance {@link #initAsyncConsumer} hands to the processor.
     * <p>
     * A seam, not a preference: a subclass that needs to observe the engine - a Mockito spy, say - can
     * substitute the instance here without restating {@link #initAsyncConsumer}'s options-builder and
     * construction, which is the copy that would otherwise drift the next time either changes.
     */
    protected Vertx createVertx(VertxOptions vertxOptions) {
        return Vertx.vertx(vertxOptions);
    }

    /**
     * The web client {@link #initAsyncConsumer} hands to the processor, built on whatever
     * {@link #createVertx} returned. The same seam, for the same reason.
     */
    protected WebClient createWebClient(Vertx vertx) {
        return WebClient.create(vertx);
    }

    @BeforeEach
    public void setupData() {
        super.primeFirstRecord();
    }

}
