package bz.stub.parallelconsumer.vertx;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessorTestBase;
import bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.internal.DrainingCloseable.DrainingMode;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.web.client.WebClient;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.time.Duration;

@Slf4j
public abstract class VertxBaseUnitTest extends ParallelEoSStreamProcessorTestBase {

    JStreamVertxParallelEoSStreamProcessor<String, String> vertxAsync;

    @Override
    protected AbstractParallelEoSStreamProcessor initAsyncConsumer(ParallelConsumerOptions parallelConsumerOptions) {
        VertxOptions vertxOptions = new VertxOptions();
        Vertx vertx = Vertx.vertx(vertxOptions);
        WebClient wc = WebClient.create(vertx);
        var build = parallelConsumerOptions.toBuilder()
                .maxConcurrency(10)
                .build();
        vertxAsync = new JStreamVertxParallelEoSStreamProcessor<>(vertx, wc, build);

        return vertxAsync;
    }

    @BeforeEach
    public void setupData() {
        super.primeFirstRecord();
    }

    /**
     * Releases the Vert.x engine and web client this class creates, which no other teardown reaches.
     * <p>
     * {@code webClient.close()} and {@code vertx.close()} live only in
     * {@link VertxParallelEoSStreamProcessor#close(Duration, DrainingMode)}, and every other shutdown path -
     * the core base class's {@code @AfterEach}, and any {@code close()} / {@code closeDrainFirst()} a test
     * makes itself - routes through the no-argument form instead. Without this, each test method in every
     * subclass strands a web client and an event-loop group for the rest of the suite.
     * <p>
     * Safe when the test already closed the processor: the shutdown short-circuits on the already-{@code
     * CLOSED} state and the Vert.x teardown after it still runs. The {@link Duration} is a ceiling rather
     * than a wait - the close polls for completion and returns as soon as it has it.
     * <p>
     * Failures are swallowed, matching the core test base's reason for guarding its own close: a test that
     * deliberately drives the consumer into a failed state must not then be reported as a teardown error.
     * Note what that cannot rescue -
     * {@link VertxParallelEoSStreamProcessor#close(Duration, DrainingMode)} closes the web client and Vert.x
     * <i>after</i> delegating to {@code super}, with no {@code finally}, so a shutdown that throws skips both
     * regardless of what happens here. Releasing them on that path needs a fix in the processor.
     */
    @AfterEach
    void closeVertxResources() {
        try {
            vertxAsync.close(Duration.ofSeconds(10), DrainingMode.DONT_DRAIN);
        } catch (Exception e) {
            log.warn("Ignoring close failure while releasing Vert.x resources in teardown", e);
        }
    }

}
