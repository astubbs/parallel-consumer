package bz.stub.parallelconsumer.vertx;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.DrainingCloseable.DrainingMode;
import bz.stub.parallelconsumer.internal.utils.WireMockUtils;
import bz.stub.parallelconsumer.vertx.VertxParallelEoSStreamProcessor.RequestInfo;
import com.github.tomakehurst.wiremock.WireMockServer;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import pl.tlinkowski.unij.api.UniMaps;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedDeque;

import static com.google.common.truth.Truth.assertThat;

/**
 * The vertx half of the astubbs#122 clear-on-close fix.
 * <p>
 * This module is where the leak was actually reported, and its close chain is the more involved of the two:
 * {@link VertxParallelEoSStreamProcessor} overrides {@code close(Duration, DrainingMode)} and does its own
 * teardown around {@code super}, so the JStream override is reached by a different route than in core.
 * Covering only the core processor would leave the reported path untested.
 *
 * @author Antony Stubbs
 * @see JStreamVertxParallelEoSStreamProcessor#close(bz.stub.parallelconsumer.internal.DrainingCloseable.DrainingMode)
 * @see <a href="https://github.com/astubbs/parallel-consumer/issues/122">astubbs#122</a>
 * @see <a href="https://github.com/confluentinc/parallel-consumer/issues/912">confluentinc#912</a>
 */
@Isolated
@Slf4j
class JStreamVertxMemoryLeak912Test extends VertxBaseUnitTest {

    WireMockServer stubServer;

    @BeforeEach
    void setupWireMock() {
        stubServer = new WireMockUtils().setupWireMock();
    }

    @AfterEach
    void closeWireMock() {
        stubServer.stop();
    }

    /**
     * Releases the Vert.x engine and web client, which the shutdown under test does <b>not</b>.
     * <p>
     * {@code webClient.close()} and {@code vertx.close()} live only in
     * {@link VertxParallelEoSStreamProcessor#close(Duration, DrainingMode)}, so the no-argument paths these
     * tests exercise never reach them - and the shared {@code @AfterEach} in the core test base skips its own
     * close once the processor reports closed, which by then it does. Without this, each test would strand a
     * web client and an event-loop group for the rest of the suite.
     * <p>
     * Calling the {@link Duration}-taking close a second time is safe: the shutdown short-circuits on the
     * already-{@code CLOSED} state and the Vert.x teardown after it still runs.
     */
    @AfterEach
    void closeVertxResources() {
        vertxAsync.close(Duration.ofSeconds(10), DrainingMode.DONT_DRAIN);
    }

    /**
     * {@code closeDrainFirst()} is the shutdown the shipped Vert.x example app calls, and it routes through
     * {@code close(DrainingMode)} rather than the no-arg {@code close()}.
     */
    @Test
    void closeDrainFirstShouldClearResultDeque() {
        ConcurrentLinkedDeque<?> deque = produceOneResultAndAwaitIt();

        vertxAsync.closeDrainFirst();

        assertThat(deque).isEmpty();
    }

    @Test
    void closeShouldClearResultDeque() {
        ConcurrentLinkedDeque<?> deque = produceOneResultAndAwaitIt();

        vertxAsync.close();

        assertThat(deque).isEmpty();
    }

    /**
     * Dispatches one request and waits for its result to land in the deque. The returned {@link
     * java.util.stream.Stream} is deliberately never consumed - that is the leak being reproduced - so once
     * the deque is non-empty nothing drains it again before the close under test.
     */
    private ConcurrentLinkedDeque<?> produceOneResultAndAwaitIt() {
        vertxAsync.vertxHttpReqInfoStream(context -> {
            log.info("Processing record: {}", context.getSingleConsumerRecord());
            return new RequestInfo("localhost", stubServer.port(), "/", UniMaps.of());
        });

        ConcurrentLinkedDeque<?> deque = getResultDeque();
        awaitUntilTrue(() -> !deque.isEmpty());
        return deque;
    }

    private ConcurrentLinkedDeque<?> getResultDeque() {
        try {
            Field field = JStreamVertxParallelEoSStreamProcessor.class.getDeclaredField("userProcessResultsStream");
            field.setAccessible(true);
            return (ConcurrentLinkedDeque<?>) field.get(vertxAsync);
        } catch (Exception e) {
            throw new RuntimeException("Failed to access userProcessResultsStream field", e);
        }
    }
}
