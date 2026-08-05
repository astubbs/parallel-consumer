package io.confluent.parallelconsumer.vertx;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.csid.utils.WireMockUtils;
import io.confluent.parallelconsumer.PollContext;
import io.confluent.parallelconsumer.vertx.VertxParallelEoSStreamProcessor.RequestInfo;
import com.github.tomakehurst.wiremock.WireMockServer;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.slf4j.MDC;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.concurrent.ConcurrentLinkedQueue;

import static com.google.common.truth.Truth.assertWithMessage;
import static org.awaitility.Awaitility.await;

/**
 * The vert.x event loop is a second thread boundary: the completion hook and the user's web-request callback run there,
 * not on the PC worker thread that the core fix covers.
 *
 * @author Antony Stubbs
 * @see io.confluent.parallelconsumer.internal.MdcPropagation
 */
@Isolated
@Slf4j
class VertxMdcPropagationTest extends VertxBaseUnitTest {

    private static final String CALLER_KEY = "trace_id";
    private static final String CALLER_VALUE = "caller-trace-abc";

    private WireMockServer stubServer;

    private final ConcurrentLinkedQueue<String> threadsUsed = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<String> contextSeen = new ConcurrentLinkedQueue<>();

    @BeforeEach
    void setupWireMock() {
        stubServer = new WireMockUtils().setupWireMock();
    }

    @AfterEach
    void tearDown() {
        stubServer.stop();
        MDC.clear();
    }

    @Test
    void callersContextReachesTheVertxEventLoop() {
        MDC.put(CALLER_KEY, CALLER_VALUE);

        // the completion hook runs on the vert.x event loop, inside the handler PC attaches to the vert.x Future
        vertxAsync.addVertxOnCompleteHook(() -> {
            threadsUsed.add(Thread.currentThread().getName());
            contextSeen.add(String.valueOf(MDC.get(CALLER_KEY)));
        });

        vertxAsync.vertxHttpReqInfoStream((PollContext<String, String> rec) ->
                new RequestInfo("localhost", stubServer.port(), "/", UniMaps.of()));

        await().atMost(defaultTimeout).untilAsserted(() -> {
            assertWithMessage("completion hooks fired").that(contextSeen).isNotEmpty();

            // if this ever stops being true, the test has stopped covering the boundary it exists to cover
            assertWithMessage("the hook must run on the vert.x event loop, not the PC worker thread")
                    .that(threadsUsed.stream().noneMatch(thread -> thread.startsWith("pc-")))
                    .isTrue();

            assertWithMessage("the caller's diagnostic context must be visible on the event loop")
                    .that(contextSeen.stream().allMatch(CALLER_VALUE::equals))
                    .isTrue();
        });
    }

}
