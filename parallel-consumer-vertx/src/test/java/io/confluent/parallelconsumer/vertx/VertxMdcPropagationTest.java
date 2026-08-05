package io.confluent.parallelconsumer.vertx;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.github.tomakehurst.wiremock.WireMockServer;
import io.confluent.csid.utils.WireMockUtils;
import io.confluent.parallelconsumer.MdcBoundaryProbe;
import io.confluent.parallelconsumer.PollContext;
import io.confluent.parallelconsumer.vertx.VertxParallelEoSStreamProcessor.RequestInfo;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import pl.tlinkowski.unij.api.UniMaps;

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

    private final MdcBoundaryProbe probe = new MdcBoundaryProbe();

    private WireMockServer stubServer;

    @BeforeEach
    void setupWireMock() {
        stubServer = new WireMockUtils().setupWireMock();
    }

    @AfterEach
    void tearDown() {
        stubServer.stop();
        probe.clearCallersContext();
    }

    @Test
    void callersContextReachesTheVertxEventLoop() {
        probe.establishCallersContext();

        // the completion hook runs on the vert.x event loop, inside the handler PC attaches to the vert.x Future
        vertxAsync.addVertxOnCompleteHook(probe::observeCurrentThread);

        vertxAsync.vertxHttpReqInfoStream((PollContext<String, String> rec) ->
                new RequestInfo("localhost", stubServer.port(), "/", UniMaps.of()));

        await().atMost(defaultTimeout).untilAsserted(() -> {
            // the base class primes the records, so the count is not fixed here - at least one hook must have fired
            assertWithMessage("completion hooks fired").that(probe.observations()).isNotEmpty();

            // vert.x names its event loop threads, but the hook could also be invoked inline on the caller - what must
            // hold is that it is not a PC worker thread, or the test has stopped covering the boundary it exists for
            probe.assertObservedOnlyOn("vert.x event loop", thread -> !thread.startsWith("pc-"));

            probe.assertCallersContextWasVisible("vert.x event loop");
        });
    }

}
