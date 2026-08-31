package bz.stub.parallelconsumer.proxy.transport;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.proxy.protocol.v1.ClientMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.Configure;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;

/**
 * The wire-test fixture the interceptor tests share: a default-configured {@link ProxyServer} hosting one
 * {@link CountingSessionService} per test, plus the {@code Configure} opener every admitted stream leads with.
 * Sits beside the package's other shared fixtures ({@link CountingSessionService},
 * {@link RecordingProxyMessageObserver}). {@code ProxyServerTest} deliberately does not extend this - its
 * scenarios each build a differently configured server, which is the very thing under test there.
 *
 * @author Antony Stubbs
 */
abstract class WireTestBase {

    CountingSessionService service;
    ProxyServer server;

    @BeforeEach
    void startServer() throws IOException {
        service = new CountingSessionService();
        server = ProxyServer.builder().sessionService(service).build().start();
    }

    @AfterEach
    void stopServer() {
        server.close();
    }

    static ClientMessage configure(String topic) {
        return ClientMessage.newBuilder()
                .setConfigure(Configure.newBuilder().addTopics(topic))
                .build();
    }
}
