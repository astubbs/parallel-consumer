package bz.stub.parallelconsumer.proxy.transport;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.util.Collections;
import java.util.Optional;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The bind posture of R17 and R18: loopback and an ephemeral port by default, a socket a non-loopback local
 * address cannot reach, and - covering AE6 - a non-loopback bind that refuses to start without the opt-in
 * (naming the missing setting) and starts with it while logging the full unauthenticated-surface warning.
 */
@Timeout(value = 30)
class ProxyServerTest {

    @Test
    void bindsLoopbackOnAnEphemeralPortByDefault() throws Exception {
        try (ProxyServer server = ProxyServer.builder()
                .sessionService(new CountingSessionService())
                .build()
                .start()) {
            assertWithMessage("an ephemeral port was requested, so a real one must have been chosen")
                    .that(server.port()).isGreaterThan(0);
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), server.port()), 2_000);
                assertThat(socket.isConnected()).isTrue();
            }
        }
    }

    @Test
    void serverSocketIsNotReachableFromANonLoopbackLocalAddress() throws Exception {
        Optional<InetAddress> nonLoopback = aNonLoopbackLocalAddress();
        assumeTrue(nonLoopback.isPresent(), "this host has no non-loopback address to probe from");

        try (ProxyServer server = ProxyServer.builder()
                .sessionService(new CountingSessionService())
                .build()
                .start()) {
            int port = server.port();
            assertThrows(IOException.class, () -> {
                try (Socket socket = new Socket()) {
                    socket.connect(new InetSocketAddress(nonLoopback.get(), port), 2_000);
                }
            }, "the server bound loopback only, so its port must not answer on " + nonLoopback.get());
        }
    }

    /** Covers AE6, refusal half: without the opt-in, a non-loopback bind refuses and names the setting. */
    @Test
    void nonLoopbackBindWithoutTheOptInRefusesToStartAndNamesTheSetting() throws Exception {
        ProxyServer server = ProxyServer.builder()
                .sessionService(new CountingSessionService())
                .bindAddress(InetAddress.getByName("0.0.0.0"))
                .build();

        IllegalStateException refusal = assertThrows(IllegalStateException.class, server::start);

        assertWithMessage("the refusal must name the missing opt-in setting")
                .that(refusal).hasMessageThat().contains(ProxyServer.NON_LOOPBACK_OPT_IN);
        assertThrows(IllegalStateException.class, server::port, "nothing may have been bound");
    }

    /**
     * Covers AE6, opt-in half: with the opt-in the server starts, and warns with R18's full statement - not
     * just the missing authentication, but the offset-advancement capability and the receipt of credentials
     * and a class-instantiating property map. A warning naming only offsets understates the surface (KTD11).
     */
    @Test
    void nonLoopbackBindWithTheOptInStartsAndLogsTheUnauthenticatedSurfaceWarning() throws Exception {
        var warnings = captureLogFor(ProxyServer.class);
        try (ProxyServer server = ProxyServer.builder()
                .sessionService(new CountingSessionService())
                .bindAddress(InetAddress.getByName("0.0.0.0"))
                .exposeUnauthenticatedSurfaceBeyondLoopback()
                .build()
                .start()) {
            assertThat(server.port()).isGreaterThan(0);

            String warning = warnings.list.stream()
                    .filter(event -> event.getLevel().toString().equals("WARN"))
                    .map(ILoggingEvent::getFormattedMessage)
                    .filter(message -> message.contains("NO AUTHENTICATION"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("no unauthenticated-surface warning was logged"));
            assertThat(warning).contains("offsets");
            assertThat(warning).contains("credentials");
            assertThat(warning).contains("class-valued");
        } finally {
            ((Logger) LoggerFactory.getLogger(ProxyServer.class)).detachAppender(warnings);
        }
    }

    private static ListAppender<ILoggingEvent> captureLogFor(Class<?> loggerOwner) {
        var appender = new ListAppender<ILoggingEvent>();
        appender.start();
        ((Logger) LoggerFactory.getLogger(loggerOwner)).addAppender(appender);
        return appender;
    }

    private static Optional<InetAddress> aNonLoopbackLocalAddress() throws IOException {
        for (NetworkInterface networkInterface : Collections.list(NetworkInterface.getNetworkInterfaces())) {
            for (InetAddress address : Collections.list(networkInterface.getInetAddresses())) {
                if (!address.isLoopbackAddress() && !address.isLinkLocalAddress()) {
                    return Optional.of(address);
                }
            }
        }
        return Optional.empty();
    }
}
