package io.confluent.parallelconsumer.dashboard.server;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import ch.qos.logback.classic.spi.ILoggingEvent;
import io.confluent.parallelconsumer.dashboard.DashboardOptions;
import io.confluent.parallelconsumer.dashboard.DashboardServer;
import io.confluent.parallelconsumer.dashboard.snapshot.SnapshotPublisher;
import io.confluent.parallelconsumer.dashboard.snapshot.StateSampler;
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.IOException;
import java.time.Duration;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * The server's lifecycle and the properties that hold across the whole route table: where it binds, how it finds a
 * port, what it says about it, and what it refuses.
 * <p>
 * <strong>{@code @Isolated} for a reason.</strong> This repository runs the unit suite in parallel (see core's
 * {@code junit-platform.properties}, which reaches this module through the core test jar). Two assertions here are
 * about global process state and cannot survive a neighbour: "exactly one line was logged" reads a logger every other
 * dashboard test also writes to, and the port-walk test claims a specific consecutive port range that a concurrently
 * starting server would take out from under it. Both were observed failing exactly that way before this annotation.
 */
@Isolated
class DashboardServerTest {

    @Test
    void defaultsBindLoopbackAndNothingElse() throws IOException {
        assertThat(DashboardOptions.defaults().getBindAddress().isLoopbackAddress()).isTrue();

        try (DashboardServer server = new DashboardServer(DashboardTestSupport.populatedPublisher(), null,
                DashboardTestSupport.testOptions().build()).start()) {
            assertThat(RawHttp.get(server.getPort(), DashboardServer.STATE_PATH).statusCode).isEqualTo(200);

            InetAddress external = anExternalAddress();
            assumeThat(external)
                    .as("this machine has no non-loopback IPv4 address to test unreachability against")
                    .isNotNull();

            assertThatThrownBy(() -> {
                try (Socket socket = new Socket()) {
                    socket.connect(new InetSocketAddress(external, server.getPort()), 2000);
                }
            }).as("a loopback-bound dashboard must not answer on %s", external)
                    .isInstanceOf(IOException.class);
        }
    }

    @Test
    void theStartupLineIsTheOnlyThingLoggedAndItCarriesAClickableUrl() {
        try (DashboardTestSupport.LogCapture logs = DashboardTestSupport.captureLogs(DashboardServer.class);
             DashboardServer server = new DashboardServer(DashboardTestSupport.populatedPublisher(), null,
                     DashboardTestSupport.testOptions().build()).start()) {

            List<String> messages = logs.formattedMessages();
            assertThat(messages).hasSize(1);
            assertThat(messages.get(0))
                    .contains("EXPERIMENTAL")
                    .contains("read-only")
                    .contains("http://127.0.0.1:" + server.getPort() + "/");
            assertThat(server.getUrl()).isEqualTo("http://127.0.0.1:" + server.getPort() + "/");
        }
    }

    @Test
    void thePortWalkIsSilentAndLandsOnTheFirstFreePortAboveTheOccupiedOnes() throws IOException {
        int start = DashboardTestSupport.freePort();
        List<ServerSocket> occupied = new ArrayList<>();
        try {
            for (int offset = 0; offset < 3; offset++) {
                ServerSocket socket = new ServerSocket();
                socket.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), start + offset));
                occupied.add(socket);
            }

            try (DashboardTestSupport.LogCapture logs = DashboardTestSupport.captureLogs(DashboardServer.class);
                 DashboardServer server = new DashboardServer(DashboardTestSupport.populatedPublisher(), null,
                         DashboardOptions.builder().port(start).maxPortAttempts(50).build()).start()) {

                assertThat(server.getPort())
                        .as("8080, 8081 and 8082 taken means 8083 - the walk, not a failure")
                        .isEqualTo(start + 3);

                List<ILoggingEvent> events = logs.events();
                assertThat(events)
                        .as("a busy port is not news; three failed binds must not log three lines: %s",
                                logs.formattedMessages())
                        .hasSize(1);
                assertThat(events.get(0).getFormattedMessage()).contains(":" + (start + 3) + "/");
            }
        } finally {
            for (ServerSocket socket : occupied) {
                socket.close();
            }
        }
    }

    @Test
    void exhaustingTheSearchFailsWithAMessageNamingThePortRatherThanAnOpaqueBindError() throws IOException {
        int port = DashboardTestSupport.freePort();
        try (ServerSocket occupied = new ServerSocket()) {
            occupied.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), port));

            DashboardServer server = new DashboardServer(DashboardTestSupport.populatedPublisher(), null,
                    DashboardOptions.builder().port(port).maxPortAttempts(1).build());
            try {
                assertThatThrownBy(server::start)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining(String.valueOf(port))
                        .hasMessageContaining("maxPortAttempts");
            } finally {
                server.close();
            }
        }
    }

    @Test
    void writeMethodsAreRefusedOnEveryRouteInTheTable() throws IOException {
        try (DashboardServer server = new DashboardServer(DashboardTestSupport.populatedPublisher(), null,
                DashboardTestSupport.testOptions().build()).start()) {

            List<String> paths = server.getProbePaths();
            assertThat(paths).contains(DashboardServer.STATE_PATH, DashboardServer.STREAM_PATH,
                    DashboardServer.STATUS_PATH, "/", "/no-such-path");

            for (String path : paths) {
                for (String method : Arrays.asList("POST", "PUT", "DELETE", "PATCH", "OPTIONS", "TRACE")) {
                    RawHttp.Response response =
                            RawHttp.request(method, server.getPort(), path, new LinkedHashMap<>());

                    assertThat(response.statusCode).as("%s %s", method, path).isEqualTo(405);
                    assertThat(response.header("allow")).isEqualTo("GET, HEAD");
                }
            }
        }
    }

    @Test
    void headIsServedSoAMonitorCanCheckWithoutDownloading() throws IOException {
        try (DashboardServer server = new DashboardServer(DashboardTestSupport.populatedPublisher(), null,
                DashboardTestSupport.testOptions().build()).start()) {

            RawHttp.Response response = RawHttp.request("HEAD", server.getPort(), DashboardServer.STATE_PATH,
                    new LinkedHashMap<>());

            assertThat(response.statusCode).isEqualTo(200);
            assertThat(response.body).isEmpty();
        }
    }

    @Test
    void aHandlerThatThrowsAnswers500WithoutHandingTheClientAStackTrace() throws IOException {
        // a publisher that blows up when read is the realistic shape of this failure: the read path is the only
        // thing a request handler touches, so a bug in it is what would reach the client
        SnapshotPublisher exploding = new SnapshotPublisher(
                new StateSampler((AbstractParallelEoSStreamProcessor<?, ?>) null, null)) {
            @Override
            public Snapshots getSnapshots() {
                throw new IllegalStateException("internal detail nobody outside should see");
            }
        };
        try (DashboardServer server = new DashboardServer(exploding, null, DashboardTestSupport.testOptions()
                // long enough that the stream route's timer never fires during this test
                .updateInterval(Duration.ofMinutes(10))
                .build()).start()) {

            RawHttp.Response response = RawHttp.get(server.getPort(), DashboardServer.STATE_PATH);

            assertThat(response.statusCode).isEqualTo(500);
            assertThat(response.body)
                    .doesNotContain("internal detail")
                    .doesNotContain("IllegalStateException")
                    .doesNotContain("io.confluent");

            // and the server is still serving - one bad handler must not take the event loop with it
            assertThat(RawHttp.get(server.getPort(), "/").statusCode).isEqualTo(200);
        }
    }

    @Test
    void twoDashboardsInOneJvmDoNotInterfere() throws IOException {
        try (DashboardServer first = new DashboardServer(DashboardTestSupport.populatedPublisher(), null,
                DashboardTestSupport.testOptions().build()).start();
             DashboardServer second = new DashboardServer(DashboardTestSupport.populatedPublisher(), null,
                     DashboardTestSupport.testOptions().build()).start()) {

            assertThat(first.getPort()).isNotEqualTo(second.getPort());
            assertThat(RawHttp.get(first.getPort(), DashboardServer.STATE_PATH).statusCode).isEqualTo(200);
            assertThat(RawHttp.get(second.getPort(), DashboardServer.STATE_PATH).statusCode).isEqualTo(200);

            first.close();

            assertThat(RawHttp.get(second.getPort(), DashboardServer.STATE_PATH).statusCode)
                    .as("closing one dashboard must not close the other's event loop")
                    .isEqualTo(200);
        }
    }

    @Test
    void closeReleasesThePortAndIsIdempotent() throws IOException {
        DashboardServer server = new DashboardServer(DashboardTestSupport.populatedPublisher(), null,
                DashboardTestSupport.testOptions().build()).start();
        int port = server.getPort();

        server.close();
        server.close();

        try (ServerSocket reclaimed = new ServerSocket()) {
            reclaimed.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), port));
            assertThat(reclaimed.getLocalPort()).isEqualTo(port);
        }
    }

    @Test
    void startingTwiceIsRefusedRatherThanQuietlyLeakingAServer() {
        try (DashboardServer server = new DashboardServer(DashboardTestSupport.populatedPublisher(), null,
                DashboardTestSupport.testOptions().build()).start()) {
            assertThatThrownBy(server::start).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void aNonLoopbackBindWarnsAndSaysExactlyWhatItExposes() {
        try (DashboardTestSupport.LogCapture logs = DashboardTestSupport.captureLogs(DashboardServer.class);
             DashboardServer server = new DashboardServer(DashboardTestSupport.populatedPublisher(), null,
                     DashboardOptions.builder()
                             .bindAddress(anyLocalAddress())
                             .port(DashboardTestSupport.freePort())
                             .maxPortAttempts(50)
                             .extraAllowedHosts(Collections.singleton("127.0.0.1"))
                             .build()).start()) {

            assertThat(server.getPort()).isPositive();
            String warning = logs.formattedMessages().stream()
                    .filter(message -> message.contains("NON-LOOPBACK"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("no exposure warning in " + logs.formattedMessages()));

            assertThat(warning)
                    .contains("0.0.0.0")
                    .contains("unauthenticated")
                    .contains("consumer group id")
                    .contains("topic names")
                    .contains("partition assignments")
                    .contains("offsets");
        }
    }

    private static InetAddress anyLocalAddress() {
        try {
            return InetAddress.getByName("0.0.0.0");
        } catch (Exception e) {
            throw new AssertionError("0.0.0.0 did not parse", e);
        }
    }

    /**
     * A routable IPv4 address of this machine, or null if it has none - a container with only loopback is a real
     * environment and the test that needs this skips rather than fails there.
     */
    private static InetAddress anExternalAddress() {
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            List<NetworkInterface> all = interfaces == null
                    ? Collections.emptyList()
                    : Collections.list(interfaces);
            for (NetworkInterface networkInterface : all) {
                if (!networkInterface.isUp() || networkInterface.isLoopback()) {
                    continue;
                }
                for (InetAddress address : Collections.list(networkInterface.getInetAddresses())) {
                    if (!address.isLoopbackAddress() && address.getAddress().length == 4) {
                        return address;
                    }
                }
            }
        } catch (SocketException e) {
            return null;
        }
        return null;
    }
}
