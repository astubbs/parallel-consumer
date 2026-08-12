package bz.stub.parallelconsumer.dashboard.server;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import ch.qos.logback.classic.spi.ILoggingEvent;
import bz.stub.parallelconsumer.dashboard.DashboardOptions;
import bz.stub.parallelconsumer.dashboard.DashboardServer;
import bz.stub.parallelconsumer.dashboard.snapshot.PcMeterFixture;
import bz.stub.parallelconsumer.dashboard.snapshot.SnapshotPublisher;
import bz.stub.parallelconsumer.dashboard.snapshot.StateSampler;
import bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

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
                // the stream route only reads the publisher when a snapshot is published, and nothing here publishes
                // one - but a ceiling this long makes that independent of timing either way
                .updateInterval(Duration.ofMinutes(10))
                .build()).start()) {

            RawHttp.Response response = RawHttp.get(server.getPort(), DashboardServer.STATE_PATH);

            assertThat(response.statusCode).isEqualTo(500);
            assertThat(response.body)
                    .doesNotContain("internal detail")
                    .doesNotContain("IllegalStateException")
                    .doesNotContain("bz.stub");

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

    /**
     * Closing the dashboard must take the sampler off the user's control loop.
     * <p>
     * It cannot be deregistered - core has no {@code removeLoopEndCallBack} - so {@code close()} stops it through the
     * publisher's own flag. Without that, a closed dashboard keeps walking the whole meter registry and allocating a
     * snapshot on the control thread for the life of the consumer, and every start/stop cycle adds another one. The
     * assertion runs the CAPTURED callback, because that is exactly what the control loop does with it.
     */
    @Test
    void closingStopsTheSamplerItPutOnTheControlLoop() {
        AbstractParallelEoSStreamProcessor<?, ?> pc = Mockito.mock(AbstractParallelEoSStreamProcessor.class);
        ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);

        DashboardServer server = DashboardServer.startFor(pc, PcMeterFixture.fullyPopulated().getRegistry(),
                DashboardTestSupport.testOptions().build());
        try {
            Mockito.verify(pc).addLoopEndCallBack(captor.capture());
            Runnable controlLoopCallback = captor.getValue();
            SnapshotPublisher publisher = server.getPublisher();

            controlLoopCallback.run();
            controlLoopCallback.run();
            long sequenceBeforeClose = publisher.getCurrent().getSampleSequence();
            assertThat(sequenceBeforeClose).isEqualTo(2L);

            server.close();
            for (int i = 0; i < 20; i++) {
                controlLoopCallback.run();
            }

            assertThat(publisher.isSamplingStopped()).isTrue();
            assertThat(publisher.getCurrent().getSampleSequence())
                    .as("a closed dashboard must not keep sampling on the consumer's control loop")
                    .isEqualTo(sequenceBeforeClose);
        } finally {
            server.close();
        }
    }

    /**
     * The other side of the ownership rule: a publisher handed in through the public constructor belongs to the
     * caller, who may be sharing it, so closing a dashboard must not silently stop somebody else's sampling.
     */
    @Test
    void closingDoesNotStopAPublisherItWasMerelyGiven() {
        SnapshotPublisher publisher = DashboardTestSupport.populatedPublisher();

        DashboardServer server = new DashboardServer(publisher, null,
                DashboardTestSupport.testOptions().build()).start();
        server.close();

        assertThat(publisher.isSamplingStopped())
                .as("this server did not create it, so it is not this server's to stop")
                .isFalse();
        publisher.sampleOnce();
        assertThat(publisher.getCurrent()).isNotNull();
    }

    /**
     * A dashboard that could not bind must leave nothing behind on the consumer.
     * <p>
     * {@code startFor} constructs the server inline, so a caller of the throwing path never receives a handle: a
     * callback registered before the bind could never be removed, and the {@link io.vertx.core.Vertx} instance -
     * event-loop threads and all - could never be closed. So registration happens only after a successful bind, and
     * the Vertx instance is closed by {@code start()} itself on the way out.
     */
    @Test
    void aFailedStartRegistersNothingOnTheConsumer() throws IOException {
        AbstractParallelEoSStreamProcessor<?, ?> pc = Mockito.mock(AbstractParallelEoSStreamProcessor.class);
        int port = DashboardTestSupport.freePort();
        try (ServerSocket occupied = new ServerSocket()) {
            occupied.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), port));

            assertThatThrownBy(() -> DashboardServer.startFor(pc, PcMeterFixture.fullyPopulated().getRegistry(),
                    DashboardOptions.builder().port(port).maxPortAttempts(1).build()))
                    .isInstanceOf(IllegalStateException.class);

            Mockito.verify(pc, Mockito.never()).addLoopEndCallBack(Mockito.any());
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
