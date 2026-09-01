package bz.stub.parallelconsumer.proxy;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.proxy.config.ConfigureHandler;
import bz.stub.parallelconsumer.proxy.lifecycle.DrainCoordinator;
import bz.stub.parallelconsumer.proxy.protocol.v1.ClientMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.Configure;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyServiceGrpc;
import com.google.common.base.Splitter;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The sidecar's process lifecycle end to end: refuse arguments, bind, announce the port, serve a real gRPC
 * call, then stop - releasing the socket - when the parent dies.
 * <p>
 * <b>Why the listener-is-gone assertion is here rather than left to the exit code.</b> A run that returns 0
 * proves the method returned; it proves nothing about whether the gRPC server was shut down, because a
 * leaked one lives on daemon-free Netty threads and would keep the port bound long after {@code run}
 * returned. The port is the observable that separates "returned" from "shut down", so the close is asserted
 * against the socket rather than against the return value.
 * <p>
 * <b>This is one class again.</b> The sidecar-shell rung and the U10 lifecycle branch each grew a
 * {@code MainTest} for the same class, differing only in whether an engine was behind it; they are merged
 * here, in {@link Main}'s own package, keeping every assertion either side had. The no-engine lane survives
 * as {@link NoEngineMain}'s contract rather than as the default.
 */
class MainTest {

    /** Generous enough that only a genuine failure reaches it, short enough that a hang is not a wait. */
    private static final long AWAIT_SECONDS = 30;

    /**
     * R39/U7: the sidecar is configured over the protocol, not the command line. Every knob arrives in
     * {@code Configure}, so an argument here is a caller misunderstanding worth failing loudly on rather than
     * a forward-compatible thing to ignore.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void argumentsAreRefusedBecauseConfigurationTravelsTheProtocol() {
        var out = new ByteArrayOutputStream();
        var err = new ByteArrayOutputStream();

        int exit = Main.run(new String[]{"--bootstrap-servers", "localhost:9092"},
                new PrintStream(out, true, StandardCharsets.UTF_8),
                new PrintStream(err, true, StandardCharsets.UTF_8),
                new PipedInputStream());

        assertThat(exit).isEqualTo(Main.EXIT_USAGE);
        assertWithMessage("the refusal must say where configuration actually goes")
                .that(err.toString(StandardCharsets.UTF_8)).contains("Configure");
        assertWithMessage("a refused start must not announce a port it never bound")
                .that(out.toString(StandardCharsets.UTF_8)).doesNotContain(Main.PORT_LINE_PREFIX);
    }

    /**
     * The whole spawning contract in one run: the port is announced on stdout line one so the parent can
     * connect, the socket really answers while the sidecar is up, the process returns cleanly the moment its
     * parent's write end closes, and the socket stops answering once it has.
     */
    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void announcesItsPortServesOnItAndReleasesItWhenTheParentDies() throws Exception {
        var out = new ByteArrayOutputStream();
        var writeEnd = new PipedOutputStream();
        var lifeline = new PipedInputStream(writeEnd);

        var pool = Executors.newSingleThreadExecutor();
        try {
            var exit = runInBackground(pool, out, lifeline);

            int port = awaitAnnouncedPort(out);
            assertWithMessage("an ephemeral port was requested, so it must be a real bound port")
                    .that(port).isGreaterThan(0);
            try (var probe = new Socket()) {
                probe.connect(loopback(port), 2_000);
                assertWithMessage("the announced port must be the one actually serving")
                        .that(probe.isConnected()).isTrue();
            }

            writeEnd.close(); // the parent dies

            assertThat(exit.get(AWAIT_SECONDS, TimeUnit.SECONDS)).isEqualTo(0);
            assertThrows(IOException.class, () -> {
                try (var probe = new Socket()) {
                    probe.connect(loopback(port), 2_000);
                }
            }, "the listener must be gone once the sidecar has shut down, not merely unattended");
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * The <b>no-engine</b> build hosts a real service on that port, and it says what it is: a client that
     * opens a session is answered with {@code UNIMPLEMENTED} naming the missing engine. It is asserted rather
     * than assumed, because the alternative failure - a call that hangs, or one that is silently accepted and
     * answers nothing - looks identical to a working sidecar from the outside.
     * <p>
     * This is the contract {@link NoEngineMain} publishes and that eight cross-language
     * {@code SidecarHandshakeTest}s assert against a spawned process. It is reached here through the session
     * seam rather than through the default, because the default now hosts the engine - see
     * {@link #theProductionEntryPointHostsTheEngine()}.
     */
    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void aNoEngineSessionIsAnsweredWithUnimplementedNamingTheMissingEngine() throws Exception {
        var out = new ByteArrayOutputStream();
        var writeEnd = new PipedOutputStream();
        var lifeline = new PipedInputStream(writeEnd);

        var pool = Executors.newSingleThreadExecutor();
        ManagedChannel channel = null;
        try {
            var exit = runInBackground(pool, out, lifeline, NoEngineSessionService::new);
            int port = awaitAnnouncedPort(out);

            channel = NettyChannelBuilder.forAddress(InetAddress.getLoopbackAddress().getHostAddress(), port)
                    .usePlaintext()
                    .build();

            var terminated = new CountDownLatch(1);
            var failure = new AtomicReference<Throwable>();
            List<ProxyMessage> replies = Collections.synchronizedList(new ArrayList<>());
            StreamObserver<ClientMessage> session = ProxyServiceGrpc.newStub(channel)
                    .session(new StreamObserver<>() {
                        @Override
                        public void onNext(ProxyMessage message) {
                            replies.add(message);
                        }

                        @Override
                        public void onError(Throwable t) {
                            failure.set(t);
                            terminated.countDown();
                        }

                        @Override
                        public void onCompleted() {
                            terminated.countDown();
                        }
                    });
            session.onNext(ClientMessage.newBuilder()
                    .setConfigure(Configure.newBuilder().addTopics("input"))
                    .build());

            assertWithMessage("the session must terminate rather than hang")
                    .that(terminated.await(AWAIT_SECONDS, TimeUnit.SECONDS)).isTrue();
            Throwable observed = failure.get();
            assertThat(observed).isInstanceOf(StatusRuntimeException.class);
            var status = ((StatusRuntimeException) observed).getStatus();
            assertThat(status.getCode()).isEqualTo(Status.Code.UNIMPLEMENTED);
            assertWithMessage("the refusal must name what is missing, or a client author debugs their own code")
                    .that(status.getDescription()).contains("hosts no Parallel Consumer engine");
            assertWithMessage("a build with no engine must not answer a Configure with anything")
                    .that(replies).isEmpty();

            writeEnd.close();
            assertThat(exit.get(AWAIT_SECONDS, TimeUnit.SECONDS)).isEqualTo(0);
        } finally {
            if (channel != null) {
                channel.shutdownNow();
            }
            pool.shutdownNow();
        }
    }

    /**
     * Two sidecars on one host must not collide. Each binds its own ephemeral port and announces that port on
     * its own stdout - so a machine running two applications gets two working sidecars, not a bind race.
     */
    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void twoSidecarsBindDifferentPortsAndEachAnnouncesItsOwn() throws Exception {
        var pool = Executors.newFixedThreadPool(2);
        var firstOut = new ByteArrayOutputStream();
        var secondOut = new ByteArrayOutputStream();
        var firstWrite = new PipedOutputStream();
        var secondWrite = new PipedOutputStream();

        try {
            var first = runInBackground(pool, firstOut, new PipedInputStream(firstWrite));
            var second = runInBackground(pool, secondOut, new PipedInputStream(secondWrite));

            int firstPort = awaitAnnouncedPort(firstOut);
            int secondPort = awaitAnnouncedPort(secondOut);
            assertThat(firstPort).isNotEqualTo(secondPort);

            firstWrite.close();
            secondWrite.close();
            assertThat(first.get(AWAIT_SECONDS, TimeUnit.SECONDS)).isEqualTo(0);
            assertThat(second.get(AWAIT_SECONDS, TimeUnit.SECONDS)).isEqualTo(0);
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * A port it cannot bind is a distinguishable failure, and a different one from a caller who mistyped an
     * argument: the invocation was fine, the socket was not. An operator who sees a usage exit code goes
     * looking at the command line for what is actually a port collision.
     * <p>
     * Provoked through the package-private overload, because the production entry point deliberately has no
     * way to name a port - and an exit code no test can reach is one nobody can rely on.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void aPortItCannotBindIsADistinguishableNonZeroExit() throws Exception {
        try (var occupier = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            var out = new ByteArrayOutputStream();
            var err = new ByteArrayOutputStream();

            int exit = Main.run(new String[0],
                    new PrintStream(out, true, StandardCharsets.UTF_8),
                    new PrintStream(err, true, StandardCharsets.UTF_8),
                    new PipedInputStream(),
                    NoEngineSessionService::new,
                    occupier.getLocalPort());

            assertThat(exit).isEqualTo(Main.EXIT_BIND_FAILED);
            assertWithMessage("a bind failure must not be mistaken for a usage error")
                    .that(Main.EXIT_BIND_FAILED).isNotEqualTo(Main.EXIT_USAGE);
            assertWithMessage("nothing was bound, so nothing may be announced")
                    .that(out.toString(StandardCharsets.UTF_8)).doesNotContain(Main.PORT_LINE_PREFIX);
            assertThat(err.toString(StandardCharsets.UTF_8)).contains("could not bind");
        }
    }

    /** The production entry point, engine and all - which is what most of these assertions are about. */
    /**
     * The transport flag must not weaken R39, and the way to check that is that a CONFIGURATION flag is
     * still refused now that one argument is accepted. {@code --socket} says where to listen, which the
     * spawning parent must know before a session exists - the same category as the port, and not a knob
     * the Configure message could carry.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void theTransportFlagIsNamedInTheUsageAndConfigurationIsStillRefused() {
        var out = new ByteArrayOutputStream();
        var err = new ByteArrayOutputStream();

        int exit = Main.run(new String[]{"--ordering", "KEY"},
                new PrintStream(out, true, StandardCharsets.UTF_8),
                new PrintStream(err, true, StandardCharsets.UTF_8),
                new PipedInputStream());

        assertThat(exit).isEqualTo(Main.EXIT_USAGE);
        var usage = err.toString(StandardCharsets.UTF_8);
        assertWithMessage("a refused argument must name the one that IS accepted")
                .that(usage).contains(Main.SOCKET_FLAG);
        assertWithMessage("the refusal must still say where configuration actually goes")
                .that(usage).contains("Configure");
        assertWithMessage("a refused start must announce no listener of either kind")
                .that(out.toString(StandardCharsets.UTF_8)).doesNotContain(Main.SOCKET_LINE_PREFIX);
    }

    /**
     * A drain that gave up with records still held is not a clean exit, and an operator has to be able to
     * tell the two apart: those records were left uncommitted for redelivery rather than resolved.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void drainTimingOutIsADistinguishableNonZeroExit() {
        assertThat(Main.exitCodeFor(DrainCoordinator.Outcome.TIMED_OUT))
                .isEqualTo(Main.EXIT_DRAIN_TIMED_OUT);
        assertThat(Main.exitCodeFor(DrainCoordinator.Outcome.DRAINED)).isEqualTo(0);
        assertWithMessage("a drain that timed out must not look like a clean exit")
                .that(Main.EXIT_DRAIN_TIMED_OUT).isNotEqualTo(0);
    }

    /**
     * Unit U10's substitution, asserted rather than described. The production entry point hosts the real
     * connect-time configuration handler; the no-engine service is a test fixture reached through the seam,
     * and {@link NoEngineMain} is what spawns it. Without this assertion the two could quietly swap back and
     * every other test here would still pass - the lifecycle is identical either way, which is the whole
     * point of the seam and also exactly what makes the regression invisible.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void theProductionEntryPointHostsTheEngine() {
        assertWithMessage("the production sidecar must host the connect-time configuration handler")
                .that(Main.sessionServiceFactory().get()).isInstanceOf(ConfigureHandler.class);
        assertWithMessage("the no-engine build must remain spawnable for the handshake tests")
                .that(new NoEngineSessionService()).isNotNull();
    }

    private static Future<Integer> runInBackground(ExecutorService pool, ByteArrayOutputStream out,
                                                   PipedInputStream lifeline) {
        return pool.submit(() -> Main.run(new String[0],
                new PrintStream(out, true, StandardCharsets.UTF_8),
                new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8),
                lifeline));
    }

    /**
     * The same lifecycle hosting a chosen service - how the no-engine build is reached, which is what
     * {@link NoEngineMain} spawns and what the eight cross-language handshake tests point at.
     */
    private static Future<Integer> runInBackground(ExecutorService pool, ByteArrayOutputStream out,
                                                   PipedInputStream lifeline,
                                                   Supplier<BindableService> sessionService) {
        return pool.submit(() -> Main.run(new String[0],
                new PrintStream(out, true, StandardCharsets.UTF_8),
                new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8),
                lifeline,
                sessionService,
                0));
    }

    private static InetSocketAddress loopback(int port) {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), port);
    }

    /** Polls the captured stdout for the port line rather than sleeping a guessed interval. */
    private static int awaitAnnouncedPort(ByteArrayOutputStream out) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(AWAIT_SECONDS);
        while (System.nanoTime() < deadline) {
            var text = out.toString(StandardCharsets.UTF_8);
            for (var line : Splitter.on('\n').split(text)) {
                if (line.startsWith(Main.PORT_LINE_PREFIX)) {
                    return Integer.parseInt(line.substring(Main.PORT_LINE_PREFIX.length()).trim());
                }
            }
            Thread.sleep(20);
        }
        throw new AssertionError("no '" + Main.PORT_LINE_PREFIX + "' line appeared on stdout");
    }
}
