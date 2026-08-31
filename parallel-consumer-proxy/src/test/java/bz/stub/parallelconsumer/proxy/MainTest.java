package bz.stub.parallelconsumer.proxy;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.proxy.protocol.v1.ClientMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.Configure;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyServiceGrpc;
import com.google.common.base.Splitter;
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
     * The shell hosts a real service on that port, and it says what it is: a client that opens a session is
     * answered with {@code UNIMPLEMENTED} naming the missing engine. That is the honest state of this build
     * and it is asserted rather than assumed, because the alternative failure - a call that hangs, or one
     * that is silently accepted and answers nothing - looks identical to a working sidecar from the outside.
     */
    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void aSessionIsAnsweredWithUnimplementedNamingTheMissingEngine() throws Exception {
        var out = new ByteArrayOutputStream();
        var writeEnd = new PipedOutputStream();
        var lifeline = new PipedInputStream(writeEnd);

        var pool = Executors.newSingleThreadExecutor();
        ManagedChannel channel = null;
        try {
            var exit = runInBackground(pool, out, lifeline);
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

    private static Future<Integer> runInBackground(ExecutorService pool, ByteArrayOutputStream out,
                                                   PipedInputStream lifeline) {
        return pool.submit(() -> Main.run(new String[0],
                new PrintStream(out, true, StandardCharsets.UTF_8),
                new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8),
                lifeline));
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
