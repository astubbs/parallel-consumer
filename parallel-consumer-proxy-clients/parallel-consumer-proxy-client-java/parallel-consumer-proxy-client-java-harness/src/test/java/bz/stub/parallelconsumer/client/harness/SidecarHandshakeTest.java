package bz.stub.parallelconsumer.client.harness;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.client.ClientOptions;
import bz.stub.parallelconsumer.client.grpc.GrpcParallelConsumerClient;
import bz.stub.parallelconsumer.proxy.Main;
import bz.stub.parallelconsumer.proxy.NoEngineMain;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * <b>The Java client's handshake, against the real sidecar rather than a stand-in.</b> The gRPC transport
 * builds its own channel, sends {@code Configure} as the stream's first message, and reports what came back;
 * this drives that path end to end through {@code parallel-consumer-proxy}'s <b>no-engine</b> entry point,
 * {@code NoEngineMain} - the real bind, the real authority allowlist, the real single-connection guard, and a
 * real session service, all of them the production lifecycle with one supplier swapped.
 * <p>
 * <b>What it can prove, and what it deliberately cannot.</b> The sidecar here hosts no Parallel
 * Consumer engine and answers every session {@code UNIMPLEMENTED}, so there is no dispatch to observe and none
 * is faked. That is now a deliberate choice of entry point rather than the state of the build: the production
 * {@code Main} hosts the engine, and the engine-backed run belongs to the tests that spawn it.
 * <p>
 * What remains is worth its own test anyway, because it is the one claim a client library makes that
 * nothing else on this rung touches: <b>the wire reaches the service, and the transport hands the caller what
 * the service said.</b> {@code SessionEndTest} drives the same transport against a fake proxy in-JVM, which
 * proves the transport's reaction to a scripted stream and nothing about whether a real server would ever
 * admit it.
 * <p>
 * <b>The status code is the assertion, not merely "it failed".</b> A refusal from the authority allowlist is
 * {@code PERMISSION_DENIED} and one from the admission slot is {@code RESOURCE_EXHAUSTED}, both raised by
 * interceptors <em>before</em> the service method runs. Only {@code UNIMPLEMENTED} can have come from the
 * service itself, so the code is what separates "the connection was turned away" from "the handshake was
 * delivered and answered". {@link #aSidecarThatIsNotListeningFailsDifferentlyFromOneThatRefuses} is the control
 * arm on the other side: a failure that is not this one looks nothing like it.
 * <p>
 * The engine-backed run - dispatch, outcomes, produce, the offset advancing - belongs to the conformance rung
 * stacked above this one, and this class is where it lands.
 *
 * @author Antony Stubbs
 */
class SidecarHandshakeTest {

    /** Generous enough that only a genuine failure reaches it, short enough that a hang is not a wait. */
    private static final long AWAIT_SECONDS = 30;

    /**
     * The whole claim in one run: a real client against a real sidecar gets the sidecar's own refusal, with the
     * status code that says it came from the service rather than from an admission rule, and the same cause
     * reaches {@code sessionEnd()} - the surface an application watches to learn its session died.
     */
    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void theHandshakeReachesTheSessionServiceAndItsRefusalReachesTheCaller() throws Exception {
        var out = new ByteArrayOutputStream();
        var writeEnd = new PipedOutputStream();
        var lifeline = new PipedInputStream(writeEnd);

        var pool = Executors.newSingleThreadExecutor();
        try {
            Future<Integer> exit = runSidecar(pool, out, lifeline);
            int port = awaitAnnouncedPort(out);

            try (var client = clientOn(port)) {
                var refused = assertThrows(ExecutionException.class,
                        () -> client.connect().toCompletableFuture().get(AWAIT_SECONDS, TimeUnit.SECONDS),
                        "the sidecar hosts no engine, so the handshake must be refused rather than answered");

                var status = statusOf(refused);
                assertWithMessage("UNIMPLEMENTED is the only code the session SERVICE raises - the allowlist "
                        + "answers PERMISSION_DENIED and the admission slot RESOURCE_EXHAUSTED, both before the "
                        + "service method runs, so this code is what proves the handshake was delivered")
                        .that(status.getCode()).isEqualTo(Status.Code.UNIMPLEMENTED);
                assertWithMessage("the refusal must name what is missing, or a client author debugs their own "
                        + "code")
                        .that(status.getDescription()).contains("hosts no Parallel Consumer engine");

                var ended = assertThrows(ExecutionException.class,
                        () -> client.sessionEnd().toCompletableFuture().get(AWAIT_SECONDS, TimeUnit.SECONDS),
                        "a session that was refused has ended, and the caller learns it from sessionEnd");
                assertWithMessage("the same cause reaches the surface an application actually watches")
                        .that(statusOf(ended).getCode()).isEqualTo(Status.Code.UNIMPLEMENTED);
            }

            writeEnd.close(); // the parent dies
            assertThat(exit.get(AWAIT_SECONDS, TimeUnit.SECONDS)).isEqualTo(0);
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * The control arm, and it is permanent rather than a one-off demonstration: pointed at a port nothing is
     * listening on, the same client fails in a way that is not the refusal above. Without it, the test that
     * matters could be passing on any failure at all - which is the shape of an assertion that cannot fail for
     * the reason it names.
     */
    @Test
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void aSidecarThatIsNotListeningFailsDifferentlyFromOneThatRefuses() throws Exception {
        int closedPort;
        try (var briefly = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            closedPort = briefly.getLocalPort();
        }

        try (var client = clientOn(closedPort)) {
            var failed = assertThrows(ExecutionException.class,
                    () -> client.connect().toCompletableFuture().get(AWAIT_SECONDS, TimeUnit.SECONDS));

            assertWithMessage("nothing answered, so nothing can have refused: " + statusOf(failed))
                    .that(statusOf(failed).getCode()).isNotEqualTo(Status.Code.UNIMPLEMENTED);
        }
    }

    private static GrpcParallelConsumerClient clientOn(int port) {
        return GrpcParallelConsumerClient.builder()
                .port(port)
                .options(ClientOptions.builder()
                        .topics(Collections.singletonList("handshake-topic"))
                        .build())
                .build();
    }

    /**
     * The gRPC status behind a completion failure. Asserted to be present rather than tolerated as absent: a
     * non-gRPC failure here would mean the transport never reached the wire, which is a different finding from
     * either outcome these tests distinguish and must not be reported as one of them.
     */
    private static Status statusOf(ExecutionException thrown) {
        Throwable cause = thrown;
        while (cause != null) {
            if (cause instanceof StatusRuntimeException) {
                return ((StatusRuntimeException) cause).getStatus();
            }
            cause = cause.getCause();
        }
        throw new AssertionError("no gRPC status in the failure chain, so the transport never reached the wire",
                thrown);
    }

    private static Future<Integer> runSidecar(ExecutorService pool, ByteArrayOutputStream out,
                                              InputStream lifeline) {
        return pool.submit(() -> NoEngineMain.run(new String[0],
                new PrintStream(out, true, StandardCharsets.UTF_8),
                new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8),
                lifeline));
    }

    /** Polls the captured stdout for the port line rather than sleeping a guessed interval. */
    private static int awaitAnnouncedPort(ByteArrayOutputStream out) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(AWAIT_SECONDS);
        while (System.nanoTime() < deadline) {
            for (String line : out.toString(StandardCharsets.UTF_8).split("\n", -1)) {
                if (line.startsWith(Main.PORT_LINE_PREFIX)) {
                    return Integer.parseInt(line.substring(Main.PORT_LINE_PREFIX.length()).trim());
                }
            }
            Thread.sleep(20);
        }
        throw new AssertionError("no '" + Main.PORT_LINE_PREFIX + "' line appeared on stdout");
    }
}
