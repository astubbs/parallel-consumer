package bz.stub.parallelconsumer.proxy;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.proxy.protocol.v1.ClientMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.Configure;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyServiceGrpc;
import bz.stub.parallelconsumer.proxy.transport.RecordingProxyMessageObserver;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/**
 * Drives an already-built <b>native executable</b> of {@link Main} through the sidecar's whole lifecycle, and
 * fails loudly if any part of it does not hold.
 *
 * <h2>Why this is a program and not a JUnit test</h2>
 *
 * Its subject does not exist until something has run a GraalVM toolchain, and this repository's rule is that a
 * test which quietly does not run is not a passing test. A JUnit test would have to skip itself on every
 * machine without GraalVM, which is nearly all of them, and a skip is indistinguishable from a pass in a
 * surefire summary. So the decision about an absent toolchain is made once, out loud, in
 * {@code bin/native-image-sidecar.sh} - the same place and the same way the eleven foreign client modules make
 * it - and this program is only ever handed a binary that exists.
 *
 * <h2>What it asserts, and why it is the same list as {@link MainTest}</h2>
 *
 * {@link MainTest} asserts the lifecycle by calling {@code Main.run} inside the test JVM. That proves the code
 * is right and proves nothing about the image: closed-world analysis can drop a class the JVM would have
 * loaded, and the failure appears only in the binary. So this drives the identical claims from outside a
 * process it spawned - the port line, a real gRPC round trip, the {@code UNIMPLEMENTED} refusal, a clean exit
 * on parent death, and the socket actually released - which is the point of the exercise rather than
 * duplication of it.
 *
 * <p>The gRPC round trip is the load-bearing arm. A bind proves Netty allocated a socket; only a call that
 * reaches the service and comes back with a status proves the transport, the generated stubs and protobuf all
 * survived into the image.
 *
 * <h2>The environment is cleared on purpose - and what that does NOT prove</h2>
 *
 * The second arm spawns the binary with an empty environment: no {@code JAVA_HOME}, no {@code PATH}, nothing.
 * A sidecar handed to a Go or Python team runs under whatever environment that application happens to have,
 * which may be none of ours, so needing nothing from it is a real property worth holding.
 *
 * <p><b>It is not a test that the binary is a native image, and it was checked rather than assumed.</b> The
 * control arm - the same exercise pointed at a shell wrapper that execs a JVM sidecar by absolute path -
 * <em>passes every arm</em>, because a hard-coded path needs no environment either. What keeps a JVM out of
 * the artifact is {@code --no-fallback} at build time, which makes the build fail rather than emit a
 * fallback image that still needs one; {@code bin/native-image-sidecar.sh} owns that flag and the reasoning
 * for it.
 *
 * @author Antony Stubbs
 * @see MainTest
 */
public final class NativeSidecarLifecycle {

    /** Long enough that only a real hang reaches it; a native sidecar starts in milliseconds. */
    private static final long AWAIT_SECONDS = 30;

    /** The exercise failed: the binary exists, and something it must do it did not do. */
    private static final int EXIT_FAILED = 1;

    /** Called wrong - no executable named, or the path is not one. */
    private static final int EXIT_USAGE = 3;

    private NativeSidecarLifecycle() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("usage: NativeSidecarLifecycle <path to the native sidecar executable>");
            System.exit(EXIT_USAGE);
        }
        Path executable = Path.of(args[0]);
        if (!Files.isExecutable(executable)) {
            System.err.println("native-sidecar-lifecycle: not an executable file: " + executable);
            System.exit(EXIT_USAGE);
        }

        try {
            argumentsAreRefusedBecauseConfigurationTravelsTheProtocol(executable);
            announcesItsPortRefusesASessionAndReleasesTheSocket(executable);
        } catch (AssertionError failed) {
            System.err.println();
            System.err.println("NATIVE SIDECAR LIFECYCLE FAILED: " + failed.getMessage());
            System.exit(EXIT_FAILED);
        }
        System.out.println("native sidecar lifecycle: all arms passed against " + executable);
    }

    /**
     * The same rule the JVM entry point keeps (R39/U7): configuration arrives in {@code Configure} over the
     * protocol, so an argument is a caller misunderstanding and the binary refuses to start. Asserted here
     * because it is the cheapest possible proof that the image contains <em>this</em> {@code Main} rather than
     * something that merely starts.
     */
    private static void argumentsAreRefusedBecauseConfigurationTravelsTheProtocol(Path executable)
            throws Exception {
        var process = new ProcessBuilder(executable.toString(), "--bootstrap-servers", "localhost:9092").start();
        String stdout = readFully(process.getInputStream());
        String stderr = readFully(process.getErrorStream());
        assertTrue(process.waitFor(AWAIT_SECONDS, TimeUnit.SECONDS), "the refusal must exit rather than serve");

        assertEquals(Main.EXIT_USAGE, process.exitValue(), "an argument must be a usage exit");
        assertTrue(stderr.contains("Configure"),
                "the refusal must say where configuration actually goes, but stderr was: " + stderr);
        assertTrue(!stdout.contains(Main.PORT_LINE_PREFIX), "a refused start must not announce a port");
        pass("arguments refused with exit " + Main.EXIT_USAGE + ", naming Configure");
    }

    /**
     * The spawning contract end to end, against a process with no JVM in its environment: the port on stdout
     * line one, a real gRPC session answered {@code UNIMPLEMENTED} and naming the missing engine, a clean exit
     * when the parent's write end closes, and the socket gone afterwards.
     */
    private static void announcesItsPortRefusesASessionAndReleasesTheSocket(Path executable) throws Exception {
        var builder = new ProcessBuilder(executable.toString());
        // Nothing inherited: no JAVA_HOME, no PATH, no TMPDIR. See the class javadoc for what this does and
        // does not establish - it is a "needs nothing from its environment" arm, not a native-image test.
        builder.environment().clear();
        var process = builder.start();

        ManagedChannel channel = null;
        try {
            int port = awaitAnnouncedPort(process);
            assertTrue(port > 0, "an ephemeral port was requested, so it must be a real bound port");
            pass("started with an empty environment and announced port " + port + " on stdout line one");

            try (var probe = new Socket()) {
                probe.connect(loopback(port), 2_000);
                assertTrue(probe.isConnected(), "the announced port must be the one actually serving");
            }

            channel = NettyChannelBuilder.forAddress(InetAddress.getLoopbackAddress().getHostAddress(), port)
                    .usePlaintext()
                    .build();
            assertSessionRefused(channel);
            pass("a session is answered UNIMPLEMENTED, naming the missing engine");

            // The parent dies: closing the write end of the inherited pipe is the primary signal.
            process.getOutputStream().close();

            assertTrue(process.waitFor(AWAIT_SECONDS, TimeUnit.SECONDS),
                    "the sidecar must stop when its parent does, rather than outliving it");
            assertEquals(0, process.exitValue(), "a parent death is an ordinary shutdown, not a failure");

            assertSocketRefused(port);
            pass("exited 0 on parent death and released the socket");
        } finally {
            if (channel != null) {
                channel.shutdownNow();
            }
            process.destroyForcibly();
        }
    }

    /**
     * Opens a session and requires it to terminate with {@code UNIMPLEMENTED} rather than hang or answer.
     * <p>
     * The observer is the transport tests' {@link RecordingProxyMessageObserver} rather than a third
     * hand-rolled copy of the same three fields.
     */
    private static void assertSessionRefused(ManagedChannel channel) throws InterruptedException {
        var recorder = new RecordingProxyMessageObserver();
        StreamObserver<ClientMessage> session = ProxyServiceGrpc.newStub(channel).session(recorder);
        session.onNext(ClientMessage.newBuilder().setConfigure(Configure.newBuilder().addTopics("input")).build());

        assertTrue(recorder.terminated.await(AWAIT_SECONDS, TimeUnit.SECONDS),
                "the session must terminate rather than hang");
        Throwable observed = recorder.error.get();
        assertTrue(observed instanceof StatusRuntimeException,
                "the session must fail with a gRPC status, but was: " + observed);
        Status status = ((StatusRuntimeException) observed).getStatus();
        assertEquals(Status.Code.UNIMPLEMENTED.value(), status.getCode().value(),
                "the refusal must be UNIMPLEMENTED, but was " + status);
        String description = String.valueOf(status.getDescription());
        assertTrue(description.contains("hosts no Parallel Consumer engine"),
                "the refusal must name what is missing, but said: " + description);
        assertTrue(recorder.messages.isEmpty(), "a build with no engine must not answer a Configure with anything");
    }

    /** The socket is the observable that separates "the process returned" from "the server shut down". */
    private static void assertSocketRefused(int port) {
        try (var probe = new Socket()) {
            probe.connect(loopback(port), 2_000);
            throw new AssertionError("the listener must be gone once the sidecar has shut down, not merely "
                    + "unattended - port " + port + " still answers");
        } catch (IOException expected) {
            // The refused connection is the assertion.
        }
    }

    /**
     * Reads the sidecar's stdout until the port line appears, rather than sleeping a guessed interval. Reading
     * the stream is also what proves the line arrives FIRST: a client parses line one, so anything the process
     * printed before it would show up here as a parse failure rather than being skipped over.
     */
    private static int awaitAnnouncedPort(Process process) throws IOException {
        var reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
        String line = reader.readLine();
        if (line == null) {
            throw new AssertionError("the sidecar printed nothing before exiting; exit value "
                    + (process.isAlive() ? "(still running)" : String.valueOf(process.exitValue())));
        }
        if (!line.startsWith(Main.PORT_LINE_PREFIX)) {
            throw new AssertionError("stdout line one must be the port line, but was: " + line);
        }
        return Integer.parseInt(line.substring(Main.PORT_LINE_PREFIX.length()).trim());
    }

    private static InetSocketAddress loopback(int port) {
        return new InetSocketAddress(InetAddress.getLoopbackAddress(), port);
    }

    private static String readFully(java.io.InputStream stream) throws IOException {
        return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    }

    private static void assertTrue(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static void assertEquals(int expected, int actual, String message) {
        if (expected != actual) {
            throw new AssertionError(message + " (expected " + expected + ", got " + actual + ")");
        }
    }

    private static void pass(String what) {
        System.out.println("  ok: " + what);
    }
}
