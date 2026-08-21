package bz.stub.parallelconsumer.proxy.lifecycle;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.proxy.Main;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The production sidecar's spawning contract: bind, announce the port, serve, and die with its parent.
 * <p>
 * The contract is deliberately the same one {@code TestModeMain} already publishes - {@code port: <n>} on
 * stdout line one, then serve until the lifeline ends - because a client that can spawn one must be able to
 * spawn the other without a second code path.
 */
class MainTest {

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
     * connect, and the process returns cleanly the moment its parent's write end closes.
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void announcesItsPortThenExitsCleanlyWhenTheParentDies() throws Exception {
        var out = new ByteArrayOutputStream();
        var err = new ByteArrayOutputStream();
        var writeEnd = new PipedOutputStream();
        var lifeline = new PipedInputStream(writeEnd);

        var pool = Executors.newSingleThreadExecutor();
        try {
            var exit = pool.submit(() -> Main.run(new String[0],
                    new PrintStream(out, true, StandardCharsets.UTF_8),
                    new PrintStream(err, true, StandardCharsets.UTF_8),
                    lifeline));

            var portLine = awaitPortLine(out);
            int port = Integer.parseInt(portLine.substring(Main.PORT_LINE_PREFIX.length()).trim());
            assertWithMessage("an ephemeral port was requested, so it must be a real bound port")
                    .that(port).isGreaterThan(0);

            writeEnd.close(); // the parent dies

            assertThat(exit.get(30, TimeUnit.SECONDS)).isEqualTo(0);
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * Two sidecars on one host must not collide. Each binds its own ephemeral port and announces that port on
     * its own stdout - so a machine running two applications gets two working sidecars, not a bind race.
     */
    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void twoSidecarsBindDifferentPortsAndEachAnnouncesItsOwn() throws Exception {
        var pool = Executors.newFixedThreadPool(2);
        var firstOut = new ByteArrayOutputStream();
        var secondOut = new ByteArrayOutputStream();
        var firstWrite = new PipedOutputStream();
        var secondWrite = new PipedOutputStream();

        try {
            var first = pool.submit(() -> Main.run(new String[0],
                    new PrintStream(firstOut, true, StandardCharsets.UTF_8),
                    new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8),
                    new PipedInputStream(firstWrite)));
            var second = pool.submit(() -> Main.run(new String[0],
                    new PrintStream(secondOut, true, StandardCharsets.UTF_8),
                    new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8),
                    new PipedInputStream(secondWrite)));

            int firstPort = Integer.parseInt(
                    awaitPortLine(firstOut).substring(Main.PORT_LINE_PREFIX.length()).trim());
            int secondPort = Integer.parseInt(
                    awaitPortLine(secondOut).substring(Main.PORT_LINE_PREFIX.length()).trim());

            assertThat(firstPort).isNotEqualTo(secondPort);

            firstWrite.close();
            secondWrite.close();
            assertThat(first.get(30, TimeUnit.SECONDS)).isEqualTo(0);
            assertThat(second.get(30, TimeUnit.SECONDS)).isEqualTo(0);
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * A drain that times out with work still held is a distinguishable failure, not a clean exit: the
     * supervising client needs to know that records were left for redelivery rather than resolved.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void drainTimingOutIsADistinguishableNonZeroExit() {
        assertThat(Main.exitCodeFor(DrainCoordinator.Outcome.TIMED_OUT))
                .isEqualTo(Main.EXIT_DRAIN_TIMED_OUT);
        assertThat(Main.exitCodeFor(DrainCoordinator.Outcome.DRAINED)).isEqualTo(0);
        assertWithMessage("a timed-out drain that exited 0 would be indistinguishable from a clean shutdown")
                .that(Main.EXIT_DRAIN_TIMED_OUT).isNotEqualTo(0);
    }

    /** Polls the captured stdout for the port line rather than sleeping a guessed interval. */
    private static String awaitPortLine(ByteArrayOutputStream out) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            var text = out.toString(StandardCharsets.UTF_8);
            for (var line : text.split("\n")) {
                if (line.startsWith(Main.PORT_LINE_PREFIX)) {
                    return line;
                }
            }
            Thread.sleep(20);
        }
        throw new AssertionError("no '" + Main.PORT_LINE_PREFIX + "' line appeared on stdout");
    }
}
