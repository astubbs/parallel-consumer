package bz.stub.parallelconsumer.proxy;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.proxy.config.ConfigureHandler;
import bz.stub.parallelconsumer.proxy.engine.ProxyProcessor;
import bz.stub.parallelconsumer.proxy.lifecycle.DrainCoordinator;
import bz.stub.parallelconsumer.proxy.lifecycle.ParentDeathWatchdog;
import bz.stub.parallelconsumer.proxy.transport.ProxyServer;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.time.Duration;

/**
 * The sidecar (KTD4, R47, R52): the application's child process, which serves one client over gRPC and dies
 * when that client does.
 *
 * <h2>The spawning contract</h2>
 *
 * Bind an ephemeral loopback port, print {@code port: <n>} as the first line of stdout, then serve until the
 * parent dies. That is deliberately the same contract {@code TestModeMain} already publishes, so a client can
 * spawn either binary through one code path.
 *
 * <h2>It parses no configuration, and that is a rule rather than an omission</h2>
 *
 * Everything - bootstrap servers, credentials, ordering, concurrency, subscription - arrives connect-time in
 * {@code Configure} over the protocol (R39, U7). Nothing is read from a file, an environment variable, or the
 * command line beyond what the lifecycle itself needs, which is currently nothing. An argument is therefore a
 * caller misunderstanding, and this refuses to start rather than ignoring it: a sidecar that silently
 * discarded {@code --bootstrap-servers} would be configured over the protocol anyway and the operator would
 * be left debugging why their flag did nothing.
 *
 * <h2>Launch it directly, never through a shell</h2>
 *
 * Parent death is detected by EOF on the inherited pipe (KTD19). A wrapper process - a shell, a launcher -
 * inherits the write end and holds it open, which defeats that signal.
 * {@link ParentDeathWatchdog}'s pid poll is the backstop, but the backstop has an interval and the pipe does
 * not.
 *
 * @author Antony Stubbs
 * @see ParentDeathWatchdog
 * @see DrainCoordinator
 */
@Slf4j
public final class Main {

    /** Argument or usage error: the caller asked for something this binary does not do. */
    public static final int EXIT_USAGE = 2;

    /**
     * The drain timed out with records still held by the client. Distinct from a clean exit because it is a
     * different outcome for the data: those records were left uncommitted for redelivery, not resolved.
     */
    public static final int EXIT_DRAIN_TIMED_OUT = 3;

    /** The port line, as the spawning client parses it. Identical to the test-mode binary's. */
    public static final String PORT_LINE_PREFIX = "port: ";

    /** Brisk enough that an orphan is short-lived, slow enough not to spin on a healthy parent. */
    private static final Duration PARENT_POLL_INTERVAL = Duration.ofMillis(250);

    /** How often the drain asks the client what it still holds. */
    private static final Duration DRAIN_POLL_INTERVAL = Duration.ofMillis(50);

    /** Used only until a client configures the engine; after that the configured drain timeout governs. */
    private static final Duration DEFAULT_DRAIN_TIMEOUT = Duration.ofSeconds(30);

    private Main() {
    }

    public static void main(String[] args) {
        System.exit(run(args, System.out, System.err, System.in));
    }

    /**
     * The testable core of {@link #main}: returns the exit code rather than calling {@code System.exit}, and
     * takes the parent lifeline as a parameter so a test can end it without killing a process.
     *
     * @param parentLifeline the inherited pipe; {@code System.in} in the real sidecar
     */
    public static int run(String[] args, PrintStream out, PrintStream err, InputStream parentLifeline) {
        if (args.length > 0) {
            return usage(err, "this binary takes no arguments (got '" + args[0] + "')");
        }

        var handler = ConfigureHandler.builder().build();

        try (var server = ProxyServer.builder().sessionService(handler).build().start()) {
            out.println(PORT_LINE_PREFIX + server.port());
            log.info("Sidecar listening on loopback port {}; waiting for the client to configure it",
                    server.port());

            try (var watchdog = ParentDeathWatchdog.watchingParentOf(parentLifeline, PARENT_POLL_INTERVAL)) {
                watchdog.start();
                watchdog.awaitDeath();
                log.info("Shutting down: {}", watchdog.cause());
            }

            return exitCodeFor(drain(handler));
        } catch (IOException bindFailed) {
            err.println("sidecar: could not bind the loopback listener: " + bindFailed.getMessage());
            return EXIT_USAGE;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while serving; shutting down");
            return exitCodeFor(drain(handler));
        }
    }

    /**
     * Runs the drain against whatever the session actually reached. <b>An engine that was never configured
     * is a clean exit, not a timeout</b> - a sidecar whose client died during the handshake holds no records,
     * so there is nothing to wait for and nothing to leave uncommitted.
     */
    private static DrainCoordinator.Outcome drain(ConfigureHandler handler) {
        var engine = handler.engine();
        if (!engine.isPresent()) {
            log.info("No engine was ever configured; nothing to drain");
            return DrainCoordinator.Outcome.DRAINED;
        }
        var target = new SessionDrainTarget(handler, engine.get());
        return DrainCoordinator.of(target, drainTimeoutOf(engine.get()), DRAIN_POLL_INTERVAL).drain();
    }

    private static Duration drainTimeoutOf(ProxyProcessor engine) {
        var configured = engine.drainTimeout();
        return configured == null ? DEFAULT_DRAIN_TIMEOUT : configured;
    }

    /** Public so the exit-code contract is assertable rather than only observable by killing a process. */
    public static int exitCodeFor(DrainCoordinator.Outcome outcome) {
        return outcome == DrainCoordinator.Outcome.TIMED_OUT ? EXIT_DRAIN_TIMED_OUT : 0;
    }

    /** Binds the drain's four steps to the live session and engine. */
    private static final class SessionDrainTarget implements DrainCoordinator.DrainTarget {

        private final ConfigureHandler handler;

        private final ProxyProcessor engine;

        private SessionDrainTarget(ConfigureHandler handler, ProxyProcessor engine) {
            this.handler = handler;
            this.engine = engine;
        }

        @Override
        public void stopAcceptingNewWork() {
            handler.stopAcceptingNewWork();
        }

        @Override
        public void tellClientToShutDown() {
            if (!handler.tellClientToShutDown()) {
                log.info("No client stream to notify; draining whatever it left behind");
            }
        }

        @Override
        public int foreignRecordsInFlight() {
            return engine.foreignRecordsInFlight();
        }

        @Override
        public void closeEngineDrainingFirst() {
            engine.closeDrainFirst();
        }
    }

    private static int usage(PrintStream err, String problem) {
        err.println("sidecar: " + problem);
        err.println();
        err.println("usage: Main");
        err.println();
        err.println("It takes no arguments. Bootstrap servers, credentials, ordering, concurrency and");
        err.println("subscription all arrive connect-time in the Configure message over the protocol, so");
        err.println("there is no flag or environment variable to set here.");
        err.println();
        err.println("Prints '" + PORT_LINE_PREFIX + "<n>' on stdout line one, then serves one client on that");
        err.println("loopback port until the parent process dies (observed as EOF on stdin).");
        err.println();
        err.println("Launch it DIRECTLY, not through a shell: a wrapper process inherits the pipe's write");
        err.println("end and holds it open, which defeats the primary parent-death signal.");
        return EXIT_USAGE;
    }
}
