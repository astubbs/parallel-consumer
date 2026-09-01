package bz.stub.parallelconsumer.proxy;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.proxy.config.ConfigureHandler;
import bz.stub.parallelconsumer.proxy.engine.ProxyProcessor;
import bz.stub.parallelconsumer.proxy.lifecycle.DrainCoordinator;
import bz.stub.parallelconsumer.proxy.lifecycle.ParentDeathWatchdog;
import bz.stub.parallelconsumer.proxy.transport.ProxyServer;
import io.grpc.BindableService;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.time.Duration;
import java.util.function.Supplier;

/**
 * The sidecar (KTD4, R47, R52): the application's child process, which serves one client over gRPC and dies
 * when that client does.
 *
 * <h2>The spawning contract</h2>
 *
 * Bind an ephemeral loopback port, print {@code port: <n>} as the first line of stdout, then serve until the
 * parent dies. Everything a spawning client needs to find this process is on that one line, so the client
 * side of the contract is a line read rather than a discovery protocol. That is deliberately the same
 * contract {@code TestModeMain} already publishes, so a client can spawn either binary through one code path.
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
 * <h2>What this build serves</h2>
 *
 * It hosts a real engine: {@link #sessionServiceFactory} returns {@link ConfigureHandler}, which builds a
 * {@link ProxyProcessor} when a client's {@code Configure} arrives, and the shutdown drain below waits for
 * records the client is still holding in its own process. That is unit U10, and it is why this class carries
 * an exit code for a drain that timed out.
 * <p>
 * A build that hosts <em>no</em> engine is still spawnable, and that is what the eight cross-language
 * {@code SidecarHandshakeTest}s point at: {@code NoEngineMain} in this module's test tree passes the
 * no-engine session service through the same seam. It moved there when this entry point gained its engine -
 * the alternative was those tests silently losing their subject.
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

    /** Could not bind the listener. Distinct from a usage error: the invocation was fine, the socket was not. */
    public static final int EXIT_BIND_FAILED = 4;

    /** The port line, as the spawning client parses it. Identical to the test-mode binary's. */
    public static final String PORT_LINE_PREFIX = "port: ";

    /**
     * The stdout announcement when the listener is a Unix domain socket - the exact counterpart of
     * {@link #PORT_LINE_PREFIX}, because the parent learns where to connect the same way either way.
     */
    public static final String SOCKET_LINE_PREFIX = "socket: ";

    /**
     * The one argument this binary accepts, and it selects a TRANSPORT rather than configuring a session.
     * That distinction is what keeps R39 intact: bootstrap servers, credentials, ordering, concurrency and
     * subscription still arrive connect-time in Configure and there is still no flag for any of them. Where
     * to listen is not one of those - it is what the spawning parent must know before a session exists, the
     * same category as the port.
     */
    public static final String SOCKET_FLAG = "--socket";

    /** Brisk enough that an orphan is short-lived, slow enough not to spin on a healthy parent. */
    private static final Duration PARENT_POLL_INTERVAL = Duration.ofMillis(250);

    /** How often the drain asks the client what it still holds. */
    private static final Duration DRAIN_POLL_INTERVAL = Duration.ofMillis(50);

    /** Used only until a client configures the engine; after that the configured drain timeout governs. */
    private static final Duration DEFAULT_DRAIN_TIMEOUT = Duration.ofSeconds(30);

    /**
     * What the production entry point always asks for: let the OS choose, so no well-known port is guessable
     * and two sidecars on one host cannot race for the same number. Only a test names a port, and only so
     * that a bind failure can be provoked.
     */
    private static final int EPHEMERAL_PORT = 0;

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
        return run(args, out, err, parentLifeline, sessionServiceFactory(), EPHEMERAL_PORT);
    }

    /**
     * The two seams this class has, both package-private because neither is a knob the sidecar exposes.
     * <p>
     * {@code sessionService} is what selects what the transport hosts: the transport's contract with it is
     * {@link BindableService} and nothing more - which is the whole reason the transport can be reviewed
     * without an engine - and it is a factory rather than an instance because each run owns its own service
     * for the life of one server. It is also how a no-engine sidecar is still spawnable, for the handshake
     * tests that assert the refusal. {@code port} exists only so a bind failure can be provoked
     * deliberately; production always passes {@link #EPHEMERAL_PORT}, and an untestable exit code is one
     * nobody can rely on.
     */
    static int run(String[] args, PrintStream out, PrintStream err, InputStream parentLifeline,
                   Supplier<BindableService> sessionService, int port) {
        boolean domainSocket = false;
        if (args.length == 1 && SOCKET_FLAG.equals(args[0])) {
            domainSocket = true;
        } else if (args.length > 0) {
            return usage(err, "the only argument this binary accepts is " + SOCKET_FLAG
                    + " (got '" + args[0] + "')");
        }

        var service = sessionService.get();

        try (var server = ProxyServer.builder()
                .sessionService(service)
                .domainSocket(domainSocket)
                .port(port)
                .build()
                .start()) {
            if (domainSocket) {
                out.println(SOCKET_LINE_PREFIX + server.socketPath());
                log.info("Sidecar listening on Unix domain socket {}; waiting for the client to configure it",
                        server.socketPath());
            } else {
                out.println(PORT_LINE_PREFIX + server.port());
                log.info("Sidecar listening on loopback port {}; waiting for the client to configure it",
                        server.port());
            }

            try (var watchdog = ParentDeathWatchdog.watchingParentOf(parentLifeline, PARENT_POLL_INTERVAL)) {
                watchdog.start();
                watchdog.awaitDeath();
                log.info("Shutting down: {}", watchdog.cause());
            }

            return exitCodeFor(drain(service));
        } catch (IOException bindFailed) {
            err.println("sidecar: could not bind the listener: " + bindFailed.getMessage());
            return EXIT_BIND_FAILED;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while serving; shutting down");
            return exitCodeFor(drain(service));
        }
    }

    /**
     * What the production sidecar hosts. Named rather than inlined so the choice is one greppable call site -
     * which is what let the no-engine build become a test entry point rather than a second copy of this
     * class.
     */
    static Supplier<BindableService> sessionServiceFactory() {
        return () -> ConfigureHandler.builder().build();
    }

    /**
     * Runs the drain against whatever the session actually reached. <b>An engine that was never configured
     * is a clean exit, not a timeout</b> - a sidecar whose client died during the handshake holds no records,
     * so there is nothing to wait for and nothing to leave uncommitted. A service that is not engine-backed
     * at all - the no-engine build the handshake tests spawn - is the same case for the same reason.
     */
    private static DrainCoordinator.Outcome drain(BindableService service) {
        if (!(service instanceof ConfigureHandler)) {
            log.info("This build hosts no engine; nothing to drain");
            return DrainCoordinator.Outcome.DRAINED;
        }
        var handler = (ConfigureHandler) service;
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
        err.println("It takes no CONFIGURATION. Bootstrap servers, credentials, ordering, concurrency and");
        err.println("subscription all arrive connect-time in the Configure message over the protocol, so");
        err.println("there is no flag or environment variable for any of them.");
        err.println();
        err.println("The one accepted argument, " + SOCKET_FLAG + ", selects the TRANSPORT: listen on a Unix");
        err.println("domain socket instead of a loopback TCP port. That needs an epoll transport, so it is");
        err.println("Linux-only - which includes inside a container on any host.");
        err.println();
        err.println("Prints '" + PORT_LINE_PREFIX + "<n>' - or '" + SOCKET_LINE_PREFIX + "<path>' under "
                + SOCKET_FLAG + " - on stdout");
        err.println("line one, then serves one client there until the parent process dies (EOF on stdin).");
        err.println();
        err.println("Launch it DIRECTLY, not through a shell: a wrapper process inherits the pipe's write");
        err.println("end and holds it open, which defeats the primary parent-death signal.");
        return EXIT_USAGE;
    }
}
