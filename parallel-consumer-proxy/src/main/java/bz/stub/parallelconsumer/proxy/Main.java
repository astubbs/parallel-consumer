package bz.stub.parallelconsumer.proxy;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

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
 * side of the contract is a line read rather than a discovery protocol.
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
 * <h2>What this build serves, and what it does not</h2>
 *
 * This is the packaging and runtime boundary only. The session service it hosts is
 * {@link NoEngineSessionService}, which refuses every session with {@code UNIMPLEMENTED} because there is no
 * engine in this module yet - so what a reviewer can hold this to is exactly the lifecycle: it binds, it
 * announces, it admits or rejects a connection under the transport's own rules, and it stops cleanly. The
 * engine, its connect-time configuration, and the shutdown drain that waits on records held in a foreign
 * process all arrive with later rungs; {@link #sessionServiceFactory} is the single seam they replace.
 *
 * @author Antony Stubbs
 * @see ParentDeathWatchdog
 * @see NoEngineSessionService
 */
@Slf4j
public final class Main {

    /** Argument or usage error: the caller asked for something this binary does not do. */
    public static final int EXIT_USAGE = 2;

    /** Could not bind the listener. Distinct from a usage error: the invocation was fine, the socket was not. */
    public static final int EXIT_BIND_FAILED = 4;

    /** The port line, as the spawning client parses it. */
    public static final String PORT_LINE_PREFIX = "port: ";

    /** Brisk enough that an orphan is short-lived, slow enough not to spin on a healthy parent. */
    private static final Duration PARENT_POLL_INTERVAL = Duration.ofMillis(250);

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
     * {@code sessionService} is what the engine rung fills: the transport's contract with the thing it hosts
     * is {@link BindableService} and nothing more - the whole reason the transport can be reviewed without an
     * engine - and it is a factory rather than an instance because each run owns its own service for the life
     * of one server. {@code port} exists only so a bind failure can be provoked deliberately; production
     * always passes {@link #EPHEMERAL_PORT}, and an untestable exit code is one nobody can rely on.
     */
    static int run(String[] args, PrintStream out, PrintStream err, InputStream parentLifeline,
                   Supplier<BindableService> sessionService, int port) {
        if (args.length > 0) {
            return usage(err, "this binary takes no arguments (got '" + args[0] + "')");
        }

        try (var server = ProxyServer.builder().sessionService(sessionService.get()).port(port).build().start()) {
            out.println(PORT_LINE_PREFIX + server.port());
            log.info("Sidecar listening on loopback port {}; waiting for the client to configure it",
                    server.port());

            try (var watchdog = ParentDeathWatchdog.watchingParentOf(parentLifeline, PARENT_POLL_INTERVAL)) {
                watchdog.start();
                watchdog.awaitDeath();
                log.info("Shutting down: {}", watchdog.cause());
            }
            return 0;
        } catch (IOException bindFailed) {
            err.println("sidecar: could not bind the loopback listener: " + bindFailed.getMessage());
            return EXIT_BIND_FAILED;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while serving; shutting down");
            return 0;
        }
    }

    /**
     * What a sidecar with no engine hosts. Named rather than inlined so the thing a later rung replaces is
     * one greppable call site.
     */
    static Supplier<BindableService> sessionServiceFactory() {
        return NoEngineSessionService::new;
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
