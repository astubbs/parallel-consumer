package bz.stub.parallelconsumer.proxy.integrationTests;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.proxy.Main;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Duration;

/**
 * The sidecar as a real child process, spawned the way a client library must: <b>directly, with no shell
 * between</b>, so the pipe this JVM holds is the one the sidecar watches for parent death (KTD19).
 * <p>
 * Shared rather than copied because there are two callers with different jobs and one spawning contract -
 * {@code SidecarLifecycleIT} proves the lifecycle, and the demo shows the engine working - and the contract
 * is the thing that must not drift between them. Getting it subtly wrong in one place (a shell wrapper, an
 * undrained pipe) produces a failure that reads as the sidecar misbehaving.
 *
 * @author Antony Stubbs
 */
@Slf4j
public final class SidecarProcess implements AutoCloseable {

    private static final Duration STARTUP_BUDGET = Duration.ofSeconds(60);

    private final Process process;

    private final int port;

    private SidecarProcess(Process process, int port) {
        this.process = process;
        this.port = port;
    }

    /** Spawns a sidecar and returns once it has announced its port. */
    public static SidecarProcess spawn() throws IOException {
        var java = Paths.get(System.getProperty("java.home"), "bin", "java").toString();
        var process = new ProcessBuilder(java, "-cp", System.getProperty("java.class.path"),
                Main.class.getName())
                .redirectErrorStream(false)
                .start();

        pump(process, new BufferedReader(
                new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8)), "stderr");

        var stdout = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
        long deadline = System.nanoTime() + STARTUP_BUDGET.toNanos();
        while (System.nanoTime() < deadline) {
            var line = stdout.readLine();
            if (line == null) {
                throw new IllegalStateException("the sidecar exited before announcing a port");
            }
            if (line.startsWith(Main.PORT_LINE_PREFIX)) {
                int port = Integer.parseInt(line.substring(Main.PORT_LINE_PREFIX.length()).trim());
                log.info("Sidecar pid {} announced port {}", process.pid(), port);
                // Keep draining what follows. Abandoning the reader here fills the pipe and the child BLOCKS
                // on its next write - a sidecar that merely logs enough would hang its caller, and it would
                // look like the lifecycle failing rather than the caller starving it.
                pump(process, stdout, "stdout");
                return new SidecarProcess(process, port);
            }
        }
        throw new IllegalStateException("no port line within " + STARTUP_BUDGET);
    }

    public int port() {
        return port;
    }

    public Process process() {
        return process;
    }

    /**
     * The parent dying, for real: closing our end of the sidecar's stdin is exactly what the kernel does to
     * the last write end when a process dies.
     */
    public void killTheParentSide() throws IOException {
        process.getOutputStream().close();
    }

    @Override
    public void close() {
        process.destroyForcibly();
    }

    /**
     * Forwards the child's output into this JVM's log. Two reasons, and the second is not optional: a failure
     * would otherwise report only that the sidecar exited while the reason - a bind failure, a refused
     * Configure, a stack trace - died inside a pipe nobody read; and an undrained pipe eventually fills, at
     * which point the child blocks on write. Daemon threads, so a stuck child cannot hold the JVM open.
     */
    private static void pump(Process process, BufferedReader reader, String which) {
        var thread = new Thread(() -> {
            try (reader) {
                String line;
                while ((line = reader.readLine()) != null) {
                    log.info("[sidecar pid {} {}] {}", process.pid(), which, line);
                }
            } catch (IOException closed) {
                // the child went away; nothing left to forward
            }
        }, "sidecar-" + which + "-" + process.pid());
        thread.setDaemon(true);
        thread.start();
    }
}
