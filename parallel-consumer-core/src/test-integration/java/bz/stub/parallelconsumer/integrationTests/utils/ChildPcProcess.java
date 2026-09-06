package bz.stub.parallelconsumer.integrationTests.utils;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * Launches, observes and kills one child JVM running {@link ChildPcMain} - the multi-process harness's launcher
 * (KTD7, R14), mirroring the proxy-conformance {@code ConformanceDriver.spawn}. Lanes and demos use this and
 * {@link FiringLedger}; nothing outside this package touches a {@link Process}.
 * <p>
 * <b>Classpath.</b> The child runs on the parent's {@code java.home} and the parent's {@code java.class.path},
 * verbatim. Under failsafe that property is a single manifest-only booter jar whose {@code Class-Path} names
 * every real entry; the JDK launcher resolves manifest {@code Class-Path} entries for {@code -cp} exactly as for
 * {@code -jar}, so the child sees the same classes the test does. {@link #describeClasspath()} says which shape
 * the parent has, and the harness self-test proves the child loads PC off it.
 * <p>
 * <b>Pumps before waits.</b> Stdout and stderr are drained on daemon threads started before anything blocks: an
 * unread pipe fills at 64KB and stalls the child's next write - which, once logging writes from the control
 * loop, stalls the control loop and reads as a mechanism bug. Lines are retained, so a self-test can prove none
 * were lost.
 * <p>
 * <b>Liveness.</b> {@link #awaitStarted} completes on the child's {@value ChildPcMain#READY_LINE} or fails
 * fast the moment the process exits first, with the exit code and captured stderr ({@link EarlyExitException}) -
 * so a child that dies before joining the group never surfaces as a group-stability timeout. A start that
 * neither readies nor exits within its budget is {@link StartTimeoutException}, still distinct from the group
 * wait.
 * <p>
 * <b>Never skip.</b> A JVM that cannot be launched fails the test naming the command; absence must never read as
 * agreement ({@code LanguageRunner.ensureAvailable}'s posture).
 *
 * @author Antony Stubbs
 * @see ChildPcMain
 * @see ChildPcOptions
 */
@Slf4j
public final class ChildPcProcess implements AutoCloseable {

    /** How long a forced kill may take to be reaped before the harness itself is declared broken. */
    public static final Duration REAP_SLACK = Duration.ofSeconds(30);

    /** Heap for a child: one PC instance and two clients need far less, and N of them share a CI runner. */
    private static final String CHILD_MAX_HEAP = "-Xmx512m";

    @Getter
    private final ChildPcOptions options;
    private final Process process;
    private final String commandLine;
    private final Instant launchedAt;
    private final LinePump stdout;
    private final LinePump stderr;
    private final CompletableFuture<Void> ready = new CompletableFuture<>();

    private ChildPcProcess(ChildPcOptions options, Process process, String commandLine) {
        this.options = options;
        this.process = process;
        this.commandLine = commandLine;
        this.launchedAt = Instant.now();
        this.stdout = new LinePump(process.getInputStream(), "stdout", options.getInstanceId(), line -> {
            if (ChildPcMain.READY_LINE.equals(line)) {
                ready.complete(null);
            }
        });
        this.stderr = new LinePump(process.getErrorStream(), "stderr", options.getInstanceId(), line -> { });
        stdout.start();
        stderr.start();
    }

    /** Launches the child; the pumps are running before this returns. Fails loudly if the JVM will not start. */
    public static ChildPcProcess launch(ChildPcOptions options) {
        List<String> command = commandFor(options);
        String commandLine = String.join(" ", command);
        log.info("Launching child PC '{}' in group '{}': {}", options.getInstanceId(), options.getGroupId(),
                commandLine);
        try {
            Process process = new ProcessBuilder(command).start();
            return new ChildPcProcess(options, process, commandLine);
        } catch (IOException e) {
            throw new IllegalStateException("cannot launch the child PC JVM: " + commandLine
                    + " - a child that will not start FAILS the lane; absence never reads as agreement", e);
        }
    }

    private static List<String> commandFor(ChildPcOptions options) {
        List<String> command = new ArrayList<>();
        command.add(Paths.get(System.getProperty("java.home"), "bin", "java").toString());
        command.add(CHILD_MAX_HEAP);
        String logLevel = System.getProperty("pc.log.level");
        if (logLevel != null) {
            command.add("-Dpc.log.level=" + logLevel);
        }
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(ChildPcMain.class.getName());
        command.addAll(options.toArgs());
        return command;
    }

    /**
     * What the parent's {@code java.class.path} looks like, for the self-test's log: the entry count, and for a
     * single-jar classpath whether that jar's manifest carries a {@code Class-Path} (the failsafe booter shape).
     */
    public static String describeClasspath() {
        String classpath = System.getProperty("java.class.path");
        String[] entries = classpath.split(File.pathSeparator, -1);
        if (entries.length != 1) {
            return entries.length + " classpath entries (explicit classpath)";
        }
        Path only = Paths.get(entries[0]);
        if (!Files.isRegularFile(only) || !only.toString().endsWith(".jar")) {
            return "one classpath entry, not a jar: " + only;
        }
        try (JarFile jar = new JarFile(only.toFile())) {
            Manifest manifest = jar.getManifest();
            String manifestClasspath = manifest == null ? null
                    : manifest.getMainAttributes().getValue("Class-Path");
            if (manifestClasspath == null) {
                return "one jar with no manifest Class-Path: " + only;
            }
            return "one manifest-only jar (" + only.getFileName() + ") whose Class-Path names "
                    + manifestClasspath.trim().split("\\s+").length + " entries";
        } catch (IOException e) {
            return "one jar whose manifest could not be read (" + e + "): " + only;
        }
    }

    // ------------------------------------------------------------------
    // Liveness
    // ------------------------------------------------------------------

    /**
     * Waits for the child's READY line. Fails fast with {@link EarlyExitException} if the process exits first,
     * and with {@link StartTimeoutException} if it does neither within {@code budget}.
     *
     * @return how long the start took, for the lane's own observations
     */
    public Duration awaitStarted(Duration budget) {
        Instant deadline = Instant.now().plus(budget);
        while (Instant.now().isBefore(deadline)) {
            if (ready.isDone()) {
                return Duration.between(launchedAt, Instant.now());
            }
            if (!process.isAlive()) {
                drainPumps();
                if (ready.isDone()) {
                    return Duration.between(launchedAt, Instant.now());
                }
                throw new EarlyExitException("child PC '" + options.getInstanceId() + "' exited with code "
                        + process.exitValue() + " before it was ready - " + Duration.between(launchedAt, Instant.now())
                        + " after launch, inside the " + budget + " start budget. This is the CHILD failing, not "
                        + "the group taking long to stabilise." + diagnostics());
            }
            sleepQuietly(50);
        }
        throw new StartTimeoutException("child PC '" + options.getInstanceId() + "' neither printed "
                + ChildPcMain.READY_LINE + " nor exited within " + budget + diagnostics());
    }

    public boolean isAlive() {
        return process.isAlive();
    }

    /** The exit code if the child has exited, else empty. */
    public OptionalInt exitCode() {
        return process.isAlive() ? OptionalInt.empty() : OptionalInt.of(process.exitValue());
    }

    // ------------------------------------------------------------------
    // Stopping
    // ------------------------------------------------------------------

    /**
     * Asks the child to stop through stdin (the graceful route: the processor closes, the ledger record is
     * emitted, exit 0) and waits for it. Falls back to a forced kill after the budget, and says so.
     *
     * @return the exit code
     */
    public int stopGracefully(Duration budget) {
        if (!process.isAlive()) {
            return process.exitValue();
        }
        try {
            OutputStream stdin = process.getOutputStream();
            stdin.write((ChildPcMain.STOP_COMMAND + "\n").getBytes(StandardCharsets.UTF_8));
            stdin.flush();
        } catch (IOException e) {
            log.warn("could not write the stop command to child '{}' - killing it instead: {}",
                    options.getInstanceId(), e.toString());
            kill();
            return process.exitValue();
        }
        boolean exited = waitQuietly(budget);
        if (!exited) {
            kill();
            throw new IllegalStateException("child PC '" + options.getInstanceId() + "' did not stop within "
                    + budget + " of being asked; it was killed" + diagnostics());
        }
        drainPumps();
        return process.exitValue();
    }

    /**
     * SIGKILL ({@code destroyForcibly}) - the lane's "process died" event (KTD10). Nothing is flushed, no
     * ledger record is emitted, and the group learns of the death only through the session timeout.
     *
     * @return how long the kill took to be reaped, which must be inside {@link #REAP_SLACK}
     */
    public Duration kill() {
        Instant started = Instant.now();
        process.destroyForcibly();
        boolean reaped = waitQuietly(REAP_SLACK);
        Duration took = Duration.between(started, Instant.now());
        if (!reaped) {
            throw new IllegalStateException("child PC '" + options.getInstanceId() + "' survived destroyForcibly "
                    + "for " + REAP_SLACK + " - the harness cannot kill its own child" + diagnostics());
        }
        drainPumps();
        return took;
    }

    /**
     * Closes the child's stdin WITHOUT a stop line - what a dead parent looks like from inside the child. The
     * child is expected to notice the EOF and stop gracefully; the harness self-test asserts that it does.
     */
    public void closeStdin() {
        try {
            process.getOutputStream().close();
        } catch (IOException e) {
            throw new UncheckedIOException("could not close the stdin of child '" + options.getInstanceId() + "'", e);
        }
    }

    /** Waits for the child to exit on its own, up to {@code budget}; the exit code, or empty if it is still alive. */
    public OptionalInt awaitExit(Duration budget) {
        boolean exited = waitQuietly(budget);
        if (!exited) {
            return OptionalInt.empty();
        }
        drainPumps();
        return OptionalInt.of(process.exitValue());
    }

    /** Kills the child if it is still alive - teardown, so a failed test never leaks a JVM. */
    @Override
    public void close() {
        if (process.isAlive()) {
            log.warn("child PC '{}' still alive at close - killing it", options.getInstanceId());
            process.destroyForcibly();
            waitQuietly(REAP_SLACK);
        }
        drainPumps();
    }

    // ------------------------------------------------------------------
    // Captured output
    // ------------------------------------------------------------------

    /** Every stdout line captured so far, in order. */
    public List<String> stdoutLines() {
        return stdout.lines();
    }

    /** Every stderr line captured so far, joined. */
    public String stderrText() {
        return String.join("\n", stderr.lines());
    }

    /** The launched-at instant on the parent's clock. */
    public Instant launchedAt() {
        return launchedAt;
    }

    /** Everything a failure message needs: the command, the exit state, the tail of both streams. */
    public String diagnostics() {
        StringBuilder text = new StringBuilder();
        text.append("\n  command: ").append(commandLine);
        text.append("\n  alive: ").append(process.isAlive());
        exitCode().ifPresent(code -> text.append(", exit code ").append(code));
        List<String> stdoutLines = stdout.lines();
        List<String> stderrLines = stderr.lines();
        text.append("\n  stdout (last ").append(DIAGNOSTIC_TAIL).append(" of ").append(stdoutLines.size())
                .append(" lines):\n    ").append(String.join("\n    ", tail(stdoutLines)));
        text.append("\n  stderr (last ").append(DIAGNOSTIC_TAIL).append(" of ").append(stderrLines.size())
                .append(" lines):\n    ").append(String.join("\n    ", tail(stderrLines)));
        return text.toString();
    }

    private static final int DIAGNOSTIC_TAIL = 30;

    private static List<String> tail(List<String> lines) {
        return lines.size() <= DIAGNOSTIC_TAIL ? lines : lines.subList(lines.size() - DIAGNOSTIC_TAIL, lines.size());
    }

    private void drainPumps() {
        stdout.drain();
        stderr.drain();
    }

    private boolean waitQuietly(Duration budget) {
        try {
            return process.waitFor(budget.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
            throw new IllegalStateException("interrupted waiting for child PC '" + options.getInstanceId() + "'", e);
        }
    }

    private static void sleepQuietly(long millis) {
        try {
            TimeUnit.MILLISECONDS.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted waiting for a child PC to start", e);
        }
    }

    /** A child that exited before it was ready: the child's own failure, with its stderr. */
    public static final class EarlyExitException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        EarlyExitException(String message) {
            super(message);
        }
    }

    /** A child that neither readied nor exited inside its start budget. */
    public static final class StartTimeoutException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        StartTimeoutException(String message) {
            super(message);
        }
    }

    /** Drains one stream line by line on a daemon thread, retaining every line, so the pipe can never fill. */
    private static final class LinePump extends Thread {
        private final InputStream source;
        private final ConcurrentLinkedQueue<String> captured = new ConcurrentLinkedQueue<>();
        private final Consumer<String> onLine;

        LinePump(InputStream source, String streamName, String instanceId, Consumer<String> onLine) {
            super("child-pc-" + streamName + "-pump-" + instanceId);
            this.source = source;
            this.onLine = onLine;
            setDaemon(true);
        }

        @Override
        public void run() {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(source, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    captured.add(line);
                    onLine.accept(line);
                }
            } catch (IOException e) {
                captured.add("<stream ended: " + e + ">");
            }
        }

        List<String> lines() {
            return new ArrayList<>(captured);
        }

        /** Bounded, so a child that leaked its stream to a grandchild cannot wedge the suite. */
        void drain() {
            try {
                join(TimeUnit.SECONDS.toMillis(10));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new UncheckedIOException(new IOException("interrupted draining a child PC's output", e));
            }
        }
    }
}
