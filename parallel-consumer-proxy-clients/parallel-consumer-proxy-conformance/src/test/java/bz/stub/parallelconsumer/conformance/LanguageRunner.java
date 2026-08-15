package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.proxy.harness.ProxyHarness;
import com.github.bsideup.jabel.Desugar;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * One registered language: where its runner is built, how to build it, and where the binary lands.
 * <p>
 * <b>Absence is a failure, never a skip.</b> A language whose toolchain is missing, whose build fails, or
 * whose runner binary is not where it said it would be, fails its scenarios with a message naming the
 * problem and the command that fixes it. Absence looks exactly like agreement - a suite that skipped an
 * unbuildable client would report a clean run for a client nobody had tested, and that is the failure most
 * likely to survive all the way to a release with ten libraries in it.
 * <p>
 * The one sanctioned way to run fewer languages is the explicit, visible
 * {@code -Dpc.conformance.language=<comma list>} - an act, recorded on the command line, rather than a
 * condition of the machine.
 *
 * @author Antony Stubbs
 * @see LanguageRunners
 */
@Slf4j
@Desugar // Jabel requires the annotation on every record, even in this module where release=17 makes it a no-op
public record LanguageRunner(String language, Path workingDirectory, List<String> buildCommand, Path executable)
        implements ConformanceBinding {

    /** A foreign runner is named by its language, in the matrix and on the selector's command line. */
    @Override
    public String name() {
        return language;
    }

    @Override
    public void ensureAvailable() {
        ensureBuilt();
    }

    /**
     * Spawns this language's runner at an engine the harness is already hosting, and waits for its verdict.
     * <p>
     * The run needs no observation window of its own: by the time the process has exited, a
     * {@code report-nothing} runner has already held its session open for the contract's fixed hold, which
     * is what the window is for.
     */
    @Override
    public Run execute(ProxyHarness harness, ConformanceScenario scenario) {
        return ConformanceDriver.spawnAgainst(this, harness, scenario);
    }

    /** How long a runner's build may take before the suite calls it stuck. Cold Go and Rust builds are slow. */
    private static final long BUILD_TIMEOUT_MINUTES = 10;

    /**
     * Built at most once per JVM per language, however many scenarios run concurrently. Memoised outside
     * the record because a record is a value and this is a fact about the machine.
     */
    private static final Map<String, Object> BUILT = new ConcurrentHashMap<>();

    /** Signals a runner that could not be built or found - kept distinct so the negative controls can name it. */
    public static class RunnerUnavailableException extends RuntimeException {
        public RunnerUnavailableException(String message) {
            super(message);
        }

        public RunnerUnavailableException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Builds the runner if it has not been built in this JVM, then checks the binary is really there.
     *
     * @throws RunnerUnavailableException naming the toolchain, the command and the expected path
     */
    public void ensureBuilt() {
        BUILT.computeIfAbsent(language, key -> {
            build();
            return Boolean.TRUE;
        });
        if (!Files.isExecutable(executable)) {
            throw new RunnerUnavailableException("the " + language + " conformance runner is not at "
                    + executable + " after running " + String.join(" ", buildCommand) + " in " + workingDirectory
                    + ". A missing runner FAILS rather than skipping: a language nobody could build is not a "
                    + "language that passed.");
        }
    }

    private void build() {
        if (buildCommand.isEmpty()) {
            return;
        }
        log.info("Building the {} conformance runner: {} (in {})", language, String.join(" ", buildCommand),
                workingDirectory);
        Process process;
        try {
            process = new ProcessBuilder(buildCommand)
                    .directory(workingDirectory.toFile())
                    .redirectErrorStream(true)
                    .start();
        } catch (IOException e) {
            throw new RunnerUnavailableException("cannot run '" + String.join(" ", buildCommand) + "' for the "
                    + language + " conformance runner - is the " + language + " toolchain installed? To run a "
                    + "narrower set deliberately, pass -Dpc.conformance.language=<comma list>; there is no "
                    + "silent skip.", e);
        }
        String output;
        int exit;
        try {
            output = new String(process.getInputStream().readAllBytes());
            if (!process.waitFor(BUILD_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
                process.destroyForcibly();
                throw new RunnerUnavailableException("building the " + language + " conformance runner did not "
                        + "finish within " + BUILD_TIMEOUT_MINUTES + " minutes");
            }
            exit = process.exitValue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RunnerUnavailableException("interrupted building the " + language + " conformance runner", e);
        } catch (IOException e) {
            throw new RunnerUnavailableException("reading the " + language + " runner build's output", e);
        }
        if (exit != 0) {
            throw new RunnerUnavailableException("building the " + language + " conformance runner failed (exit "
                    + exit + "):\n" + output);
        }
    }

    /** The full command line for one scenario - identical in shape for every language, which is the point. */
    public List<String> commandFor(ConformanceScenario scenario, Path sidecar) {
        return List.of(executable.toString(),
                RunnerContract.FLAG_SCENARIO, scenario.name(),
                RunnerContract.FLAG_BEHAVIOUR, scenario.behaviour().token(),
                RunnerContract.FLAG_SIDECAR, sidecar.toString(),
                RunnerContract.FLAG_EXPECT_DISPATCHES, Integer.toString(scenario.expectedDispatches()),
                RunnerContract.FLAG_TIMEOUT_SECONDS, Long.toString(scenario.runnerBudget().toSeconds()));
    }

    @Override
    public String toString() {
        return language;
    }
}
