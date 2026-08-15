package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.proxy.harness.ProxyHarness;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Runs one language through one scenario: boot the engine in this JVM, spawn that language's runner at it,
 * wait for its verdict, and hand the transcript to the scenario's assertions.
 * <p>
 * <b>Everything here is language-blind</b>, and that is the property that makes the next four languages
 * mechanical: the only thing this class knows about Go is a path in a registry entry.
 * <p>
 * <b>The engine is in this process</b>, on an ephemeral loopback port, so the suite reads engine state
 * directly. Against a real broker later, the same shape holds with the Kafka Admin API in place of the
 * mock consumer's commit history - the client half of the contract does not move.
 *
 * @author Antony Stubbs
 * @see SidecarShim
 */
@Slf4j
public final class ConformanceDriver {

    /**
     * How long past the runner's own budget the suite waits before killing it. A runner that overruns its
     * budget is expected to exit 1 by itself; this is the backstop for one that has stopped running at all,
     * and it must be generous enough that a slow machine does not turn a pass into a kill.
     */
    private static final Duration REAP_SLACK = Duration.ofSeconds(30);

    /**
     * Concurrent runs, and the high-water mark of them. Measured rather than asserted about: the suite's
     * whole value during the implementation phase is running languages side by side, and "we configured
     * JUnit for parallelism" is a claim about a properties file, not about what happened.
     */
    private static final AtomicInteger IN_FLIGHT = new AtomicInteger();

    private static final AtomicInteger PEAK_IN_FLIGHT = new AtomicInteger();

    /** The high-water mark of concurrent runner processes since this JVM started. */
    public static int peakConcurrentRuns() {
        return PEAK_IN_FLIGHT.get();
    }

    /**
     * The whole of one conformance run. Owns the harness's lifecycle so a scenario's failure still tears the
     * engine down.
     *
     * @return the runner's transcript, after the scenario's own assertions have passed
     */
    public static RunnerTranscript drive(LanguageRunner runner, ConformanceScenario scenario) {
        runner.ensureBuilt();

        try (var harness = new ProxyHarness(scenario.harnessScenario())) {
            int port = harness.startEngine();
            // The PORT is in the name, not just the language and scenario. Two tests may drive the same
            // language through the same scenario at the same moment - the parallelism proof does exactly
            // that - and a shared filename made the second run overwrite the first's shim, pointing both
            // clients at one engine; the loser failed its handshake with the single-connection guard's
            // RESOURCE_EXHAUSTED. The port is unique per engine, so it is the right discriminator.
            var name = runner.language() + "-" + scenario.name() + "-" + port;
            var sidecar = SidecarShim.write(RepoLayout.scratch(), name, port);
            var transcript = spawn(runner, scenario, sidecar);

            assertWithMessage("the %s runner's exit status IS the verdict: %s means it could not do what the "
                            + "scenario prescribed%s", runner.language(),
                    RunnerContract.EXIT_BEHAVIOUR_FAILED, transcript.diagnostics())
                    .that(transcript.exitCode()).isEqualTo(RunnerContract.EXIT_OK);

            scenario.assertion().check(harness, transcript);
            return transcript;
        }
    }

    private static RunnerTranscript spawn(LanguageRunner runner, ConformanceScenario scenario, Path sidecar) {
        var command = runner.commandFor(scenario, sidecar);
        var commandLine = String.join(" ", command);
        log.info("Driving {} through {}: {}", runner.language(), scenario.name(), commandLine);

        Process process;
        try {
            process = new ProcessBuilder(command).directory(runner.workingDirectory().toFile()).start();
        } catch (IOException e) {
            throw new LanguageRunner.RunnerUnavailableException("cannot spawn the " + runner.language()
                    + " conformance runner: " + commandLine + ". A runner that will not start FAILS - absence "
                    + "must never read as agreement.", e);
        }

        int peak = PEAK_IN_FLIGHT.get();
        int now = IN_FLIGHT.incrementAndGet();
        while (now > peak && !PEAK_IN_FLIGHT.compareAndSet(peak, now)) {
            peak = PEAK_IN_FLIGHT.get();
        }

        try {
            var stdout = new StreamPump(process.getInputStream());
            var stderr = new StreamPump(process.getErrorStream());
            stdout.start();
            stderr.start();

            long budget = scenario.runnerBudget().toSeconds() + REAP_SLACK.toSeconds();
            boolean exited = process.waitFor(budget, TimeUnit.SECONDS);
            if (!exited) {
                process.destroyForcibly();
                process.waitFor(REAP_SLACK.toSeconds(), TimeUnit.SECONDS);
            }
            stdout.drain();
            stderr.drain();

            var out = stdout.text();
            var transcript = new RunnerTranscript(runner.language(), commandLine,
                    exited ? process.exitValue() : -1, observations(out), out, stderr.text());

            assertWithMessage("the %s runner did not exit within its own %ss budget plus %ss of slack, so it "
                            + "was killed - a hung runner FAILS%s", runner.language(),
                    scenario.runnerBudget().toSeconds(), REAP_SLACK.toSeconds(), transcript.diagnostics())
                    .that(exited).isTrue();
            return transcript;
        } catch (InterruptedException e) {
            process.destroyForcibly();
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted waiting for the " + runner.language() + " runner", e);
        } finally {
            IN_FLIGHT.decrementAndGet();
            process.destroyForcibly();
        }
    }

    private static List<DispatchObservation> observations(String stdout) {
        var found = new ArrayList<DispatchObservation>();
        for (var line : stdout.split("\n")) {
            DispatchObservation.parse(line.strip()).ifPresent(found::add);
        }
        return List.copyOf(found);
    }

    /** Drains one of the child's streams on its own thread, so neither can fill its pipe and wedge the runner. */
    private static final class StreamPump extends Thread {

        private final InputStream source;

        private volatile String text = "";

        private StreamPump(InputStream source) {
            super("conformance-stream-pump");
            this.source = source;
            setDaemon(true);
        }

        @Override
        public void run() {
            try {
                text = new String(source.readAllBytes(), StandardCharsets.UTF_8);
            } catch (IOException e) {
                text = "<stream ended: " + e + ">";
            }
        }

        private String text() {
            return text;
        }

        /** Bounded, so a child that leaked a still-open stream to a grandchild cannot wedge the suite. */
        private void drain() {
            try {
                join(TimeUnit.SECONDS.toMillis(10));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new UncheckedIOException(new IOException("interrupted draining a runner's output", e));
            }
        }
    }

    private ConformanceDriver() {
    }
}
