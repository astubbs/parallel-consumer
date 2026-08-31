package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Runs one binding through one scenario: boot the engine in this JVM, have the binding carry out what the
 * scenario prescribes, and hand the transcript to the scenario's assertions.
 * <p>
 * <b>Everything here is binding-blind</b>, and that is the property that makes the next binding mechanical:
 * this class knows a name, a prescription and a transcript, and nothing about how any of them were produced.
 * The foreign half - write the sidecar shim, spawn the language's runner at the harness's port, reap it and
 * parse its stdout - is the next extraction out of astubbs/parallel-consumer#293, and it needs an engine to
 * spawn against, which this stack does not have.
 * <p>
 * <b>The engine is in this process</b>, over mock Kafka clients, so the suite reads engine state directly.
 * Against a real broker later, the same shape holds with the Kafka Admin API in place of the mock consumer's
 * commit history - the client half of the contract does not move.
 *
 * @author Antony Stubbs
 * @see ConformanceBinding
 */
@Slf4j
public final class ConformanceDriver {

    /**
     * The whole of one conformance run, for any binding. Owns the harness's lifecycle so a scenario's
     * failure still tears the engine down.
     * <p>
     * <b>The assertions are made while the binding's run is still open.</b> A binding may be holding a
     * record deliberately - {@code report-nothing} prescribes it - and "the offset never advanced" means
     * nothing once the client that was holding it has gone.
     *
     * @return the binding's transcript, after the scenario's own assertions have passed
     */
    public static RunnerTranscript drive(ConformanceBinding binding, ConformanceScenario scenario) {
        binding.ensureAvailable();

        try (var harness = new ConformanceHarness(scenario.harnessScenario());
             var run = binding.execute(harness, scenario)) {
            var transcript = run.transcript();

            assertWithMessage("the %s binding's exit status IS the verdict: %s means it could not do what the "
                            + "scenario prescribed%s", binding.name(),
                    RunnerContract.EXIT_BEHAVIOUR_FAILED, transcript.diagnostics())
                    .that(transcript.exitCode()).isEqualTo(RunnerContract.EXIT_OK);

            scenario.assertion().check(harness, transcript);
            return transcript;
        }
    }

    private ConformanceDriver() {
    }
}
