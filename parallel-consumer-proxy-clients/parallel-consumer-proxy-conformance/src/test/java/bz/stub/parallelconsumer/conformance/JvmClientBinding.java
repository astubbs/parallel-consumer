package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.client.ClientOptions;
import bz.stub.parallelconsumer.client.InboundRecord;
import bz.stub.parallelconsumer.client.Outcome;
import bz.stub.parallelconsumer.client.ParallelConsumerClient;
import bz.stub.parallelconsumer.client.RecordProcessor;
import bz.stub.parallelconsumer.proxy.harness.ProxyHarness;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * A JVM client wrapper, as a binding: the same scenarios, driven through
 * {@link ParallelConsumerClient} rather than through a child process.
 * <p>
 * <b>The runner is a function call, not a program, and that is a deliberate reading of the contract rather
 * than an exemption from it.</b> A conformance runner exists to do three things a test cannot do from
 * inside the suite's JVM: use the client library as an application would, cross a process boundary, and
 * exercise the library's own sidecar spawn. A JVM client needs no process boundary to be used as an
 * application uses it - the suite can hold the very object an application holds - and neither Java transport
 * offers a spawn to exercise: {@code GrpcParallelConsumerClient} connects to a port it is given, by design,
 * because spawning is the lifecycle unit's job. Wrapping one in a subprocess would therefore have meant
 * <em>writing</em> the spawn the library does not have, and then testing that. The spawn path is not left
 * untested by this choice: the Kotlin client owns one, and its runner is a real child process.
 * <p>
 * <b>What is not relaxed is the prescription.</b> {@link PrescribedRun} is the same code the control arm
 * runs and states the same contract a foreign runner implements - the four behaviour tokens, the fixed
 * failure literal, one observation per delivery, an exit status as the verdict - so a scenario cannot tell
 * this binding from a Ruby process, and no assertion can be written to suit it.
 * <p>
 * <b>Two transports, one binding class.</b> {@code java-direct}'s wire is a method call into core and
 * {@code java-grpc}'s is a real gRPC stream; everything else about driving them is identical, which is the
 * shared API's central claim. Registering them as two instances of one class rather than two classes is what
 * keeps that claim structural: there is no per-transport branch here for one of them to grow.
 *
 * @author Antony Stubbs
 * @see JvmClientBindings
 * @see PrescribedRun
 */
@Slf4j
public final class JvmClientBinding implements ConformanceBinding {

    /**
     * How one transport builds its client and starts it against a harness. Everything before the first
     * delivery differs between the two - one is handed mock Kafka clients, the other a loopback port - and
     * nothing after it does.
     */
    @FunctionalInterface
    public interface Transport {

        /**
         * Builds the client for this scenario, starts it on the given processor, and returns it so the run
         * can close it. Whatever engine-side arrangement the transport needs - assignment, seeding - has
         * happened by the time this returns.
         */
        ParallelConsumerClient start(ProxyHarness harness, ConformanceScenario scenario,
                                     RecordProcessor processor);
    }

    private final String name;

    private final Transport transport;

    JvmClientBinding(String name, Transport transport) {
        this.name = name;
        this.transport = transport;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Run execute(ProxyHarness harness, ConformanceScenario scenario) {
        var prescription = new PrescribedRun(name, scenario);
        var client = transport.start(harness, scenario, record -> outcomeFor(prescription, record));
        prescription.awaitPrescribedBehaviour();
        return new ClientRun(prescription, client);
    }

    @Override
    public String toString() {
        return name;
    }

    /**
     * The user's function: hand the delivery to the prescription, and say what it decided in the shared
     * API's own vocabulary. A failure returns {@link Outcome#failure(String)} rather than throwing, because
     * the reason has to reach the redelivery verbatim and a throw would put the exception's message there by
     * a second route.
     */
    private static Outcome outcomeFor(PrescribedRun prescription, InboundRecord record) {
        var failure = prescription.deliver(text(record.key()), record.offset(), record.attempt(),
                record.lastFailureReason().orElse(""));
        return failure.map(Outcome::failure).orElseGet(Outcome::success);
    }

    /** The transcript's keys are text, as every runner's observation line is; the API's are the raw bytes. */
    private static String text(byte[] key) {
        return key == null ? "" : new String(key, StandardCharsets.UTF_8);
    }

    /**
     * The connect-time configuration every binding uses, foreign runners included: the scenario's name is
     * the topic, the in-flight ceiling is the scenario's own shape so a held record cannot deadlock on an
     * executor count smaller than it, and the two fixed tunables come from {@link RunnerContract}.
     * <p>
     * <b>Ordering is deliberately not set</b> - "unset means take the engine's default", which is what every
     * language's runner does. A binding that pinned it would be running a configuration no client runs.
     */
    static ClientOptions optionsFor(ConformanceScenario scenario) {
        return ClientOptions.builder()
                .topics(List.of(scenario.name()))
                .maxConcurrency(scenario.expectedDispatches())
                .commitInterval(RunnerContract.COMMIT_INTERVAL)
                .defaultMessageRetryDelay(RunnerContract.RETRY_DELAY)
                .build();
    }

    /**
     * The run, open until the assertions are done. Closing releases the prescription first - a held record
     * goes back to being an ordinary success - and only then closes the client, so its shutdown drains work
     * that can actually finish instead of waiting out a function that was never going to return.
     */
    private static final class ClientRun implements Run {

        private final PrescribedRun prescription;

        private final ParallelConsumerClient client;

        private ClientRun(PrescribedRun prescription, ParallelConsumerClient client) {
            this.prescription = prescription;
            this.client = client;
        }

        @Override
        public RunnerTranscript transcript() {
            return prescription.transcript();
        }

        @Override
        public void close() {
            prescription.close();
            client.close();
        }
    }
}
