package bz.stub.parallelconsumer.proxy.testmode;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.proxy.config.ConfigureHandler;
import bz.stub.parallelconsumer.proxy.harness.HarnessScenario;
import bz.stub.parallelconsumer.proxy.harness.ProxyHarness;
import bz.stub.parallelconsumer.proxy.protocol.v1.ClientMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.Configure;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyServiceGrpc;
import bz.stub.parallelconsumer.proxy.protocol.v1.Report;
import com.github.bsideup.jabel.Desugar;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.OptionalLong;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The test-mode sidecar's spawning contract, both halves: the usage refusals a foreign test process observes,
 * and the real thing - a correct invocation serves the selected scenario over the production gRPC protocol,
 * port on stdout line one, until the parent dies (stdin EOF). The serving half runs {@link TestModeMain#run}
 * in-thread rather than as a spawned JVM so it stays in the surefire lane, but the wire is real: a genuine
 * gRPC channel into the genuine loopback server, exactly what a spawned non-JVM test would open.
 *
 * @author Antony Stubbs
 */
@Timeout(120)
class TestModeMainTest {

    /** Fixture selection is explicit: without {@code --mock} the sidecar refuses rather than guessing. */
    @Test
    void refusesToStartWithoutTheMockFlag() {
        var run = run();
        assertThat(run.exitCode).isEqualTo(TestModeMain.EXIT_USAGE);
        assertThat(run.err).contains(TestModeMain.MOCK_FLAG);
        assertWithMessage("the refusal explains why fixture selection is a flag and not a protocol field")
                .that(run.err).contains("R39");
    }

    @Test
    void refusesAnUnknownArgument() {
        var run = run(TestModeMain.MOCK_FLAG, "--definitely-not-a-flag");
        assertThat(run.exitCode).isEqualTo(TestModeMain.EXIT_USAGE);
        assertThat(run.err).contains("--definitely-not-a-flag");
    }

    /** The usage text lists the valid scenario names, so a refused caller can self-correct. */
    @Test
    void refusesAnUnknownScenarioAndListsTheRealOnes() {
        var run = run(TestModeMain.MOCK_FLAG, TestModeMain.SCENARIO_FLAG, "no-such-scenario");
        assertThat(run.exitCode).isEqualTo(TestModeMain.EXIT_USAGE);
        assertThat(run.err).contains("no-such-scenario");
        assertThat(run.err).contains("a-processed-record-advances-the-committed-offset");
    }

    /**
     * The whole spawning contract, end to end over the real wire: a correct invocation prints the bound port
     * as stdout line one, serves the baseline scenario's record over genuine gRPC to a test client, commits the
     * offset when the client reports success, and exits 0 when the parent dies (stdin EOF). This replaced the
     * designed-to-fail engine-seam marker test when U5-U7 landed the engine.
     */
    @Test
    void aCorrectInvocationServesTheScenarioOverRealGrpcUntilParentDeath() throws Exception {
        var scenario = HarnessScenario.A_PROCESSED_RECORD_ADVANCES_THE_COMMITTED_OFFSET;
        var outBytes = new ByteArrayOutputStream();
        var errBytes = new ByteArrayOutputStream();
        var lifeline = new PipedInputStream();
        var parentEnd = new PipedOutputStream(lifeline);
        var harnessRef = new AtomicReference<ProxyHarness>();
        ExecutorService sidecarThread = Executors.newSingleThreadExecutor();
        ManagedChannel channel = null;
        try {
            Future<Integer> exitCode = sidecarThread.submit(() -> TestModeMain.run(
                    new String[]{TestModeMain.MOCK_FLAG, TestModeMain.SCENARIO_FLAG, scenario.name()},
                    new PrintStream(outBytes, true, StandardCharsets.UTF_8),
                    new PrintStream(errBytes, true, StandardCharsets.UTF_8),
                    lifeline, harnessRef::set));

            int port = awaitPortLine(outBytes);
            Awaitility.await().atMost(ProxyHarness.CONVERGENCE_BUDGET).until(() -> harnessRef.get() != null);

            channel = ManagedChannelBuilder.forAddress("127.0.0.1", port).usePlaintext().build();
            var responses = new LinkedBlockingQueue<ProxyMessage>();
            var streamError = new AtomicReference<Throwable>();
            StreamObserver<ClientMessage> requests = ProxyServiceGrpc.newStub(channel)
                    .session(new StreamObserver<>() {
                        @Override
                        public void onNext(ProxyMessage message) {
                            responses.add(message);
                        }

                        @Override
                        public void onError(Throwable t) {
                            streamError.set(t);
                        }

                        @Override
                        public void onCompleted() {
                            // the test drives completion from the client side
                        }
                    });

            // connect-time configuration over the wire: options travel the protocol, per R39 - only the mock
            // fixture selection came from the flag (the KTD5-recorded exception)
            requests.onNext(ClientMessage.newBuilder()
                    .setConfigure(Configure.newBuilder()
                            .addTopics(scenario.name())
                            .setMaxConcurrency(2)
                            .setCommitInterval(com.google.protobuf.Duration.newBuilder()
                                    .setSeconds(ProxyHarness.COMMIT_INTERVAL.getSeconds())
                                    .setNanos(ProxyHarness.COMMIT_INTERVAL.getNano())))
                    .build());

            var configured = take(responses, streamError).getConfigured();
            assertThat(configured.getTopicsList()).containsExactly(scenario.name());
            assertThat(configured.getMaxConcurrency()).isEqualTo(2);
            assertWithMessage("the executor count travels once, in Configured (KTD38)")
                    .that(configured.getExecutorCount()).isEqualTo(2);
            assertThat(configured.getCapabilitiesList()).contains(ConfigureHandler.CAPABILITY_DISPATCH);

            var dispatch = take(responses, streamError).getDispatch();
            assertWithMessage("the scenario's seeded record travelled the real wire")
                    .that(dispatch.getRecord().getValue().toStringUtf8()).isEqualTo("hello");

            requests.onNext(ClientMessage.newBuilder()
                    .setReport(Report.newBuilder()
                            .setToken(dispatch.getToken())
                            .setSuccess(Report.Success.newBuilder()))
                    .build());

            Awaitility.await().atMost(ProxyHarness.CONVERGENCE_BUDGET).untilAsserted(() ->
                    assertWithMessage("the committed offset advances past the served record")
                            .that(harnessRef.get().lastCommittedOffset()).isEqualTo(OptionalLong.of(1)));

            requests.onCompleted();

            // parent death: closing the lifeline is the shutdown order, and 0 is the clean exit
            parentEnd.close();
            assertThat(exitCode.get(60, TimeUnit.SECONDS)).isEqualTo(0);

            var stdout = outBytes.toString(StandardCharsets.UTF_8);
            assertWithMessage("the port is stdout line ONE - the spawning parent reads it before anything else")
                    .that(stdout.lines().findFirst().orElseThrow()).startsWith(TestModeMain.PORT_LINE_PREFIX);
        } finally {
            if (channel != null) {
                channel.shutdownNow();
            }
            parentEnd.close();
            sidecarThread.shutdownNow();
        }
    }

    private static int awaitPortLine(ByteArrayOutputStream outBytes) {
        Awaitility.await().atMost(ProxyHarness.CONVERGENCE_BUDGET).until(() ->
                outBytes.toString(StandardCharsets.UTF_8).contains("\n"));
        String firstLine = outBytes.toString(StandardCharsets.UTF_8).lines().findFirst().orElseThrow();
        assertThat(firstLine).startsWith(TestModeMain.PORT_LINE_PREFIX);
        return Integer.parseInt(firstLine.substring(TestModeMain.PORT_LINE_PREFIX.length()).trim());
    }

    private static ProxyMessage take(BlockingQueue<ProxyMessage> responses, AtomicReference<Throwable> streamError)
            throws InterruptedException {
        var message = responses.poll(ProxyHarness.CONVERGENCE_BUDGET.toSeconds(), TimeUnit.SECONDS);
        assertWithMessage("no proxy message arrived within the budget (stream error: %s)", streamError.get())
                .that(message).isNotNull();
        return message;
    }

    @Desugar // Jabel requires the annotation on every record, even in this module where release=17 makes it a no-op
    private record Run(int exitCode, String out, String err) {
    }

    private static Run run(String... args) {
        var outBytes = new ByteArrayOutputStream();
        var errBytes = new ByteArrayOutputStream();
        int exitCode = TestModeMain.run(args,
                new PrintStream(outBytes, true, StandardCharsets.UTF_8),
                new PrintStream(errBytes, true, StandardCharsets.UTF_8));
        return new Run(exitCode,
                outBytes.toString(StandardCharsets.UTF_8),
                errBytes.toString(StandardCharsets.UTF_8));
    }
}
