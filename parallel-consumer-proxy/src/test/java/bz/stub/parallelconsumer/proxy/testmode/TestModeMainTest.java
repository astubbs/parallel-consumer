package bz.stub.parallelconsumer.proxy.testmode;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.github.bsideup.jabel.Desugar;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Pins the test-mode sidecar's spawning contract - the exit codes and refusals a foreign test process
 * observes - while the engine seam behind it is still stubbed. The full sidecar scenario ("starts from a
 * spawned non-JVM process, serves one record over the real gRPC path, exits on parent death") activates when
 * the engine units (U5-U7) fill {@code ProxyHarness.startEngine()}.
 *
 * @author Antony Stubbs
 */
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
     * ENGINE SEAM - while U5-U7 are unbuilt, a correct invocation exits with {@link
     * TestModeMain#EXIT_ENGINE_PENDING} and the message routes the caller to the seam. This test is DESIGNED
     * to fail and be replaced when the engine lands: at that point a correct invocation serves instead of
     * refusing, and the sidecar's real scenarios (spawn, serve one record over gRPC, die with the parent)
     * take over.
     */
    @Test
    void aCorrectInvocationReportsTheEngineSeamAsPending() {
        var run = run(TestModeMain.MOCK_FLAG);
        assertThat(run.exitCode).isEqualTo(TestModeMain.EXIT_ENGINE_PENDING);
        assertThat(run.err).contains("ProxyProcessor");
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
