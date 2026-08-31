package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The shim is four lines and it is the whole of the spawning contract as a client observes it, so it is
 * <b>run</b> here rather than read.
 * <p>
 * <b>What this can prove without an engine, and it is most of what the shim is for.</b> A client library's
 * one entry point spawns a sidecar, reads {@code port: <n>} from its stdout, and holds its stdin as the
 * parent-death lifeline; the shim exists so that contract is exercised for real while the engine sits in the
 * asserting JVM instead of in a child process. Both halves of that are testable today: the announcement, and
 * the fact that the process stays alive until its stdin closes and then exits by itself. Only the third part
 * - a client connecting to the announced port and finding an engine - needs the rung above.
 * <p>
 * <b>The lifeline half is the half worth testing.</b> A shim that announced its port and exited immediately
 * would look correct in every transcript and would break every spawned run, because the client reaps a
 * sidecar by closing its stdin and a sidecar that had already gone would be reported as a crash. It is also
 * the half most easily broken by an innocent-looking edit - replacing the builtin {@code read} loop with a
 * {@code cat} gives the same visible behaviour and leaves a grandchild that outlives the reap.
 *
 * @author Antony Stubbs
 * @see SidecarShim
 */
class SidecarShimTest {

    /** Long enough that a loaded machine is not mistaken for a hung child; short enough that a hang is not a wait. */
    private static final long AWAIT_SECONDS = 30;

    @Test
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void itAnnouncesThePortItWasGivenAndThenHoldsItsStdinUntilItIsClosed() throws Exception {
        var script = SidecarShim.write(RepoLayout.scratch(), "shim-test", 41287);

        assertWithMessage("the client libraries refuse a relative or PATH-resolved sidecar - a rule about "
                + "which binary receives the user's Kafka credentials, which this suite has no business "
                + "making an exception to")
                .that(script.isAbsolute()).isTrue();
        assertThat(Files.isExecutable(script)).isTrue();

        var process = new ProcessBuilder(script.toString()).start();
        try {
            try (var stdout = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                assertWithMessage("the announcement is the whole of what a spawning client needs, and its "
                        + "format is the contract")
                        .that(stdout.readLine()).isEqualTo("port: 41287");

                assertWithMessage("a sidecar that exited as soon as it had announced would be reported by "
                        + "every client as a crash, because closing stdin is how a client reaps one")
                        .that(process.waitFor(1, TimeUnit.SECONDS)).isFalse();

                // The reap, exactly as a client library performs it.
                process.getOutputStream().close();

                assertWithMessage("EOF on stdin ends the loop, so the shim exits by itself rather than "
                        + "having to be killed")
                        .that(process.waitFor(AWAIT_SECONDS, TimeUnit.SECONDS)).isTrue();
                assertThat(process.exitValue()).isEqualTo(0);
            }
        } finally {
            process.destroyForcibly();
        }
    }

    /**
     * Two shims written for two ports are two files, which is not decoration: the driver names a shim after
     * the language, the scenario AND the port precisely because two tests may drive the same language through
     * the same scenario at once, and a shared filename made the second run overwrite the first's - pointing
     * both clients at one engine, where the loser failed its handshake on the single-connection guard.
     */
    @Test
    void aShimIsWrittenPerNameSoTwoConcurrentRunsCannotOverwriteEachOther() {
        var first = SidecarShim.write(RepoLayout.scratch(), "overlap-test-1", 1);
        var second = SidecarShim.write(RepoLayout.scratch(), "overlap-test-2", 2);

        assertThat(first).isNotEqualTo(second);
    }
}
