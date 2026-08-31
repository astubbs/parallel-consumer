package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;

import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The negative controls on the suite itself: a runner that is not there, and a runner that is there and
 * broken, must both FAIL - loudly, with a message naming what was wrong.
 * <p>
 * <b>Absence looks exactly like agreement.</b> Of everything that could go wrong with a suite driving ten
 * languages, a language quietly not running is the one most likely to survive to a release: nothing goes
 * red, the run is fast, and the report says every scenario passed. So the suite is held to saying no about
 * its own machinery before it is trusted to say yes about a client.
 *
 * @author Antony Stubbs
 */
class AbsentAndBrokenRunnersFailTest {

    @Test
    void aRunnerThatIsNotThereFailsRatherThanSkipping() {
        var absent = LanguageRunners.absent();
        var scenario = ConformanceScenarios.PROCESSED_RECORD_ADVANCES_THE_COMMITTED_OFFSET;

        var thrown = assertThrows(LanguageRunner.RunnerUnavailableException.class,
                () -> ConformanceDriver.drive(absent, scenario));

        assertWithMessage("the failure must name the missing runner, so nobody has to guess whether the "
                + "language ran")
                .that(thrown).hasMessageThat().contains("no-such-conformance-runner");
        assertWithMessage("and must say that absence is a failure rather than a skip")
                .that(thrown).hasMessageThat().contains("FAILS rather than skipping");
    }

    @Test
    void aRunnerThatCrashesFailsWithItsExitStatusAndOutput() {
        var broken = LanguageRunners.deliberatelyFailing(writeCrashingRunner("crashing-runner", 0));
        var scenario = ConformanceScenarios.PROCESSED_RECORD_ADVANCES_THE_COMMITTED_OFFSET;

        var thrown = assertThrows(AssertionError.class, () -> ConformanceDriver.drive(broken, scenario));

        assertWithMessage("the exit status IS the verdict, so the failure must report it")
                .that(thrown).hasMessageThat().contains("exit     : 3");
        assertWithMessage("and must carry the runner's own words, which are usually the interesting half")
                .that(thrown).hasMessageThat().contains("this runner is deliberately broken");
    }

    /**
     * Writes a runner that ignores its arguments, optionally dawdles, and exits non-zero. A shell one-liner
     * rather than a real client: what is under test is the suite's reaction to a bad runner, and a real
     * client would only add ways for this test to fail for reasons that are not the point.
     *
     * @param dawdleSeconds how long to run before failing - non-zero when a test needs the failure to
     *                      OVERLAP something else, as the parallelism proof does
     */
    static Path writeCrashingRunner(String name, int dawdleSeconds) {
        try {
            var directory = RepoLayout.scratch();
            Files.createDirectories(directory);
            var script = directory.resolve(name + ".sh").toAbsolutePath();
            Files.writeString(script, """
                    #!/bin/sh
                    # A deliberately broken conformance runner - the suite's negative control.
                    echo "this runner is deliberately broken" >&2
                    sleep %d
                    exit 3
                    """.formatted(dawdleSeconds));
            Files.setPosixFilePermissions(script, PosixFilePermissions.fromString("rwxr-x---"));
            return script;
        } catch (IOException e) {
            throw new UncheckedIOException("writing the deliberately broken runner", e);
        }
    }
}
