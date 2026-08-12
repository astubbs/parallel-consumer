package io.confluent.parallelconsumer;
/*-
 * Copyright (C) 2020-2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Behavioural self-test of {@code bin/quarantine-lane-report.sh}'s outcome classification (DRY_RUN
 * seam - no gh calls): FAILED = expected/report-only; PASSED + deterministic = ACTION with a
 * merge-blocking thread; PASSED + {@code flapping = true} = report-only; NOT_RUN flagged. Fixture
 * surefire/failsafe XML mimics real report shapes including parameterized test entries.
 */
class QuarantineLaneReportScriptTest extends AbstractQuarantineScriptTest {

    @Test
    void failingQuarantinedTestIsExpectedReportOnly() throws Exception {
        fixtureWith("SomeQuarantinedIT", "someMethod", false,
                testcaseXml("SomeQuarantinedIT", "someMethod", true));
        Result r = runReport();
        assertThat(r.exitCode).isEqualTo(0);
        assertThat(r.output).contains("SomeQuarantinedIT.someMethod -> FAILED");
        assertThat(r.output).doesNotContain("would ensure review thread");
    }

    @Test
    void deterministicPassDemandsActionAndThread() throws Exception {
        fixtureWith("SomeQuarantinedIT", "someMethod", false,
                testcaseXml("SomeQuarantinedIT", "someMethod", false));
        Result r = runReport();
        assertThat(r.exitCode).isEqualTo(0);
        assertThat(r.output).contains("SomeQuarantinedIT.someMethod -> PASSED");
        assertThat(r.output).contains("ACTION REQUIRED");
        assertWithMessage("deterministic pass must plan a merge-blocking thread - output: %s", r.output)
                .that(r.output).contains("would ensure review thread for SomeQuarantinedIT.someMethod");
    }

    @Test
    void flapperPassIsReportOnly() throws Exception {
        fixtureWith("SomeQuarantinedIT", "someMethod", true,
                testcaseXml("SomeQuarantinedIT", "someMethod", false));
        Result r = runReport();
        assertThat(r.exitCode).isEqualTo(0);
        assertThat(r.output).contains("flapping=1");
        assertThat(r.output).contains("flapper");
        assertThat(r.output).doesNotContain("would ensure review thread");
    }

    @Test
    void parameterizedEntriesWithAnyFailureCountAsFailed() throws Exception {
        // real failsafe shape: one <testcase> per param - [latest] fails, others pass => FAILED overall
        String xml = "<testsuite>\n" +
                "  <testcase name=\"someMethod(OffsetResetStrategy)[1]\" classname=\"x.SomeQuarantinedIT\" time=\"1\"/>\n" +
                "  <testcase name=\"someMethod(OffsetResetStrategy)[2]\" classname=\"x.SomeQuarantinedIT\" time=\"1\">\n" +
                "    <failure message=\"await timed out\">stack</failure>\n" +
                "  </testcase>\n" +
                "</testsuite>\n";
        fixtureWith("SomeQuarantinedIT", "someMethod", false, xml);
        Result r = runReport();
        assertThat(r.output).contains("SomeQuarantinedIT.someMethod -> FAILED");
    }

    @Test
    void missingReportIsFlaggedNotRun() throws Exception {
        fixtureWith("SomeQuarantinedIT", "someMethod", false, null);
        Result r = runReport();
        assertThat(r.exitCode).isEqualTo(0);
        assertThat(r.output).contains("SomeQuarantinedIT.someMethod -> NOT_RUN");
    }

    @Test
    void nonQuarantinedTestInReportsFailsTheLaneLeakSelfCheck() throws Exception {
        // the user-mandated guarantee: the lane must PROVE it ran only quarantined tests - a stray
        // testcase in the reports means group filtering regressed (the surefire-binding P1 class)
        fixtureWith("SomeQuarantinedIT", "someMethod", false,
                testcaseXml("SomeQuarantinedIT", "someMethod", true));
        Path reports = fixture.resolve("module/target/surefire-reports");
        Files.createDirectories(reports);
        Files.write(reports.resolve("TEST-x.LeakedUnitTest.xml"),
                testcaseXml("LeakedUnitTest", "shouldNeverRunHere", false).getBytes(StandardCharsets.UTF_8));
        Result r = runReport();
        assertWithMessage("a leaked non-quarantined test must fail the job - output: %s", r.output)
                .that(r.exitCode).isEqualTo(1);
        assertThat(r.output).contains("LANE_LEAK: x.LeakedUnitTest.shouldNeverRunHere");
    }

    @Test
    void leakSelfCheckPassesWhenOnlyQuarantinedTestsRan() throws Exception {
        fixtureWith("SomeQuarantinedIT", "someMethod", false,
                testcaseXml("SomeQuarantinedIT", "someMethod", true));
        Result r = runReport();
        assertThat(r.output).contains("Lane-leak self-check passed");
    }

    // ---- fixtures ----

    private void fixtureWith(String cls, String method, boolean flapping, String reportXml) throws IOException {
        Path src = fixture.resolve("module/src/test-integration/java");
        Files.createDirectories(src);
        String java = "class " + cls + " {\n" +
                "    @Quarantined(reason = \"d\", tracking = \"t\", fixedBy = \"PR #999999\"" +
                (flapping ? ", flapping = true" : "") + ")\n" +
                "    void " + method + "() {}\n}\n";
        Files.write(src.resolve(cls + ".java"), java.getBytes(StandardCharsets.UTF_8));
        writeRegistry("- [ ] `" + cls + "." + method + "` - diagnosed. **Owner: PR #999999**\n");
        if (reportXml != null) {
            Path reports = fixture.resolve("module/target/failsafe-reports");
            Files.createDirectories(reports);
            Files.write(reports.resolve("TEST-x." + cls + ".xml"), reportXml.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static String testcaseXml(String cls, String method, boolean failed) {
        return "<testsuite>\n  <testcase name=\"" + method + "\" classname=\"x." + cls + "\" time=\"1\">" +
                (failed ? "<failure message=\"boom\">stack</failure>" : "") +
                "</testcase>\n</testsuite>\n";
    }

    private Result runReport() throws IOException, InterruptedException {
        return runScript("bin/quarantine-lane-report.sh", "DRY_RUN", "1", "PR_NUMBER", "123", "HEAD_SHA", "deadbeef");
    }
}
