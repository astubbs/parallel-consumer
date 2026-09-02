package bz.stub.parallelconsumer;
/*-
 * Copyright (C) 2020-2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Behavioural self-test of {@code bin/quarantine-lane-report.sh}'s outcome classification (DRY_RUN
 * seam - no gh calls): FAILED = expected/report-only; PASSED + deterministic = ACTION with a
 * merge-blocking thread; PASSED + {@code flapping = true} = report-only; NOT_RUN flagged. Fixture
 * surefire/failsafe XML mimics real report shapes including parameterized test entries.
 * <p>
 * IT READS THE REPORT FILE, NOT THE SCRIPT'S STDOUT, for everything about the comment body. The
 * script no longer posts the comment - it writes the report into {@code target/} and the workflow
 * hands that file to the shared sticky-comment module - so the file is the only place the body
 * exists, and a test that grepped stdout would be asserting about a log line rather than about what
 * gets published. That distinction is not pedantry: the equivalent self-test in
 * astubbs/parallel-consumer#407 passed on six corrupted report bodies because it grepped for a
 * status literal instead of checking where the literal had landed.
 * <p>
 * The machine-readable payload gets the same treatment: every assertion about it splits the file on
 * the payload marker first and then asserts, so a status word that leaked into the PROSE cannot be
 * mistaken for one that reached the payload.
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
        // From the REPORT FILE, which is what actually gets posted - the script's stdout says only
        // that the file was written.
        assertThat(readReport()).contains("ACTION REQUIRED");
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
        // The ROW's wording, read from the report file - the script's stdout only names the file.
        assertThat(readReport()).contains("passed (flapper)");
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


    // ---- the machine-readable payload, and the delta it makes possible -------------------------
    //
    // The workflow reads this payload back off its own last comment to decide whether to edit that
    // comment or post a fresh one. Everything below is therefore about a CONTRACT with
    // .github/workflows/quarantine-lane.yml and .github/scripts/sticky-report-comment.js, not about
    // this script's own output - and nothing enforces that the three agree on the marker's name, so
    // `grep -rn quarantine-lane-data` is the list to change if it ever moves.

    @Test
    void theReportCarriesAPayloadNamingTheOutcomeOfEachTest() throws Exception {
        fixtureWith("SomeQuarantinedIT", "someMethod", false,
                testcaseXml("SomeQuarantinedIT", "someMethod", true));
        runReport();
        assertThat(payload()).contains("\"SomeQuarantinedIT.someMethod\":\"FAILED\"");
    }

    @Test
    void thePayloadIsALINEOfItsOwnBelowTheWholeTable() throws Exception {
        // THE astubbs#407 TRAP, AND THE FIRST VERSION OF THIS TEST WALKED STRAIGHT INTO IT. That version
        // split the report on the marker and asserted the outcome word was absent from the half
        // above. Reintroducing the original corruption - splicing the payload into the HEADING line -
        // left it green: with the marker on line one there is almost no prose above it, so "the word
        // is not up there" was trivially true. An assertion that cannot fail on the defect it names
        // is worse than no assertion, because it reads as coverage.
        //
        // What actually distinguishes a good report from a corrupted one is POSITION, so that is what
        // is asserted: the payload occupies a line by itself, and the whole human-readable table sits
        // above it. Both halves fail on the splice.
        fixtureWith("SomeQuarantinedIT", "someMethod", false,
                testcaseXml("SomeQuarantinedIT", "someMethod", true));
        runReport();
        assertWithMessage("the payload is not alone on its line - it was spliced into the prose: %s", payloadLine())
                .that(payloadLine()).startsWith(PAYLOAD_MARKER);
        assertThat(payloadLine()).endsWith("-->");
        assertWithMessage("the table is not above the payload, so the payload landed inside the report body: %s", prose())
                .that(prose()).contains("| `SomeQuarantinedIT.someMethod` |");
    }

    @Test
    void aFlapperPassAndADeterministicPassAreDIFFERENTOutcomesInThePayload() throws Exception {
        // They say opposite things to a reader - one demands the annotation be deleted, the other
        // proves nothing - so collapsing both to PASSED would make an annotation GAINING
        // `flapping = true` invisible to the delta, and that flag is what decides whether a pass
        // demands action.
        fixtureWith("SomeQuarantinedIT", "someMethod", false,
                testcaseXml("SomeQuarantinedIT", "someMethod", false));
        runReport();
        assertThat(payload()).contains("\"SomeQuarantinedIT.someMethod\":\"PASSED_ACTION\"");

        fixtureWith("SomeQuarantinedIT", "someMethod", true,
                testcaseXml("SomeQuarantinedIT", "someMethod", false));
        runReport();
        assertThat(payload()).contains("\"SomeQuarantinedIT.someMethod\":\"PASSED_FLAPPER\"");
    }

    @Test
    void aTestThatDidNotRunIsInThePayloadToo() throws Exception {
        fixtureWith("SomeQuarantinedIT", "someMethod", false, null);
        runReport();
        assertThat(payload()).contains("\"SomeQuarantinedIT.someMethod\":\"NOT_RUN\"");
    }

    @Test
    void theStatusDigestIsSTABLEWhenNothingMoved() throws Exception {
        // If the digest varied between two runs of an unchanged lane, the workflow would read every
        // push as a status change and post a fresh comment on every one of them - the
        // fifteen-comments problem the stickiness exists to prevent, arriving through the mechanism
        // added to fix a different one.
        fixtureWith("SomeQuarantinedIT", "someMethod", false,
                testcaseXml("SomeQuarantinedIT", "someMethod", true));
        runReport();
        String first = statusDigest();
        runReport();
        assertThat(statusDigest()).isEqualTo(first);
        assertThat(first).isNotEmpty();
    }

    @Test
    void theStatusDigestCHANGESWhenAnOutcomeDoes() throws Exception {
        // The transition the operator cares about most: a quarantined test that starts passing means
        // its fix landed. The digest is what turns that into a NEW comment instead of a silent edit.
        fixtureWith("SomeQuarantinedIT", "someMethod", false,
                testcaseXml("SomeQuarantinedIT", "someMethod", true));
        runReport();
        String failing = statusDigest();

        fixtureWith("SomeQuarantinedIT", "someMethod", false,
                testcaseXml("SomeQuarantinedIT", "someMethod", false));
        runReport();
        assertWithMessage("failing -> passing must change the digest, or the change is edited in silently")
                .that(statusDigest()).isNotEqualTo(failing);
    }

    @Test
    void theStatusDigestIsSORTEDSoRegistryOrderCannotFakeAChange() throws Exception {
        // Registry order is editorial - somebody reordering two entries while fixing a typo must not
        // read as a lane whose outcomes moved.
        twoEntryFixture("Alpha", "Bravo");
        runReport();
        String digest = statusDigest();
        assertThat(digest).isEqualTo("AlphaIT.someMethod=FAILED;BravoIT.someMethod=FAILED");

        twoEntryFixture("Bravo", "Alpha");
        runReport();
        assertThat(statusDigest()).isEqualTo(digest);
    }

    @Test
    void aRunWithNoPrStillWritesTheReport() throws Exception {
        // The push-to-master path. It has no PR to comment on, but its report is the canonical
        // master-state record, and the previous version of this script returned before building the
        // table at all - so a log-only run printed a list of outcomes and no report.
        fixtureWith("SomeQuarantinedIT", "someMethod", false,
                testcaseXml("SomeQuarantinedIT", "someMethod", true));
        Result r = runScript("bin/quarantine-lane-report.sh", "DRY_RUN", "1");
        assertThat(r.exitCode).isEqualTo(0);
        assertThat(r.output).contains("report logged only");
        assertThat(readReport()).contains("Quarantine Lane Report");
        assertThat(payload()).contains("\"SomeQuarantinedIT.someMethod\":\"FAILED\"");
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

    /** A fixture with two quarantined classes, written to the registry in the order given. */
    private void twoEntryFixture(String first, String second) throws IOException {
        Path src = fixture.resolve("module/src/test-integration/java");
        Files.createDirectories(src);
        Path reports = fixture.resolve("module/target/failsafe-reports");
        Files.createDirectories(reports);
        StringBuilder registry = new StringBuilder();
        for (String name : new String[]{first, second}) {
            String cls = name + "IT";
            Files.write(src.resolve(cls + ".java"),
                    ("class " + cls + " {\n"
                            + "    @Quarantined(reason = \"d\", tracking = \"t\", fixedBy = \"PR #999999\")\n"
                            + "    void someMethod() {}\n}\n").getBytes(StandardCharsets.UTF_8));
            Files.write(reports.resolve("TEST-x." + cls + ".xml"),
                    testcaseXml(cls, "someMethod", true).getBytes(StandardCharsets.UTF_8));
            registry.append("- [ ] `").append(cls).append(".someMethod` - diagnosed. **Owner: PR #999999**\n");
        }
        writeRegistry(registry.toString());
    }

    /** The report the script wrote - what the workflow posts, and the only place the body exists. */
    private String readReport() throws IOException {
        Path report = fixture.resolve("target/quarantine-lane-report.md");
        // file-refs: N/A - a path the lane GENERATES inside this test's temporary fixture root;
        // `target/` holds build output and is never in the tree for the citation gate to resolve.
        assertWithMessage("the script wrote no report at %s", report).that(Files.exists(report)).isTrue();
        return new String(Files.readAllBytes(report), StandardCharsets.UTF_8);
    }

    /** The machine-readable half of the report, split off so assertions cannot confuse it with prose. */
    private String payload() throws IOException {
        String report = readReport();
        int at = report.indexOf(PAYLOAD_MARKER);
        assertWithMessage("the report carries no %s payload: %s", PAYLOAD_MARKER, report).that(at).isAtLeast(0);
        return report.substring(at);
    }

    /** The human-readable half - everything above the payload's line. */
    private String prose() throws IOException {
        String report = readReport();
        int at = report.indexOf(PAYLOAD_MARKER);
        return at < 0 ? report : report.substring(0, at);
    }

    /**
     * The whole LINE the payload sits on, trailing newline stripped. The line rather than the
     * substring-from-the-marker, because "where did it land" is the only question a corrupted report
     * answers differently - see {@link #thePayloadIsALINEOfItsOwnBelowTheWholeTable()}.
     */
    private String payloadLine() throws IOException {
        for (String line : readReport().split("\n")) {
            if (line.contains(PAYLOAD_MARKER)) {
                return line;
            }
        }
        throw new AssertionError("no " + PAYLOAD_MARKER + " line in the report: " + readReport());
    }

    /** The `status` field: the sorted `Class.method=OUTCOME` digest the workflow compares. */
    private String statusDigest() throws IOException {
        Matcher m = STATUS.matcher(payload());
        assertWithMessage("no status in the payload: %s", payload()).that(m.find()).isTrue();
        return m.group(1);
    }

    private static final String PAYLOAD_MARKER = "<!-- quarantine-lane-data:";
    private static final Pattern STATUS = Pattern.compile("\"status\":\"([^\"]*)\"");

    private Result runReport() throws IOException, InterruptedException {
        return runScript("bin/quarantine-lane-report.sh", "DRY_RUN", "1", "PR_NUMBER", "123", "HEAD_SHA", "deadbeef");
    }
}
