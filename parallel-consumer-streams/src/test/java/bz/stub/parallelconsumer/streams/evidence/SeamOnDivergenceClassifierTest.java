package bz.stub.parallelconsumer.streams.evidence;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.streams.PcUnsupportedConstruct;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The check on the check: this proves the seam-on lane can fail in each of the three ways it claims to, on
 * synthetic reports, in milliseconds.
 * <p>
 * <b>Why not prove it only end to end.</b> The real lane runs Apache Kafka's whole suite twice, so a single
 * sabotage experiment costs about a quarter of an hour and exercises one path. Both are needed and they answer
 * different questions: these cases pin the classifier's own logic against regression, and the end-to-end
 * sabotage (recorded on the pull request) proves the wiring - that the executions run in the right order, that
 * the pins arrive, and that a genuine semantic change lands in the unexplained pile rather than being absorbed
 * by an over-broad predicate.
 * <p>
 * Every case here states what it would take to make the lane wrong, and then makes it: a missing directory, a
 * stale one, an arm that was never pinned, a divergence nobody has attributed. A gate that has only ever been
 * seen to pass has not been tested.
 *
 * @author Antony Stubbs
 * @see SeamOnDivergenceLaneTest
 */
class SeamOnDivergenceClassifierTest {

    private static final String KAFKA_CLASS = "org.apache.kafka.streams.processor.internals.StreamTaskTest";

    private static final Instant BUILD_STARTED = Instant.parse("2026-01-01T00:00:00Z");

    // -------------------------------------------------------------------------------------------------
    // Reading an arm: every way a report directory can be read as a pass without having measured anything.
    // -------------------------------------------------------------------------------------------------

    @Test
    void aMissingReportDirectoryIsAnErrorAndNotAnEmptyResult(@TempDir final Path temp) {
        final Path never = temp.resolve("was-never-written");

        assertThatThrownBy(() -> SurefireArm.read("seam-on", never, BUILD_STARTED))
                .as("the whole point: an execution that was skipped leaves no directory, and 'no failures "
                        + "found in a directory that does not exist' is the shape of a false green")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no report directory");
    }

    @Test
    void anEmptyReportDirectoryIsAnErrorBecauseAnEmptyGlobSumsToZeroFailures(@TempDir final Path temp)
            throws IOException {
        final Path empty = Files.createDirectories(temp.resolve("empty"));

        assertThatThrownBy(() -> SurefireArm.read("seam-on", empty, BUILD_STARTED))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("holds no TEST-*.xml");
    }

    @Test
    void aReportLeftOverFromAnEarlierRunIsRejectedRatherThanReadAsThisRun(@TempDir final Path temp)
            throws IOException {
        final Path stale = writeArm(temp, "stale", true, passing("shouldDoSomething"));
        for (final Path report : listReports(stale)) {
            Files.setLastModifiedTime(report, FileTime.from(BUILD_STARTED.minusSeconds(3600)));
        }

        assertThatThrownBy(() -> SurefireArm.read("seam-on", stale, BUILD_STARTED))
                .as("it parses perfectly and says nothing about this run - which is exactly why it has to "
                        + "throw rather than be believed")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("before this build started");
    }

    // -------------------------------------------------------------------------------------------------
    // Proving each arm is the arm it claims to be.
    // -------------------------------------------------------------------------------------------------

    @Test
    void anArmThatWasNeverPinnedIsRefusedBecauseItMeasuredTheOtherOne(@TempDir final Path temp)
            throws IOException {
        // Both arms recorded dispatch=false: the seam-on execution's pin did not arrive, so this run
        // differenced the control against itself and would report a flawless zero divergences.
        final SurefireArm off = read(temp, "off", false, passing("shouldDoSomething"));
        final SurefireArm on = read(temp, "on", false, passing("shouldDoSomething"));

        assertThatThrownBy(() -> classifier(off, on, ledgerWith(temp)).assertBothArmsRanUnderTheirOwnPin())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("The seam-ON arm");
    }

    @Test
    void armsCoveringDifferentClassesAreRefusedRatherThanDifferenced(@TempDir final Path temp)
            throws IOException {
        final SurefireArm off = read(temp, "off", false, passing("shouldDoSomething"));
        final Path onDir = writeArm(temp, "on", true, passing("shouldDoSomething"));
        Files.write(onDir.resolve("TEST-org.apache.kafka.OtherTest.xml"),
                suite("org.apache.kafka.OtherTest", true, passing("shouldDoSomethingElse"))
                        .getBytes(StandardCharsets.UTF_8));
        final SurefireArm on = SurefireArm.read("seam-on", onDir, BUILD_STARTED);

        assertThatThrownBy(() -> classifier(off, on, ledgerWith(temp)).assertBothArmsCoveredTheSameClasses())
                .as("a case present in one arm only cannot be differenced, and dropping it silently is the "
                        + "direction that HIDES divergences")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("different classes");
    }

    // -------------------------------------------------------------------------------------------------
    // The three ways a run can end.
    // -------------------------------------------------------------------------------------------------

    @Test
    void aDivergenceNoMechanismRecognisesIsReportedAsUnexplained(@TempDir final Path temp) throws IOException {
        final SurefireArm off = read(temp, "off", false, passing("shouldProcessTheRecord"));
        final SurefireArm on = read(temp, "on", true,
                failing("shouldProcessTheRecord", "org.opentest4j.AssertionFailedError",
                        "expected: <5> but was: <4>"));

        final SeamOnDivergenceClassifier.Result result = classifier(off, on, ledgerWith(temp)).classify();

        assertThat(result.getUnexplained())
                .as("nothing in that message names a mechanism, so the lane must say so rather than find "
                        + "one - this is the case a sabotaged semantic has to land in")
                .hasSize(1);
        assertThat(result.getUnexplained().get(0).getTestCase().getName()).isEqualTo("shouldProcessTheRecord");
        assertThat(result.getExplained()).isEmpty();
    }

    @Test
    void aRefusalIsExplainedAndTheConstructItNamesIsReportedWithIt(@TempDir final Path temp)
            throws IOException {
        // Derived, not written down: the message is whatever the envelope's own enum produces today.
        final String refusal = PcUnsupportedConstruct.EXACTLY_ONCE.describe();
        final SurefireArm off = read(temp, "off", false, passing("shouldUseExactlyOnce"));
        final SurefireArm on = read(temp, "on", true,
                failing("shouldUseExactlyOnce", "java.lang.UnsupportedOperationException", refusal));

        final SeamOnDivergenceClassifier.Result result = classifier(off, on, ledgerWith(temp)).classify();

        assertThat(result.getUnexplained()).isEmpty();
        assertThat(result.getExplained()).hasSize(1);
        assertThat(result.getExplained().get(0).getMechanism().getName()).isEqualTo("refused-construct");
        assertThat(result.getExplained().get(0).getAttribution())
                .as("the specific construct, so a reader does not have to open the enum to find out which "
                        + "refusal fired")
                .isEqualTo(PcUnsupportedConstruct.EXACTLY_ONCE.getDisplayName());
    }

    @Test
    void aCaseAssertingTheStockCommitEncodingIsExplainedFromWhatItRendered(@TempDir final Path temp)
            throws IOException {
        final SurefireArm off = read(temp, "off", false, passing("shouldCommitTheNextOffset"));
        final SurefireArm on = read(temp, "on", true,
                failing("shouldCommitTheNextOffset", "java.lang.AssertionError",
                        "Expected: <{topic1-0=OffsetAndMetadata{offset=5, metadata='AgAA'}}> but: was <{}>"));

        final SeamOnDivergenceClassifier.Result result = classifier(off, on, ledgerWith(temp)).classify();

        assertThat(result.getExplained()).hasSize(1);
        assertThat(result.getExplained().get(0).getMechanism().getName()).isEqualTo("commit-frontier-encoding");
    }

    @Test
    void aCaseFailingInBOTHArmsIsNotADivergenceBecauseTheSeamDidNotCauseIt(@TempDir final Path temp)
            throws IOException {
        final SurefireArm off = read(temp, "off", false,
                failing("shouldDoSomething", "java.lang.AssertionError", "boom"));
        final SurefireArm on = read(temp, "on", true,
                failing("shouldDoSomething", "java.lang.AssertionError", "boom"));

        final SeamOnDivergenceClassifier.Result result = classifier(off, on, ledgerWith(temp)).classify();

        assertThat(result.getDivergences())
                .as("attributing it to the seam would credit a change that did not cause it - which is what "
                        + "a lane with no control arm does by construction")
                .isEmpty();
        assertThat(result.getFailingInBothArms()).hasSize(1);
    }

    // -------------------------------------------------------------------------------------------------
    // The ledger: attributions that cannot be derived, and the flake that dirties the control.
    // -------------------------------------------------------------------------------------------------

    @Test
    void aLedgeredAttributionExplainsADivergenceAndNamesTheNoteItCameFrom(@TempDir final Path temp)
            throws IOException {
        final InflightMarkers ledger = ledgerWith(temp,
                "<!-- seam-on-divergence-class: asynchronous-dispatch = the test drives the task synchronously -->",
                "<!-- seam-on-divergence: " + KAFKA_CLASS + "#shouldProcessInOrder = asynchronous-dispatch -->");
        final SurefireArm off = read(temp, "off", false, passing("shouldProcessInOrder"));
        final SurefireArm on = read(temp, "on", true,
                failing("shouldProcessInOrder", "org.opentest4j.AssertionFailedError",
                        "expected: <5> but was: <0>"));

        final SeamOnDivergenceClassifier.Result result = classifier(off, on, ledger).classify();

        assertThat(result.getExplained()).hasSize(1);
        assertThat(result.getExplained().get(0).getMechanism().getName()).isEqualTo("asynchronous-dispatch");
        assertThat(result.getExplained().get(0).getAttribution()).contains("docs/inflight/");
    }

    @Test
    void aLedgeredAttributionMatchesEveryParameterisationOfItsMethod(@TempDir final Path temp)
            throws IOException {
        final InflightMarkers ledger = ledgerWith(temp,
                "<!-- seam-on-divergence-class: asynchronous-dispatch = the test drives the task synchronously -->",
                "<!-- seam-on-divergence: " + KAFKA_CLASS + "#shouldPunctuate = asynchronous-dispatch -->");
        final SurefireArm off = read(temp, "off", false, passing("shouldPunctuate(boolean)[2]"));
        final SurefireArm on = read(temp, "on", true,
                failing("shouldPunctuate(boolean)[2]", "org.opentest4j.AssertionFailedError", "nope"));

        assertThat(classifier(off, on, ledger).classify().getExplained())
                .as("which parameter loses a race is a property of the run, not of the diagnosis")
                .hasSize(1);
    }

    @Test
    void anAttributionToAnUndeclaredClassIsRefusedBecauseANameIsNotAnExplanation(@TempDir final Path temp)
            throws IOException {
        final Path notes = Files.createDirectories(temp.resolve("ledger-bad"));
        Files.write(notes.resolve("test-thing.md"),
                ("<!-- seam-on-divergence: " + KAFKA_CLASS + "#shouldDoSomething = nobody-declared-this -->")
                        .getBytes(StandardCharsets.UTF_8));

        assertThatThrownBy(() -> DivergenceMechanism.registry(InflightMarkers.from(notes)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no seam-on-divergence-class marker");
    }

    @Test
    void anUnledgeredControlArmFailureMeansThereIsNoControlToDifferenceAgainst(@TempDir final Path temp)
            throws IOException {
        final SurefireArm off = read(temp, "off", false,
                failing("shouldDoSomething", "java.lang.AssertionError", "the control arm broke"));
        final SurefireArm on = read(temp, "on", true, passing("shouldDoSomething"));

        final SeamOnDivergenceClassifier.Result result = classifier(off, on, ledgerWith(temp)).classify();

        assertThat(result.getUnexplainedControlArmFailures()).hasSize(1);
    }

    @Test
    void aLedgeredFlakeInTheControlArmIsNamedRatherThanReRun(@TempDir final Path temp) throws IOException {
        final InflightMarkers ledger = ledgerWith(temp,
                "<!-- flaky-case: " + KAFKA_CLASS + "#shouldLogSomething -->");
        final SurefireArm off = read(temp, "off", false,
                failing("shouldLogSomething(boolean)[3]", "org.opentest4j.AssertionFailedError",
                        "expected: <true> but was: <false>"));
        final SurefireArm on = read(temp, "on", true, passing("shouldLogSomething(boolean)[3]"));

        final SeamOnDivergenceClassifier.Result result = classifier(off, on, ledger).classify();

        assertThat(result.getUnexplainedControlArmFailures())
                .as("a retry destroys the sighting; a ledger entry relocates it and lets the run still "
                        + "produce a verdict")
                .isEmpty();
        assertThat(result.getControlArmFailures()).hasSize(1);
        assertThat(result.getControlArmFailures().get(0).getAttribution()).contains("docs/inflight/");
    }

    @Test
    void aTriagedAttributionBEATSAFlakeMarkerOnTheSameCase(@TempDir final Path temp) throws IOException {
        // The branch forest counted exactly this shape - a ledgered-flaky case, green in the control and red
        // in the measured arm - as "the known flake firing", and it hid a divergence that recurs every run
        // under one parameter. The flake registry is consulted LAST among the divergence mechanisms for that
        // reason: a real attribution always wins, and the flake is only ever the answer of last resort.
        final InflightMarkers ledger = ledgerWith(temp,
                "<!-- flaky-case: " + KAFKA_CLASS + "#shouldLogSomething -->",
                "<!-- seam-on-divergence-class: asynchronous-dispatch = the test drives the task synchronously -->",
                "<!-- seam-on-divergence: " + KAFKA_CLASS + "#shouldLogSomething = asynchronous-dispatch -->");
        final SurefireArm off = read(temp, "off", false, passing("shouldLogSomething(boolean)[3]"));
        final SurefireArm on = read(temp, "on", true,
                failing("shouldLogSomething(boolean)[3]", "org.opentest4j.AssertionFailedError",
                        "expected: <true> but was: <false>"));

        final SeamOnDivergenceClassifier.Result result = classifier(off, on, ledger).classify();

        assertThat(result.getExplained()).hasSize(1);
        assertThat(result.getExplained().get(0).getMechanism().getName())
                .as("attributing this to the flake is what made a systematic divergence disappear once "
                        + "already; the diagnosis has to outrank the excuse")
                .isEqualTo("asynchronous-dispatch");
    }

    @Test
    void aLedgerEntryForACaseThatNoLongerDivergesIsReportedRatherThanIgnored(@TempDir final Path temp)
            throws IOException {
        final InflightMarkers ledger = ledgerWith(temp,
                "<!-- seam-on-divergence-class: asynchronous-dispatch = the test drives the task synchronously -->",
                "<!-- seam-on-divergence: " + KAFKA_CLASS + "#shouldHaveBeenFixed = asynchronous-dispatch -->");
        final SurefireArm off = read(temp, "off", false, passing("shouldHaveBeenFixed"));
        final SurefireArm on = read(temp, "on", true, passing("shouldHaveBeenFixed"));

        final SeamOnDivergenceClassifier.Result result = classifier(off, on, ledger).classify();

        assertThat(result.getLedgerEntriesThatMatchedNothing())
                .as("usually good news - a rung fixed it - but silence would leave the ledger rotting")
                .anyMatch(entry -> entry.contains("shouldHaveBeenFixed"));
    }

    @Test
    void theRenderedReportNamesEveryUnexplainedCaseAndItsRecordedFailure(@TempDir final Path temp)
            throws IOException {
        final SurefireArm off = read(temp, "off", false, passing("shouldProcessTheRecord"));
        final SurefireArm on = read(temp, "on", true,
                failing("shouldProcessTheRecord", "org.opentest4j.AssertionFailedError",
                        "expected: <5> but was: <4>"));
        final SeamOnDivergenceClassifier classifier = classifier(off, on, ledgerWith(temp));

        final String report = classifier.render(classifier.classify());

        assertThat(report)
                .as("when the lane is red the next reader's first question is what it actually said, and a "
                        + "verdict line cannot answer that")
                .contains("shouldProcessTheRecord")
                .contains("expected: <5> but was: <4>")
                .contains("unexplained divergences present");
    }

    // -------------------------------------------------------------------------------------------------

    private static SeamOnDivergenceClassifier classifier(final SurefireArm off,
                                                         final SurefireArm on,
                                                         final InflightMarkers ledger) {
        return new SeamOnDivergenceClassifier(off, on, DivergenceMechanism.registry(ledger));
    }

    private static InflightMarkers ledgerWith(final Path temp, final String... markers) throws IOException {
        final Path notes = Files.createDirectories(temp.resolve("ledger-" + markers.length));
        final StringBuilder note = new StringBuilder("# a synthetic note\n\n");
        for (final String marker : markers) {
            note.append(marker).append('\n');
        }
        Files.write(notes.resolve("test-synthetic.md"), note.toString().getBytes(StandardCharsets.UTF_8));
        return InflightMarkers.from(notes);
    }

    private static SurefireArm read(final Path temp,
                                    final String armName,
                                    final boolean dispatchEnabled,
                                    final String... cases) throws IOException {
        return SurefireArm.read(armName, writeArm(temp, armName, dispatchEnabled, cases), BUILD_STARTED);
    }

    private static Path writeArm(final Path temp,
                                 final String armName,
                                 final boolean dispatchEnabled,
                                 final String... cases) throws IOException {
        final Path directory = Files.createDirectories(temp.resolve(armName));
        Files.write(directory.resolve("TEST-" + KAFKA_CLASS + ".xml"),
                suite(KAFKA_CLASS, dispatchEnabled, cases).getBytes(StandardCharsets.UTF_8));
        for (final Path report : listReports(directory)) {
            Files.setLastModifiedTime(report, FileTime.from(BUILD_STARTED.plusSeconds(30)));
        }
        return directory;
    }

    private static List<Path> listReports(final Path directory) throws IOException {
        final List<Path> reports = new ArrayList<>();
        try (java.nio.file.DirectoryStream<Path> stream =
                     Files.newDirectoryStream(directory, "TEST-*.xml")) {
            for (final Path report : stream) {
                reports.add(report);
            }
        }
        return reports;
    }

    private static String suite(final String className, final boolean dispatchEnabled, final String... cases) {
        final StringBuilder xml = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        xml.append("<testsuite name=\"").append(className).append("\">\n");
        xml.append("  <properties>\n    <property name=\"")
                .append(SeamOnDivergenceClassifier.DISPATCH_PROPERTY)
                .append("\" value=\"").append(dispatchEnabled).append("\"/>\n  </properties>\n");
        for (final String testCase : cases) {
            xml.append(testCase.replace("${classname}", className));
        }
        return xml.append("</testsuite>\n").toString();
    }

    private static String passing(final String name) {
        return "  <testcase name=\"" + name + "\" classname=\"${classname}\"/>\n";
    }

    private static String failing(final String name, final String type, final String message) {
        return "  <testcase name=\"" + name + "\" classname=\"${classname}\">\n"
                + "    <failure type=\"" + escape(type) + "\" message=\"" + escape(message) + "\">"
                + escape(message) + "\n\tat some.Frame.method(Frame.java:1)</failure>\n"
                + "  </testcase>\n";
    }

    /**
     * Surefire escapes the attribute payload, so the fixtures have to as well - a fixture that is easier to
     * read than the thing it stands in for is a fixture that tests something else.
     */
    private static String escape(final String text) {
        return text.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;");
    }
}
