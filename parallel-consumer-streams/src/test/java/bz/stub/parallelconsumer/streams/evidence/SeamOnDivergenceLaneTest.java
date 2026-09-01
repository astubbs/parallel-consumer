package bz.stub.parallelconsumer.streams.evidence;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.format.DateTimeParseException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The gate on the seam-on measurement: every divergence Apache Kafka's own suite shows between the two arms
 * must be explained by a named mechanism, or this fails.
 * <p>
 * <b>It is deliberately excluded from the module's ordinary test run</b> (by name, in the surefire
 * {@code default-test} execution) and runs only in the {@code seam-on-evidence-classification} execution,
 * after the two Kafka executions have written the reports it reads. Run before them it could only report
 * their absence.
 * <p>
 * <b>It is a separate class from the run that produces its input, and that is the whole architecture.</b> The
 * seam-on execution carries {@code testFailureIgnore=true} because its failures are the measurement rather
 * than a regression - a suite that goes red on every build is a suite people stop reading. Making the
 * classification a different execution is what lets "these failures are expected" and "a new divergence fails
 * the build" both be true at once.
 * <p>
 * Reach it through {@code bin/ci-streams-seam-on-evidence.sh}, which deletes both report directories first -
 * a directory left over from an earlier run parses perfectly and reads as this run's result.
 *
 * @author Antony Stubbs
 * @see SeamOnDivergenceClassifier
 * @see SeamOnDivergenceClassifierTest
 */
@Slf4j
class SeamOnDivergenceLaneTest {

    private static final String SEAM_ON_REPORTS = "seam.on.reports.dir";

    private static final String SEAM_OFF_REPORTS = "seam.off.reports.dir";

    private static final String BUILD_STARTED_AT = "seam.on.evidence.buildStartedAt";

    /** Where the whole measurement is written, so a CI run has an artifact rather than only a verdict. */
    private static final String REPORT_FILE = "seam-on-evidence-report.txt";

    @Test
    void everyDivergenceBetweenTheTwoArmsIsExplainedByANamedMechanism() {
        final Instant buildStartedAt = requiredInstant();

        final SurefireArm seamOff = SurefireArm.read("seam-off", requiredDirectory(SEAM_OFF_REPORTS), buildStartedAt);
        final SurefireArm seamOn = SurefireArm.read("seam-on", requiredDirectory(SEAM_ON_REPORTS), buildStartedAt);

        final SeamOnDivergenceClassifier classifier = new SeamOnDivergenceClassifier(
                seamOff, seamOn, DivergenceMechanism.registry(InflightMarkers.load()));
        classifier.assertBothArmsRanUnderTheirOwnPin();
        classifier.assertBothArmsCoveredTheSameClasses();

        final SeamOnDivergenceClassifier.Result result = classifier.classify();
        final String report = classifier.render(result);
        log.info("\n{}", report);
        write(report);

        // Asserted BEFORE the divergences, and the order is the point: an undiagnosed failure in the control
        // arm means the set difference below was taken against something nobody trusts, so reporting the
        // divergence set first would be reporting a number computed from a broken input.
        assertThat(result.getUnexplainedControlArmFailures())
                .as("The seam-OFF arm is the control, and it must be clean or its failures must be diagnosed "
                        + "in the inflight ledger with a flaky-case marker. Anything else and there is "
                        + "nothing trustworthy to difference the seam-on arm against. Re-running until the "
                        + "control is green is what this ledger exists to replace - a retry destroys the "
                        + "sighting, an entry relocates it. Full report in target/%s.", REPORT_FILE)
                .isEmpty();

        assertThat(result.getUnexplained())
                .as("Every case that passes with the seam OFF and fails with it ON must be explained by a "
                        + "named mechanism. The ones listed here are not - each is either a mechanism nobody "
                        + "has named yet, or a regression in the dispatch path. The full report, including "
                        + "the recorded failure of each, is in target/%s. Do not silence one by widening a "
                        + "mechanism's predicate until it swallows everything: the check on a predicate's "
                        + "breadth is that sabotaging a semantic still lands here.", REPORT_FILE)
                .isEmpty();
    }

    private static Path requiredDirectory(final String property) {
        final String value = System.getProperty(property);
        if (value == null || value.isEmpty()) {
            throw new IllegalStateException("-D" + property + " was not set, so this lane does not know which "
                    + "report directory to read. It is set by the seam-on-evidence-classification execution "
                    + "in parallel-consumer-streams/pom.xml; running this class outside that execution is "
                    + "what leaves it unset.");
        }
        return Paths.get(value);
    }

    /**
     * The build's own start time, used to reject a report directory written by an earlier run.
     * <p>
     * Required rather than optional: an absent timestamp would turn the staleness check off, and a check that
     * turns itself off when its input is missing is the failure mode this whole lane exists to avoid.
     */
    private static Instant requiredInstant() {
        final String value = System.getProperty(BUILD_STARTED_AT);
        if (value == null || value.isEmpty()) {
            throw new IllegalStateException("-D" + BUILD_STARTED_AT + " was not set. It carries ${maven.build."
                    + "timestamp} so a report directory older than this build can be rejected; without it "
                    + "the staleness check would be off, and a stale directory reads as a clean pass.");
        }
        try {
            return Instant.parse(value);
        } catch (final DateTimeParseException e) {
            throw new IllegalStateException("-D" + BUILD_STARTED_AT + "=" + value + " is not an ISO instant. "
                    + "The module pins maven.build.timestamp.format for exactly this reason; if that property "
                    + "moved, this parse has to move with it.", e);
        }
    }

    private static void write(final String report) {
        final Path target = Paths.get("target").resolve(REPORT_FILE);
        try {
            Files.createDirectories(target.getParent());
            Files.write(target, report.getBytes(StandardCharsets.UTF_8));
            log.info("Seam-on evidence report written to {}", target.toAbsolutePath());
        } catch (final IOException e) {
            throw new UncheckedIOException("Could not write the seam-on evidence report to " + target, e);
        }
    }
}
