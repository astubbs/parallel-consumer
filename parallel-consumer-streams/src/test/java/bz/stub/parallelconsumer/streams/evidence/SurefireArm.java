package bz.stub.parallelconsumer.streams.evidence;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * One arm of the seam-on evidence lane: everything a surefire report directory records about a run.
 * <p>
 * <b>Reading a report directory is where this lane can most easily lie to itself, so the reading is where the
 * checks are.</b> A directory left behind by an earlier run parses perfectly and reads as this run's result; a
 * directory that was never written because the execution was skipped is an empty glob, and an empty glob
 * summed over "how many failures" is zero, which is indistinguishable from a clean pass. Both cases throw here
 * rather than returning an empty arm.
 * <p>
 * The system properties surefire records in each report are read too, and they are what proves the arm was
 * pinned the way its name claims - see {@link #getSystemProperty(String)}. Nothing else in this module can
 * establish that after the fact: the arm ran in a forked JVM that no longer exists.
 *
 * @author Antony Stubbs
 * @see SeamOnDivergenceClassifier
 */
@Getter
public final class SurefireArm {

    /**
     * What surefire recorded for one test case.
     * <p>
     * {@code name} keeps the parameterisation surefire wrote ({@code shouldX[3]}), because two parameters of
     * one method routinely differ in outcome across the two arms and collapsing them would difference the
     * wrong things against each other.
     */
    @RequiredArgsConstructor
    @Getter
    public static final class Case {

        private final String className;

        private final String name;

        private final Outcome outcome;

        /** The exception class surefire recorded, or the empty string for a case that did not fail. */
        private final String failureType;

        /** The failure message, or the empty string. Never null, so a predicate never has to null-check. */
        private final String failureMessage;

        /** Message plus stack trace: everything recorded about the failure, in one searchable string. */
        private final String failureDetail;

        public String getId() {
            return className + "#" + name;
        }

        public boolean isFailed() {
            return outcome == Outcome.FAILED || outcome == Outcome.ERRORED;
        }
    }

    public enum Outcome {
        PASSED, FAILED, ERRORED, SKIPPED
    }

    /** Which arm this is, for messages: the words a reader sees when the lane reports a difference. */
    private final String armName;

    private final Path reportsDirectory;

    /** Insertion-ordered, keyed by {@link Case#getId()} - so a lookup across arms is by identity, not index. */
    private final Map<String, Case> casesById;

    private final Map<String, String> systemProperties;

    private SurefireArm(final String armName,
                        final Path reportsDirectory,
                        final Map<String, Case> casesById,
                        final Map<String, String> systemProperties) {
        this.armName = armName;
        this.reportsDirectory = reportsDirectory;
        this.casesById = Collections.unmodifiableMap(casesById);
        this.systemProperties = Collections.unmodifiableMap(systemProperties);
    }

    /**
     * Read a report directory, refusing every way it could be read as a pass without having measured anything.
     *
     * @param notOlderThan the instant the build started. Every report file must be at least this new; one that
     *                     is older is a directory left behind by an earlier run, which parses perfectly and
     *                     says nothing about this one. Pass {@code null} only from a caller that has some other
     *                     proof of freshness - and there is currently no such caller.
     * @throws IllegalStateException if the directory is missing, holds no reports, or holds a stale one
     */
    public static SurefireArm read(final String armName, final Path reportsDirectory, final Instant notOlderThan) {
        if (!Files.isDirectory(reportsDirectory)) {
            throw new IllegalStateException("The " + armName + " arm has no report directory at "
                    + reportsDirectory + ". That is an ERROR and not an empty result: the execution that "
                    + "writes it was skipped, or the directory was deleted after it ran. Run the lane through "
                    + "bin/ci-streams-seam-on-evidence.sh, which deletes both directories and then produces "
                    + "both of them.");
        }

        final List<Path> reports = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(reportsDirectory, "TEST-*.xml")) {
            for (final Path report : stream) {
                reports.add(report);
            }
        } catch (final IOException e) {
            throw new UncheckedIOException("Could not list the " + armName + " arm's reports", e);
        }
        if (reports.isEmpty()) {
            throw new IllegalStateException("The " + armName + " arm's report directory " + reportsDirectory
                    + " holds no TEST-*.xml. An empty glob summed over 'how many failed' is zero, which reads "
                    + "exactly like a clean pass - so this is an error. The usual cause is scoping the run "
                    + "with -Dtest=, which silently overrides the execution's own <includes> and runs nothing.");
        }
        Collections.sort(reports);

        final Map<String, Case> cases = new LinkedHashMap<>();
        final Map<String, String> properties = new LinkedHashMap<>();
        for (final Path report : reports) {
            assertFresh(armName, report, notOlderThan);
            parse(armName, report, cases, properties);
        }
        return new SurefireArm(armName, reportsDirectory, cases, properties);
    }

    private static void assertFresh(final String armName, final Path report, final Instant notOlderThan) {
        if (notOlderThan == null) {
            return;
        }
        final Instant written;
        try {
            written = Files.getLastModifiedTime(report).toInstant();
        } catch (final IOException e) {
            throw new UncheckedIOException("Could not read the modification time of " + report, e);
        }
        // A second of slack: the build timestamp has second resolution, so a report written in the same
        // second as the build started can round to just before it.
        if (written.isBefore(notOlderThan.minusSeconds(1))) {
            throw new IllegalStateException("The " + armName + " arm's report " + report + " was written at "
                    + written + ", before this build started at " + notOlderThan + ". It is an earlier run's "
                    + "result and reads as this one's. Delete the report directories before the run - "
                    + "bin/ci-streams-seam-on-evidence.sh does.");
        }
    }

    private static void parse(final String armName,
                              final Path report,
                              final Map<String, Case> cases,
                              final Map<String, String> properties) {
        final Document document;
        try {
            final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            // Surefire writes no DOCTYPE and no external entities; refusing them keeps this parser from
            // being a file-read primitive if one ever appears.
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            factory.setExpandEntityReferences(false);
            final DocumentBuilder builder = factory.newDocumentBuilder();
            document = builder.parse(report.toFile());
        } catch (final Exception e) {
            throw new IllegalStateException("The " + armName + " arm's report " + report + " could not be "
                    + "parsed. A truncated report means the fork died mid-run, which is a result to look at "
                    + "rather than one to skip past.", e);
        }

        final NodeList propertyNodes = document.getElementsByTagName("property");
        for (int i = 0; i < propertyNodes.getLength(); i++) {
            final Element property = (Element) propertyNodes.item(i);
            properties.putIfAbsent(property.getAttribute("name"), property.getAttribute("value"));
        }

        final NodeList testcases = document.getElementsByTagName("testcase");
        for (int i = 0; i < testcases.getLength(); i++) {
            final Element testcase = (Element) testcases.item(i);
            final Case parsed = toCase(testcase);
            cases.put(parsed.getId(), parsed);
        }
    }

    private static Case toCase(final Element testcase) {
        final String className = testcase.getAttribute("classname");
        final String name = testcase.getAttribute("name");

        final Element failure = firstChildElement(testcase, "failure");
        final Element error = firstChildElement(testcase, "error");
        final Element skipped = firstChildElement(testcase, "skipped");

        if (failure == null && error == null) {
            final Outcome outcome = skipped == null ? Outcome.PASSED : Outcome.SKIPPED;
            return new Case(className, name, outcome, "", "", "");
        }

        final Element recorded = failure != null ? failure : error;
        final Outcome outcome = failure != null ? Outcome.FAILED : Outcome.ERRORED;
        final String type = recorded.getAttribute("type");
        final String message = recorded.getAttribute("message");
        final String detail = message + "\n" + recorded.getTextContent();
        return new Case(className, name, outcome, type, message, detail);
    }

    private static Element firstChildElement(final Element parent, final String tagName) {
        final NodeList children = parent.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            final Node child = children.item(i);
            if (child.getNodeType() == Node.ELEMENT_NODE && tagName.equals(child.getNodeName())) {
                return (Element) child;
            }
        }
        return null;
    }

    public List<Case> getCases() {
        return new ArrayList<>(casesById.values());
    }

    public Case find(final String id) {
        return casesById.get(id);
    }

    /** The test classes this arm covered, so the classifier can refuse to difference arms of different shape. */
    public Set<String> getClassNames() {
        final Set<String> names = new TreeSet<>();
        for (final Case testCase : casesById.values()) {
            names.add(testCase.getClassName());
        }
        return names;
    }

    /**
     * A system property as the forked JVM actually saw it, read back out of the report surefire wrote.
     * <p>
     * This is the only way left to prove an arm was pinned the way its name claims: the fork is gone, and a
     * pin that silently failed to arrive produces a plausible run of the wrong thing.
     */
    public String getSystemProperty(final String name) {
        return systemProperties.get(name);
    }

    /** For messages: a directory a reader can go and look at. */
    public String getReportsDirectoryDisplay() {
        return reportsDirectory.toAbsolutePath().toString().replace(File.separatorChar, '/');
    }
}
