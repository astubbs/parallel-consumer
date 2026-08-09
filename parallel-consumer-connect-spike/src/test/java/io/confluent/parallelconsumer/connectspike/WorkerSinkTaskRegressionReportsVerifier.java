package io.confluent.parallelconsumer.connectspike;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Compares the two isolated upstream-test report arms against the checked exact manifest. */
final class WorkerSinkTaskRegressionReportsVerifier {

    static final String UPSTREAM_TEST_CLASS = "org.apache.kafka.connect.runtime.WorkerSinkTaskTest";
    static final int EXPECTED_TEST_COUNT = 30;

    private WorkerSinkTaskRegressionReportsVerifier() {
    }

    static void verify(Path manifest, Path stockReports, Path patchedReports) throws Exception {
        Set<String> expected = readManifest(manifest);
        if (expected.size() != EXPECTED_TEST_COUNT) {
            throw new IllegalStateException("checked WorkerSinkTaskTest manifest must contain exactly "
                    + EXPECTED_TEST_COUNT + " unique tests, but contained " + expected.size());
        }

        Map<String, String> stock = readOutcomes("stock", stockReports);
        Map<String, String> patched = readOutcomes("patched-disabled", patchedReports);

        requireExactIdentities("stock", expected, stock.keySet());
        requireExactIdentities("patched-disabled", expected, patched.keySet());
        if (!stock.equals(patched)) {
            throw new IllegalStateException("stock and patched-disabled WorkerSinkTaskTest outcomes differ: stock="
                    + stock + ", patched-disabled=" + patched);
        }

        List<String> nonPassing = new ArrayList<>();
        for (Map.Entry<String, String> outcome : stock.entrySet()) {
            if (!"passed".equals(outcome.getValue())) {
                nonPassing.add(outcome.getKey() + "=" + outcome.getValue());
            }
        }
        if (!nonPassing.isEmpty()) {
            throw new IllegalStateException("WorkerSinkTaskTest regression arms must pass every manifest test: "
                    + nonPassing);
        }
    }

    private static Set<String> readManifest(Path manifest) throws IOException {
        if (!Files.isRegularFile(manifest)) {
            throw new IllegalStateException("missing checked WorkerSinkTaskTest manifest: " + manifest);
        }

        Set<String> tests = new LinkedHashSet<>();
        for (String rawLine : Files.readAllLines(manifest, StandardCharsets.UTF_8)) {
            String line = rawLine.trim();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            if (!line.startsWith(UPSTREAM_TEST_CLASS + "#")) {
                throw new IllegalStateException("manifest entry is not a WorkerSinkTaskTest identity: " + line);
            }
            if (!tests.add(line)) {
                throw new IllegalStateException("duplicate WorkerSinkTaskTest manifest entry: " + line);
            }
        }
        if (tests.isEmpty()) {
            throw new IllegalStateException("checked WorkerSinkTaskTest manifest is empty: " + manifest);
        }
        return Collections.unmodifiableSet(tests);
    }

    private static Map<String, String> readOutcomes(String arm, Path reportsDirectory) throws Exception {
        if (!Files.isDirectory(reportsDirectory)) {
            throw new IllegalStateException(arm + " report directory is missing: " + reportsDirectory);
        }

        Map<String, String> outcomes = new LinkedHashMap<>();
        try (DirectoryStream<Path> reports = Files.newDirectoryStream(reportsDirectory, "TEST-*.xml")) {
            for (Path report : reports) {
                Document document = newDocumentBuilderFactory().newDocumentBuilder().parse(report.toFile());
                NodeList cases = document.getElementsByTagName("testcase");
                for (int index = 0; index < cases.getLength(); index++) {
                    Element testCase = (Element) cases.item(index);
                    if (!UPSTREAM_TEST_CLASS.equals(testCase.getAttribute("classname"))) {
                        continue;
                    }

                    String identity = UPSTREAM_TEST_CLASS + "#" + testCase.getAttribute("name");
                    String previous = outcomes.put(identity, outcomeOf(testCase));
                    if (previous != null) {
                        throw new IllegalStateException(arm + " reported duplicate test identity " + identity);
                    }
                }
            }
        }
        if (outcomes.isEmpty()) {
            throw new IllegalStateException(arm + " reports discovered no WorkerSinkTaskTest cases in "
                    + reportsDirectory + "; zero-test discovery is never a passing control arm");
        }
        return outcomes;
    }

    private static DocumentBuilderFactory newDocumentBuilderFactory() throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        factory.setXIncludeAware(false);
        factory.setExpandEntityReferences(false);
        return factory;
    }

    private static String outcomeOf(Element testCase) {
        if (testCase.getElementsByTagName("failure").getLength() > 0) {
            return "failed";
        }
        if (testCase.getElementsByTagName("error").getLength() > 0) {
            return "errored";
        }
        if (testCase.getElementsByTagName("skipped").getLength() > 0) {
            return "skipped";
        }
        return "passed";
    }

    private static void requireExactIdentities(String arm, Set<String> expected, Set<String> actual) {
        if (expected.equals(actual)) {
            return;
        }

        Set<String> missing = new LinkedHashSet<>(expected);
        missing.removeAll(actual);
        Set<String> unexpected = new LinkedHashSet<>(actual);
        unexpected.removeAll(expected);
        throw new IllegalStateException(arm + " WorkerSinkTaskTest identities differ from the checked manifest; "
                + "missing=" + missing + ", unexpected=" + unexpected);
    }
}
