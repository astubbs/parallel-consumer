package bz.stub.parallelconsumer.streams.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;

/**
 * The two fixture moves every test class in this module makes: writing YAML case documents into a directory and
 * loading them through {@link CaseLoader}, and picking one loaded case out of a corpus by name.
 * <p>
 * <b>Fixtures go through the real loader, never a test-only builder.</b> Every case a test asserts over is therefore
 * a case the corpus could hold; a hand-built {@link ConformanceCase} would let a test make claims about a shape the
 * loader refuses. The one deliberate exception is {@code CorpusCoverageTest}'s no-input-record case, which exists
 * precisely because the loader refuses that shape and the coverage gate's second line of defence still has to be
 * reddenable.
 * <p>
 * <b>Not named {@code *Test}</b>, so surefire does not collect it - it holds no test methods, and a name matching
 * surefire's default includes would report an empty class as a run one.
 */
final class CorpusFixtures {

    private CorpusFixtures() {
    }

    /**
     * Writes each YAML document into its own file under one directory and loads the lot, so every fixture is a real
     * case rather than a hand-built object.
     *
     * @param directory     the directory to write into; created if it does not exist, so a {@code @TempDir}
     *                      sub-path works
     * @param yamlDocuments one case document per file, in the order they are written
     * @return the loaded corpus, ordered as {@link CaseLoader#load} orders it - by case name, not by file
     * @throws CaseLoader.CorpusRefusedException if the loader refuses any of them, which is what several tests are
     *                                           actually asserting
     */
    static List<ConformanceCase> load(Path directory, String... yamlDocuments) {
        try {
            Path created = Files.createDirectories(directory);
            for (int index = 0; index < yamlDocuments.length; index++) {
                Path written = Files.write(created.resolve("case-" + index + ".yaml"),
                        yamlDocuments[index].getBytes(StandardCharsets.UTF_8));
                assertThat(Files.exists(written)).isTrue();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("cannot write the hand-built corpus into " + directory, e);
        }
        return CaseLoader.load(directory);
    }

    /**
     * {@link #load} for the common single-document case, asserting that exactly one case came back - so a fixture
     * that silently loaded as two cases cannot be asserted over as if it were one.
     */
    static ConformanceCase loadOne(Path directory, String yamlDocument) {
        List<ConformanceCase> loaded = load(directory, yamlDocument);
        assertThat(loaded).hasSize(1);
        return loaded.get(0);
    }

    /**
     * The case a corpus holds under {@code name}.
     *
     * @throws AssertionError if the corpus holds no such case - naming what it does hold, because a test that picks
     *                        a case out by name has usually gone wrong by naming the wrong one
     */
    static ConformanceCase caseNamed(List<ConformanceCase> corpus, String name) {
        return corpus.stream()
                .filter(candidate -> name.equals(candidate.name()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no case named " + name + " in " + corpus));
    }
}
