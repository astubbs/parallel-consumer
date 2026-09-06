package bz.stub.parallelconsumer.streams.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * One test per load-time rule, each named for the rule it holds.
 * <p>
 * Every refusal assertion checks that the message <strong>names the case</strong>, and, where the rule is about a
 * particular part of a case, names that too - the field, the handle id, the record, or the file. That is the whole
 * point of the rules: a corpus that fails to load has to say which case a maintainer must open, or the loader has
 * only replaced one silent failure with another.
 * <p>
 * The fixtures live under {@code src/test/resources/invalid-cases/<rule>/}, one directory per rule, because a rule
 * that spans two files (the duplicate name) needs a directory of its own and because a directory per rule keeps each
 * refusal isolated - a fixture tripping two rules at once would let one of them rot untested.
 */
class CaseLoaderTest {

    private static final String CORPUS = "cases";

    @Test
    void aWellFormedCaseLoadsWithEveryFieldPopulatedAndNoEmitRule() {
        ConformanceCase loaded = caseNamed(CaseLoader.loadClasspathDirectory(CORPUS), "hopping-count-by-key");

        assertThat(loaded.baseInstant()).isEqualTo(Instant.parse("2025-01-01T02:00:00Z"));
        assertThat(kindsOf(loaded)).containsExactly(
                ConformanceCase.OperationKind.SOURCE,
                ConformanceCase.OperationKind.GROUP_BY_KEY,
                ConformanceCase.OperationKind.WINDOWED_BY,
                ConformanceCase.OperationKind.COUNT,
                ConformanceCase.OperationKind.TO_STREAM,
                ConformanceCase.OperationKind.SINK).inOrder();

        ConformanceCase.Operation windowedBy = operation(loaded, ConformanceCase.OperationKind.WINDOWED_BY);
        assertThat(windowedBy.inputs()).containsExactly("g");
        ConformanceCase.WindowSpec window = requireNonNull(windowedBy.window());
        assertThat(window.sizeMs()).isEqualTo(3_600_000L);
        assertThat(window.advanceMs()).isEqualTo(300_000L);
        assertThat(window.graceMs()).isEqualTo(0L);
        assertThat(window.retentionMs()).isEqualTo(3_600_000L);

        assertThat(operation(loaded, ConformanceCase.OperationKind.COUNT).store()).isEqualTo("counts");
        assertThat(operation(loaded, ConformanceCase.OperationKind.SINK).topic()).isEqualTo("out");
        // A sink mints no handle, mirroring the wire's one presence signal for "a handle was minted".
        assertThat(operation(loaded, ConformanceCase.OperationKind.SINK).id()).isNull();

        assertThat(loaded.inputs()).hasSize(3);
        assertThat(loaded.inputs().get(1).key()).isEqualTo("a");
        assertThat(loaded.inputs().get(1).atMs()).isEqualTo(60_000L);
        assertThat(loaded.inputs().get(1).timestamp()).isEqualTo(Instant.parse("2025-01-01T02:01:00Z"));
        assertThat(loaded.perturbation()).hasSize(3);
        assertThat(loaded.perturbation().get(0).key()).isEqualTo("b");

        assertThat(loaded.agreement()).isEqualTo(ConformanceCase.Agreement.FINAL_STATE);
        assertThat(loaded.emit()).isNull();
        assertThat(loaded.callLog()).isNull();
        assertThat(loaded.refusalClass()).isFalse();
        assertThat(loaded.expectsFault()).isNull();
    }

    @Test
    void aRefusalClassCaseLoadsFlaggedNeverExecutedWithItsFaultName() {
        ConformanceCase loaded =
                caseNamed(CaseLoader.loadClasspathDirectory(CORPUS), "aggregate-names-function-and-combine");

        assertThat(loaded.refusalClass()).isTrue();
        assertThat(loaded.expectsFault()).isEqualTo("aggregate-names-function-and-combine");
        // It carries a specification to refuse, and no outcome to compute: no inputs, no twin, nothing to run.
        assertThat(loaded.inputs()).isEmpty();
        assertThat(loaded.perturbation()).isEmpty();
        assertThat(operation(loaded, ConformanceCase.OperationKind.AGGREGATE).combine()).isEqualTo("last-bytes");
    }

    /**
     * Covers AE5. Naming an emit rule does not buy the level: it is refused either way, as not yet implemented,
     * rather than loaded and silently compared at final state alone.
     */
    @Test
    void theUpdatesAgreementLevelIsRefusedByNameWithOrWithoutAnEmitRule() {
        String withoutEmit = refusalsFrom("invalid-cases/agreement-updates");
        assertThat(withoutEmit).contains("agreement-updates");
        assertThat(withoutEmit).contains("final-state+updates");
        assertThat(withoutEmit).contains("not yet implemented on this rung");

        String withEmit = refusalsFrom("invalid-cases/agreement-updates-with-emit");
        assertThat(withEmit).contains("agreement-updates-with-emit");
        assertThat(withEmit).contains("final-state+updates");
        assertThat(withEmit).contains("not yet implemented on this rung");
    }

    @Test
    void anOperationNamingAnUndeclaredOrSelfJoinedHandleIsRefusedNamingTheId() {
        String undeclared = refusalsFrom("invalid-cases/unknown-handle");
        assertThat(undeclared).contains("unknown-handle");
        assertThat(undeclared).contains("nope");

        String noInput = refusalsFrom("invalid-cases/missing-input");
        assertThat(noInput).contains("missing-input");
        assertThat(noInput).contains("g");

        String selfJoin = refusalsFrom("invalid-cases/self-join");
        assertThat(selfJoin).contains("self-join");
        assertThat(selfJoin).contains("s1");
    }

    @Test
    void aWindowedByMissingRetentionIsRefusedNamingTheField() {
        String refusals = refusalsFrom("invalid-cases/window-missing-retention");

        assertThat(refusals).contains("window-missing-retention");
        assertThat(refusals).contains("retention-ms");
    }

    @Test
    void aFunctionOutsideTheVocabularyIsRefusedNamingTheCaseAndTheName() {
        String refusals = refusalsFrom("invalid-cases/unknown-function");

        assertThat(refusals).contains("unknown-function");
        assertThat(refusals).contains("shout");
        assertThat(refusals).contains("map-values");
    }

    @Test
    void aPinnedEmitCaseWithNoRecordPastWindowCloseIsRefused() {
        String refusals = refusalsFrom("invalid-cases/pinned-emit-no-trailing-record");
        assertThat(refusals).contains("pinned-emit-no-trailing-record");
        assertThat(refusals).contains("on-window-close");

        // The same case with a record past the close of the last window an earlier record could open loads, so the
        // rule is a real discrimination and not a blanket refusal of pinned emit.
        List<ConformanceCase> loaded =
                CaseLoader.loadClasspathDirectory("valid-variants/pinned-emit-with-trailing-record");
        assertThat(caseNamed(loaded, "pinned-emit-with-trailing-record").emit())
                .isEqualTo(ConformanceCase.EmitRule.ON_WINDOW_CLOSE);
    }

    /** Covers AE8. A stateless case cannot pass by observing nothing. */
    @Test
    void aTopologyWithNoStoreAndNoSinkIsRefusedAndTheSameTopologyWithASinkLoads() {
        String refusals = refusalsFrom("invalid-cases/no-final-state");
        assertThat(refusals).contains("no-final-state");
        assertThat(refusals).contains("final state");

        List<ConformanceCase> loaded = CaseLoader.loadClasspathDirectory("valid-variants/no-final-state-plus-sink");
        ConformanceCase withSink = caseNamed(loaded, "no-final-state-plus-sink");
        assertThat(operation(withSink, ConformanceCase.OperationKind.SINK).topic()).isEqualTo("out");
    }

    @Test
    void anInputRecordWithNoTimestampIsRefusedNamingTheRecord() {
        String refusals = refusalsFrom("invalid-cases/missing-timestamp");

        assertThat(refusals).contains("missing-timestamp");
        assertThat(refusals).contains("at-ms");
        // The second record is the one without a timestamp, and the message has to say which.
        assertThat(refusals).contains("input record 2");
    }

    @Test
    void twoFilesDeclaringOneCaseNameAreRefusedNamingBothFiles() {
        String refusals = refusalsFrom("invalid-cases/duplicate-name");

        assertThat(refusals).contains("twice-declared");
        assertThat(refusals).contains("first.yaml");
        assertThat(refusals).contains("second.yaml");
    }

    /**
     * An empty corpus is not this loader's failure to raise. U4's gate treats an empty corpus as a red, because that
     * is where "the run executed nothing and reported green" is actually decidable; here it is just an empty list.
     */
    @Test
    void anEmptyDirectoryYieldsAnEmptyCorpus(@TempDir Path emptyDirectory) {
        assertThat(CaseLoader.load(emptyDirectory)).isEmpty();
    }

    @Test
    void anUnparseableFileIsRefusedNamingItAndDoesNotAbortTheRest() {
        CaseLoader.CorpusRefusedException refused = refusal("invalid-cases/unparseable");

        assertThat(refused.refusals()).hasSize(1);
        assertThat(refused.refusals().get(0)).contains("broken.yaml");
        // The sound file beside it was still read: one bad file refuses the corpus, it does not stop the pass.
        assertThat(refused.loadedCaseNames()).containsExactly("sound-neighbour");
    }

    /** Every refusal the corpus produced, as one string - what the assertions above look for names in. */
    private static String refusalsFrom(String resourceDirectory) {
        return String.join(System.lineSeparator(), refusal(resourceDirectory).refusals());
    }

    private static CaseLoader.CorpusRefusedException refusal(String resourceDirectory) {
        try {
            List<ConformanceCase> wronglyLoaded = CaseLoader.loadClasspathDirectory(resourceDirectory);
            fail("expected " + resourceDirectory + " to be refused, but it loaded " + wronglyLoaded);
            throw new AssertionError("unreachable");
        } catch (CaseLoader.CorpusRefusedException expected) {
            return expected;
        }
    }

    private static ConformanceCase caseNamed(List<ConformanceCase> corpus, String name) {
        return corpus.stream()
                .filter(c -> name.equals(c.name()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no case named " + name + " in " + corpus));
    }

    private static List<ConformanceCase.OperationKind> kindsOf(ConformanceCase loaded) {
        return loaded.topology().stream()
                .map(ConformanceCase.Operation::kind)
                .collect(java.util.stream.Collectors.toList());
    }

    private static ConformanceCase.Operation operation(ConformanceCase loaded, ConformanceCase.OperationKind kind) {
        return loaded.topology().stream()
                .filter(op -> op.kind() == kind)
                .findFirst()
                .orElseThrow(() -> new AssertionError("no " + kind.spelling() + " in " + loaded));
    }
}
