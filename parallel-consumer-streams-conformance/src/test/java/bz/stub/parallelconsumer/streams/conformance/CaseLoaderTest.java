package bz.stub.parallelconsumer.streams.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static bz.stub.parallelconsumer.streams.conformance.CorpusFixtures.caseNamed;
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
        assertThat(window.retentionMs()).isEqualTo(10_800_000L);

        assertThat(operation(loaded, ConformanceCase.OperationKind.COUNT).store()).isEqualTo("counts");
        assertThat(operation(loaded, ConformanceCase.OperationKind.SINK).topic()).isEqualTo("out");
        // A sink mints no handle, mirroring the wire's one presence signal for "a handle was minted".
        assertThat(operation(loaded, ConformanceCase.OperationKind.SINK).id()).isNull();

        assertThat(loaded.inputs()).hasSize(3);
        assertThat(loaded.inputs().get(1).key()).isEqualTo("a");
        assertThat(loaded.inputs().get(1).atMs()).isEqualTo(60_000L);
        assertThat(loaded.inputs().get(1).timestamp()).isEqualTo(Instant.parse("2025-01-01T02:01:00Z"));
        // The topology declares one source, so a record naming no topic takes that one - and the loaded record
        // carries the resolved topic, never the absence, because the oracle pipes what the case resolved to.
        assertThat(loaded.inputs().get(1).topic()).isEqualTo("in");
        assertThat(loaded.perturbation()).hasSize(3);
        assertThat(loaded.perturbation().get(0).key()).isEqualTo("b");
        assertThat(loaded.perturbation().get(0).topic()).isEqualTo("in");

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
        // The fault is spelt in the wire's vocabulary rather than the case's own name: the proto's Aggregate names
        // function_token and combine as alternatives, and refuses a call carrying both.
        assertThat(loaded.expectsFault()).isEqualTo("aggregate-carries-both-function-token-and-combine");
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
        CaseLoader.CorpusRefusedException refused = refusal("invalid-cases/pinned-emit-no-trailing-record");

        // One rule, one refusal: the fixture's perturbation carries the trailing record its inputs lack, so the
        // twin's half of the rule cannot fire here and rot untested.
        assertThat(refused.refusals()).hasSize(1);
        String refusals = String.join(System.lineSeparator(), refused.refusals());
        assertThat(refusals).contains("pinned-emit-no-trailing-record");
        assertThat(refusals).contains("on-window-close");
        assertThat(refusals).contains("input records");

        // The same case with a record past the close of the last window an earlier record could open loads, so the
        // rule is a real discrimination and not a blanket refusal of pinned emit.
        List<ConformanceCase> loaded =
                CaseLoader.loadClasspathDirectory("valid-variants/pinned-emit-with-trailing-record");
        assertThat(caseNamed(loaded, "pinned-emit-with-trailing-record").emit())
                .isEqualTo(ConformanceCase.EmitRule.ON_WINDOW_CLOSE);
    }

    /**
     * The twin's half of KTD5. {@link Oracle#runPerturbation} drives the perturbation through the identical
     * suppressed topology, so a twin with no trailing record emits nothing at all - and the positive control would
     * then fire on that absence, reporting a green arm that measured the emit rule rather than the perturbation.
     * The refusal has to say <em>which</em> list is short, because the two lists are fixed in different places.
     */
    @Test
    void aPinnedEmitCaseWhoseTwinHasNoRecordPastWindowCloseIsRefusedNamingThePerturbation() {
        CaseLoader.CorpusRefusedException refused = refusal("invalid-cases/pinned-emit-twin-no-trailing-record");

        assertThat(refused.refusals()).hasSize(1);
        String refusals = String.join(System.lineSeparator(), refused.refusals());
        assertThat(refusals).contains("pinned-emit-twin-no-trailing-record");
        assertThat(refusals).contains("on-window-close");
        // The inputs here are sound; naming the list is the whole difference between a fixable message and a hunt.
        assertThat(refusals).contains("perturbation");
    }

    /**
     * A file the loader skips is a case nobody runs, and the corpus stays green one case smaller - the silent shape
     * this rung exists to refuse. So every regular file in a case directory is either a {@code *.yaml} case or the
     * corpus {@code README.md}, and anything else is refused by name.
     */
    @Test
    void aCaseSavedWithTheWrongExtensionIsRefusedNamingTheFileAndTheRequiredOne() {
        CaseLoader.CorpusRefusedException refused = refusal("invalid-cases/wrong-extension");

        assertThat(refused.refusals()).hasSize(1);
        assertThat(refused.refusals().get(0)).contains("case.yml");
        assertThat(refused.refusals().get(0)).contains(".yaml");
        // The .yaml file beside it was still read: a mis-named file refuses the corpus, it does not stop the pass.
        assertThat(refused.loadedCaseNames()).containsExactly("sound-neighbour-of-the-mis-extended");
    }

    /**
     * {@code expects-fault: ""} is a case that names its class and then names nothing. It is refused rather than
     * silently loaded as an outcome case, because the two readings of the field disagree - the loader's
     * "is the fault non-blank" and {@link ConformanceCase#refusalClass()}'s "is the fault present" - and a case that
     * loads under one reading and reports the other is a case nobody can act on.
     */
    @Test
    void aBlankExpectsFaultIsRefusedByNameRatherThanLoadingAsAnOutcomeCase() {
        CaseLoader.CorpusRefusedException refused = refusal("invalid-cases/blank-expects-fault");

        // Exactly one: the blank field is the fault to report, not the missing inputs and twin that follow from it.
        assertThat(refused.refusals()).hasSize(1);
        assertThat(refused.refusals().get(0)).contains("blank-expects-fault");
        assertThat(refused.refusals().get(0)).contains("expects-fault");
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

    /**
     * The multi-source half of the topic rule. With one source a record's topic defaults to it; with more than one
     * there is no obvious default, and inventing one would silently decide which side of a join a record feeds - so
     * every record must name a topic, and the refusal has to say which record omitted it.
     */
    @Test
    void aRecordNamingNoTopicUnderTwoSourcesIsRefusedNamingTheRecord() {
        String refusals = refusalsFrom("invalid-cases/record-without-topic");

        assertThat(refusals).contains("record-without-topic");
        assertThat(refusals).contains("topic");
        // The second input record is the one that omits it, and the message has to say which.
        assertThat(refusals).contains("input record 2");

        // The same shape with every record naming a topic loads, so the rule discriminates rather than refusing
        // every multi-source case outright.
        List<ConformanceCase> loaded = CaseLoader.loadClasspathDirectory("valid-variants/multi-source-named-topics");
        ConformanceCase named = caseNamed(loaded, "multi-source-named-topics");
        assertThat(named.inputs().get(0).topic()).isEqualTo("left");
        assertThat(named.inputs().get(1).topic()).isEqualTo("right");
        assertThat(named.perturbation().get(1).topic()).isEqualTo("right");
    }

    /**
     * A record piped to a topic no source reads is a record that vanishes - the silent shape this rung exists to
     * refuse - so the topic is checked against the declared sources even when one source would have supplied a
     * default.
     */
    @Test
    void aRecordNamingATopicNoSourceDeclaresIsRefusedNamingTheTopic() {
        String refusals = refusalsFrom("invalid-cases/record-topic-not-declared");

        assertThat(refusals).contains("record-topic-not-declared");
        assertThat(refusals).contains("elsewhere");
    }

    @Test
    void twoFilesDeclaringOneCaseNameAreRefusedNamingBothFiles() {
        String refusals = refusalsFrom("invalid-cases/duplicate-name");

        assertThat(refusals).contains("twice-declared");
        assertThat(refusals).contains("first.yaml");
        assertThat(refusals).contains("second.yaml");
    }

    /**
     * An empty corpus is not this loader's failure to raise. The corpus gate (U4) treats an empty corpus as a red,
     * because that is where "the run executed nothing and reported green" is actually decidable; here it is just an
     * empty list.
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
