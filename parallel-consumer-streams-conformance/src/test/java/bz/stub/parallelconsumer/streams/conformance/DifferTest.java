package bz.stub.parallelconsumer.streams.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * One test per U4 differ scenario, over outcomes built <strong>by hand</strong> rather than computed.
 * <p>
 * That is the point of this class and the line it draws with {@link CorpusGateTest}: a hand-built pair of outcomes
 * lets a test state exactly which observable differs and check that the red names it, which no executed case can do
 * on demand. It is emphatically <em>not</em> the determinism proof's sabotage arm - KTD9 rules a stubbed
 * nondeterministic oracle out as an arm for the same reason R8 refuses a perturbed copy of a computed outcome: it
 * would exercise the comparison alone and say nothing about the pipeline that feeds it. The arm is a real
 * perturbation of the oracle, recorded in the PR body.
 */
class DifferTest {

    private static final String CASE = "hand-built";

    /**
     * Covers AE7. Two outcomes differing in one store produce a red that names the case, the proof and the store,
     * carrying both values - so a maintainer can act on the line without re-running anything (R11).
     */
    @Test
    void twoOutcomesDifferingInOneStoreProduceARedNamingDeterminismAndThatStore() {
        FinalState first = outcome(stores("counts", entries("\"a\" -> 2", "\"b\" -> 1")), noSinks());
        FinalState second = outcome(stores("counts", entries("\"a\" -> 3", "\"b\" -> 1")), noSinks());

        List<RedReport> reds = Differ.divergences(CASE, RedReport.Proof.DETERMINISM, first, second);

        assertThat(reds).hasSize(1);
        RedReport red = reds.get(0);
        assertThat(red.kind()).isEqualTo(RedReport.Kind.COMPARISON);
        assertThat(red.caseName()).isEqualTo(CASE);
        assertThat(red.proof()).isEqualTo("determinism");
        assertThat(red.observable()).isEqualTo("store:counts");
        // Both values, and the entry that carries them, because the store's name alone does not say what to fix.
        assertThat(red.what()).contains("\"a\" -> 2");
        assertThat(red.what()).contains("\"a\" -> 3");
        assertThat(red.render()).isEqualTo(CASE + "/determinism/store:counts: " + red.what());
    }

    /**
     * The happy path: two structurally equal outcomes built independently - separate maps, separate lists, equal
     * contents - agree in every observable, so the control arm has nothing to report.
     */
    @Test
    void twoStructurallyEqualOutcomesBuiltIndependentlyAgreeInEveryObservable() {
        FinalState first = outcome(stores("counts", entries("\"a\" -> 2")), sinks("out", entries("\"a\" -> 2 @10")));
        FinalState second = outcome(stores("counts", entries("\"a\" -> 2")), sinks("out", entries("\"a\" -> 2 @10")));

        assertThat(Differ.divergences(CASE, RedReport.Proof.DETERMINISM, first, second)).isEmpty();
        assertThat(Differ.agreesInEveryObservable(first, second)).isTrue();
        // Built independently, so this is a value comparison and not two references to one list.
        assertThat(first.stores()).isNotSameInstanceAs(second.stores());
    }

    /**
     * Edge: two outcomes differing only in one sink's one key report that sink and that key, and say nothing about
     * the store or the other sink that agreed.
     */
    @Test
    void twoOutcomesDifferingInOneSinkKeyReportThatSinkAndThatKey() {
        Map<String, List<String>> agreeingStore = stores("counts", entries("\"a\" -> 2"));
        Map<String, List<String>> leftSinks = new LinkedHashMap<>(sinks("out", entries("\"a\" -> 2 @10",
                "\"b\" -> 1 @20")));
        leftSinks.put("other", entries("\"c\" -> 9 @30"));
        Map<String, List<String>> rightSinks = new LinkedHashMap<>(sinks("out", entries("\"a\" -> 2 @10",
                "\"b\" -> 7 @20")));
        rightSinks.put("other", entries("\"c\" -> 9 @30"));

        List<RedReport> reds = Differ.divergences(CASE, RedReport.Proof.POSITIVE_CONTROL,
                outcome(agreeingStore, leftSinks), outcome(agreeingStore, rightSinks));

        assertThat(reds).hasSize(1);
        assertThat(reds.get(0).observable()).isEqualTo("sink:out");
        assertThat(reds.get(0).what()).contains("\"b\" -> 1 @20");
        assertThat(reds.get(0).what()).contains("\"b\" -> 7 @20");
        // The differing entry is the second, and saying which one keeps a long sink list actionable.
        assertThat(reds.get(0).what()).contains("entry 2");
        // The proof supplies the vocabulary: this comparison is the perturbed twin against the inputs.
        assertThat(reds.get(0).what()).contains("the perturbed twin");
    }

    /** Edge: an observable one outcome lacks entirely is reported by name, rather than by an empty comparison. */
    @Test
    void anObservableOneOutcomeLacksIsReportedByName() {
        FinalState both = outcome(storesOf("counts", entries("\"a\" -> 2"), "totals", entries("\"a\" -> \"5\"")),
                noSinks());
        FinalState missingOne = outcome(stores("counts", entries("\"a\" -> 2")), noSinks());

        List<RedReport> reds = Differ.divergences(CASE, RedReport.Proof.DETERMINISM, both, missingOne);

        assertThat(reds).hasSize(1);
        assertThat(reds.get(0).observable()).isEqualTo("store:totals");
        assertThat(reds.get(0).what()).contains("the first run has this observable");
        assertThat(reds.get(0).what()).contains("the second run does not");

        // And the other way round, so the report says which side is missing it rather than only that one is.
        List<RedReport> reversed = Differ.divergences(CASE, RedReport.Proof.DETERMINISM, missingOne, both);
        assertThat(reversed).hasSize(1);
        assertThat(reversed.get(0).observable()).isEqualTo("store:totals");
        assertThat(reversed.get(0).what()).contains("the second run has this observable");
    }

    /**
     * Error: an empty executable corpus fails the gate naming the corpus directory (R11) - it is a red, never a
     * green run over nothing, and a corpus of refusal-class cases alone is empty for this purpose.
     * <p>
     * The rule is checked through the cells {@link CorpusGateTest} actually builds, because the failure being
     * guarded against is a corpus that produces no cell at all: asserting only the red's wording would pass just as
     * well against a gate that never emitted it.
     */
    @Test
    void anEmptyExecutableCorpusIsARedNamingTheCorpusDirectory() {
        List<DynamicTest> cells = CorpusGateTest.cellsFor(Collections.emptyList(), "cases");

        assertThat(cells).hasSize(1);
        assertThat(cells.get(0).getDisplayName()).isEqualTo("cases/empty-corpus");
        Throwable red = assertThrows(Throwable.class, () -> cells.get(0).getExecutable().execute());
        assertThat(red).hasMessageThat().contains("cases");
        assertThat(red).hasMessageThat().contains("holds no executable case");

        RedReport report = RedReport.emptyCorpus("cases");
        assertThat(report.kind()).isEqualTo(RedReport.Kind.EMPTY_CORPUS);
        assertThat(report.render()).startsWith("cases/empty-corpus/");
    }

    /**
     * KTD10: a thrown oracle is its own category, so a maintainer never has to infer whether the outcomes differed
     * or whether there was an outcome at all.
     */
    @Test
    void anOracleExecutionFailureIsItsOwnCategoryAndNamesTheCase() {
        Oracle.OracleExecutionException thrown = assertThrows(Oracle.OracleExecutionException.class,
                () -> Oracle.run(refusalClassCase()));

        RedReport red = RedReport.oracleExecution(thrown.caseName(), RedReport.Proof.DETERMINISM, thrown);

        assertThat(red.kind()).isEqualTo(RedReport.Kind.ORACLE_EXECUTION);
        assertThat(red.kind()).isNotEqualTo(RedReport.Kind.COMPARISON);
        assertThat(red.caseName()).isEqualTo("aggregate-names-function-and-combine");
        assertThat(red.observable()).isEqualTo("(oracle execution)");
        assertThat(red.what()).contains("no outcome was computed");
        assertThat(red.render()).contains("aggregate-names-function-and-combine/determinism/");
    }

    // ------------------------------------------------------------------------------------------------ fixtures

    /** The corpus's own refusal-class case - the one shape the oracle refuses to execute at all (R15). */
    private static ConformanceCase refusalClassCase() {
        return CaseLoader.loadClasspathDirectory(CorpusGateTest.CORPUS).stream()
                .filter(ConformanceCase::refusalClass)
                .findFirst()
                .orElseThrow(() -> new AssertionError("the corpus holds no refusal-class case"));
    }

    private static FinalState outcome(Map<String, List<String>> stores, Map<String, List<String>> sinks) {
        // The topology description is not compared - see FinalState#topologyDescription - so a hand-built outcome
        // says so rather than inventing one that looks like Kafka's.
        return new FinalState(stores, sinks, "(hand-built outcome, not a described topology)");
    }

    private static List<String> entries(String... rendered) {
        return new ArrayList<>(Arrays.asList(rendered));
    }

    private static Map<String, List<String>> stores(String name, List<String> entries) {
        Map<String, List<String>> stores = new LinkedHashMap<>();
        List<String> previous = stores.put(name, entries);
        assertThat(previous).isNull();
        return stores;
    }

    private static Map<String, List<String>> storesOf(String first, List<String> firstEntries,
                                                      String second, List<String> secondEntries) {
        Map<String, List<String>> stores = stores(first, firstEntries);
        List<String> previous = stores.put(second, secondEntries);
        assertThat(previous).isNull();
        return stores;
    }

    private static Map<String, List<String>> sinks(String topic, List<String> entries) {
        return stores(topic, entries);
    }

    private static Map<String, List<String>> noSinks() {
        return Collections.emptyMap();
    }
}
