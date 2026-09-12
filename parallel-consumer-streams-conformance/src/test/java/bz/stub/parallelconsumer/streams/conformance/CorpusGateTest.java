package bz.stub.parallelconsumer.streams.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The gate: every executable case in the corpus is run through the control arm and the positive control, one cell
 * each (R7, R8, R11, KTD9, KTD10).
 *
 * <h2>One cell per (case, proof), named so a red is legible without opening a log</h2>
 *
 * The cells are dynamic tests named {@code <case>/determinism} and {@code <case>/positive-control} - the case name
 * and {@link RedReport.Proof#spelling()}, the same two strings a {@link RedReport} renders, so the surefire report
 * and the failure message name the same cell. A red inside a cell is one {@link RedReport} per diverging observable.
 *
 * <h2>What each cell proves</h2>
 *
 * <ul>
 *     <li><b>{@code <case>/determinism}</b> (R7, AE1) - the oracle runs the case twice in this run and the two
 *     outcomes must be identical in every observable. It is a <b>control arm</b>: nondeterminism is the forbidden
 *     anomaly, so the cell exists to <em>not</em> fire, and a green one is a checked claim rather than an
 *     unexamined pass only because the arm would have failed had the anomaly appeared.</li>
 *     <li><b>{@code <case>/positive-control}</b> (R8, AE2, KTD4) - the case's author-chosen perturbed twin is
 *     executed through the identical oracle path, and must differ in at least one observable. It is the arm whose
 *     only job is to fire: agreement means the whole pipeline from execution to comparison was insensitive to its
 *     input on this case, and until it fires the determinism cell's green is uninterpretable rather than
 *     clean.</li>
 * </ul>
 *
 * <h2>Two shapes that must not read as green</h2>
 *
 * <ul>
 *     <li><b>A refusal-class case</b> (R15) is skipped by design, in its own cell, reported as skipped - it declares
 *     a fault the wire must raise and plain Kafka Streams never refuses what the wire invented, so there is no
 *     oracle row to compute. Dropping it silently would make a corpus that shrank look like one that passed.</li>
 *     <li><b>An empty executable corpus</b> is a red naming the corpus directory, not a green run over nothing -
 *     and a corpus holding only refusal-class cases is empty for this purpose. This is the failure
 *     {@code CaseLoaderTest}'s empty-directory test deliberately leaves to the gate, because here it is decidable.
 *     </li>
 * </ul>
 *
 * <h2>An oracle that throws is its own category (KTD10)</h2>
 *
 * A cell whose {@link Oracle} call throws reports an {@link RedReport.Kind#ORACLE_EXECUTION} red rather than a
 * comparison red, because no outcome was computed and "the outcomes differ" would be a false description of a run
 * that produced none.
 */
class CorpusGateTest {

    /** The committed corpus. Named in the empty-corpus red, so a maintainer knows which directory to fill. */
    static final String CORPUS = "cases";

    /** The cell a refusal-class case gets: present, named, and aborted rather than absent (R15). */
    static final String SKIPPED_BY_DESIGN = "skipped-by-design";

    /**
     * The one test in this module that sets {@link BindingRows#BINDING_PROPERTY}, and it is allowed to only because
     * the wiring <em>is</em> what it proves (KTD7).
     * <p>
     * The selector's behaviour is tested property-free in {@code SelectorMatchingNothingFailsTest}, over the pure
     * function; nothing there can tell whether the gate ever reads the property. It did not: for seven commits
     * {@link BindingRows#fromSystemProperty()} had no caller, so {@code -Dpc.streams.conformance.binding=typo} ran
     * the whole corpus and reported green - the exact "a typo reads as a pass" outcome the selector exists to
     * refuse, one layer up from where it was being refused.
     * <p>
     * The property is restored in a {@code finally}: a JVM-wide property left set by one test is read by every test
     * beside it.
     */
    @Test
    void anUnregisteredBindingNameFailsTheGateRatherThanSelectingNothing() {
        List<ConformanceCase> corpus = CaseLoader.loadClasspathDirectory(CORPUS);
        String restore = System.getProperty(BindingRows.BINDING_PROPERTY);
        try {
            String ignoredPrevious = System.setProperty(BindingRows.BINDING_PROPERTY, "java-wrappre");

            IllegalArgumentException thrown =
                    assertThrows(IllegalArgumentException.class, () -> cellsFor(corpus, CORPUS));

            assertWithMessage("the gate has to fail on the typo the CI row actually wrote")
                    .that(thrown).hasMessageThat().contains("java-wrappre");
            assertWithMessage("and name what is registered, so the fix is in the message rather than in the source")
                    .that(thrown).hasMessageThat().contains(BindingRows.ORACLE);
        } finally {
            if (restore == null) {
                Object ignoredRemoved = System.getProperties().remove(BindingRows.BINDING_PROPERTY);
            } else {
                Object ignoredReplaced = System.setProperty(BindingRows.BINDING_PROPERTY, restore);
            }
        }
    }

    /** With no selector set - every ordinary run - the gate resolves the oracle and builds its cells. */
    @Test
    void withNoSelectorSetTheGateResolvesTheOracleAndBuildsItsCells() {
        assertThat(System.getProperty(BindingRows.BINDING_PROPERTY)).isNull();
        assertThat(cellsFor(CaseLoader.loadClasspathDirectory(CORPUS), CORPUS)).isNotEmpty();
    }

    @TestFactory
    Stream<DynamicTest> everyExecutableCaseHoldsTheControlArmAndThePositiveControl() {
        return cellsFor(CaseLoader.loadClasspathDirectory(CORPUS), CORPUS).stream();
    }

    /**
     * The cells for one corpus. Package-private and taking its corpus rather than loading one so the empty-corpus
     * rule is testable without an empty directory on the classpath - {@code DifferTest} holds that test.
     * <p>
     * This is also where {@link BindingRows#BINDING_PROPERTY} is read, once per gate entry and nowhere else (KTD7).
     * Reading it here rather than inside a cell is deliberate: an unregistered name has to throw out of the test
     * <em>factory</em>, which reddens the run, where the same throw inside one cell would redden one cell of a run
     * that still executed everything the selector was supposed to narrow.
     */
    static List<DynamicTest> cellsFor(List<ConformanceCase> corpus, String corpusDirectory) {
        List<String> selection = BindingRows.fromSystemProperty();
        assertWithMessage("the oracle is the control arm and is in every selection, so a selection without it means "
                        + "the registry and the selector have come apart - and on this rung the oracle is the whole "
                        + "selection, since no binding row is registered yet. The driver rung iterates the rest of "
                        + "this list; today there is no rest")
                .that(selection)
                .contains(BindingRows.ORACLE);

        List<DynamicTest> cells = new ArrayList<>();
        int executable = 0;
        for (ConformanceCase conformanceCase : corpus) {
            if (conformanceCase.refusalClass()) {
                cells.add(DynamicTest.dynamicTest(cell(conformanceCase, SKIPPED_BY_DESIGN),
                        () -> Assumptions.abort("case " + conformanceCase.name() + " is refusal-class (R15): it "
                                + "declares the fault the wire must raise for an invalid specification, and plain "
                                + "Kafka Streams never refuses what the wire invented, so there is no oracle row to "
                                + "compute - skipped by design, never dropped")));
                continue;
            }
            executable++;
            cells.add(DynamicTest.dynamicTest(cell(conformanceCase, RedReport.Proof.DETERMINISM.spelling()),
                    () -> determinism(conformanceCase)));
            cells.add(DynamicTest.dynamicTest(cell(conformanceCase, RedReport.Proof.POSITIVE_CONTROL.spelling()),
                    () -> positiveControl(conformanceCase)));
        }
        if (executable == 0) {
            RedReport red = RedReport.emptyCorpus(corpusDirectory);
            cells.add(DynamicTest.dynamicTest(corpusDirectory + "/" + RedReport.EMPTY_CORPUS,
                    () -> fail(red.render())));
        }
        return cells;
    }

    /**
     * R7's control arm on one case: two runs, identical in every observable, and any divergence is a red naming the
     * observable.
     */
    private static void determinism(ConformanceCase conformanceCase) {
        RedReport.Proof proof = RedReport.Proof.DETERMINISM;
        FinalState first = execute(conformanceCase, proof, false);
        FinalState second = execute(conformanceCase, proof, false);

        List<RedReport> reds = Differ.divergences(conformanceCase.name(), proof, first, second);
        assertWithMessage("the control arm fired: one case executed twice produced different outcomes, which is the "
                        + "forbidden anomaly%s%s%s%s", System.lineSeparator(), first, System.lineSeparator(), second)
                .that(rendered(reds))
                .isEmpty();
    }

    /**
     * R8's positive control on one case: the perturbed twin through the identical path, required to differ. The red
     * is the <em>absence</em> of divergence, which is why this cell cannot be written as an assertion over the
     * differ's output.
     */
    private static void positiveControl(ConformanceCase conformanceCase) {
        RedReport.Proof proof = RedReport.Proof.POSITIVE_CONTROL;
        FinalState fromInputs = execute(conformanceCase, proof, false);
        FinalState fromTwin = execute(conformanceCase, proof, true);

        if (Differ.agreesInEveryObservable(fromInputs, fromTwin)) {
            fail(RedReport.comparison(conformanceCase.name(), proof, RedReport.EVERY_OBSERVABLE,
                    "the positive control did not fire: the perturbed twin agrees with the inputs in every "
                            + "observable, so nothing here shows the pipeline from execution to comparison is "
                            + "sensitive to its input. A twin the case's operations can absorb proves nothing, and "
                            + "the fix is the twin, never the control." + System.lineSeparator() + fromInputs)
                    .render());
        }
    }

    /** Runs one side of a proof, turning a thrown oracle into an oracle-execution red rather than a comparison. */
    private static FinalState execute(ConformanceCase conformanceCase, RedReport.Proof proof, boolean twin) {
        try {
            return twin ? Oracle.runPerturbation(conformanceCase) : Oracle.run(conformanceCase);
        } catch (Oracle.OracleExecutionException e) {
            fail(RedReport.oracleExecution(conformanceCase.name(), proof, e).render(), e);
            throw new AssertionError("unreachable: fail() always throws");
        }
    }

    private static List<String> rendered(List<RedReport> reds) {
        return reds.stream().map(RedReport::render).collect(Collectors.toList());
    }

    private static String cell(ConformanceCase conformanceCase, String proof) {
        return conformanceCase.name() + "/" + proof;
    }
}
