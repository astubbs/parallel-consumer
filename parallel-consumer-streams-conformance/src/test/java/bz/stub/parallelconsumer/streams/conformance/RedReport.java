package bz.stub.parallelconsumer.streams.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.Objects;

/**
 * One red: a single failure of one proof, about one observable, on one case (R11, KTD10).
 * <p>
 * <b>One red per (case, proof, observable), never one per run.</b> A comparison that found three diverging stores
 * produces three of these rather than one saying "the outcomes differ", because the thing a maintainer has to act on
 * is the observable, and a report that names only the case has replaced one silent failure with a slow one.
 *
 * <h2>The render, which is the whole point</h2>
 *
 * {@link #render()} is {@code <case>/<proof>/<observable>: <what>}. The first two slots are exactly the name of the
 * gate cell that failed, so a red read in a surefire report and a red read in a log identify the same cell, and
 * neither can drift from the other - {@link CorpusGateTest} builds its cell names from {@link Proof#spelling()} too.
 *
 * <h2>Three categories, because "which proof failed" must never be inferred (KTD10)</h2>
 *
 * <ul>
 *     <li>{@link Kind#COMPARISON} - two outcomes were computed and disagreed (or, for the positive control, agreed
 *     when they had to disagree).</li>
 *     <li>{@link Kind#ORACLE_EXECUTION} - no comparison happened at all: the topology build, a pipe or a snapshot
 *     threw. Distinct from a comparison red on purpose; reporting it as one would say "the outcomes differ" about a
 *     run that produced no outcome. It is also distinct from a load-time refusal
 *     ({@link CaseLoader.CorpusRefusedException}), which happens before any case is executed.</li>
 *     <li>{@link Kind#EMPTY_CORPUS} - the gate had nothing to execute. It is a red rather than a green because "the
 *     run executed nothing" and "the run executed everything and all of it passed" are otherwise the same output;
 *     the corpus directory takes the case slot, since there is no case to name.</li>
 * </ul>
 */
public final class RedReport {

    /**
     * Which proof this red belongs to - and, for the two per-case proofs, the name of the gate cell that carries it.
     * <p>
     * The two labels are the vocabulary the messages use for the sides being compared, taken from {@code CONCEPTS.md}
     * rather than invented here: the determinism proof is a <em>control arm</em>, which declares nondeterminism
     * forbidden and so must not fire; the positive control is an arm that <em>must</em> fire, and its second side is
     * the case's author-chosen <em>perturbed twin</em>.
     */
    public enum Proof {

        /** R7's control arm: one case executed twice, where any difference is the forbidden anomaly. */
        DETERMINISM("determinism", "the first run", "the second run"),

        /** R8's positive control: the perturbed twin executed through the identical oracle path (KTD4). */
        POSITIVE_CONTROL("positive-control", "the inputs", "the perturbed twin");

        private final String spelling;

        private final String leftLabel;

        private final String rightLabel;

        Proof(String spelling, String leftLabel, String rightLabel) {
            this.spelling = spelling;
            this.leftLabel = leftLabel;
            this.rightLabel = rightLabel;
        }

        /** The proof's name in a red and in the gate cell that runs it - {@code <case>/<spelling>}. */
        public String spelling() {
            return spelling;
        }

        /** What the first outcome is called in a message about this proof. */
        public String leftLabel() {
            return leftLabel;
        }

        /** What the second outcome is called in a message about this proof. */
        public String rightLabel() {
            return rightLabel;
        }
    }

    /** The categories KTD10 requires be distinguishable without reading the message. */
    public enum Kind {
        COMPARISON, ORACLE_EXECUTION, EMPTY_CORPUS
    }

    /** The cell an empty corpus fails in, and the proof slot of its red - there is no per-case proof to name. */
    static final String EMPTY_CORPUS = "empty-corpus";

    /** The observable slot of a red that is not about one observable in particular. */
    static final String NO_OBSERVABLE = "(oracle execution)";

    /** The observable slot of the positive control's red: it fails when EVERY observable agreed. */
    static final String EVERY_OBSERVABLE = "(every observable)";

    private final Kind kind;

    private final String caseName;

    private final String proof;

    private final String observable;

    private final String what;

    private RedReport(Kind kind, String caseName, String proof, String observable, String what) {
        this.kind = kind;
        this.caseName = caseName;
        this.proof = proof;
        this.observable = observable;
        this.what = what;
    }

    /** A comparison red: two outcomes were computed, and {@code observable} is what a maintainer must open. */
    public static RedReport comparison(String caseName, Proof proof, String observable, String what) {
        return new RedReport(Kind.COMPARISON, caseName, proof.spelling(), observable, what);
    }

    /**
     * An oracle-execution red (KTD10): the case never produced an outcome to compare, so this is not a comparison
     * red however much it looks like one in a report.
     */
    public static RedReport oracleExecution(String caseName, Proof proof, Oracle.OracleExecutionException failure) {
        return new RedReport(Kind.ORACLE_EXECUTION, caseName, proof.spelling(), NO_OBSERVABLE,
                "no outcome was computed - " + failure.getMessage());
    }

    /**
     * The corpus-level red: the gate found no executable case, which is a failure naming the directory rather than a
     * green run over nothing.
     *
     * @param corpusDirectory the classpath directory the gate loaded - it takes the case slot, there being no case
     */
    public static RedReport emptyCorpus(String corpusDirectory) {
        return new RedReport(Kind.EMPTY_CORPUS, corpusDirectory, EMPTY_CORPUS, "(no executable case)",
                "the corpus directory " + corpusDirectory + " holds no executable case, so the gate proved nothing "
                        + "and would otherwise report green; a corpus of refusal-class cases alone is empty for this "
                        + "purpose, because a refusal-class case is never executed (R15)");
    }

    public Kind kind() {
        return kind;
    }

    /** The case a maintainer must open - or, for {@link Kind#EMPTY_CORPUS}, the corpus directory. */
    public String caseName() {
        return caseName;
    }

    /** Which proof failed, spelt as the gate cell spells it. */
    public String proof() {
        return proof;
    }

    /** Which observable diverged - {@code store:<name>} or {@code sink:<topic>} for a comparison red. */
    public String observable() {
        return observable;
    }

    /** What went wrong, including both values when two were compared. */
    public String what() {
        return what;
    }

    /** {@code <case>/<proof>/<observable>: <what>} - one line, actionable without re-running anything (R11). */
    public String render() {
        return caseName + "/" + proof + "/" + observable + ": " + what;
    }

    @Override
    public String toString() {
        return render();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof RedReport)) {
            return false;
        }
        RedReport that = (RedReport) other;
        return kind == that.kind && caseName.equals(that.caseName) && proof.equals(that.proof)
                && observable.equals(that.observable) && what.equals(that.what);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, caseName, proof, observable, what);
    }
}
