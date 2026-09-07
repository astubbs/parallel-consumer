package bz.stub.parallelconsumer.streams.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Everything a case is run against, and the one sanctioned way to run fewer of them (R10, KTD7).
 *
 * <h2>The oracle is the control arm, and it is not a row</h2>
 *
 * {@link #ORACLE} is in every selection and can never be selected away. Its whole job is to answer "is this case
 * wrong?" in the same run in which a binding went red - an answer that arrives in a different CI job, hours later,
 * is not the answer anybody needed. It is deliberately <em>not</em> a {@link Row}: a row is a foreign binding this
 * suite drives and compares <em>against</em> the oracle, so putting the oracle in the registry would let a selector
 * name it as one of the things being tested.
 *
 * <h2>The registry is empty on this rung, and that is the honest state</h2>
 *
 * There is no binding to drive yet: the wrapper's engine is not on this module's classpath, and a stub in its place
 * would turn "every binding agrees with the oracle" into a statement about a mock.
 * {@link TheEngineArrivingMustBringTheStreamsRowTest} is the guard that keeps the emptiness honest - it fails the
 * moment the wrapper arrives without a row, and the moment a row is written without the wrapper.
 *
 * <h2>A selector that matches nothing FAILS</h2>
 *
 * A CI row naming a binding by name is on the hot path of every run there is, so a typo that silently selected zero
 * bindings would evaluate nothing, take two seconds, and report the row green - a binding nobody tested,
 * indistinguishable from a binding that passed. This repository has recorded instances of checks that reported
 * success without ever having run, and a per-row selector is the cheapest way yet invented to add another. So an
 * unregistered name is an error naming what is registered, never an empty selection.
 *
 * <h2>The property is read in exactly one place</h2>
 *
 * {@link #fromSystemProperty()} is that place, and it is called at the gate's entry. Everything worth testing is in
 * {@link #select(String, List)}, the pure function, so no test in this module ever sets the property: a JVM-wide
 * property set by one test is read by every test running beside it.
 *
 * @see TheEngineArrivingMustBringTheStreamsRowTest
 * @see SelectorMatchingNothingFailsTest
 */
public final class BindingRows {

    /**
     * The selector: {@code -Dpc.streams.conformance.binding=<name>}, or a comma-separated list. Absent or blank
     * means every registered row.
     * <p>
     * <b>It is deliberately not {@code pc.conformance.language}</b>, which the proxy conformance suite owns and
     * which that suite's CI matrix rows are written against (KTD7). Two registries answering one property would let
     * a name select a row in one suite while selecting nothing in the other, and the suite that selected nothing
     * would report green.
     */
    public static final String BINDING_PROPERTY = "pc.streams.conformance.binding";

    /**
     * The wrapper's builder entry point, named as a string and never imported - importing it is exactly what this
     * module cannot do today (KTD6).
     * <p>
     * <b>This is the single place the driver rung has to keep in step.</b> The class is the wrapper's topology
     * assembler on astubbs/parallel-consumer#334, already under the fork's post-rename package. When a later rung
     * declares the wrapper module as a test dependency, this constant is what
     * {@link TheEngineArrivingMustBringTheStreamsRowTest} probes to decide that the engine has arrived - so a rename
     * on the wrapper side is caught here rather than by a guard that quietly stopped finding anything.
     */
    public static final String WRAPPER_ASSEMBLER = "bz.stub.parallelconsumer.streams.TopologyAssembler";

    /** The control arm's name in a selection. Always selected; never a {@link Row}. */
    public static final String ORACLE = "oracle";

    /**
     * Every binding row this suite drives. Empty on this rung - see the class javadoc - and the guard is what keeps
     * that emptiness from rotting into a silence.
     */
    private static final ImmutableList<Row> REGISTERED = ImmutableList.of();

    private BindingRows() {
    }

    /** The registry the guard and the selector are both held against. */
    public static List<Row> registered() {
        return REGISTERED;
    }

    /**
     * The gate's entry point, and the <em>only</em> read of {@link #BINDING_PROPERTY} in this module (KTD7).
     * <p>
     * Nothing in this module's tests calls it, on purpose: everything it does beyond reading one property is
     * {@link #select(String, List)}, which a test can call directly with whatever value it wants to fail on.
     */
    static List<String> fromSystemProperty() {
        return select(System.getProperty(BINDING_PROPERTY), registered());
    }

    /**
     * The selection, as a pure function of what was asked for and what is registered - so the failure that matters
     * most here can be proven without a JVM-wide property.
     *
     * @param requested the property's value: {@code null} or blank for everything, else a comma-separated list of
     *                  row names ({@link #ORACLE} may be named like any other)
     * @param registry  the rows a name may select, in the order a run drives them
     * @return the selected names, the oracle always first
     * @throws IllegalArgumentException if any name is not registered, or if a non-blank selector names nothing at
     *                                  all. Failing is the whole point: a selection nobody matched must never be
     *                                  reported as a clean run
     */
    static List<String> select(@Nullable String requested, List<Row> registry) {
        List<String> known = new ArrayList<>();
        known.add(ORACLE);
        registry.forEach(row -> known.add(row.name()));

        if (requested == null || requested.trim().isEmpty()) {
            // A blank value is what an unset CI variable expands to, so it must mean the same as an absent one -
            // anything else turns a missing variable into a run of something nobody chose.
            return Collections.unmodifiableList(known);
        }

        List<String> wanted = Arrays.stream(requested.split(","))
                .map(String::trim)
                .filter(name -> !name.isEmpty())
                .collect(Collectors.toList());
        if (wanted.isEmpty()) {
            // "," and " , " are typos, not requests for everything: treating them as absent would run every row
            // for a selector the author clearly meant to name one.
            throw unselectable(requested, "names no binding at all - it is punctuation without a name", known);
        }

        List<String> unknown = wanted.stream().filter(name -> !known.contains(name)).collect(Collectors.toList());
        if (!unknown.isEmpty()) {
            throw unselectable(requested, "names bindings this suite does not register: " + unknown, known);
        }

        List<String> selected = new ArrayList<>();
        selected.add(ORACLE);
        registry.stream().map(Row::name).filter(wanted::contains).forEach(selected::add);
        return Collections.unmodifiableList(selected);
    }

    /**
     * The message contract, in one place so both refusals say the same three things: what was asked for, what is
     * registered, and why selecting nothing is not the alternative.
     */
    private static IllegalArgumentException unselectable(String requested, String what, List<String> known) {
        return new IllegalArgumentException("-D" + BINDING_PROPERTY + "=" + requested + " " + what
                + " (registered: " + known + "). A typo here would otherwise select nothing, execute nothing and "
                + "read as a pass, which is the one outcome this suite exists to refuse - so an unmatched name "
                + "fails the run instead. " + (known.size() == 1
                ? "Only the oracle control arm is registered on this rung; there is no binding row to name yet, and "
                + "the wrapper arriving is what adds one (see TheEngineArrivingMustBringTheStreamsRowTest)."
                : "Selecting nothing is never the alternative: the oracle is in every selection and cannot be "
                + "selected away, because a case that fails against the oracle is a WRONG CASE."));
    }

    /**
     * One binding row: a foreign implementation of the wrapper's builder surface that this suite drives through the
     * same corpus and compares against the oracle.
     * <p>
     * A row is a <em>name</em> today, and nothing more, because there is nothing to drive: the thing that executes a
     * case through a binding - the wire, the handle table, the outcome capture - is the driver rung's work (R14,
     * U7). The type exists now so the registry, the selector and the guard have something real to be written
     * against; when the driver rung lands, what it adds is a member here, not a second registry.
     */
    public static final class Row {

        private final String name;

        /**
         * Package-private: a row is registered by editing {@link #REGISTERED}, never by a caller minting one. Tests
         * construct phantom rows to prove the guard and the selector can fail, which is the only other use.
         */
        Row(String name) {
            this.name = name;
        }

        /** How a selector names this row, and how a red names it back. */
        public String name() {
            return name;
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
