package bz.stub.parallelconsumer.streams.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Compares two {@link FinalState}s <em>per observable</em> and says which ones diverged (R11, KTD10).
 * <p>
 * Both of this rung's proofs go through here, and they read the answer in opposite directions - which is the
 * distinction {@code CONCEPTS.md} draws between a control arm and a positive control:
 *
 * <ul>
 *     <li>The <b>determinism</b> proof (R7) is a <b>control arm</b>: it declares divergence between two runs of one
 *     case forbidden, so any red {@link #divergences} returns fails the run.</li>
 *     <li>The <b>positive control</b> (R8) is the mirror: the perturbed twin <em>must</em> differ, so it is
 *     {@link #agreesInEveryObservable} returning {@code true} that fails the run. A twin that agrees is a twin the
 *     case's operations absorbed, and the fix is the twin, never the control.</li>
 * </ul>
 *
 * <h2>Every diverging observable, not the first</h2>
 *
 * KTD10 asks for one red per (case, proof, observable), so a comparison that found three diverging stores returns
 * three reds. <em>Within</em> one observable it reports the first differing entry rather than every one: the entries
 * of a store are not independent of each other - one extra record shifts every later entry - so listing them all
 * would turn one fault into a wall of reds without naming anything the first one does not.
 *
 * <h2>The observable names</h2>
 *
 * {@code store:<name>} and {@code sink:<topic>}, which is what a red's observable slot carries. The prefix is there
 * because a store and a sink may legitimately share a name, and a red that said only {@code counts} would send a
 * maintainer to the wrong one.
 */
public final class Differ {

    private static final String STORE = "store:";

    private static final String SINK = "sink:";

    /** What the message says instead of a value when one side does not have the observable or the entry at all. */
    private static final String ABSENT = "(absent)";

    private Differ() {
    }

    /**
     * Every observable in which the two outcomes disagree, as one red each.
     *
     * @param caseName the case both outcomes were computed from - named in every red (R11)
     * @param proof    which proof is doing the comparing; it supplies the cell name and what each side is called
     * @param left     the first outcome - {@link RedReport.Proof#leftLabel()} names it in the messages
     * @param right    the second outcome
     * @return the reds, in observable order (stores in the order the topology declared them, then sinks); empty when
     *         the two outcomes agree in every observable
     */
    public static List<RedReport> divergences(String caseName,
                                              RedReport.Proof proof,
                                              FinalState left,
                                              FinalState right) {
        List<RedReport> reds = new ArrayList<>();
        compare(reds, caseName, proof, STORE, left.stores(), right.stores());
        compare(reds, caseName, proof, SINK, left.sinks(), right.sinks());
        return reds;
    }

    /**
     * Whether the two outcomes agree in every observable - the positive control's question, asked in its own words.
     * <p>
     * It is {@link #divergences} with the same arguments being empty, expressed that way rather than as a separate
     * walk so the two answers cannot disagree: an equality that said "same" while the differ could name a diverging
     * store would make the positive control pass on a pipeline that is not sensitive to its input at all.
     */
    public static boolean agreesInEveryObservable(FinalState left, FinalState right) {
        // The case name and proof do not affect WHETHER anything diverged, only how it would be reported.
        return divergences("(unnamed)", RedReport.Proof.DETERMINISM, left, right).isEmpty();
    }

    private static void compare(List<RedReport> reds,
                                String caseName,
                                RedReport.Proof proof,
                                String prefix,
                                Map<String, List<String>> left,
                                Map<String, List<String>> right) {
        // Left first, so the reds come out in the order the topology declared its observables; a name only the
        // right side has still gets one, appended after them.
        Set<String> everyName = new LinkedHashSet<>(left.keySet());
        boolean ignoredGrew = everyName.addAll(right.keySet());

        for (String name : everyName) {
            List<String> leftEntries = left.get(name);
            List<String> rightEntries = right.get(name);
            if (leftEntries == null || rightEntries == null) {
                reds.add(RedReport.comparison(caseName, proof, prefix + name,
                        (leftEntries == null ? proof.rightLabel() : proof.leftLabel()) + " has this observable and "
                                + (leftEntries == null ? proof.leftLabel() : proof.rightLabel()) + " does not"));
                continue;
            }
            String difference = firstDifference(proof, leftEntries, rightEntries);
            if (difference != null) {
                reds.add(RedReport.comparison(caseName, proof, prefix + name, difference));
            }
        }
    }

    /**
     * The first entry the two renderings disagree on, said in one line with both values.
     *
     * @return the message, or {@code null} when the two lists are equal
     */
    @Nullable
    private static String firstDifference(RedReport.Proof proof, List<String> left, List<String> right) {
        int shared = Math.min(left.size(), right.size());
        for (int index = 0; index < shared; index++) {
            if (!left.get(index).equals(right.get(index))) {
                return entryDiffers(proof, index, left, right);
            }
        }
        if (left.size() != right.size()) {
            return entryDiffers(proof, shared, left, right);
        }
        return null;
    }

    private static String entryDiffers(RedReport.Proof proof, int index, List<String> left, List<String> right) {
        String sizes = left.size() == right.size()
                ? ""
                : " (" + proof.leftLabel() + " has " + left.size() + " entries, " + proof.rightLabel() + " has "
                        + right.size() + ")";
        return "entry " + (index + 1) + " differs" + sizes + ": " + proof.leftLabel() + " has "
                + entry(left, index) + ", " + proof.rightLabel() + " has " + entry(right, index);
    }

    private static String entry(List<String> entries, int index) {
        return index < entries.size() ? entries.get(index) : ABSENT;
    }
}
