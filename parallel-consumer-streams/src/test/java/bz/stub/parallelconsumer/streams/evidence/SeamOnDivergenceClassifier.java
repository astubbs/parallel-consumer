package bz.stub.parallelconsumer.streams.evidence;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Getter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Set-differences the seam-off and seam-on runs of Apache Kafka's own suite, and classifies what is left by
 * the mechanism its failure names.
 * <p>
 * <b>The seam-off arm is the control, and having one is what makes this a measurement rather than a tally.</b>
 * A case that fails in both arms is failing for a reason the seam has nothing to do with - a broken build, a
 * loaded machine, a flake - and counting it as a divergence would attribute it to a change that did not cause
 * it. Only a case that <em>passes</em> with the switch off and <em>fails</em> with it on says anything about
 * the dispatch path, and that set is what gets classified.
 * <p>
 * Three ways this can end, and each has to be reachable or the lane is decoration:
 * <ul>
 *   <li>a divergence matching a {@link DivergenceMechanism} - reported with the mechanism named, lane green;</li>
 *   <li>a divergence matching none - reported with its recorded failure, lane <b>red</b>;</li>
 *   <li>an arm that cannot be read at all - {@link SurefireArm#read} throws, lane <b>red</b>, because a
 *       missing or stale report directory is an error and never an empty result.</li>
 * </ul>
 *
 * @author Antony Stubbs
 * @see SeamOnDivergenceLaneTest
 */
public final class SeamOnDivergenceClassifier {

    /** The pin each arm's report must show it ran under, read back out of the report surefire wrote. */
    public static final String DISPATCH_PROPERTY = "pc.streams.dispatch.enabled";

    private final SurefireArm seamOff;

    private final SurefireArm seamOn;

    private final List<DivergenceMechanism> mechanisms;

    public SeamOnDivergenceClassifier(final SurefireArm seamOff,
                                      final SurefireArm seamOn,
                                      final List<DivergenceMechanism> mechanisms) {
        this.seamOff = seamOff;
        this.seamOn = seamOn;
        this.mechanisms = mechanisms;
    }

    /**
     * One divergence and what the lane could say about it.
     */
    @Getter
    public static final class Divergence {

        private final SurefireArm.Case testCase;

        /** The mechanism that explained it, or null - and null is what makes the lane red. */
        private final DivergenceMechanism mechanism;

        /** What that mechanism said specifically: which construct, which ledger note. */
        private final String attribution;

        Divergence(final SurefireArm.Case testCase,
                   final DivergenceMechanism mechanism,
                   final String attribution) {
            this.testCase = testCase;
            this.mechanism = mechanism;
            this.attribution = attribution;
        }

        public boolean isExplained() {
            return mechanism != null;
        }
    }

    @Getter
    public static final class Result {

        /** Failing seam-on, passing seam-off: the only set that says anything about the dispatch path. */
        private final List<Divergence> divergences;

        /**
         * Every failure in the CONTROL arm, with the ledger note that explains it or null.
         * <p>
         * A control arm with unexplained failures is not a control: there is nothing trustworthy to
         * difference the measured arm against. A ledgered one is - the flake is named, and the run still
         * yields a verdict rather than being thrown away and re-run.
         */
        private final List<Divergence> controlArmFailures;

        /** Failing in BOTH arms - not the seam's doing, and reported so nobody re-attributes them to it. */
        private final List<SurefireArm.Case> failingInBothArms;

        /** Failing seam-off and passing seam-on. Rare and worth seeing; the seam-off oracle should be green. */
        private final List<SurefireArm.Case> passingOnlySeamOn;

        /** Mechanisms that explained nothing this run - an expectation that may have stopped being true. */
        private final List<DivergenceMechanism> mechanismsThatMatchedNothing;

        /** Ledger entries naming a method that did not diverge this run - the same rot, one level finer. */
        private final List<String> ledgerEntriesThatMatchedNothing;

        Result(final List<Divergence> divergences,
               final List<Divergence> controlArmFailures,
               final List<SurefireArm.Case> failingInBothArms,
               final List<SurefireArm.Case> passingOnlySeamOn,
               final List<DivergenceMechanism> mechanismsThatMatchedNothing,
               final List<String> ledgerEntriesThatMatchedNothing) {
            this.divergences = Collections.unmodifiableList(divergences);
            this.controlArmFailures = Collections.unmodifiableList(controlArmFailures);
            this.failingInBothArms = Collections.unmodifiableList(failingInBothArms);
            this.passingOnlySeamOn = Collections.unmodifiableList(passingOnlySeamOn);
            this.mechanismsThatMatchedNothing = Collections.unmodifiableList(mechanismsThatMatchedNothing);
            this.ledgerEntriesThatMatchedNothing = Collections.unmodifiableList(ledgerEntriesThatMatchedNothing);
        }

        /**
         * Control-arm failures nobody has diagnosed. Non-empty means the measurement cannot be trusted, which
         * is a different and worse verdict than "the measured arm diverged".
         */
        public List<Divergence> getUnexplainedControlArmFailures() {
            final List<Divergence> unexplained = new ArrayList<>();
            for (final Divergence failure : controlArmFailures) {
                if (!failure.isExplained()) {
                    unexplained.add(failure);
                }
            }
            return unexplained;
        }

        public List<Divergence> getUnexplained() {
            final List<Divergence> unexplained = new ArrayList<>();
            for (final Divergence divergence : divergences) {
                if (!divergence.isExplained()) {
                    unexplained.add(divergence);
                }
            }
            return unexplained;
        }

        public List<Divergence> getExplained() {
            final List<Divergence> explained = new ArrayList<>();
            for (final Divergence divergence : divergences) {
                if (divergence.isExplained()) {
                    explained.add(divergence);
                }
            }
            return explained;
        }
    }

    /**
     * Prove each arm ran under the pin its name claims, from the arm's own report.
     * <p>
     * A pin that fails to arrive produces a plausible run of the wrong thing: two seam-off arms differenced
     * against each other yield no divergences at all, which reads as the best possible result. The forked JVM
     * is gone by the time this runs, so the report surefire wrote is the only witness left.
     *
     * @throws IllegalStateException if either arm was not pinned, or both were pinned the same way
     */
    public void assertBothArmsRanUnderTheirOwnPin() {
        final String off = seamOff.getSystemProperty(DISPATCH_PROPERTY);
        final String on = seamOn.getSystemProperty(DISPATCH_PROPERTY);
        if (!"false".equals(off)) {
            throw new IllegalStateException("The seam-OFF arm's own report records " + DISPATCH_PROPERTY
                    + "=" + off + ", not false. Its pin did not arrive, so it is not a control arm and there "
                    + "is nothing to difference against.");
        }
        if (!"true".equals(on)) {
            throw new IllegalStateException("The seam-ON arm's own report records " + DISPATCH_PROPERTY
                    + "=" + on + ", not true. The seam was never on, so this run measured the control twice - "
                    + "which produces zero divergences and reads like the best possible result.");
        }
    }

    /**
     * Refuse to difference arms of different shape.
     * <p>
     * A case present in one arm and absent from the other cannot be differenced, so it would be dropped
     * silently - and dropping is the direction that hides divergences rather than inventing them.
     *
     * @throws IllegalStateException if the two arms covered different test classes
     */
    public void assertBothArmsCoveredTheSameClasses() {
        final Set<String> off = new TreeSet<>(seamOff.getClassNames());
        final Set<String> on = new TreeSet<>(seamOn.getClassNames());
        if (!off.equals(on)) {
            final Set<String> onlyOff = new TreeSet<>(off);
            onlyOff.removeAll(on);
            final Set<String> onlyOn = new TreeSet<>(on);
            onlyOn.removeAll(off);
            throw new IllegalStateException("The two arms covered different classes, so they cannot be "
                    + "differenced. Only seam-off: " + onlyOff + ". Only seam-on: " + onlyOn
                    + ". The two executions' <includes> lists must stay identical.");
        }
    }

    public Result classify() {
        final List<Divergence> divergences = new ArrayList<>();
        final List<SurefireArm.Case> failingInBoth = new ArrayList<>();
        final List<SurefireArm.Case> passingOnlySeamOn = new ArrayList<>();
        final Map<String, Boolean> matched = new LinkedHashMap<>();
        for (final DivergenceMechanism mechanism : mechanisms) {
            matched.put(mechanism.getName(), Boolean.FALSE);
        }

        // The control arm first, and against the flake ledger ALONE. A control-arm failure is not a
        // divergence and must not be explained by a divergence mechanism - "the refusal envelope threw" says
        // nothing about a run in which the seam was off.
        final List<Divergence> controlArmFailures = new ArrayList<>();
        for (final SurefireArm.Case offCase : seamOff.getCases()) {
            if (!offCase.isFailed()) {
                continue;
            }
            String note = null;
            for (final DivergenceMechanism mechanism : mechanisms) {
                if (mechanism instanceof DivergenceMechanism.LedgeredFlake) {
                    note = mechanism.attribute(offCase);
                    if (note != null) {
                        controlArmFailures.add(new Divergence(offCase, mechanism, note));
                        matched.put(mechanism.getName(), Boolean.TRUE);
                    }
                    break;
                }
            }
            if (note == null) {
                controlArmFailures.add(new Divergence(offCase, null, null));
            }
        }

        for (final SurefireArm.Case onCase : seamOn.getCases()) {
            final SurefireArm.Case offCase = seamOff.find(onCase.getId());
            if (offCase == null) {
                // assertBothArmsCoveredTheSameClasses catches the class-level version of this; a case that
                // exists in one arm only, within a shared class, is a parameterisation that did not run.
                continue;
            }
            if (!onCase.isFailed()) {
                if (offCase.isFailed()) {
                    passingOnlySeamOn.add(offCase);
                }
                continue;
            }
            if (offCase.isFailed()) {
                failingInBoth.add(onCase);
                continue;
            }
            DivergenceMechanism explanation = null;
            String attribution = null;
            for (final DivergenceMechanism mechanism : mechanisms) {
                final String candidate = mechanism.attribute(onCase);
                if (candidate != null) {
                    explanation = mechanism;
                    attribution = candidate;
                    matched.put(mechanism.getName(), Boolean.TRUE);
                    break;
                }
            }
            divergences.add(new Divergence(onCase, explanation, attribution));
        }

        final List<DivergenceMechanism> matchedNothing = new ArrayList<>();
        for (final DivergenceMechanism mechanism : mechanisms) {
            if (!matched.get(mechanism.getName())) {
                matchedNothing.add(mechanism);
            }
        }

        // Per-entry rot, one level finer than a whole mechanism going quiet: a ledger entry naming a method
        // that no longer diverges. Advisory rather than fatal - the usual cause is a rung fixing the thing,
        // which is good news that should still be visible rather than silently absorbed.
        final Set<String> divergingMethods = new TreeSet<>();
        for (final Divergence divergence : divergences) {
            divergingMethods.add(divergence.getTestCase().getClassName() + "#"
                    + DivergenceMechanism.methodOf(divergence.getTestCase().getName()));
        }
        final List<String> staleEntries = new ArrayList<>();
        for (final DivergenceMechanism mechanism : mechanisms) {
            if (!(mechanism instanceof DivergenceMechanism.LedgeredTriage)) {
                continue;
            }
            for (final String method : ((DivergenceMechanism.LedgeredTriage) mechanism).getMethods()) {
                if (!divergingMethods.contains(method)) {
                    staleEntries.add(method + " -> " + mechanism.getName());
                }
            }
        }

        return new Result(divergences, controlArmFailures, failingInBoth, passingOnlySeamOn,
                matchedNothing, staleEntries);
    }

    /**
     * The whole measurement as text: the arms, the divergences by mechanism, and every unexplained one with
     * the failure it recorded.
     * <p>
     * Deliberately verbose about the unexplained ones. They are the lane's output when it is red, and the
     * next reader's first question is always "what did it actually say", which a summary line cannot answer.
     */
    public String render(final Result result) {
        final StringBuilder out = new StringBuilder();
        out.append("SEAM-ON EVIDENCE LANE - Apache Kafka's own suite, run twice\n");
        out.append("===========================================================\n\n");
        out.append("  control (seam off): ").append(seamOff.getReportsDirectoryDisplay()).append('\n');
        out.append("  measured (seam on): ").append(seamOn.getReportsDirectoryDisplay()).append("\n\n");
        out.append("  A divergence is a case that PASSES with the switch off and FAILS with it on. Anything\n");
        out.append("  failing in both arms is not the seam's doing and is listed separately.\n\n");

        appendArmShape(out, seamOff);
        appendArmShape(out, seamOn);
        out.append('\n');

        out.append("CONTROL ARM INTEGRITY - a control with unexplained failures is not a control\n");
        if (result.getControlArmFailures().isEmpty()) {
            out.append("  clean\n");
        }
        for (final Divergence failure : result.getControlArmFailures()) {
            out.append("  ").append(failure.getTestCase().getId());
            if (failure.isExplained()) {
                out.append("  <- ledgered in ").append(failure.getAttribution()).append('\n');
            } else {
                out.append("  <- NOT LEDGERED: ").append(oneLine(failure.getTestCase().getFailureMessage()))
                        .append('\n');
            }
        }
        out.append('\n');

        final Map<String, List<Divergence>> byMechanism = new LinkedHashMap<>();
        for (final Divergence divergence : result.getExplained()) {
            byMechanism.computeIfAbsent(divergence.getMechanism().getName(), key -> new ArrayList<>())
                    .add(divergence);
        }

        out.append("EXPLAINED DIVERGENCES, by mechanism\n");
        if (byMechanism.isEmpty()) {
            out.append("  (none)\n");
        }
        for (final Map.Entry<String, List<Divergence>> entry : byMechanism.entrySet()) {
            final DivergenceMechanism mechanism = entry.getValue().get(0).getMechanism();
            out.append("  ").append(entry.getKey()).append('\n');
            out.append("    ").append(mechanism.getDescription()).append('\n');
            for (final Divergence divergence : entry.getValue()) {
                out.append("      ").append(divergence.getTestCase().getId())
                        .append("  <- ").append(divergence.getAttribution()).append('\n');
            }
            if (mechanism instanceof DivergenceMechanism.LedgeredFlake) {
                // The weakest explanation the lane can give, and it says so. A flake is the right answer for
                // a dirty CONTROL arm; reaching for it to explain a case that was green in the control and
                // red here is how a systematic divergence got counted as a coin toss once already.
                out.append("    CAUTION: a flake is the answer of last resort for a divergence. If these\n");
                out.append("    recur in consecutive runs, or a sibling case fails in the same\n");
                out.append("    parameterisation, they are not flaking - triage them.\n");
            }
        }
        out.append('\n');

        out.append("UNEXPLAINED DIVERGENCES - these are what make this lane red\n");
        if (result.getUnexplained().isEmpty()) {
            out.append("  (none)\n");
        }
        for (final Divergence divergence : result.getUnexplained()) {
            final SurefireArm.Case testCase = divergence.getTestCase();
            out.append("  ").append(testCase.getId()).append('\n');
            out.append("    outcome: ").append(testCase.getOutcome())
                    .append("   type: ").append(testCase.getFailureType()).append('\n');
            out.append("    message: ").append(oneLine(testCase.getFailureMessage())).append('\n');
            out.append("    first frames:\n");
            appendFirstFrames(out, testCase.getFailureDetail());
        }
        out.append('\n');

        out.append("FAILING IN BOTH ARMS - not attributable to the seam\n");
        if (result.getFailingInBothArms().isEmpty()) {
            out.append("  (none)\n");
        }
        for (final SurefireArm.Case testCase : result.getFailingInBothArms()) {
            out.append("  ").append(testCase.getId()).append("   ")
                    .append(oneLine(testCase.getFailureMessage())).append('\n');
        }
        out.append('\n');

        if (!result.getPassingOnlySeamOn().isEmpty()) {
            out.append("PASSING ONLY WITH THE SEAM ON - the control arm is not clean, look at it\n");
            for (final SurefireArm.Case testCase : result.getPassingOnlySeamOn()) {
                out.append("  ").append(testCase.getId()).append("   ")
                        .append(oneLine(testCase.getFailureMessage())).append('\n');
            }
            out.append('\n');
        }

        if (!result.getMechanismsThatMatchedNothing().isEmpty()
                || !result.getLedgerEntriesThatMatchedNothing().isEmpty()) {
            out.append("EXPECTATIONS THAT EXPLAINED NOTHING THIS RUN - advisory, and usually good news\n");
            for (final DivergenceMechanism mechanism : result.getMechanismsThatMatchedNothing()) {
                out.append("  mechanism ").append(mechanism.getName()).append(" - ")
                        .append(mechanism.getDescription()).append('\n');
            }
            for (final String entry : result.getLedgerEntriesThatMatchedNothing()) {
                out.append("  ledger entry ").append(entry)
                        .append("  (the case no longer diverges - delete the entry, or find out why)\n");
            }
            out.append('\n');
        }

        out.append("VERDICT: ");
        if (!result.getUnexplainedControlArmFailures().isEmpty()) {
            out.append("the CONTROL arm has failures nobody has diagnosed, so there is nothing trustworthy "
                    + "to difference against. Diagnose them, or record them in the inflight ledger with a "
                    + "flaky-case marker. This verdict outranks whatever the divergence set says.");
        } else {
            out.append(result.getUnexplained().isEmpty()
                    ? "every divergence is explained by a named mechanism."
                    : "unexplained divergences present - each one above is either a mechanism nobody has "
                            + "named yet or a regression.");
        }
        out.append('\n');
        return out.toString();
    }

    private static void appendArmShape(final StringBuilder out, final SurefireArm arm) {
        int failed = 0;
        int skipped = 0;
        for (final SurefireArm.Case testCase : arm.getCases()) {
            if (testCase.isFailed()) {
                failed++;
            } else if (testCase.getOutcome() == SurefireArm.Outcome.SKIPPED) {
                skipped++;
            }
        }
        // Printed, never asserted on. The counts move with the branch, the Kafka version and the seam, so
        // they are output for a reader to re-derive from - and this lane's verdict does not consult them.
        out.append(String.format(Locale.ROOT, "  %-10s %d cases, %d failing, %d skipped, %d classes%n",
                arm.getArmName(), arm.getCases().size(), failed, skipped, arm.getClassNames().size()));
    }

    private static void appendFirstFrames(final StringBuilder out, final String detail) {
        int printed = 0;
        for (final String line : detail.split("\n")) {
            final String trimmed = line.trim();
            if (!trimmed.startsWith("at ")) {
                continue;
            }
            out.append("      ").append(trimmed).append('\n');
            if (++printed == 6) {
                return;
            }
        }
        if (printed == 0) {
            out.append("      (no stack recorded)\n");
        }
    }

    private static String oneLine(final String text) {
        final String collapsed = text.replace('\n', ' ').replace('\r', ' ').trim();
        return collapsed.length() <= 300 ? collapsed : collapsed.substring(0, 300) + "...";
    }
}
