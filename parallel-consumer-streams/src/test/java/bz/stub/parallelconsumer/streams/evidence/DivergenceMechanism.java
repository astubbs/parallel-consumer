package bz.stub.parallelconsumer.streams.evidence;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.streams.PcUnsupportedConstruct;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A named reason a case that passes with the seam off can legitimately fail with it on.
 * <p>
 * <b>Where a mechanism can read the failure's own evidence, it does, and never its name.</b> That rule is
 * what keeps this lane useful while the module is still being built: the semantics under it are changing on
 * several branches at once, so an expectation written as a count is stale before it is reviewed, and one
 * written as a list of names goes stale the moment a rung fixes something. {@link RefusedConstruct} is the
 * model - it derives its marker and its attribution from {@link PcUnsupportedConstruct} at run time, so the
 * refusal envelope can gain or lose a construct with no edit here at all.
 * <p>
 * <b>What cannot be derived is attributed in the ledger, not here.</b> "This assertion fails because Kafka's
 * test drives the task synchronously" is a judgement about a test, not a fact recoverable from
 * {@code expected: <5> but was: <0>}. Those live in {@code docs/inflight/}, are read by
 * {@link LedgeredTriage}, and are reviewed as prose by whoever changes the semantics under them.
 * <p>
 * The corollary is the honest one: <b>a divergence matching no mechanism is unexplained, and unexplained
 * fails the lane.</b> Explaining it means naming its mechanism, not widening a predicate until it swallows
 * everything - the check on a predicate's breadth is that sabotaging a semantic still lands in the
 * unexplained pile.
 *
 * @author Antony Stubbs
 * @see SeamOnDivergenceClassifier
 */
public interface DivergenceMechanism {

    /** Short, stable, and how the lane's output refers to this class of divergence. */
    String getName();

    /** One line: what makes a failure of this shape expected rather than a regression. */
    String getDescription();

    /**
     * @return what this mechanism can say about the case - the specific construct, the specific ledger entry -
     *         or {@code null} if the case is not an instance of this mechanism.
     */
    String attribute(SurefireArm.Case testCase);

    /**
     * The mechanisms this lane knows, in the order they are tried against a DIVERGENCE.
     * <p>
     * Order is attribution, not verdict - a case explained by an earlier mechanism is never offered to a
     * later one - but one placement is load-bearing. {@link LedgeredFlake} is <b>last</b>, because letting a
     * known-flaky name explain a seam-on divergence is exactly how a systematic one disappears: it happened
     * on the branch forest, where a case that diverges every run under one parameter was counted as the
     * two-in-five thread-name flake. On the CONTROL arm the same registry is consulted first, which is the
     * job it is actually good at.
     */
    static List<DivergenceMechanism> registry(final InflightMarkers ledger) {
        final List<DivergenceMechanism> mechanisms = new ArrayList<>();
        mechanisms.add(new RefusedConstruct());
        mechanisms.add(new CommitFrontierEncoding());
        mechanisms.addAll(LedgeredTriage.from(ledger));
        mechanisms.add(new LedgeredFlake(ledger));
        return Collections.unmodifiableList(mechanisms);
    }

    /**
     * The refusal envelope firing, which is the module working rather than diverging.
     * <p>
     * <b>Derived entirely from {@link PcUnsupportedConstruct} at run time</b> - the marker is the common
     * prefix of the messages the enum itself produces, and the attribution is the display name of whichever
     * constant the message names. Nothing here is a copy of a refusal string, so a construct added to or
     * removed from the envelope changes what this mechanism recognises with no edit in this file. That
     * matters because the envelope belongs to a different rung and moves independently of this lane.
     */
    final class RefusedConstruct implements DivergenceMechanism {

        private final String marker = commonPrefixOfEveryRefusalMessage();

        private static String commonPrefixOfEveryRefusalMessage() {
            String prefix = null;
            for (final PcUnsupportedConstruct construct : PcUnsupportedConstruct.values()) {
                final String describe = construct.describe();
                if (prefix == null) {
                    prefix = describe;
                    continue;
                }
                int shared = 0;
                while (shared < prefix.length() && shared < describe.length()
                        && prefix.charAt(shared) == describe.charAt(shared)) {
                    shared++;
                }
                prefix = prefix.substring(0, shared);
            }
            if (prefix == null || prefix.isEmpty()) {
                throw new IllegalStateException("PcUnsupportedConstruct produced no shared refusal marker, so "
                        + "this mechanism cannot recognise a refusal by its own message. Either the enum is "
                        + "empty or the message shape changed - fix the derivation, do not hardcode a string.");
            }
            return prefix;
        }

        @Override
        public String getName() {
            return "refused-construct";
        }

        @Override
        public String getDescription() {
            return "the refusal envelope threw, because Kafka's own test builds a construct this module "
                    + "refuses on the PC path - the envelope working, not a divergence in behaviour";
        }

        @Override
        public String attribute(final SurefireArm.Case testCase) {
            if (!testCase.getFailureDetail().contains(marker)) {
                return null;
            }
            final Set<String> named = new LinkedHashSet<>();
            for (final PcUnsupportedConstruct construct : PcUnsupportedConstruct.values()) {
                if (testCase.getFailureDetail().contains(construct.getDisplayName())) {
                    named.add(construct.getDisplayName());
                }
            }
            if (named.isEmpty()) {
                // The marker is there but no constant claims it. Reported rather than swallowed: a refusal
                // message naming nothing recognisable is itself a finding.
                return "a refusal message naming no known construct";
            }
            return String.join(", ", named);
        }
    }

    /**
     * A deliberate divergence in what a committed offset means.
     * <p>
     * Stock Kafka Streams commits the offset after the last record {@code PartitionGroup.nextRecord()} handed
     * out, because on a serial path that record is also the last one finished. Under concurrent dispatch those
     * are different records, so this module commits Parallel Consumer's frontier - the point below which
     * nothing is outstanding - and encodes what is still in flight above it. Kafka's own tests assert the
     * stock encoding, so they read a different offset and a different metadata payload.
     * <p>
     * Recognised from the failure's own rendering: an assertion that prints an {@code OffsetAndMetadata} is an
     * assertion about the commit encoding, whatever the case is called.
     * <p>
     * <b>Its limitation is stated rather than hidden.</b> An offset REGRESSION inside these unit tests renders
     * the same way and would be explained by this mechanism too. Nothing in a Kafka unit test's assertion text
     * separates "we commit a different offset on purpose" from "we commit the wrong offset", so this lane does
     * not pretend to. What defends offset correctness is the broker-backed commit-frontier law, which asserts
     * no loss and no gap across a real crash and restart rather than comparing an encoded string.
     */
    final class CommitFrontierEncoding implements DivergenceMechanism {

        /** The type Kafka's tests render when they assert a committed offset. */
        private static final String RENDERED_TYPE =
                org.apache.kafka.clients.consumer.OffsetAndMetadata.class.getSimpleName() + "{";

        @Override
        public String getName() {
            return "commit-frontier-encoding";
        }

        @Override
        public String getDescription() {
            return "the case asserts stock Kafka Streams' offset or commit-metadata encoding, which the seam "
                    + "deliberately diverges from - PC's frontier is not Streams' consumedOffsets. NOTE: an "
                    + "offset regression would land here too; the broker-backed commit-frontier law is what "
                    + "separates the two";
        }

        @Override
        public String attribute(final SurefireArm.Case testCase) {
            if (!testCase.getFailureMessage().contains(RENDERED_TYPE)) {
                return null;
            }
            return "asserts a committed " + RENDERED_TYPE.replace("{", "");
        }
    }

    /**
     * A mechanism whose membership is attributed in {@code docs/inflight/}, because it cannot be read off a
     * failure.
     * <p>
     * One instance per {@code seam-on-divergence-class:} marker. Cases join it through
     * {@code seam-on-divergence:} markers keyed by method - never by parameterisation, because which
     * parameter loses a race is not a property of the diagnosis.
     */
    final class LedgeredTriage implements DivergenceMechanism {

        private final String name;

        private final String description;

        private final String note;

        private final Set<String> methods = new LinkedHashSet<>();

        LedgeredTriage(final String name, final String description, final String note) {
            this.name = name;
            this.description = description;
            this.note = note;
        }

        /**
         * Build one mechanism per declared class, and refuse a case whose class was never declared.
         * <p>
         * An entry pointing at an undeclared class is a triage with no reason attached, which is the failure
         * mode this whole arrangement exists to prevent - so it throws rather than quietly explaining a
         * divergence with a name and nothing behind it.
         */
        static List<LedgeredTriage> from(final InflightMarkers ledger) {
            final Map<String, LedgeredTriage> byName = new LinkedHashMap<>();
            for (final InflightMarkers.Marker marker : ledger.withKey("seam-on-divergence-class")) {
                final int split = marker.getValue().indexOf('=');
                if (split < 0) {
                    throw new IllegalStateException("A seam-on-divergence-class marker in " + marker.getNote()
                            + " has no '= <description>': " + marker.getValue());
                }
                final String name = marker.getValue().substring(0, split).trim();
                final String description = marker.getValue().substring(split + 1).trim();
                byName.put(name, new LedgeredTriage(name, description, marker.getNote()));
            }
            for (final InflightMarkers.Marker marker : ledger.withKey("seam-on-divergence")) {
                final int split = marker.getValue().indexOf('=');
                if (split < 0) {
                    throw new IllegalStateException("A seam-on-divergence marker in " + marker.getNote()
                            + " has no '= <class>': " + marker.getValue());
                }
                final String method = marker.getValue().substring(0, split).trim();
                final String className = marker.getValue().substring(split + 1).trim();
                final LedgeredTriage mechanism = byName.get(className);
                if (mechanism == null) {
                    throw new IllegalStateException(marker.getNote() + " attributes " + method + " to the "
                            + "divergence class '" + className + "', which no seam-on-divergence-class marker "
                            + "declares. A case attributed to a class with no stated mechanism is a name, not "
                            + "an explanation.");
                }
                mechanism.methods.add(method);
            }
            return new ArrayList<>(byName.values());
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getDescription() {
            return description + "  [attributed in " + note + "]";
        }

        @Override
        public String attribute(final SurefireArm.Case testCase) {
            final String method = testCase.getClassName() + "#" + methodOf(testCase.getName());
            return methods.contains(method) ? note : null;
        }

        /** The set this mechanism claims, so the lane can report an entry that has stopped matching. */
        public Set<String> getMethods() {
            return Collections.unmodifiableSet(methods);
        }
    }

    /**
     * A flake already diagnosed and recorded in {@code docs/inflight/}, matched from the note's own marker.
     * <p>
     * <b>This is the alternative to re-running a ten-minute lane until it is green.</b> A retry destroys the
     * signal; this relocates it to the ledger, where the diagnosis lives, and lets the lane name the note that
     * explains a dirty control arm and carry on. A control-arm failure with no ledger entry is not explained
     * away - it stops the lane, because an unexplained failure in the control means there is nothing to
     * difference against.
     */
    final class LedgeredFlake implements DivergenceMechanism {

        private final InflightMarkers ledger;

        private final Map<String, String> byMethod = new LinkedHashMap<>();

        LedgeredFlake(final InflightMarkers ledger) {
            this.ledger = ledger;
            for (final InflightMarkers.Marker marker : ledger.withKey("flaky-case")) {
                byMethod.put(marker.getValue().trim(), marker.getNote());
            }
        }

        @Override
        public String getName() {
            return "ledgered-flake";
        }

        @Override
        public String getDescription() {
            return "already diagnosed and recorded in the inflight ledger, matched from the note's own "
                    + "flaky-case marker rather than from a list kept in this lane (ledger: "
                    + ledger.describeSource() + ")";
        }

        @Override
        public String attribute(final SurefireArm.Case testCase) {
            return byMethod.get(testCase.getClassName() + "#" + methodOf(testCase.getName()));
        }
    }

    /**
     * The method a surefire case name belongs to, with the parameterisation and the argument list stripped.
     * <p>
     * A ledger entry names a method because the diagnosis is about the method; which parameter it lost on is
     * a property of the run.
     */
    static String methodOf(final String caseName) {
        int end = caseName.length();
        final int bracket = caseName.indexOf('[');
        if (bracket >= 0) {
            end = bracket;
        }
        final int paren = caseName.indexOf('(');
        if (paren >= 0 && paren < end) {
            end = paren;
        }
        return caseName.substring(0, end);
    }
}
