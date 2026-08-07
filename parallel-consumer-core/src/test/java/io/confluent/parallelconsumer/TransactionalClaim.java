package io.confluent.parallelconsumer;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * The guarantees this project <em>documents</em> for {@link ParallelConsumerOptions.CommitMode#PERIODIC_TRANSACTIONAL_PRODUCER},
 * one constant per claim, each carrying the sentence it was taken from.
 * <p>
 * The point of naming them is that a test can reference a claim instead of restating it, and that
 * {@link TransactionalClaimCoverageTest} can then check two things no reviewer reliably checks by hand:
 * <ol>
 *     <li>every claim we say is covered actually has a test referencing it, and</li>
 *     <li>every recorded sentence still appears in the file it was taken from - so editing the javadoc without
 *     updating this register fails the build instead of silently leaving the register describing a promise the
 *     code no longer makes.</li>
 * </ol>
 * <p>
 * <strong>What the coverage check does and does not prove.</strong> It proves a claim is <em>referenced</em> by a
 * test. It cannot prove the claim is <em>proved</em> - a {@link ProvesClaim} annotation on a weak or gutted test
 * satisfies it forever. Proof strength lives entirely in the negative controls recorded per claim in
 * {@code docs/plans/2026-08-07-001-test-transactional-eos-battle-test-plan.md}: a claim is only
 * {@link Status#PROVED} once breaking the mechanism it guards was <em>observed</em> to turn its test red. Do not
 * read a green build as evidence that the guarantees still hold.
 */
public enum TransactionalClaim {

    /**
     * C1 - all parallel workers share one bulk transaction, not one transaction per record or per worker.
     */
    BULK_SHARED_TRANSACTION(Source.OPTIONS_JAVADOC,
            "Messages sent in parallel by different workers get added to the same transaction block",
            Status.PROVED, "TransactionalBulkCommitTest#recordsFromDifferentWorkersInOneIntervalShareOneTransaction. "
            + "Negative control observed (U2): splitting the four records across two commit cycles instead of one "
            + "turned it red with 'Wanted 1 time ... but was 2 times' on beginTransaction()"),

    /**
     * C2 - the all-or-none visibility guarantee, per source offset, at READ_COMMITTED.
     */
    ALL_OR_NONE_PER_SOURCE_OFFSET(Source.OPTIONS_JAVADOC,
            "All records produced from a given source offset will either all be visible, or none will be",
            Status.NOT_YET_COVERED, "owned by U4"),

    /**
     * C3 - a failed or crashed transaction is never visible, and is retried as a new transaction whose record
     * grouping need not match the original.
     */
    FAILURE_INVISIBLE_AND_RECOMBINED(Source.OPTIONS_JAVADOC,
            "none will ever be visible and the system will eventually retry them in new transactions - "
                    + "potentially with different combinations of records from the original.",
            Status.NOT_YET_COVERED, "owned by U13"),

    /**
     * C4 - the source offset and its produced records commit together or not at all.
     */
    OFFSET_AND_RECORDS_ATOMIC(Source.OPTIONS_JAVADOC,
            "A source offset, and it's produced records will be committed as an atomic set.",
            Status.NOT_YET_COVERED, "owned by U13"),

    /**
     * C5 - selecting transactional mode silently changes the commit interval default.
     */
    COMMIT_INTERVAL_AUTO_REDUCED(Source.OPTIONS_JAVADOC,
            "gets automatically reduced from the default of 5 seconds to 100ms",
            Status.COVERED_NO_CONTROL, "TransactionalBulkCommitTest#transactionalModeWithNoExplicitCommitIntervalResolvesTo100ms "
            + "and its two sibling arms. No negative control: breaking the mechanism means changing the resolution "
            + "in ParallelConsumerOptions#transactionsValidation itself, which U11 owns"),

    /**
     * C6 - this one is Kafka's guarantee, not ours. We document it, so we record it and test it once; we do not
     * gate the build on it, because no change to this repository can break it.
     */
    READ_COMMITTED_BLOCKED_TO_FIRST_OPEN_TX(Source.OPTIONS_JAVADOC,
            "blocked up to the offset of the first STILL open transaction",
            Status.KAFKA_GUARANTEE, "broker behaviour, surfaced by our docs - reported, not enforced"),

    /**
     * C7 - pollAndProduceMany is all-or-none across the whole produced set.
     */
    PRODUCE_MANY_ALL_OR_NONE(Source.OPTIONS_JAVADOC,
            "all records must have been produced successfully to the broker before the transaction will commit, "
                    + "after which all will be visible together, or none.",
            Status.NOT_YET_COVERED, "owned by U5"),

    /**
     * C8 - an aborted or timed-out transaction leaves nothing visible, ever.
     */
    ABORTED_NEVER_VISIBLE(Source.OPTIONS_JAVADOC,
            "Records produced into a transaction that gets aborted or timed out, will never be visible.",
            Status.NOT_YET_COVERED, "owned by U4"),

    /**
     * C9 - the exactly-once ordering invariant the produce/commit lock pair exists to protect.
     */
    NO_PRODUCE_WITHOUT_ITS_OFFSET(Source.OPTIONS_JAVADOC,
            "The system must prevent records from being produced to the brokers whose source consumer record "
                    + "offsets has not been included in this transaction.",
            Status.NOT_YET_COVERED, "owned by U3"),

    /**
     * C10 - holding the commit lock stops processing for the duration of the commit.
     */
    PROCESSING_BLOCKED_DURING_COMMIT(Source.OPTIONS_JAVADOC,
            "This periodically slows down record production during this phase, by the time needed to commit the "
                    + "transaction.",
            Status.NOT_YET_COVERED, "owned by U3"),

    /**
     * C11 - already proved by {@code TransactionTimeoutsTest#commitTimeout}; U3 attributes it rather than
     * reproving it.
     */
    COMMIT_LOCK_TIMEOUT_FAILS_FAST(Source.OPTIONS_JAVADOC,
            "If the system cannot acquire the commit lock in time, it will shut down for whatever reason, the "
                    + "system will shut down (fail fast) - during the shutdown a final commit attempt will be made.",
            Status.NOT_YET_COVERED, "owned by U3 - attribution of the existing TransactionTimeoutsTest#commitTimeout"),

    /**
     * C12 - already proved by {@code TransactionTimeoutsTest#produceTimeout}; U3 attributes it rather than
     * reproving it.
     */
    PRODUCE_LOCK_TIMEOUT_RETRIES_RECORD(Source.OPTIONS_JAVADOC,
            "If the system cannot acquire the produce lock in time, it will fail the record processing and retry "
                    + "the record later.",
            Status.NOT_YET_COVERED, "owned by U3 - attribution of the existing TransactionTimeoutsTest#produceTimeout"),

    /**
     * C13 - the documented cost of eager processing during commit.
     */
    EAGER_PROCESSING_MAY_REPLAY(Source.OPTIONS_JAVADOC,
            "this may cause side effect replay when the record is retried, otherwise there is no replay.",
            Status.NOT_YET_COVERED, "owned by U6"),

    /**
     * C14 - the README's own promise. Users read the README, not the javadoc, so it is registered separately: a
     * refuted guarantee must not stay published on the front page because only the javadoc was corrected.
     */
    RESULTS_EXACTLY_ONCE_UNDER_FAILURE(Source.README_TEMPLATE,
            "This means that even under failure, the results will exist exactly once in the Kafka output topic.",
            Status.NOT_YET_COVERED, "owned by U13");

    /**
     * Where a claim is published, and how to find the text that must still contain it.
     */
    public enum Source {
        /**
         * The region rendered into {@code README.adoc} from the options javadoc. Bounded by the asciidoc tag
         * markers, so a claim moved out of the tag - and therefore off the rendered page - fails the check even
         * though the sentence still exists in the file.
         */
        OPTIONS_JAVADOC("parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/ParallelConsumerOptions.java",
                "// tag::transactionalJavadoc[]", "// end::transactionalJavadoc[]"),

        /**
         * The hand-written companion prose. Checked whole-file: unlike the javadoc there is no tag marking what
         * gets published, because the whole template is.
         */
        README_TEMPLATE("src/docs/README_TEMPLATE.adoc", null, null);

        private final String repoRelativePath;
        private final String startMarker;
        private final String endMarker;

        Source(String repoRelativePath, String startMarker, String endMarker) {
            this.repoRelativePath = repoRelativePath;
            this.startMarker = startMarker;
            this.endMarker = endMarker;
        }

        public String getRepoRelativePath() {
            return repoRelativePath;
        }

        /**
         * The published text of this source, whitespace-normalised so a claim wrapped across javadoc lines still
         * matches the single-line sentence recorded here.
         */
        public String readPublishedText() {
            Path file = repoRoot().resolve(repoRelativePath);
            if (!Files.exists(file)) {
                throw new IllegalStateException("claim source file not found: " + file
                        + " - the register cannot check itself against a file it cannot read");
            }
            List<String> lines;
            try {
                lines = Files.readAllLines(file, StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new UncheckedIOException("could not read claim source " + file, e);
            }
            StringBuilder sb = new StringBuilder();
            boolean inRegion = startMarker == null;
            for (String line : lines) {
                if (startMarker != null && line.contains(startMarker)) {
                    inRegion = true;
                    continue;
                }
                if (endMarker != null && line.contains(endMarker)) {
                    inRegion = false;
                }
                if (inRegion) {
                    sb.append(stripJavadocPrefix(line)).append(' ');
                }
            }
            if (startMarker != null && sb.length() == 0) {
                throw new IllegalStateException("marker '" + startMarker + "' not found in " + file
                        + " - the tag was renamed or removed, so the register can no longer verify itself");
            }
            return normalise(sb.toString());
        }

        private static String stripJavadocPrefix(String line) {
            String trimmed = line.trim();
            if (trimmed.startsWith("*")) {
                return trimmed.substring(1);
            }
            return trimmed;
        }
    }

    /**
     * What we currently assert about a claim. Only {@link #isCoverageEnforced()} statuses fail the build when no
     * test references the claim - the others are reported, so that "not covered yet" and "not ours to prove" are
     * expressible without either lying or turning the build red.
     */
    public enum Status {
        /**
         * Covered by a test, and breaking the guarded mechanism was observed to turn that test red.
         */
        PROVED(true),
        /**
         * Covered by a test, and the test says the documented claim is false. The disposition - correct the docs
         * or file the defect - is a triage decision, not automatic.
         */
        REFUTED(true),
        /**
         * Covered by a test, but no negative control was produced, so the test has never been seen to fail. Not
         * counted as proved.
         */
        COVERED_NO_CONTROL(true),
        /**
         * The guarantee is Kafka's, surfaced by our documentation. Tested once for the record; not gated, because
         * no change here can break it.
         */
        KAFKA_GUARANTEE(false),
        /**
         * Owned by work that has not landed. Requires a reason naming the owner, so the status cannot be used to
         * park a claim silently.
         */
        NOT_YET_COVERED(false);

        private final boolean coverageEnforced;

        Status(boolean coverageEnforced) {
            this.coverageEnforced = coverageEnforced;
        }

        /**
         * @return true when a claim in this status must have at least one {@link ProvesClaim} reference, on pain
         *         of failing the build
         */
        public boolean isCoverageEnforced() {
            return coverageEnforced;
        }
    }

    private final Source source;
    private final String documentedSentence;
    private final Status status;
    private final String note;

    TransactionalClaim(Source source, String documentedSentence, Status status, String note) {
        this.source = source;
        this.documentedSentence = documentedSentence;
        this.status = status;
        this.note = note;
    }

    public Source getSource() {
        return source;
    }

    /**
     * @return the claim's sentence exactly as published, which {@link TransactionalClaimCoverageTest} requires to
     *         still be present in {@link #getSource()}
     */
    public String getDocumentedSentence() {
        return documentedSentence;
    }

    public Status getStatus() {
        return status;
    }

    /**
     * @return why the claim is in its current status - required for the non-enforced statuses so that parking a
     *         claim always leaves a reason behind
     */
    public String getNote() {
        return note;
    }

    /**
     * Collapses every run of whitespace to one space so a sentence wrapped across javadoc or asciidoc lines
     * compares equal to the single-line form recorded in this register.
     */
    public static String normalise(String text) {
        return text.replaceAll("\\s+", " ").trim();
    }

    /**
     * Walks up from the working directory to the repository root, identified by the README template the register
     * checks against. Surefire runs with the module directory as the working directory, and this class is
     * referenced from both the unit and the integration lane, so neither a module-relative nor an absolute path
     * would work from every caller.
     */
    static Path repoRoot() {
        Path dir = Paths.get("").toAbsolutePath();
        while (dir != null) {
            if (Files.exists(dir.resolve("src/docs/README_TEMPLATE.adoc"))) {
                return dir;
            }
            dir = dir.getParent();
        }
        throw new IllegalStateException("could not locate the repository root above "
                + Paths.get("").toAbsolutePath() + " - looked for src/docs/README_TEMPLATE.adoc");
    }
}
