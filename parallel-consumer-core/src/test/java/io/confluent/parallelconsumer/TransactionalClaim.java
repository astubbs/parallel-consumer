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
            Status.REFUTED, "REFUTED by counterexample, downgraded from PROVED after U5. It holds for every path "
            + "U4 exercises - TransactionalVisibilityIT#openTransactionIsInvisibleAtReadCommittedAndVisibleAtReadUncommitted "
            + "and #readCommittedIsBlockedAtTheFirstStillOpenTransactionNotMerelyFiltered, each with an observed "
            + "control (flipping isolation.level to read_uncommitted turns them red). But none of those arms makes a "
            + "send FAIL, and that is the case the sentence does not survive: "
            + "TransactionalBatchVisibilityIT#aTerminallyFailedSendLeavesTheWholeTransactionInvisible observes "
            + "'poison-key-0 has 2 of 5' - two records from ONE source offset visible at read_committed while three "
            + "are not, 2/2 reproductions. Cause is shared with C7: ProducerManager#produceMessages installs a "
            + "Callback that THROWS from onCompletion (its own comment says it is 'only needed if not using tx'), "
            + "and KafkaProducer#doSend runs that callback inside its catch(ApiException) block BEFORE calling "
            + "transactionManager.maybeTransitionToErrorState, so the throw escapes, the transaction is never marked "
            + "abortable, and the next commit publishes the partial set. Whether to correct the javadoc or fix the "
            + "abort is a maintainer decision; the register records only that the sentence as published is false"),

    /**
     * C3 - a failed or crashed transaction is never visible, and is retried as a new transaction whose record
     * grouping need not match the original.
     */
    FAILURE_INVISIBLE_AND_RECOMBINED(Source.OPTIONS_JAVADOC,
            "none will ever be visible and the system will eventually retry them in new transactions - "
                    + "potentially with different combinations of records from the original.",
            Status.PROVED, "TransactionalCrashReplayIT#abandonedTransactionIsInvisibleAndTheReplacementFencesItsProducer "
            + "covers the invisibility half (an instance abandoned mid-transaction, its records seen by a "
            + "read_uncommitted control arm and never by a read_committed one), and "
            + "#theReplayRecombinesTheSameResultsIntoDifferentTransactions covers the recombination half - the "
            + "abandoned attempt held every payload result in ONE transaction, the replay spread the same results "
            + "over several, and their union is exactly the expected set. Negative control observed (U13): "
            + "replacing the shared, stable transactional.id with KafkaClientUtils' default random one - one term, "
            + "everything else identical - turned it red at assertTheAbandonedProducerWasFenced, because the "
            + "replacement no longer fences its predecessor and the abandoned instance is never refused. That is "
            + "the control that matters here: without it, 'nothing is visible' would also be satisfied by an "
            + "unfenced open transaction pinning the last stable offset"),

    /**
     * C4 - the source offset and its produced records commit together or not at all.
     */
    OFFSET_AND_RECORDS_ATOMIC(Source.OPTIONS_JAVADOC,
            "A source offset, and it's produced records will be committed as an atomic set.",
            Status.PROVED, "TransactionalCrashReplayIT#replayCommitsTheResultsAndTheirSourceOffsetTogether asserts "
            + "both halves at both ends of a crash: before, no payload result visible AND the source offset still "
            + "on the priming record; after, every result visible AND the offset moved to the end of the input. A "
            + "system committing the offset without the records, or the records without the offset, fails one of "
            + "the four. Record counts come from what the verifier consumed, never from offsets (markers occupy "
            + "offsets); the only offset read is the group's committed position on the non-transactional INPUT "
            + "topic. Negative control observed (U13): the same random-transactional.id control recorded on "
            + "FAILURE_INVISIBLE_AND_RECOMBINED - with no fencing the abandoned instance is never refused and the "
            + "test goes red before the replay begins. VALID ONLY AT batchSize=1: at batchSize>=2 the produce-lock "
            + "double-release stops the instance committing at all, which is recorded under "
            + "RESULTS_EXACTLY_ONCE_UNDER_FAILURE and in docs/inflight/bug-producing-lock-double-release.md"),

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
            Status.KAFKA_GUARANTEE, "broker behaviour, surfaced by our docs - reported, not enforced. Tested once "
            + "for the record by TransactionalVisibilityIT#readCommittedIsBlockedAtTheFirstStillOpenTransactionNotMerelyFiltered, "
            + "which distinguishes blocking from filtering: with one transaction open and a LATER one committed, the "
            + "read_committed arm sees neither while the read_uncommitted arm sees both. Stays KAFKA_GUARANTEE "
            + "regardless: no change to this repository can break it"),

    /**
     * C7 - pollAndProduceMany is all-or-none across the whole produced set.
     */
    PRODUCE_MANY_ALL_OR_NONE(Source.OPTIONS_JAVADOC,
            "all records must have been produced successfully to the broker before the transaction will commit, "
                    + "after which all will be visible together, or none.",
            Status.REFUTED, "SECOND HALF HOLDS, FIRST HALF REFUTED. "
            + "TransactionalBatchVisibilityIT#everyResultSetForAnInputRecordIsVisibleInFullOrNotAtAll proves "
            + "'after which all will be visible together, or none': 20 input records x 5 results each at "
            + "batchSize=1, with a read_committed verifier polling continuously THROUGHOUT the run and failing "
            + "the instant any input record's result set is seen part-visible - not merely asserted complete at "
            + "the end, which a system with no atomicity would also satisfy. Negative control observed and kept "
            + "as a permanent running test, #thePartialResultSetAssertionRejectsASetSplitAcrossTwoTransactions: "
            + "the same five records committed as two transactions instead of one - one term, everything else "
            + "identical - makes the same assertion throw naming 'split-key has 2 of 5', and it then accepts the "
            + "set once completed. "
            + "But 'all records must have been produced successfully to the broker before the transaction will "
            + "commit' is FALSE. #aTerminallyFailedSendLeavesTheWholeTransactionInvisible feeds one input record "
            + "whose middle result is 2MB and so is rejected against max.request.size: results 0 and 1 are "
            + "accepted into the open transaction, result 2 fails, results 3 and 4 are never attempted - and the "
            + "next commit SUCCEEDS, making result|poison-key-0|0 and |1 visible at read_committed. Two of five "
            + "records from one source offset, visible. The instance neither failed nor shut down. Mechanism: "
            + "ProducerManager#produceMessages installs a Callback - the one its own comment calls 'only needed "
            + "if not using tx' - that throws InternalRuntimeException from Callback#onCompletion; "
            + "KafkaProducer#doSend invokes it from inside its catch (ApiException) handler and only AFTERWARDS "
            + "calls transactionManager.maybeTransitionToErrorState(e), so the throw escapes first and the "
            + "transaction is never moved to abortable-error. Nothing else in PC covers the case: a failed "
            + "WorkContainer leaves the already-sent records in the transaction, and only an abort removes them. "
            + "That arm ships @Disabled because this class runs in the gating lane. "
            + "KNOCK-ON: the same observation falsifies C2's sentence (ALL_OR_NONE_PER_SOURCE_OFFSET), which is "
            + "recorded PROVED on the strength of arms that never make a send fail. Re-triaging C2, and choosing "
            + "between correcting the javadoc and fixing the abort, is a maintainer decision, not this "
            + "register's"),

    /**
     * C8 - an aborted or timed-out transaction leaves nothing visible, ever.
     */
    ABORTED_NEVER_VISIBLE(Source.OPTIONS_JAVADOC,
            "Records produced into a transaction that gets aborted or timed out, will never be visible.",
            Status.PROVED, "TransactionalVisibilityIT#abortedTransactionRecordsAreNeverVisible covers the abort arm "
            + "(before and after the abort, the 'after' anchored on a sentinel committed post-abort so the verifier "
            + "demonstrably read past the aborted region), and #transactionThatExceedsItsTimeoutLeavesNoVisibleRecord "
            + "covers the timeout arm via a 2s transaction.timeout.ms. Negative control observed (U4): flipping the "
            + "verifying consumer's isolation.level to read_uncommitted turned the abort arm red on 'must not have "
            + "seen any aborted record' - so the invisibility is the isolation level's, and an aborted record really "
            + "is still sitting in the log"),

    /**
     * C9 - the exactly-once ordering invariant the produce/commit lock pair exists to protect.
     */
    NO_PRODUCE_WITHOUT_ITS_OFFSET(Source.OPTIONS_JAVADOC,
            "The system must prevent records from being produced to the brokers whose source consumer record "
                    + "offsets has not been included in this transaction.",
            Status.PROVED, "ProducerManagerTest#commitLockIsGrantedOnlyAfterTheProducedWorkReachesTheMailbox, with "
            + "ProducerManagerTest#producedRecordsCantBeInTransactionWithoutItsOffsetDirect covering the outcome and "
            + "the docs/plans/2026-08-03-001 §11 guard. Negative control observed (U3): releasing the produce lock "
            + "before the mailbox handoff, with the 400ms window §11's experiment used, failed 3/3 with 'the work "
            + "reaches the controller's mailbox only after its record was sent' - the commit had completed while the "
            + "work was still not in the mailbox. Position control: the same 400ms spent inside the lock, before the "
            + "handoff, passed 2/2, so it is the ordering and not the added latency"),

    /**
     * C10 - holding the commit lock stops processing for the duration of the commit.
     */
    PROCESSING_BLOCKED_DURING_COMMIT(Source.OPTIONS_JAVADOC,
            "This periodically slows down record production during this phase, by the time needed to commit the "
                    + "transaction.",
            Status.PROVED, "ProducerManagerTest#producingIsBlockedForTheDurationOfTheCommitAndResumesOnRelease, "
            + "with ProducerManagerTest#sendingGetsLockedInTx covering the same pair of transitions. Negative "
            + "control observed (U3): releasing the commit lock before the produce attempt starts made "
            + "beginProducing return in ~40ms instead of blocking, failing 2/2 on 'getElapsed() expected to be at "
            + "least PT1S'"),

    /**
     * C11 - already proved by {@code TransactionTimeoutsTest#commitTimeout}; U3 attributes it rather than
     * reproving it.
     */
    COMMIT_LOCK_TIMEOUT_FAILS_FAST(Source.OPTIONS_JAVADOC,
            "If the system cannot acquire the commit lock in time, it will shut down for whatever reason, the "
                    + "system will shut down (fail fast) - during the shutdown a final commit attempt will be made.",
            Status.COVERED_NO_CONTROL, "TransactionTimeoutsTest#commitTimeout, both timeout arms - attributed by U3, "
            + "not reproved. No negative control: the test needs a broker, so it is not in the lane U3 ran, and "
            + "breaking what it guards means changing the commit-lock timeout handling in ProducerManager itself. "
            + "The control recorded in that test (dropping its overlap latch) proves the test's own guard, not the "
            + "documented fail-fast behaviour"),

    /**
     * C12 - already proved by {@code TransactionTimeoutsTest#produceTimeout}; U3 attributes it rather than
     * reproving it.
     */
    PRODUCE_LOCK_TIMEOUT_RETRIES_RECORD(Source.OPTIONS_JAVADOC,
            "If the system cannot acquire the produce lock in time, it will fail the record processing and retry "
                    + "the record later.",
            Status.COVERED_NO_CONTROL, "TransactionTimeoutsTest#produceTimeout - attributed by U3, not reproved. No "
            + "negative control: broker-bound, so outside the lane U3 ran, and the retry it asserts is driven by a "
            + "5s sleep injected into the commit path rather than by anything U3 can flip"),

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
            Status.REFUTED, "REFUTED AT batchSize >= 2, holds at batchSize = 1. "
            + "TransactionalCrashReplayIT#outputHoldsEachResultExactlyOnceAcrossTheReplay passes 4/4 at "
            + "batchSize=1 across a real crash and replay (200 payload records, every input key demonstrably "
            + "reprocessed, each result present exactly once). The sibling arm "
            + "#outputHoldsEachResultExactlyOnceAcrossTheReplayWhenBatching fails 5/5 at batchSize=3, same volume "
            + "and machine, unloaded - and the refutation is by LIVENESS, not duplication: the produce lock is "
            + "acquired per PollContextInternal but released per WorkContainer, so every batch fails, only a "
            + "success sets a partition dirty (PartitionState#onFailure is a no-op), the commit gate ANDs "
            + "wm.isDirty(), and the instance therefore stops committing entirely - the replacement's source "
            + "offset froze at 3 of 201 for the whole await against a 200ms commit interval, with no commit-path "
            + "error, i.e. commits were never attempted rather than attempted and failing. The promised results "
            + "never come to exist. That arm ships @Disabled because this class runs in the gating lane; the fix "
            + "is d95a21d4 on fix/produce-lock-double-release. Full evidence, alternatives ruled out and the "
            + "n/N in docs/inflight/bug-producing-lock-double-release.md. Triage - correct the README or land the "
            + "fix - is a decision for the maintainer, not this register");

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
