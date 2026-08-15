package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.proxy.harness.HarnessScenario;
import bz.stub.parallelconsumer.proxy.harness.ProxyHarness;
import org.awaitility.Awaitility;

import java.time.Duration;
import java.util.List;
import java.util.OptionalLong;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The wired conformance set: every scenario the suite can drive a foreign runner through today, with its
 * prescription and its assertions.
 * <p>
 * <b>The assertions live here, in Java, for every language.</b> Offset frontiers, ordering, redelivery and
 * attempt counts are already understood on this side, by the code that owns the engine; re-expressing that
 * understanding in ten languages would produce ten subtly different definitions of correct, and the first
 * time two of them disagreed nobody could say which was wrong. Keeping it in one place also means a
 * sharpened assertion sharpens for every client at once.
 *
 * @author Antony Stubbs
 * @see ConformanceScenario
 */
public final class ConformanceScenarios {

    /** Every runner gets the same budget; a healthy run finishes in a fraction of it. */
    private static final Duration RUNNER_BUDGET = Duration.ofSeconds(60);

    /**
     * How long the negative control watches for a commit that must never come. Thirty of the contract's
     * 100ms commit intervals, so a client whose report DID land has had ample opportunity to advance the
     * offset - the proof that the window is not simply too short is that the positive scenarios, on the
     * same interval, converge inside it.
     */
    private static final Duration NO_COMMIT_SETTLE = Duration.ofSeconds(3);

    /**
     * The trivial baseline every language runs first: one record in, processed once, the committed offset
     * advances past it.
     */
    public static final ConformanceScenario PROCESSED_RECORD_ADVANCES_THE_COMMITTED_OFFSET =
            new ConformanceScenario(HarnessScenario.A_PROCESSED_RECORD_ADVANCES_THE_COMMITTED_OFFSET,
                    RunnerBehaviour.SUCCEED, 1, RUNNER_BUDGET,
                    (harness, transcript) -> {
                        assertWithMessage("deliveries the runner saw%s", transcript.diagnostics())
                                .that(transcript.dispatches()).hasSize(1);
                        var only = transcript.dispatches().get(0);
                        assertWithMessage("the seeded record's key%s", transcript.diagnostics())
                                .that(only.key()).isEqualTo("lone-key");
                        assertWithMessage("attempt on a first delivery%s", transcript.diagnostics())
                                .that(only.attempt()).isEqualTo(1);
                        // The engine-side truth no client can see: the frontier moved past the record.
                        harness.awaitCommittedOffset(1);
                    });

    /**
     * The negative control, and the load-bearing one: a client that never reports must leave the offset
     * where it was. A suite that cannot say no about a broken client would say yes about ten of them at
     * once.
     */
    public static final ConformanceScenario UNREPORTED_RECORD_HOLDS_BACK_THE_COMMIT =
            new ConformanceScenario(HarnessScenario.AN_UNREPORTED_RECORD_HOLDS_BACK_THE_COMMIT,
                    RunnerBehaviour.REPORT_NOTHING, 1, RUNNER_BUDGET,
                    (harness, transcript) -> {
                        // ARRIVAL-SYNC FIRST. "The offset never advanced" is vacuously true before the
                        // record is ever dispatched, so the observation line is what makes the assertion
                        // below mean something: the record reached a client, and was then not reported.
                        assertWithMessage("the record must reach the client before its silence can mean "
                                + "anything%s", transcript.diagnostics())
                                .that(transcript.dispatches()).hasSize(1);
                        assertNoCommitAdvancesPast(harness, transcript);
                    });

    /** A reported failure comes back, carrying the history it earned: attempt 2, and the reason verbatim. */
    public static final ConformanceScenario FAILED_RECORD_IS_REDELIVERED_WITH_ITS_FAILURE_HISTORY =
            new ConformanceScenario(HarnessScenario.A_FAILED_RECORD_IS_REDELIVERED_WITH_ITS_FAILURE_HISTORY,
                    RunnerBehaviour.FAIL_THEN_SUCCEED, 2, RUNNER_BUDGET,
                    (harness, transcript) -> {
                        assertWithMessage("deliveries the runner saw%s", transcript.diagnostics())
                                .that(transcript.dispatches()).hasSize(2);
                        var first = transcript.dispatches().get(0);
                        var redelivery = transcript.dispatches().get(1);

                        assertWithMessage("attempt on the first delivery%s", transcript.diagnostics())
                                .that(first.attempt()).isEqualTo(1);
                        assertWithMessage("a first delivery carries no failure history%s", transcript.diagnostics())
                                .that(first.reason()).isEmpty();

                        assertWithMessage("the redelivery is the same record%s", transcript.diagnostics())
                                .that(redelivery.offset()).isEqualTo(first.offset());
                        assertWithMessage("attempt on the redelivery%s", transcript.diagnostics())
                                .that(redelivery.attempt()).isEqualTo(2);
                        assertWithMessage("the redelivery carries the previous reason VERBATIM%s",
                                transcript.diagnostics())
                                .that(redelivery.reason()).isEqualTo(RunnerContract.PRESCRIBED_FAILURE_REASON);

                        harness.awaitCommittedOffset(1);
                    });

    /**
     * <b>A client test, not a shard-selection test.</b> What it catches is a client that can only run one
     * record at a time - an admin loop that head-of-line-blocks, a queue that hands out wrongly, a worker
     * pool of one - which is a defect no other scenario here can see. Shard selection itself is the engine's,
     * and is tested far more cheaply from inside the JVM.
     * <p>
     * The instrument is the PRESCRIBED HOLD: the runner does not report its first record until a second
     * arrives. A client that cannot run two at once therefore never gets its second, and exits 1 having said
     * so - which is where this scenario's red actually comes from.
     * <p>
     * <b>Measured, and worth knowing before strengthening this:</b> removing the hold leaves every assertion
     * below still true, because the engine dispatches both shards in one wave regardless. So the transcript
     * assertions are corroboration - they catch a client reporting a record whose function has not returned -
     * while the load-bearing half is the hold plus the runner reaching the end at all. A serialized client
     * was the sabotage that proved it red.
     * <p>
     * The seeds are {@code shared@0}, {@code shared@1}, {@code distinct@2}. Whichever shard is dispatched
     * first, the record that arrives WHILE the first is held must be the other key: {@code shared@1} is
     * fenced behind {@code shared@0}, and the distinct key is not.
     */
    public static final ConformanceScenario RECORDS_SHARING_A_KEY_SHARE_A_SHARD =
            new ConformanceScenario(HarnessScenario.RECORDS_SHARING_A_KEY_SHARE_A_SHARD_DISTINCT_KEYS_RUN_CONCURRENTLY,
                    RunnerBehaviour.HOLD_FIRST_UNTIL_SECOND, 3, RUNNER_BUDGET,
                    (harness, transcript) -> {
                        assertWithMessage("deliveries the runner saw%s", transcript.diagnostics())
                                .that(transcript.dispatches()).hasSize(3);

                        // DISTINCT KEYS RUN CONCURRENTLY: something was dispatched while the first record
                        // was still outstanding, and it was the other key.
                        assertWithMessage("the second delivery arrived while the first was held, so it must "
                                + "carry the OTHER key - a same-key record here would mean the shard did not "
                                + "serialize%s", transcript.diagnostics())
                                .that(transcript.dispatches().get(1).key())
                                .isNotEqualTo(transcript.dispatches().get(0).key());

                        // RECORDS SHARING A KEY SHARE A SHARD: the second 'shared' record could not be
                        // dispatched until the first was reported, so it is last in the transcript.
                        var shared = transcript.dispatchesForKey("shared");
                        assertWithMessage("both 'shared' records were delivered%s", transcript.diagnostics())
                                .that(shared).hasSize(2);
                        assertWithMessage("the shard delivered 'shared' in offset order%s", transcript.diagnostics())
                                .that(List.of(shared.get(0).offset(), shared.get(1).offset()))
                                .isEqualTo(List.of(0L, 1L));
                        assertWithMessage("the second 'shared' record must be the LAST delivery: it was fenced "
                                + "behind the held one%s", transcript.diagnostics())
                                .that(transcript.dispatches().get(2).key()).isEqualTo("shared");

                        harness.awaitCommittedOffset(3);
                    });

    /**
     * The scenarios this suite drives today, in the order a new language should attempt them.
     * <p>
     * {@code ScenarioCoverageTest} holds this list against {@link HarnessScenario#conformanceScenarios()},
     * so a scenario the harness grows cannot quietly go undriven.
     */
    public static List<ConformanceScenario> all() {
        return List.of(
                PROCESSED_RECORD_ADVANCES_THE_COMMITTED_OFFSET,
                UNREPORTED_RECORD_HOLDS_BACK_THE_COMMIT,
                FAILED_RECORD_IS_REDELIVERED_WITH_ITS_FAILURE_HISTORY,
                RECORDS_SHARING_A_KEY_SHARE_A_SHARD);
    }

    /**
     * Watches for a commit that must never come. Written as "never advanced past record zero" rather than
     * "never committed at all", because a commit of offset 0 says only that the engine's commit loop ran -
     * it is the frontier reaching 1 that would mean the unreported record had been counted as done.
     */
    private static void assertNoCommitAdvancesPast(ProxyHarness harness, RunnerTranscript transcript) {
        Awaitility.await()
                .during(NO_COMMIT_SETTLE)
                .atMost(NO_COMMIT_SETTLE.plusSeconds(5))
                .untilAsserted(() -> {
                    OptionalLong committed = harness.lastCommittedOffset();
                    assertWithMessage("the committed offset must not advance past a record no client "
                            + "reported%s", transcript.diagnostics())
                            .that(committed.orElse(0L)).isLessThan(1L);
                });
    }

    private ConformanceScenarios() {
    }
}
