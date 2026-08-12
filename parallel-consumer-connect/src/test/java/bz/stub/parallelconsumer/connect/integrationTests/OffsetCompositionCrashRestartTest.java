package bz.stub.parallelconsumer.connect.integrationTests;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.connect.PcSinkTaskDurabilityBarrier.ConfirmationRule;
import bz.stub.parallelconsumer.connect.PcSinkTaskLane;
import bz.stub.parallelconsumer.connect.PcSinkTaskLaneRouter;
import bz.stub.parallelconsumer.integrationTests.BrokerIntegrationTest;
import bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import bz.stub.parallelconsumer.streams.PcTaskDispatcher;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import pl.tlinkowski.unij.api.UniLists;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The crash-safety half of U3 in
 * {@code docs/plans/2026-08-10-001-investigate-connect-offset-composition.md}: the arms in
 * {@code OffsetCompositionProbeTest} prove the composition rule is internally sound against a model of
 * Connect assembled by reading its source. This one carries that rule's answers all the way into a
 * <b>real consumer-group commit</b> and a <b>real restart resume point</b>.
 *
 * <p><b>Read this before quoting a green run as evidence.</b> There is no Connect runtime here: no worker,
 * no converter, no connector lifecycle, no rebalance. {@code PcConnectDispatchBridge.enabled()} still
 * returns a hard-coded {@code false}. The poll/dispatch/durability/commit loop below is written by hand and
 * the sink is this file's own {@link TopicSinkTask}, so the driver and the sink <em>are</em> the model that
 * reading {@code WorkerSinkTask} produced - only the broker, the commit and the resume are real. What this
 * arm therefore evidences is that {@link bz.stub.parallelconsumer.connect.PcSinkTaskDurabilityBarrier}'s
 * confirmations reach broker state correctly. It does <em>not</em> evidence the reading of
 * {@code WorkerSinkTask} that the barrier was designed against; that stays argued, not executed.
 *
 * <p>The sink is a Kafka topic because it is durable and independently readable: after the crash we can ask
 * what the sink really wrote rather than asking the sink, which would not have survived.
 *
 * <p><b>Nor does it evidence the barrier under concurrency.</b> The driver runs {@code runDurabilityCycle()}
 * inline on its own thread, which both {@code PcSinkTaskLaneRouter} and {@code PcSinkTaskDurabilityBarrier}
 * document as the wrong thread for it, and which the investigation's U1.4 decided against. That serialises
 * the cycle against dispatch, so the one window the design is nervous about - {@code pollWatermarks()} is
 * deliberately unsynchronized between two synchronized halves - is never entered by a second cycle here.
 *
 * <h2>Three arms, each varying exactly one term</h2>
 * <ul>
 *   <li>{@link #aCommitNeverCoversARecordNoLaneDurablyWrote()} - sound rule, sink refuses the parked record.
 *       The committed offset must be the parked record's own.</li>
 *   <li>{@link #theFrontierAdvancesOnceEveryLaneWritesItsRecords()} - sound rule, sink refuses <b>nothing</b>.
 *       The trigger-removed control: without it, a barrier that confirmed <em>nothing at all</em> would
 *       produce exactly the state the first arm asserts, and green would mean "the frontier never moved"
 *       rather than "the frontier stopped where it should".</li>
 *   <li>{@link #negativeControlHighestWatermarkOverCommitsAndLosesTheParkedRecord()} - sink refuses the
 *       parked record, but the confirmation rule is inverted. The over-commit and the resulting data loss
 *       must both show up on the broker.</li>
 * </ul>
 * Arms 2 and 3 each differ from arm 1 in one term only - the sink's refusal, and the confirmation rule -
 * per {@code docs/solutions/best-practices/control-arms-vary-exactly-one-term.md}.
 */
@Slf4j
@Isolated
class OffsetCompositionCrashRestartTest extends BrokerIntegrationTest<String, String> {

    /** The record the sink refuses to write. First in the partition, so it is the frontier. */
    private static final String PARKED_VALUE = "parked";

    private static final int FAST_RECORDS = 8;
    /** The parked record plus the fast ones - offsets 0..8. */
    private static final int TOTAL_RECORDS = FAST_RECORDS + 1;
    private static final int LANES = 4;
    private static final int POOL_SIZE = 4;

    /**
     * How long each arm waits for the restart to be handed something, and how long it reads the sink for.
     * Shared by every arm on purpose: two of them assert on an <em>absent</em> record, so a shorter deadline
     * there would be a second varying term and the comparison would stop being a control.
     */
    private static final Duration READ_BACK_WAIT = Duration.ofSeconds(30);

    /**
     * Budget for confirming an emptiness the broker has already proven structurally. Short on purpose: the
     * proof is the committed offset against the log end, and this poll only checks the proof against reality.
     */
    private static final Duration EMPTY_CONFIRM_WAIT = Duration.ofSeconds(2);

    /** Consecutive quiet passes - nothing newly confirmed, nothing left dirty - before the driver stops. */
    private static final int SETTLED_PASSES = 3;

    @Test
    void aCommitNeverCoversARecordNoLaneDurablyWrote() {
        final Outcome outcome = runScenario(ConfirmationRule.OWNING_LANE, true);
        assertRunIsMeaningful(outcome);

        assertThat(outcome.committed.offset())
                .as("offset 0 is the parked record and no lane ever declared it durable, so the committed "
                        + "offset must be 0 - anything higher records it as done, and a crash there loses it")
                .isEqualTo(0L);

        assertThat(outcome.committed.metadata())
                .as("THE HOLES, and the half of the frontier design only PC can express: the committed "
                        + "offset is 0, but the lanes that ran ahead really did complete records beyond it, "
                        + "and PC encodes those into the commit's metadata. An empty payload here would be a "
                        + "BARE frontier - correct about where to resume, amnesiac about what is already "
                        + "durable - and this arm would still be green. Asserted rather than merely logged")
                .isNotEmpty();

        // --- the crash. Everything above is gone; only the broker's state survives. ---

        assertThat(outcome.redelivered)
                .as("restarting on the committed offset must hand the parked record back")
                .contains(PARKED_VALUE);

        assertThat(outcome.sinkContents)
                .as("and the fast records really were durably written - so the frontier is being held back by "
                        + "the parked record specifically, not by the sink having written nothing at all")
                .hasSizeGreaterThanOrEqualTo(FAST_RECORDS);
    }

    /**
     * Trigger-removed control for the arm above. Same rule, same driver, same lanes; the only difference is
     * that the sink refuses nothing, so every lane's watermark eventually covers everything it holds.
     *
     * <p>Its job is to make the first arm falsifiable. A barrier that confirmed no record at all would leave
     * the committed offset at 0 with the parked record redelivered and eight records in the sink - which is
     * precisely what arm 1 asserts. Only this arm distinguishes "the frontier stopped at the right place"
     * from "the frontier never moved".
     */
    @Test
    void theFrontierAdvancesOnceEveryLaneWritesItsRecords() {
        final Outcome outcome = runScenario(ConfirmationRule.OWNING_LANE, false);
        assertRunIsMeaningful(outcome);

        assertThat(outcome.committed.offset())
                .as("with nothing refused, every lane's watermark covers every record it holds, so the "
                        + "frontier must reach %d - the next offset to resume from. A barrier that simply "
                        + "never confirms anything fails here, which is what makes the sibling arm's 0 mean "
                        + "something", TOTAL_RECORDS)
                .isEqualTo((long) TOTAL_RECORDS);

        assertThat(outcome.redelivered)
                .as("nothing is outstanding, so a restart at the committed offset is handed nothing at all")
                .isEmpty();

        assertThat(outcome.sinkContents)
                .as("and the record the sibling arm parks was written here - the one varying term")
                .contains(PARKED_VALUE);
    }

    /**
     * The negative control, and the reason arm 1 is evidence rather than a coincidence. Exactly one term
     * differs from arm 1: the confirmation rule. Under {@link ConfirmationRule#HIGHEST_ACROSS_LANES} a
     * record is confirmed against the highest watermark <em>any</em> lane returned, so the fastest lane's
     * progress speaks for a record it never saw - and the loss shows up on the broker, not in a model.
     */
    @Test
    void negativeControlHighestWatermarkOverCommitsAndLosesTheParkedRecord() {
        final Outcome outcome = runScenario(ConfirmationRule.HIGHEST_ACROSS_LANES, true);
        assertRunIsMeaningful(outcome);

        assertThat(outcome.committed.offset())
                .as("THE OVER-COMMIT: no lane ever wrote offset 0, yet the inverted rule let another lane's "
                        + "watermark confirm it, so the committed offset ran past it")
                .isGreaterThan(0L);

        assertThat(outcome.redelivered)
                .as("THE LOSS: the group resumes past the parked record, so a restart is never handed it "
                        + "again - silent data loss, in broker state rather than in a model")
                .doesNotContain(PARKED_VALUE);

        assertThat(outcome.sinkContents)
                .as("and it really was never written - a record that vanished, not one the reader merely "
                        + "failed to see")
                .doesNotContain(PARKED_VALUE);

        assertThat(outcome.sinkContents)
                .as("and this run did the same work the sound arm did. Without a positive floor every "
                        + "assertion in this arm is an absence or an inequality, all of which a run that "
                        + "processed three records would also satisfy - and the over-commit number would "
                        + "then not be comparable with the sound arm's 0")
                .hasSizeGreaterThanOrEqualTo(FAST_RECORDS);
    }

    /**
     * The preconditions every arm's own claim rests on. Each of these, left unasserted, lets an arm pass
     * while measuring nothing:
     * <ul>
     *   <li><b>lanesUsed</b> - with every record in one lane, {@code OWNING_LANE} and
     *       {@code HIGHEST_ACROSS_LANES} are the <em>same rule</em>, so no arm can tell them apart. The
     *       negative control asserted this; the sound arm, whose green is the quoted evidence, did not.</li>
     *   <li><b>settled</b> - the driver stops on quiescence <em>or</em> a deadline, and a run that stalled
     *       after one confirmation produces exactly the state the sound arm asserts.</li>
     *   <li><b>foreignOffsetAsked</b> - see {@link TopicSinkTask#preCommit}. Everything else here tests how
     *       the barrier interprets a watermark; without this, nothing tested what it asked for.</li>
     * </ul>
     */
    private static void assertRunIsMeaningful(final Outcome outcome) {
        assertThat(outcome.lanesUsed)
                .as("the two rules are only distinguishable when the partition is genuinely split across "
                        + "lanes - with every record in one lane they are the same rule and this arm would "
                        + "pass while measuring nothing. Lane choice is a ShardKey hash, so this is asserted "
                        + "rather than assumed")
                .isGreaterThan(1);
        assertThat(outcome.settled)
                .as("the driver must have reached a settled state, or the frontier reported below is "
                        + "wherever the clock ran out rather than where the rule stopped it")
                .isTrue();
        assertThat(outcome.foreignOffsetAsked)
                .as("every lane must only ever be asked about offsets it actually received. Being asked "
                        + "about a wider range is the over-claim that would make a real connector flush past "
                        + "records it never saw - and with SinkTask's base preCommit being "
                        + "`flush(offsets); return offsets;`, it would come straight back as a watermark the "
                        + "sound rule then honours")
                .isNull();
        assertThat(outcome.committed)
                .as("the driver must have committed something, or the assertions below are vacuous")
                .isNotNull();
    }

    /** What the broker still holds once the driver is gone - the only evidence a crash leaves behind. */
    private static final class Outcome {
        private final OffsetAndMetadata committed;
        private final List<String> redelivered;
        private final List<String> sinkContents;
        private final int lanesUsed;
        private final boolean settled;
        private final Long foreignOffsetAsked;

        private Outcome(final Run run, final List<String> redelivered, final List<String> sinkContents) {
            this.committed = run.committed;
            this.redelivered = redelivered;
            this.sinkContents = sinkContents;
            this.lanesUsed = run.lanesUsed;
            this.settled = run.settled;
            this.foreignOffsetAsked = run.foreignOffsetAsked;
        }
    }

    /**
     * Runs the whole scenario end to end and returns what the broker was left holding. Every arm calls this
     * with identical everything except the two terms under control.
     *
     * @param rule         which watermark may confirm a record
     * @param refuseParked whether the sink refuses to write the parked record
     */
    private Outcome runScenario(final ConfirmationRule rule, final boolean refuseParked) {
        final String inputTopic = setupTopic("connect-frontier-in");
        final String outputTopic = setupTopic("connect-frontier-out");
        final TopicPartition inputPartition = new TopicPartition(inputTopic, 0);
        final String groupId = "connect-frontier-" + inputTopic;

        produceParkedThenFastRecords(inputTopic);

        final Run run = runUntilSettled(outputTopic, groupId, inputPartition, rule, refuseParked);
        final Outcome outcome = new Outcome(run,
                redeliveredFrom(groupId, inputPartition, run.committed),
                sinkContents(outputTopic));

        // Logged, not merely asserted: a passing assertion prints nothing, and this arm exists to be
        // evidence someone else can read out of a build log without re-running it.
        log.info("=== rule={} refuseParked={} lanesUsed={} settled={} foreignOffsetAsked={} "
                        + "committedOffset={} committedMetadataLength={} inputLogEnd={} redelivered={} "
                        + "sinkRecords={}",
                rule, refuseParked, outcome.lanesUsed, outcome.settled, outcome.foreignOffsetAsked,
                outcome.committed == null ? null : outcome.committed.offset(),
                outcome.committed == null ? 0 : outcome.committed.metadata().length(),
                logEndOffset(inputPartition), outcome.redelivered, outcome.sinkContents.size());
        return outcome;
    }

    /** What the driver loop itself observed, before anything is read back off the broker. */
    private static final class Run {
        private final OffsetAndMetadata committed;
        private final int lanesUsed;
        /** False when the loop hit its deadline instead of quiescing - see {@link #runUntilSettled}. */
        private final boolean settled;
        private final Long foreignOffsetAsked;

        private Run(final OffsetAndMetadata committed, final int lanesUsed, final boolean settled,
                    final Long foreignOffsetAsked) {
            this.committed = committed;
            this.lanesUsed = lanesUsed;
            this.settled = settled;
            this.foreignOffsetAsked = foreignOffsetAsked;
        }
    }

    /**
     * Polls, dispatches, runs a durability cycle and commits - the loop the real runtime would own - until
     * the system has settled: nothing newly confirmed and nothing left dirty for {@link #SETTLED_PASSES}
     * consecutive passes.
     *
     * <p>Settling rather than stopping at the first commit matters for the trigger-removed arm, which has to
     * show the frontier reaching its final value; stopping early would report a partial frontier and the arm
     * would fail for a reason that has nothing to do with the mechanism.
     */
    private Run runUntilSettled(final String outputTopic, final String groupId,
                                final TopicPartition inputPartition, final ConfirmationRule rule,
                                final boolean refuseParked) {
        final List<PcSinkTaskLane> lanes = new ArrayList<>();
        final List<TopicSinkTask> tasks = new ArrayList<>();

        try (KafkaProducer<String, String> sinkProducer =
                     getKcu().createNewProducer(KafkaClientUtils.ProducerMode.NOT_TRANSACTIONAL);
             KafkaConsumer<String, String> consumer = getKcu().createNewConsumer(groupId)) {

            for (int lane = 0; lane < LANES; lane++) {
                final TopicSinkTask task = new TopicSinkTask(sinkProducer, outputTopic, refuseParked);
                tasks.add(task);
                lanes.add(new PcSinkTaskLane(task));
            }
            final PcSinkTaskLaneRouter router =
                    new PcSinkTaskLaneRouter(lanes, OffsetCompositionCrashRestartTest::project, rule);

            final PcTaskDispatcher dispatcher =
                    new PcTaskDispatcher("connect-crash", Collections.singleton(inputPartition), POOL_SIZE);
            consumer.assign(UniLists.of(inputPartition));
            consumer.seek(inputPartition, 0);

            OffsetAndMetadata lastCommitted = null;
            int settled = 0;
            try {
                final long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
                while (System.nanoTime() < deadline && settled < SETTLED_PASSES) {
                    final ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(200));
                    if (!polled.isEmpty()) {
                        dispatcher.registerRecords(inputPartition, toBytes(polled.records(inputPartition)));
                    }
                    dispatcher.dispatchAvailable(router);
                    final int confirmed = router.runDurabilityCycle().confirmed();

                    final Map<TopicPartition, OffsetAndMetadata> toCommit = dispatcher.collectCommitData();
                    if (toCommit.containsKey(inputPartition)) {
                        consumer.commitSync(toCommit);
                        dispatcher.onCommitSuccess(toCommit);
                        lastCommitted = toCommit.get(inputPartition);
                    }

                    final boolean quiet = confirmed == 0
                            && lastCommitted != null
                            && dispatcher.isQuiescent()
                            && !dispatcher.hasCommitDataOutstanding();
                    settled = quiet ? settled + 1 : 0;
                }
            } finally {
                // A crash, not a shutdown - the same injection CommitFrontierCrashRestartTest uses. An
                // orderly close() drains the pool, feeds completions back and revokes partitions, which
                // hands a simulated crash a repair pass a real one never gets. The class is @Isolated,
                // which is abortClose's stated invariant.
                //
                // Nested INSIDE the try-with-resources on purpose: resources close at the end of their own
                // block, so an outer finally would abort the dispatcher only after the sink producer had
                // already been closed - and a worker still inside put() would then die on a closed producer,
                // burying a real signal under an artefact of shutdown order. This NARROWS that window rather
                // than closing it: shutdownNow() interrupts but does not join, and ReentrantLock.lock() is
                // uninterruptible, so a worker already past it can still meet a closed producer. The
                // resulting failure lands on a dispatcher nobody drains and cannot reach an assertion.
                dispatcher.abortClose();
            }
            return new Run(lastCommitted, lanesThatReceivedRecords(tasks), settled >= SETTLED_PASSES,
                    firstForeignOffsetAsked(tasks));
        }
    }

    /** The first lane that was asked about an offset it never received, if any. */
    private static Long firstForeignOffsetAsked(final List<TopicSinkTask> tasks) {
        for (final TopicSinkTask task : tasks) {
            final Long foreign = task.foreignOffsetAsked();
            if (foreign != null) {
                return foreign;
            }
        }
        return null;
    }

    /**
     * How many lanes the router actually put records into. The negative control asserts on this because lane
     * selection is a {@code ShardKey} hash: "the partition really was split" has to be an observed fact
     * about this run, not something the test assumes.
     */
    private static int lanesThatReceivedRecords(final List<TopicSinkTask> tasks) {
        int used = 0;
        for (final TopicSinkTask task : tasks) {
            if (task.receivedCount() > 0) {
                used++;
            }
        }
        return used;
    }

    /**
     * What a restarting consumer in the same group is handed - the real proof nothing was skipped.
     *
     * <p>Deliberately {@code auto.offset.reset=none}. The suite's default is {@code earliest}, under which a
     * consumer with <b>no committed offset</b> reads from offset 0 and hands back the parked record whether
     * or not any commit ever landed - so a green {@code contains(PARKED_VALUE)} would be satisfied by the
     * failure state. {@code none} makes that path throw instead of silently passing, which is the
     * make-the-wrong-answer-unreachable fix rather than the assert-harder one
     * ({@code docs/solutions/test-issues/a-restart-assertion-satisfiable-by-pre-crash-data-proves-nothing.md}).
     *
     * <p><b>Structural proof before empirical wait.</b> Two of the three arms expect to be handed nothing, and
     * "we polled for {@link #READ_BACK_WAIT} and saw nothing" cannot tell an exhausted log from a slow reader
     * - it is inductive, and it costs the full budget every time. So this first asks the broker where the log
     * actually ends: if the group's resume point is already at or past that, there is <em>provably</em>
     * nothing left to hand back, and the poll below becomes a short confirmation rather than the evidence.
     * When the resume point is behind the end of the log - the sound arm, where a record really is owed - the
     * full budget still applies. Same code in every arm; only what the broker reports differs.
     */
    private List<String> redeliveredFrom(final String groupId, final TopicPartition inputPartition,
                                         final OffsetAndMetadata committed) {
        if (committed == null) {
            // Nothing was committed, so there is no resume point to read from and auto.offset.reset=none
            // would throw out of this helper. Return empty and let the arm's own "must have committed
            // something" assertion report it - a helper's exception buries the diagnosis.
            return Collections.emptyList();
        }
        final long logEnd = logEndOffset(inputPartition);
        final boolean provablyExhausted = committed.offset() >= logEnd;

        final Properties noReset = new Properties();
        noReset.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");

        try (KafkaConsumer<String, String> consumer = getKcu().createNewConsumer(groupId, noReset)) {
            consumer.assign(UniLists.of(inputPartition));
            // No seek: the group's committed offset is the resume point, which is exactly what is under test.

            // Resolve the position BEFORE polling, and assert it. Without this an empty result is ambiguous:
            // a consumer that never managed to find its coordinator inside the budget returns exactly what an
            // exhausted log returns, and pollValuesUntil deliberately never throws on timeout - so both
            // absence assertions below would pass having observed nothing at all. auto.offset.reset=none
            // makes position() throw outright when the group has no committed offset, so the only way past
            // this line is a reader that really did resume where the group said.
            assertThat(consumer.position(inputPartition))
                    .as("the read-back consumer must resume at the group's committed offset, or an empty "
                            + "result would be an unpositioned consumer rather than an exhausted log")
                    .isEqualTo(committed.offset());

            return pollValuesUntil(consumer,
                    provablyExhausted ? EMPTY_CONFIRM_WAIT : READ_BACK_WAIT,
                    values -> !values.isEmpty());
        }
    }

    /** Where the log actually ends, asked of the broker rather than inferred from what the test produced. */
    @SneakyThrows
    private long logEndOffset(final TopicPartition partition) {
        return getKcu().getAdmin()
                .listOffsets(Collections.singletonMap(partition, OffsetSpec.latest()))
                .partitionResult(partition).get().offset();
    }

    /**
     * Reads the sink topic from the beginning. Safe here, unlike the streams module's phase-scoped reader:
     * this topic is created fresh for each scenario and written only by the driver under test, so an
     * earliest-read cannot be satisfied by data from an earlier phase.
     *
     * <p><b>Reads to the end of the log, not to an expected count.</b> Two arms assert that the parked value
     * is <em>absent</em> from the sink, and a reader that stops at the eighth record satisfies that by giving
     * up rather than by the record's absence - the parked value could sit at the ninth position, since four
     * lanes write concurrently and nothing fixes the order. Draining to the log end makes the negative claim
     * exhaustive, and it is faster than the old count-plus-timeout because a fully-read log ends the loop
     * immediately instead of burning the remaining budget.
     */
    private List<String> sinkContents(final String outputTopic) {
        final TopicPartition outputPartition = new TopicPartition(outputTopic, 0);
        final long sinkEnd = logEndOffset(outputPartition);
        try (KafkaConsumer<String, String> consumer =
                     getKcu().createNewConsumer(KafkaClientUtils.GroupOption.NEW_GROUP)) {
            consumer.assign(UniLists.of(outputPartition));
            consumer.seekToBeginning(UniLists.of(outputPartition));
            return pollValuesUntil(consumer, READ_BACK_WAIT,
                    values -> consumer.position(outputPartition) >= sinkEnd);
        }
    }

    /**
     * Polls one consumer until {@code enough} is satisfied or the budget runs out, and returns whatever
     * arrived.
     *
     * <p>Deliberately does <b>not</b> throw on timeout, which is why it is a hand-rolled loop rather than an
     * Awaitility {@code until()}: two of the three arms assert on an expected-empty result, so exhausting the
     * budget is a legitimate outcome and the caller's own AssertJ description has to be the thing that
     * explains a failure. Awaitility would replace those scenario-specific messages with a generic
     * {@code ConditionTimeoutException} raised inside this helper.
     */
    private static List<String> pollValuesUntil(final KafkaConsumer<String, String> consumer,
                                                final Duration budget,
                                                final Predicate<List<String>> enough) {
        final List<String> collected = new ArrayList<>();
        final long deadline = System.nanoTime() + budget.toNanos();
        while (System.nanoTime() < deadline && !enough.test(collected)) {
            consumer.poll(Duration.ofMillis(500)).forEach(record -> collected.add(record.value()));
        }
        return collected;
    }

    @SneakyThrows
    private void produceParkedThenFastRecords(final String inputTopic) {
        try (KafkaProducer<String, String> producer =
                     getKcu().createNewProducer(KafkaClientUtils.ProducerMode.NOT_TRANSACTIONAL)) {
            // Offset 0, so the parked record IS the frontier - the defining case.
            producer.send(new ProducerRecord<>(inputTopic, "key-parked", PARKED_VALUE)).get();
            for (int i = 0; i < FAST_RECORDS; i++) {
                producer.send(new ProducerRecord<>(inputTopic, "key-fast-" + i, "fast-" + i));
            }
            producer.flush();
        }
    }

    private static List<ConsumerRecord<byte[], byte[]>> toBytes(final List<ConsumerRecord<String, String>> records) {
        final List<ConsumerRecord<byte[], byte[]>> converted = new ArrayList<>(records.size());
        for (final ConsumerRecord<String, String> record : records) {
            converted.add(new ConsumerRecord<>(record.topic(), record.partition(), record.offset(),
                    record.key() == null ? null : record.key().getBytes(StandardCharsets.UTF_8),
                    record.value().getBytes(StandardCharsets.UTF_8)));
        }
        return converted;
    }

    private static SinkRecord project(final ConsumerRecord<byte[], byte[]> record) {
        return new SinkRecord(record.topic(), record.partition(), Schema.OPTIONAL_BYTES_SCHEMA, record.key(),
                Schema.OPTIONAL_BYTES_SCHEMA, record.value(), record.offset());
    }

    /**
     * A sink that writes to a Kafka topic, and optionally refuses one record.
     *
     * <p>Its {@code preCommit} models a connector that <b>overrides</b> {@code preCommit} with a real
     * durability watermark: it reports the highest prefix of the records <b>it itself received</b> that it
     * durably wrote, stopping at anything refused. Refusing one record therefore pins that lane's own
     * watermark below it forever, which is what a sink that cannot write a poison record actually does.
     *
     * <p><b>It is deliberately NOT {@code SinkTask}'s base implementation</b>, which is
     * {@code flush(offsets); return offsets;} over an empty {@code flush} body - a pure echo, established by
     * disassembly in the investigation's U2.0. Under an echo this barrier degenerates: the lane is handed
     * {@code max(delivered) + 1}, echoes it back unchanged, and every record whose {@code put} returned is
     * confirmed - the exact "put returning is a durability claim" conflation the barrier exists to prevent.
     * All three arms use this overriding sink, so <b>none of them evidences anything about that connector
     * population</b>. The investigation carries it as an inherited precondition, not as a covered case.
     */
    private static final class TopicSinkTask extends SinkTask {

        private final KafkaProducer<String, String> producer;
        private final String outputTopic;
        private final boolean refuseParked;
        private final NavigableSet<Long> durable = new TreeSet<>();
        private final Set<Long> received = new LinkedHashSet<>();
        private volatile long lowestRefused = Long.MAX_VALUE;
        /** Set if this lane was ever asked about an offset it never received. See {@link #preCommit}. */
        private Long foreignOffsetAsked;

        private TopicSinkTask(final KafkaProducer<String, String> producer, final String outputTopic,
                              final boolean refuseParked) {
            this.producer = producer;
            this.outputTopic = outputTopic;
            this.refuseParked = refuseParked;
        }

        @Override
        @SneakyThrows
        public void put(final Collection<SinkRecord> records) {
            for (final SinkRecord record : records) {
                final String value = new String((byte[]) record.value(), StandardCharsets.UTF_8);
                synchronized (this) {
                    received.add(record.kafkaOffset());
                    if (refuseParked && PARKED_VALUE.equals(value)) {
                        // Cannot write this one, ever. Note it still RETURNS normally - buffering succeeded,
                        // durability did not, and conflating the two is the defect under investigation.
                        lowestRefused = Math.min(lowestRefused, record.kafkaOffset());
                        continue;
                    }
                }
                // Blocking on the send's own acknowledgement, not merely handing it to the producer: this
                // task's watermark is a DURABILITY claim, and a task that reported a record durable on the
                // strength of an un-acked send would be making the exact over-claim the barrier exists to
                // prevent - inside the harness meant to detect it.
                producer.send(new ProducerRecord<>(outputTopic, record.key() == null ? null
                        : new String((byte[]) record.key(), StandardCharsets.UTF_8), value)).get();
                synchronized (this) {
                    durable.add(record.kafkaOffset());
                }
            }
        }

        @Override
        public synchronized Map<TopicPartition, OffsetAndMetadata> preCommit(
                final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
            if (currentOffsets.isEmpty()) {
                return Collections.emptyMap();
            }
            if (currentOffsets.size() > 1) {
                // Taking the first key would silently answer for one partition while keying the reply to an
                // arbitrary one. Sound today only because these topics have a single partition; loud rather
                // than latent if that ever changes.
                throw new IllegalStateException(
                        "this sink double assumes one partition per lane, was handed " + currentOffsets.keySet());
            }
            final TopicPartition partition = currentOffsets.keySet().iterator().next();

            // Assert on the QUESTION, not just the answer. Everything else in this file tests how the
            // barrier INTERPRETS a watermark; nothing tested what map it hands over - so a barrier that
            // asked each lane about the whole partition would pass every arm here unchanged. That is not a
            // hypothetical defect: SinkTask's base preCommit is `flush(offsets); return offsets;`, so a
            // connector overriding neither would echo a partition-wide watermark straight back, and even the
            // sound rule would then confirm records this lane never received. PcSinkTaskLane's javadoc calls
            // the lane-locality of this map load-bearing; this is the assertion that holds it to it.
            final OffsetAndMetadata asked = currentOffsets.get(partition);
            if (asked != null) {
                final long highestReceived = received.isEmpty() ? -1L : Collections.max(received);
                if (asked.offset() > highestReceived + 1) {
                    foreignOffsetAsked = asked.offset();
                }
            }

            // The highest contiguous prefix of MY OWN records that is durable. Walk what this lane
            // RECEIVED, not just what it wrote: iterating `durable` alone cannot see a gap, so a lane holding
            // {5,6,7} with 6's put still in flight would report 8 and claim 6 durable. Harmless today only
            // because the barrier gates on `deliverable` - but this sink is the oracle for the whole verdict,
            // and an oracle that is honest only by virtue of the thing it is testing is the wrong shape.
            long watermark = 0;
            for (final Long offset : new TreeSet<>(received)) {
                if (offset >= lowestRefused || !durable.contains(offset)) {
                    break;
                }
                watermark = offset + 1;
            }
            return Collections.singletonMap(partition, new OffsetAndMetadata(watermark));
        }

        synchronized int receivedCount() {
            return received.size();
        }

        /** The offset this lane was asked about but never received, or null if it was only ever asked honestly. */
        synchronized Long foreignOffsetAsked() {
            return foreignOffsetAsked;
        }

        @Override
        public String version() {
            return "crash-restart-probe";
        }

        @Override
        public void start(final Map<String, String> props) {
            // nothing to start
        }

        @Override
        public void stop() {
            // the producer is owned by the test, which closes it
        }
    }
}
