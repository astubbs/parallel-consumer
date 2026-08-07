package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.ProvesClaim;
import io.confluent.parallelconsumer.TransactionalClaim;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption;
import io.confluent.parallelconsumer.integrationTests.utils.TransactionalTopicVerifier;
import io.confluent.parallelconsumer.integrationTests.utils.WorkerFunctionFailureCapture;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertWithMessage;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.ProducerMode.TRANSACTIONAL;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static java.util.Optional.empty;
import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * C7, the {@code pollAndProduceMany} half of the transactional guarantee, proved against a real broker: <em>"all
 * records must have been produced successfully to the broker before the transaction will commit, after which all
 * will be visible together, or none."</em>
 *
 * <h2>The two halves, and the two tests that carry them</h2>
 * <b>"after which all will be visible together"</b> is
 * {@link #everyResultSetForAnInputRecordIsVisibleInFullOrNotAtAll()}. It runs a real instance producing several
 * result records per input record and watches the output topic <em>continuously while the work is happening</em>,
 * failing the instant any input record's result set is observed part-visible. Asserting only at the end would
 * prove the weaker "they all arrive eventually", which a system with no atomicity at all would also satisfy.
 * <p>
 * <b>"all records must have been produced successfully ... before the transaction will commit"</b> is
 * {@link #aTerminallyFailedSendLeavesTheWholeTransactionInvisible()}, and <b>that half is REFUTED</b>. One send
 * in a {@code pollAndProduceMany} result set is made to fail terminally; the transaction commits anyway, and the
 * part of that result set which was sent before the failure becomes visible at {@code read_committed} - two of
 * five records produced from one source offset. That arm therefore ships {@code @Disabled}, with the mechanism,
 * the evidence and the knock-on for C2 recorded on the method and in
 * {@link TransactionalClaim#PRODUCE_MANY_ALL_OR_NONE}.
 *
 * <h2>Why the mid-flight watch cannot see a false partial</h2>
 * A {@code read_committed} consumer can only be shown records below the last stable offset, which moves in whole
 * transactions - but a single fetch could still return a prefix of what is available if it hit
 * {@code max.partition.fetch.bytes} (1MB) or {@code max.poll.records}
 * ({@link io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils#MAX_POLL_RECORDS}). The volumes
 * here are deliberately tiny - {@link #INPUT_RECORDS} x {@link #RESULTS_PER_INPUT_RECORD} short strings, a few
 * kilobytes in total - so neither bound is reachable and a part-visible result set can only mean a
 * part-committed one. <b>Raising those volumes materially would re-introduce the possibility of a truncated
 * fetch and make this class flaky.</b>
 *
 * <h2>batchSize is 1, on purpose</h2>
 * "Produce many" is about many RESULT records per input record and has nothing to do with consumer batching.
 * They are kept unconfused here because at {@code batchSize >= 2} the produce lock is acquired per
 * {@code PollContextInternal} but released per {@code WorkContainer}, which stalls the instance outright - see
 * {@code TransactionalCrashReplayIT#outputHoldsEachResultExactlyOnceAcrossTheReplayWhenBatching} and
 * {@code docs/solutions/test-issues/transactional-batching-stall-produce-lock-released-per-record-2026-08-08.md}. Nothing in C7 needs batching to express it.
 *
 * <h2>What is counted</h2>
 * Records, never offsets. Commit and abort markers occupy offsets on a transactional topic, so the difference
 * between two offsets is not a record count. Every count here comes from
 * {@link TransactionalTopicVerifier#consumedRecords()}, which counts what it actually consumed. The absence
 * assertions are all guarded by {@link TransactionalTopicVerifier#requireLiveAndCaughtUp} first, and paired with
 * a {@code read_uncommitted} arm that <em>does</em> see the records, so "absent" means "hidden by the isolation
 * level" and not "never written".
 *
 * @author Antony Stubbs
 * @see TransactionalClaim#PRODUCE_MANY_ALL_OR_NONE
 * @see TransactionalVisibilityIT
 * @see TransactionalCrashReplayIT
 */
@Tag("transactions")
@Slf4j
class TransactionalBatchVisibilityIT extends BrokerIntegrationTest<String, String> {

    /**
     * Every result record's value starts with this, so the warm-up marker and the sentinel - which are fixture,
     * not results - are excluded from every count by construction rather than by remembering to filter them.
     */
    private static final String RESULT = "result|";

    private static final String WARMUP = "warmup";

    private static final String SENTINEL = "sentinel";

    /**
     * Input-key prefixes. Every result record carries its input record's key, so a prefix identifies both the
     * input records of a stage and - after {@link #RESULT} - the result records they produced. That is what lets
     * one absence assertion be written about a whole stage.
     */
    private static final String TOGETHER_KEYS = "together-";

    private static final String PRIMED_KEYS = "primed-";

    private static final String OK_KEYS = "ok-";

    private static final String POISON_KEYS = "poison-";

    /**
     * Small enough that a single fetch can always carry everything currently visible - see the class javadoc -
     * and large enough that the run spans several commit intervals, so there are real transaction boundaries for
     * a result set to be split across if the guarantee did not hold.
     */
    private static final int INPUT_RECORDS = 20;

    /**
     * The "many" in {@code pollAndProduceMany}. More than two, so a part-visible set is a state the assertion can
     * actually distinguish rather than a coin flip.
     */
    private static final int RESULTS_PER_INPUT_RECORD = 5;

    private static final int MAX_CONCURRENCY = 4;

    /**
     * See the class javadoc - this is not a knob, it is the value the produce-lock defect makes mandatory.
     */
    private static final int BATCH_SIZE = 1;

    private static final Duration COMMIT_INTERVAL = ofMillis(200);

    /**
     * Long enough that no transaction opened by this class can be reaped while it runs. A reap would give every
     * "not visible" assertion a second, uninteresting explanation.
     */
    private static final Duration NO_REAP_WHILE_WE_RUN = ofMinutes(5);

    /**
     * The failure arm's commit interval. Longer than the test, so that after the instance has spent its
     * unavoidable startup commit (see {@link #PRIMING_RECORDS}) the only commit it ever attempts is the one this
     * test asks for, on a transaction it has already poisoned.
     */
    private static final Duration ONE_COMMIT_ONLY = ofMinutes(10);

    /**
     * Spends the first commit, which no commit interval can prevent: {@code isTimeToCommitNow()} treats a null
     * {@code lastCommitTime} as a day elapsed, so every instance commits once as soon as anything is dirty.
     * Exactly one record, so that commit cannot land half way through a set that matters.
     */
    private static final int PRIMING_RECORDS = 1;

    private static final int OK_RECORDS = 3;

    /**
     * The failure arm's terminal send. {@code max.request.size} defaults to 1MB, so a value twice that is
     * rejected by the producer itself - before the record is appended to the accumulator, and without depending
     * on any broker-side configuration. Terminal by nature: no retry can make an oversized record fit.
     */
    private static final int OVERSIZED_VALUE_BYTES = 2 * 1024 * 1024;

    private static final Duration PROGRESS_TIMEOUT = ofSeconds(120);

    /**
     * How long the failure arm watches the output topic across the commit attempt it asked for. Not a race
     * against the broker: the commit is requested of a running instance whose records are already acknowledged,
     * so this only has to outlast the request reaching the control thread and the commit landing.
     */
    private static final Duration COMMIT_ATTEMPT_OBSERVATION_WINDOW = ofSeconds(20);

    private final List<AutoCloseable> toClose = new ArrayList<>();

    private String inputTopic;

    private String outputTopic;

    private String instanceNonce;

    private String warmupMarker;

    private TransactionalTopicVerifier committed;

    private TransactionalTopicVerifier uncommitted;

    private WorkerFunctionFailureCapture workerFailures;

    @AfterEach
    void closeTestClients() {
        for (AutoCloseable closeable : toClose) {
            try {
                closeable.close();
            } catch (Exception e) {
                // Teardown only, and after every assertion has run, so this cannot mask a result. The failure
                // arm deliberately leaves an instance whose producer is in an error state - it legitimately
                // fails to close cleanly, and that IS the scenario.
                log.warn("Problem closing test client {} - tolerated during teardown", closeable, e);
            }
        }
        toClose.clear();
    }

    private <T extends AutoCloseable> T register(T closeable) {
        toClose.add(closeable);
        return closeable;
    }

    // ---------------------------------------------------------------------------------------------------------
    // Fixture
    // ---------------------------------------------------------------------------------------------------------

    /**
     * Input and output topics, a committed warm-up record on the output topic, and two verifiers proved live and
     * caught up to it - so that no absence asserted later is vacuous.
     * <p>
     * The warm-up record is written by a producer with its own random {@code transactional.id}: it fences
     * nothing and belongs to no scenario, its only job is to be the marker
     * {@link TransactionalTopicVerifier#requireLiveAndCaughtUp} needs.
     */
    private void prepare(String scenario) {
        instanceNonce = scenario + "-" + nextInt();
        inputTopic = setupTopic(getClass().getSimpleName() + "-" + scenario + "-input");
        outputTopic = setupTopic(getClass().getSimpleName() + "-" + scenario + "-output");

        warmupMarker = WARMUP + "-" + instanceNonce;
        commitOneRecord(register(getKcu().createAndInitNewTransactionalProducer()), warmupMarker);

        committed = register(TransactionalTopicVerifier.readCommitted(getKcu(), scenario, outputTopic));
        uncommitted = register(TransactionalTopicVerifier.readUncommitted(getKcu(), scenario, outputTopic));
        committed.requireLiveAndCaughtUp(warmupMarker);
        uncommitted.requireLiveAndCaughtUp(warmupMarker);

        workerFailures = register(new WorkerFunctionFailureCapture(instanceNonce));
    }

    private void commitOneRecord(Producer<String, String> producer, String value) {
        producer.beginTransaction();
        producer.send(new ProducerRecord<>(outputTopic, value, value));
        producer.commitTransaction();
    }

    private ParallelEoSStreamProcessor<String, String> buildInstance(Duration commitInterval) {
        KafkaProducer<String, String> producer = getKcu()
                .createNewProducer(TRANSACTIONAL, NO_REAP_WHILE_WE_RUN, empty());
        KafkaConsumer<String, String> consumer = getKcu().createNewConsumer(GroupOption.NEW_GROUP);

        var pc = new ParallelEoSStreamProcessor<>(ParallelConsumerOptions.<String, String>builder()
                .consumer(consumer)
                .producer(producer)
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                .ordering(UNORDERED)
                .maxConcurrency(MAX_CONCURRENCY)
                .batchSize(BATCH_SIZE)
                .commitInterval(commitInterval)
                .build());
        // names this instance's worker threads, which is how WorkerFunctionFailureCapture tells our worker
        // failures from those of any other test sharing this JVM
        pc.setMyId(Optional.of(instanceNonce));
        pc.subscribe(UniSets.of(inputTopic));
        return register(pc);
    }

    private static String resultValue(String inputKey, int index) {
        return RESULT + inputKey + "|" + index;
    }

    private static List<String> expectedResults(List<String> inputKeys, int resultsPerRecord) {
        List<String> expected = new ArrayList<>();
        for (String key : inputKeys) {
            for (int i = 0; i < resultsPerRecord; i++) {
                expected.add(resultValue(key, i));
            }
        }
        return expected;
    }

    @SneakyThrows
    private List<String> produceInput(int count, String keyPrefix) {
        return getKcu().produceMessages(inputTopic, count, keyPrefix);
    }

    // ---------------------------------------------------------------------------------------------------------
    // The all-or-nothing assertion, and the watch that applies it while the work is happening
    // ---------------------------------------------------------------------------------------------------------

    /**
     * @return how many result records are currently visible for each input key that has any at all - counted
     *         from the records consumed, never inferred from offsets
     */
    private static Map<String, Integer> visibleResultCountsByKey(TransactionalTopicVerifier verifier) {
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (ConsumerRecord<String, String> record : verifier.consumedRecords()) {
            if (record.value().startsWith(RESULT)) {
                counts.merge(record.key(), 1, Integer::sum);
            }
        }
        return counts;
    }

    /**
     * The claim itself, as an assertion: no input record's result set may ever be seen part-visible.
     * <p>
     * Deliberately <em>not</em> wrapped in an awaited condition anywhere it is used. A partial set is a
     * transient state - it resolves into a complete one the moment the rest arrives - so retrying this until it
     * passes would convert the one observation that refutes the claim into a green result.
     */
    private static void assertNoPartialResultSetVisible(TransactionalTopicVerifier verifier, int expectedPerKey) {
        List<String> partial = visibleResultCountsByKey(verifier).entrySet().stream()
                .filter(entry -> entry.getValue() != expectedPerKey)
                .map(entry -> entry.getKey() + " has " + entry.getValue() + " of " + expectedPerKey)
                .collect(Collectors.toList());

        assertWithMessage("verifier %s can see PART of an input record's result set. All the records produced "
                        + "from one source offset go into one transaction, so a partial set means a transaction "
                        + "became partly visible - which is exactly what the all-or-none guarantee denies",
                verifier.name())
                .that(partial)
                .isEmpty();
    }

    /**
     * Consume continuously until every expected result is visible, applying
     * {@link #assertNoPartialResultSetVisible} after every single poll.
     * <p>
     * Hand-rolled rather than awaited for the reason given on that method: awaitility retries a failing
     * assertion, and retrying this one would discard the very observation the test exists to make.
     */
    private static void watchUntilAllVisible(TransactionalTopicVerifier verifier,
                                             List<String> expected,
                                             int expectedPerKey) {
        long deadline = System.nanoTime() + PROGRESS_TIMEOUT.toNanos();
        while (true) {
            verifier.poll();
            assertNoPartialResultSetVisible(verifier, expectedPerKey);
            if (verifier.consumed().containsAll(expected)) {
                return;
            }
            if (System.nanoTime() > deadline) {
                List<String> missing = expected.stream()
                        .filter(value -> !verifier.consumed().contains(value))
                        .collect(Collectors.toList());
                fail("verifier " + verifier.name() + " never saw every expected result within " + PROGRESS_TIMEOUT
                        + " - still missing " + missing.size() + " of " + expected.size() + ": " + missing);
            }
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // Tests
    // ---------------------------------------------------------------------------------------------------------

    /**
     * C7, second half: every input record's whole result set becomes visible in one step, or not at all.
     * <p>
     * The instance produces {@link #RESULTS_PER_INPUT_RECORD} records per input record over a commit interval
     * short enough that the run spans many transactions, while a {@code read_committed} verifier watches the
     * output topic throughout and fails the moment any key is seen part-visible. The end-state assertions -
     * every key complete, and the total record count exact - are what stop a watch that simply never saw
     * anything from passing.
     */
    @Test
    @ProvesClaim(TransactionalClaim.PRODUCE_MANY_ALL_OR_NONE)
    void everyResultSetForAnInputRecordIsVisibleInFullOrNotAtAll() {
        prepare("all-together");

        List<String> inputKeys = produceInput(INPUT_RECORDS, TOGETHER_KEYS);
        List<String> expected = expectedResults(inputKeys, RESULTS_PER_INPUT_RECORD);

        ParallelEoSStreamProcessor<String, String> pc = buildInstance(COMMIT_INTERVAL);
        pc.pollAndProduceMany(context -> {
            List<ProducerRecord<String, String>> results = new ArrayList<>();
            for (ConsumerRecord<String, String> record : context.getConsumerRecordsFlattened()) {
                for (int i = 0; i < RESULTS_PER_INPUT_RECORD; i++) {
                    results.add(new ProducerRecord<>(outputTopic, record.key(), resultValue(record.key(), i)));
                }
            }
            return results;
        });

        watchUntilAllVisible(committed, expected, RESULTS_PER_INPUT_RECORD);

        Map<String, Integer> counts = visibleResultCountsByKey(committed);
        assertWithMessage("every input record must have contributed a result set, and every set must be whole")
                .that(counts.keySet())
                .containsExactlyElementsIn(inputKeys);
        assertNoPartialResultSetVisible(committed, RESULTS_PER_INPUT_RECORD);

        assertWithMessage("the output topic must hold exactly one result record per input record per result - "
                + "counted from consumed records, because commit markers occupy offsets and would inflate any "
                + "offset-derived count")
                .that(counts.values().stream().mapToInt(Integer::intValue).sum())
                .isEqualTo(INPUT_RECORDS * RESULTS_PER_INPUT_RECORD);

        assertWithMessage("the worker path threw, and nothing in main reads WorkContainer#future, so this would "
                + "otherwise have been invisible - the run above was not the clean one it appeared to be")
                .that(workerFailures.describe())
                .isEmpty();
    }

    /**
     * C7, first half - <b>REFUTED on master.</b> The claim is that one terminally failed send means the
     * transaction never commits, so nothing it held ever becomes visible: neither the rest of that result set,
     * nor the results of earlier input records the broker had already acknowledged into the same transaction.
     * What actually happens is that the transaction commits anyway, carrying the part of the failing result set
     * that was sent before the failure - so a source offset's produced records become <em>partly</em> visible.
     *
     * <h2>What was observed</h2>
     * One input record returns five results, of which the middle one is 2MB and so is rejected by the producer
     * itself against {@code max.request.size}. Results 0 and 1 are accepted into the open transaction, result 2
     * fails, results 3 and 4 are never attempted. The instance neither fails nor shuts down, and the next commit
     * succeeds: the {@code read_committed} verifier then sees {@code result|poison-key-0|0} and
     * {@code result|poison-key-0|1} and nothing else of that set - <b>two of five</b> - alongside the complete
     * result sets of the earlier records that shared the transaction.
     *
     * <h2>Why</h2>
     * {@code ProducerManager#produceMessages} installs a {@code Callback} - the one its own comment describes as
     * "only needed if not using tx" - which throws {@code InternalRuntimeException} from
     * {@code Callback#onCompletion}. {@code KafkaProducer#doSend} invokes that callback from inside its
     * {@code catch (ApiException)} handler and calls {@code transactionManager.maybeTransitionToErrorState(e)}
     * only <em>afterwards</em>. The throw escapes the handler first, so the transaction is never moved into the
     * abortable-error state that would have forced an abort, and the next commit is accepted with a partial
     * result set inside it. PC has no other mechanism for the case either: a failed {@code WorkContainer} leaves
     * the records that were already sent sitting in the transaction, and only an abort could remove them.
     * <p>
     * The same observation falsifies C2's sentence - "All records produced from a given source offset will
     * either all be visible, or none will be" - under the same conditions. C2 is currently recorded
     * {@code PROVED} on the strength of arms that never make a send fail; re-triaging it is the maintainer's
     * call, not this test's.
     *
     * <h2>Structure of the assertions</h2>
     * The earlier records are what make this a proof rather than a restatement. Their results are shown to be on
     * the broker by the {@code read_uncommitted} arm <em>before</em> the poisoned record is ever fed in, so what
     * the {@code read_committed} arm then does or does not see is about the transaction and not about whether a
     * send happened.
     * <p>
     * The sentinel at the end is the same device {@code TransactionalVisibilityIT#abortedTransactionRecordsAreNeverVisible}
     * uses, and it is what would keep this test honest if the defect were fixed: a record committed by an
     * unrelated producer <em>after</em> the poisoned transaction is resolved. The verifier consuming it proves
     * it read PAST the region, so "it never saw those results" would be a statement about records it had every
     * opportunity to see - and not about a still-open transaction pinning the last stable offset, which would
     * make the absence true for a completely different reason.
     * <p>
     * Note what is deliberately <em>not</em> asserted: that the instance fails or shuts down. A correct
     * implementation could equally abort and retry, so requiring a failure would be testing a mechanism rather
     * than the guarantee. The first run of this test did require it, and failed on that scaffolding instead of
     * on the claim.
     */
    @Test
    @ProvesClaim(TransactionalClaim.PRODUCE_MANY_ALL_OR_NONE)
    void aTerminallyFailedSendLeavesTheWholeTransactionInvisible() {
        prepare("failed-send");

        String oversized = repeat(OVERSIZED_VALUE_BYTES);
        List<String> primedKeys = produceInput(PRIMING_RECORDS, PRIMED_KEYS);

        ParallelEoSStreamProcessor<String, String> pc = buildInstance(ONE_COMMIT_ONLY);
        pc.pollAndProduceMany(context -> {
            List<ProducerRecord<String, String>> results = new ArrayList<>();
            for (ConsumerRecord<String, String> record : context.getConsumerRecordsFlattened()) {
                boolean poisoned = record.key().startsWith(POISON_KEYS);
                for (int i = 0; i < RESULTS_PER_INPUT_RECORD; i++) {
                    // one send in the middle of the set, so there are results before it that the producer
                    // accepts and results after it that it never reaches
                    String value = poisoned && i == RESULTS_PER_INPUT_RECORD / 2
                            ? oversized
                            : resultValue(record.key(), i);
                    results.add(new ProducerRecord<>(outputTopic, record.key(), value));
                }
            }
            return results;
        });

        // the startup commit, awaited rather than assumed: until it lands the instance still has one
        // interval-free commit in hand, and could commit part of what follows
        committed.awaitAllVisible(expectedResults(primedKeys, RESULTS_PER_INPUT_RECORD), PROGRESS_TIMEOUT);

        List<String> okKeys = produceInput(OK_RECORDS, OK_KEYS);
        List<String> okResults = expectedResults(okKeys, RESULTS_PER_INPUT_RECORD);

        // the control arm: these results really are on the broker, inside a transaction nobody has committed
        uncommitted.awaitAllVisible(okResults, PROGRESS_TIMEOUT);
        committed.assertNeverVisible(RESULT + OK_KEYS);

        List<String> poisonKeys = produceInput(1, POISON_KEYS);
        log.info("Fed the poisoned record {} - one of its {} results is {} bytes, above the producer's "
                + "max.request.size", poisonKeys, RESULTS_PER_INPUT_RECORD, OVERSIZED_VALUE_BYTES);

        await().atMost(PROGRESS_TIMEOUT)
                .pollInterval(ofMillis(200))
                .untilAsserted(() -> assertWithMessage("the oversized send never reported a %s on the worker "
                                + "path, so the terminal produce failure this test is about did not happen. All "
                                + "captured worker failures: %s",
                        RecordTooLargeException.class.getSimpleName(), workerFailures.describe())
                        .that(workerFailures.mentioning(RecordTooLargeException.class))
                        .isNotEmpty());

        // ask for the commit the long interval would otherwise never reach - the whole point being that it must
        // not succeed, because one of the transaction's records was never produced successfully
        pc.requestCommitAsap();

        // Watch across the commit attempt. Whatever the instance decides to do, it may not make PART of an
        // input record's result set visible - the same assertion the all-together arm applies, which is why a
        // failure here reports "poison-key-0 has 2 of 5" rather than a bare "unexpected record".
        long deadline = System.nanoTime() + COMMIT_ATTEMPT_OBSERVATION_WINDOW.toNanos();
        do {
            committed.poll();
            assertNoPartialResultSetVisible(committed, RESULTS_PER_INPUT_RECORD);
        } while (System.nanoTime() < deadline);

        // resolving the poisoned transaction is what lets the last stable offset move past it, so the sentinel
        // below can become visible and the absence assertions can be about records the verifier could have seen
        closeQuietly(pc);

        String sentinel = SENTINEL + "-after-failure-" + instanceNonce;
        commitOneRecord(register(getKcu().createAndInitNewTransactionalProducer()), sentinel);
        committed.awaitAllVisible(UniLists.of(sentinel), PROGRESS_TIMEOUT);

        assertNoPartialResultSetVisible(committed, RESULTS_PER_INPUT_RECORD);
        committed.assertNoneSeen(RESULT + POISON_KEYS);
        committed.assertNoneSeen(RESULT + OK_KEYS);

        assertWithMessage("the read_uncommitted control arm should still hold the results the read_committed arm "
                + "never saw - if it does not, the absence above was never about the isolation level")
                .that(uncommitted.consumed())
                .containsAtLeastElementsIn(okResults);
    }

    /**
     * Not a claim - a demonstration that {@link #assertNoPartialResultSetVisible} is load-bearing, kept as a
     * running test rather than a comment because an assertion nobody has watched fail is decoration.
     * <p>
     * A result set is split across two committed transactions by a raw producer, which is precisely what a
     * broken all-or-none guarantee would look like from the outside, and the assertion is required to reject the
     * half-visible state and then accept the completed one. Same records, same verifier, same assertion: the only
     * thing that changes is whether the set arrived in one transaction or two.
     */
    @Test
    void thePartialResultSetAssertionRejectsASetSplitAcrossTwoTransactions() {
        prepare("split-control");

        String key = "split-key";
        Producer<String, String> producer = register(getKcu().createAndInitNewTransactionalProducer());

        List<String> firstHalf = UniLists.of(resultValue(key, 0), resultValue(key, 1));
        List<String> secondHalf = UniLists.of(resultValue(key, 2), resultValue(key, 3), resultValue(key, 4));

        producer.beginTransaction();
        firstHalf.forEach(value -> producer.send(new ProducerRecord<>(outputTopic, key, value)));
        producer.commitTransaction();
        committed.awaitAllVisible(firstHalf, PROGRESS_TIMEOUT);

        AssertionError rejected = assertThrows(AssertionError.class,
                () -> assertNoPartialResultSetVisible(committed, RESULTS_PER_INPUT_RECORD),
                "the all-or-none assertion accepted a result set of which only part was visible, so it cannot "
                        + "tell a whole transaction from half of one and proves nothing in the tests above");
        assertWithMessage("the assertion must fail naming the key and how much of its set it could see, or a "
                + "real failure would not say what went wrong")
                .that(rejected.getMessage())
                .contains(key + " has " + firstHalf.size() + " of " + RESULTS_PER_INPUT_RECORD);

        producer.beginTransaction();
        secondHalf.forEach(value -> producer.send(new ProducerRecord<>(outputTopic, key, value)));
        producer.commitTransaction();
        committed.awaitAllVisible(secondHalf, PROGRESS_TIMEOUT);

        // and it accepts the completed set - so what it rejected above was the partiality, not the fixture
        assertNoPartialResultSetVisible(committed, RESULTS_PER_INPUT_RECORD);
    }

    // ---------------------------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------------------------

    private static String repeat(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append('x');
        }
        return sb.toString();
    }

    /**
     * The instance under test has already failed by the time this is called; closing it is only how the poisoned
     * transaction gets aborted, and a producer in an error state legitimately complains on the way out.
     */
    private void closeQuietly(ParallelEoSStreamProcessor<String, String> pc) {
        try {
            pc.close();
        } catch (Exception e) {
            log.info("Closing the failed instance threw, as expected for a producer in an error state", e);
        }
    }
}
