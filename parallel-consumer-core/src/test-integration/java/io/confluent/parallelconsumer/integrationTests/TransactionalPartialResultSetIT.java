package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import io.confluent.parallelconsumer.internal.ProducerManager;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.truth.Truth.assertWithMessage;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.awaitility.Awaitility.await;

/**
 * A transactional PC must publish the result set of a source record ALL-OR-NONE. This pins the case where it
 * published neither - a PARTIAL result set, permanently missing the record that could not be sent.
 * <p>
 * {@link ProducerManager#produceMessages} installed a producer {@link org.apache.kafka.clients.producer.Callback}
 * that <em>threw</em> from {@code onCompletion}, under a comment saying that was "only needed if not using tx" -
 * while installing it unconditionally, transactional mode included.
 * {@link org.apache.kafka.clients.producer.KafkaProducer#send} invokes that callback from inside its own
 * {@code catch (ApiException)} handler and only <em>afterwards</em> calls
 * {@code transactionManager.maybeTransitionToErrorState(e)}. The throw escaped first, so a terminally failed send
 * never moved the transaction into an abortable state: the result records the producer had already accepted stayed
 * in the transaction, the next commit succeeded, and a {@code read_committed} consumer saw part of a result set
 * that can never be completed.
 * <p>
 * The test drives that directly. One source record ({@link #POISON_KEY}) returns {@link #RESULTS_PER_INPUT}
 * result records of which one is deliberately larger than the producer's {@code max.request.size}
 * ({@link #OVERSIZED_VALUE_BYTES} bytes), so its send fails terminally with {@link RecordTooLargeException} - the
 * sends before it having already been accepted. The assertion is then simply that no key is ever visible with
 * <em>some</em> of its results: without the fix, the poison key shows up as "has 2 of 5".
 * <p>
 * Notes on how this is measured, both of which are load-bearing:
 * <ul>
 *     <li><b>Consumed records are counted, never offsets.</b> The output topic is transactional, so commit and
 *     abort markers occupy offsets in it - offset arithmetic would overstate the count.</li>
 *     <li><b>Healthy source records are fed in after the poison one</b>
 *     ({@link #settleWhileNudgingCommits()}) so that a commit is actually attempted on the transaction holding the
 *     partial set. Without them nothing succeeds, nothing is dirty, no commit is attempted, and the partial set
 *     stays invisible with or without the fix - i.e. the test would pass either way.</li>
 *     <li><b>Absence is only asserted after non-vacuity is proven</b> ({@link #proveVerifierIsActuallyReading()}):
 *     the verifying consumer is shown to hold a non-empty assignment AND to have already consumed a set of
 *     known-committed result records. A consumer that is assigned nothing trivially satisfies "no partial set
 *     is visible" and would prove nothing at all.</li>
 * </ul>
 *
 * @author Antony Stubbs
 * @see ProducerManager#produceMessages
 */
@Tag("transactions")
@Slf4j
class TransactionalPartialResultSetIT extends BrokerIntegrationTest<String, String> {

    /**
     * Result records produced per source record. Deliberately tiny: everything visible must comfortably fit in a
     * single fetch, otherwise {@code max.partition.fetch.bytes} / {@code max.poll.records} truncation could
     * masquerade as a partial result set.
     */
    private static final int RESULTS_PER_INPUT = 5;

    /**
     * Which of the {@link #RESULTS_PER_INPUT} results of {@link #POISON_KEY} is the oversized one. Must be in the
     * middle: results before it are accepted into the transaction (they are what becomes visible when the bug is
     * present), results after it are never even attempted.
     */
    private static final int POISON_RESULT_INDEX = 2;

    private static final int GOOD_INPUT_RECORDS = 3;

    /**
     * Comfortably above the producer default {@link ProducerConfig#MAX_REQUEST_SIZE_CONFIG} of 1MiB, so
     * {@code KafkaProducer} rejects the record itself (client side, deterministically) rather than the test
     * depending on a broker-side size limit.
     */
    private static final int OVERSIZED_VALUE_BYTES = 2 * 1024 * 1024;

    private static final String POISON_KEY = "poison-key-0";

    /**
     * How long to let the system settle after the poison record has been processed. Without the fix, this is the
     * window in which the partial set gets committed and becomes visible - which takes one commit interval after
     * the first nudge, so this is many times longer than it needs to be.
     */
    private static final Duration SETTLE_TIMEOUT = ofSeconds(20);

    /**
     * Gap between commit nudges - see {@link #settleWhileNudgingCommits()}. Longer than the commit interval, so
     * each nudge gets its own commit attempt.
     */
    private static final Duration COMMIT_NUDGE_INTERVAL = ofSeconds(2);

    /**
     * Longer than the whole test window on purpose, so the poison record is attempted EXACTLY ONCE. Retries would
     * re-send the same accepted-then-abandoned prefix over and over, inflating the visible count of a partial set
     * for no extra diagnostic value ("has 2 of 5" reads better than "has 14 of 5").
     */
    private static final Duration NO_RETRY_WITHIN_THIS_TEST = ofSeconds(120);

    private String outputTopic;

    private ParallelEoSStreamProcessor<String, String> pc;

    /**
     * A {@code read_committed} consumer - {@link KafkaClientUtils#setupConsumerProps} already sets that isolation
     * level, so nothing here configures it. This is the only thing that gets to say what is "visible".
     */
    private Consumer<String, String> verifier;

    /**
     * Every result record the {@link #verifier} has consumed, by the key of the source record it derives from.
     * Records, not offsets.
     */
    private final Map<String, List<String>> resultsSeenByInputKey = new LinkedHashMap<>();

    /**
     * Source keys the user function has been invoked for. Written by PC worker threads, read by the test thread.
     */
    private final Set<String> sourceKeysProcessed = ConcurrentHashMap.newKeySet();

    private final List<String> goodKeys = IntStream.range(0, GOOD_INPUT_RECORDS)
            .mapToObj(i -> "good-key-" + i)
            .collect(Collectors.toList());

    /**
     * Source keys sent AFTER the poison record, to force commit attempts - see
     * {@link #settleWhileNudgingCommits()}. They are ordinary healthy records, so the all-or-none rule applies to
     * them too and they are asserted alongside everything else.
     */
    private final List<String> nudgeKeys = new ArrayList<>();

    @BeforeEach
    void setUp() {
        setupTopic(TransactionalPartialResultSetIT.class.getSimpleName());
        outputTopic = getTopic() + "-output";
        getKcu().createTopic(outputTopic, numPartitions);

        // PC's consumer first - createNewConsumer(NEW_GROUP) overwrites the shared group id, so the verifier
        // must take its own explicit group afterwards
        var pcConsumer = getKcu().<String, String>createNewConsumer(KafkaClientUtils.GroupOption.NEW_GROUP);

        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(pcConsumer)
                .producer(getKcu().createNewProducer(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER))
                .commitMode(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER)
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
                .batchSize(1)
                .commitInterval(ofSeconds(1))
                .defaultMessageRetryDelay(NO_RETRY_WITHIN_THIS_TEST)
                .build();

        pc = new ParallelEoSStreamProcessor<>(options);
        pc.subscribe(UniLists.of(getTopic()));

        verifier = getKcu().createNewConsumer("partial-result-set-verifier-" + nextInt());
        verifier.subscribe(UniLists.of(outputTopic));
    }

    @AfterEach
    void tearDown() {
        closeQuietly();
        if (verifier != null) {
            verifier.close();
        }
    }

    @Test
    void aTerminallyFailedSendMustNotLeaveHalfOfAResultSetVisible() {
        String oversizedValue = repeat('x', OVERSIZED_VALUE_BYTES);

        pc.pollAndProduceMany(context -> {
            String sourceKey = context.key();
            sourceKeysProcessed.add(sourceKey);
            log.info("Processing source key {}, returning {} result records", sourceKey, RESULTS_PER_INPUT);

            List<ProducerRecord<String, String>> resultSet = new ArrayList<>(RESULTS_PER_INPUT);
            for (int i = 0; i < RESULTS_PER_INPUT; i++) {
                boolean thisOneCannotBeSent = POISON_KEY.equals(sourceKey) && i == POISON_RESULT_INDEX;
                String value = thisOneCannotBeSent ? oversizedValue : "result-" + i;
                resultSet.add(new ProducerRecord<>(outputTopic, sourceKey, value));
            }
            return resultSet;
        });

        // phase one: healthy traffic, committed and read back - this is the non-vacuity anchor
        goodKeys.forEach(key -> sendSourceRecord(key));
        proveVerifierIsActuallyReading();

        // phase two: the source record whose result set contains a send that can never succeed
        sendSourceRecord(POISON_KEY);
        await("the poison source record to reach the user function")
                .atMost(SETTLE_TIMEOUT)
                .until(() -> sourceKeysProcessed.contains(POISON_KEY));
        settleWhileNudgingCommits();

        // the abort (if there is one to do) happens on close, so read only once PC is fully down
        closeQuietly();
        drainFor(ofSeconds(10));

        assertNoPartialResultSetIsVisible();
    }

    /**
     * Proves the {@link #verifier} can see things before anything is asserted about what it cannot see: it must
     * hold a non-empty assignment, and it must already have consumed the full, known-committed result sets of the
     * healthy source records. Without this, {@link #assertNoPartialResultSetIsVisible()} would pass against a
     * consumer that had simply never been assigned the output topic.
     */
    private void proveVerifierIsActuallyReading() {
        await("the verifier to consume every result record of the healthy source records")
                .atMost(ofSeconds(120))
                .pollInterval(ofMillis(200))
                .untilAsserted(() -> {
                    drainFor(ofMillis(500));
                    for (String key : goodKeys) {
                        assertWithMessage("result records consumed for healthy source key %s", key)
                                .that(resultsFor(key))
                                .hasSize(RESULTS_PER_INPUT);
                    }
                });

        assertWithMessage("the verifying consumer must hold a non-empty assignment, else asserting the ABSENCE "
                + "of a partial result set below proves nothing")
                .that(verifier.assignment())
                .isNotEmpty();

        int knownCommittedRecordsConsumed = GOOD_INPUT_RECORDS * RESULTS_PER_INPUT;
        assertWithMessage("the verifying consumer must already have consumed known-committed result records, else "
                + "asserting the ABSENCE of a partial result set below proves nothing")
                .that(totalResultsSeen())
                .isAtLeast(knownCommittedRecordsConsumed);

        log.info("Verifier non-vacuity proven: assignment {}, {} known-committed result records consumed",
                verifier.assignment(), totalResultsSeen());
    }

    /**
     * The assertion this class exists for. Counts CONSUMED RECORDS per source key - never offsets, as commit and
     * abort markers occupy offsets on a transactional topic and would overstate every count here.
     * <p>
     * Note the oversized result record itself can never be delivered, so for {@link #POISON_KEY} the only legal
     * outcome is that NONE of its result set is visible.
     */
    private void assertNoPartialResultSetIsVisible() {
        List<String> allSourceKeys = new ArrayList<>(goodKeys);
        allSourceKeys.addAll(nudgeKeys);
        allSourceKeys.add(POISON_KEY);
        // anything the verifier saw that the test did not send is itself a result worth failing on
        resultsSeenByInputKey.keySet().stream()
                .filter(key -> !allSourceKeys.contains(key))
                .forEach(allSourceKeys::add);

        log.info("Result records visible per source key: {}", resultsSeenByInputKey.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().size())));

        for (String key : allSourceKeys) {
            int visible = resultsFor(key).size();
            assertWithMessage("%s has %s of %s result records visible to a read_committed consumer - a result set "
                            + "must be published all-or-none, so a partial one means a terminally failed send did "
                            + "not abort its transaction",
                    key, visible, RESULTS_PER_INPUT)
                    .that(visible)
                    .isAnyOf(0, RESULTS_PER_INPUT);
        }
    }

    /**
     * Let the system settle after the failed send, while feeding it healthy source records ("commit nudges").
     * <p>
     * The nudges are ESSENTIAL, not decoration. PC only attempts a commit when something has SUCCEEDED since the
     * last one ({@code WorkManager#isDirty} gates the commit), and the poison record fails - so with no further
     * traffic, nothing is dirty, no commit is attempted, and the half-populated transaction is simply never
     * committed. Nothing would be visible EITHER WAY and the test could not tell the fix from the regression.
     * Each nudge is a source record that succeeds, marking the partition dirty and forcing a commit attempt on a
     * transaction that already holds the partial result set: with the fix the transaction is in an abortable error
     * state and that commit cannot succeed, without it the commit goes through and publishes the partial set.
     * <p>
     * Stops early once PC has died, which is what it does when it finally cannot commit. PC surviving is a
     * legitimate outcome to continue from (that is the buggy behaviour, and the assertion on visible records is
     * what has to catch it) - failing here instead would report the regression as scaffolding trouble.
     */
    private void settleWhileNudgingCommits() {
        Instant deadline = Instant.now().plus(SETTLE_TIMEOUT);
        int nudge = 0;
        while (Instant.now().isBefore(deadline)) {
            if (pc.isClosedOrFailed()) {
                log.info("PC closed or failed after the terminally failed send - it could not commit the "
                        + "transaction holding the partial result set, which is the point");
                return;
            }
            String nudgeKey = "commit-nudge-key-" + nudge++;
            nudgeKeys.add(nudgeKey);
            sendSourceRecord(nudgeKey);
            drainFor(COMMIT_NUDGE_INTERVAL);
        }
        log.info("PC still running {} after the terminally failed send - continuing; whether a PARTIAL result "
                + "set became visible is the actual check", SETTLE_TIMEOUT);
    }

    @SneakyThrows
    private void sendSourceRecord(String key) {
        log.info("Sending source record with key {}", key);
        getKcu().getProducer().send(new ProducerRecord<>(getTopic(), key, "source-" + key)).get();
    }

    /**
     * Polls the verifier for the given duration, accumulating what it consumes into
     * {@link #resultsSeenByInputKey}.
     */
    private void drainFor(Duration duration) {
        Instant deadline = Instant.now().plus(duration);
        do {
            ConsumerRecords<String, String> polled = verifier.poll(ofMillis(250));
            for (ConsumerRecord<String, String> record : polled) {
                resultsSeenByInputKey.computeIfAbsent(record.key(), ignored -> new ArrayList<>())
                        .add(record.value());
            }
        } while (Instant.now().isBefore(deadline));
    }

    private List<String> resultsFor(String key) {
        return resultsSeenByInputKey.getOrDefault(key, Collections.emptyList());
    }

    private int totalResultsSeen() {
        return resultsSeenByInputKey.values().stream().mapToInt(List::size).sum();
    }

    /**
     * PC may already have failed and closed itself by this point, and is closed again from
     * {@link #tearDown()} - so closing must never be what fails this test.
     */
    private void closeQuietly() {
        if (pc == null) {
            return;
        }
        try {
            pc.close();
        } catch (Exception closingAnAlreadyBrokenPc) {
            log.info("Ignoring exception closing PC - the visible-records assertion is the check that matters",
                    closingAnAlreadyBrokenPc);
        }
        pc = null;
    }

    /**
     * Java 8 baseline - no {@code String#repeat}.
     */
    private static String repeat(char c, int length) {
        char[] chars = new char[length];
        java.util.Arrays.fill(chars, c);
        return new String(chars);
    }

}
