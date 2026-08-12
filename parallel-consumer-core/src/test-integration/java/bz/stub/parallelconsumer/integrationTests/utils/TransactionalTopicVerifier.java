package bz.stub.parallelconsumer.integrationTests.utils;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.IsolationLevel;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertWithMessage;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.awaitility.Awaitility.await;

/**
 * A consumer that watches one topic and remembers, as VALUES, everything it has actually consumed - plus the
 * assertions that a transactional visibility test is allowed to make about it.
 * <p>
 * Shared by every test that asks "what can a consumer see, and when?", because the two traps such a test has to
 * be built around are the same wherever it is written, and both are silent when you get them wrong:
 * <ol>
 *     <li><b>Vacuity.</b> "Nothing is visible" is trivially true of a consumer that has not been assigned
 *     anything yet, so an absence assertion made too early passes while testing nothing - the failure mode
 *     written up in {@code docs/solutions/test-flakiness/vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md}.
 *     Nothing here may assert an absence until {@link #requireLiveAndCaughtUp} has passed for the consumer doing
 *     the asserting, and {@code TransactionalVisibilityIT#theAbsenceAssertionIsVacuousWithoutTheNonVacuityGuard}
 *     demonstrates - as a running test, not a comment - that the guard is load-bearing.</li>
 *     <li><b>Marker inflation.</b> Commit and abort markers occupy offsets, so on a transactional topic the end
 *     offset is not the record count and their difference is not "how many records are there". Nothing here
 *     derives a count from an offset: this class counts the records it actually consumed.</li>
 * </ol>
 * Absence assertions are only worth making against a {@code read_uncommitted} control arm - see
 * {@link #readUncommitted} - that <em>does</em> see the records, so "absent" is demonstrably "hidden by the
 * isolation level" rather than "never written".
 *
 * @author Antony Stubbs
 */
@Slf4j
public class TransactionalTopicVerifier implements AutoCloseable {

    /**
     * How long an absence is observed before it is believed.
     * <p>
     * This is not a race against the broker: an absence assertion should only run after a control arm has
     * already seen the very records being asserted absent, so they are known to be on the broker before the
     * window opens. The window exists to catch a late arrival, not to wait for a first one, which is why it is
     * short enough to run in a shared lane.
     */
    public static final Duration ABSENCE_OBSERVATION_WINDOW = ofSeconds(3);

    public static final Duration VISIBILITY_TIMEOUT = ofSeconds(60);

    private static final Duration POLL_TIME = ofMillis(500);

    private final String name;

    private final KafkaConsumer<String, String> consumer;

    private final List<String> consumed = new ArrayList<>();

    private final List<ConsumerRecord<String, String>> consumedRecords = new ArrayList<>();

    private TransactionalTopicVerifier(String name, KafkaConsumer<String, String> consumer) {
        this.name = name;
        this.consumer = consumer;
    }

    /**
     * A verifier at the default {@code read_committed} - {@link KafkaClientUtils#setupConsumerProps} already sets
     * the isolation level, so the arm under test needs no configuration at all.
     */
    public static TransactionalTopicVerifier readCommitted(KafkaClientUtils kcu, String name, String topicToWatch) {
        KafkaConsumer<String, String> consumer = kcu.createNewConsumer(uniqueGroup(name));
        consumer.subscribe(UniSets.of(topicToWatch));
        return new TransactionalTopicVerifier(name + " (read_committed)", consumer);
    }

    /**
     * The control arm. It is the {@code read_uncommitted} consumer that has to opt in, and its job is to make an
     * absence assertion falsifiable: it sees the records the {@code read_committed} arm cannot, so "absent" means
     * "the isolation level hid them", not "the producer never wrote them".
     */
    @SuppressWarnings("deprecation") // the (groupId, Properties) overload is the only way to override isolation.level
    public static TransactionalTopicVerifier readUncommitted(KafkaClientUtils kcu, String name, String topicToWatch) {
        Properties overrides = new Properties();
        overrides.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                IsolationLevel.READ_UNCOMMITTED.toString().toLowerCase(Locale.ROOT));
        KafkaConsumer<String, String> consumer = kcu.createNewConsumer(uniqueGroup(name), overrides);
        consumer.subscribe(UniSets.of(topicToWatch));
        return new TransactionalTopicVerifier(name + " (read_uncommitted)", consumer);
    }

    private static String uniqueGroup(String name) {
        return "verifier-" + name + "-" + nextInt();
    }

    public String name() {
        return name;
    }

    public KafkaConsumer<String, String> consumer() {
        return consumer;
    }

    /**
     * @return every value consumed so far, in consumption order - counted, never inferred from offsets
     */
    public List<String> consumed() {
        return consumed;
    }

    /**
     * @return every record consumed so far. Needed by callers that have to reason about the <em>structure</em> of
     *         the log - where one transaction ends and the next begins - rather than only its contents.
     */
    public List<ConsumerRecord<String, String>> consumedRecords() {
        return consumedRecords;
    }

    /**
     * @return the values returned by this one poll - the batch boundary matters to
     *         {@link #awaitFirstBatchContaining}, which is why the batch is returned rather than only accumulated
     */
    public List<String> poll() {
        ConsumerRecords<String, String> records = consumer.poll(POLL_TIME);
        List<String> batch = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            batch.add(record.value());
            consumedRecords.add(record);
        }
        consumed.addAll(batch);
        if (!batch.isEmpty()) {
            log.debug("Verifier {} consumed {} records: {}", name, batch.size(), batch);
        }
        return batch;
    }

    public List<String> seenWithPrefix(String prefix) {
        return withPrefix(consumed, prefix);
    }

    public static List<String> withPrefix(List<String> values, String prefix) {
        return values.stream()
                .filter(value -> value.startsWith(prefix))
                .collect(Collectors.toList());
    }

    /**
     * The non-vacuity guard. No absence may be asserted about this verifier until this has passed.
     * <p>
     * Two clauses, and both are needed. A non-empty assignment alone is not enough - a consumer can be assigned
     * and still be positioned before everything the test cares about - so the guard also requires that the
     * consumer has actually <em>consumed</em> a record known to be committed and to precede the window under
     * test. Only then does "and it has not seen the records under test" mean anything.
     *
     * @param mustHaveSeen a value committed BEFORE the transaction under test opened
     */
    public void requireLiveAndCaughtUp(String mustHaveSeen, Duration atMost) {
        log.info("Establishing non-vacuity for verifier {}: awaiting assignment and the committed marker '{}'",
                name, mustHaveSeen);
        await().atMost(atMost)
                .pollInterval(ofMillis(200))
                .untilAsserted(() -> {
                    poll();
                    assertWithMessage("verifier %s holds no partition assignment, so any assertion that it "
                            + "cannot see something is vacuous", name)
                            .that(consumer.assignment())
                            .isNotEmpty();
                    assertWithMessage("verifier %s has not yet consumed the committed marker '%s', so it has not "
                                    + "reached the window under test and any absence assertion is vacuous",
                            name, mustHaveSeen)
                            .that(consumed)
                            .contains(mustHaveSeen);
                });
    }

    public void requireLiveAndCaughtUp(String mustHaveSeen) {
        requireLiveAndCaughtUp(mustHaveSeen, VISIBILITY_TIMEOUT);
    }

    /**
     * Assert against what this verifier has consumed SO FAR, without polling.
     * <p>
     * Kept separate from {@link #assertNeverVisible} deliberately: this is the raw assertion, and
     * {@code TransactionalVisibilityIT#theAbsenceAssertionIsVacuousWithoutTheNonVacuityGuard} needs to run
     * exactly this against a consumer that never looked, to show what it is worth on its own.
     */
    public void assertNoneSeen(String prefix) {
        assertWithMessage("verifier %s must not have seen any '%s' record", name, prefix)
                .that(seenWithPrefix(prefix))
                .isEmpty();
    }

    /**
     * Keep consuming for {@link #ABSENCE_OBSERVATION_WINDOW} and require that nothing with this prefix ever turns
     * up.
     */
    public void assertNeverVisible(String prefix) {
        long deadline = System.nanoTime() + ABSENCE_OBSERVATION_WINDOW.toNanos();
        do {
            poll();
            assertNoneSeen(prefix);
        } while (System.nanoTime() < deadline);
        log.info("Verifier {} saw no '{}' record across a {} observation window, having already consumed {} records",
                name, prefix, ABSENCE_OBSERVATION_WINDOW, consumed.size());
    }

    public void awaitAllVisible(List<String> expected) {
        awaitAllVisible(expected, VISIBILITY_TIMEOUT);
    }

    public void awaitAllVisible(List<String> expected, Duration atMost) {
        await().atMost(atMost)
                .pollInterval(ofMillis(200))
                .untilAsserted(() -> {
                    poll();
                    assertWithMessage("verifier %s should have consumed every expected record", name)
                            .that(consumed)
                            .containsAtLeastElementsIn(expected);
                });
    }

    /**
     * @return the values, matching {@code prefix}, of the FIRST poll batch in which any of them appeared - the
     *         operational form of "all of them, or none of them". Partial visibility of a committed transaction
     *         would show up as a first batch holding some but not all of its records.
     */
    public List<String> awaitFirstBatchContaining(String prefix) {
        AtomicReference<List<String>> firstBatch = new AtomicReference<>();
        await().atMost(VISIBILITY_TIMEOUT)
                .pollInterval(ofMillis(200))
                .untilAsserted(() -> {
                    List<String> matching = withPrefix(poll(), prefix);
                    if (!matching.isEmpty() && firstBatch.get() == null) {
                        firstBatch.set(matching);
                    }
                    assertWithMessage("verifier %s never saw a '%s' record", name, prefix)
                            .that(firstBatch.get())
                            .isNotNull();
                });
        return firstBatch.get();
    }

    @Override
    public void close() {
        consumer.close();
    }
}
