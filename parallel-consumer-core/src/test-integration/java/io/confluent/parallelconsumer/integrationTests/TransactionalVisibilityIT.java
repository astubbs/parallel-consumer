package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.ProvesClaim;
import io.confluent.parallelconsumer.TransactionalClaim;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.IsolationLevel;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.Optional.empty;
import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The visibility half of {@link io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode#PERIODIC_TRANSACTIONAL_PRODUCER}'s
 * documented guarantees, proved against a real broker: <em>what</em> a consumer can see, and <em>when</em>.
 * <p>
 * These claims are broker-only by construction. {@code MockProducer} models the transaction state machine but
 * has no log, no commit markers, no isolation level and no transaction timeout, so a claim containing the word
 * "visible" cannot be proved in the unit lane at all - only asserted about a mock that was told to agree.
 * <p>
 * <b>Two traps this class is built around; break either and it proves nothing.</b>
 * <ol>
 *     <li><b>Vacuity.</b> "Nothing is visible" is trivially true of a consumer that has not been assigned
 *     anything yet, so an absence assertion made too early passes while testing nothing - the failure mode
 *     written up in {@code docs/solutions/test-flakiness/vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md}.
 *     Every absence assertion here is therefore preceded by {@link #requireLiveAndCaughtUp}, and
 *     {@link #theAbsenceAssertionIsVacuousWithoutTheNonVacuityGuard()} exists to demonstrate - as a running
 *     test, not a comment - that the guard is load-bearing.</li>
 *     <li><b>Marker inflation.</b> Commit and abort markers occupy offsets, so on a transactional topic the end
 *     offset is not the record count and their difference is not "how many records are there". Nothing here
 *     derives a count from an offset: {@link Verifier} counts the records it actually consumed.</li>
 * </ol>
 * Each absence assertion is also paired with a {@code read_uncommitted} control arm that <em>does</em> see the
 * records, so "absent" is demonstrably "hidden by the isolation level" rather than "never written".
 *
 * @author Antony Stubbs
 * @see TransactionalClaim
 * @see TransactionTimeoutsTest
 */
@Tag("transactions")
@Slf4j
class TransactionalVisibilityIT extends BrokerIntegrationTest<String, String> {

    private static final int RECORDS_PER_TRANSACTION = 4;

    private static final String WARMUP = "warmup";
    private static final String IN_FLIGHT = "in-flight";
    private static final String STILL_OPEN = "still-open";
    private static final String LATER_COMMITTED = "later-committed";
    private static final String ABORTED = "aborted";
    private static final String TIMED_OUT = "timed-out";
    private static final String SENTINEL = "sentinel";
    private static final String COMMITTED_AND_VISIBLE = "committed-and-visible";

    /**
     * How long an absence is observed before it is believed.
     * <p>
     * This is not a race against the broker: every absence assertion runs only after the
     * {@code read_uncommitted} control arm has already seen the very records being asserted absent, so they are
     * known to be on the broker before the window opens. The window exists to catch a late arrival, not to wait
     * for a first one, which is why it is short enough to run in a shared lane.
     */
    private static final Duration ABSENCE_OBSERVATION_WINDOW = ofSeconds(3);

    private static final Duration VISIBILITY_TIMEOUT = ofSeconds(60);

    /**
     * The wait for the broker to reap a transaction that blew its {@code transaction.timeout.ms}. Generous
     * because the reap is the timeout plus up to one broker cleanup tick
     * ({@code transaction.abort.timed.out.transaction.cleanup.interval.ms}, 10s by default), and a shared
     * broker's tick is nobody's to schedule.
     */
    private static final Duration TRANSACTION_REAP_TIMEOUT = ofSeconds(120);

    private static final Duration POLL_TIME = ofMillis(500);

    private final List<AutoCloseable> toClose = new ArrayList<>();

    @AfterEach
    void closeClients() {
        for (AutoCloseable closeable : toClose) {
            try {
                closeable.close();
            } catch (Exception e) {
                // Teardown only, and after every assertion has run, so this cannot mask a result. A producer
                // whose transaction the broker already reaped (the timeout arm) legitimately fails to close
                // cleanly - that is the scenario, not a defect, so it is logged rather than thrown.
                log.warn("Problem closing test client {} - tolerated during teardown", closeable, e);
            }
        }
        toClose.clear();
    }

    /**
     * Records consumed so far by one verifying consumer, kept as VALUES and counted - never inferred from
     * offsets. On a transactional topic the offsets include commit and abort markers, so offset arithmetic
     * overstates the record count and would make every "how many are visible?" answer wrong in the direction
     * that hides a bug.
     */
    private static final class Verifier implements AutoCloseable {

        // package-private on purpose: private members of a nested class are read through synthetic
        // accessors, and Truth then reports the failing subject as "value of: access$NNN(...)" instead of
        // naming it - which is exactly the moment you most want the message to be readable
        final String name;
        final KafkaConsumer<String, String> consumer;
        final List<String> consumed = new ArrayList<>();

        Verifier(String name, KafkaConsumer<String, String> consumer) {
            this.name = name;
            this.consumer = consumer;
        }

        /**
         * @return the values returned by this one poll - the batch boundary matters to
         *         {@link #awaitFirstBatchContaining}, which is why the batch is returned rather than only
         *         accumulated
         */
        List<String> poll() {
            ConsumerRecords<String, String> records = consumer.poll(POLL_TIME);
            List<String> batch = new ArrayList<>();
            for (ConsumerRecord<String, String> record : records) {
                batch.add(record.value());
            }
            consumed.addAll(batch);
            if (!batch.isEmpty()) {
                log.debug("Verifier {} consumed {} records: {}", name, batch.size(), batch);
            }
            return batch;
        }

        List<String> seenWithPrefix(String prefix) {
            return withPrefix(consumed, prefix);
        }

        @Override
        public void close() {
            consumer.close();
        }
    }

    private static List<String> withPrefix(List<String> values, String prefix) {
        return values.stream()
                .filter(value -> value.startsWith(prefix))
                .collect(Collectors.toList());
    }

    /**
     * A verifier at the default {@code read_committed} - {@code KafkaClientUtils#setupConsumerProps} already
     * sets the isolation level, so the arm under test needs no configuration at all.
     */
    private Verifier readCommittedVerifier(String name, String topicToWatch) {
        KafkaConsumer<String, String> consumer = getKcu().createNewConsumer(uniqueGroup(name));
        consumer.subscribe(UniSets.of(topicToWatch));
        return register(new Verifier(name + " (read_committed)", consumer));
    }

    /**
     * The control arm. It is the {@code read_uncommitted} consumer that has to opt in, and its job is to make
     * every absence assertion in this class falsifiable: it sees the records the {@code read_committed} arm
     * cannot, so "absent" means "the isolation level hid them", not "the producer never wrote them".
     */
    @SuppressWarnings("deprecation") // the (groupId, Properties) overload is the only way to override isolation.level
    private Verifier readUncommittedControl(String name, String topicToWatch) {
        Properties overrides = new Properties();
        overrides.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                IsolationLevel.READ_UNCOMMITTED.toString().toLowerCase(Locale.ROOT));
        KafkaConsumer<String, String> consumer = getKcu().createNewConsumer(uniqueGroup(name), overrides);
        consumer.subscribe(UniSets.of(topicToWatch));
        return register(new Verifier(name + " (read_uncommitted)", consumer));
    }

    private Verifier register(Verifier verifier) {
        toClose.add(verifier);
        return verifier;
    }

    private static String uniqueGroup(String name) {
        return "visibility-" + name + "-" + nextInt();
    }

    /**
     * The non-vacuity guard. Nothing in this class may assert an absence until this has passed for the consumer
     * doing the asserting.
     * <p>
     * Two clauses, and both are needed. A non-empty assignment alone is not enough - a consumer can be assigned
     * and still be positioned before everything the test cares about - so the guard also requires that the
     * consumer has actually <em>consumed</em> a record known to be committed and to precede the window under
     * test. Only then does "and it has not seen the in-flight records" mean anything.
     *
     * @param mustHaveSeen a value committed BEFORE the transaction under test opened
     */
    private void requireLiveAndCaughtUp(Verifier verifier, String mustHaveSeen, Duration atMost) {
        log.info("Establishing non-vacuity for verifier {}: awaiting assignment and the committed marker '{}'",
                verifier.name, mustHaveSeen);
        await().atMost(atMost)
                .pollInterval(ofMillis(200))
                .untilAsserted(() -> {
                    verifier.poll();
                    assertWithMessage("verifier %s holds no partition assignment, so any assertion that it "
                            + "cannot see something is vacuous", verifier.name)
                            .that(verifier.consumer.assignment())
                            .isNotEmpty();
                    assertWithMessage("verifier %s has not yet consumed the committed marker '%s', so it has not "
                            + "reached the window under test and any absence assertion is vacuous",
                            verifier.name, mustHaveSeen)
                            .that(verifier.consumed)
                            .contains(mustHaveSeen);
                });
    }

    private void requireLiveAndCaughtUp(Verifier verifier, String mustHaveSeen) {
        requireLiveAndCaughtUp(verifier, mustHaveSeen, VISIBILITY_TIMEOUT);
    }

    /**
     * Assert against what the verifier has consumed SO FAR, without polling.
     * <p>
     * Kept separate from {@link #assertNeverVisible} deliberately: this is the raw assertion, and
     * {@link #theAbsenceAssertionIsVacuousWithoutTheNonVacuityGuard()} needs to run exactly this against a
     * consumer that never looked, to show what it is worth on its own.
     */
    private static void assertNoneSeen(Verifier verifier, String prefix) {
        assertWithMessage("verifier %s must not have seen any '%s' record", verifier.name, prefix)
                .that(verifier.seenWithPrefix(prefix))
                .isEmpty();
    }

    /**
     * Keep consuming for {@link #ABSENCE_OBSERVATION_WINDOW} and require that nothing with this prefix ever
     * turns up.
     */
    private static void assertNeverVisible(Verifier verifier, String prefix) {
        long deadline = System.nanoTime() + ABSENCE_OBSERVATION_WINDOW.toNanos();
        do {
            verifier.poll();
            assertNoneSeen(verifier, prefix);
        } while (System.nanoTime() < deadline);
        log.info("Verifier {} saw no '{}' record across a {} observation window, having already consumed {} records",
                verifier.name, prefix, ABSENCE_OBSERVATION_WINDOW, verifier.consumed.size());
    }

    private static void awaitAllVisible(Verifier verifier, List<String> expected) {
        await().atMost(VISIBILITY_TIMEOUT)
                .pollInterval(ofMillis(200))
                .untilAsserted(() -> {
                    verifier.poll();
                    assertWithMessage("verifier %s should have consumed every expected record", verifier.name)
                            .that(verifier.consumed)
                            .containsAtLeastElementsIn(expected);
                });
    }

    /**
     * @return the values, matching {@code prefix}, of the FIRST poll batch in which any of them appeared - the
     *         operational form of "all of them, or none of them". Partial visibility of a committed transaction
     *         would show up as a first batch holding some but not all of its records.
     */
    private static List<String> awaitFirstBatchContaining(Verifier verifier, String prefix) {
        AtomicReference<List<String>> firstBatch = new AtomicReference<>();
        await().atMost(VISIBILITY_TIMEOUT)
                .pollInterval(ofMillis(200))
                .untilAsserted(() -> {
                    List<String> matching = withPrefix(verifier.poll(), prefix);
                    if (!matching.isEmpty() && firstBatch.get() == null) {
                        firstBatch.set(matching);
                    }
                    assertWithMessage("verifier %s never saw a '%s' record", verifier.name, prefix)
                            .that(firstBatch.get())
                            .isNotNull();
                });
        return firstBatch.get();
    }

    private static List<String> valuesFor(String prefix, int count) {
        List<String> values = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            values.add(prefix + "-" + i);
        }
        return values;
    }

    private void send(Producer<String, String> producer, String topicToUse, List<String> values) {
        for (String value : values) {
            producer.send(new ProducerRecord<>(topicToUse, value, value));
        }
        // acked before the test looks away - so a later "not visible" result cannot be "not written yet"
        producer.flush();
    }

    /**
     * One committed transaction holding a single record, whose value the verifiers then use as their
     * non-vacuity marker.
     */
    private String commitMarkerRecord(Producer<String, String> producer, String topicToUse, String value) {
        producer.beginTransaction();
        send(producer, topicToUse, UniLists.of(value));
        producer.commitTransaction();
        return value;
    }

    private Producer<String, String> transactionalProducer() {
        return register(getKcu().createAndInitNewTransactionalProducer());
    }

    private Producer<String, String> transactionalProducer(Duration transactionTimeout) {
        return register(getKcu().createAndInitNewTransactionalProducer(transactionTimeout, empty()));
    }

    private Producer<String, String> register(Producer<String, String> producer) {
        toClose.add(producer);
        return producer;
    }

    /**
     * C2 and C6: records written into a transaction that is still open are invisible at {@code read_committed}
     * and visible at {@code read_uncommitted}; committing makes all of them visible in one step.
     * <p>
     * The {@code read_uncommitted} arm is what makes this a proof rather than a tautology - without it, a
     * broker that silently dropped the sends would produce exactly the same green result.
     */
    @Test
    @ProvesClaim({TransactionalClaim.ALL_OR_NONE_PER_SOURCE_OFFSET,
            TransactionalClaim.READ_COMMITTED_BLOCKED_TO_FIRST_OPEN_TX})
    void openTransactionIsInvisibleAtReadCommittedAndVisibleAtReadUncommitted() {
        setupTopic(getClass().getSimpleName());
        Producer<String, String> producer = transactionalProducer();

        String marker = commitMarkerRecord(producer, getTopic(), WARMUP + "-0");

        Verifier committed = readCommittedVerifier("open-tx", getTopic());
        Verifier uncommitted = readUncommittedControl("open-tx", getTopic());
        requireLiveAndCaughtUp(committed, marker);
        requireLiveAndCaughtUp(uncommitted, marker);

        List<String> inFlight = valuesFor(IN_FLIGHT, RECORDS_PER_TRANSACTION);
        producer.beginTransaction();
        send(producer, getTopic(), inFlight);

        // control arm first: the records ARE on the broker, so the absence below is the isolation level at work
        awaitAllVisible(uncommitted, inFlight);

        assertNeverVisible(committed, IN_FLIGHT);

        producer.commitTransaction();

        List<String> firstBatch = awaitFirstBatchContaining(committed, IN_FLIGHT);
        assertWithMessage("a committed transaction's records must become visible together - this batch held "
                + "some of them but not all, which is the partial visibility the guarantee denies")
                .that(firstBatch)
                .containsExactlyElementsIn(inFlight);
    }

    /**
     * C6, in its sharper form: {@code read_committed} consumption is BLOCKED at the first still-open
     * transaction, not filtered around it.
     * <p>
     * The distinction is the whole claim. A filtering consumer would show the later, committed transaction and
     * hide only the open one; a blocked one shows neither. Only the second is safe, because it is what stops a
     * consumer from reading past an offset whose transaction may still abort.
     */
    @Test
    @ProvesClaim({TransactionalClaim.READ_COMMITTED_BLOCKED_TO_FIRST_OPEN_TX,
            TransactionalClaim.ALL_OR_NONE_PER_SOURCE_OFFSET})
    void readCommittedIsBlockedAtTheFirstStillOpenTransactionNotMerelyFiltered() {
        setupTopic(getClass().getSimpleName());
        Producer<String, String> first = transactionalProducer();
        Producer<String, String> second = transactionalProducer();

        String marker = commitMarkerRecord(first, getTopic(), WARMUP + "-0");

        Verifier committed = readCommittedVerifier("blocked-lso", getTopic());
        Verifier uncommitted = readUncommittedControl("blocked-lso", getTopic());
        requireLiveAndCaughtUp(committed, marker);
        requireLiveAndCaughtUp(uncommitted, marker);

        List<String> openValues = valuesFor(STILL_OPEN, 2);
        List<String> laterValues = valuesFor(LATER_COMMITTED, 2);

        // an open transaction, and then a LATER one that completes fully while the first is still open
        first.beginTransaction();
        send(first, getTopic(), openValues);

        second.beginTransaction();
        send(second, getTopic(), laterValues);
        second.commitTransaction();

        awaitAllVisible(uncommitted, openValues);
        awaitAllVisible(uncommitted, laterValues);

        assertNeverVisible(committed, STILL_OPEN);
        assertNoneSeen(committed, LATER_COMMITTED);

        // now unblock: both transactions' records arrive at once
        first.commitTransaction();

        List<String> everything = new ArrayList<>(openValues);
        everything.addAll(laterValues);
        awaitAllVisible(committed, everything);
    }

    /**
     * C8, abort arm: an aborted transaction's records are never visible at {@code read_committed}, before or
     * after the abort.
     * <p>
     * "After" needs its own non-vacuity argument, and a sentinel supplies it: a record committed after the
     * abort, which the verifier must consume. Seeing the sentinel proves the verifier read <em>past</em> the
     * aborted region - so "it never saw the aborted records" is a statement about records it had every
     * opportunity to see.
     */
    @Test
    @ProvesClaim(TransactionalClaim.ABORTED_NEVER_VISIBLE)
    void abortedTransactionRecordsAreNeverVisible() {
        setupTopic(getClass().getSimpleName());
        Producer<String, String> producer = transactionalProducer();

        String marker = commitMarkerRecord(producer, getTopic(), WARMUP + "-0");

        Verifier committed = readCommittedVerifier("aborted", getTopic());
        Verifier uncommitted = readUncommittedControl("aborted", getTopic());
        requireLiveAndCaughtUp(committed, marker);
        requireLiveAndCaughtUp(uncommitted, marker);

        List<String> doomed = valuesFor(ABORTED, RECORDS_PER_TRANSACTION);
        producer.beginTransaction();
        send(producer, getTopic(), doomed);

        awaitAllVisible(uncommitted, doomed);

        // before the abort
        assertNeverVisible(committed, ABORTED);

        producer.abortTransaction();

        // after the abort: read past the aborted region, then check what was skipped
        String sentinel = commitMarkerRecord(producer, getTopic(), SENTINEL + "-after-abort");
        awaitAllVisible(committed, UniLists.of(sentinel));

        assertNoneSeen(committed, ABORTED);
    }

    /**
     * C8, timeout arm: a transaction the broker reaps for exceeding {@code transaction.timeout.ms} leaves no
     * visible record either.
     * <p>
     * There is no "before the reap" absence assertion here on purpose. The reap lands somewhere between the
     * timeout and the timeout plus one broker cleanup tick, so any window asserted before it would be racing
     * the broker for no gain. Instead the verifier consumes continuously from before the transaction opened
     * until after the sentinel that follows it becomes visible, and the assertion is over everything it
     * consumed in that whole span - which is strictly stronger than a window.
     */
    @Test
    @ProvesClaim(TransactionalClaim.ABORTED_NEVER_VISIBLE)
    void transactionThatExceedsItsTimeoutLeavesNoVisibleRecord() {
        setupTopic(getClass().getSimpleName());
        Producer<String, String> sentinelProducer = transactionalProducer();
        Producer<String, String> shortTimeout = transactionalProducer(ofSeconds(2));

        String marker = commitMarkerRecord(sentinelProducer, getTopic(), WARMUP + "-0");

        Verifier committed = readCommittedVerifier("timed-out", getTopic());
        Verifier uncommitted = readUncommittedControl("timed-out", getTopic());
        requireLiveAndCaughtUp(committed, marker);
        requireLiveAndCaughtUp(uncommitted, marker);

        List<String> doomed = valuesFor(TIMED_OUT, RECORDS_PER_TRANSACTION);
        shortTimeout.beginTransaction();
        send(shortTimeout, getTopic(), doomed);
        // and then nothing - the transaction is simply left open until the broker reaps it

        awaitAllVisible(uncommitted, doomed);

        // committed AFTER the doomed transaction opened, so it sits behind the last stable offset until the
        // reap: seeing it is exactly the signal that the broker aborted the abandoned transaction
        String sentinel = commitMarkerRecord(sentinelProducer, getTopic(), SENTINEL + "-after-timeout");

        log.info("Awaiting the broker to reap the timed-out transaction (up to {})", TRANSACTION_REAP_TIMEOUT);
        await().atMost(TRANSACTION_REAP_TIMEOUT)
                .pollInterval(ofMillis(500))
                .untilAsserted(() -> {
                    committed.poll();
                    assertWithMessage("the sentinel committed after the doomed transaction never became visible, "
                            + "so the broker had not reaped the timed-out transaction within %s",
                            TRANSACTION_REAP_TIMEOUT)
                            .that(committed.consumed)
                            .contains(sentinel);
                });

        assertNoneSeen(committed, TIMED_OUT);
    }

    /**
     * Not a claim - a demonstration that {@link #requireLiveAndCaughtUp} is load-bearing, kept as a running
     * test rather than a comment because a guard nobody has watched fail is decoration.
     * <p>
     * Both halves are shown against records that are plainly, committedly visible to anyone who looks:
     * {@link #assertNoneSeen} passes against a consumer that never polled, and passes again against one that
     * holds a perfectly good assignment to the wrong topic - so a non-empty assignment on its own would not
     * have saved the assertion either. The guard is what rejects both.
     */
    @Test
    void theAbsenceAssertionIsVacuousWithoutTheNonVacuityGuard() {
        // two topics: the one the assertion is about, and one for a consumer to be validly assigned to instead.
        // setupTopic overwrites the inherited field, so the topic under test is created last and read back.
        String otherTopic = setupTopic(getClass().getSimpleName() + "-elsewhere");
        String topicUnderTest = setupTopic(getClass().getSimpleName() + "-under-test");

        Producer<String, String> producer = transactionalProducer();
        List<String> visible = valuesFor(COMMITTED_AND_VISIBLE, RECORDS_PER_TRANSACTION);
        producer.beginTransaction();
        send(producer, topicUnderTest, visible);
        producer.commitTransaction();

        // the records really are visible - a properly guarded verifier sees all of them
        Verifier guarded = readCommittedVerifier("guarded", topicUnderTest);
        awaitAllVisible(guarded, visible);

        // arm 1: never polled, so no assignment at all. The absence assertion is trivially satisfied.
        Verifier neverPolled = readCommittedVerifier("never-polled", topicUnderTest);
        assertThat(neverPolled.consumer.assignment()).isEmpty();
        assertNoneSeen(neverPolled, COMMITTED_AND_VISIBLE); // passes, about records that are plainly visible

        // arm 2: assigned - just not to the topic the assertion is about. Assignment alone is not the guard.
        Verifier wrongTopic = readCommittedVerifier("assigned-elsewhere", otherTopic);
        await().atMost(VISIBILITY_TIMEOUT)
                .pollInterval(ofMillis(200))
                .untilAsserted(() -> {
                    wrongTopic.poll();
                    assertThat(wrongTopic.consumer.assignment()).isNotEmpty();
                });
        assertNoneSeen(wrongTopic, COMMITTED_AND_VISIBLE); // passes too, and it has an assignment

        // and the guard rejects it, which is the only reason the assertions above are ever meaningful
        ConditionTimeoutException rejected = assertThrows(ConditionTimeoutException.class,
                () -> requireLiveAndCaughtUp(wrongTopic, visible.get(0), ofSeconds(5)),
                "the non-vacuity guard passed for a consumer that has consumed nothing from the topic under "
                        + "test - it is not guarding anything");
        assertWithMessage("the guard must fail on the clause that matters - having consumed a known committed "
                + "record - not merely on the assignment it does hold")
                .that(rejected.getMessage())
                .contains("has not yet consumed the committed marker");
    }
}
