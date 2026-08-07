package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.ProvesClaim;
import io.confluent.parallelconsumer.TransactionalClaim;
import io.confluent.parallelconsumer.integrationTests.utils.TransactionalTopicVerifier;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static io.confluent.parallelconsumer.integrationTests.utils.TransactionalTopicVerifier.VISIBILITY_TIMEOUT;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.Optional.empty;
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
 * The two traps this class is built around - vacuity and marker inflation - and the guards against them live in
 * {@link TransactionalTopicVerifier}, which every visibility test shares.
 * {@link #theAbsenceAssertionIsVacuousWithoutTheNonVacuityGuard()} exists here to demonstrate - as a running
 * test, not a comment - that the non-vacuity guard is load-bearing.
 * <p>
 * Each absence assertion is also paired with a {@code read_uncommitted} control arm that <em>does</em> see the
 * records, so "absent" is demonstrably "hidden by the isolation level" rather than "never written".
 *
 * @author Antony Stubbs
 * @see TransactionalClaim
 * @see TransactionalCrashReplayIT
 * @see TransactionTimeoutsTest
 */
@Tag("transactions")
@Slf4j
// Per-method timeout, following DrainingMemberRebalanceIT and the rest of this package. Without one, a thread
// blocked on a broker call or a lock becomes a job that runs to the CI-level timeout with no failing test to
// point at. Comfortably above this class's own budget: the worst arm is the transaction-timeout one, which pays a
// 60s read_uncommitted wait, then the 120s TRANSACTION_REAP_TIMEOUT, then a 60s wait for the post-reap sentinel,
// on top of broker and topic setup.
@Timeout(600)
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
     * The wait for the broker to reap a transaction that blew its {@code transaction.timeout.ms}. Generous
     * because the reap is the timeout plus up to one broker cleanup tick
     * ({@code transaction.abort.timed.out.transaction.cleanup.interval.ms}, 10s by default), and a shared
     * broker's tick is nobody's to schedule.
     */
    private static final Duration TRANSACTION_REAP_TIMEOUT = ofSeconds(120);

    private TransactionalTopicVerifier readCommittedVerifier(String name, String topicToWatch) {
        return register(TransactionalTopicVerifier.readCommitted(getKcu(), name, topicToWatch));
    }

    private TransactionalTopicVerifier readUncommittedControl(String name, String topicToWatch) {
        return register(TransactionalTopicVerifier.readUncommitted(getKcu(), name, topicToWatch));
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

        TransactionalTopicVerifier committed = readCommittedVerifier("open-tx", getTopic());
        TransactionalTopicVerifier uncommitted = readUncommittedControl("open-tx", getTopic());
        committed.requireLiveAndCaughtUp(marker);
        uncommitted.requireLiveAndCaughtUp(marker);

        List<String> inFlight = valuesFor(IN_FLIGHT, RECORDS_PER_TRANSACTION);
        producer.beginTransaction();
        send(producer, getTopic(), inFlight);

        // control arm first: the records ARE on the broker, so the absence below is the isolation level at work
        uncommitted.awaitAllVisible(inFlight);

        committed.assertNeverVisible(IN_FLIGHT);

        producer.commitTransaction();

        List<String> firstBatch = committed.awaitFirstBatchContaining(IN_FLIGHT);
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

        TransactionalTopicVerifier committed = readCommittedVerifier("blocked-lso", getTopic());
        TransactionalTopicVerifier uncommitted = readUncommittedControl("blocked-lso", getTopic());
        committed.requireLiveAndCaughtUp(marker);
        uncommitted.requireLiveAndCaughtUp(marker);

        List<String> openValues = valuesFor(STILL_OPEN, 2);
        List<String> laterValues = valuesFor(LATER_COMMITTED, 2);

        // an open transaction, and then a LATER one that completes fully while the first is still open
        first.beginTransaction();
        send(first, getTopic(), openValues);

        second.beginTransaction();
        send(second, getTopic(), laterValues);
        second.commitTransaction();

        uncommitted.awaitAllVisible(openValues);
        uncommitted.awaitAllVisible(laterValues);

        committed.assertNeverVisible(STILL_OPEN);
        committed.assertNoneSeen(LATER_COMMITTED);

        // now unblock: both transactions' records arrive at once
        first.commitTransaction();

        List<String> everything = new ArrayList<>(openValues);
        everything.addAll(laterValues);
        committed.awaitAllVisible(everything);
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

        TransactionalTopicVerifier committed = readCommittedVerifier("aborted", getTopic());
        TransactionalTopicVerifier uncommitted = readUncommittedControl("aborted", getTopic());
        committed.requireLiveAndCaughtUp(marker);
        uncommitted.requireLiveAndCaughtUp(marker);

        List<String> doomed = valuesFor(ABORTED, RECORDS_PER_TRANSACTION);
        producer.beginTransaction();
        send(producer, getTopic(), doomed);

        uncommitted.awaitAllVisible(doomed);

        // before the abort
        committed.assertNeverVisible(ABORTED);

        producer.abortTransaction();

        // after the abort: read past the aborted region, then check what was skipped
        String sentinel = commitMarkerRecord(producer, getTopic(), SENTINEL + "-after-abort");
        committed.awaitAllVisible(UniLists.of(sentinel));

        committed.assertNoneSeen(ABORTED);
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

        TransactionalTopicVerifier committed = readCommittedVerifier("timed-out", getTopic());
        TransactionalTopicVerifier uncommitted = readUncommittedControl("timed-out", getTopic());
        committed.requireLiveAndCaughtUp(marker);
        uncommitted.requireLiveAndCaughtUp(marker);

        List<String> doomed = valuesFor(TIMED_OUT, RECORDS_PER_TRANSACTION);
        shortTimeout.beginTransaction();
        send(shortTimeout, getTopic(), doomed);
        // and then nothing - the transaction is simply left open until the broker reaps it

        uncommitted.awaitAllVisible(doomed);

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
                            .that(committed.consumed())
                            .contains(sentinel);
                });

        committed.assertNoneSeen(TIMED_OUT);
    }

    /**
     * Not a claim - a demonstration that {@link TransactionalTopicVerifier#requireLiveAndCaughtUp} is
     * load-bearing, kept as a running test rather than a comment because a guard nobody has watched fail is
     * decoration.
     * <p>
     * Both halves are shown against records that are plainly, committedly visible to anyone who looks:
     * {@link TransactionalTopicVerifier#assertNoneSeen} passes against a consumer that never polled, and passes
     * again against one that holds a perfectly good assignment to the wrong topic - so a non-empty assignment on
     * its own would not have saved the assertion either. The guard is what rejects both.
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
        TransactionalTopicVerifier guarded = readCommittedVerifier("guarded", topicUnderTest);
        guarded.awaitAllVisible(visible);

        // arm 1: never polled, so no assignment at all. The absence assertion is trivially satisfied.
        TransactionalTopicVerifier neverPolled = readCommittedVerifier("never-polled", topicUnderTest);
        assertThat(neverPolled.consumer().assignment()).isEmpty();
        neverPolled.assertNoneSeen(COMMITTED_AND_VISIBLE); // passes, about records that are plainly visible

        // arm 2: assigned - just not to the topic the assertion is about. Assignment alone is not the guard.
        TransactionalTopicVerifier wrongTopic = readCommittedVerifier("assigned-elsewhere", otherTopic);
        await().atMost(VISIBILITY_TIMEOUT)
                .pollInterval(ofMillis(200))
                .untilAsserted(() -> {
                    wrongTopic.poll();
                    assertThat(wrongTopic.consumer().assignment()).isNotEmpty();
                });
        wrongTopic.assertNoneSeen(COMMITTED_AND_VISIBLE); // passes too, and it has an assignment

        // and the guard rejects it, which is the only reason the assertions above are ever meaningful
        ConditionTimeoutException rejected = assertThrows(ConditionTimeoutException.class,
                () -> wrongTopic.requireLiveAndCaughtUp(visible.get(0), ofSeconds(5)),
                "the non-vacuity guard passed for a consumer that has consumed nothing from the topic under "
                        + "test - it is not guarding anything");
        assertWithMessage("the guard must fail on the clause that matters - having consumed a known committed "
                + "record - not merely on the assignment it does hold")
                .that(rejected.getMessage())
                .contains("has not yet consumed the committed marker");
    }
}
