package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * {@link RebalanceInProgressException} on commit must not be fatal.
 * <p>
 * Kafka throws it when a commit lands while the group is mid-rebalance, and documents the remedy as
 * "complete the rebalance by calling poll() and then retry" - i.e. it is a transient protocol
 * condition, not a failure. {@link io.confluent.parallelconsumer.internal.ConsumerManager#commitSync}
 * classifies its sibling {@link org.apache.kafka.clients.consumer.CommitFailedException} exactly that
 * way, but this one used to escape the ladder entirely, with disproportionate consequences:
 * <ol>
 *     <li>it propagates out of {@code BrokerPollSystem.controlLoop()}, killing the broker-poll
 *         thread permanently;</li>
 *     <li>that thread is the only producer of commit responses, so the control thread's
 *         {@code commitAndWait()} blocks until {@code offsetCommitTimeout} and then throws
 *         "Timeout waiting for commit response" - a misleading symptom pointing nowhere near the
 *         cause;</li>
 *     <li>the control thread dies too, taking the whole PC instance down.</li>
 * </ol>
 * One retriable blip therefore killed the consumer. This surfaced as the Chaos Pain Suite's W4
 * revoke-under-work scenario going RED (both assignors), diagnosed in
 * {@code docs/plans/2026-08-01-001-investigate-chaos-w4-red-report.md}; cooperative rebalancing makes
 * it far likelier, because members keep committing <i>during</i> rebalances by design.
 * <p>
 * Mirrors {@link MockConsumerTestWithCommitTimeoutException} and
 * {@link MockConsumerTestWithSaslAuthenticationException}, which pin the other rungs of the ladder.
 */
@Slf4j
@Timeout(120)
class MockConsumerRebalanceInProgressTest {

    private static final int RECORDS = 10;

    /** Commits that throw before the mock consumer starts accepting them. */
    private static final int FAILING_COMMITS = 3;

    private final String topic = MockConsumerRebalanceInProgressTest.class.getSimpleName();

    /**
     * PC must survive a run of {@link RebalanceInProgressException}s on commit and still drain the
     * backlog. Before the fix this failed on the very first commit: the poll thread died, no further
     * records were processed, and PC ended with a failure cause.
     */
    @Test
    void surviveRebalanceInProgressOnCommit() {
        final AtomicInteger commitAttempts = new AtomicInteger();
        var mockConsumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
                if (commitAttempts.incrementAndGet() <= FAILING_COMMITS) {
                    log.info("Mock commit attempt {} - throwing RebalanceInProgressException", commitAttempts.get());
                    throw new RebalanceInProgressException("Offset commit cannot be completed since the "
                            + "consumer is undergoing a rebalance for auto partition assignment (mocking)");
                }
                super.commitSync(offsets);
            }
        };

        HashMap<TopicPartition, Long> startOffsets = new HashMap<>();
        TopicPartition tp = new TopicPartition(topic, 0);
        startOffsets.put(tp, 0L);

        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(mockConsumer)
                .commitInterval(Duration.ofMillis(200L)) // commit often, so the failing commits happen early
                .commitMode(ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC) // the mode that waits on a commit response
                .build();
        var parallelConsumer = new ParallelEoSStreamProcessor<String, String>(options);
        parallelConsumer.subscribe(of(topic));

        // MockConsumer is not a correct implementation of the Consumer contract - must manually rebalance
        mockConsumer.rebalance(Collections.singletonList(tp));
        parallelConsumer.onPartitionsAssigned(of(tp));
        mockConsumer.updateBeginningOffsets(startOffsets);

        for (int i = 0; i < RECORDS; i++) {
            mockConsumer.addRecord(new org.apache.kafka.clients.consumer.ConsumerRecord<>(topic, 0, i, "key", "value-" + i));
        }

        try {
            ConcurrentLinkedQueue<RecordContext<String, String>> processed = new ConcurrentLinkedQueue<>();
            parallelConsumer.poll(recordContexts -> recordContexts.forEach(processed::add));

            // The discriminating assertion: commits must keep being ATTEMPTED past the failing run.
            // Only the broker-poll thread issues them, so getting beyond FAILING_COMMITS proves it
            // survived the exception. Unfixed, it dies on attempt 1 and this count never moves again.
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                    assertThat(commitAttempts.get()).isGreaterThan(FAILING_COMMITS));

            // the backlog must also drain despite the failing commits
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                    assertThat(processed).hasSize(RECORDS));

            // and the instance must still be healthy: this is the exact property the chaos suite
            // asserts (ChaosScenarioBase - no instance may end the run with an unclassified cause)
            assertThat(parallelConsumer.getFailureCause()).isNull();
            assertThat(parallelConsumer.isClosedOrFailed()).isFalse();
        } finally {
            parallelConsumer.closeDontDrainFirst();
        }
    }
}
