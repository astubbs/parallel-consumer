package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static com.google.common.truth.Truth.assertThat;

/**
 * Tests that PC works fine with a consumer where the commitSync fails with TimeoutException after 5 seconds.
 *
 * After the first 20 seconds, commitSync will resume normal behavior: Succeed immediately
 *
 * In this test, we want to make sure the PC still resumes normal operation after several TimeoutException on commitSync timeout.
 * @author Shilin Wu
 * @see MockConsumerTestBase
 */
@Slf4j
class MockConsumerCommitTimeoutTest extends MockConsumerTestBase {

    private static final int RECORDS = 10;

    /** How long commitSync keeps timing out before it starts succeeding. */
    private static final Duration OUTAGE = Duration.ofSeconds(20);

    /** How long each failing commitSync blocks before throwing - i.e. the simulated broker timeout. */
    private static final Duration COMMIT_HANG = Duration.ofSeconds(5);

    @Override
    protected MockConsumer<String, String> createMockConsumer() {
        // captured, not a field: final-field semantics publish it safely to PC's threads, which are
        // started after this returns
        final long failUntil = System.currentTimeMillis() + OUTAGE.toMillis();
        return new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
                // polls stay normal throughout - only committing is affected
                if (System.currentTimeMillis() < failUntil) {
                    try {
                        Thread.sleep(COMMIT_HANG.toMillis());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                    throw new TimeoutException("Timeout after " + COMMIT_HANG.getSeconds() + " seconds (mocking)");
                }
                super.commitSync(offsets);
            }
        };
    }

    @Override
    protected void customiseOptions(ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> builder) {
        builder.offsetCommitTimeout(Duration.ofSeconds(25L)) // commit timeout set to 25 seconds
                .commitInterval(Duration.ofSeconds(1L)) // commit interval set to 1 second
                .commitMode(ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC); // use sync commit
    }

    /**
     * Test that the PC can resume operation after several failures
     */
    @Test
    void mockConsumer() {
        // the backlog keeps arriving during the outage, so commits are still being attempted while they fail
        addRecordsInBackground(RECORDS, Duration.ofSeconds(1));

        startProcessing();

        // Scope the timeout locally (don't mutate Awaitility's global default - that was leaking
        // across tests if the assertion below throws before reset()).
        Awaitility.await().atMost(Duration.ofSeconds(50)).untilAsserted(() ->
                assertThat(processedRecords).hasSize(RECORDS));
    }

}
