package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;

/**
 * Shared scenario for the commit-rejection exceptions: Kafka refuses a commit for a reason that
 * means "not now" rather than "this consumer is broken", and PC must neither die nor record the
 * commit as done.
 * <p>
 * Subclasses supply only the exception. Everything else - the mock consumer, the backlog, and the
 * assertions - is identical, because these are one scenario with one variable. Keeping them apart
 * would mean two copies drifting out of step, and a future third rejection reason copying whichever
 * one it found first. The wiring below the scenario (topic, assignment, PC lifecycle, teardown) is
 * shared further still, with the other vanilla-{@link MockConsumer} tests, in
 * {@link MockConsumerTestBase}.
 * <p>
 * The discriminating assertion is that commits keep being <b>attempted</b> after the rejections. If
 * the offsets had been marked clean, {@code collectCommitDataForDirtyPartitions()} would return
 * empty and the mock's {@code commitSync} would never be called again - the counter stalls. A naive
 * "no exception escaped" test passes even with the old swallow-and-return bug, so it would guard
 * nothing.
 */
@Slf4j
abstract class CommitRejectionTestBase extends MockConsumerTestBase {

    private static final int RECORDS = 10;

    /** Commits rejected before the mock consumer starts accepting them. */
    private static final int REJECTED_COMMITS = 3;

    private final AtomicInteger commitAttempts = new AtomicInteger();

    /** The rejection under test. A fresh instance per call - these carry stack traces. */
    protected abstract RuntimeException rejection();

    @Override
    protected MockConsumer<String, String> createMockConsumer() {
        return new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
                if (commitAttempts.incrementAndGet() <= REJECTED_COMMITS) {
                    var rejection = rejection();
                    log.info("Mock commit attempt {} - rejecting with {}",
                            commitAttempts.get(), rejection.getClass().getSimpleName());
                    throw rejection;
                }
                super.commitSync(offsets);
            }
        };
    }

    @Override
    protected void customiseOptions(ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> builder) {
        builder.commitInterval(Duration.ofMillis(200L)) // commit often, so the rejections happen early
                .commitMode(ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC); // the mode that waits on a commit response
    }

    @Test
    void rejectedCommitIsNotFatalAndIsNotRecordedAsSuccessful() {
        addRecords(RECORDS);

        startProcessing();

        // the offsets must still be dirty after a rejection, so commits keep being attempted
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(commitAttempts.get()).isGreaterThan(REJECTED_COMMITS));

        // and the rejection must not be fatal - the backlog still drains
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(processedRecords).hasSize(RECORDS));

        // deferral means the offsets are re-committed later, not lost - so ask the broker side
        // rather than inferring it from the attempt count
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            var committed = mockConsumer.committed(Collections.singleton(topicPartition)).get(topicPartition);
            assertThat(committed).isNotNull();
            assertThat(committed.offset()).isEqualTo(RECORDS);
        });

        // the exact property the chaos suite asserts: no instance ends with an unclassified cause
        assertThat(parallelConsumer.getFailureCause()).isNull();
        assertThat(parallelConsumer.isClosedOrFailed()).isFalse();
    }

    /**
     * Nothing is left to drain by the time this test ends, and a drain would only mean another commit cycle
     * against a mock that has been rejecting them.
     */
    @Override
    protected void closeParallelConsumer() {
        parallelConsumer.closeDontDrainFirst();
    }
}
