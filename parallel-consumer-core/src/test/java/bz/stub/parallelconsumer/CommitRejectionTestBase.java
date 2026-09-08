package bz.stub.parallelconsumer;

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

    /**
     * How long each stage of the scenario may take. Stated here rather than inside the shared helpers in
     * {@link MockConsumerTestBase}, because a deadline has to clear the outage its own scenario simulates -
     * here, {@value #REJECTED_COMMITS} rejections at a 200ms commit interval, with ample headroom for CI.
     */
    private static final Duration SCENARIO_TIMEOUT = Duration.ofSeconds(30);

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

        // The discriminating assertion, and the reason it is written out here rather than shared: the offsets
        // must still be dirty after a rejection, so commits keep being ATTEMPTED. Were they marked clean,
        // collectCommitDataForDirtyPartitions() would return empty and this counter would stall.
        Awaitility.await().atMost(SCENARIO_TIMEOUT).untilAsserted(() ->
                assertThat(commitAttempts.get()).isGreaterThan(REJECTED_COMMITS));

        // and the rejection must not be fatal - the backlog still drains
        awaitAllRecordsProcessed(RECORDS, SCENARIO_TIMEOUT);

        // deferral means the offsets are re-committed later, not lost - so ask the broker side
        // rather than inferring it from the attempt count
        awaitBrokerCommittedOffset(RECORDS, SCENARIO_TIMEOUT);

        assertParallelConsumerStillRunningWithNoFailureCause();
    }
}
