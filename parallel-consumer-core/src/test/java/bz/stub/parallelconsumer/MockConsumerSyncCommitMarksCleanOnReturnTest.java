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
 * The control arm for {@link AsyncCommitAcknowledgementTestBase}: the <em>synchronous</em> mode still records a
 * commit as successful the moment {@code commitSync} returns.
 * <p>
 * That is correct there and must stay that way - {@code commitSync} returns only once the broker has answered,
 * so the return <b>is</b> the acknowledgement, and there is no later event to wait for. The change this test
 * guards is deliberately one-sided: {@code AbstractOffsetCommitter#commitOffsetsReturnsOnlyOnceAcknowledged()}
 * is false for {@code PERIODIC_CONSUMER_ASYNCHRONOUS} alone, and if it were ever widened to cover this mode as
 * well, nothing would call {@code onOffsetCommitSuccess} here at all.
 * <p>
 * <b>What makes that failure visible</b> is the commit-attempt count after success, not the committed offset:
 * a sync mode that never marked clean would still reach offset {@value #RECORDS} on its first commit, so the
 * broker-side assertion alone passes either way. It is the partition going <em>clean</em> that stops further
 * commits, so the discriminator is that the count stands still once every record is committed. Under the
 * defect it would climb on every {@code commitInterval} forever.
 * <p>
 * No commit is rejected or withheld here; the only variable against the async scenarios is the mode. The
 * shared wiring - topic, assignment, PC lifecycle, teardown - is in {@link MockConsumerTestBase}, and the
 * sibling family for a sync commit that is <em>rejected</em> is {@link CommitRejectionTestBase}.
 */
@Slf4j
class MockConsumerSyncCommitMarksCleanOnReturnTest extends MockConsumerTestBase {

    private static final int RECORDS = 10;

    /** How long the attempt count must stand still after success to count as stopped: 5 commit intervals. */
    private static final Duration QUIET_PERIOD = Duration.ofSeconds(1);

    /**
     * How long each stage may take. Stated here rather than inside the shared helpers in
     * {@link MockConsumerTestBase}, because a deadline has to clear whatever its own scenario simulates - here
     * nothing is rejected or withheld at all, so this is plain CI headroom, kept equal to the async scenarios'
     * so that the one variable between them stays the commit mode.
     */
    private static final Duration SCENARIO_TIMEOUT = Duration.ofSeconds(30);

    private final AtomicInteger commitAttempts = new AtomicInteger();

    @Override
    protected MockConsumer<String, String> createMockConsumer() {
        return new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
                log.info("Mock sync commit attempt {} - accepting {}", commitAttempts.incrementAndGet(), offsets);
                super.commitSync(offsets);
            }
        };
    }

    @Override
    protected void customiseOptions(ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> builder) {
        // the same cadence the async scenarios use, so "the count stood still" is measured over the same
        // number of commit opportunities
        builder.commitInterval(Duration.ofMillis(200L))
                .commitMode(ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC);
    }

    @Test
    void acknowledgedSyncCommitMarksTheOffsetsCleanImmediately() {
        addRecords(RECORDS);

        startProcessing();

        awaitAllRecordsProcessed(RECORDS, SCENARIO_TIMEOUT);

        // the offsets reach the broker, as they do in the async scenarios
        awaitBrokerCommittedOffset(RECORDS, SCENARIO_TIMEOUT);

        // The discriminating assertion, and the reason it is written out here rather than shared with the async
        // families: they assert this counter CLIMBING past a floor, and this one asserts it STANDING STILL once
        // every record is committed, which is what says the success marking happened on return. Under the defect
        // it would climb on every commitInterval forever.
        int afterSuccess = commitAttempts.get();
        Awaitility.await()
                .pollDelay(QUIET_PERIOD)
                .atMost(QUIET_PERIOD.plusSeconds(10))
                .untilAsserted(() -> assertThat(commitAttempts.get()).isEqualTo(afterSuccess));

        assertParallelConsumerStillRunningWithNoFailureCause();
    }
}
