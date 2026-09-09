package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;

/**
 * Shared scenario for the asynchronous commit mode: an offset is only committed when the broker
 * <em>acknowledges</em> it, not when the request is sent.
 * <p>
 * {@link ParallelConsumerOptions.CommitMode#PERIODIC_CONSUMER_ASYNCHRONOUS} is the shipped default,
 * and {@code Consumer#commitAsync} answers through an {@link OffsetCommitCallback} some time after
 * it returns. Returning is therefore not an acknowledgement: it says the request was handed to the
 * client, and nothing more. Two things can follow a return that never becomes a durable commit -
 * the callback arriving with an exception, and the callback not arriving at all - and each is one
 * subclass here, because they are one scenario with one variable. The sibling family for the
 * <em>synchronous</em> mode, where the rejection arrives as a thrown exception instead, is
 * {@link CommitRejectionTestBase}; the wiring below both (topic, assignment, PC lifecycle,
 * teardown) is shared further still in {@link MockConsumerTestBase}.
 * <p>
 * <b>The discriminating assertion is the broker's own committed offset</b>, and the mechanism is
 * worth stating because a weaker test here would guard nothing. All {@value #RECORDS} records are
 * published before processing starts, so they are complete long before the first commit falls due
 * at {@code commitInterval}: that first commit carries the whole batch. If the send is treated as
 * the commit, the partition is marked clean at that point, {@code collectCommitDataForDirtyPartitions()}
 * returns empty from then on, and no further {@code commitAsync} is ever issued - so the mock
 * consumer's {@code committed()} stays empty forever even though PC believes offset
 * {@value #RECORDS} is durable. That is the silent-loss shape: whoever owns the partition next
 * resumes from the broker's position, which is nowhere.
 * <p>
 * {@link #commitAttempts} is asserted as well, and is discriminating for the same reason rather
 * than as a restatement: on the send-is-success reading it stops at exactly one.
 */
@Slf4j
abstract class AsyncCommitAcknowledgementTestBase extends MockConsumerTestBase {

    private static final int RECORDS = 10;

    /** Commit requests whose acknowledgement is withheld before the mock consumer starts answering. */
    protected static final int UNACKNOWLEDGED_COMMITS = 3;

    /**
     * How long each stage of the scenario may take. Stated here rather than inside the shared helpers in
     * {@link MockConsumerTestBase}, because a deadline has to clear the outage its own scenario simulates -
     * here, {@value #UNACKNOWLEDGED_COMMITS} withheld acknowledgements at a 200ms commit interval, with ample
     * headroom for CI.
     */
    private static final Duration SCENARIO_TIMEOUT = Duration.ofSeconds(30);

    private final AtomicInteger commitAttempts = new AtomicInteger();

    /**
     * What the mock does with a commit request whose acknowledgement is being withheld. The offsets
     * are deliberately <em>not</em> recorded as committed by any implementation - that is the point
     * of the scenario.
     *
     * @param offsets  the offsets the request carried
     * @param callback PC's callback, which the implementation may fail or drop
     */
    protected abstract void withholdAcknowledgement(Map<TopicPartition, OffsetAndMetadata> offsets,
                                                    OffsetCommitCallback callback);

    @Override
    protected MockConsumer<String, String> createMockConsumer() {
        return new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets,
                                                 OffsetCommitCallback callback) {
                if (commitAttempts.incrementAndGet() <= UNACKNOWLEDGED_COMMITS) {
                    log.info("Mock async commit attempt {} - withholding acknowledgement of {}",
                            commitAttempts.get(), offsets);
                    withholdAcknowledgement(offsets, callback);
                    return;
                }
                log.info("Mock async commit attempt {} - acknowledging {}", commitAttempts.get(), offsets);
                super.commitAsync(offsets, callback);
            }
        };
    }

    @Override
    protected void customiseOptions(ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> builder) {
        // commit often, so the withheld acknowledgements happen early; and name the mode under test
        // explicitly rather than relying on it remaining the default
        builder.commitInterval(Duration.ofMillis(200L))
                .commitMode(ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS);
    }

    @Test
    void unacknowledgedAsyncCommitIsNotRecordedAsSuccessful() {
        addRecords(RECORDS);

        startProcessing();

        // the backlog drains regardless - an unacknowledged commit must not be fatal
        awaitAllRecordsProcessed(RECORDS, SCENARIO_TIMEOUT);

        // A discriminating assertion, and the reason it is written out here rather than shared: the offsets
        // must still be dirty while the acknowledgements are withheld, so commits keep being ATTEMPTED. On the
        // send-is-success reading this counter stops at exactly one.
        Awaitility.await().atMost(SCENARIO_TIMEOUT).untilAsserted(() ->
                assertThat(commitAttempts.get()).isGreaterThan(UNACKNOWLEDGED_COMMITS));

        // and the offsets reach the broker in the end - ask the broker side rather than inferring it
        // from the attempt count
        awaitBrokerCommittedOffset(RECORDS, SCENARIO_TIMEOUT);

        assertParallelConsumerStillRunningWithNoFailureCause();
    }
}
