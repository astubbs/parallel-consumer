package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;

/**
 * Tests that PC can be closed ahead of time. Make sure PC can shut down cleanly.
 *
 * In this test, the MockConsumer will start throwing SaslAuthenticationException from 2 seconds onwards, until infinity.
 *
 * The offsetCommitTimeout as well as the saslAuthenticationRetryTimeout had been set to infinity as well.
 *
 * Once PC is observably retrying the failing broker, it is requested to close. The expected behavior is that the PC can
 * be shutdown cleanly - i.e. {@link ParallelEoSStreamProcessor#close()} returns rather than blocking on a retry budget
 * that will never be exhausted.
 *
 * @see MockConsumerTestBase
 */
@Slf4j
class MockConsumerEarlyCloseTest extends MockConsumerTestBase {

    /** How long the consumer behaves before it starts failing, and keeps failing. */
    private static final Duration HEALTHY_PERIOD = Duration.ofSeconds(2);

    /**
     * Enough records that the feed outlives the test - close must succeed with work still arriving.
     */
    private static final int RECORDS = 100_000;

    /**
     * Counts the auth failures served on the POLL path specifically. Poll-only on purpose: the commit path fails too,
     * from a different thread, so a combined counter reaching two proves only that two calls failed somewhere -
     * possibly one of each, concurrently, with no retry in between. It is the second POLL failure that can only follow
     * a completed poll retry back-off.
     */
    private final AtomicInteger pollAuthFailures = new AtomicInteger();

    @Override
    protected MockConsumer<String, String> createMockConsumer() {
        // captured, not a field: final-field semantics publish it safely to PC's threads, which are
        // started after this returns
        final long startFailing = System.currentTimeMillis() + HEALTHY_PERIOD.toMillis();
        return new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized ConsumerRecords<String, String> poll(Duration timeout) {
                if (outageStarted()) {
                    pollAuthFailures.incrementAndGet();
                    throw authFailure();
                }
                return super.poll(timeout);
            }

            @Override
            public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
                if (outageStarted()) {
                    throw authFailure();
                }
                super.commitSync(offsets);
            }

            /** Never recovers - the point is that close does not wait for a recovery that will not come. */
            private boolean outageStarted() {
                return System.currentTimeMillis() > startFailing;
            }

            private SaslAuthenticationException authFailure() {
                return new SaslAuthenticationException("Invalid username or password");
            }
        };
    }

    @Override
    protected void customiseOptions(ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> builder) {
        // effectively infinite: close must not depend on either budget running out
        builder.offsetCommitTimeout(Duration.ofSeconds(10000000L))
                .saslAuthenticationRetryTimeout(Duration.ofSeconds(250000000L));
    }

    /**
     * Test that the mock consumer works as expected
     */
    @Test
    void mockConsumer() {
        addRecordsInBackground(RECORDS, Duration.ofSeconds(1));

        startProcessing();

        // Close from the state the test is about: the broker is failing every call and PC is retrying.
        // Waiting for failures to actually have been served beats sleeping for a duration in which we
        // hope some were. Two POLL failures, not one, and not two failures of any kind: only a second
        // failure on the same path can be behind a completed retry back-off, so by then the poll loop is
        // demonstrably retrying - and close lands mid-back-off, which is where the old 5s sleep used to
        // (accidentally) put it.
        Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> pollAuthFailures.get() >= 2);

        log.info("Trying to close...");
        parallelConsumer.close(); // request close while the consumer is still failing
        log.info("Close successful!");

        // "cleanly": actually closed, and not merely because the SASL storm killed it - a PC that had died
        // would also report closed-or-failed, so the cause has to be checked too
        assertThat(parallelConsumer.isClosedOrFailed()).isTrue();
        assertThat(parallelConsumer.getFailureCause()).isNull();
    }

}
