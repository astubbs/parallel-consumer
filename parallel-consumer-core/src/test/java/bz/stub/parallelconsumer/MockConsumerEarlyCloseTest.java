package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static com.google.common.truth.Truth.assertThat;

/**
 * Tests that PC can be closed ahead of time. Make sure PC can shut down cleanly.
 *
 * In this test, the MockConsumer will start throwing SaslAuthenticationException from 2 seconds onwards, until infinity.
 *
 * The offsetCommitTimeout as well as the saslAuthenticationRetryTimeout had been set to infinity as well.
 *
 * After 5 seconds PC will be requested to close. The expected behavior is that the PC can be shutdown cleanly -
 * i.e. {@link ParallelEoSStreamProcessor#close()} returns rather than blocking on a retry budget that will
 * never be exhausted.
 *
 * @see MockConsumerTestBase
 */
@Slf4j
class MockConsumerEarlyCloseTest extends MockConsumerTestBase {

    /** How long the consumer behaves before it starts failing, and keeps failing. */
    private static final Duration HEALTHY_PERIOD = Duration.ofSeconds(2);

    /** How long PC is left running in the outage before close is requested. */
    private static final Duration RUN_BEFORE_CLOSE = Duration.ofSeconds(5);

    /**
     * Enough records that the feed outlives the test - close must succeed with work still arriving.
     */
    private static final int RECORDS = 100_000;

    @Override
    protected MockConsumer<String, String> createMockConsumer() {
        // captured, not a field: final-field semantics publish it safely to PC's threads, which are
        // started after this returns
        final long startFailing = System.currentTimeMillis() + HEALTHY_PERIOD.toMillis();
        return new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized ConsumerRecords<String, String> poll(Duration timeout) {
                failOnceOutageStarts();
                return super.poll(timeout);
            }

            @Override
            public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
                failOnceOutageStarts();
                super.commitSync(offsets);
            }

            /** Never recovers - the point is that close does not wait for a recovery that will not come. */
            private void failOnceOutageStarts() {
                if (System.currentTimeMillis() > startFailing) {
                    throw new SaslAuthenticationException("Invalid username or password");
                }
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
    @SneakyThrows
    void mockConsumer() {
        addRecordsInBackground(RECORDS, Duration.ofSeconds(1));

        startProcessing();

        Thread.sleep(RUN_BEFORE_CLOSE.toMillis());

        log.info("Trying to close...");
        parallelConsumer.close(); // request close while the consumer is still failing
        log.info("Close successful!");

        // "cleanly": actually closed, and not merely because the SASL storm killed it - a PC that had died
        // would also report closed-or-failed, so the cause has to be checked too
        assertThat(parallelConsumer.isClosedOrFailed()).isTrue();
        assertThat(parallelConsumer.getFailureCause()).isNull();
    }

}
