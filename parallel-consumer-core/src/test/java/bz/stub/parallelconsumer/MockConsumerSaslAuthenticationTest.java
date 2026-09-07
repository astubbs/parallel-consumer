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

import static com.google.common.truth.Truth.assertThat;

/**
 * Test that PC can survive a temporary SaslAuthenticationException.
 *
 * In this test, MockConsumer throws SaslAuthenticationException from the beginning for 8 seconds, then
 * goes back to normal.
 *
 * The saslAuthenticationRetryTimeout is set to 30 seconds (generous margin over the 8s outage window) so
 * PC has room to recover. The whole test fits comfortably inside PIT's per-test baseline coverage budget
 * (which caps around ~80s). The earlier 20s outage + 25s retry version intermittently failed PIT's baseline
 * because the total runtime scraped that cap. Test still verifies the same property: PC recovers if the
 * retry budget exceeds the outage window.
 * @author Shilin Wu
 * @see MockConsumerTestBase
 */
@Slf4j
class MockConsumerSaslAuthenticationTest extends MockConsumerTestBase {

    private static final int RECORDS = 3;

    /** How long every call fails before the consumer goes back to normal. */
    private static final Duration OUTAGE = Duration.ofSeconds(8);

    @Override
    protected MockConsumer<String, String> createMockConsumer() {
        // captured, not a field: final-field semantics publish it safely to PC's threads, which are
        // started after this returns
        final long failUntil = System.currentTimeMillis() + OUTAGE.toMillis();
        return new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized ConsumerRecords<String, String> poll(Duration timeout) {
                if (System.currentTimeMillis() < failUntil) {
                    log.info("Mocking failure until the {} outage window closes", OUTAGE);
                    throw new SaslAuthenticationException("Invalid username or password");
                }
                return super.poll(timeout);
            }

            @Override
            public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
                if (System.currentTimeMillis() < failUntil) {
                    throw new SaslAuthenticationException("Invalid username or password");
                }
                super.commitSync(offsets);
            }
        };
    }

    @Override
    protected void customiseOptions(ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> builder) {
        // 30s retry budget over an 8s mock-failure window - generous margin (22s) for
        // PC's recovery poll even under PIT's slower JVM.
        builder.saslAuthenticationRetryTimeout(Duration.ofSeconds(30L));
    }

    /**
     * PC survives an auth outage shorter than its retry budget: once the broker recovers, the backlog that
     * built up during the outage drains.
     */
    @Test
    void backlogStillDrainsAfterATransientSaslOutage() {
        addRecords(RECORDS);

        startProcessing();

        // Scope the timeout locally (don't mutate Awaitility's global default - that was leaking
        // across tests under PIT's different ordering).
        // 45s: 8s mock-failure window + retry + PIT's JVM slowdown, with headroom.
        Awaitility.await().atMost(Duration.ofSeconds(45)).untilAsserted(() ->
                assertThat(processedRecords).hasSize(RECORDS));
    }

}
