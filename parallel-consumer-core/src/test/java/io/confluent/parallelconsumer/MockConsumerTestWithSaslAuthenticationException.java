package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 */

import io.confluent.csid.utils.LongPollingMockConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.truth.Truth.assertThat;
import static pl.tlinkowski.unij.api.UniLists.of;

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
 */
@Slf4j
@Timeout(60000L)
class MockConsumerTestWithSaslAuthenticationException {

    private final String topic = MockConsumerTestWithSaslAuthenticationException.class.getSimpleName();

    // Field so @AfterEach can close it. This class doesn't extend
    // AbstractParallelEoSStreamProcessorTestBase, so no base-class cleanup runs.
    private ParallelEoSStreamProcessor<String, String> parallelConsumer;

    @AfterEach
    void close() {
        if (parallelConsumer != null && !parallelConsumer.isClosedOrFailed()) {
            parallelConsumer.close();
        }
    }

    /**
     * Test that the mock consumer works as expected
     */
    @Test
    void mockConsumer() {
        // 8s mock-failure window (was 20s) — keeps total test runtime well within PIT's baseline
        // per-test budget while still triggering PC's SASL retry path meaningfully.
        final AtomicLong failUntil = new AtomicLong(System.currentTimeMillis() + 8000L);
        var mockConsumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized ConsumerRecords<String, String> poll(Duration timeout) {
                if(System.currentTimeMillis() < failUntil.get()) {
                    log.info("Mocking failure before 20 seconds");
                    throw new SaslAuthenticationException("Invalid username or password");
                }
                return super.poll(timeout);
            }

            @Override
            public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
                if(System.currentTimeMillis() < failUntil.get()) {
                    throw new SaslAuthenticationException("Invalid username or password");
                }
                super.commitSync(offsets);
            }
        };
        HashMap<TopicPartition, Long> startOffsets = new HashMap<>();
        TopicPartition tp = new TopicPartition(topic, 0);
        startOffsets.put(tp, 0L);

        //
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(mockConsumer)
                // 30s retry budget over an 8s mock-failure window — generous margin (22s) for
                // PC's recovery poll even under PIT's slower JVM.
                .saslAuthenticationRetryTimeout(Duration.ofSeconds(30L))
                .build();
        parallelConsumer = new ParallelEoSStreamProcessor<>(options);
        parallelConsumer.subscribe(of(topic));

        // MockConsumer is not a correct implementation of the Consumer contract - must manually rebalance++ - or use LongPollingMockConsumer
        mockConsumer.rebalance(Collections.singletonList(tp));
        parallelConsumer.onPartitionsAssigned(of(tp));
        mockConsumer.updateBeginningOffsets(startOffsets);

        //
        addRecords(mockConsumer);

        //
        ConcurrentLinkedQueue<RecordContext<String, String>> records = new ConcurrentLinkedQueue<>();
        parallelConsumer.poll(recordContexts -> {
            recordContexts.forEach(recordContext -> {
                log.warn("Processing: {}", recordContext);
                records.add(recordContext);
            });
        });

        // Scope the timeout locally (don't mutate Awaitility's global default — that was leaking
        // across tests under PIT's different ordering, since this class doesn't have base-class
        // Awaitility.reset() cleanup).
        // 45s: 8s mock-failure window + retry + PIT's JVM slowdown, with headroom.
        Awaitility.await().atMost(Duration.ofSeconds(45)).untilAsserted(() -> {
            assertThat(records).hasSize(3);
        });
    }

    private void addRecords(MockConsumer<String, String> mockConsumer) {
        mockConsumer.addRecord(new org.apache.kafka.clients.consumer.ConsumerRecord<>(topic, 0, 0, "key", "value"));
        mockConsumer.addRecord(new org.apache.kafka.clients.consumer.ConsumerRecord<>(topic, 0, 1, "key", "value"));
        mockConsumer.addRecord(new org.apache.kafka.clients.consumer.ConsumerRecord<>(topic, 0, 2, "key", "value"));
    }

}
