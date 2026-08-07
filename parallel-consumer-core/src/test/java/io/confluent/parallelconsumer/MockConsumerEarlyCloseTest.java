package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.csid.utils.LongPollingMockConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.truth.Truth.assertThat;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Tests that PC can be closed ahead of time. Make sure PC can shut down cleanly.
 *
 * In this test, the MockConsumer will start throwing SaslAuthenticationException from 2 seconds onwards, until infinity.
 *
 * The offsetCommitTimeout as well as the saslAuthenticationRetryTimeout had been set to infinity as well.
 *
 * Once PC is observably retrying the failing broker, it is requested to close. The expected behavior is that the PC can
 * be shutdown cleanly.
 */
@Slf4j
@Timeout(60000L)
class MockConsumerEarlyCloseTest {

    private final String topic = MockConsumerEarlyCloseTest.class.getSimpleName();

    /**
     * Test that the mock consumer works as expected
     */
    @Test
    void mockConsumer() {
        final AtomicLong startFail = new AtomicLong(System.currentTimeMillis() + 2000L); // start failing after 2 seconds
        final AtomicLong failUntil = new AtomicLong(System.currentTimeMillis() + 200000000L); // never recover
        // counts the auth failures PC has actually been served - the observable that says PC is in the
        // retrying-a-failing-broker state we want to close it from
        final AtomicInteger authFailuresServed = new AtomicInteger();
        var mockConsumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized ConsumerRecords<String, String> poll(Duration timeout) {
                long now = System.currentTimeMillis();
                if(now > startFail.get() && now < failUntil.get()) {
                    log.info("Mocking failure before 20 seconds");
                    authFailuresServed.incrementAndGet();
                    throw new SaslAuthenticationException("Invalid username or password");
                }
                return super.poll(timeout);
            }

            @Override
            public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
                long now = System.currentTimeMillis();
                if(now > startFail.get() && now < failUntil.get()) {
                    authFailuresServed.incrementAndGet();
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
                .offsetCommitTimeout(Duration.ofSeconds(10000000L))
                .saslAuthenticationRetryTimeout(Duration.ofSeconds(250000000L))
                .build();
        var parallelConsumer = new ParallelEoSStreamProcessor<String, String>(options);
        parallelConsumer.subscribe(of(topic));

        // MockConsumer is not a correct implementation of the Consumer contract - must manually rebalance++ - or use LongPollingMockConsumer
        mockConsumer.rebalance(Collections.singletonList(tp));
        parallelConsumer.onPartitionsAssigned(of(tp));
        mockConsumer.updateBeginningOffsets(startOffsets);

        // Daemon thread: must NOT survive past this test method, or when it wakes
        // from sleep it'll addRecord() on a closed mockConsumer and throw an
        // uncaught exception that PIT attributes to whatever test is running next
        // in the same minion JVM. We also interrupt it explicitly in the finally
        // block to stop the loop promptly.
        Thread recordAdder = new Thread(() -> addRecords(mockConsumer), "early-close-record-adder");
        recordAdder.setDaemon(true);
        recordAdder.start();

        try {
            //
            ConcurrentLinkedQueue<RecordContext<String, String>> records = new ConcurrentLinkedQueue<>();
            parallelConsumer.poll(recordContexts -> {
                recordContexts.forEach(recordContext -> {
                    log.warn("Processing: {}", recordContext);
                    records.add(recordContext);
                });
            });
            // Close from the state the test is about: the broker is failing every call and PC is retrying.
            // Waiting for failures to actually have been served beats sleeping for a duration in which we
            // hope some were. Two, not one: the second failure can only follow a completed retry back-off,
            // so by then the poll loop is demonstrably retrying - and close lands mid-back-off, which is
            // where the 5s sleep used to (accidentally) put it.
            Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> authFailuresServed.get() >= 2);

            log.info("Trying to close...");
            parallelConsumer.close(); // request close while the consumer is failing every call
            log.info("Close successful!");
        } finally {
            recordAdder.interrupt();
        }
    }

    private void addRecords(MockConsumer<String, String> mockConsumer) {
        for (int i = 0; i < 100000; i++) {
            try {
                mockConsumer.addRecord(new org.apache.kafka.clients.consumer.ConsumerRecord<>(topic, 0, i, "key", "value"));
                Thread.sleep(1000L);
            } catch (IllegalStateException e) {
                // mockConsumer was closed - test has ended, stop quietly
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

}
