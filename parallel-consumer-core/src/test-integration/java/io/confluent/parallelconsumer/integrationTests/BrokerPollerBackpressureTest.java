
/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */
package io.confluent.parallelconsumer.integrationTests;

import io.confluent.csid.utils.ThreadUtils;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Slf4j
public class BrokerPollerBackpressureTest extends BrokerIntegrationTest<String, String> {

    static final int MAX_CONCURRENCY = 10;

    /**
     * Must divide exactly by {@link #MAX_CONCURRENCY}: {@link io.confluent.parallelconsumer.internal.PCModule} turns
     * this into a STATIC load factor (buffer / concurrency = 15), which caps records taken out of the shards at
     * exactly this buffer size.
     */
    static final int MESSAGE_BUFFER_SIZE = 150;

    static final int MESSAGE_COUNT = 200;

    ParallelEoSStreamProcessor<String, String> pc;

    @BeforeEach
    void setUp() {
        pc = startPcOnNewTopic(options -> options
                .ordering(KEY)
                .maxConcurrency(MAX_CONCURRENCY)
                .messageBufferSize(MESSAGE_BUFFER_SIZE));
    }

    /**
     * Verifies that records blocked in-flight count toward broker-poll backpressure (upstream #836): with every
     * worker latch-blocked, PC takes exactly {@link #MESSAGE_BUFFER_SIZE} records out of the shards (the static
     * load-factor cap) and pauses polling - even though the 50 records left in the shards are, on their own, far
     * below the pause threshold. Only the blocked in-flight records being counted makes the pause fire.
     * <p>
     * Replaces upstream's {@code brokerPollPausedWithEmptyShardsButHighInFlight}, whose first await
     * ({@code awaitingSelection == 0}) was vacuously true before partition assignment and unsatisfiable after
     * records arrived: the take-cap and the pause threshold are both {@link #MESSAGE_BUFFER_SIZE}, so "paused with
     * empty shards" is unreachable by design. On loaded machines the vacuous window closed and the test failed
     * deterministically (highcpu run 30603617471; see
     * docs/solutions/test-flakiness/vacuous-await-condition-brokerpoller-backpressure-2026-07-31.md).
     */
    @Test
    @SneakyThrows
    void brokerPollPausedWhenBlockedInFlightFillsBuffer() {
        var messageProcessingLatch = new CountDownLatch(1);
        assertThat(pc.getPausedPartitionSize()).isEqualTo(0); // should be polling initially
        getKcu().produceMessages(topic, MESSAGE_COUNT);
        AtomicInteger count = new AtomicInteger(0);
        pc.poll((context) -> {
            try {
                count.incrementAndGet();
                messageProcessingLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        // steady state: PC has taken its cap (the buffer size) out for processing, the remainder stays queued in
        // the shards - nothing completes while the workers are latch-blocked, so these values are stable
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(20)).untilAsserted(() -> {
            assertThat(pc.getWm().getNumberRecordsOutForProcessing()).isEqualTo(MESSAGE_BUFFER_SIZE);
            assertThat(pc.getWm().getNumberOfWorkQueuedInShardsAwaitingSelection()).isEqualTo(MESSAGE_COUNT - MESSAGE_BUFFER_SIZE);
        });
        // poll must pause: the 50 records in shards alone are below the threshold - only in-flight counting in
        // (upstream #836) trips it
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(20)).untilAsserted(() -> assertThat(pc.getPausedPartitionSize()).isEqualTo(1));
        //give it a second to make sure it does not get resumed again
        ThreadUtils.sleepQuietly(1000);
        assertThat(pc.getPausedPartitionSize()).isEqualTo(1); // should be polling still paused
        messageProcessingLatch.countDown();
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(20)).untilAsserted(() -> {
            assertThat(count.get()).isEqualTo(MESSAGE_COUNT);
            assertThat(pc.getWm().getNumberRecordsOutForProcessing()).isEqualTo(0);
        }); //wait for all messages to complete and verify no messages are out for processing anymore.
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(20)).untilAsserted(() -> assertThat(pc.getPausedPartitionSize()).isEqualTo(0)); //should resume now that messages have been processed and inflight is 0.
        getKcu().produceMessages(topic, 10); //send 10 more - just to make sure PC is actually polling for msgs
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(20)).untilAsserted(() -> assertThat(count.get()).isEqualTo(MESSAGE_COUNT + 10)); //should process the 10 new messages
    }

    @Test
    @SneakyThrows
    void brokerPollPausedWithHighNumberInShardsButLowInFlight() {
        var messageProcessingLatch = new CountDownLatch(1);
        assertThat(pc.getPausedPartitionSize()).isEqualTo(0); // should be polling initially
        getKcu().produceMessages(topic, 200, 5);
        AtomicInteger count = new AtomicInteger(0);
        pc.poll((context) -> {
            try {
                count.incrementAndGet();
                messageProcessingLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(200)).until(() -> pc.getWm().getNumberOfWorkQueuedInShardsAwaitingSelection() == 195); //wait for all processing threads to be blocked
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(20)).untilAsserted(() -> assertThat(pc.getPausedPartitionSize()).isEqualTo(1)); //polling should be paused with shards size being above buffer size and inflight being low;
        //give it a second to make sure it does not get resumed again
        ThreadUtils.sleepQuietly(1000);
        assertThat(pc.getPausedPartitionSize()).isEqualTo(1); // should be polling still paused
        messageProcessingLatch.countDown();
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(20)).untilAsserted(() -> {
            assertThat(count.get()).isEqualTo(200);
            assertThat(pc.getWm().getNumberRecordsOutForProcessing()).isEqualTo(0);
        }); //wait for all messages to complete and verify no messages are out for processing anymore.
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(20)).untilAsserted(() -> assertThat(pc.getPausedPartitionSize()).isEqualTo(0)); //should resume now that messages have been processed and inflight is 0.
        getKcu().produceMessages(topic, 10); //send 10 more - just to make sure PC is actually polling for msgs
        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(20)).untilAsserted(() -> assertThat(count.get()).isEqualTo(210)); //should process the 10 new messages
    }
}