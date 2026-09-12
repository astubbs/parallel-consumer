package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.FakeRuntimeException;
import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static bz.stub.parallelconsumer.AbstractParallelEoSStreamProcessorTestBase.defaultTimeout;
import static com.google.common.truth.Truth.assertThat;

/**
 * What a parked record costs while it is parked (R11, R27): one key under key ordering, nothing under unordered
 * processing, and never a worker.
 * <p>
 * This is the claim park in place rests on. The offset map commits past an incomplete record, so an exhausted
 * record can stay where it is with the source topic as its store - and the price is bounded and visible: the
 * records behind it on its own key wait, everything else carries on, and a draining close does not wait for it.
 */
@Timeout(120)
class ParkedRecordsHoldTheKeyNotThePartitionTest extends AbstractFluentEngineTest {




    /**
     * R11. Under key ordering the parked record is the head of its key's shard, so later records with that key wait
     * behind it - and records with any other key do not. Park holds the key, not the partition, which is why the
     * documentation says park serves key and unordered processing.
     */
    @Test
    void parkUnderKeyOrderingHoldsItsKeyWhileOtherKeysCarryOn() {
        var behindTheParkedKey = new AtomicInteger();
        var otherKeys = new AtomicInteger();
        var pc = ParallelConsumer.connect(props()).withDefaultOrdering(ProcessingOrder.KEY);
        pc.string(TOPIC)
                .retryLimit(0)
                .retryDelay(Duration.ofMillis(10))
                .process(context -> {
                    if (context.offset() == 0) {
                        throw new FakeRuntimeException("this record never succeeds");
                    }
                    if ("stuck".equals(context.key())) {
                        behindTheParkedKey.incrementAndGet();
                    } else {
                        otherKeys.incrementAndGet();
                    }
                    return Outcome.succeeded();
                });

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "stuck", "the record that parks");
        runtime.publish(TOPIC, 0, 1, "stuck", "behind it on the same key");
        runtime.publish(TOPIC, 0, 2, "moving", "a different key");
        runtime.publish(TOPIC, 0, 3, "moving-too", "another different key");

        RouteDispatcher dispatcher = pc.dispatcher();
        Awaitility.await().atMost(defaultTimeout).untilAsserted(() -> {
            assertThat(dispatcher.parkedForRoute(TOPIC)).hasSize(1);
            assertThat(otherKeys.get()).isEqualTo(2);
        });

        // The record sharing the parked key waits behind it, and keeps waiting.
        Awaitility.await().pollDelay(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(behindTheParkedKey.get()).isEqualTo(0));
        assertThat(otherKeys.get()).isEqualTo(2);
        // Nothing past the parked record has committed as the base offset, because it is still incomplete...
        assertThat(runtime.committedOffset(TOPIC, 0)).isAtMost(0L);
        // ...and the offsets of the keys that carried on are in the commit's offset map.
        assertThat(runtime.committedMetadata(TOPIC, 0)).isNotEmpty();
    }

    /**
     * A parked record waits for ever by design, so a draining close must not wait for it: the drain figure nets a
     * failed record still inside its retry delay out against the retry queue, which is what makes a far-future park
     * cost the close nothing.
     * <p>
     * <b>This is the reason the shared harness closes without draining elsewhere</b> - a test with parked or
     * unbounded work uses {@link RecordingClientRuntime#closeWithoutDraining}. Here the draining close is the thing
     * under test, so it is the one place that calls {@link ParallelConsumerInstance#close()} on a parked instance.
     */
    @Test
    void aDrainingCloseWithOnlyParkedRecordsDoesNotWaitTheDrainTimeout() {
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC)
                .retryLimit(0)
                .retryDelay(Duration.ofMillis(10))
                .process(context -> {
                    throw new FakeRuntimeException("this record never succeeds");
                });

        var started = runtime.startAndAssign(pc, 1);
        handle = started;
        runtime.publish(TOPIC, 0, 0, "key-0", "an order");
        runtime.publish(TOPIC, 0, 1, "key-1", "another order that parks");

        RouteDispatcher dispatcher = pc.dispatcher();
        Awaitility.await().atMost(defaultTimeout).untilAsserted(() ->
                assertThat(dispatcher.parkedForRoute(TOPIC)).hasSize(2));

        Instant before = Instant.now();
        started.close();
        Duration closeTook = Duration.between(before, Instant.now());
        handle = null;

        // The drain timeout is thirty seconds; a close that waited for the parked records would spend all of it.
        assertThat(closeTook).isLessThan(Duration.ofSeconds(15));
    }

    /**
     * R27's small advantage of park in place over a broker-side queue, pinned: the instance holds the parked record
     * in memory, so it can still be resumed or exported after the broker's log start has moved past it. Only a
     * restart loses it, because it can no longer be re-polled - which is why the documentation tells a route whose
     * parked records must survive a restart to declare a destination or an age bound.
     * <p>
     * The mock consumer stands in for the broker here: moving its beginning offsets past the parked record is what
     * retention would do, and nothing about the running instance changes.
     */
    @Test
    void aParkedRecordIsStillResumableAfterTheBrokersLogStartHasPassedIt() {
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC)
                .retryLimit(0)
                .retryDelay(Duration.ofMillis(10))
                .process(context -> {
                    throw new FakeRuntimeException("this record never succeeds");
                });

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "customer-3", "an order worth keeping");

        RouteDispatcher dispatcher = pc.dispatcher();
        Awaitility.await().atMost(defaultTimeout).untilAsserted(() ->
                assertThat(dispatcher.parkedForRoute(TOPIC)).hasSize(1));

        // Retention removes everything up to offset 5 on the broker: the parked record is no longer fetchable.
        runtime.mockConsumer().updateBeginningOffsets(
                Collections.singletonMap(new TopicPartition(TOPIC, 0), 5L));

        // The instance is unaffected: the record, its bytes and its provenance are all still here, so a resume or
        // an export has everything it needs without going back to the broker.
        Awaitility.await().pollDelay(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(dispatcher.parkedForRoute(TOPIC)).hasSize(1));
        ParkedRecord parked = dispatcher.parkedForRoute(TOPIC).get(0);
        assertThat(parked.offset()).isEqualTo(0);
        assertThat(parked.key()).isEqualTo("customer-3");
        assertThat(new String(parked.raw().value(), StandardCharsets.UTF_8)).isEqualTo("an order worth keeping");
        // And it is still incomplete, so the committed position has not moved past it either.
        assertThat(runtime.committedOffset(TOPIC, 0)).isAtMost(0L);
        assertThat(handle.failureCause().isPresent()).isFalse();
    }
}
