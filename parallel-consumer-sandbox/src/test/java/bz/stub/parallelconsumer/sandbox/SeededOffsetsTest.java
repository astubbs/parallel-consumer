package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The seeding order, with a control arm - because "it worked" is not evidence that the order was the reason.
 * <p>
 * The failure this guards against is a silent one and not a hypothetical: a mock consumer assigned before its
 * beginning offsets were recorded killed the broker-poll thread in a CI row, and the reported failure was a
 * thirty-second commit timeout four seconds later with no mention of the cause. See {@link SandboxConsumer}'s own
 * documentation and
 * {@code docs/solutions/test-flakiness/assign-the-mock-consumer-after-seeding-its-offsets-2026-08-15.md}.
 * <!-- file-refs: N/A - that solution write-up is branch-only and has not merged to master -->
 */
@Timeout(60)
class SeededOffsetsTest {

    private static final TopicPartition ORDERS_0 = new TopicPartition("orders", 0);

    @Test
    void assigningBeforeAnythingHasSubscribedIsRefusedRatherThanFailingInsideKafka() {
        SandboxConsumer<byte[], byte[]> consumer = new SandboxConsumer<>(Collections.singletonList("orders"), 1);

        IllegalStateException refusal = assertThrows(IllegalStateException.class, consumer::assignAfterSeeding);

        assertThat(refusal).hasMessageThat().contains("subscribed");
    }

    @Test
    void aSandboxConsumerHasItsBeginningOffsetsBeforeAnythingIsAssigned() {
        SandboxConsumer<byte[], byte[]> consumer = new SandboxConsumer<>(asList("orders", "dispatches"), 2);

        assertWithMessage("nothing may be assigned until assignAfterSeeding is called - the window between "
                + "assigning and seeding is the whole of the bug")
                .that(consumer.assignment()).isEmpty();

        Map<TopicPartition, Long> beginnings = consumer.beginningOffsets(consumer.partitions());
        assertThat(beginnings).hasSize(4);
        assertThat(beginnings.get(ORDERS_0)).isEqualTo(0L);
        assertThat(beginnings.get(new TopicPartition("dispatches", 1))).isEqualTo(0L);
    }

    @Test
    void theFirstPollReturnsTheRecordsAlreadyPublished() {
        SandboxConsumer<byte[], byte[]> consumer = new SandboxConsumer<>(Collections.singletonList("orders"), 1);
        // MockConsumer#rebalance is the DYNAMIC assignment path and refuses to run before something has
        // subscribed - which in a real run is the engine, in ClientRuntime#started.
        consumer.subscribe(Collections.singletonList("orders"));
        consumer.assignAfterSeeding();
        consumer.publish("orders", 0, "k".getBytes(), "v".getBytes());

        assertWithMessage("a consumer seeded before assignment resets to the beginning and returns what is there")
                .that(consumer.poll(Duration.ofMillis(10)).count()).isEqualTo(1);
    }

    /**
     * <b>The control arm.</b> The same consumer, the same reset strategy, the same poll - and only the seeding
     * removed. If this did not throw, the test above would be passing for some other reason and the ordering in
     * {@link SandboxConsumer} would be a superstition.
     */
    @Test
    void aConsumerAssignedWithNoBeginningOffsetsThrowsOnItsFirstPoll() {
        LongPollingMockConsumer<byte[], byte[]> unseeded =
                new LongPollingMockConsumer<>(OffsetResetStrategy.EARLIEST);
        unseeded.subscribe(Collections.singletonList("orders"));
        unseeded.rebalanceWithoutAssignment(Collections.singletonList(ORDERS_0));

        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> unseeded.poll(Duration.ofMillis(10)));

        assertThat(thrown).hasMessageThat().contains("beginning offset");
    }
}
