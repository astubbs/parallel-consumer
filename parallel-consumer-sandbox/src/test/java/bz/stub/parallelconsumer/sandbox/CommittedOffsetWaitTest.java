package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The wait a bound performs before it closes: every record this sandbox published accounted for by a committed
 * offset, or a refusal naming what is missing.
 * <p>
 * <b>Why a committed offset and not a delivery count.</b> The bound used to wait for every published record to
 * have been handed out of {@code poll}, plus two further poll cycles, which is a timing model of the engine
 * rather than a fact about it - a drain-first close transitions to closing while the worker pool may still hold
 * queued tasks, and then clears that queue. On a loaded runner that lost records (astubbs#504). The full
 * reasoning is on {@link SandboxConsumer#awaitEveryPublishedRecordCommitted()}; what is tested here is the wait
 * itself, driven directly rather than through a run, so that its refusal is exercised without needing an engine
 * that misbehaves.
 */
@Timeout(60)
class CommittedOffsetWaitTest {

    private static final TopicPartition ORDERS_0 = new TopicPartition("orders", 0);

    private static final TopicPartition ORDERS_1 = new TopicPartition("orders", 1);

    /**
     * Short, because every use of it here is a wait that is meant to fail - the default budget is twenty seconds
     * and a test of the refusal should not pay it.
     */
    private static final Duration IMPATIENT = Duration.ofMillis(250);

    @Test
    void aSandboxThatPublishedNothingHasNothingToWaitFor() {
        SandboxConsumer<String, String> consumer = assignedConsumer(1);

        consumer.awaitEveryPublishedRecordCommitted(IMPATIENT);

        assertThat(consumer.publishedRecords()).isEqualTo(0);
    }

    @Test
    void theWaitReturnsOnceEachPartitionsCommittedOffsetHasReachedItsPublishedCount() {
        SandboxConsumer<String, String> consumer = assignedConsumer(2);
        consumer.publish("orders", 0, "a", "1");
        consumer.publish("orders", 0, "b", "2");
        consumer.publish("orders", 1, "c", "3");

        consumer.commitAsync(offsets(ORDERS_0, 2L, ORDERS_1, 1L), null);

        // No assertion needed beyond returning: a wait that did not see the commits would throw.
        consumer.awaitEveryPublishedRecordCommitted(IMPATIENT);
    }

    @Test
    void aPartiallyCommittedPartitionFailsTheWaitNamingItAndTheShortfall() {
        SandboxConsumer<String, String> consumer = assignedConsumer(2);
        consumer.publish("orders", 0, "a", "1");
        consumer.publish("orders", 0, "b", "2");
        consumer.publish("orders", 0, "c", "3");
        consumer.publish("orders", 1, "d", "4");
        consumer.commitAsync(offsets(ORDERS_0, 1L, ORDERS_1, 1L), null);

        IllegalStateException refusal = assertThrows(IllegalStateException.class,
                () -> consumer.awaitEveryPublishedRecordCommitted(IMPATIENT));

        // The whole rendered map, not just the entry: it is what proves that a partition which DID commit
        // everything is absent from the shortfall, rather than merely that the short one is present.
        assertWithMessage("the refusal has to name the partition and how far short it is - and only the short "
                + "one - or it says no more than a timeout would")
                .that(refusal).hasMessageThat().contains("still uncommitted): {orders-0=2}");
        assertWithMessage("a parked record is the one legitimate way to reach this state, so the message says so")
                .that(refusal).hasMessageThat().contains("PARKS");
    }

    /**
     * Under the transactional commit mode the offsets go to the broker through the producer, so nothing at all
     * reaches the consumer's own commit history. A wait that read only that history would fail a run which had
     * committed every record - which is the transactional arm of {@link ClassicSandboxTest} exactly.
     */
    @Test
    void offsetsCommittedInsideAProducerTransactionCountAsCommitted() {
        SandboxConsumer<String, String> consumer = assignedConsumer(1);
        consumer.publish("orders", 0, "a", "1");
        consumer.publish("orders", 0, "b", "2");

        MockProducer<String, String> producer =
                new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        consumer.alsoCountingCommitsThrough(producer);

        assertWithMessage("with the offsets still only inside an uncommitted transaction, nothing is committed")
                .that(assertThrows(IllegalStateException.class,
                        () -> consumer.awaitEveryPublishedRecordCommitted(IMPATIENT)))
                .hasMessageThat().contains("orders-0=2");

        producer.initTransactions();
        producer.beginTransaction();
        producer.sendOffsetsToTransaction(Collections.singletonMap(ORDERS_0, new OffsetAndMetadata(2L)),
                new ConsumerGroupMetadata("sandbox"));
        producer.commitTransaction();

        consumer.awaitEveryPublishedRecordCommitted(IMPATIENT);
        assertThat(consumer.highestCommittedOffsets()).containsEntry(ORDERS_0, 2L);
    }

    /**
     * A consumer closed under the wait ends it quietly: that is how an unbounded run stops, and it is not a
     * shortfall anybody can act on.
     */
    @Test
    void aConsumerClosedUnderTheWaitEndsItRatherThanFailingIt() {
        SandboxConsumer<String, String> consumer = assignedConsumer(1);
        consumer.publish("orders", 0, "a", "1");
        consumer.close();

        consumer.awaitEveryPublishedRecordCommitted(IMPATIENT);

        assertThat(consumer.highestCommittedOffsets()).doesNotContainKey(ORDERS_0);
    }

    private static SandboxConsumer<String, String> assignedConsumer(int partitions) {
        SandboxConsumer<String, String> consumer =
                new SandboxConsumer<>(Collections.singletonList("orders"), partitions);
        // MockConsumer#rebalance is the DYNAMIC assignment path and refuses to run before something has
        // subscribed - which in a real run is the engine, in ClientRuntime#started.
        consumer.subscribe(Collections.singletonList("orders"));
        consumer.assignAfterSeeding();
        return consumer;
    }

    private static Map<TopicPartition, OffsetAndMetadata> offsets(TopicPartition first, long firstOffset,
                                                                  TopicPartition second, long secondOffset) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new LinkedHashMap<>();
        offsets.put(first, new OffsetAndMetadata(firstOffset));
        offsets.put(second, new OffsetAndMetadata(secondOffset));
        return offsets;
    }
}
