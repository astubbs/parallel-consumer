package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.offsets.OffsetSimultaneousEncoder;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The wait a bound performs before it closes: every record this sandbox published accounted for - by a committed
 * offset, or by being parked - or a refusal naming what is missing.
 * <p>
 * <b>Why a committed offset and not a delivery count.</b> The bound used to wait for every published record to
 * have been handed out of {@code poll}, plus two further poll cycles, which is a timing model of the engine
 * rather than a fact about it - a drain-first close transitions to closing while the worker pool may still hold
 * queued tasks, and then clears that queue. On a loaded runner that lost records (astubbs#504). The full
 * reasoning is on {@link SandboxConsumer#awaitEveryPublishedRecordCommitted()}; what is tested here is the wait
 * itself, driven directly rather than through a run, so that its refusal is exercised without needing an engine
 * that misbehaves.
 * <p>
 * <b>Why a parked record counts as accounted for.</b> Parking is a terminal outcome for the run, and a parked
 * record's partition never commits past it - so the first version of this wait refused a bounded run whose records
 * park, and the README's own quickstart is exactly such a run. The wait now counts a partition done when its
 * published records equal what the commit says is complete plus what is parked on it right now; the arithmetic and
 * the reasoning it overrides are on {@link SandboxConsumer#awaitEveryPublishedRecordCommitted()}.
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
        SandboxConsumer<String, String> consumer = SandboxFixtures.assignedConsumer("orders", 1);

        consumer.awaitEveryPublishedRecordCommitted(IMPATIENT);

        assertThat(consumer.publishedRecords()).isEqualTo(0);
    }

    @Test
    void theWaitReturnsOnceEachPartitionsCommittedOffsetHasReachedItsPublishedCount() {
        SandboxConsumer<String, String> consumer = SandboxFixtures.assignedConsumer("orders", 2);
        consumer.publish("orders", 0, "a", "1");
        consumer.publish("orders", 0, "b", "2");
        consumer.publish("orders", 1, "c", "3");

        consumer.commitAsync(offsets(ORDERS_0, 2L, ORDERS_1, 1L), null);

        // No assertion needed beyond returning: a wait that did not see the commits would throw.
        consumer.awaitEveryPublishedRecordCommitted(IMPATIENT);
    }

    @Test
    void aPartiallyCommittedPartitionFailsTheWaitNamingItAndTheShortfall() {
        SandboxConsumer<String, String> consumer = SandboxFixtures.assignedConsumer("orders", 2);
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
                .that(refusal).hasMessageThat()
                .contains("{orders-0=published 3, completed 1, parked 0, so 2 unaccounted for}");
    }

    /**
     * The quickstart's parcel-scans partition exactly: every record on it parked, so not one of them ever
     * completed, so the engine never had an offset to commit for that partition at all. Nothing about that run is
     * unfinished, and this is the case the first version of the wait refused.
     */
    @Test
    void aPartitionWhoseOnlyOutstandingRecordIsParkedIsAccountedFor() {
        SandboxConsumer<String, String> consumer = SandboxFixtures.assignedConsumer("orders", 1);
        consumer.publish("orders", 0, "a", "1");
        consumer.countingParkedRecordsWith(() -> Collections.singletonMap(ORDERS_0, 1L));

        // No assertion needed beyond returning: a wait that did not count the parked record would refuse.
        consumer.awaitEveryPublishedRecordCommitted(IMPATIENT);
    }

    /**
     * A parked record below the highest succeeded offset is in both halves of the sum - it is one of the
     * incompletes the commit's metadata encodes, and it is in the parked view - so the wait has to count it once.
     * <p>
     * Counting it twice is not visible from a run that satisfies the wait, so the fourth record is what makes this
     * test discriminating: with the parked record double-counted, four records would be accounted for and the wait
     * would pass over a record that is genuinely still in flight.
     */
    @Test
    void aParkedRecordInsideTheEncodedRangeIsCountedOnceAndNotTwice() {
        SandboxConsumer<String, String> consumer = SandboxFixtures.assignedConsumer("orders", 1);
        // Offset 0 parks; 1 and 2 succeed; 3 is still in flight.
        consumer.publish("orders", 0, "a", "1");
        consumer.publish("orders", 0, "b", "2");
        consumer.publish("orders", 0, "c", "3");
        consumer.publish("orders", 0, "d", "4");
        consumer.commitAsync(Collections.singletonMap(ORDERS_0,
                new OffsetAndMetadata(0L, offsetMapCommittedAt(0L, 2L, 0L))), null);
        consumer.countingParkedRecordsWith(() -> Collections.singletonMap(ORDERS_0, 1L));

        IllegalStateException refusal = assertThrows(IllegalStateException.class,
                () -> consumer.awaitEveryPublishedRecordCommitted(IMPATIENT));

        assertWithMessage("offsets 1 and 2 are complete inside the encoded range and offset 0 is the parked one, "
                + "so three of the four are accounted for and the fourth is what the refusal is about")
                .that(refusal).hasMessageThat()
                .contains("{orders-0=published 4, completed 2, parked 1, so 1 unaccounted for}");
    }

    /**
     * The same partition once its fourth record has completed too: the wait ends, which is what proves the
     * previous test's refusal was about that record and not about the parked one.
     */
    @Test
    void aParkedRecordInsideTheEncodedRangeSatisfiesTheWaitOnceTheRestHaveCompleted() {
        SandboxConsumer<String, String> consumer = SandboxFixtures.assignedConsumer("orders", 1);
        consumer.publish("orders", 0, "a", "1");
        consumer.publish("orders", 0, "b", "2");
        consumer.publish("orders", 0, "c", "3");
        consumer.commitAsync(Collections.singletonMap(ORDERS_0,
                new OffsetAndMetadata(0L, offsetMapCommittedAt(0L, 2L, 0L))), null);
        consumer.countingParkedRecordsWith(() -> Collections.singletonMap(ORDERS_0, 1L));

        consumer.awaitEveryPublishedRecordCommitted(IMPATIENT);
    }

    /**
     * A partition whose committed offset has stopped advancing still reports progress, and it reports it in the
     * metadata - which is the case a "highest commit" chosen by offset alone cannot see.
     * <p>
     * A parked record low in the partition pins {@code highestSequentialSucceeded + 1} at its own offset for good,
     * while every record above it keeps completing. Successive commits therefore share an offset and differ only
     * in their offset map. Keeping the first one seen at that offset freezes the accounting on the least complete
     * map for the rest of the run, and the wait then spends its whole budget and blames the instance for holding
     * records it finished long ago.
     * <p>
     * This is the shape the wait was rewritten for - a run that parks - so it is the case most likely to hit it,
     * and the reason nothing caught it is that no existing test has two commits at one offset: in
     * {@code FluentQuickstartAppTest} every scan fails, so that partition never commits at all, and the orders
     * partition advances monotonically.
     */
    @Test
    void twoCommitsAtOneOffsetKeepTheOneThatAccountsForMore() {
        SandboxConsumer<String, String> consumer = SandboxFixtures.assignedConsumer("orders", 1);
        // Offset 0 parks, so the committed offset can never move past it; 1, 2 and 3 complete.
        consumer.publish("orders", 0, "a", "1");
        consumer.publish("orders", 0, "b", "2");
        consumer.publish("orders", 0, "c", "3");
        consumer.publish("orders", 0, "d", "4");
        consumer.countingParkedRecordsWith(() -> Collections.singletonMap(ORDERS_0, 1L));

        // The earlier commit: offset 1 is done, 2 and 3 are not.
        consumer.commitAsync(Collections.singletonMap(ORDERS_0,
                new OffsetAndMetadata(0L, offsetMapCommittedAt(0L, 1L, 0L))), null);
        // The later one, at the SAME offset because the park still pins it, with 2 and 3 done as well.
        consumer.commitAsync(Collections.singletonMap(ORDERS_0,
                new OffsetAndMetadata(0L, offsetMapCommittedAt(0L, 3L, 0L))), null);

        // No assertion needed beyond returning: reading the earlier commit accounts for two of the four records
        // and the wait would refuse.
        consumer.awaitEveryPublishedRecordCommitted(IMPATIENT);
    }

    /**
     * One partition's commit metadata as the engine writes it, built with the engine's own encoder rather than by
     * hand: the encoded range runs from the committed offset to the highest succeeded one, and the incompletes are
     * the offsets inside it that are not done.
     *
     * @param committedOffset  the offset being committed, which the payload's offsets are relative to
     * @param highestSucceeded the top of the encoded range
     * @param incomplete       the offsets inside the range that are not complete
     */
    private static String offsetMapCommittedAt(long committedOffset, long highestSucceeded, long... incomplete) {
        SortedSet<Long> incompletes = new TreeSet<>();
        for (long offset : incomplete) {
            incompletes.add(offset);
        }
        OffsetSimultaneousEncoder encoder =
                new OffsetSimultaneousEncoder(committedOffset, highestSucceeded, incompletes);
        try {
            return Base64.getEncoder().encodeToString(encoder.invoke().packSmallest());
        } catch (Exception e) {
            throw new AssertionError("the offset map for a range this small must be encodable", e);
        }
    }

    /**
     * Under the transactional commit mode the offsets go to the broker through the producer, so nothing at all
     * reaches the consumer's own commit history. A wait that read only that history would fail a run which had
     * committed every record - which is the transactional arm of {@link ClassicSandboxTest} exactly.
     */
    @Test
    void offsetsCommittedInsideAProducerTransactionCountAsCommitted() {
        SandboxConsumer<String, String> consumer = SandboxFixtures.assignedConsumer("orders", 1);
        consumer.publish("orders", 0, "a", "1");
        consumer.publish("orders", 0, "b", "2");

        MockProducer<String, String> producer =
                new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        consumer.alsoCountingCommitsThrough(producer);

        assertWithMessage("with the offsets still only inside an uncommitted transaction, nothing is committed")
                .that(assertThrows(IllegalStateException.class,
                        () -> consumer.awaitEveryPublishedRecordCommitted(IMPATIENT)))
                .hasMessageThat().contains("{orders-0=published 2, completed 0, parked 0, so 2 unaccounted for}");

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
        SandboxConsumer<String, String> consumer = SandboxFixtures.assignedConsumer("orders", 1);
        consumer.publish("orders", 0, "a", "1");
        consumer.close();

        consumer.awaitEveryPublishedRecordCommitted(IMPATIENT);

        assertThat(consumer.highestCommittedOffsets()).doesNotContainKey(ORDERS_0);
    }

    private static Map<TopicPartition, OffsetAndMetadata> offsets(TopicPartition first, long firstOffset,
                                                                  TopicPartition second, long secondOffset) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new LinkedHashMap<>();
        offsets.put(first, new OffsetAndMetadata(firstOffset));
        offsets.put(second, new OffsetAndMetadata(secondOffset));
        return offsets;
    }
}
