package bz.stub.parallelconsumer.internal.utils;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.truth.Truth.assertThat;
import static bz.stub.parallelconsumer.internal.utils.RecordBatchSummary.MAX_PARTITIONS_LISTED;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.nCopies;
import static java.util.Collections.singletonList;

/**
 * The point of {@link RecordBatchSummary} is that its output stays SHORT as the batch grows, so these tests assert the
 * exact rendering <em>and</em> that the length stops growing - see astubbs#169 / astubbs#170.
 *
 * @author Antony Stubbs
 */
class RecordBatchSummaryTest {

    private static final TopicPartition TP = new TopicPartition("my-topic", 3);

    private static final int PARTITIONS = 500;

    private static final int RECORDS_PER_PARTITION = 1000;

    /**
     * Kafka's own limit on a topic name, so the longest one this class can ever be handed.
     */
    private static final int LONGEST_LEGAL_TOPIC_NAME = 249;

    @Test
    void summarisesCountAndOffsetRangeForOnePartition() {
        assertThat(RecordBatchSummary.summariseOffsets(TP, asList(1000L, 1001L, 1499L)))
                .isEqualTo("my-topic-3: 3 records, offsets 1000-1499");
    }

    @Test
    void singleRecordReadsAsOneOffset() {
        assertThat(RecordBatchSummary.summariseOffsets(TP, singletonList(1000L)))
                .isEqualTo("my-topic-3: 1 record, offset 1000");
    }

    @Test
    void emptyBatchStillNamesThePartition() {
        assertThat(RecordBatchSummary.summariseOffsets(TP, emptyList()))
                .isEqualTo("my-topic-3: 0 records");
    }

    @Test
    void offsetsAreDerivedFromRecordsWithoutRenderingThem() {
        List<ConsumerRecord<String, String>> records = asList(
                new ConsumerRecord<>(TP.topic(), TP.partition(), 7, "the-key", "the-secret-value"),
                new ConsumerRecord<>(TP.topic(), TP.partition(), 9, "the-key", "the-secret-value"));

        String summary = RecordBatchSummary.summariseRecords(TP, records);

        assertThat(summary).isEqualTo("my-topic-3: 2 records, offsets 7-9");
        // keys and values are the unbounded part - they must never reach the line
        assertThat(summary).doesNotContain("the-key");
        assertThat(summary).doesNotContain("the-secret-value");
    }

    @Test
    void aggregatesAcrossPartitionsInStableOrder() {
        Map<TopicPartition, List<Long>> offsets = new HashMap<>();
        offsets.put(new TopicPartition("b-topic", 0), singletonList(9L));
        offsets.put(new TopicPartition("a-topic", 2), asList(5L, 6L));
        offsets.put(new TopicPartition("a-topic", 1), singletonList(1L));

        assertThat(RecordBatchSummary.summariseOffsets(offsets)).isEqualTo(
                "4 records across 3 partitions: "
                        + "a-topic-1: 1 record, offset 1; "
                        + "a-topic-2: 2 records, offsets 5-6; "
                        + "b-topic-0: 1 record, offset 9");
    }

    @Test
    void singlePartitionBatchDoesNotRepeatItselfInTotals() {
        Map<TopicPartition, List<Long>> offsets = new HashMap<>();
        offsets.put(TP, asList(5L, 6L));

        assertThat(RecordBatchSummary.summariseOffsets(offsets)).isEqualTo("my-topic-3: 2 records, offsets 5-6");
    }

    @Test
    void emptyMapSummarisesAsNoRecords() {
        assertThat(RecordBatchSummary.summariseOffsets(new HashMap<>())).isEqualTo("0 records");
    }

    /**
     * The regression that matters: a consumer can hold thousands of partitions and {@code max.poll.records} records
     * each. The summary must not grow with either.
     */
    @Test
    void lineIsBoundedRegardlessOfBatchAndPartitionCount() {
        String summary = RecordBatchSummary.summariseOffsets(hugeBatchOn("my-topic"));

        assertThat(summary).startsWith("500000 records across 500 partitions: ");
        assertThat(summary).contains("my-topic-0: 1000 records, offsets 0-999");
        // only the first few partitions are named, the rest are counted
        assertThat(summary).contains("and " + (PARTITIONS - MAX_PARTITIONS_LISTED) + " more partitions");
        assertThat(summary).doesNotContain("my-topic-" + MAX_PARTITIONS_LISTED + ":");
        // a fixed ceiling - the whole point of the class
        assertThat(summary.length()).isLessThan(400);
    }

    /**
     * The same batch under Kafka's longest legal topic name.
     * <p>
     * Without this, the ceiling asserted above is an artefact of an eight-character name rather than a property of
     * the class: a listed entry still carries its topic's name, so the true bound is
     * {@value RecordBatchSummary#MAX_PARTITIONS_LISTED} names plus a fixed overhead. What the class promises is that
     * the line stops growing with the <em>batch</em>, and that is what {@link #ceilingFor} makes checkable - here the
     * name is 30x longer and the identical 500,000-record batch still lands under its own derived ceiling.
     */
    @Test
    void theBoundIsSetByTheTopicNameAndTheCapAlone() {
        String longestName = String.join("", nCopies(LONGEST_LEGAL_TOPIC_NAME, "x"));

        String summary = RecordBatchSummary.summariseOffsets(hugeBatchOn(longestName));

        assertThat(summary).startsWith("500000 records across 500 partitions: ");
        assertThat(summary).contains("and " + (PARTITIONS - MAX_PARTITIONS_LISTED) + " more partitions");
        assertThat(summary.length()).isLessThan(ceilingFor(longestName));
    }

    /**
     * {@link #PARTITIONS} partitions of {@link #RECORDS_PER_PARTITION} records each - half a million records, all on
     * one topic, which is the shape a large assignment actually takes.
     */
    private static Map<TopicPartition, List<Long>> hugeBatchOn(String topic) {
        Map<TopicPartition, List<Long>> hugeBatch = new HashMap<>(PARTITIONS);
        for (int partition = 0; partition < PARTITIONS; partition++) {
            List<Long> offsets = new ArrayList<>(RECORDS_PER_PARTITION);
            for (long offset = 0; offset < RECORDS_PER_PARTITION; offset++) {
                offsets.add(offset);
            }
            hugeBatch.put(new TopicPartition(topic, partition), offsets);
        }
        return hugeBatch;
    }

    /**
     * The ceiling derived from what the class actually caps - the number of partitions it names - rather than from
     * whatever a particular test's topic happens to be called. Each named entry is
     * {@code <topic>-<partition>: <count> records, offsets <lowest>-<highest>}, so the fixed part is comfortably
     * under 80 characters (two 19-digit offsets and a 10-digit partition being the worst case), plus the
     * separators, the totals prefix and the "and N more partitions" tail.
     */
    private static int ceilingFor(String topic) {
        return MAX_PARTITIONS_LISTED * (topic.length() + 80 + 2) + 100;
    }

}
