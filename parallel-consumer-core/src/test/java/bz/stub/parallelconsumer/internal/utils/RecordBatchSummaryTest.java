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
import static java.util.Collections.singletonList;

/**
 * The point of {@link RecordBatchSummary} is that its output stays SHORT as the batch grows, so these tests assert the
 * exact rendering <em>and</em> that the length stops growing - see astubbs#169 / astubbs#170.
 *
 * @author Antony Stubbs
 */
class RecordBatchSummaryTest {

    private static final TopicPartition TP = new TopicPartition("my-topic", 3);

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
        Map<TopicPartition, List<Long>> hugeBatch = new HashMap<>();
        for (int partition = 0; partition < 500; partition++) {
            List<Long> offsets = new ArrayList<>();
            for (long offset = 0; offset < 1000; offset++) {
                offsets.add(offset);
            }
            hugeBatch.put(new TopicPartition("my-topic", partition), offsets);
        }

        String summary = RecordBatchSummary.summariseOffsets(hugeBatch);

        assertThat(summary).startsWith("500000 records across 500 partitions: ");
        assertThat(summary).contains("my-topic-0: 1000 records, offsets 0-999");
        // only the first few partitions are named, the rest are counted
        assertThat(summary).contains("and " + (500 - MAX_PARTITIONS_LISTED) + " more partitions");
        assertThat(summary).doesNotContain("my-topic-" + MAX_PARTITIONS_LISTED + ":");
        // a fixed ceiling - the whole point of the class
        assertThat(summary.length()).isLessThan(400);
    }

}
