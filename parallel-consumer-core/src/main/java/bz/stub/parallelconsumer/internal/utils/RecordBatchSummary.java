package bz.stub.parallelconsumer.internal.utils;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.experimental.UtilityClass;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * Renders a batch of polled records as a short, <b>bounded</b> description, for use in log lines.
 * <p>
 * A log line that interpolates a whole batch - or a whole {@code PollContext} - grows with
 * {@code max.poll.records} and with the number of assigned partitions, so log tooling truncates it and the
 * operator loses exactly the part that identified the event. These helpers keep what actually diagnoses it
 * (topic-partition, record count, offset range) and cap the number of partitions named individually, so the
 * line has a fixed upper bound however large the batch is. The unabridged dump belongs at {@code DEBUG}.
 * <p>
 * Two call sites use it; the same defect is live on several more, which
 * {@code docs/inflight/bug-unbounded-log-lines.md} lists with their anchors - including the ones deliberately
 * dismissed, where the collection <em>is</em> the diagnostic and summarising it would destroy the line.
 *
 * @author Antony Stubbs
 * @see <a href="https://github.com/astubbs/parallel-consumer/issues/169">#169 - dropped batch WARN</a>
 * @see <a href="https://github.com/astubbs/parallel-consumer/issues/170">#170 - user function failure ERROR</a>
 */
@UtilityClass
public class RecordBatchSummary {

    /**
     * The most partitions named individually before the summary collapses the rest into a count. A consumer can be
     * assigned thousands of partitions, and naming every one of them is the unbounded-line bug this class exists to
     * prevent.
     */
    public static final int MAX_PARTITIONS_LISTED = 5;

    /**
     * Hoisted rather than rebuilt per call: {@link Comparator#comparing} plus {@link Comparator#thenComparingInt}
     * allocates two composed comparators, and this is reached from a log line on a failure path.
     */
    private static final Comparator<Map.Entry<TopicPartition, List<Long>>> BY_TOPIC_THEN_PARTITION = Comparator
            .comparing((Map.Entry<TopicPartition, List<Long>> entry) -> entry.getKey().topic())
            .thenComparingInt(entry -> entry.getKey().partition());

    /**
     * @param topicPartition the partition the records were polled from
     * @param records        the records - only their offsets are read, never their keys or values
     * @return e.g. {@code my-topic-3: 500 records, offsets 1000-1499}
     */
    public static String summariseRecords(TopicPartition topicPartition, Collection<? extends ConsumerRecord<?, ?>> records) {
        // one primitive pass, rather than copying every offset into a boxed List just to min/max it - a 500-record
        // batch was allocating ~500 Longs plus the list to produce two numbers, on a path that only ever logs
        long lowest = Long.MAX_VALUE;
        long highest = Long.MIN_VALUE;
        for (ConsumerRecord<?, ?> record : records) {
            long offset = record.offset();
            lowest = Math.min(lowest, offset);
            highest = Math.max(highest, offset);
        }
        return summarise(topicPartition, records.size(), lowest, highest);
    }

    /**
     * @return e.g. {@code my-topic-3: 500 records, offsets 1000-1499}, or {@code my-topic-3: 1 record, offset 1000}
     */
    public static String summariseOffsets(TopicPartition topicPartition, Collection<Long> offsets) {
        long lowest = Long.MAX_VALUE;
        long highest = Long.MIN_VALUE;
        for (long offset : offsets) {
            lowest = Math.min(lowest, offset);
            highest = Math.max(highest, offset);
        }
        return summarise(topicPartition, offsets.size(), lowest, highest);
    }

    /**
     * The one rendering both single-partition entry points share, taking the count and range already reduced to
     * primitives - so neither caller has to materialise the offsets it walked.
     */
    private static String summarise(TopicPartition topicPartition, int recordCount, long lowest, long highest) {
        if (recordCount == 0) {
            return msg("{}: 0 records", topicPartition);
        }
        String range = (lowest == highest)
                ? msg("offset {}", lowest)
                : msg("offsets {}-{}", lowest, highest);
        return msg("{}: {}, {}", topicPartition, pluralise(recordCount, "record"), range);
    }

    /**
     * @param offsetsByPartition offsets of the records in the batch, grouped by the partition they came from
     * @return e.g. {@code 3 records across 2 partitions: my-topic-0: 2 records, offsets 5-6; my-topic-1: 1 record,
     * offset 9} - listing at most {@value #MAX_PARTITIONS_LISTED} partitions, then a count of the remainder. A batch
     * from a single partition renders as just that partition's summary, since the totals would only repeat it
     */
    public static String summariseOffsets(Map<TopicPartition, List<Long>> offsetsByPartition) {
        int partitionCount = offsetsByPartition.size();
        if (partitionCount == 0) {
            return pluralise(0, "record");
        }
        if (partitionCount == 1) {
            // the totals would just repeat the single entry - and this is the common shape (one user-function batch
            // is usually one partition), so it returns before the count, the sort and the join below
            Map.Entry<TopicPartition, List<Long>> only = offsetsByPartition.entrySet().iterator().next();
            return summariseOffsets(only.getKey(), only.getValue());
        }

        long recordCount = 0;
        for (List<Long> offsets : offsetsByPartition.values()) {
            recordCount += offsets.size();
        }

        String detail = offsetsByPartition.entrySet().stream()
                .sorted(BY_TOPIC_THEN_PARTITION)
                .limit(MAX_PARTITIONS_LISTED)
                .map(entry -> summariseOffsets(entry.getKey(), entry.getValue()))
                .collect(Collectors.joining("; "));

        int notListed = partitionCount - Math.min(partitionCount, MAX_PARTITIONS_LISTED);
        if (notListed > 0) {
            detail += msg("; and {} more {}", notListed, noun(notListed, "partition"));
        }

        return msg("{} across {}: {}",
                pluralise(recordCount, "record"),
                pluralise(partitionCount, "partition"),
                detail);
    }

    private static String pluralise(long count, String noun) {
        return count + " " + noun(count, noun);
    }

    private static String noun(long count, String noun) {
        return (count == 1) ? noun : noun + "s";
    }

}
