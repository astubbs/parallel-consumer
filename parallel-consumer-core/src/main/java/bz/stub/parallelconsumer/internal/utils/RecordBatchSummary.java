package bz.stub.parallelconsumer.internal.utils;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.experimental.UtilityClass;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * Renders a batch of polled records as a short, <b>bounded</b> description, for use in log lines.
 * <p>
 * A log line that interpolates a whole batch - or a whole {@code PollContext} - grows with
 * {@code max.poll.records} and with the number of assigned partitions, so log tooling truncates it and the
 * operator loses exactly the part that identified the event. These helpers keep what actually diagnoses it
 * (topic-partition, record count, offset range) and cap the number of partitions named individually, so the
 * line has a fixed upper bound however large the batch is. The unabridged dump belongs at {@code DEBUG}.
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
     * @param topicPartition the partition the records were polled from
     * @param records        the records - only their offsets are read, never their keys or values
     * @return e.g. {@code my-topic-3: 500 records, offsets 1000-1499}
     */
    public static String summariseRecords(TopicPartition topicPartition, Collection<? extends ConsumerRecord<?, ?>> records) {
        List<Long> offsets = new ArrayList<>(records.size());
        for (ConsumerRecord<?, ?> record : records) {
            offsets.add(record.offset());
        }
        return summariseOffsets(topicPartition, offsets);
    }

    /**
     * @return e.g. {@code my-topic-3: 500 records, offsets 1000-1499}, or {@code my-topic-3: 1 record, offset 1000}
     */
    public static String summariseOffsets(TopicPartition topicPartition, Collection<Long> offsets) {
        if (offsets.isEmpty()) {
            return msg("{}: 0 records", topicPartition);
        }
        long lowest = Long.MAX_VALUE;
        long highest = Long.MIN_VALUE;
        for (Long offset : offsets) {
            lowest = Math.min(lowest, offset);
            highest = Math.max(highest, offset);
        }
        String range = (lowest == highest)
                ? msg("offset {}", lowest)
                : msg("offsets {}-{}", lowest, highest);
        return msg("{}: {}, {}", topicPartition, pluralise(offsets.size(), "record"), range);
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

        long recordCount = offsetsByPartition.values().stream().mapToLong(Collection::size).sum();

        List<Map.Entry<TopicPartition, List<Long>>> sorted = new ArrayList<>(offsetsByPartition.entrySet());
        sorted.sort(Comparator
                .comparing((Map.Entry<TopicPartition, List<Long>> entry) -> entry.getKey().topic())
                .thenComparingInt(entry -> entry.getKey().partition()));

        StringBuilder detail = new StringBuilder();
        int listed = Math.min(partitionCount, MAX_PARTITIONS_LISTED);
        for (int i = 0; i < listed; i++) {
            if (i > 0) {
                detail.append("; ");
            }
            Map.Entry<TopicPartition, List<Long>> entry = sorted.get(i);
            detail.append(summariseOffsets(entry.getKey(), entry.getValue()));
        }
        int notListed = partitionCount - listed;
        if (notListed > 0) {
            detail.append(msg("; and {} more {}", notListed, noun(notListed, "partition")));
        }

        if (partitionCount == 1) {
            // the totals would just repeat the single entry
            return detail.toString();
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
