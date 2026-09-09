package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.state.PartitionStateManager;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.function.Function;

/**
 * For tagging polled records with our epoch
 *
 * @see BrokerPollSystem#partitionAssignmentEpoch
 */
@Slf4j
@Value
public class EpochAndRecordsMap<K, V> {

    Map<TopicPartition, RecordsAndEpoch> recordMap = new HashMap<>();

    /**
     * A batch that carries no log end offset - every partition's is {@link OptionalLong#empty()}.
     * <p>
     * Correct for any caller that is not the poll loop, because the log end offset is only knowable to the thread
     * that owns the consumer, and only from the fetch that produced these very records. A reader of it must treat
     * absence as "not established yet" and try again on the next batch, never as a bound.
     */
    public EpochAndRecordsMap(ConsumerRecords<K, V> poll, PartitionStateManager<K, V> pm) {
        this(poll, pm, partition -> OptionalLong.empty());
    }

    /**
     * @param logEndOffsetAtPoll where each partition ended <b>according to the fetch that returned these records</b>,
     *                           or {@link OptionalLong#empty()} when the consumer could not say without blocking.
     *                           Read on the poll thread, at the poll, because that is the only thread that may touch
     *                           the consumer and the only moment the answer belongs to this batch -
     *                           {@code ConsumerManager#logEndOffsetIfKnownWithoutBlocking} is what supplies it, and
     *                           {@code PartitionState#maybeVerifyLoadedOffsetMapAgainstThePartition} is what it is
     *                           for
     */
    public EpochAndRecordsMap(ConsumerRecords<K, V> poll,
                              PartitionStateManager<K, V> pm,
                              Function<TopicPartition, OptionalLong> logEndOffsetAtPoll) {
        poll.partitions().forEach(partition -> {
            var records = poll.records(partition);
            Optional<Long> epochOfPartition = pm.epochOfPartitionIfAssigned(partition);
            if (!epochOfPartition.isPresent()) {
                // Race: poll() returned records for a partition before onPartitionsAssigned()
                // has fired. This is more likely with Kafka 2.x's eager rebalance protocol.
                // Safe to skip - these records haven't been committed, so Kafka will re-deliver
                // them on the next poll after the assignment callback completes.
                log.warn("Skipping {} records for partition {} — no epoch assigned yet. " +
                        "Records will be re-delivered on next poll after assignment completes.", records.size(), partition);
                return;
            }
            log.trace("Tagging {} records for {} with epoch {}", records.size(), partition, epochOfPartition.get());
            RecordsAndEpoch entry = new RecordsAndEpoch(partition, epochOfPartition.get(), records,
                    logEndOffsetAtPoll.apply(partition));
            recordMap.put(partition, entry);
        });
    }

    /**
     * Get the partitions which have records contained in this record set.
     *
     * @return the set of partitions with data in this record set (may be empty if no data was returned)
     */
    public Set<TopicPartition> partitions() {
        return Collections.unmodifiableSet(recordMap.keySet());
    }

    /**
     * Get just the records for the given partition
     *
     * @param partition The partition to get records for
     */
    public RecordsAndEpoch records(TopicPartition partition) {
        return this.recordMap.get(partition);
    }

    /**
     * The number of records for all topics
     */
    public int count() {
        return this.recordMap.values().stream()
                .mapToInt(x ->
                        x.getRecords().size()
                )
                .sum();
    }

    @Value
    public class RecordsAndEpoch {
        @NonNull TopicPartition topicPartition;
        @NonNull Long epochOfPartitionAtPoll;
        @NonNull List<ConsumerRecord<K, V>> records;

        /**
         * Where the partition ended when this batch was fetched (exclusive), if the consumer could say so without a
         * request of its own - the high watermark the fetch response already carried.
         * <p>
         * Empty is not "zero" and not "unbounded": it means nobody has established it yet, and the only correct
         * response is to look again at the next batch.
         */
        @NonNull OptionalLong logEndOffsetAtPoll;
    }

}
