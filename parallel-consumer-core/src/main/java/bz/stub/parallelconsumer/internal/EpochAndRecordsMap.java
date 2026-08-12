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

/**
 * For tagging polled records with our epoch
 *
 * @see BrokerPollSystem#partitionAssignmentEpoch
 */
@Slf4j
@Value
public class EpochAndRecordsMap<K, V> {

    Map<TopicPartition, RecordsAndEpoch> recordMap = new HashMap<>();

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(EpochAndRecordsMap.class);

    public EpochAndRecordsMap(ConsumerRecords<K, V> poll, PartitionStateManager<K, V> pm) {
        poll.partitions().forEach(partition -> {
            var records = poll.records(partition);
            Long epochOfPartition = pm.getEpochOfPartition(partition);
            if (epochOfPartition == null) {
                log.warn("Skipping {} records for partition {} — no epoch assigned yet. " +
                        "Records will be re-delivered on next poll after assignment completes.", records.size(), partition);
                return;
            }
            log.trace("Tagging {} records for {} with epoch {}", records.size(), partition, epochOfPartition);
            RecordsAndEpoch entry = new RecordsAndEpoch(partition, epochOfPartition, records);
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
    }

}
