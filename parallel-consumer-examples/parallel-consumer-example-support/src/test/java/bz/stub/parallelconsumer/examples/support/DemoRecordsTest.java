package bz.stub.parallelconsumer.examples.support;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DemoRecordsTest {

    private static final String TOPIC = "payments";

    private static final int RECORDS = 100;

    private static final int PARTITIONS = 4;

    private static final int DISTINCT_KEYS = 10;

    @Test
    void twoRunsProduceIdenticalInput() {
        List<String> keys = DemoRecords.keys("account", DISTINCT_KEYS);

        List<ConsumerRecord<String, String>> first = DemoRecords.generate(TOPIC, RECORDS, PARTITIONS, keys);
        List<ConsumerRecord<String, String>> second = DemoRecords.generate(TOPIC, RECORDS, PARTITIONS, keys);

        assertThat(describe(first))
                .as("a summary whose numbers moved between runs would invite a benchmark reading")
                .isEqualTo(describe(second));
    }

    @Test
    void everyRequestedKeyIsActuallyPresent() {
        List<String> keys = DemoRecords.keys("account", DISTINCT_KEYS);

        List<ConsumerRecord<String, String>> records = DemoRecords.generate(TOPIC, RECORDS, PARTITIONS, keys);

        assertThat(DemoRecords.distinctKeyCount(records))
                .as("the run summary reports this count as the ordering ceiling, so the data has to contain it")
                .isEqualTo(DISTINCT_KEYS);
        assertThat(records).hasSize(RECORDS);
    }

    @Test
    void aKeyAlwaysLandsOnOnePartitionAndNoPartitionIsLeftEmpty() {
        List<String> keys = DemoRecords.keys("account", DISTINCT_KEYS);

        List<ConsumerRecord<String, String>> records = DemoRecords.generate(TOPIC, RECORDS, PARTITIONS, keys);

        Map<String, Integer> partitionOfKey = new HashMap<>();
        Set<Integer> usedPartitions = new HashSet<>();
        for (ConsumerRecord<String, String> record : records) {
            Integer previous = partitionOfKey.putIfAbsent(record.key(), record.partition());
            assertThat(previous == null ? record.partition() : previous)
                    .as("key %s must always land on one partition, as a real partitioner guarantees", record.key())
                    .isEqualTo(record.partition());
            usedPartitions.add(record.partition());
        }
        assertThat(usedPartitions).containsExactlyInAnyOrder(0, 1, 2, 3);
    }

    @Test
    void offsetsRunContiguouslyFromZeroWithinEachPartition() {
        List<ConsumerRecord<String, String>> records =
                DemoRecords.generate(TOPIC, RECORDS, PARTITIONS, DemoRecords.keys("account", DISTINCT_KEYS));

        Map<Integer, Long> nextExpected = new HashMap<>();
        for (ConsumerRecord<String, String> record : records) {
            long expected = nextExpected.getOrDefault(record.partition(), 0L);
            assertThat(record.offset()).isEqualTo(expected);
            nextExpected.put(record.partition(), expected + 1);
            assertThat(record.topic()).isEqualTo(TOPIC);
        }
    }

    @Test
    void theCallerSuppliesTheDomainValues() {
        List<ConsumerRecord<String, String>> records = DemoRecords.generate(TOPIC, 3, 1,
                DemoRecords.keys("account", 3), index -> "GBP " + index);

        assertThat(records).extracting(ConsumerRecord::value).containsExactly("GBP 0", "GBP 1", "GBP 2");
    }

    @Test
    void inputThatCannotContainTheRequestedKeysIsRejected() {
        List<String> tooManyKeys = DemoRecords.keys("account", 5);

        assertThatThrownBy(() -> DemoRecords.generate(TOPIC, 4, PARTITIONS, tooManyKeys))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ordering ceiling");
        assertThatThrownBy(() -> DemoRecords.keys("account", 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("distinctKeys");
    }

    private List<String> describe(List<ConsumerRecord<String, String>> records) {
        List<String> described = new ArrayList<>(records.size());
        for (ConsumerRecord<String, String> record : records) {
            described.add(record.partition() + "/" + record.offset() + "/" + record.key() + "/" + record.value());
        }
        return described;
    }
}
