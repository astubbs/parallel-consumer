package io.confluent.parallelconsumer.examples.support;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;
import pl.tlinkowski.unij.api.UniLists;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.IntFunction;

/**
 * Builds the input an example feeds itself: a fixed number of records, over a fixed number of
 * partitions, carrying a fixed number of distinct keys.
 * <p>
 * Example scaffolding, not a library type.
 * <p>
 * Deterministic by construction. The key sequence comes from a fixed seed, so two runs of the same
 * example see the same input and the run summary's key and partition figures are reproducible - a
 * summary whose numbers moved between runs would invite exactly the benchmark reading the summary is
 * written to avoid. Keys are spread across partitions round-robin by key index, so a key always lands
 * on one partition (as a real partitioner would guarantee) and no partition is left empty.
 */
public final class DemoRecords {

    /**
     * Fixed so every run of every example sees the same input.
     */
    public static final long SEED = 20260808L;

    private DemoRecords() {
    }

    /**
     * {@code distinctKeys} keys named {@code prefix-0000}, {@code prefix-0001}, ...
     */
    public static List<String> keys(String prefix, int distinctKeys) {
        if (prefix == null || prefix.trim().isEmpty()) {
            throw new IllegalArgumentException("prefix must be set - it is what makes demo keys readable in logs");
        }
        requireAtLeastOne("distinctKeys", distinctKeys);
        List<String> keys = new ArrayList<>(distinctKeys);
        for (int i = 0; i < distinctKeys; i++) {
            keys.add(String.format("%s-%04d", prefix, i));
        }
        return UniLists.copyOf(keys);
    }

    /**
     * Records with generated values, for examples that only care about the keys.
     *
     * @see #generate(String, int, int, List, IntFunction)
     */
    public static List<ConsumerRecord<String, String>> generate(String topic,
                                                                int recordCount,
                                                                int partitionCount,
                                                                List<String> keys) {
        return generate(topic, recordCount, partitionCount, keys, index -> "demo-value-" + index);
    }

    /**
     * Generates the example's input.
     * <p>
     * Every key in {@code keys} appears at least once (the first records take them in turn), so the
     * distinct-key count the run summary reports as the ordering ceiling is the count actually present
     * in the data. The remaining records draw keys from the fixed seed.
     *
     * @param topic          the topic name to stamp on every record
     * @param recordCount    how many records to generate; must be at least {@code keys.size()}
     * @param partitionCount how many partitions to spread them over
     * @param keys           the distinct keys, e.g. from {@link #keys(String, int)}
     * @param valueFactory   builds the domain value for record {@code index} - this is where the example
     *                       puts its industry
     * @return the records, in the order the example should feed them in
     */
    public static List<ConsumerRecord<String, String>> generate(String topic,
                                                                int recordCount,
                                                                int partitionCount,
                                                                List<String> keys,
                                                                IntFunction<String> valueFactory) {
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("topic must be set");
        }
        if (keys == null || keys.isEmpty()) {
            throw new IllegalArgumentException("keys must not be empty - see keys(String, int)");
        }
        if (valueFactory == null) {
            throw new IllegalArgumentException("valueFactory must be set");
        }
        requireAtLeastOne("recordCount", recordCount);
        requireAtLeastOne("partitionCount", partitionCount);
        if (recordCount < keys.size()) {
            throw new IllegalArgumentException("recordCount (" + recordCount + ") must be at least the number of "
                    + "distinct keys (" + keys.size() + "), otherwise the run summary would report an ordering "
                    + "ceiling the data does not contain");
        }

        Map<String, Integer> partitionOfKey = new HashMap<>();
        for (int i = 0; i < keys.size(); i++) {
            partitionOfKey.put(keys.get(i), i % partitionCount);
        }

        Random random = new Random(SEED);
        long[] nextOffset = new long[partitionCount];
        List<ConsumerRecord<String, String>> records = new ArrayList<>(recordCount);
        for (int index = 0; index < recordCount; index++) {
            // the first pass takes each key in turn, so every key is genuinely present
            String key = index < keys.size() ? keys.get(index) : keys.get(random.nextInt(keys.size()));
            int partition = partitionOfKey.get(key);
            long offset = nextOffset[partition]++;
            records.add(new ConsumerRecord<>(topic, partition, offset, key, valueFactory.apply(index)));
        }
        return UniLists.copyOf(records);
    }

    /**
     * The distinct-key count actually present in {@code records} - what the run summary should report as
     * the ordering ceiling under key ordering, rather than the count that was requested.
     */
    public static <V> int distinctKeyCount(Collection<ConsumerRecord<String, V>> records) {
        Set<String> distinct = new HashSet<>();
        for (ConsumerRecord<String, V> record : records) {
            distinct.add(record.key());
        }
        return distinct.size();
    }

    private static void requireAtLeastOne(String name, int value) {
        if (value < 1) {
            throw new IllegalArgumentException(name + " must be at least 1, was: " + value);
        }
    }
}
