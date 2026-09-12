package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.fluent.Consumed;
import bz.stub.parallelconsumer.fluent.ParallelConsumerInstance;
import bz.stub.parallelconsumer.fluent.Formats;
import bz.stub.parallelconsumer.fluent.Outcome;
import bz.stub.parallelconsumer.fluent.ParallelConsumerDefinition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongFunction;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Where the generator puts a key, on a topic with more than one partition.
 *
 * <h2>Two promises, and they were both broken for every key type but String</h2>
 * A key sticks to a partition - which is what makes a key-ordered run in the sandbox shard the way it would
 * against a broker - and two runs of one program place it the same way, so a sandbox failure can be reproduced.
 * <p>
 * The feed used to partition on {@code Objects.hashCode} of the key <b>object</b>. A pooled {@code String} key
 * survives that, but a key function returning a <em>freshly built</em> {@code byte[]} - the ordinary shape for a
 * {@code Formats#bytes()} route - overrides no {@code hashCode}, so that was the identity hash: a different
 * partition for every record of the same logical key, and a different placement on the next run.
 * <p>
 * <b>Nothing could have caught it</b>, which is half of why it is worth a file: {@code partitionsPerTopic(} had no
 * caller anywhere in the tree outside its own builder method, and every other test runs one partition, where
 * {@code floorMod(anything, 1)} is zero whatever the hash was.
 * <p>
 * This does not claim the sandbox agrees with a broker about <em>which</em> partition. Kafka's default
 * partitioner murmur2s the serialised key; this hashes the same bytes with {@link java.util.Arrays#hashCode},
 * which is stable and value-based and nothing more.
 */
@Timeout(120)
class KeyPartitioningTest {

    private static final String TOPIC = "orders";

    private static final int PARTITIONS = 4;

    private static final int KEYS = 5;

    /**
     * Comfortably more than {@link #KEYS}, so every key is drawn several times and a key that wanders between
     * partitions is seen wandering rather than merely seen once.
     */
    private static final int RECORD_BOUND = 60;

    /**
     * <b>A FRESH {@code byte[]} on every call</b>, which is the hazard this test exists for rather than an
     * accident of how it is written: an array's inherited {@code hashCode} is its identity, so a key placed by
     * {@code Objects.hashCode} would land somewhere different for every record of the same logical key and
     * somewhere different again on a second run. Reusing one array per key would hide exactly that.
     * <p>
     * It used to come from the module's own filler, which manufactured the same shape; written out here, the test
     * says what it is testing instead of depending on something else to be careless in the right way.
     */
    private static final LongFunction<byte[]> FRESH_BYTE_ARRAY_KEYS =
            index -> ("key-" + Math.floorMod(index, KEYS)).getBytes(StandardCharsets.UTF_8);

    @Test
    void aByteArrayKeyStaysOnOnePartitionAndOneSeedPlacesItTheSameWayTwice() {
        Map<String, Set<Integer>> first = placementsOfARun();
        Map<String, Set<Integer>> second = placementsOfARun();

        assertWithMessage("every key the feed draws from should have been seen")
                .that(first).hasSize(KEYS);
        for (Map.Entry<String, Set<Integer>> key : first.entrySet()) {
            assertWithMessage("key %s went to partitions %s - a key that does not stick to one partition makes "
                    + "key ordering in the sandbox show parallelism a broker would not give", key.getKey(),
                    key.getValue())
                    .that(key.getValue()).hasSize(1);
        }

        // The guard that keeps the assertions above from being satisfied by a run that used one partition: with
        // everything on partition 0 each key trivially has one partition and two runs trivially agree.
        Set<Integer> used = new HashSet<>();
        for (Set<Integer> partitions : first.values()) {
            used.addAll(partitions);
        }
        assertWithMessage("the keys have to be spread over more than one partition or this test cannot fail")
                .that(used.size()).isGreaterThan(1);

        assertWithMessage("two runs of the same program have to place the same keys on the same partitions, or a "
                + "sandbox failure is not reproducible")
                .that(second).isEqualTo(first);
    }

    /**
     * The classic path had the same defect, from the same cause: {@code ClassicSandbox.TypedFeed} also hashed the
     * key object. It has no encoded form to hash instead - a classic instance's mock consumer holds records of the
     * user's own types - so it hashes arrays by value and everything else by its own {@code hashCode}, which is
     * what {@code ShardKey} does when the engine shards on that same key.
     */
    @Test
    void theClassicPathAlsoKeepsAByteArrayKeyOnOnePartitionAcrossTwoSeededRuns() {
        Map<String, Set<Integer>> first = classicPlacementsOfARun();
        Map<String, Set<Integer>> second = classicPlacementsOfARun();

        assertThat(first).hasSize(KEYS);
        for (Map.Entry<String, Set<Integer>> key : first.entrySet()) {
            assertWithMessage("key %s went to partitions %s", key.getKey(), key.getValue())
                    .that(key.getValue()).hasSize(1);
        }
        Set<Integer> used = new HashSet<>();
        for (Set<Integer> partitions : first.values()) {
            used.addAll(partitions);
        }
        assertWithMessage("the keys have to be spread over more than one partition or this test cannot fail")
                .that(used.size()).isGreaterThan(1);
        assertThat(second).isEqualTo(first);
    }

    /**
     * The classic arm of {@link #placementsOfARunSeeded}: an options-builder instance over byte-array keys.
     */
    private static Map<String, Set<Integer>> classicPlacementsOfARun() {
        Map<String, Set<Integer>> placements = new ConcurrentHashMap<>();

        Sandbox sandbox = Sandbox.builder()
                .perSecond(500)
                .partitionsPerTopic(PARTITIONS)
                .bound(Bound.afterRecords(RECORD_BOUND))
                .build();

        try (ClassicSandbox<byte[], String> classic = sandbox.classic(byte[].class, String.class, TOPIC)) {
            ParallelEoSStreamProcessor<byte[], String> pc = SandboxFixtures.startClassic(classic,
                    SandboxFixtures.partitionOrdered(classic),
                    context -> {
                        var record = context.getSingleRecord();
                        placements.computeIfAbsent(new String(record.key(), StandardCharsets.UTF_8),
                                absent -> ConcurrentHashMap.newKeySet()).add(record.partition());
                    },
                    FRESH_BYTE_ARRAY_KEYS,
                    index -> TOPIC + "-" + index);

            assertWithMessage("the record bound should have been reached")
                    .that(classic.awaitBound(Duration.ofSeconds(60))).isTrue();
            pc.closeDrainFirst();
        }
        return placements;
    }

    /**
     * One whole run: generate to the bound over {@link #PARTITIONS} partitions with byte-array keys, and record
     * which partitions each key was seen on.
     *
     * @return key, as the UTF-8 text its bytes carry, to the set of partitions it arrived on
     */
    private static Map<String, Set<Integer>> placementsOfARun() {
        Map<String, Set<Integer>> placements = new ConcurrentHashMap<>();

        ParallelConsumerDefinition definition = SandboxFixtures.definition();
        definition.topic(TOPIC)
                .consumed(Consumed.with(Formats.bytes(), Formats.string()))
                .process(context -> {
                    String key = new String(context.key(), StandardCharsets.UTF_8);
                    placements.computeIfAbsent(key, absent -> ConcurrentHashMap.newKeySet())
                            .add(context.partition());
                    return Outcome.succeeded();
                });

        Sandbox sandbox = Sandbox.builder()
                .perSecond(500)
                .partitionsPerTopic(PARTITIONS)
                .bound(Bound.afterRecords(RECORD_BOUND))
                .feedingKeys(TOPIC, FRESH_BYTE_ARRAY_KEYS)
                .feeding(TOPIC, index -> TOPIC + "-" + index)
                .build();

        try (ParallelConsumerInstance instance = definition.start(sandbox)) {
            assertWithMessage("the record bound should have been reached and the instance closed")
                    .that(sandbox.awaitBound(Duration.ofSeconds(60))).isTrue();
            instance.awaitShutdown();
        }

        assertThat(sandbox.generatedRecords()).isEqualTo(RECORD_BOUND);
        return placements;
    }
}
