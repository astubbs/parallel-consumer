package io.confluent.parallelconsumer.dashboard.snapshot;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * Builds a {@link SimpleMeterRegistry} shaped like the one a running Parallel Consumer publishes into, without
 * needing a broker or a live instance.
 * <p>
 * Meter names here are deliberately written out as <strong>string literals</strong> rather than taken from
 * {@code PCMetricsDef}. Production code reads the names from that enum, so a fixture that did the same could never
 * detect a wrong name - both sides would simply move together. Literals here make these tests an actual check on the
 * wire names, and {@code MeterRegistrySamplerTest#meterNamesMatchCoreDefinitions} pins the two together explicitly.
 */
class PcMeterFixture {

    static final String PARTITION_HIGHEST_SEEN = "pc.partition.highest.seen.offset";

    static final String PARTITION_HIGHEST_COMPLETED = "pc.partition.highest.completed.offset";

    static final String PARTITION_HIGHEST_SEQUENTIAL_SUCCEEDED = "pc.partition.highest.sequential.succeeded.offset";

    static final String PARTITION_LATEST_COMMITTED = "pc.partition.latest.committed.offset";

    static final String PARTITION_INCOMPLETE_OFFSETS = "pc.partition.incomplete.offsets";

    static final String PARTITION_ASSIGNMENT_EPOCH = "pc.partition.assignment.epoch";

    static final String PROCESSED_RECORDS = "pc.processed.records";

    static final String FAILED_RECORDS = "pc.failed.records";

    static final String SLOW_RECORDS = "pc.slow.records";

    static final String INFLIGHT_RECORDS = "pc.inflight.records";

    static final String WAITING_RECORDS = "pc.waiting.records";

    static final String SHARDS = "pc.shards";

    static final String SHARDS_SIZE = "pc.shards.size";

    static final String INCOMPLETE_OFFSETS_TOTAL = "pc.incomplete.offsets.total";

    static final String DYNAMIC_LOAD_FACTOR = "pc.dynamic.load.factor";

    static final String PARTITIONS_PAUSED = "pc.partitions.paused";

    static final String PARTITIONS_NUMBER = "pc.partitions.number";

    static final String STATUS = "pc.status";

    static final String POLLER_STATUS = "pc.poller.status";

    static final String ENCODING_TIME = "pc.offsets.encoding.time";

    static final String ENCODING_USAGE = "pc.offsets.encoding.usage";

    static final String METADATA_SPACE_USED = "pc.metadata.space.used";

    static final String PAYLOAD_RATIO_USED = "pc.payload.ratio.used";

    private final MeterRegistry registry = new SimpleMeterRegistry();

    MeterRegistry getRegistry() {
        return registry;
    }

    /**
     * Registers the six offset gauges for one partition, in an order that satisfies the ribbon invariant.
     */
    PcMeterFixture partitionOffsets(String topic,
                                    int partition,
                                    long committed,
                                    long sequentialSucceeded,
                                    long completed,
                                    long seen,
                                    long incomplete,
                                    long epoch) {
        gauge(PARTITION_LATEST_COMMITTED, topic, partition, committed);
        gauge(PARTITION_HIGHEST_SEQUENTIAL_SUCCEEDED, topic, partition, sequentialSucceeded);
        gauge(PARTITION_HIGHEST_COMPLETED, topic, partition, completed);
        gauge(PARTITION_HIGHEST_SEEN, topic, partition, seen);
        gauge(PARTITION_INCOMPLETE_OFFSETS, topic, partition, incomplete);
        gauge(PARTITION_ASSIGNMENT_EPOCH, topic, partition, epoch);
        return this;
    }

    PcMeterFixture partitionCounters(String topic, int partition, double processed, double failed, double slow) {
        counter(PROCESSED_RECORDS, topic, partition, processed);
        counter(FAILED_RECORDS, topic, partition, failed);
        counter(SLOW_RECORDS, topic, partition, slow);
        return this;
    }

    PcMeterFixture aggregates(long inflight,
                              long waiting,
                              long shards,
                              long shardsSize,
                              long incompleteTotal,
                              double loadFactor,
                              long paused,
                              long partitions) {
        gauge(INFLIGHT_RECORDS, inflight);
        gauge(WAITING_RECORDS, waiting);
        gauge(SHARDS, shards);
        gauge(SHARDS_SIZE, shardsSize);
        gauge(INCOMPLETE_OFFSETS_TOTAL, incompleteTotal);
        gauge(DYNAMIC_LOAD_FACTOR, loadFactor);
        gauge(PARTITIONS_PAUSED, paused);
        gauge(PARTITIONS_NUMBER, partitions);
        return this;
    }

    PcMeterFixture lifecycle(int statusValue, int pollerStatusValue) {
        gauge(STATUS, statusValue);
        gauge(POLLER_STATUS, pollerStatusValue);
        return this;
    }

    PcMeterFixture encoding() {
        io.micrometer.core.instrument.Timer.builder(ENCODING_TIME).register(registry)
                .record(Duration.ofMillis(7));
        io.micrometer.core.instrument.Timer.builder(ENCODING_TIME).register(registry)
                .record(Duration.ofMillis(3));
        Counter.builder(ENCODING_USAGE).tag("encoding", "RunLength").register(registry).increment(4);
        Counter.builder(ENCODING_USAGE).tag("encoding", "BitSetV2Compressed").register(registry).increment(2);
        DistributionSummary.builder(METADATA_SPACE_USED).register(registry).record(0.25);
        DistributionSummary.builder(METADATA_SPACE_USED).register(registry).record(0.75);
        DistributionSummary.builder(PAYLOAD_RATIO_USED).register(registry).record(0.5);
        return this;
    }

    /**
     * A gauge whose supplier returns NaN - which is what Micrometer reports once a weakly-referenced gauge target has
     * been collected. A missing reading, not a measurement of zero.
     */
    PcMeterFixture nanGauge(String name) {
        Gauge.builder(name, (Supplier<Number>) () -> Double.NaN).register(registry);
        return this;
    }

    PcMeterFixture gauge(String name, double value) {
        Gauge.builder(name, (Supplier<Number>) () -> value).register(registry);
        return this;
    }

    PcMeterFixture gauge(String name, String topic, int partition, double value) {
        Gauge.builder(name, (Supplier<Number>) () -> value)
                .tags(topicPartitionTags(topic, partition))
                .register(registry);
        return this;
    }

    PcMeterFixture counter(String name, String topic, int partition, double amount) {
        Counter.builder(name).tags(topicPartitionTags(topic, partition)).register(registry).increment(amount);
        return this;
    }

    private static Tags topicPartitionTags(String topic, int partition) {
        return Tags.of("topic", topic, "partition", String.valueOf(partition));
    }

    /**
     * The registry a fully-populated PC would look like: two partitions on one topic, one on another, plus every
     * aggregate, lifecycle and encoding meter.
     */
    static PcMeterFixture fullyPopulated() {
        return new PcMeterFixture()
                .partitionOffsets("orders", 0, 100, 100, 140, 150, 12, 3)
                .partitionCounters("orders", 0, 100, 2, 1)
                .partitionOffsets("orders", 1, 50, 55, 55, 60, 4, 3)
                .partitionCounters("orders", 1, 55, 0, 0)
                .partitionOffsets("payments", 7, 0, 0, 0, 0, 0, 1)
                .partitionCounters("payments", 7, 0, 0, 0)
                .aggregates(9, 31, 4, 40, 16, 2.5d, 1, 3)
                .lifecycle(1, 1)
                .encoding();
    }
}
