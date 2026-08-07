package io.confluent.parallelconsumer.dashboard.snapshot;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.internal.State;
import io.confluent.parallelconsumer.metrics.PCMetricsDef;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Reads Parallel Consumer's published meters out of a {@link MeterRegistry} and shapes them into the snapshot value
 * types.
 * <p>
 * <strong>Every method here must be called on the control thread.</strong> Reading a {@link Gauge} <em>evaluates</em>
 * it, on the calling thread - that is the mechanism behind confluentinc/parallel-consumer#618, where a Prometheus
 * scrape thread evaluated a PC gauge and got {@code ConcurrentModificationException: KafkaConsumer is not safe for
 * multi-threaded access}. {@link SnapshotPublisher} is what guarantees the calling thread is the right one.
 * <p>
 * <strong>One pass over the registry per sample.</strong> Every sampling method takes the {@link MeterIndex} rather
 * than reaching for the registry itself, and {@link StateSampler#sample()} builds exactly one of those per pass. That
 * is deliberate and is the reason there is no no-argument overload to fall back to: this runs on the loop that can
 * turn over thousands of times a second, and a per-lookup registry scan is precisely the cost this module exists not
 * to impose. See {@link MeterIndex} for what a {@code Search} call actually costs.
 * <p>
 * Meter names are taken from {@link PCMetricsDef} rather than written out as string literals here, so a rename in
 * core breaks this module's compilation instead of silently turning every value absent.
 * <p>
 * <strong>Absent, NaN and zero are three different things.</strong> A meter that is not in the registry yields
 * {@code null}. A gauge whose value is {@code NaN} - which is what Micrometer reports once the object a gauge weakly
 * references has been collected - also yields {@code null}, because it is a missing reading rather than a measured
 * one. Only a real reading of zero yields zero.
 * <p>
 * Experimental: the dashboard module is opt-in and its API may change without notice.
 */
@InterfaceStability.Unstable
public class MeterSource {

    private static final String TOPIC_TAG = "topic";

    private static final String PARTITION_TAG = "partition";

    /**
     * The tag {@code pc.offsets.encoding.usage} counters actually carry. Note this differs from the tag key
     * {@link PCMetricsDef} documents for that meter ({@code codec}) - that metadata feeds the generated metrics
     * reference only, while {@code OffsetMapCodecManager} tags the live counter {@code encoding}. The live tag is
     * what has to be read.
     */
    private static final String ENCODING_TAG = "encoding";

    private static final Map<Integer, String> STATE_NAMES_BY_VALUE = stateNamesByValue();

    private final MeterRegistry registry;

    public MeterSource(MeterRegistry registry) {
        this.registry = registry;
    }

    /**
     * Indexes the registry once, for one sample's worth of reads. Every other method here takes the result.
     */
    public MeterIndex index() {
        return MeterIndex.of(registry);
    }

    /**
     * Whether any of Parallel Consumer's own meters are present. Distinguishes "PC has not bound its meters yet, or
     * no registry was supplied" from "PC is genuinely idle", which the page must not conflate.
     */
    public boolean isPopulated(MeterIndex index) {
        return index.hasPcMeters();
    }

    /**
     * One row per topic-partition, built by grouping every {@code topic}/{@code partition}-tagged meter family into
     * the row for its partition. Ordered by topic then partition so the page's table keeps a stable row order.
     */
    public List<PartitionSnapshot> samplePartitions(MeterIndex index) {
        Map<TopicPartition, PartitionSnapshot.PartitionSnapshotBuilder> rows = new HashMap<>();

        gaugePerPartition(index, rows, PCMetricsDef.PARTITION_HIGHEST_SEEN_OFFSET,
                PartitionSnapshot.PartitionSnapshotBuilder::highestSeenOffset);
        gaugePerPartition(index, rows, PCMetricsDef.PARTITION_HIGHEST_COMPLETED_OFFSET,
                PartitionSnapshot.PartitionSnapshotBuilder::highestCompletedOffset);
        gaugePerPartition(index, rows, PCMetricsDef.PARTITION_HIGHEST_SEQUENTIAL_SUCCEEDED_OFFSET,
                PartitionSnapshot.PartitionSnapshotBuilder::highestSequentialSucceededOffset);
        gaugePerPartition(index, rows, PCMetricsDef.PARTITION_LAST_COMMITTED_OFFSET,
                PartitionSnapshot.PartitionSnapshotBuilder::lastCommittedOffset);
        gaugePerPartition(index, rows, PCMetricsDef.PARTITION_INCOMPLETE_OFFSETS,
                PartitionSnapshot.PartitionSnapshotBuilder::incompleteOffsets);
        gaugePerPartition(index, rows, PCMetricsDef.PARTITION_ASSIGNMENT_EPOCH,
                PartitionSnapshot.PartitionSnapshotBuilder::assignmentEpoch);

        counterPerPartition(index, rows, PCMetricsDef.PROCESSED_RECORDS,
                PartitionSnapshot.PartitionSnapshotBuilder::processedRecords);
        counterPerPartition(index, rows, PCMetricsDef.FAILED_RECORDS,
                PartitionSnapshot.PartitionSnapshotBuilder::failedRecords);
        counterPerPartition(index, rows, PCMetricsDef.SLOW_RECORDS,
                PartitionSnapshot.PartitionSnapshotBuilder::slowRecords);

        List<TopicPartition> keys = new ArrayList<>(rows.keySet());
        keys.sort(Comparator.comparing(TopicPartition::topic).thenComparingInt(TopicPartition::partition));

        List<PartitionSnapshot> result = new ArrayList<>(keys.size());
        for (TopicPartition key : keys) {
            result.add(rows.get(key).build());
        }
        return result;
    }

    /**
     * Instance-wide work state from the aggregate gauges.
     */
    public WorkSnapshot sampleWork(MeterIndex index) {
        return WorkSnapshot.builder()
                .inflightRecords(gaugeAsLong(index, PCMetricsDef.INFLIGHT_RECORDS))
                .waitingRecords(gaugeAsLong(index, PCMetricsDef.WAITING_RECORDS))
                .shards(gaugeAsLong(index, PCMetricsDef.NUMBER_OF_SHARDS))
                .shardsSize(gaugeAsLong(index, PCMetricsDef.SHARDS_SIZE))
                .incompleteOffsetsTotal(gaugeAsLong(index, PCMetricsDef.INCOMPLETE_OFFSETS_TOTAL))
                .pausedPartitions(gaugeAsLong(index, PCMetricsDef.NUM_PAUSED_PARTITIONS))
                .numberOfPartitions(gaugeAsLong(index, PCMetricsDef.NUMBER_OF_PARTITIONS))
                .dynamicLoadFactor(gaugeValue(index, PCMetricsDef.DYNAMIC_EXTRA_LOAD_FACTOR))
                .build();
    }

    /**
     * Controller and poller run state, mapped from the numeric gauges back to {@link State} names.
     * <p>
     * Returns a builder rather than a finished value because {@link DirectStateSource} contributes the remaining
     * fields - the composition happens once, in {@link StateSampler}.
     */
    public LifecycleSnapshot.LifecycleSnapshotBuilder sampleLifecycle(MeterIndex index) {
        Integer status = gaugeAsInteger(index, PCMetricsDef.PC_STATUS);
        Integer pollerStatus = gaugeAsInteger(index, PCMetricsDef.PC_POLLER_STATUS);
        return LifecycleSnapshot.builder()
                .stateValue(status)
                .state(stateName(status))
                .pollerStateValue(pollerStatus)
                .pollerState(stateName(pollerStatus));
    }

    /**
     * Offset-encoding timings, codec usage and payload-size ratios.
     */
    public EncodingSnapshot sampleEncoding(MeterIndex index) {
        EncodingSnapshot.EncodingSnapshotBuilder builder = EncodingSnapshot.builder();

        Timer encodingTimer = index.first(PCMetricsDef.OFFSETS_ENCODING_TIME, Timer.class);
        if (encodingTimer != null) {
            builder.encodingCount(encodingTimer.count())
                    .encodingTotalTimeMillis(sane(encodingTimer.totalTime(TimeUnit.MILLISECONDS)))
                    .encodingMaxTimeMillis(sane(encodingTimer.max(TimeUnit.MILLISECONDS)));
        }

        builder.usageByEncoding(sampleEncodingUsage(index));

        DistributionSummary metadataSpace = index.first(PCMetricsDef.METADATA_SPACE_USED, DistributionSummary.class);
        if (metadataSpace != null) {
            builder.metadataSpaceUsedCount(metadataSpace.count())
                    .metadataSpaceUsedMean(sane(metadataSpace.mean()))
                    .metadataSpaceUsedMax(sane(metadataSpace.max()));
        }

        DistributionSummary payloadRatio = index.first(PCMetricsDef.PAYLOAD_RATIO_USED, DistributionSummary.class);
        if (payloadRatio != null) {
            builder.payloadRatioUsedCount(payloadRatio.count())
                    .payloadRatioUsedMean(sane(payloadRatio.mean()))
                    .payloadRatioUsedMax(sane(payloadRatio.max()));
        }

        return builder.build();
    }

    private Map<String, Double> sampleEncodingUsage(MeterIndex index) {
        Map<String, Double> usage = new LinkedHashMap<>();
        List<Counter> ordered = index.all(PCMetricsDef.OFFSETS_ENCODING_USAGE, Counter.class);
        // deterministic order, so the page's legend does not reshuffle between ticks
        ordered.sort(Comparator.comparing(c -> String.valueOf(c.getId().getTag(ENCODING_TAG))));
        for (Counter counter : ordered) {
            String encoding = counter.getId().getTag(ENCODING_TAG);
            if (encoding == null) {
                continue;
            }
            Double count = sane(counter.count());
            if (count != null) {
                usage.put(encoding, count);
            }
        }
        return usage;
    }

    private void gaugePerPartition(MeterIndex index,
                                   Map<TopicPartition, PartitionSnapshot.PartitionSnapshotBuilder> rows,
                                   PCMetricsDef def,
                                   BiConsumer<PartitionSnapshot.PartitionSnapshotBuilder, Long> setter) {
        for (Gauge gauge : index.all(def, Gauge.class)) {
            PartitionSnapshot.PartitionSnapshotBuilder row = rowFor(rows, gauge);
            if (row == null) {
                continue;
            }
            Double value = sane(gauge.value());
            if (value != null) {
                setter.accept(row, (long) (double) value);
            }
        }
    }

    private void counterPerPartition(MeterIndex index,
                                     Map<TopicPartition, PartitionSnapshot.PartitionSnapshotBuilder> rows,
                                     PCMetricsDef def,
                                     BiConsumer<PartitionSnapshot.PartitionSnapshotBuilder, Double> setter) {
        for (Counter counter : index.all(def, Counter.class)) {
            PartitionSnapshot.PartitionSnapshotBuilder row = rowFor(rows, counter);
            if (row == null) {
                continue;
            }
            Double value = sane(counter.count());
            if (value != null) {
                setter.accept(row, value);
            }
        }
    }

    /**
     * The row a partition-tagged meter belongs to, created on first sight. Returns null - meaning "skip this meter" -
     * for a meter that is missing either tag or whose partition tag is not a number, rather than inventing a row
     * keyed on a guess.
     */
    private PartitionSnapshot.PartitionSnapshotBuilder rowFor(
            Map<TopicPartition, PartitionSnapshot.PartitionSnapshotBuilder> rows, Meter meter) {
        String topic = meter.getId().getTag(TOPIC_TAG);
        String partition = meter.getId().getTag(PARTITION_TAG);
        if (topic == null || partition == null) {
            return null;
        }
        int partitionNumber;
        try {
            partitionNumber = Integer.parseInt(partition.trim());
        } catch (NumberFormatException e) {
            return null;
        }
        TopicPartition key = new TopicPartition(topic, partitionNumber);
        PartitionSnapshot.PartitionSnapshotBuilder existing = rows.get(key);
        if (existing == null) {
            existing = PartitionSnapshot.builder().topic(topic).partition(partitionNumber);
            rows.put(key, existing);
        }
        return existing;
    }

    private Double gaugeValue(MeterIndex index, PCMetricsDef def) {
        Gauge gauge = index.first(def, Gauge.class);
        return gauge == null ? null : sane(gauge.value());
    }

    private Long gaugeAsLong(MeterIndex index, PCMetricsDef def) {
        Double value = gaugeValue(index, def);
        return value == null ? null : (long) (double) value;
    }

    private Integer gaugeAsInteger(MeterIndex index, PCMetricsDef def) {
        Double value = gaugeValue(index, def);
        return value == null ? null : (int) (double) value;
    }

    /**
     * NaN and the infinities are missing readings, not measurements - collapse them to absent so nothing downstream
     * has to decide what a NaN offset means.
     */
    private static Double sane(double value) {
        return (Double.isNaN(value) || Double.isInfinite(value)) ? null : value;
    }

    /**
     * Name for a {@link State#getValue()}, or null if the number matches no known state - which is what happens when
     * core gains a state this module has not been rebuilt against. Absent beats a wrong label.
     */
    static String stateName(Integer value) {
        return value == null ? null : STATE_NAMES_BY_VALUE.get(value);
    }

    private static Map<Integer, String> stateNamesByValue() {
        Map<Integer, String> names = new HashMap<>();
        for (State state : State.values()) {
            names.put(state.getValue(), state.name());
        }
        return names;
    }
}
