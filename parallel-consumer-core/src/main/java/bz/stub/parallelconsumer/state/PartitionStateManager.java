package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2025 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.internal.BrokerPollSystem;
import bz.stub.parallelconsumer.internal.EpochAndRecordsMap;
import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.metrics.PCMetrics;
import bz.stub.parallelconsumer.metrics.PCMetricsDef;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tag;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * In charge of managing {@link PartitionState}s.
 * <p>
 * This state is shared between the {@link BrokerPollSystem} thread and the {@link AbstractParallelEoSStreamProcessor}.
 *
 * @author Antony Stubbs
 * @see PartitionState
 */
// metrics: assigned partitions and their epochs, number of assigned partitions,
@Slf4j
public class PartitionStateManager<K, V> implements ConsumerRebalanceListener {

    public static final double USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT = 0.75;

    /**
     * Best efforts attempt to prevent usage of offset payload beyond X% - as encoding size test is currently only done
     * per batch, we need to leave some buffer for the required space to overrun before hitting the hard limit where we
     * have to drop the offset payload entirely.
     */
    @Getter
    @Setter
    // todo remove static
    private static double USED_PAYLOAD_THRESHOLD_MULTIPLIER = USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT;

    private final ShardManager<K, V> sm;

    /**
     * Hold the tracking state for each of our managed partitions.
     */
    private final Map<TopicPartition, PartitionState<K, V>> partitionStates = new ConcurrentHashMap<>();

    /**
     * Record the generations of partition assignment, for fencing off invalid work.
     * <p>
     * NOTE: This must live outside of {@link PartitionState}, as it must be tracked across partition lifecycles.
     * <p>
     * Starts at zero.
     * <p>
     * NOTE: Must be concurrent because it can be set by one thread, but read by another.
     */
    private final Map<TopicPartition, Long> partitionsAssignmentEpochs = new ConcurrentHashMap<>();

    private final PCModule<K, V> module;

    private Gauge numberOfPartitionsGauge;
    private Gauge totalIncompletesGauge;
    private final Map<TopicPartition, Counter> slowWorkCounters = new HashMap<>();

    private final PCMetrics pcMetrics;

    public PartitionStateManager(PCModule<K, V> module, ShardManager<K, V> sm) {
        this.sm = sm;
        this.module = module;
        this.pcMetrics = module.pcMetrics();
        initMetrics();
    }

    public PartitionState<K, V> getPartitionState(TopicPartition tp) {
        return partitionStates.get(tp);
    }

    private PartitionState<K, V> getPartitionState(EpochAndRecordsMap<K, V>.RecordsAndEpoch recordsAndEpoch) {
        return getPartitionState(recordsAndEpoch.getTopicPartition());
    }

    protected PartitionState<K, V> getPartitionState(WorkContainer<K, V> workContainer) {
        TopicPartition topicPartition = workContainer.getTopicPartition();
        return getPartitionState(topicPartition);
    }

    /**
     * Load offset map for assigned assignedPartitions
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> assignedPartitions) {
        log.debug("Partitions assigned: {}", assignedPartitions);
        log.trace("Epoch map before assignment: {}", partitionsAssignmentEpochs);

        for (final TopicPartition partitionAssignment : assignedPartitions) {
            boolean isAlreadyAssigned = this.partitionStates.containsKey(partitionAssignment);
            if (isAlreadyAssigned) {
                PartitionState<K, V> previouslyAssignedState = partitionStates.get(partitionAssignment);
                if (previouslyAssignedState.isRemoved()) {
                    log.trace("Reassignment of previously revoked partition {} - state: {}", partitionAssignment, previouslyAssignedState);
                } else {
                    log.warn("New assignment of partition which already exists and isn't recorded as removed in " +
                            "partition state. Could be a state bug - was the partition revocation somehow missed, " +
                            "or is this a race? Please file a GH issue. Partition: {}, state: {}", partitionAssignment, previouslyAssignedState);
                }
            }
        }

        incrementPartitionAssignmentEpoch(assignedPartitions);

        try {
            OffsetMapCodecManager<K, V> om = new OffsetMapCodecManager<>(module); // todo remove throw away instance creation - confluentinc#233
            var partitionStates = om.loadPartitionStateForAssignment(assignedPartitions);
            this.partitionStates.putAll(partitionStates);
            initPartitionCounters(assignedPartitions);

            // remove stale work containers after partition epoch changed
            // because we will judge if container is stale or not by comparing between
            // epoch from WorkContainer to partitionsAssignmentEpoch in PartitionState
            long staleContainerCnt = sm.removeStaleContainers();
            log.debug("removed stale container count : {}", staleContainerCnt);
        } catch (Exception e) {
            log.error("Error in onPartitionsAssigned", e);
            throw e;
        }
    }

    private void initPartitionCounters(Collection<TopicPartition> assignedPartitions) {
        assignedPartitions.forEach(topicPartition -> {
            if (!slowWorkCounters.containsKey(topicPartition)) {
                slowWorkCounters.put(topicPartition, pcMetrics
                        .getCounterFromMetricDef(PCMetricsDef.SLOW_RECORDS,
                                Tag.of("topic", topicPartition.topic()),
                                Tag.of("partition", String.valueOf(topicPartition.partition())))
                );
            }
        });
    }

    /**
     * Metrics de-registration for revoked partitions - and it must NEVER throw.
     * <p>
     * This runs inside {@code onPartitionsRevoked}, which runs on the broker-poll thread inside
     * {@code poll()}. The meter registry is usually the USER'S, so this is third-party code on the
     * rebalance path: an exception here escapes the callback and kills the poll thread, which is the
     * only producer of commit responses, so every later commit blocks until it times out. That is the
     * confluentinc#857 family's worst failure shape, reached from a reporting concern.
     * <p>
     * No try/catch here on purpose: {@link PCMetrics#removeMeter} carries the never-throws contract,
     * guarded once at the source because this is one of eleven teardown call sites and a guard at each
     * is a guard someone will miss. A second one here could never fire, and defensive code that cannot
     * fire is worse than none - it implies the contract is doubted. Losing a meter is an acceptable
     * outcome; losing the poll thread is not.
     */
    private void deregisterPartitionCounters(Collection<TopicPartition> removedPartitions) {
        removedPartitions.forEach(topicPartition -> {
            Counter counter = slowWorkCounters.remove(topicPartition);
            if (counter != null) {
                pcMetrics.removeMeter(counter);
            }
        });
    }

    public void incrementSlowWorkCounter(TopicPartition topicPartition) {
        Optional.ofNullable(slowWorkCounters.get(topicPartition)).ifPresent(Counter::increment);
    }

    /**
     * Clear offset map for revoked partitions
     * <p>
     * {@link AbstractParallelEoSStreamProcessor#onPartitionsRevoked} handles committing off offsets upon revoke
     *
     * @see AbstractParallelEoSStreamProcessor#onPartitionsRevoked
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.info("Partitions revoked: {}", partitions);

        try {
            onPartitionsRemoved(partitions);
        } catch (Exception e) {
            log.error("Error in onPartitionsRevoked", e);
            throw e;
        }
    }

    void onPartitionsRemoved(final Collection<TopicPartition> partitions) {
        incrementPartitionAssignmentEpoch(partitions);
        resetOffsetMapAndRemoveWork(partitions);
        deregisterPartitionCounters(partitions);

        // remove stale work containers after partition epoch changed
        // because we will judge if container is stale or not by comparing between
        // epoch from WorkContainer to partitionsAssignmentEpoch in PartitionState
        sm.removeStaleContainers();
    }

    /**
     * Clear offset map for lost partitions
     */
    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        try {
            log.info("Lost partitions: {}", partitions);
            onPartitionsRemoved(partitions);
        } catch (Exception e) {
            log.error("Error in onPartitionsLost", e);
            throw e;
        }
    }

    /**
     * Records that a commit succeeded, for each partition that was committed.
     * <p>
     * Per partition, this delegates to {@link PartitionState#onOffsetCommitSuccess}, which stores the newly committed
     * offset as the partition's last committed offset and marks the partition clean (unless its state changed again
     * while the commit was in flight, in which case it stays dirty and will be committed again).
     * <p>
     * <b>No offsets are discarded here.</b> Earlier versions of this javadoc described truncating tracked offsets below
     * the committed offset once a commit landed. That does not happen, and cannot: {@link PartitionState} tracks only
     * <em>incomplete</em> offsets, and the offset committed is the lowest incomplete one - so there is nothing below it
     * left to throw away.
     * <p>
     * Truncation of tracked state does still exist, but it happens on the <b>bootstrap poll</b> rather than on commit -
     * see {@link PartitionState}'s {@code maybeTruncateBelowOrAbove}, reached from its
     * {@code maybeTruncateOrPruneTrackedOffsets}. That is where records removed by retention or compaction, or a
     * committed offset raised externally, get reconciled against the offsets we track.
     *
     * @param committed the offsets just successfully committed to the broker, by partition
     */
    public void onOffsetCommitSuccess(Map<TopicPartition, OffsetAndMetadata> committed) {
        committed.forEach((tp, meta) -> {
            var partition = getPartitionState(tp);
            partition.onOffsetCommitSuccess(meta);
        });
    }

    /**
     * Remove work from removed partition.
     * <p>
     *
     * <b>On shard removal:</b>
     *
     * <li>{@link  ProcessingOrder#PARTITION} ordering, work shards and partition queues are the same,
     * so remove all from referenced shards
     *
     * <li>{@link ProcessingOrder#KEY} ordering, all records in a shard will be of
     * the same key, so by definition all records with this key should be removed - i.e. the entire shard
     *
     * <li>{@link ProcessingOrder#UNORDERED} ordering, {@link WorkContainer}s go into shards keyed by partition, so
     * falls back to the {@link ProcessingOrder#PARTITION} case
     */
    private void resetOffsetMapAndRemoveWork(Collection<TopicPartition> allRemovedPartitions) {
        for (TopicPartition removedPartition : allRemovedPartitions) {
            // by replacing with a no op implementation, we protect for stale messages still in queues which reference it
            // however it means the map will only grow, but only it's key set
            var partition = this.partitionStates.get(removedPartition);
            partitionStates.put(removedPartition, RemovedPartitionState.getSingleton());

            //
            partition.onPartitionsRemoved(sm);
        }
    }

    /**
     * @return the current epoch of the partition, or null if not yet assigned
     */
    public Long getEpochOfPartition(TopicPartition partition) {
        return partitionsAssignmentEpochs.get(partition);
    }


    private void incrementPartitionAssignmentEpoch(final Collection<TopicPartition> partitions) {
        for (final TopicPartition partition : partitions) {
            Long oldEpoch = partitionsAssignmentEpochs.getOrDefault(partition, PartitionState.KAFKA_OFFSET_ABSENCE);
            Long newEpoch = oldEpoch + 1;
            partitionsAssignmentEpochs.put(partition, newEpoch);
            log.trace("Epoch for {} incremented: {} -> {}", partition, oldEpoch, newEpoch);
        }
    }

    /**
     * Check we have capacity in offset storage to process more messages
     */
    public boolean isAllowedMoreRecords(TopicPartition tp) {
        PartitionState<K, V> partitionState = getPartitionState(tp);
        return partitionState.isAllowedMoreRecords();
    }

    /**
     * @see #isAllowedMoreRecords(TopicPartition)
     */
    public boolean isAllowedMoreRecords(WorkContainer<?, ?> wc) {
        return isAllowedMoreRecords(wc.getTopicPartition());
    }

    public boolean hasIncompleteOffsets() {
        for (var partition : getAssignedPartitions().values()) {
            if (partition.hasIncompleteOffsets())
                return true;
        }
        return false;
    }

    public long getNumberOfIncompleteOffsets() {
        Collection<PartitionState<K, V>> values = getAssignedPartitions().values();
        return values.stream()
                .mapToLong(PartitionState::getNumberOfIncompleteOffsets)
                .reduce(Long::sum)
                .orElse(0);
    }

    public long getHighestSeenOffset(final TopicPartition tp) {
        return getPartitionState(tp).getOffsetHighestSeen();
    }

    public void onSuccess(WorkContainer<K, V> wc) {
        PartitionState<K, V> partitionState = getPartitionState(wc.getTopicPartition());
        partitionState.onSuccess(wc.offset());
    }

    public void onFailure(WorkContainer<K, V> wc) {
        PartitionState<K, V> partitionState = getPartitionState(wc.getTopicPartition());
        partitionState.onFailure(wc);
    }

    /**
     * Takes a record as work and puts it into internal queues, unless it's been previously recorded as completed as per
     * loaded records.
     */
    void maybeRegisterNewRecordAsWork(final EpochAndRecordsMap<K, V> recordsMap) {
        log.debug("Incoming {} new records...", recordsMap.count());
        for (var recordsAndEpoch : recordsMap.getRecordMap().values()) {
            PartitionState<K, V> partitionState = getPartitionState(recordsAndEpoch);
            partitionState.maybeRegisterNewPollBatchAsWork(recordsAndEpoch);
        }
    }

    public Map<TopicPartition, OffsetAndMetadata> collectDirtyCommitData() {
        var dirties = new HashMap<TopicPartition, OffsetAndMetadata>();
        for (var state : getAssignedPartitions().values()) {
            var offsetAndMetadata = state.getCommitDataIfDirty();
            //noinspection ObjectAllocationInLoop
            offsetAndMetadata.ifPresent(andMetadata -> dirties.put(state.getTp(), andMetadata));
        }
        return dirties;
    }

    private Map<TopicPartition, PartitionState<K, V>> getAssignedPartitions() {
        return Collections.unmodifiableMap(this.partitionStates.entrySet().stream()
                .filter(e -> !e.getValue().isRemoved())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    /**
     * @return true if this record be taken from its partition as work.
     */
    public boolean couldBeTakenAsWork(WorkContainer<K, V> workContainer) {
        return getPartitionState(workContainer)
                .couldBeTakenAsWork(workContainer);
    }

    public boolean isDirty() {
        return this.partitionStates.values().stream()
                .anyMatch(PartitionState::isDirty);
    }

    private void initMetrics() {
        numberOfPartitionsGauge = pcMetrics.gaugeFromMetricDef(PCMetricsDef.NUMBER_OF_PARTITIONS, this, pm -> getAssignedPartitions().size());
        totalIncompletesGauge = pcMetrics.gaugeFromMetricDef(PCMetricsDef.INCOMPLETE_OFFSETS_TOTAL,
                this, partitionStateManager -> partitionStateManager.getAssignedPartitions().values().stream()
                        .mapToInt(PartitionState::getNumberOfIncompleteOffsets)
                        .sum()
        );
    }
}
