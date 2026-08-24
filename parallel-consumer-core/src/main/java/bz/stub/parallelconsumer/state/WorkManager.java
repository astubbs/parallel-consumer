package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.*;
import bz.stub.parallelconsumer.metrics.PCMetrics;
import bz.stub.parallelconsumer.metrics.PCMetricsDef;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tag;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;

import static java.lang.Boolean.TRUE;
import static lombok.AccessLevel.PUBLIC;

/**
 * Sharded, prioritised, offset managed, order controlled, delayed work queue.
 * <p>
 * Low Watermark - the highest offset (continuously successful) with all it's previous messages succeeded (the offset
 * one commits to broker)
 * <p>
 * High Water Mark - the highest offset which has succeeded (previous may be incomplete)
 * <p>
 * Highest seen offset - the highest ever seen offset
 * <p>
 * This state is shared between the {@link BrokerPollSystem} thread and the {@link AbstractParallelEoSStreamProcessor}.
 *
 * @author Antony Stubbs
 */
@Slf4j
public class WorkManager<K, V> implements ConsumerRebalanceListener {

    @Getter
    private final ParallelConsumerOptions<K, V> options;

    // todo make private
    @Getter(PUBLIC)
    final PartitionStateManager<K, V> pm;

    // todo make private
    @Getter(PUBLIC)
    private final ShardManager<K, V> sm;

    /**
     * The multiple of {@link ParallelConsumerOptions#getMaxConcurrency()} that should be pre-loaded awaiting
     * processing.
     * <p>
     * We use it here as well to make sure we have a matching number of messages in queues available.
     */
    private final DynamicLoadFactor dynamicLoadFactor;

    @Getter
    private int numberRecordsOutForProcessing = 0;
    private PCModule<K, V> module;
    /**
     * Useful for testing
     */
    @Getter(PUBLIC)
    private final List<Consumer<WorkContainer<K, V>>> successfulWorkListeners = new ArrayList<>();

    private Gauge waitingRecordsNumberGauge;
    private Gauge inflightRecordsNumberGauge;
    private Map<TopicPartition, Counter> succeededRecordsCounters = new HashMap<>();
    private Map<TopicPartition, Counter> failedRecordsCounters = new HashMap<>();

    private final PCMetrics pcMetrics;

    public WorkManager(PCModule<K, V> module,
                       DynamicLoadFactor dynamicExtraLoadFactor) {
        this.module = module;
        this.options = module.options();
        this.dynamicLoadFactor = dynamicExtraLoadFactor;
        this.sm = new ShardManager<>(module, this);
        this.pm = new PartitionStateManager<>(module, sm);
        this.pcMetrics = module.pcMetrics();
        initMetrics();
    }

    /**
     * Load offset map for assigned partitions
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        pm.onPartitionsAssigned(partitions);
        initTopicPartitionSpecificMetrics(partitions);
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
        pm.onPartitionsRevoked(partitions);
        onPartitionsRemoved(partitions);
    }

    /**
     * Clear offset map for lost partitions
     */
    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        pm.onPartitionsLost(partitions);
        onPartitionsRemoved(partitions);
    }

    void onPartitionsRemoved(final Collection<TopicPartition> partitions) {
        deregisterTopicPartitionSpecificMetrics(partitions);
    }

    public void registerWork(EpochAndRecordsMap<K, V> records) {
        pm.maybeRegisterNewRecordAsWork(records);
    }

    /**
     * Get work with no limit on quantity, useful for testing.
     */
    public List<WorkContainer<K, V>> getWorkIfAvailable() {
        return getWorkIfAvailable(Integer.MAX_VALUE);
    }

    /**
     * Depth first work retrieval.
     */
    public List<WorkContainer<K, V>> getWorkIfAvailable(final int requestedMaxWorkToRetrieve) {
        // optimise early
        if (requestedMaxWorkToRetrieve < 1) {
            return UniLists.of();
        }

        //
        var work = sm.getWorkIfAvailable(requestedMaxWorkToRetrieve);

        //
        if (log.isDebugEnabled()) {
            log.debug("Got {} of {} requested records of work. In-flight: {}, Awaiting in commit (partition) queues: {}",
                    work.size(),
                    requestedMaxWorkToRetrieve,
                    getNumberRecordsOutForProcessing(),
                    getNumberOfIncompleteOffsets());
        }
        numberRecordsOutForProcessing += work.size();
        return work;
    }

    public void onSuccessResult(WorkContainer<K, V> wc) {
        onSuccessResult(wc, pm.getPartitionState(wc.getTopicPartition()));
    }

    private void onSuccessResult(WorkContainer<K, V> wc, PartitionState<K, V> partitionState) {
        log.trace("Work success ({}), removing from processing shard queue", wc);

        incrementCounterIfPresent(succeededRecordsCounters, wc.getTopicPartition());

        wc.endFlight();

        // update as we go - against the SAME state object the staleness checkpoint validated, never a second
        // lookup. If a rebalance revoked the partition since that validation, this mutates an object already
        // unlinked from the live map: invisible to the commit path, and the record is redelivered to the
        // partition's next owner (at-least-once). A second lookup here instead dirties the freshly assigned
        // state with a dead epoch's completion - which is the one production route into the bootstrap-reset
        // commit tear (the torn-read family's candidates 3 and 1; both interleavings are pinned in
        // WorkManagerStaleCheckDoubleLookupTest).
        pm.onSuccess(wc, partitionState);
        sm.onSuccess(wc);

        // notify listeners
        successfulWorkListeners.forEach(c -> c.accept(wc));

        numberRecordsOutForProcessing--;
    }

    /**
     * Can run from controller or poller thread, depending on which is responsible for committing
     *
     * @see PartitionStateManager#onOffsetCommitSuccess(Map)
     */
    public void onOffsetCommitSuccess(Map<TopicPartition, OffsetAndMetadata> committed) {
        pm.onOffsetCommitSuccess(committed);
    }

    public void onFailureResult(WorkContainer<K, V> wc) {
        onFailureResult(wc, pm.getPartitionState(wc.getTopicPartition()));
    }

    private void onFailureResult(WorkContainer<K, V> wc, PartitionState<K, V> partitionState) {
        // error occurred, put it back in the queue if it can be retried
        incrementCounterIfPresent(failedRecordsCounters, wc.getTopicPartition());
        wc.endFlight();
        pm.onFailure(wc, partitionState);
        // Re-validate against the LIVE map immediately before the retry re-queue - the staleness checkpoint's
        // answer cannot carry this decision, because a rebalance can complete between there and here. The
        // revoke sweep cleans the retry queue only through shard contents, so a stale add landing after the
        // sweep is permanent: nothing can ever remove the entry, and once its retry delay elapses it reads as
        // ready-to-retry forever - phantom waiting work that gates the broker poller (a confluentinc#857-family
        // stall; the count mechanism is traced in docs/inflight/bug-retry-queue-orphaned-by-inline-stale-removal.md).
        // Skipping the re-queue is the safe direction: the partition's next owner redelivers the record.
        if (checkIfWorkIsStale(wc)) {
            log.debug("Not re-queueing failed work for retry - its partition was revoked mid-flight, so the retry belongs to the partition's next owner. {}", wc);
        } else {
            sm.onFailure(wc);
        }
        numberRecordsOutForProcessing--;
    }

    public long getNumberOfIncompleteOffsets() {
        return pm.getNumberOfIncompleteOffsets();
    }

    public Map<TopicPartition, OffsetAndMetadata> collectCommitDataForDirtyPartitions() {
        return pm.collectDirtyCommitData();
    }

    /**
     * Have our partitions been revoked? Can a batch contain messages of different epochs?
     *
     * @return true if any epoch is stale, false if not
     * @see #checkIfWorkIsStale(WorkContainer)
     */
    public boolean checkIfWorkIsStale(final List<WorkContainer<K, V>> workContainers) {
        for (final WorkContainer<K, V> workContainer : workContainers) {
            if (checkIfWorkIsStale(workContainer)) return true;
        }
        return false;
    }

    /**
     * Have our partitions been revoked?
     *
     * @return true if epoch doesn't match, false if ok
     */
    public boolean checkIfWorkIsStale(WorkContainer<K, V> workContainer) {
        return pm.getPartitionState(workContainer).checkIfWorkIsStale(workContainer);
    }

    /**
     * As {@link #checkIfWorkIsStale(WorkContainer)}, but against a state the caller has ALREADY resolved - so
     * the caller can go on to act against the exact state object the answer was computed from. Checkpoint 3
     * ({@link #handleFutureResult}) depends on that: an answer computed on one lookup with actions running on
     * another is a torn read whenever a rebalance swaps the map entry in between.
     */
    protected boolean checkIfWorkIsStale(PartitionState<K, V> partitionState, WorkContainer<K, V> workContainer) {
        return partitionState.checkIfWorkIsStale(workContainer);
    }

    public boolean shouldThrottle() {
        return isSufficientlyLoaded();
    }

    /**
     * @return true if there's enough messages downloaded from the broker already to satisfy the pipeline, false if more
     *         should be downloaded (or pipelined in the Consumer)
     */
    public boolean isSufficientlyLoaded() {
        long awaitingSelection = getNumberOfWorkQueuedInShardsAwaitingSelection();
        long outForProcessing = getNumberRecordsOutForProcessing();
        long threshold = (long) options.getTargetAmountOfRecordsInFlight() * getLoadingFactor();
        boolean loaded = (awaitingSelection + outForProcessing) > threshold;
        // Silent-stall diagnostic (confluentinc#857): this gates the broker-poller pause/resume. If it stays true while no
        // records are actually flowing, the poller never resumes and the PC stalls. A high outForProcessing with
        // no awaitingSelection and no real progress is the numberRecordsOutForProcessing counter-drift signature.
        // See docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md
        if (log.isDebugEnabled()) {
            log.debug("isSufficientlyLoaded={} (awaitingSelection={} + outForProcessing={} = {} vs target({})*loadingFactor({})={})",
                    loaded, awaitingSelection, outForProcessing, awaitingSelection + outForProcessing,
                    options.getTargetAmountOfRecordsInFlight(), getLoadingFactor(), threshold);
        }
        return loaded;
    }

    private int getLoadingFactor() {
        return dynamicLoadFactor.getCurrentFactor();
    }

    public boolean workIsWaitingToBeProcessed() {
        return sm.workIsWaitingToBeProcessed();
    }

    public boolean hasWorkInFlight() {
        return getNumberRecordsOutForProcessing() != 0;
    }

    public boolean isWorkInFlightMeetingTarget() {
        return getNumberRecordsOutForProcessing() >= options.getTargetAmountOfRecordsInFlight();
    }

    public long getNumberOfWorkQueuedInShardsAwaitingSelection() {
        return sm.getNumberOfWorkQueuedInShardsAwaitingSelection();
    }

    public boolean hasIncompleteOffsets() {
        return pm.hasIncompleteOffsets();
    }

    public boolean isRecordsAwaitingProcessing() {
        return sm.getNumberOfWorkQueuedInShardsAwaitingSelection() > 0;
    }

    public void handleFutureResult(WorkContainer<K, V> wc) {
        // Third of the three staleness checkpoints - see PartitionState#epochIsStale for the scheme.
        // Work that went stale mid-flight never reaches onSuccessResult/onFailureResult, which is what
        // stops a returning stale result removing a FRESH container that replaced it at the same offset.
        //
        // The partition state is resolved ONCE, and the check and the actions share that reference. Two
        // separate lookups here used to be a torn read: a rebalance (broker-poll thread - nothing
        // serialises it against this one, the lock went in confluentinc#219) completing in the gap meant
        // the check validated the OLD state while the actions ran against its replacement. Since no epoch
        // check here can ever be atomic with the actions, the actions are instead made safe by
        // construction: the success path mutates only the validated state object, and the failure path
        // re-validates against the live map at its re-queue decision (see onFailureResult).
        var partitionState = pm.getPartitionState(wc.getTopicPartition());
        if (checkIfWorkIsStale(partitionState, wc)) {
            // no op, partition has been revoked
            log.debug("Work result received, but from an old generation. Dropping work from revoked partition {}", wc);
            wc.endFlight();
            this.numberRecordsOutForProcessing--;
        } else {
            Optional<Boolean> userFunctionSucceeded = wc.getMaybeUserFunctionSucceeded();
            if (userFunctionSucceeded.isPresent()) {
                if (TRUE.equals(userFunctionSucceeded.get())) {
                    onSuccessResult(wc, partitionState);
                } else {
                    onFailureResult(wc, partitionState);
                }
            } else {
                throw new IllegalStateException("Work returned, but without a success flag - report a bug");
            }
        }
    }

    public boolean isNoRecordsOutForProcessing() {
        return getNumberRecordsOutForProcessing() == 0;
    }

    public Optional<Duration> getLowestRetryTime() {
        return sm.getLowestRetryTime();
    }

    public boolean isDirty() {
        return pm.isDirty();
    }

    private void initMetrics() {
        waitingRecordsNumberGauge = pcMetrics.gaugeFromMetricDef(PCMetricsDef.WAITING_RECORDS,
                this, WorkManager::getNumberOfWorkQueuedInShardsAwaitingSelection);
        inflightRecordsNumberGauge = pcMetrics.gaugeFromMetricDef(PCMetricsDef.INFLIGHT_RECORDS,
                this, WorkManager::getNumberRecordsOutForProcessing);
    }

    private void initTopicPartitionSpecificMetrics(Collection<TopicPartition> partitions) {
        partitions.forEach(topicPartition -> {
            if (!succeededRecordsCounters.containsKey(topicPartition)) {
                succeededRecordsCounters.put(topicPartition, pcMetrics.getCounterFromMetricDef(PCMetricsDef.PROCESSED_RECORDS, getWorkManagerCounterTags(topicPartition)));
            }
            if (!failedRecordsCounters.containsKey(topicPartition)) {
                failedRecordsCounters.put(topicPartition, pcMetrics.getCounterFromMetricDef(PCMetricsDef.FAILED_RECORDS, getWorkManagerCounterTags(topicPartition)));
            }
        });
    }

    private void incrementCounterIfPresent(Map<TopicPartition, Counter> counterMap, TopicPartition topicPartition) {
        Optional.ofNullable(counterMap.get(topicPartition)).ifPresent(Counter::increment);
    }

    private Tag[] getWorkManagerCounterTags(TopicPartition topicPartition) {
        return new Tag[]{Tag.of("topic", topicPartition.topic()), Tag.of("partition", String.valueOf(topicPartition.partition()))};
    }

    private void deregisterTopicPartitionSpecificMetrics(Collection<TopicPartition> partitions) {
        partitions.forEach(topicPartition -> {
            Counter counter = succeededRecordsCounters.remove(topicPartition);
            if (counter != null) {
                pcMetrics.removeMeter(counter);
            }
            counter = failedRecordsCounters.remove(topicPartition);
            if (counter != null) {
                pcMetrics.removeMeter(counter);
            }
        });
    }
}
