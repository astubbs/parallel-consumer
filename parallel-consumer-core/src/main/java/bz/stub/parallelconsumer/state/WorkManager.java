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
import io.micrometer.core.instrument.Timer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
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

    /**
     * Atomic because the direct-pull engine increments this from every worker thread as it takes its own work, while
     * the control loop still decrements it on return. It gates the broker poller, so drift in it stalls the consumer
     * while it still looks alive - see {@link #isSufficientlyLoaded()}. Under the default engine only the control
     * loop touches it and a plain {@code int} would do.
     */
    private final AtomicInteger numberRecordsOutForProcessing = new AtomicInteger();

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

    /**
     * How long each record spent inside Parallel Consumer, from arrival to the controller finishing with it.
     * <p>
     * <b>One timer for the whole consumer, deliberately NOT tagged by topic-partition</b>, unlike the two
     * counters above. Two reasons, and the second is the binding one. A percentile histogram per partition is a
     * far heavier meter than a counter - an instance assigned a hundred partitions would carry a hundred of
     * them. And percentiles cannot be merged after the fact: per-partition timers can never be combined into a
     * consumer-wide p99, whereas an operator hunting a slow partition still has the per-partition counters and
     * offset gauges to narrow it with. A per-partition breakdown, if it is ever wanted, has to be an additional
     * meter rather than a re-tagging of this one.
     *
     * @see WorkContainer#getResidenceTime()
     */
    private Timer recordResidenceTimer;

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
        numberRecordsOutForProcessing.addAndGet(work.size());
        return work;
    }

    public void onSuccessResult(WorkContainer<K, V> wc) {
        log.trace("Work success ({}), removing from processing shard queue", wc);

        incrementCounterIfPresent(succeededRecordsCounters, wc.getTopicPartition());
        recordResidenceTime(wc);

        wc.endFlight();

        // update as we go
        pm.onSuccess(wc);
        sm.onSuccess(wc);

        // notify listeners
        successfulWorkListeners.forEach(c -> c.accept(wc));

        numberRecordsOutForProcessing.decrementAndGet();
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
        // error occurred, put it back in the queue if it can be retried
        incrementCounterIfPresent(failedRecordsCounters, wc.getTopicPartition());
        recordResidenceTime(wc);
        wc.endFlight();
        pm.onFailure(wc);
        sm.onFailure(wc);
        numberRecordsOutForProcessing.decrementAndGet();
    }

    /**
     * Work returned with no verdict at all - neither succeeded nor failed. Reached when the process holding a
     * record goes away before reporting on it: the record has to go back into scheduling without the return
     * counting as a processing attempt.
     * <p>
     * Deliberately not {@link #onFailureResult}: that increments the failure counter, records failure history
     * against the partition, and queues a retry the record never earned. The only thing shared is the in-flight
     * bookkeeping, which must net out exactly - {@link #numberRecordsOutForProcessing} gates the broker poller,
     * and drift in it stalls the consumer silently while it still looks alive.
     * <p>
     * Not an entry point. Engines mark the delivery with {@link WorkContainer#markAbandoned(long)} and hand the
     * container back through {@link #handleFutureResult}, which owns the superseded-delivery and revoked-partition
     * checks. Calling this directly skips both, and an increment landing on a revoked shard is never swept.
     */
    void onAbandonedResult(WorkContainer<K, V> wc) {
        if (wc.isNotInFlight()) {
            log.warn("Abandoned work is not in flight, ignoring the return {}", wc);
            return;
        }

        log.debug("Work returned without a verdict, returning to scheduling {}", wc);

        recordResidenceTime(wc);
        wc.endFlight();
        sm.onAbandoned(wc);
        numberRecordsOutForProcessing.decrementAndGet();
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

    public boolean shouldThrottle() {
        return isSufficientlyLoaded();
    }

    /**
     * @return true if there's enough messages downloaded from the broker already to satisfy the pipeline, false if more
     *         should be downloaded (or pipelined in the Consumer)
     */
    public boolean isSufficientlyLoaded() {
        long workable = getNumberOfWorkableRecordsInSystem();
        // The LIVE admission target under active ENFORCE, today's static derivation otherwise - so a contracted
        // target pauses the poller sooner. The load-factor multiplication stays even in adaptive mode (the plan's
        // KTD10): this threshold is buffer arithmetic - how deep a stock of polled records to hold - not dispatch
        // arithmetic, which consumes the live target un-multiplied.
        long targetRecords = module.admissionTargetRecords();
        long threshold = targetRecords * getLoadingFactor();
        boolean loaded = workable > threshold;
        // Silent-stall diagnostic (confluentinc#857): this gates the broker-poller pause/resume. If it stays true while
        // no records are actually flowing, the poller never resumes and the PC stalls. Because the figure below is
        // derived by conservation, "stays true with nothing flowing" now means records really are being held and not
        // finished with - a leak in the shards - rather than possibly just a counter that has drifted.
        // See docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md
        if (log.isDebugEnabled()) {
            log.debug("isSufficientlyLoaded={} (inShards={} - parkedForRetry={} = {} vs target({})*loadingFactor({})={})",
                    loaded, sm.getNumberOfRecordsInShards(), sm.getNumberOfRecordsParkedForRetry(), workable,
                    targetRecords, getLoadingFactor(), threshold);
        }
        return loaded;
    }

    /**
     * How many records the system is holding that it can actually make progress on - the figure that gates record
     * intake from the broker.
     * <p>
     * <b>Derived by conservation, not counted.</b> The gate only ever wanted the <em>sum</em> of "queued in shards"
     * and "out for processing", never the split, and that sum is simply "records inside the system": everything
     * admitted from the broker that has not yet been finished with. {@link ShardManager} keeps that as
     * {@code admitted - retired} over the one collection that holds the records, which is why it cannot drift the
     * way the two separately-maintained counters it replaces could - a defect the previous implementation
     * acknowledged with a clamp on one of them.
     * <p>
     * Records waiting out a retry delay are excluded: they occupy the buffer but no amount of worker capacity can
     * advance them, so a consumer whose whole buffer is in back-off should keep fetching rather than idle.
     * <p>
     * The one thing this does <em>not</em> count, which the old expression did, is a record whose partition was
     * revoked while it was still out at a worker. That record has been dropped from the shards and its result will
     * be discarded on return, so counting it as loaded only ever delayed a fetch.
     */
    public long getNumberOfWorkableRecordsInSystem() {
        return sm.getNumberOfRecordsInShards() - sm.getNumberOfRecordsParkedForRetry();
    }

    private int getLoadingFactor() {
        return dynamicLoadFactor.getCurrentFactor();
    }

    public boolean workIsWaitingToBeProcessed() {
        return sm.workIsWaitingToBeProcessed();
    }

    /**
     * @see ShardManager#getUpperBoundOnSelectableWork()
     */
    public long getUpperBoundOnSelectableWork() {
        return sm.getUpperBoundOnSelectableWork();
    }

    /**
     * @return the number of records currently handed out for processing and not yet returned
     */
    public int getNumberRecordsOutForProcessing() {
        return numberRecordsOutForProcessing.get();
    }

    public boolean hasWorkInFlight() {
        return getNumberRecordsOutForProcessing() != 0;
    }

    /**
     * Deliberately reads the PINNED (static, ceiling-derived) target, never the live admission target - the plan's
     * KTD7 "bounded pin". Sole reader is the control loop's block-time arithmetic
     * ({@code AbstractParallelEoSStreamProcessor#timeToBlockFor()}): a live read here would let a contracted target
     * slow the loop's wake-ups, and with them the admission controller's own recovery cadence. The cost of the pin
     * is bounded at that reader, not here.
     */
    public boolean isWorkInFlightMeetingTarget() {
        return getNumberRecordsOutForProcessing() >= options.getTargetAmountOfRecordsInFlight();
    }

    public long getNumberOfWorkQueuedInShardsAwaitingSelection() {
        return sm.getNumberOfWorkQueuedInShardsAwaitingSelection();
    }

    /**
     * @see ShardManager#getNumberOfRecordsInShards()
     */
    public long getNumberOfRecordsInShards() {
        return sm.getNumberOfRecordsInShards();
    }

    public boolean hasIncompleteOffsets() {
        return pm.hasIncompleteOffsets();
    }

    /**
     * @see PartitionStateManager#isAnyPartitionBlocked()
     */
    public boolean isAnyPartitionBlocked() {
        return pm.isAnyPartitionBlocked();
    }

    public boolean isRecordsAwaitingProcessing() {
        return sm.getNumberOfWorkQueuedInShardsAwaitingSelection() > 0;
    }

    /**
     * Control thread only. Results must reach here through the controller's mailbox rather than being applied on
     * whichever thread finished the work: the decrements here have to be ordered against the partition and shard
     * state they accompany, which is not thread safe, and {@link #numberRecordsOutForProcessing} gates the broker
     * poller. The direct-pull engine changes only who *takes* work, never who returns it.
     */
    public void handleFutureResult(WorkContainer<K, V> wc) {
        // Must come before the stale-partition branch, which decrements unconditionally. An abandoned record is
        // immediately re-selectable and the control loop re-selects in the same iteration it drains returns, so
        // a late duplicate would otherwise end a live delivery's flight and decrement a second time.
        if (wc.isReturnForSupersededDelivery()) {
            log.debug("Ignoring a verdict-free return for a delivery that has already ended {}", wc);
            return;
        }

        // Third of the three staleness checkpoints - see PartitionState#epochIsStale for the scheme.
        // Work that went stale mid-flight never reaches onSuccessResult/onFailureResult, which is what
        // stops a returning stale result removing a FRESH container that replaced it at the same offset.
        if (checkIfWorkIsStale(wc)) {
            // no op, partition has been revoked
            log.debug("Work result received, but from an old generation. Dropping work from revoked partition {}", wc);
            wc.endFlight();
            this.numberRecordsOutForProcessing.decrementAndGet();
        } else {
            Optional<Boolean> userFunctionSucceeded = wc.getMaybeUserFunctionSucceeded();
            if (userFunctionSucceeded.isPresent()) {
                if (TRUE.equals(userFunctionSucceeded.get())) {
                    onSuccessResult(wc);
                    recordAdmissionOutcome(wc, true);
                } else {
                    onFailureResult(wc);
                    recordAdmissionOutcome(wc, false);
                }
            } else if (wc.isAbandonedForCurrentDelivery()) {
                onAbandonedResult(wc);
            } else {
                throw new IllegalStateException("Work returned, but without a success flag - report a bug");
            }
        }
    }

    /**
     * The admission controller's OUTCOME tap - this method sits on the one verdict-carrying completion path, on the
     * control thread, AFTER the superseded-delivery and stale-partition filters above, so late duplicates and
     * revoked-partition returns never count. Verdict-free returns (abandoned work) carry no outcome either: the
     * delivery will run again, and only its eventual verdict is signal.
     * <p>
     * Retry attempts DO route through here - a retry's failure counts as failure signal even though its latency
     * never enters the service-time window (that exclusion is the sampler's,
     * {@code AbstractParallelEoSStreamProcessor#admissionServiceTimeSampleNanos}). Classification lives behind
     * {@link bz.stub.parallelconsumer.internal.admission.AdmissionController#recordCompletion(boolean, Throwable)}.
     * Pure tap: reads only; gated on {@link PCModule#adaptiveSignalsActive()} so an engine that refused the mode
     * feeds nothing.
     */
    private void recordAdmissionOutcome(WorkContainer<K, V> wc, boolean succeeded) {
        if (!module.adaptiveSignalsActive()) {
            return;
        }
        Optional<Throwable> failureReason = succeeded ? Optional.empty() : wc.getLastFailureReason();
        module.admissionController().recordCompletion(succeeded, failureReason == null ? null : failureReason.orElse(null));
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
        recordResidenceTimer = pcMetrics.getTimerFromMetricDef(PCMetricsDef.RECORD_RESIDENCE_TIME);
    }

    /**
     * Records how long this record has been inside Parallel Consumer, at the moment the controller finishes with
     * the delivery.
     * <p>
     * <b>Called from every outcome - success, failure and abandonment - not only success.</b> A record that is
     * failing and being retried is precisely the case a residence-time metric exists to expose, and sampling only
     * the successes would make the distribution look best exactly when the engine is doing worst.
     * <p>
     * The instant taken is the CONTROLLER's, not the worker's {@link WorkContainer#getSucceededAt()}. The hop
     * from the worker's mailbox back to the control loop is Parallel Consumer's own queueing, so it belongs
     * inside the number rather than outside it - and taking it here is the one point that is common to all three
     * outcomes, two of which stamp no completion instant at all.
     */
    private void recordResidenceTime(WorkContainer<K, V> wc) {
        if (recordResidenceTimer != null) {
            recordResidenceTimer.record(wc.getResidenceTime());
        }
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
