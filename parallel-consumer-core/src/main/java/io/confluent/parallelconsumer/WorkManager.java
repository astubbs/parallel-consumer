package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.csid.utils.LoopingResumingIterator;
import io.confluent.csid.utils.Range;
import io.confluent.csid.utils.WallClock;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.MDC;
import org.slf4j.event.Level;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static io.confluent.csid.utils.BackportUtils.toSeconds;
import static io.confluent.csid.utils.KafkaUtils.toTP;
import static io.confluent.csid.utils.LogUtils.at;
import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static java.lang.Math.abs;
import static java.lang.Math.min;
import static lombok.AccessLevel.PACKAGE;

/**
 * Sharded, prioritised, offset managed, order controlled, delayed work queue.
 * <p>
 * Low Water Mark - the highest offset (continuously successful) with all it's previous messages succeeded (the offset
 * one commits to broker)
 * <p>
 * High Water Mark - the highest offset which has succeeded (previous may be incomplete)
 * <p>
 * Highest seen offset - the highest ever seen offset
 *
 * @param <K>
 * @param <V>
 */
@Slf4j
public class WorkManager<K, V> implements ConsumerRebalanceListener {

    @Getter
    private final ParallelConsumerOptions options;

//    private final BackoffAnalyser backoffer;

    /**
     * todo docs The multiple that should be pre-loaded awaiting processing. Consumer already pipelines, so we shouldn't
     * need to pipeline ourselves too much.
     * todo docs
     * The multiple that should be pre-loaded awaiting processing. Consumer already pipelines, so we shouldn't need to
     * pipeline ourselves too much.
     * <p>
     * Note how this relates to {@link BrokerPollSystem#getLongPollTimeout()} - if longPollTimeout is high and loading
     * factor is low, there may not be enough messages queued up to satisfy demand.
     */
    private final DynamicLoadFactor dynamicLoadFactor;

    private final WorkMailBoxManager<K, V> wmbm;

    /**
     * Iteration resume point, to ensure fairness (prevent shard starvation) when we can't process messages from every
     * shard.
     */
    private Optional<Object> iterationResumePoint = Optional.empty();

    @Getter
    private int numberRecordsOutForProcessing = 0;

    /**
     * Useful for testing
     */
    @Getter(PACKAGE)
    private final List<Consumer<WorkContainer<K, V>>> successfulWorkListeners = new ArrayList<>();

    @Setter(PACKAGE)
    private WallClock clock = new WallClock();

    ConsumerManager consumerMgr;

    // visible for testing
    long MISSING_HIGHEST_SEEN = -1L;

    /**
     * Get's set to true whenever work is returned completed, so that we know when a commit needs to be made.
     * <p>
     * In normal operation, this probably makes very little difference, as typical commit frequency is 1 second, so low
     * chances no work has completed in the last second.
     */
    private AtomicBoolean workStateIsDirtyNeedsCommitting = new AtomicBoolean(false);

    private PartitionMonitor pm;

    // TODO remove
    public WorkManager(ParallelConsumerOptions options, ConsumerManager consumer) {
        this(options, consumer, new DynamicLoadFactor());
    }

    public WorkManager(final ParallelConsumerOptions newOptions, final ConsumerManager consumer, final DynamicLoadFactor dynamicExtraLoadFactor) {
        this.options = newOptions;
        this.consumerMgr = consumer;
        this.dynamicLoadFactor = dynamicExtraLoadFactor;
        this.wmbm = new WorkMailBoxManager<K, V>();

        //        backoffer = new BackoffAnalyser(options.getMaxConcurrency() * 10);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

        pm.onPartitionsAssined(partitions);
    }


    private List<TopicPartition> partitionsToRemove = new ArrayList<>();



    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        pm.onPartitionsRevoked(partitions);
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        pm.onPartitionsLost(partitions);
    }

    /**
     * Called by other threads (broker poller) to be later removed inline by control.
     */
    private void registerPartitionsToBeRemoved(Collection<TopicPartition> partitions) {
        partitionsToRemove.addAll(partitions);
    }

    public void registerWork(List<ConsumerRecords<K, V>> records) {
        for (var record : records) {
            registerWork(record);
        }
    }

    /**
     * @see WorkMailBoxManager#registerWork(ConsumerRecords)
     */
    public void registerWork(ConsumerRecords<K, V> records) {
        wmbm.registerWork(records);
    }

    private void processInbox(final int requestedMaxWorkToRetrieve) {
        wmbm.processInbox(requestedMaxWorkToRetrieve);

    }


    private void prepareContinuousEncoder(final WorkContainer<K, V> wc) {
        TopicPartition tp = wc.getTopicPartition();
        if (!partitionContinuousOffsetEncoders.containsKey(tp)) {
            OffsetSimultaneousEncoder encoder = new OffsetSimultaneousEncoder(partitionOffsetHighestContinuousSucceeded.get(tp), partitionOffsetHighestSucceeded.get(tp));
            partitionContinuousOffsetEncoders.put(tp, encoder);
        }
    }

    private void checkHighestSucceededSoFar(final WorkContainer<K, V> wc) {
        // preivous record must be completed if we've never seen this before
        partitionOffsetHighestSucceeded.putIfAbsent(wc.getTopicPartition(), wc.offset() - 1);
    }

    /**
     * If we've never seen a record for this partition before, it must be our first ever seen record for this partition,
     * which means by definition, it's previous offset is the low water mark.
     */
    private void checkPreviousLowWaterMarks(final WorkContainer<K, V> wc) {
        long previousLowWaterMark = wc.offset() - 1;
        partitionOffsetHighestContinuousSucceeded.putIfAbsent(wc.getTopicPartition(), previousLowWaterMark);
    }

    void raisePartitionHighestSeen(long seenOffset, TopicPartition tp) {
        // rise the high water mark
        Long oldHighestSeen = partitionOffsetHighestSeen.getOrDefault(tp, MISSING_HIGHEST_SEEN);
        if (seenOffset > oldHighestSeen || seenOffset == MISSING_HIGHEST_SEEN) {
            partitionOffsetHighestSeen.put(tp, seenOffset);
        }
    }

    private Object computeShardKey(ConsumerRecord<K, V> rec) {
        return switch (options.getOrdering()) {
            case KEY -> rec.key();
            case PARTITION, UNORDERED -> new TopicPartition(rec.topic(), rec.partition());
        };
    }

    /**
     * @return the maximum amount of work possible
     */
    public <R> List<WorkContainer<K, V>> maybeGetWork() {
        return maybeGetWork(Integer.MAX_VALUE);
    }

    public void onSuccess(WorkContainer<K, V> wc) {
        //
//        successRatePer5Seconds.newEvent();
//        successRatePer5SecondsEMA.

        //
        workStateIsDirtyNeedsCommitting.set(true);

        //
        // update as we go
        updateHighestSucceededOffsetSoFar(wc);

        //
        ConsumerRecord<K, V> cr = wc.getCr();
        log.trace("Work success ({}), removing from processing shard queue", wc);
        wc.succeed();

        //
        Object key = computeShardKey(cr);

        // remove from processing queues
        NavigableMap<Long, WorkContainer<K, V>> shard = processingShards.get(key);
        long offset = cr.offset();
        shard.remove(offset);

        // If using KEY ordering, where the shard key is a message key, garbage collect old shard keys (i.e. KEY ordering we may never see a message for this key again)
        boolean keyOrdering = options.getOrdering().equals(KEY);
        if (keyOrdering && shard.isEmpty()) {
            log.trace("Removing empty shard (key: {})", key);
            processingShards.remove(key);
        }

        //
        successfulWorkListeners.forEach((c) -> c.accept(wc)); // notify listeners
        numberRecordsOutForProcessing--;

        encodeWorkResult(true, wc);

        // remove work from partition commit queue
        log.trace("Removing {} from partition queue", wc.offset());
        partitionCommitQueues.get(wc.getTopicPartition()).remove(wc.offset());
    }

    public void onResultBatch(final Set<WorkContainer<K, V>> results) {
        //
        if (!results.isEmpty()) {
            onResultUpdatePartitionRecordsBatch(results);
        }

        //
        manageOffsetEncoderSpaceRequirements();


//        // individual
//        for (var work : results) {
//            handleFutureResult(work);
//        }
    }

    protected void handleFutureResult(WorkContainer<K, V> wc) {
        MDC.put("offset", wc.toString());
        TopicPartition tp = wc.getTopicPartition();
        if (wc.getEpoch() < partitionsAssignmentEpochs.get(tp)) {
            log.warn("message assigned from old epoch, ignore: {}", wc);
            return;
        }
        if (wc.isUserFunctionSucceeded()) {
            onSuccess(wc);
        } else {
            onFailure(wc);
        }
        MDC.clear();
    }

    /**
     * Rin algorithms that benefit from seeing chunks of work results
     */
    private void onResultUpdatePartitionRecordsBatch(final Set<WorkContainer<K, V>> results) {
        //
        onResultUpdateHighestContinuousBatch(results);
    }

    private void onResultUpdatePartitionRecords(WorkContainer<K, V> work) {
        TopicPartition tp = work.getTopicPartition();

        if (work.isUserFunctionSucceeded()) {

        } else {
            // no op?

            // this is only recorded in the encoders
            // partitionOffsetsIncompleteMetadataPayloads;
        }
    }

    /**
     * Make encoders add new work result to their encodings
     * <p>
     * todo refactor to offset manager?
     */
    private void encodeWorkResult(final boolean offsetComplete, final WorkContainer<K, V> wc) {
        TopicPartition tp = wc.getTopicPartition();
        long lowWaterMark = partitionOffsetHighestContinuousSucceeded.get(tp);
        Long highestCompleted = partitionOffsetHighestSucceeded.get(tp);

        long nextExpectedOffsetFromBroker = lowWaterMark + 1;

        OffsetSimultaneousEncoder offsetSimultaneousEncoder = partitionContinuousOffsetEncoders.get(tp);

        long offset = wc.offset();

        // give encoders chance to truncate
        offsetSimultaneousEncoder.maybeReinitialise(nextExpectedOffsetFromBroker, highestCompleted);

        if (offset <= nextExpectedOffsetFromBroker) {
            // skip - nothing to encode
            return;
        }

        long relativeOffset = offset - nextExpectedOffsetFromBroker;
        if (relativeOffset < 0) {
//            throw new InternalRuntimeError(msg("Relative offset negative {}", relativeOffset));
            log.trace("Offset {} now below low water mark {}, no need to encode", offset, lowWaterMark);
            return;
        }

        if (offsetComplete)
            offsetSimultaneousEncoder.encodeCompleteOffset(nextExpectedOffsetFromBroker, relativeOffset, highestCompleted);
        else
            offsetSimultaneousEncoder.encodeIncompleteOffset(nextExpectedOffsetFromBroker, relativeOffset, highestCompleted);
    }

    private int getMetadataSpaceAvailablePerPartition() {
        int defaultMaxMetadataSize = OffsetMapCodecManager.DefaultMaxMetadataSize;
        // TODO what else is the overhead in b64 encoding?
        int maxMetadataSize = defaultMaxMetadataSize - OffsetEncoding.standardOverhead;
        if (numberOfAssignedPartitions == 0) {
            // no partitions assigned - all available
            return maxMetadataSize;
//            throw new InternalRuntimeError("Nothing assigned");
        }
        int perPartition = maxMetadataSize / numberOfAssignedPartitions;
        return perPartition;
    }

    public void onFailure(WorkContainer<K, V> wc) {
        //
        wc.fail(clock);

        //
        putBack(wc);

        //
        encodeWorkResult(false, wc);
    }

    /**
     * Idempotent - work may have not been removed, either way it's put back
     */
    private void putBack(WorkContainer<K, V> wc) {
        log.debug("Work FAILED, returning to shard");
        ConsumerRecord<K, V> cr = wc.getCr();
        Object key = computeShardKey(cr);
        var shard = processingShards.get(key);
        long offset = wc.getCr().offset();
        shard.put(offset, wc);
        numberRecordsOutForProcessing--;
    }


    /**
     * @return Work count in mailbox plus work added to the processing shards
     */
    public int getTotalWorkWaitingProcessing() {
        int workQueuedInShardsCount = getWorkQueuedInShardsCount();
        Integer workQueuedInMailboxCount = getWorkQueuedInMailboxCount();
        return workQueuedInShardsCount + workQueuedInMailboxCount;
    }

    Integer getWorkQueuedInMailboxCount() {
        return wmbm.getWorkQueuedInMailboxCount();
    }

    public WorkContainer<K, V> getWorkContainerForRecord(ConsumerRecord<K, V> rec) {
        Object key = computeShardKey(rec);
        var longWorkContainerTreeMap = this.processingShards.get(key);
        long offset = rec.offset();
        WorkContainer<K, V> wc = longWorkContainerTreeMap.get(offset);
        return wc;
    }

    Map<TopicPartition, OffsetAndMetadata> findCompletedEligibleOffsetsAndRemove() {
        return findCompletedEligibleOffsetsAndRemove(true);
    }

    boolean hasCommittableOffsets() {
        return isDirty();
    }


    /**
     * Called after a successful commit off offsets
     */
    public void onOffsetCommitSuccess(Map<TopicPartition, OffsetAndMetadata> offsetsCommitted) {
        truncateOffsetsIncompleteMetadataPayloads(offsetsCommitted);
        workStateIsDirtyNeedsCommitting.set(false);
    }

    public boolean shouldThrottle() {
        return isSufficientlyLoaded();
    }

    /**
     * @return true if there's enough messages downloaded from the broker already to satisfy the pipeline, false if more
     *         should be downloaded (or pipelined in the Consumer)
     */
    boolean isSufficientlyLoaded() {
//        int total = getTotalWorkWaitingProcessing();
//        int inPartitions = getNumberOfEntriesInPartitionQueues();
//        int maxBeyondOffset = getMaxToGoBeyondOffset();
//        boolean loadedEnoughInPipeline = total > maxBeyondOffset * loadingFactor;
//        boolean overMaxUncommitted = inPartitions >= maxBeyondOffset;
//        boolean remainingIsSufficient = loadedEnoughInPipeline || overMaxUncommitted;
////        if (remainingIsSufficient) {
//        log.debug("isSufficientlyLoaded? loadedEnoughInPipeline {} || overMaxUncommitted {}", loadedEnoughInPipeline, overMaxUncommitted);
////        }
//        return remainingIsSufficient;
//
//        return !workInbox.isEmpty();

        return getWorkQueuedInMailboxCount() > options.getMaxConcurrency() * getLoadingFactor();
    }

    private int getLoadingFactor() {
        return dynamicLoadFactor.getCurrentFactor();
    }

    // TODO effeciency issues
    public boolean workIsWaitingToBeCompletedSuccessfully() {
        Collection<NavigableMap<Long, WorkContainer<K, V>>> values = processingShards.values();
        for (NavigableMap<Long, WorkContainer<K, V>> value : values) {
            if (!value.isEmpty())
                return true;
        }
        return false;
    }

    public boolean hasWorkInFlight() {
        return getNumberRecordsOutForProcessing() != 0;
    }

    public boolean isClean() {
        return !isDirty();
    }

    private boolean isDirty() {
        return this.workStateIsDirtyNeedsCommitting.get();
    }

    public boolean hasWorkInMailboxes() {
        return getWorkQueuedInMailboxCount() > 0;
    }


}
