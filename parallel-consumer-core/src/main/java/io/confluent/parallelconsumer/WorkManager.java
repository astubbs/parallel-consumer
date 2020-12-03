package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import io.confluent.csid.utils.LoopingResumingIterator;
import io.confluent.csid.utils.Range;
import io.confluent.csid.utils.StringUtils;
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
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniSets;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static io.confluent.csid.utils.KafkaUtils.toTP;
import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static java.lang.Math.min;
import static lombok.AccessLevel.PACKAGE;

/**
 * Sharded, prioritised, offset managed, order controlled, delayed work queue.
 *
 * @param <K>
 * @param <V>
 */
@Slf4j
public class WorkManager<K, V> implements ConsumerRebalanceListener {

    @Getter
    private final ParallelConsumerOptions options;

    // todo performance: disable/remove if using partition order
    /**
     * Map of Object keys to Map of offset to WorkUnits
     * <p>
     * Object is either the K key type, or it is a {@link TopicPartition}
     * <p>
     * Used to collate together a queue of work units for each unique key consumed
     *
     * @see K
     * @see #maybeGetWork()
     */
    private final Map<Object, NavigableMap<Long, WorkContainer<K, V>>> processingShards = new ConcurrentHashMap<>();
    private final LinkedBlockingQueue<ConsumerRecords<K, V>> workInbox = new LinkedBlockingQueue<>();

    /**
     * Map of partitions to Map of offsets to WorkUnits
     * <p>
     * Need to record globally consumed records, to ensure correct offset order committal. Cannot rely on incrementally
     * advancing offsets, as this isn't a guarantee of kafka's.
     * <p>
     * Concurrent because either the broker poller thread or the control thread may be requesting offset to commit
     * ({@link #findCompletedEligibleOffsetsAndRemove})
     *
     * @see #findCompletedEligibleOffsetsAndRemove
     */
    private final Map<TopicPartition, NavigableMap<Long, WorkContainer<K, V>>> partitionCommitQueues = new ConcurrentHashMap<>();
    //    private final Map<TopicPartition, NavigableMap<Long, WorkContainer<K, V>>> partitionCommitQueues = new HashMap<>();

    /**
     * Continuous offset encodings
     */
    private final Map<TopicPartition, OffsetSimultaneousEncoder> continuousOffsetEncodings = new ConcurrentHashMap<>();

    private final BackoffAnalyser backoffer;

    /**
     * Iteration resume point, to ensure fairness (prevent shard starvation) when we can't process messages from every
     * shard.
     */
    private Optional<Object> iterationResumePoint = Optional.empty();

    private int recordsOutForProcessing = 0;

    /**
     * todo docs The multiple that should be pre-loaded awaiting processing. Consumer already pipelines, so we shouldn't
     * need to pipeline ourselves too much.
     * <p>
     * Note how this relates to {@link BrokerPollSystem#getLongPollTimeout()} - if longPollTimeout is high and loading
     * factor is low, there may not be enough messages queued up to satisfy demand.
     */
    private final int loadingFactor = 3;

    /**
     * Useful for testing
     */
    @Getter(PACKAGE)
    private final List<Consumer<WorkContainer<K, V>>> successfulWorkListeners = new ArrayList<>();

    @Setter(PACKAGE)
    private WallClock clock = new WallClock();

    org.apache.kafka.clients.consumer.Consumer<K, V> consumer;

    // visible for testing
    /**
     * A subset of Offsets, beyond the highest committable offset, which haven't been totally completed.
     * <p>
     * We only need to know the full incompletes when we do the {@link #findCompletedEligibleOffsetsAndRemove} scan, so
     * find the full sent only then, and discard. Otherwise, for continuous encoding, the encoders track it them
     * selves.
     *
     * @see #findCompletedEligibleOffsetsAndRemove(boolean)
     * @see #manageEncoding(boolean, WorkContainer)
     * @see #onSuccess(WorkContainer)
     * @see #onFailure(WorkContainer)
     */
    Map<TopicPartition, TreeSet<Long>> partitionOffsetsIncompleteMetadataPayloads = new HashMap<>();

    // visible for testing
    /**
     * The highest seen offset for a partition
     */
    Map<TopicPartition, Long> partitionOffsetHighestSeen = new HashMap<>();

    /**
     * Highest offset which has completed
     */
    Map<TopicPartition, Long> partitionOffsetHighestCompleted = new HashMap<>();

    /**
     * todo partitionMaximumRecrodRangeOrMaxMessagesCountOrSomething
     * <p>
     * if true, more messages are allowed
     */
    Map<TopicPartition, Boolean> partitionMaximumRecrodRangeOrMaxMessagesCountOrSomething = new HashMap<>();

    /**
     * Highest committable offset - the end offset of the highest (from the lowest seen) continuous set of completed
     * offsets. AKA low water mark.
     */
    Map<TopicPartition, Long> partitionOffsetHighestContinuousCompleted = new HashMap<>();

    // visible for testing
    long MISSING_HIGHEST_SEEN = -1L;

    /**
     * Get's set to true whenever work is returned completed, so that we know when a commit needs to be made.
     * <p>
     * In normal operation, this probably makes very little difference, as typical commit frequency is 1 second, so low
     * chances no work has completed in the last second.
     */
    private AtomicBoolean workStateIsDirtyNeedsCommitting = new AtomicBoolean(false);

    private int numberOfAssignedPartitions;

    public WorkManager(ParallelConsumerOptions options, org.apache.kafka.clients.consumer.Consumer<K, V> consumer) {
        this.options = options;
        this.consumer = consumer;

        backoffer = new BackoffAnalyser(options.getNumberOfThreads() * 10);
    }

    /**
     * Load offset map for assigned partitions
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        numberOfAssignedPartitions = numberOfAssignedPartitions + partitions.size();
        log.info("Assigned {} partitions - that's {} bytes per partition for encoding offset overruns",
                numberOfAssignedPartitions, OffsetMapCodecManager.DefaultMaxMetadataSize / numberOfAssignedPartitions);

        try {
            log.debug("onPartitionsAssigned: {}", partitions);
            Set<TopicPartition> partitionsSet = UniSets.copyOf(partitions);
            OffsetMapCodecManager<K, V> om = new OffsetMapCodecManager<>(this, this.consumer);
            om.loadOffsetMapForPartition(partitionsSet);
        } catch (Exception e) {
            log.error("Error in onPartitionsAssigned", e);
            throw e;
        }
    }

    /**
     * Clear offset map for revoked partitions
     * <p>
     * {@link ParallelEoSStreamProcessor#onPartitionsRevoked} handles committing off offsets upon revoke
     *
     * @see ParallelEoSStreamProcessor#onPartitionsRevoked
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        numberOfAssignedPartitions = numberOfAssignedPartitions - partitions.size();

        try {
            log.debug("Partitions revoked: {}", partitions);
            removePartitionFromRecordsAndShardWork(partitions);
        } catch (Exception e) {
            log.error("Error in onPartitionsRevoked", e);
            throw e;
        }
    }

    /**
     * Clear offset map for lost partitions
     */
    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        numberOfAssignedPartitions = numberOfAssignedPartitions - partitions.size();

        try {
            log.warn("Partitions have been lost");
            log.debug("Lost partitions: {}", partitions);
            removePartitionFromRecordsAndShardWork(partitions);
        } catch (Exception e) {
            log.error("Error in onPartitionsLost", e);
            throw e;
        }
    }

    private void removePartitionFromRecordsAndShardWork(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions) {
            log.debug("Removing records for partition {}", partition);
            partitionOffsetHighestSeen.remove(partition);
            partitionOffsetHighestCompleted.remove(partition);
            partitionOffsetHighestContinuousCompleted.remove(partition);
            partitionOffsetsIncompleteMetadataPayloads.remove(partition);
            partitionMaximumRecrodRangeOrMaxMessagesCountOrSomething.remove(partition);

            NavigableMap<Long, WorkContainer<K, V>> oldWorkPartitionCommitQueue = partitionCommitQueues.remove(partition);
            if (oldWorkPartitionCommitQueue != null) {
                removeShardsFoundIn(oldWorkPartitionCommitQueue);
            } else {
                log.trace("Removed empty commit queue");
            }
        }
    }

    /**
     * Remove only the work shards which are referenced from revoked partitions
     *
     * @param oldWorkPartitionQueue partition set to scan for unique keys to be removed from our shard queue
     */
    private void removeShardsFoundIn(NavigableMap<Long, WorkContainer<K, V>> oldWorkPartitionQueue) {
        log.trace("Searching for and removing work found in shard queue");
        // this all scanning loop could be avoided if we also store a map of unique keys found referenced when a
        // partition is assigned, but that could worst case grow forever
        for (WorkContainer<K, V> work : oldWorkPartitionQueue.values()) {
            K key = work.getCr().key();
            this.processingShards.remove(key);
        }
    }

    public void registerWork(List<ConsumerRecords<K, V>> records) {
        for (var record : records) {
            registerWork(record);
        }
    }

    /**
     * Work must be registered in offset order
     * <p>
     * Thread safe for use by control and broker poller thread.
     *
     * @see #onSuccess
     * @see #raisePartitionHighestSeen
     */
    public void registerWork(ConsumerRecords<K, V> records) {
        workInbox.add(records);
    }

    private final Queue<ConsumerRecords<K, V>> internalBatchMailQueue = new LinkedList<>();
    private final Queue<ConsumerRecord<K, V>> internalFlattenedMailQueue = new LinkedList<>();

    /**
     * Take our inbound messages from the {@link BrokerPollSystem} and add them to our registry.
     *
     * @param requestedMaxWorkToRetrieve
     */
    private void processInbox(final int requestedMaxWorkToRetrieve) {
        workInbox.drainTo(internalBatchMailQueue);

        // flatten
        while (!internalBatchMailQueue.isEmpty()) {
            ConsumerRecords<K, V> consumerRecords = internalBatchMailQueue.poll();
            log.debug("Flattening {} records", consumerRecords.count());
            for (final ConsumerRecord<K, V> consumerRecord : consumerRecords) {
                internalFlattenedMailQueue.add(consumerRecord);
            }
        }

        //
//        int inFlight = getNumberOfEntriesInPartitionQueues();
//        int max = getMaxToGoBeyondOffset();
//        int gap = max - inFlight;
        int gap = requestedMaxWorkToRetrieve;
        int taken = 0;

//        log.debug("Will register {} (max configured: {}) records of work ({} already registered)", gap, max, inFlight);
        log.debug("Will attempt to register {} - {} available", requestedMaxWorkToRetrieve, internalFlattenedMailQueue.size());

        //
        while (taken < gap && !internalFlattenedMailQueue.isEmpty()) {
            ConsumerRecord<K, V> poll = internalFlattenedMailQueue.poll();
            processInbox(poll);
            taken++;
        }

        log.debug("{} new records were registered.", taken);

//        ArrayList<ConsumerRecords<K, V>> toRemove = new ArrayList<>();
//        for (final ConsumerRecords<K, V> records : internalBatchMailQueue) {
//            records.
//
//        }
//        boolean moreRecordsCanBeAccepted = processInbox(records);
//        if (moreRecordsCanBeAccepted)
//            toRemove.add(records);
//        internalBatchMailQueue.removeAll(toRemove);
    }

    private int getMaxToGoBeyondOffset() {
        return backoffer.getCurrentTotalMaxCountBeyondOffset();
    }

    /**
     * @return true if the records were accepted, false if they cannot be
     * @see #processInbox()
     */
    private boolean processInbox(ConsumerRecords<K, V> records) {
        int partitionWorkRemainingCount = getWorkQueuedInShardsCount();
        int recordsToAdd = records.count();
        // we don't break up individual record sets (although we could, but "overhead") so need to queue up records even if it goes over by some amount
        boolean overMax = partitionWorkRemainingCount - recordsToAdd >= getMaxToGoBeyondOffset();
        if (overMax) {
            log.debug("Work remaining in partition queues has surpassed max, so won't bring further messages in from the pipeline queued: {} / max: {}",
                    partitionWorkRemainingCount, getMaxToGoBeyondOffset());
            return false;
        }

//        if (!inboundOffsetWidthWithinRange(records)) {
//            return false;
//        }

        //
        log.debug("Registering {} records of work ({} already registered)", recordsToAdd, partitionWorkRemainingCount);

        for (ConsumerRecord<K, V> rec : records) {
            processInbox(rec);
        }

        return true;
    }

    private boolean inboundOffsetWidthWithinRange(final ConsumerRecords<K, V> records) {
        // brute force - surely very slow. surely this info can be cached?
        Map<TopicPartition, List<ConsumerRecord<K, V>>> inbound = new HashMap<>();
        for (final ConsumerRecord<K, V> record : records) {
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            inbound.computeIfAbsent(tp, (ignore) -> new ArrayList<>()).add(record);
        }

        Set<Map.Entry<TopicPartition, List<ConsumerRecord<K, V>>>> inboundPartitionQueues = inbound.entrySet();
        for (final Map.Entry<TopicPartition, List<ConsumerRecord<K, V>>> inboundPartitionQueue : inboundPartitionQueues) {
            // get highest start offset
            long start = 0l;
            TopicPartition tp = inboundPartitionQueue.getKey();
            NavigableMap<Long, WorkContainer<K, V>> longWorkContainerNavigableMap = partitionCommitQueues.get(tp);
            if (longWorkContainerNavigableMap != null) {
                for (final Map.Entry<Long, WorkContainer<K, V>> longWorkContainerEntry : longWorkContainerNavigableMap.entrySet()) {
                    WorkContainer<K, V> value = longWorkContainerEntry.getValue();
                    boolean userFunctionSucceeded = value.isUserFunctionSucceeded();
                    if (!userFunctionSucceeded) {
                        start = value.getCr().offset();

                        // now find any record what would make the width too big. Binary search?
                        // brute force
                        List<ConsumerRecord<K, V>> inboundRecordQueue = inboundPartitionQueue.getValue();
//                        ConsumerRecord<K, V> highestOffsetInboundRecord = inboundRecordQueue.get(inboundRecordQueue.size() - 1);
//                        long newEnd = highestOffsetInboundRecord.offset();

                        for (final ConsumerRecord<K, V> inboundRecord : inboundRecordQueue) {
                            long newEnd = inboundRecord.offset();
                            long width = newEnd - start;

                            if (width >= BitsetEncoder.MAX_LENGTH_ENCODABLE) {
                                long oldWidth = partitionOffsetHighestSeen.get(tp) - start;
                                // can't be more accurate unless we break up the inbound records and count them per queue
                                log.debug("Incoming outstanding offset difference too large for BitSet encoder (incoming width: {}, old width: {}), will wait before adding these records until the width shrinks (below {})",
                                        width, oldWidth, BitsetEncoder.MAX_LENGTH_ENCODABLE);
                                return false;
//                                break;
                            } else {
                                log.debug("Width was ok {}", width);
                            }
                        }
                    }
                }
            }
        }
        return true;
    }

    private void processInbox(final ConsumerRecord<K, V> rec) {
        if (isRecordPreviouslyProcessedSuccessfully(rec)) {
            log.trace("Record previously processed, skipping. offset: {}", rec.offset());
        } else {
            Object shardKey = computeShardKey(rec);
            long offset = rec.offset();
            var wc = new WorkContainer<>(rec);

            TopicPartition tp = toTP(rec);
            raisePartitionHighestSeen(offset, tp);

            processingShards.computeIfAbsent(shardKey, (ignore) -> new ConcurrentSkipListMap<>()).put(offset, wc);

            partitionCommitQueues.computeIfAbsent(tp, (ignore) -> new ConcurrentSkipListMap<>()).put(offset, wc);
        }
    }

    void raisePartitionHighestSeen(long seenOffset, TopicPartition tp) {
        // rise the high water mark
        Long oldHighestSeen = partitionOffsetHighestSeen.getOrDefault(tp, MISSING_HIGHEST_SEEN);
        if (seenOffset >= oldHighestSeen || seenOffset == MISSING_HIGHEST_SEEN) {
            partitionOffsetHighestSeen.put(tp, seenOffset);
        }
    }

    private boolean isRecordPreviouslyProcessedSuccessfully(ConsumerRecord<K, V> rec) {
        long thisRecordsOffset = rec.offset();
        TopicPartition tp = new TopicPartition(rec.topic(), rec.partition());
        TreeSet<Long> incompleteOffsets = this.partitionOffsetsIncompleteMetadataPayloads.getOrDefault(tp, new TreeSet<>());
        if (incompleteOffsets.contains(thisRecordsOffset)) {
            // record previously saved into incompletes as having not been processed successfully yet
            return false;
        } else {
            Long offsetHighWaterMarks = partitionOffsetHighestSeen.getOrDefault(tp, MISSING_HIGHEST_SEEN);
            if (thisRecordsOffset < offsetHighWaterMarks) {
                // within the range of tracked offsets, but not in incompletes, so must have been previously completed
                return true;
            } else {
                // not in incompletes, and is a higher offset than we've ever seen, as we haven't recorded this far up, so must not have been processed yet
                return false;
            }
        }
    }

    private Object computeShardKey(ConsumerRecord<K, V> rec) {
        return switch (options.getOrdering()) {
            case KEY -> rec.key();
            case PARTITION, UNORDERED -> new TopicPartition(rec.topic(), rec.partition());
        };
    }

    public <R> List<WorkContainer<K, V>> maybeGetWork() {
        return maybeGetWork(getMaxMessagesToQueue());
    }

    /**
     * Depth first work retrieval.
     */
    public List<WorkContainer<K, V>> maybeGetWork(int requestedMaxWorkToRetrieve) {
        //int minWorkToGetSetting = min(min(requestedMaxWorkToRetrieve, getMaxMessagesToQueue()), getMaxToGoBeyondOffset());
//        int minWorkToGetSetting = min(requestedMaxWorkToRetrieve, getMaxToGoBeyondOffset());
//        int workToGetDelta = requestedMaxWorkToRetrieve - getRecordsOutForProcessing();
        int workToGetDelta = requestedMaxWorkToRetrieve;

        // optimise early
        if (workToGetDelta < 1) {
            return UniLists.of();
        }

        int extraNeededFromInboxToSatisfy = requestedMaxWorkToRetrieve - getWorkQueuedInShardsCount();
        processInbox(extraNeededFromInboxToSatisfy);

        //
        List<WorkContainer<K, V>> work = new ArrayList<>();

        //
        var it = new LoopingResumingIterator<>(iterationResumePoint, processingShards);

        //
        for (var shard : it) {
            log.trace("Looking for work on shard: {}", shard.getKey());
            if (work.size() >= workToGetDelta) {
                this.iterationResumePoint = Optional.of(shard.getKey());
                log.debug("Work taken is now over max, stopping (saving iteration resume point {})", iterationResumePoint);
                break;
            }

            ArrayList<WorkContainer<K, V>> shardWork = new ArrayList<>();
            SortedMap<Long, WorkContainer<K, V>> shardQueue = shard.getValue();

            // then iterate over shardQueue queue
            Set<Map.Entry<Long, WorkContainer<K, V>>> shardQueueEntries = shardQueue.entrySet();
            for (var queueEntry : shardQueueEntries) {
                int taken = work.size() + shardWork.size();
                if (taken >= workToGetDelta) {
                    log.trace("Work taken ({}) exceeds max ({})", taken, workToGetDelta);
                    break;
                }

                var wc = queueEntry.getValue();
                boolean alreadySucceeded = !wc.isUserFunctionSucceeded();
                if (wc.hasDelayPassed(clock) && wc.isNotInFlight() && alreadySucceeded) {
                    log.trace("Taking {} as work", wc);
                    wc.takingAsWork();
                    shardWork.add(wc);
                } else {
                    log.trace("Work ({}) still delayed or is in flight, can't take...", wc);
                }

                ProcessingOrder ordering = options.getOrdering();
                if (ordering == UNORDERED) {
                    // continue - we don't care about processing order, so check the next message
                    continue;
                } else {
                    // can't take any more from this partition until this work is finished
                    // processing blocked on this partition, continue to next partition
                    log.trace("Processing by {}, so have cannot get more messages on this ({}) shard.", this.options.getOrdering(), shard.getKey());
                    break;
                }
            }
            work.addAll(shardWork);
        }


        log.debug("Got {} records of work. In-flight: {}, Awaiting: {}", work.size(), getRecordsOutForProcessing(), getNumberOfEntriesInPartitionQueues());
        recordsOutForProcessing += work.size();

        return work;
    }

    private int getMaxMessagesToQueue() {
        //return options.getNumberOfThreads() * options.getLoadingFactor();
        double rate = successRatePer5Seconds.getRate();
        int newRatae = (int) rate * 2;
        int max = Math.max(newRatae, options.getNumberOfThreads() * 10);
        log.debug("max to queue: {}", max);
        return max;
//        return options.getNumberOfThreads() * 10;
    }

    private final WindowedEventRate successRatePer5Seconds = new WindowedEventRate(5);
    private final ExponentialMovingAverage successRatePer5SecondsEMA = new ExponentialMovingAverage(0.5);

    public void onSuccess(WorkContainer<K, V> wc) {
        //
        successRatePer5Seconds.newEvent();
//        successRatePer5SecondsEMA.
        workStateIsDirtyNeedsCommitting.set(true);

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

        //
        recordsOutForProcessing--;

        manageEncoding(true, wc);
    }

    public void onResultBatch(final Set<WorkContainer<K, V>> results) {
        //
        if (!results.isEmpty()) {
            onResultUpdatePartitionRecordsBatch(results);
        }

        // individual
        for (var work : results) {
            MDC.put("offset", work.toString());
            handleFutureResult(work);
            MDC.clear();
        }
    }

    protected void handleFutureResult(WorkContainer<K, V> wc) {
        if (wc.isUserFunctionSucceeded()) {
            onSuccess(wc);
        } else {
            onFailure(wc);
        }
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
     * AKA highest committable or low water mark
     *
     * @param results must be sorted by offset - partition ordering doesn't matter, as long as we see offsets in order
     */
    private void onResultUpdateHighestContinuousBatch(final Set<? extends WorkContainer<K, V>> results) {
        Map<TopicPartition, Boolean> partitionNowFormsAContiniousBlock = new HashMap<>();
        for (final WorkContainer<K, V> work : results) {

            long thisOffset = work.getCr().offset();

            boolean thisOffsetIsFailed = !work.isUserFunctionSucceeded();
            TopicPartition tp = work.getTopicPartition();

            long missingPreviousHighValue = -1l; // this offset will always be higher
            long previousHighestContinuous = partitionOffsetHighestContinuousCompleted.getOrDefault(work.getTopicPartition(), missingPreviousHighValue);

            Boolean partitionSoFarIsContinuous = partitionNowFormsAContiniousBlock.get(tp);
            if (partitionSoFarIsContinuous != null && !partitionSoFarIsContinuous) {
                // previously we found non continuous block so we can skip
                continue; // to next record
            } else {
                // we can't know, we have to keep digging
            }

            if (thisOffsetIsFailed) {
                // simpler path
                // work isn't successful. Is this the first? Is there a gap previously? Perhaps the gap doesn't exist (skipped offsets in partition)
                Boolean previouslyContinuous = partitionNowFormsAContiniousBlock.get(tp);
                partitionNowFormsAContiniousBlock.put(tp, false); // this partitions continuous block
            } else {
                if (thisOffset > previousHighestContinuous) { // sanity? by definition it must be higher
                    //
                    updateHighestCompletedOffset(work, tp);

                    // does it form a new continuous block?

                    // queue this offset belongs to
                    NavigableMap<Long, WorkContainer<K, V>> commitQueue = partitionCommitQueues.get(tp);

                    // easy path
                    if (thisOffset == previousHighestContinuous + 1) {
                        // easy, yes it's continuous, as there's no gap from previous highest
                        partitionNowFormsAContiniousBlock.put(tp, true);
                    } else {
                        // do the entries in the gap exist in our partition queue? or are they skipped in the source log?
                        Range offSetRangeToCheck = new Range(previousHighestContinuous, thisOffset);
                        for (var relativeOffset : offSetRangeToCheck) {
                            long offsetToCheck = previousHighestContinuous + relativeOffset;
                            WorkContainer<K, V> workToExamine = commitQueue.get(offsetToCheck);
                            if (workToExamine == null) {
                                // offset doesnt exist in source partition
                            } else {
                                if (workToExamine.isUserFunctionSucceeded()) {
                                    // counts as continuous, just isn't in this batch - previously successful but there used to be gaps
                                    partitionNowFormsAContiniousBlock.put(tp, true);
                                } else {
                                    // record exists but is incomplete - breaks continuity
                                    partitionNowFormsAContiniousBlock.put(tp, false);
                                }
                            }
                        }
                    }
                } else {
                    throw new InternalRuntimeError(msg("Unexpected new offset {} lower than low water mark {}", thisOffset, previousHighestContinuous));
                }
            }
        }

    }

    /**
     * Update highest Completed seen so far
     */
    private void updateHighestCompletedOffset(final WorkContainer<K, V> work, final TopicPartition tp) {
        //
        Long highestCompleted = partitionOffsetHighestCompleted.get(tp);
        long thisOffset = work.getCr().offset();
        if (thisOffset > highestCompleted) {
            log.debug("Updaing highest completed - was: {} now: {}", highestCompleted, thisOffset);
            partitionOffsetHighestCompleted.put(tp, thisOffset);
        }
    }

    /**
     * todo refactor to offset manager?
     */
    private void manageEncoding(final boolean offsetComplete, final WorkContainer<K, V> wc) {
        TopicPartition tp = wc.getTopicPartition();
        long baseOffset = partitionOffsetHighestContinuousCompleted.get(tp);
        Long highestCompleted = partitionOffsetHighestCompleted.get(tp);
        long offsetAtEndOfEncodingRange = highestCompleted + 1;
        long nextExpectedOffset = offsetAtEndOfEncodingRange;

        OffsetSimultaneousEncoder offsetSimultaneousEncoder = continuousOffsetEncodings
                .computeIfAbsent(tp, (ignore) ->
                        new OffsetSimultaneousEncoder(baseOffset, nextExpectedOffset, new HashSet<>())
                );

        long offset = wc.getCr().offset();
        long relativeOffset = offset - baseOffset;

        if (offsetComplete)
            offsetSimultaneousEncoder.encodeCompletedOffset(baseOffset, relativeOffset);
        else
            offsetSimultaneousEncoder.encodeIncompleteOffset(baseOffset, relativeOffset);

        calculateSpaceRequirements();
    }

    private void calculateSpaceRequirements() {
        int available = OffsetMapCodecManager.DefaultMaxMetadataSize;
        int perPartition = available / numberOfAssignedPartitions;
        double tolerance = 0.8; //90%

        // for each encoded partition so far, check if we're within tolerance of available space
        for (final Map.Entry<TopicPartition, OffsetSimultaneousEncoder> entry : continuousOffsetEncodings.entrySet()) {
            TopicPartition tp = entry.getKey();
            OffsetSimultaneousEncoder encoder = entry.getValue();
            int encodedSize = encoder.getEncodedSize();

            int allowed = (int) (perPartition * tolerance);
            // tolerance threshold crossed - turn on back pressure - no more for this partition
            boolean moreMessagesAreAllowed = encodedSize > allowed;
            partitionMaximumRecrodRangeOrMaxMessagesCountOrSomething.put(tp, moreMessagesAreAllowed);
        }
    }

    public void onFailure(WorkContainer<K, V> wc) {
        //
        wc.fail(clock);

        //
        putBack(wc);

        //
        manageEncoding(false, wc);
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
        recordsOutForProcessing--;
    }

    public int getNumberOfEntriesInPartitionQueues() {
        int count = 0;
        for (var e : this.partitionCommitQueues.entrySet()) {
            count += e.getValue().size();
        }
        return count;
    }

    /**
     * @return Work count in mailbox plus work added to the processing shards
     */
    public int getTotalWorkWaitingProcessing() {
        return getWorkQueuedInShardsCount() + getWorkQueuedInMailboxCount();
    }

    /**
     * @return Work queued in the mail box, awaiting processing into shards
     */
    Integer getWorkQueuedInMailboxCount() {
        int batchCount = 0;
        for (final ConsumerRecords<K, V> inboxEntry : workInbox) {
            batchCount += inboxEntry.count();
        }
//        for (final ConsumerRecords<K, V> consumerRecords : Collections.unmodifiableCollection(internalBatchMailQueue)) { // copy for concurrent access - as it holds batches of polled records, it should be relatively small
        if (internalBatchMailQueue.size() > 10) {
            log.warn("Larger than expected {}", internalBatchMailQueue.size());
        }
        for (final ConsumerRecords<K, V> consumerRecords : new ArrayList<>(internalBatchMailQueue)) { // copy for concurrent access - as it holds batches of polled records, it should be relatively small
            if (consumerRecords != null) {
                batchCount += consumerRecords.count();
            }
        }
//        Integer batchCount = internalBatchMailQueue.stream()
//                .map(ConsumerRecords::count)
//                .reduce(0, Integer::sum);
        return batchCount + internalFlattenedMailQueue.size();
    }

    /**
     * @return Work ready in the processing shards, awaiting selection as work to do
     */
    public int getWorkQueuedInShardsCount() {
        int count = 0;
        for (var e : this.processingShards.entrySet()) {
            count += e.getValue().size();
        }
        return count;
    }

    boolean isRecordsAwaitingProcessing() {
        int partitionWorkRemainingCount = getWorkQueuedInShardsCount();
        //return partitionWorkRemainingCount > 0 || !workInbox.isEmpty() || !internalMailQueue.isEmpty();
        return partitionWorkRemainingCount > 0 || !internalBatchMailQueue.isEmpty() || !internalFlattenedMailQueue.isEmpty();
    }

    boolean isRecordsAwaitingToBeCommitted() {
        // todo could be improved - shouldn't need to count all entries if we simply want to know if there's > 0
        int partitionWorkRemainingCount = getNumberOfEntriesInPartitionQueues();
        return partitionWorkRemainingCount > 0;
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
     * TODO: This entire loop could be possibly redundant, if we instead track low water mark, and incomplete offsets as
     * work is submitted and returned.
     * <p>
     * todo: refactor into smaller methods?
     */
    <R> Map<TopicPartition, OffsetAndMetadata> findCompletedEligibleOffsetsAndRemove(boolean remove) {
        Map<TopicPartition, OffsetAndMetadata> perPartitionNextExpectedOffset = new HashMap<>();
        int totalPartitionQueueSizeForLogging = 0;
        int removed = 0;
        log.trace("Scanning for in order in-flight work that has completed...");
        int totalOffsetMetaCharacterLength = 0;
        for (final var partitionQueueEntry : partitionCommitQueues.entrySet()) {
            totalPartitionQueueSizeForLogging += partitionQueueEntry.getValue().size();
            var partitionQueue = partitionQueueEntry.getValue();

            var workToRemove = new LinkedList<WorkContainer<K, V>>();
            var incompleteOffsets = new LinkedHashSet<Long>(); // we only need to know the full incompletes when we do this scan, so find them only now, and discard

            //

            // can't commit this offset or beyond, as this is the latest offset that is incomplete
            // i.e. only commit offsets that come before the current one, and stop looking for more
            boolean iteratedBeyondLowWaterMarkBeingLowestCommittableOffset = false;

            TopicPartition topicPartitionKey = partitionQueueEntry.getKey();
            log.trace("Starting scan of partition: {}", topicPartitionKey);
            for (final var offsetAndItsWorkContainer : partitionQueue.entrySet()) {
                // ordered iteration via offset keys thanks to the tree-map
                WorkContainer<K, V> work = offsetAndItsWorkContainer.getValue();
                long offset = work.getCr().offset();
                boolean workCompleted = work.isUserFunctionComplete();
                if (workCompleted) {
                    if (work.isUserFunctionSucceeded() && !iteratedBeyondLowWaterMarkBeingLowestCommittableOffset) {
                        log.trace("Found offset candidate ({}) to add to offset commit map", work);
                        workToRemove.add(work);
                        // as in flights are processed in order, this will keep getting overwritten with the highest offset available
                        // current offset is the highest successful offset, so commit +1 - offset to be committed is defined as the offset of the next expected message to be read
                        long offsetOfNextExpectedMessageAkaHighestCommittableAkaLowWaterMark = offset + 1;
                        OffsetAndMetadata offsetData = new OffsetAndMetadata(offsetOfNextExpectedMessageAkaHighestCommittableAkaLowWaterMark);
                        perPartitionNextExpectedOffset.put(topicPartitionKey, offsetData);
                    } else if (work.isUserFunctionSucceeded() && iteratedBeyondLowWaterMarkBeingLowestCommittableOffset) {
                        log.trace("Offset {} is complete and succeeded, but we've iterated past the lowest committable offset. Will mark as complete in the offset map.", work.getCr().offset());
                        // no-op - offset map is only for not succeeded or completed offsets
//                        // mark as complete complete so remove from work
//                        workToRemove.add(work);
                    } else {
                        log.trace("Offset {} is complete, but failed processing. Will track in offset map as not complete. Can't do normal offset commit past this point.", work.getCr().offset());
                        iteratedBeyondLowWaterMarkBeingLowestCommittableOffset = true;
                        incompleteOffsets.add(offset);
                    }
                } else {
                    // work not complete - either successfully or unsuccessfully
                    iteratedBeyondLowWaterMarkBeingLowestCommittableOffset = true;
                    log.trace("Offset ({}) is incomplete, holding up the queue ({}) of size {}.",
                            work.getCr().offset(),
                            partitionQueueEntry.getKey(),
                            partitionQueueEntry.getValue().size());
                    incompleteOffsets.add(offset);
                }
            }

            {
                // offset map building
                // Get final offset data, build the the offset map, and replace it in our map of offset data to send
                // TODO potential optimisation: store/compare the current incomplete offsets to the last committed ones, to know if this step is needed or not (new progress has been made) - isdirty?
                if (!incompleteOffsets.isEmpty()) {
                    long offsetOfNextExpectedMessage;
                    OffsetAndMetadata finalOffsetOnly = perPartitionNextExpectedOffset.get(topicPartitionKey);
                    if (finalOffsetOnly == null) {
                        // no new low water mark to commit, so use the last one again
                        offsetOfNextExpectedMessage = incompleteOffsets.iterator().next(); // first element
                    } else {
                        offsetOfNextExpectedMessage = finalOffsetOnly.offset();
                    }

                    OffsetMapCodecManager<K, V> om = new OffsetMapCodecManager<>(this, this.consumer);
                    try {
                        String offsetMapPayload = om.makeOffsetMetadataPayload(offsetOfNextExpectedMessage, topicPartitionKey, incompleteOffsets);
                        totalOffsetMetaCharacterLength += offsetMapPayload.length();
                        OffsetAndMetadata offsetWithExtraMap = new OffsetAndMetadata(offsetOfNextExpectedMessage, offsetMapPayload);
                        perPartitionNextExpectedOffset.put(topicPartitionKey, offsetWithExtraMap);
                    } catch (EncodingNotSupportedException e) {
                        log.warn("No encodings could be used to encode the offset map, skipping. Warning: messages might be replayed on rebalance", e);
                        backoffer.onFailure();
                    }
                }
            }

            if (remove) {
                removed += workToRemove.size();
                for (var workContainer : workToRemove) {
                    var offset = workContainer.getCr().offset();
                    partitionQueue.remove(offset);
                }
            }
        }

        maybeStripOffsetPayload(perPartitionNextExpectedOffset, totalOffsetMetaCharacterLength);

        log.debug("Scan finished, {} were in flight, {} completed offsets removed, coalesced to {} offset(s) ({}) to be committed",
                totalPartitionQueueSizeForLogging, removed, perPartitionNextExpectedOffset.size(), perPartitionNextExpectedOffset);
        return perPartitionNextExpectedOffset;
    }

    /**
     * Once all the offset maps have been calculated, check if they're too big, and if so, remove all of them.
     * <p>
     * Implication of this is that if the system has to recover from this offset, then it will have to replay all the
     * messages that were otherwise complete.
     * <p>
     * Must be thread safe.
     *
     * @see OffsetMapCodecManager#DefaultMaxMetadataSize
     */
    private void maybeStripOffsetPayload(Map<TopicPartition, OffsetAndMetadata> offsetsToSend,
                                         int totalOffsetMetaCharacterLength) {
        // TODO: Potential optimisation: if max metadata size is shared across partitions, the limit used could be relative to the number of
        //  partitions assigned to this consumer. In which case, we could derive the limit for the number of downloaded but not committed
        //  offsets, from this max over some estimate. This way we limit the possibility of hitting the hard limit imposed in the protocol, thus
        //  retaining the offset map feature, at the cost of potential performance by hitting a soft maximum in our uncommitted concurrent processing.
        if (totalOffsetMetaCharacterLength > OffsetMapCodecManager.DefaultMaxMetadataSize) {
            log.warn("Offset map data too large (size: {}) to fit in metadata payload - stripping offset map out. " +
                            "See kafka.coordinator.group.OffsetConfig#DefaultMaxMetadataSize = 4096",
                    totalOffsetMetaCharacterLength);
            // strip all payloads
            // todo iteratively strip the largest payloads until we're under the limit
            for (var entry : offsetsToSend.entrySet()) {
                TopicPartition key = entry.getKey();
                OffsetAndMetadata v = entry.getValue();
                OffsetAndMetadata stripped = new OffsetAndMetadata(v.offset()); // meta data gone
                offsetsToSend.replace(key, stripped);
            }
            backoffer.onFailure();
        } else if (totalOffsetMetaCharacterLength != 0) {
            log.debug("Offset map small enough to fit in payload: {} (max: {})", totalOffsetMetaCharacterLength, OffsetMapCodecManager.DefaultMaxMetadataSize);
            backoffer.onSuccess();
        }
    }

    /**
     * Called after a successful commit off offsets
     */
    public void onOffsetCommitSuccess(Map<TopicPartition, OffsetAndMetadata> offsetsCommitted) {
        truncateOffsetsIncompleteMetadataPayloads(offsetsCommitted);
    }

    /**
     * Truncate our tracked offsets as a commit was successful, so the low water mark rises, and we don't need to track
     * as much anymore.
     * <p>
     * When commits are made to broker, we can throw away all the individually tracked offsets lower than the base
     * offset which is in the commit.
     */
    private void truncateOffsetsIncompleteMetadataPayloads(
            final Map<TopicPartition, OffsetAndMetadata> offsetsCommitted) {
        // partitionOffsetHighWaterMarks this will get overwritten in due course
        offsetsCommitted.forEach((tp, meta) -> {
            Set<Long> incompleteOffsets = partitionOffsetsIncompleteMetadataPayloads.get(tp);
            boolean trackedOffsetsForThisPartitionExist = incompleteOffsets != null;
            if (trackedOffsetsForThisPartitionExist) {
                long newLowWaterMark = meta.offset();
                incompleteOffsets.removeIf(offset -> offset < newLowWaterMark);
            }
        });
    }

    public boolean shouldThrottle() {
        return isSufficientlyLoaded();
    }

    /**
     * @return true if there's enough messages downloaded from the broker already to satisfy the pipeline, false if more
     *         should be downloaded (or pipelined in the Consumer)
     */
    boolean isSufficientlyLoaded() {
        int total = getTotalWorkWaitingProcessing();
        int inPartitions = getNumberOfEntriesInPartitionQueues();
        int maxBeyondOffset = getMaxToGoBeyondOffset();
        boolean loadedEnoughInPipeline = total > maxBeyondOffset * loadingFactor;
        boolean overMaxUncommitted = inPartitions >= maxBeyondOffset;
        boolean remainingIsSufficient = loadedEnoughInPipeline || overMaxUncommitted;
//        if (remainingIsSufficient) {
        log.debug("isSufficientlyLoaded? loadedEnoughInPipeline {} || overMaxUncommitted {}", loadedEnoughInPipeline, overMaxUncommitted);
//        }
        return remainingIsSufficient;
    }

    public int getRecordsOutForProcessing() {
        return recordsOutForProcessing;
    }

    public boolean workIsWaitingToBeCompletedSuccessfully() {
        Collection<NavigableMap<Long, WorkContainer<K, V>>> values = processingShards.values();
        for (NavigableMap<Long, WorkContainer<K, V>> value : values) {
            if (!value.isEmpty())
                return true;
        }
        return false;
    }

    public boolean hasWorkInFlight() {
        return getRecordsOutForProcessing() != 0;
    }

    public boolean isClean() {
        return !isDirty();
    }

    private boolean isDirty() {
        return this.workStateIsDirtyNeedsCommitting.get();
    }

    public boolean isNotPartitionedOrDrained() {
        return getNumberOfEntriesInPartitionQueues() > 0 && getWorkQueuedInMailboxCount() > 0;

    }

}
