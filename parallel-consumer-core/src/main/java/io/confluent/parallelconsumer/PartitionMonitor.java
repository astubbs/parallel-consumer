package io.confluent.parallelconsumer;

import io.confluent.csid.utils.LoopingResumingIterator;
import io.confluent.csid.utils.Range;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.event.Level;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.util.*;

import static io.confluent.csid.utils.BackportUtils.toSeconds;
import static io.confluent.csid.utils.LogUtils.at;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;

@Slf4j
public class PartitionMonitor<K, V> {

    private int numberOfAssignedPartitions;

    Map<TopicPartition, PartitionState> states = new HashMap<>();

    public PartitionState getState(TopicPartition tp){
        return states.get(tp);
    }

    /**
     * Load offset map for assigned partitions
     */
    public void onPartitionsAssined(final Collection<TopicPartition> partitions) {

        // init messages allowed state
        for (final TopicPartition partition : partitions) {
            PartitionState partitionState = states.get(partition);

            partitionState.partitionMoreRecordsAllowedToProcess.putIfAbsent(partition, true);
        }

        numberOfAssignedPartitions = numberOfAssignedPartitions + partitions.size();
        log.info("Assigned {} partitions - that's {} bytes per partition for encoding offset overruns",
                numberOfAssignedPartitions, OffsetMapCodecManager.DefaultMaxMetadataSize / numberOfAssignedPartitions);

        try {
            log.debug("onPartitionsAssigned: {}", partitions);
            Set<TopicPartition> partitionsSet = UniSets.copyOf(partitions);
            OffsetMapCodecManager<K, V> om = new OffsetMapCodecManager<K, V>(this, this.consumerMgr);
            om.loadOffsetMapForPartition(partitionsSet);
        } catch (Exception e) {
            log.error("Error in onPartitionsAssigned", e);
            throw e;
        }
    }


    private void incrementPartitionAssignmentEpoch(final Collection<TopicPartition> partitions) {
        for (final TopicPartition partition : partitions) {
            int epoch = partitionsAssignmentEpochs.getOrDefault(partition, -1);
            epoch++;
            partitionsAssignmentEpochs.put(partition, epoch);
        }
    }

    /**
     * Clear offset map for revoked partitions
     * <p>
     * {@link ParallelEoSStreamProcessor#onPartitionsRevoked} handles committing off offsets upon revoke
     *
     * @see ParallelEoSStreamProcessor#onPartitionsRevoked
     * @param partitions
     */
    public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
        incrementPartitionAssignmentEpoch(partitions);

        numberOfAssignedPartitions = numberOfAssignedPartitions - partitions.size();

        try {
            log.debug("Partitions revoked: {}", partitions);
//            removePartitionFromRecordsAndShardWork(partitions);
            registerPartitionsToBeRemoved(partitions);
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
        incrementPartitionAssignmentEpoch(partitions);

        numberOfAssignedPartitions = numberOfAssignedPartitions - partitions.size();

        try {
            log.warn("Partitions have been lost: {}", partitions);
//            log.debug("Lost partitions: {}", partitions);
//            removePartitionFromRecordsAndShardWork(partitions);
            registerPartitionsToBeRemoved(partitions);
        } catch (Exception e) {
            log.error("Error in onPartitionsLost", e);
            throw e;
        }
    }

    private void removePartitionFromRecordsAndShardWork() {
        for (TopicPartition partition : partitionsToRemove) {
            log.debug("Removing records for partition {}", partition);
            // todo is there a safer way than removing these?
            partitionOffsetHighestSeen.remove(partition);
            partitionOffsetHighestSucceeded.remove(partition);
            partitionOffsetHighestContinuousSucceeded.remove(partition);
            partitionOffsetsIncompleteMetadataPayloads.remove(partition);
//            partitionMoreRecordsAllowedToProcess.remove(partition);

            //
            NavigableMap<Long, WorkContainer<K, V>> oldWorkPartitionCommitQueue = partitionCommitQueues.remove(partition);

            //
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


    /**
     * Depth first work retrieval.
     */
    public List<WorkContainer<K, V>> maybeGetWork(int requestedMaxWorkToRetrieve) {
        //int minWorkToGetSetting = min(min(requestedMaxWorkToRetrieve, getMaxMessagesToQueue()), getMaxToGoBeyondOffset());
//        int minWorkToGetSetting = min(requestedMaxWorkToRetrieve, getMaxToGoBeyondOffset());
//        int workToGetDelta = requestedMaxWorkToRetrieve - getRecordsOutForProcessing();
        removePartitionFromRecordsAndShardWork();

        int workToGetDelta = requestedMaxWorkToRetrieve;

        // optimise early
        if (workToGetDelta < 1) {
            return UniLists.of();
        }

        // todo this counts all partitions as a whole - this may cause some partitions to starve. need to round robin it?
        int workAvailable = getWorkQueuedInShardsCount();
        int extraNeededFromInboxToSatisfy = requestedMaxWorkToRetrieve - workAvailable;
        log.debug("Requested: {}, workAvailable in shards: {}, will try retrieve from mailbox the delta of: {}",
                requestedMaxWorkToRetrieve, workAvailable, extraNeededFromInboxToSatisfy);
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
                log.debug("Work taken is now over max requested, stopping (saving iteration resume point {})", iterationResumePoint);
                break;
            }

            ArrayList<WorkContainer<K, V>> shardWork = new ArrayList<>();
            SortedMap<Long, WorkContainer<K, V>> shardQueue = shard.getValue();

            // then iterate over shardQueue queue
            Set<Map.Entry<Long, WorkContainer<K, V>>> shardQueueEntries = shardQueue.entrySet();
            for (var queueEntry : shardQueueEntries) {
                int taken = work.size() + shardWork.size();
                if (taken >= workToGetDelta) {
                    log.trace("Work taken ({}) exceeds max requested ({})", taken, workToGetDelta);
                    break;
                }

                var workContainer = queueEntry.getValue();
                var topicPartitionKey = workContainer.getTopicPartition();

                {
                    if (checkEpoch(workContainer)) continue;
                }



                // TODO refactor this and the rest of the partition state monitoring code out
                // check we have capacity in offset storage to process more messages
                PartitionState state = pm.getState(topicPartitionKey);
                Boolean allowedMoreRecords = state.areMoreRecordsAllowedToProcess();
                Boolean allowedMoreRecords = partitionMoreRecordsAllowedToProcess.get(topicPartitionKey);
                // If the record has been previosly attempted, it is already represented in the current offset encoding,
                // and may in fact be the message holding up the partition so must be retried
                if (!allowedMoreRecords && workContainer.hasPreviouslyFailed()) {
                    OffsetSimultaneousEncoder offsetSimultaneousEncoder = state.getOffsetEncoer();
                    OffsetSimultaneousEncoder offsetSimultaneousEncoder = partitionContinuousOffsetEncoders.get(topicPartitionKey);
                    int encodedSizeEstimate = offsetSimultaneousEncoder.getEncodedSizeEstimate();
                    int metaDataAvailable = getMetadataSpaceAvailablePerPartition();
                    log.warn("Not allowed more records for the partition ({}) that this record ({}) belongs to due to offset " +
                                    "encoding back pressure, continuing on to next container in shard (estimated " +
                                    "required: {}, max available (without tolerance threshold): {})",
                            topicPartitionKey, workContainer.offset(), encodedSizeEstimate, metaDataAvailable);
                    continue;
                }

                boolean alreadySucceeded = !workContainer.isUserFunctionSucceeded();
                if (workContainer.hasDelayPassed(clock) && workContainer.isNotInFlight() && alreadySucceeded) {
                    log.trace("Taking {} as work", workContainer);
                    workContainer.takingAsWork();
                    shardWork.add(workContainer);
                } else {
                    Duration timeInFlight = workContainer.getTimeInFlight();
                    Level level = Level.TRACE;
                    if (toSeconds(timeInFlight) > 1) {
                        level = Level.WARN;
                    }
                    at(log, level).log("Work ({}) still delayed ({}) or is in flight ({}, time in flight: {}), alreadySucceeded? {} can't take...",
                            workContainer, !workContainer.hasDelayPassed(clock), !workContainer.isNotInFlight(), timeInFlight, alreadySucceeded);
                }

                ParallelConsumerOptions.ProcessingOrder ordering = options.getOrdering();
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

        checkShardsForProgress();

        log.debug("Got {} records of work. In-flight: {}, Awaiting in commit queues: {}", work.size(), getNumberRecordsOutForProcessing(), getNumberOfEntriesInPartitionQueues());
        numberRecordsOutForProcessing += work.size();

        return work;
    }

    // todo slooooow
    // todo refactor to partition state
    private void checkShardsForProgress() {
        for (var shard : processingShards.entrySet()) {
            for (final Map.Entry<Long, WorkContainer<K, V>> entry : shard.getValue().entrySet()) {
                WorkContainer<K, V> work = entry.getValue();
                long seconds = toSeconds(work.getTimeInFlight());
                if (work.isInFlight() && seconds > 1) {
                    log.warn("Work taking too long {} s : {}", seconds, entry);
                }
            }
        }

    }

    /**
     * Have our partitions been revoked?
     */
    private boolean checkEpoch(final WorkContainer<K, V> workContainer) {
        TopicPartition topicPartitionKey = workContainer.getTopicPartition();

        Integer currentPartitionEpoch = partitionsAssignmentEpochs.get(topicPartitionKey);
        int workEpoch = workContainer.getEpoch();
        if (currentPartitionEpoch != workEpoch) {
            log.warn("Epoch mismatch {} vs {} - were partitions lost? Skipping message - it's already assigned to a different consumer.", workEpoch, currentPartitionEpoch);
            return true;
        }
        return false;
    }


    /**
     * AKA highest committable or low water mark
     *
     * @param workResults must be sorted by offset - partition ordering doesn't matter, as long as we see offsets in
     *                    order
     */
    private void onResultUpdateHighestContinuousBatch(final Set<? extends WorkContainer<K, V>> workResults) {
        HashSet<TopicPartition> partitionsSeenForLogging = new HashSet<>();
        Map<TopicPartition, Long> originalMarks = new HashMap<>();
        Map<TopicPartition, Boolean> partitionNowFormsAContinuousBlock = new HashMap<>();
        for (final WorkContainer<K, V> work : workResults) {
            if (checkEpoch(work)) continue;

            TopicPartition tp = work.getTopicPartition();

            // guard against old epoch messages
            // TODO don't do this as upon revoke we try to commit this work. Maybe the commit attempt needs to mark the epoch as discarded, and in that case we should do this drop
//            if (work.getEpoch() < parittionsAssignmentEpochs.get(tp)) {
//                log.warn("Message assigned from old epoch, ignore: {}", work);
//                continue;
//            }


            long thisOffset = work.getCr().offset();


            // this offset has already been scanned as a part of a high range batch, so skip (we already know the highest continuous block incorporates this offset)
            Long previousHighestContinuous = partitionOffsetHighestContinuousSucceeded.get(tp);
            if (thisOffset <= previousHighestContinuous) {
//                     sanity? by definition it must be higher
//                    throw new InternalRuntimeError(msg("Unexpected new offset {} lower than low water mark {}", thisOffset, previousHighestContinuous));
                // things can be racey, so this can happen, if so, just continue
                log.debug("Completed offset {} lower than current highest continuous offset {} - must have been completed while previous continuous blocks were being examined", thisOffset, previousHighestContinuous);
//                continue; - can't skip #handleResult
            } else {


                // We already know this partition's continuous range has been broken, no point checking
                Boolean partitionSoFarIsContinuous = partitionNowFormsAContinuousBlock.get(tp);
                if (partitionSoFarIsContinuous != null && !partitionSoFarIsContinuous) {
                    // previously we found non continuous block so we can skip
//                    continue; // to next record - can't skip #handleResult
                } else {
                    // we can't know, we have to keep digging


                    boolean thisOffsetIsFailed = !work.isUserFunctionSucceeded();
                    partitionsSeenForLogging.add(tp);

                    if (thisOffsetIsFailed) {
                        // simpler path
                        // work isn't successful. Is this the first? Is there a gap previously? Perhaps the gap doesn't exist (skipped offsets in partition)
                        Boolean previouslyContinuous = partitionNowFormsAContinuousBlock.get(tp);
                        partitionNowFormsAContinuousBlock.put(tp, false); // this partitions continuous block
                    } else {

                        // does it form a new continuous block?

                        // queue this offset belongs to
                        NavigableMap<Long, WorkContainer<K, V>> commitQueue = partitionCommitQueues.get(tp);

                        boolean continuous = true;
                        if (thisOffset != previousHighestContinuous + 1) {
                            // do the entries in the gap exist in our partition queue? or are they skipped in the source log?
                            long rangeBase = (previousHighestContinuous < 0) ? 0 : previousHighestContinuous + 1;
                            Range offSetRangeToCheck = new Range(rangeBase, thisOffset);
                            log.trace("Gap detected between {} and {}", rangeBase, thisOffset);
                            for (var offsetToCheck : offSetRangeToCheck) {
                                WorkContainer<K, V> workToExamine = commitQueue.get((long) offsetToCheck);
                                if (workToExamine != null) {
                                    if (!workToExamine.isUserFunctionSucceeded()) {
                                        log.trace("Record exists {} but is incomplete - breaks continuity finish early", workToExamine);
                                        partitionNowFormsAContinuousBlock.put(tp, false);
                                        continuous = false;
                                        break;
                                    } else if (workToExamine.isUserFunctionSucceeded() && !workToExamine.isNotInFlight()) {
                                        log.trace("Work {} comparing to succeeded work still in flight: {} (but not part of this batch)", work.offset(), workToExamine);
//                                        continue;  - can't skip #handleResult
                                    } else {
                                        // counts as continuous, just isn't in this batch - previously successful but there used to be gaps
                                        log.trace("Work not in batch, but seen now in commitQueue as succeeded {}", workToExamine);
                                    }
                                } else {
                                    // offset doesn't exist in commit queue, can assume doesn't exist in source, or is completed
                                    log.trace("Work offset {} checking against offset {} missing from commit queue, assuming doesn't exist in source", work.offset(), offsetToCheck);
                                }
                            }

                        }
                        if (continuous) {
                            partitionNowFormsAContinuousBlock.put(tp, true);
                            if (!originalMarks.containsKey(tp)) {
                                Long previousOffset = partitionOffsetHighestContinuousSucceeded.get(tp);
                                originalMarks.put(tp, previousOffset);
                            }
                            partitionOffsetHighestContinuousSucceeded.put(tp, thisOffset);
                        } else {
                            partitionNowFormsAContinuousBlock.put(tp, false);
//                        Long old = partitionOffsetHighestContinuousCompleted.get(tp);
                        }
//                    else {
//                        // easy, yes it's continuous, as there's no gap from previous highest
//                        partitionNowFormsAContinuousBlock.put(tp, true);
//                        partitionOffsetHighestContinuousCompleted.put(tp, thisOffset);
//                    }
                    }
                }
            }

            //
            {
                handleFutureResult(work);
            }

        }
        for (final TopicPartition tp : partitionsSeenForLogging) {
            Long oldOffset = originalMarks.get(tp);
            Long newOffset = partitionOffsetHighestContinuousSucceeded.get(tp);
            log.debug("Low water mark (highest continuous completed) for partition {} moved from {} to {}, highest succeeded {}",
                    tp, oldOffset, newOffset, partitionOffsetHighestSucceeded.get(tp));
        }
    }

    /**
     * Update highest Succeeded seen so far
     */
    private void updateHighestSucceededOffsetSoFar(final WorkContainer<K, V> work) {
        //
        TopicPartition tp = work.getTopicPartition();
        Long highestCompleted = partitionOffsetHighestSucceeded.getOrDefault(tp, -1L);
        long thisOffset = work.getCr().offset();
        if (thisOffset > highestCompleted) {
            log.trace("Updating highest completed - was: {} now: {}", highestCompleted, thisOffset);
            partitionOffsetHighestSucceeded.put(tp, thisOffset);
        }
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

}
