package io.confluent.parallelconsumer;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class KafkaQueueManager<K, V> {


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


    public int getNumberOfEntriesInPartitionQueues() {
        int count = 0;
        for (var e : this.partitionCommitQueues.entrySet()) {
            count += e.getValue().size();
        }
        return count;
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
        boolean internalQueuesNotEmpty = hasWorkInMailboxes();
        return partitionWorkRemainingCount > 0 || internalQueuesNotEmpty;
    }

    boolean isRecordsAwaitingToBeCommitted() {
        // todo could be improved - shouldn't need to count all entries if we simply want to know if there's > 0
        int partitionWorkRemainingCount = getNumberOfEntriesInPartitionQueues();
        return partitionWorkRemainingCount > 0;
    }


    /**
     * TODO: This entire loop could be possibly redundant, if we instead track low water mark, and incomplete offsets as
     * work is submitted and returned.
     * <p>
     * todo: refactor into smaller methods?
     */
    <R> Map<TopicPartition, OffsetAndMetadata> findCompletedEligibleOffsetsAndRemove(boolean remove) {
        if (!isDirty()) {
            // nothing to commit
            return UniMaps.of();
        }

        Map<TopicPartition, OffsetAndMetadata> offsetMetadataToCommit = new HashMap<>();
        int totalPartitionQueueSizeForLogging = 0;
        int removed = 0;
        log.trace("Scanning for in order in-flight work that has completed...");
        int totalOffsetMetaCharacterLengthUsed = 0;
        for (final var partitionQueueEntry : partitionCommitQueues.entrySet()) {
            //
            totalPartitionQueueSizeForLogging += partitionQueueEntry.getValue().size();
            var partitionQueue = partitionQueueEntry.getValue();

            var workToRemove = new LinkedList<WorkContainer<K, V>>();
            var incompleteOffsets = new LinkedHashSet<Long>();  // we only need to know the full incompletes when we do this scan, so find them only now, and discard

            //
            long lowWaterMark = -1;
            // can't commit this offset or beyond, as this is the latest offset that is incomplete
            // i.e. only commit offsets that come before the current one, and stop looking for more
            boolean iteratedBeyondLowWaterMarkBeingLowestCommittableOffset = false;

            //
            TopicPartition topicPartitionKey = partitionQueueEntry.getKey();
            log.trace("Starting scan of partition: {}", topicPartitionKey);
            Long firstIncomplete = null;
            Long baseOffset = partitionOffsetHighestContinuousSucceeded.get(topicPartitionKey);
            for (final var offsetAndItsWorkContainer : partitionQueue.entrySet()) {
                // ordered iteration via offset keys thanks to the tree-map
                WorkContainer<K, V> work = offsetAndItsWorkContainer.getValue();
                boolean inFlight = !work.isNotInFlight(); // check is part of this mailbox set / not in flight
                if (inFlight) {
                    log.trace("Skipping comparing to work still in flight: {}", work);
                    continue;
                }
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
                        offsetMetadataToCommit.put(topicPartitionKey, offsetData);
                    } else if (work.isUserFunctionSucceeded() && iteratedBeyondLowWaterMarkBeingLowestCommittableOffset) {
                        // todo lookup the low water mark and include here
                        log.trace("Offset {} is complete and succeeded, but we've iterated past the lowest committable offset ({}). Will mark as complete in the offset map.",
                                work.getCr().offset(), lowWaterMark);
                        // no-op - offset map is only for not succeeded or completed offsets
//                        // mark as complete complete so remove from work
//                        workToRemove.add(work);
                    } else {
                        log.trace("Offset {} is complete, but failed processing. Will track in offset map as not complete. Can't do normal offset commit past this point.", work.getCr().offset());
                        iteratedBeyondLowWaterMarkBeingLowestCommittableOffset = true;
                        incompleteOffsets.add(offset);
                        if (firstIncomplete == null)
                            firstIncomplete = offset;
                    }
                } else {
                    lowWaterMark = work.offset();

                    // work not complete - either successfully or unsuccessfully
                    iteratedBeyondLowWaterMarkBeingLowestCommittableOffset = true;
                    log.trace("Offset ({}) is incomplete, holding up the queue ({}) of size {}.",
                            work.getCr().offset(),
                            partitionQueueEntry.getKey(),
                            partitionQueueEntry.getValue().size());
                    incompleteOffsets.add(offset);
                    if (firstIncomplete == null)
                        firstIncomplete = offset;
                }
            }

//            {
//                OffsetSimultaneousEncoder precomputed = partitionContinuousOffsetEncoders.get(topicPartitionKey);
//                byte[] bytes = new byte[0];
//                try {
//                    Long currentHighestCompleted = partitionOffsetHighestSucceeded.get(topicPartitionKey) + 1;
//                    if (firstIncomplete != null && baseOffset != firstIncomplete - 1) {
//                        log.warn("inconsistent base new vs old {} {} diff: {}", baseOffset, firstIncomplete, firstIncomplete - baseOffset);
//                        if (baseOffset > firstIncomplete) {
//                            log.warn("batch computed is higher than this scan??");
//                        }
//                    }
//                    Long highestSeen = partitionOffsetHighestSeen.get(topicPartitionKey); // we don't expect these to be different
//                    if (currentHighestCompleted != highestSeen) {
//                        log.debug("New system upper end vs old system {} {} (delta: {})", currentHighestCompleted, highestSeen, highestSeen - currentHighestCompleted);
//                    }
//
////                    precomputed.runOverIncompletes(incompleteOffsets, baseOffset, currentHighestCompleted);
//                    precomputed.serializeAllEncoders();
//
//                    // make this a field instead - has no state?
//                    OffsetMapCodecManager<K, V> om = new OffsetMapCodecManager<>(this, this.consumerMgr);
//                    String smallestMetadataPacked = om.makeOffsetMetadataPayload(precomputed);
//
//                    totalOffsetMetaCharacterLengthUsed += smallestMetadataPacked.length();
//                    log.debug("comparisonOffsetPayloadString :{}:", smallestMetadataPacked);
//                    OffsetAndMetadata offsetWithExtraMap = new OffsetAndMetadata(baseOffset + 1, smallestMetadataPacked);
//                    offsetMetadataToCommit.put(topicPartitionKey, offsetWithExtraMap);
//                } catch (EncodingNotSupportedException e) {
//                    e.printStackTrace();
//                }

//            OffsetAndMetadata offsetAndMetadata = offsetMetadataToCommit.get(topicPartitionKey);
//            {
//                int offsetMetaPayloadSpaceUsed = getTotalOffsetMetaCharacterLength(offsetMetadataToCommit, totalOffsetMetaCharacterLengthUsed, incompleteOffsets, topicPartitionKey);
//                totalOffsetMetaCharacterLengthUsed += offsetMetaPayloadSpaceUsed;
//            }

            if (remove) {
                removed += workToRemove.size();
                for (var workContainer : workToRemove) {
                    var offset = workContainer.getCr().offset();
                    partitionQueue.remove(offset);
                }
            }

        }

        maybeStripOffsetPayload(offsetMetadataToCommit, totalOffsetMetaCharacterLengthUsed);

        log.debug("Scan finished, {} were in flight, {} completed offsets removed, coalesced to {} offset(s) ({}) to be committed",
                totalPartitionQueueSizeForLogging, removed, offsetMetadataToCommit.size(), offsetMetadataToCommit);
        return offsetMetadataToCommit;
    }

    /**
     * fastish
     *
     * @return
     */
    public boolean hasWorkInCommitQueues() {
        for (var e : this.partitionCommitQueues.entrySet()) {
            if (!e.getValue().isEmpty())
                return true;
        }
        return false;
    }

}
