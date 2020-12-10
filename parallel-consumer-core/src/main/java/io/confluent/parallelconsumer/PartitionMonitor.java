package io.confluent.parallelconsumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import pl.tlinkowski.unij.api.UniSets;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

import static io.confluent.csid.utils.KafkaUtils.toTP;
import static io.confluent.csid.utils.StringUtils.msg;

@Slf4j
public class PartitionMonitor<K, V> {

    private int numberOfAssignedPartitions;

    Map<TopicPartition, PartitionState> states = new HashMap<>();


    private List<TopicPartition> partitionsToRemove = new ArrayList<>();


    public PartitionState getState(TopicPartition tp){
        return states.get(tp);
    }


    /**
     * Load offset map for assigned partitions
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        incrementPartitionAssignmentEpoch(partitions);

        // init messages allowed state
        for (final TopicPartition partition : partitions) {
            partitionMoreRecordsAllowedToProcess.putIfAbsent(partition, true);
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
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
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


    /**
     * Called by other threads (broker poller) to be later removed inline by control.
     */
    private void registerPartitionsToBeRemoved(Collection<TopicPartition> partitions) {
        partitionsToRemove.addAll(partitions);
    }

    void removePartitionFromRecordsAndShardWork() {
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

    public void processInbox(final WorkContainer<K, V> wc) {
        ConsumerRecord<K, V> rec = wc.getCr();
        long offset = rec.offset();
        TopicPartition tp = toTP(rec);

        raisePartitionHighestSeen(offset, tp);

        //
        checkPreviousLowWaterMarks(wc);
        checkHighestSucceededSoFar(wc);

        //
        prepareContinuousEncoder(wc);

        //
        partitionCommitQueues.computeIfAbsent(tp, (ignore) -> new ConcurrentSkipListMap<>()).put(offset, wc);
    }

    public int getEpoch(final ConsumerRecord<K, V> rec, final TopicPartition tp) {
        Integer currentPartitionEpoch = partitionsAssignmentEpochs.get(tp);
        if (currentPartitionEpoch == null) {
            throw new InternalRuntimeError(msg("Received message for a partition which is not assigned: {}", rec));
        }
        return currentPartitionEpoch;
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


    public boolean isRecordPreviouslyProcessedSuccessfully(ConsumerRecord<K, V> rec) {
        long thisRecordsOffset = rec.offset();
        TopicPartition tp = new TopicPartition(rec.topic(), rec.partition());
        Set<Long> incompleteOffsets = this.partitionOffsetsIncompleteMetadataPayloads.getOrDefault(tp, new TreeSet<>());
        if (incompleteOffsets.contains(thisRecordsOffset)) {
            // record previously saved as having not been processed
            return false;
        } else {
            Long partitionHighestSeenRecord = partitionOffsetHighestSeen.getOrDefault(tp, MISSING_HIGHEST_SEEN);
            if (thisRecordsOffset <= partitionHighestSeenRecord) {
                // within the range of tracked offsets, but not in incompletes, so must have been previously completed
                return true;
            } else {
                // not in incompletes, and is a higher offset than we've ever seen, as we haven't recorded this far up, so must not have been processed yet
                return false;
            }
        }
    }

    public void onSuccess(final WorkContainer<K, V> wc) {
        getState(wc.getTopicPartition()).partitionCommitQueue.remove(wc.offset());
    }
}
