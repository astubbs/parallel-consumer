package io.confluent.parallelconsumer;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static io.confluent.csid.utils.KafkaUtils.toTP;
import static io.confluent.csid.utils.StringUtils.msg;

@Slf4j
public class WorkMailBoxManager<K, V> {

    private int sharedBoxCount;

    private final LinkedBlockingQueue<ConsumerRecords<K, V>> workInbox = new LinkedBlockingQueue<>();

    private final CountingCRLinkedList<K, V> internalBatchMailQueue = new CountingCRLinkedList<>();

    // TODO when partition state is also refactored, remove Getter
    @Getter
    private final Queue<ConsumerRecord<K, V>> internalFlattenedMailQueue = new LinkedList<>();

    /**
     * @return amount of work queued in the mail box, awaiting processing into shards, not exact
     */
    Integer getWorkQueuedInMailboxCount() {
        return sharedBoxCount +
                internalBatchMailQueue.getNestedCount() +
                internalFlattenedMailQueue.size();
    }

    /**
     * Work must be registered in offset order
     * <p>
     * Thread safe for use by control and broker poller thread.
     *
     * @see WorkManager#success
     * @see WorkManager#raisePartitionHighWaterMark
     */
    public void registerWork(final ConsumerRecords<K, V> records) {
        synchronized (workInbox) {
            sharedBoxCount += records.count();
            workInbox.add(records);
        }
    }

    private void drainSharedMailbox() {
        synchronized (workInbox) {
            workInbox.drainTo(internalBatchMailQueue);
            sharedBoxCount = 0;
        }
    }

    /**
     * Take our inbound messages from the {@link BrokerPollSystem} and add them to our registry.
     *
     * @param requestedMaxWorkToRetrieve
     */
    public void processInbox(final int requestedMaxWorkToRetrieve) {
        drainSharedMailbox();

        flatten();


        if (requestedMaxWorkToRetrieve < 1) {
            // none requested
            return;
        }

        //
//        int inFlight = getNumberOfEntriesInPartitionQueues();
//        int max = getMaxToGoBeyondOffset();
//        int gap = max - inFlight;
        int gap = requestedMaxWorkToRetrieve;
        int taken = 0;

//        log.debug("Will register {} (max configured: {}) records of work ({} already registered)", gap, max, inFlight);
        Queue<ConsumerRecord<K, V>> internalFlattenedMailQueue = wmbm.getInternalFlattenedMailQueue();
        log.debug("Will attempt to register and get {} requested records, {} available", requestedMaxWorkToRetrieve, internalFlattenedMailQueue.size());

        // process individual records
        while (taken < gap && !internalFlattenedMailQueue.isEmpty()) {
            ConsumerRecord<K, V> poll = internalFlattenedMailQueue.poll();
            boolean takenAsWork = processInbox(poll);
            if (takenAsWork) {
                taken++;
            }
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

    private void flatten() {
        // flatten
        while (!internalBatchMailQueue.isEmpty()) {
            ConsumerRecords<K, V> consumerRecords = internalBatchMailQueue.poll();
            log.debug("Flattening {} records", consumerRecords.count());
            for (final ConsumerRecord<K, V> consumerRecord : consumerRecords) {
                internalFlattenedMailQueue.add(consumerRecord);
            }
        }
    }


    /**
     * @return true if the record was taken, false if it was skipped (previously successful)
     */
    private boolean processInbox(final ConsumerRecord<K, V> rec) {
        if (isRecordPreviouslyProcessedSuccessfully(rec)) {
            log.trace("Record previously processed, skipping. offset: {}", rec.offset());
            return false;
        } else {
            Object shardKey = computeShardKey(rec);
            long offset = rec.offset();
            TopicPartition tp = toTP(rec);

            Integer currentPartitionEpoch = partitionsAssignmentEpochs.get(tp);
            if (currentPartitionEpoch == null) {
                throw new InternalRuntimeError(msg("Received message for a partition which is not assigned: {}", rec));
            }
            var wc = new WorkContainer<>(currentPartitionEpoch, rec);

            raisePartitionHighestSeen(offset, tp);

            //
            checkPreviousLowWaterMarks(wc);
            checkHighestSucceededSoFar(wc);

            //
            prepareContinuousEncoder(wc);

            //
            processingShards.computeIfAbsent(shardKey, (ignore) -> new ConcurrentSkipListMap<>()).put(offset, wc);

            partitionCommitQueues.computeIfAbsent(tp, (ignore) -> new ConcurrentSkipListMap<>()).put(offset, wc);

            return true;
        }
    }


    private boolean isRecordPreviouslyProcessedSuccessfully(ConsumerRecord<K, V> rec) {
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

}
