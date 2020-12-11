package io.confluent.parallelconsumer;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Handles the incoming mail for {@link WorkManager}.
 */
@Slf4j
public class WorkMailBoxManager<K, V> {

    /**
     * The number of nested {@link ConsumerRecord} entries in the shared blocking mail box. Cached for performance.
     */
    private int sharedBoxNestedRecordCount;

    /**
     * The shared thread safe mail box.
     */
    private final LinkedBlockingQueue<ConsumerRecords<K, V>> workInbox = new LinkedBlockingQueue<>();

    /**
     * Mail box where mail is transferred to immediately.
     */
    private final CountingCRLinkedList<K, V> internalBatchMailQueue = new CountingCRLinkedList<>();

    /**
     * Queue of records flattened from the {@link #internalBatchMailQueue}.
     * <p>
     * This is needed because {@link java.util.concurrent.BlockingQueue#drainTo(Collection)} must drain to a collection
     * of the same type. We could have {@link BrokerPollSystem} do the flattening, but that would require many calls to
     * the Concurrent queue, where this only needs one. Also as we don't expect there to be that many elements in these
     * collections (as they contain large batches of records), the overhead will be small.
     */
    // TODO when partition state is also refactored, remove Getter
    @Getter
    private final Queue<ConsumerRecord<K, V>> internalFlattenedMailQueue = new LinkedList<>();

    /**
     * @return amount of work queued in the mail box, awaiting processing into shards, not exact
     */
    Integer getWorkQueuedInMailboxCount() {
        return sharedBoxNestedRecordCount +
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
            sharedBoxNestedRecordCount += records.count();
            workInbox.add(records);
        }
    }

    private void drainSharedMailbox() {
        synchronized (workInbox) {
            workInbox.drainTo(internalBatchMailQueue);
            sharedBoxNestedRecordCount = 0;
        }
    }

    /**
     * Take our inbound messages from the {@link BrokerPollSystem} and add them to our registry.
     *
     * @param requestedMaxWorkToRetrieve
     */
    public void processInbox(final int requestedMaxWorkToRetrieve) {
        drainSharedMailbox();

        // flatten
        while (!internalBatchMailQueue.isEmpty()) {
            ConsumerRecords<K, V> consumerRecords = internalBatchMailQueue.poll();
            log.debug("Flattening {} records", consumerRecords.count());
            for (final ConsumerRecord<K, V> consumerRecord : consumerRecords) {
                internalFlattenedMailQueue.add(consumerRecord);
            }
        }
    }


}
