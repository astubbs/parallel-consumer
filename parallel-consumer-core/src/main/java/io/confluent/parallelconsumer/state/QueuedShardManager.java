package io.confluent.parallelconsumer.state;

import io.confluent.parallelconsumer.internal.PCModule;
import lombok.Value;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A blocking queue implementation, backed by a map of non-blocking queues.
 *
 * @author Antony Stubbs
 * @author CoPilot
 * @author ChatCGPT-3
 */
@Value
public class QueuedShardManager<K, V> extends ShardManager<K, V> {

    //
//    /**
//     * Map of BlockingQueue entries, keyed on a unique identifier.
//     */
//    ConcurrentSkipListMap<ShardKey<?>, Queue<Object>> queueMap;

    /**
     * A map of locks for the queues in the queue map.
     */
    Map<ShardKey<?>, ReentrantLock> lockMap = new ConcurrentHashMap<>();

    /**
     * Map of entries that have been polled by each thread, keyed on the thread's ID.
     */
    Map<Long, ShardKey> polledEntries;

    /**
     * Atomic integer to keep track of the last position in the queueMap that was polled by each thread.
     */
    AtomicInteger lastPosition;

    /**
     * Constructs a new BlockingQueue.
     */
    public QueuedShardManager(PCModule<K, V> module) {
        super(module, module.workManager());
//        queueMap = super.getProcessingShards();

        // Initialize the polledEntries map
        polledEntries = new ConcurrentHashMap<>();

        // Initialize the lastPosition atomic integer
        lastPosition = new AtomicInteger();
    }

    @Override
    public void addWorkContainer(long epochOfInboundRecords, ConsumerRecord<K, V> aRecord) {
        super.addWorkContainer(epochOfInboundRecords, aRecord);
        ShardKey<?> shardKey = computeShardKey(aRecord);
        lockMap.computeIfAbsent(shardKey, k -> {
            // Create the corresponding ReentrantLock
            return new ReentrantLock();
        });

        // Wake up any threads that are waiting in the take method
        synchronized (getQueueMap()) {
            // notifyAll in the put method is not necessary in this case because there will only be one thread
            // waiting on the queueMap monitor at any given time. Using notify is sufficient to wake up that thread and
            // avoid thread starvation.
            //noinspection CallToNotifyInsteadOfNotifyAll
            getQueueMap().notify();
        }
    }

    /**
     * Removes an element from the queue and returns it. This method blocks until an element is available.
     *
     * @return The removed element from the queue.
     * @throws InterruptedException If the current thread is interrupted while waiting for an element to be available.
     */
    // todo needs to return a batch
    public Batch<K, V> take() throws InterruptedException {
        // Get the current thread's ID
        var threadId = Thread.currentThread().getId();
        var queueMap = getQueueMap();

        // Loop until an element is available
        while (true) {
            // Check if the thread has already polled an entry in the queueMap
            var polledEntry = polledEntries.get(threadId);

            // If an entry has not been polled, start from the beginning of the queueMap
            if (polledEntry == null) {
                polledEntry = queueMap.keySet().iterator().next();
            }

            // Get an iterator for the queueMap, starting from the polledEntry
            var iterator = queueMap.tailMap(polledEntry).keySet().iterator();

            // Loop through the queueMap, starting from the polledEntry
            while (iterator.hasNext()) {
                // Get the next key from the iterator
                var key = iterator.next();

                // Get the QueueLock for the key
                var queueLock = lockMap.get(key);

                // Acquire the lock on the QueueLock
                synchronized (queueLock) {
                    // Check if the QueueLock's queue is empty
                    var queue = queueMap.get(key);
                    if (queue.isEmpty()) {
                        // If the queue is empty, release the lock and continue to the next entry in the queueMap
                        continue;
                    }

                    // If the queue is not empty, remove an element from the queue and return it
                    var element = queue.pollBatch();

                    // Update the polledEntry for the current thread
                    polledEntries.put(threadId, key);

                    // Return the removed element
//                    return new Batch<>(element);
                    return element;
                }
            }

            // If all entries in the queueMap have been polled and are empty, wait for a new element to be added to the queue
            synchronized (getQueueMap()) {
                // In the take method, the call to wait on the queueMap monitor is unconditional because it is
                // only called if no non-empty queue was found in the map. This means that there will only be one
                // thread waiting on the queueMap monitor at any given time, and the put method will always call
                // notify (or notifyAll) to wake up that thread when a new element is added to any queue in the map.
                // If the queue is not empty, remove an element from the queue and return it

                // Using an unconditional wait in this way is safe because the put method always calls notify (or
                // notifyAll) to wake up the waiting thread. This ensures that the waiting thread will not be blocked
                // indefinitely, and that it will only wait for as long as it takes for a new element to be added to
                // one of the queues in the map.
                //noinspection WaitOrAwaitWithoutTimeout,UnconditionalWait
                getQueueMap().wait(); // NOSONAR
            }
        }
    }

    private ConcurrentSkipListMap<ShardKey<?>, ProcessingShard<K, V>> getQueueMap() {
//    private ConcurrentSkipListMap<ShardKey<?>, Queue<WorkContainer<K, V>>> getQueueMap() {

        var processingShards = super.getProcessingShards();
        // map values into queues

//        // todo too slow for loop call
//        return processingShards.entrySet().stream().collect(
//                ConcurrentSkipListMap::new,
//                (m, e) -> m.put(e.getKey(), e.getValue().queue()),
//                Map::putAll
//        );

        return processingShards;
    }

    public int size() {
        return getQueueMap().values().stream().mapToInt(value -> Math.toIntExact(value.getCountOfWorkAwaitingSelection())).sum();
    }

    @Value
    private static class QueueLock<T> {

        /**
         * Threadsafe for {@link #size()} access
         */
        BlockingQueue<T> queue;

        ReentrantLock lock;
    }
}
