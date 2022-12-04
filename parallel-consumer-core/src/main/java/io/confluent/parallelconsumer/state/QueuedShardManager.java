package io.confluent.parallelconsumer.state;

import io.confluent.parallelconsumer.internal.PCModule;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A blocking queue implementation, backed by a map of non-blocking queues.
 *
 * @author Antony Stubbs
 * @author CoPilot
 * @author ChatCGPT-3
 */
@Slf4j
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

    static final Object monitor = new Object();

    /**
     * Atomic integer to keep track of the last position in the queueMap that was polled by each thread.
     */
    AtomicInteger lastPosition;

    /**
     * Map of entries that have been polled by each thread, keyed on the thread's ID.
     */
    Map<Long, ShardKey<?>> lastPolledEntriesPerThread;

    /**
     * A map of entries that have been polled by each thread, keyed on the thread's ID. This is used to ensure that a
     * given thread does not poll the same entry multiple times and to avoid thread starvation, where a thread may
     * always be blocked on the same entry in the queueMap and never get a chance to poll other entries.
     */
    ConcurrentHashMap<Long, ShardKey<?>> polledEntries = new ConcurrentHashMap<>();

    ReentrantLock newWorkLockMaker = new ReentrantLock();

    PCModule<K, V> module;

    @NonFinal
    Condition newWorkEvent = newWorkLockMaker.newCondition();

    /**
     * Shards in this queue:
     * <p>
     * - always have at least one entry,
     * <p>
     * - and aren't being modified by any other thread.
     * <p>
     * It must only contain Shards once.
     */
//    BlockingQueue<ProcessingShard<K, V>> shardQueue = new LinkedTransferQueue<>();
    ThreadLocal<BlockingQueue<ProcessingShard<K, V>>> threadLocalShardQueue = new ThreadLocal<>();

    // todo does this have to be concurrent?
    Map<Long, BlockingQueue<ProcessingShard<K, V>>> shardQueueMap = new ConcurrentHashMap<>();

    /**
     * Constructs a new BlockingQueue.
     */
    public QueuedShardManager(PCModule<K, V> module) {
        super(module);

        this.module = module;

//        queueMap = super.getProcessingShards();

        // Initialize the polledEntries map
        lastPolledEntriesPerThread = new ConcurrentHashMap<>();

        // Initialize the lastPosition atomic integer
        lastPosition = new AtomicInteger();
    }

    // todo sneaky
    @SneakyThrows
    @Override
    public void addWorkContainer(long epochOfInboundRecords, ConsumerRecord<K, V> aRecord) {
//        synchronized (monitor) {
        ShardKey<?> shardKey = computeShardKey(aRecord);

        super.addWorkContainer(epochOfInboundRecords, aRecord);

        var processingShard = processingShards.get(shardKey);
        module.workerPool().onWorkAdded(processingShard);

//
//        // todo clean up
//        module.workerPool().distributeShards(processingShard);
//
//        maybeAddShardToQueue(shardKey);

//        lockMap.computeIfAbsent(shardKey, k -> {
//            // Create the corresponding ReentrantLock
//            return new ReentrantLock();
//        });


        // Wake up any threads that are waiting in the take method
//        var queueMap = getQueueMap();

        // notifyAll in the put method is not necessary in this case because there will only be one thread
        // waiting on the queueMap monitor at any given time. Using notify is sufficient to wake up that thread and
        // avoid thread starvation.
        //noinspection
//            monitor.notifyAll(); // todo notifies too indiscriminately?
//        }


    }

    private void maybeAddShardToQueue(ShardKey<?> shardKey) {
        var shard = processingShards.get(shardKey);

        BlockingQueue<ProcessingShard<K, V>> shardQueue = getShardQueueFromMap();

        if (!shardQueue.contains(shard)) {
            shardQueue.add(shard);
        }
    }

    private BlockingQueue<ProcessingShard<K, V>> getShardQueueFromMap() {
        var shardQueue = shardQueueMap.computeIfAbsent(Thread.currentThread().getId(),
                aLong -> new LinkedBlockingQueue<>());
        return shardQueue;
    }

    /**
     * Removes an element from the queue and returns it. This method blocks until an element is available.
     *
     * @return The removed element from the queue.
     * @throws InterruptedException If the current thread is interrupted while waiting for an element to be available.
     */
    // todo needs to return a batch
    public List<WorkContainer<K, V>> take() throws InterruptedException {

//        var startKey = getStartKey();

//        while (true) {
        var element = inner();

        // Return the removed element
        return element;
//        }

    }

    private List<WorkContainer<K, V>> inner() throws InterruptedException {
//        var threadId = Thread.currentThread().getId();
        List<WorkContainer<K, V>> element = null;
        while (element == null) {

//            var lock = new Semaphore(1);
//            lock.
//            element = tryOneIteration(threadId);
            element = tryOneIterationQueueRemoval();

//            if (element == null) {

//            ReentrantLock lock = null;


            // If all entries in the queueMap have been polled and are empty, wait for a new element to be added to the queue
//                synchronized (monitor) {
            // todo move out of sync, use Lock? - fix race condition between adding, and notifying
//                    element = tryOneIteration(threadId);

            // In the take method, the call to wait on the queueMap monitor is unconditional because it is
            // only called if no non-empty queue was found in the map. This means that there will only be one
            // thread waiting on the queueMap monitor at any given time, and the put method will always call
            // notify (or notifyAll) to wake up that thread when a new element is added to any queue in the map.
            // If the queue is not empty, remove an element from the queue and return it

            // Using an unconditional wait in this way is safe because the put method always calls notify (or
            // notifyAll) to wake up the waiting thread. This ensures that the waiting thread will not be blocked
            // indefinitely, and that it will only wait for as long as it takes for a new element to be added to
            // one of the queues in the map.
//            if (element == null) {
//                log.debug("No work found, going to sleep until notified");
//                //noinspection WaitOrAwaitWithoutTimeout,UnconditionalWait
////                        monitor.wait(1000); // NOSONAR
//                newWorkLockMaker.lock();
//                try {
//                    boolean wasSignaled = newWorkEvent.await(1, SECONDS);
//                } finally {
//                    newWorkLockMaker.unlock();
//                }
//            }
////                }
////            }
        }
        return element;
    }

    private List<WorkContainer<K, V>> tryOneIterationQueueRemoval() throws InterruptedException {
        // save it to the
        if (threadLocalShardQueue.get() == null) {
            var threadLocalQueue = getShardQueueFromMap();
            threadLocalShardQueue.set(threadLocalQueue);
        }


        int quantity = 100;

        var shardQueue = threadLocalShardQueue.get();

        while (true) {
            var shard = shardQueue.take();
            try {
                if (!shard.isEmpty()) {
                    var workIfAvailable = shard.getWorkIfAvailable(quantity);

                    if (!workIfAvailable.isEmpty()) {
//                        return new Batch<>(workIfAvailable);
                        return workIfAvailable;
                    }
                }
            } finally {
                // only add back to queue if not empty
                if (!shard.isEmpty()) {
                    shardQueue.add(shard);
                }
            }
        }
    }

//    /**
//     * Loop through the queueMap, starting from the startKey
//     */
//    private Batch<K, V> tryOneIteration(long threadId) {
//        var queueMap = getQueueMap();
//
//        var startKey = getStartKey();
//
////        // Get an iterator for the queueMap, starting from the startKey
////        Iterator<ShardKey<?>> shardIterator;
////        if (isEmpty(startKey)) {
////            shardIterator = queueMap.keySet().iterator();
////        } else {
////            var tailMap = queueMap.tailMap(startKey.get());
////            if (tailMap.isEmpty()) {
////                log.debug("Reached empty tail map, removing start marker: {}", startKey.get());
////                lastPolledEntriesPerThread.remove(threadId);
////                shardIterator = queueMap.keySet().iterator();
////            } else {
////                shardIterator = tailMap.keySet().iterator();
////            }
////        }
//
//        // iterate the entries using the thread id modulo, to avoid starvation - use an incrementing multiple offset
//        // from the base
//        var shardIterator = queueMap.keySet().iterator();
//
////        var lastPosition = this.lastPosition.get();
////        var position = lastPosition;
////        var size = queueMap.size();
////        var max = getOptions().getMaxConcurrency();
////        var offset = max % size;
////        var sets = size / max;
////
////        var listView = new ArrayList<ProcessingShard<K, V>>(queueMap.values());
////        var multipleOffset = offset * size;
//
//
////        Iterator<ShardKey<?>> shardIterator = queueMap.keySet().iterator();
//
//        Batch<K, V> element;
////        var base = 0;
////        int round = base;
//        while (shardIterator.hasNext()) {
////            var index =  % size;
////            round++;
//
//            // Get the next key from the iterator
////            var shardKey = listView.get(index);
//            var shardKey = shardIterator.next();
//
//            var shardOpt = Optional.ofNullable(processingShards.get(shardKey));
//
//            if (isEmpty(shardOpt)) {
////                log.error("Unexpected null shard for {}", shardKey);
//                continue;
//            }
//
//            // Check if shard is empty
//            var shard = shardOpt.get();
//            if (shard.isEmpty()) {
//                // If the shard is empty, release the lock and continue to the next entry in the queueMap
//                continue;
//            }
//
//            // Check if the shard has been polled by another thread
////            if (polledEntries.containsValue(shardKey)) {
////                // Skip this shard and try the next one
////                continue;
////            }
//
//            // Get the QueueLock for the key
//            var lock = lockMap.get(shardKey);
//
//
//            // If the lock is acquired
////            if (lock.tryLock()) {
//            try {
//                // Mark the shard as polled by the current thread
////                    polledEntries.put(threadId, shardKey);
//
//                element = shard.pollBatch();
//
//                // Update the startKey for the current thread
//                lastPolledEntriesPerThread.put(threadId, shardKey);
//
//            } finally {
//                // Release the lock
////                    lock.unlock();
//            }
//
//            // If we found an element, break out of the loop and remove the entry from the polledEntries map
//            if (element != null) {
//                log.debug("Found work {}", element);
//                return element;
//            }
////            } else {
////                log.debug("Could not acquire lock for {}", shardKey);
////            }
//        }
//
//        log.debug("Finished iterating all queues, no work found");
//
//        // Iteration complete, remove our marker from the polledEntries map, so that we will start from the beginning
//        // again
//        polledEntries.remove(threadId);
//
//        return null;
//    }

    // todo remove
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

    private Optional<ShardKey<?>> getStartKey() {
        // Get the current thread's ID
        var threadId = Thread.currentThread().getId();

        var queueMap = getQueueMap();

        // Check if the thread has already polled an entry in the queueMap
        var startKey = lastPolledEntriesPerThread.get(threadId);

        // If an entry has not been polled, start from the beginning of the queueMap
        if (startKey == null) {
            var iterator = queueMap.keySet().iterator();
            if (iterator.hasNext()) {
                startKey = iterator.next();
            }
        }
        return Optional.ofNullable(startKey);
    }

    public int size() {
        return getQueueMap().values().stream().mapToInt(value -> Math.toIntExact(value.getCountOfWorkAwaitingSelection())).sum();
    }

    public void onFinishNewWork() {
//        newWorkLockMaker.lock();
//        try {
//            newWorkEvent.signalAll();
//        } finally {
//            newWorkLockMaker.unlock();
//        }
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
