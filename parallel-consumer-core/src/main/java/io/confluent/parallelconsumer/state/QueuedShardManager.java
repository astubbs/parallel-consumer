package io.confluent.parallelconsumer.state;

import lombok.Value;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
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
public class QueuedShardManager<K, V, T> {

    /**
     * Map of BlockingQueue entries, keyed on a unique identifier.
     */
    ConcurrentHashMap<ShardKey, QueueLock<T>> queueMap;

    /**
     * Map of entries that have been polled by each thread, keyed on the thread's ID.
     */
    Map<Long, String> polledEntries;

    /**
     * Atomic integer to keep track of the last position in the queueMap that was polled by each thread.
     */
    AtomicInteger lastPosition;

    ShardManager<K, V> sm;

    /**
     * Constructs a new BlockingQueue.
     */
    public QueuedShardManager() {
        // Initialize the queueMap with a LinkedBlockingQueue for each entry
        queueMap = new ConcurrentHashMap<>();

        // Initialize the polledEntries map
        polledEntries = new ConcurrentHashMap<>();

        // Initialize the lastPosition atomic integer
        lastPosition = new AtomicInteger();
    }

    /**
     * Puts an element in the queue with the specified key.
     *
     * @param key     The key of the queue to put the element in.
     * @param element The element to put in the queue.
     */
    public void put(ShardKey key, T value) throws InterruptedException {
        queueMap.computeIfAbsent(key, k -> {
            // Create a new LinkedBlockingQueue and a corresponding ReentrantLock
            LinkedBlockingQueue<T> queue = new LinkedBlockingQueue<>();
            ReentrantLock lock = new ReentrantLock();

            // Return a QueueLock object that contains the queue and the lock
            return new QueueLock<>(queue, lock);
        }).queue.put(value);

        // Wake up any threads that are waiting in the take method
        synchronized (queueMap) {
            // notifyAll in the put method is not necessary in this case because there will only be one thread
            // waiting on the queueMap monitor at any given time. Using notify is sufficient to wake up that thread and
            // avoid thread starvation.
            //noinspection CallToNotifyInsteadOfNotifyAll
            queueMap.notify();
        }
    }

    /**
     * Removes an element from the queue and returns it. This method blocks until an element is available.
     *
     * @return The removed element from the queue.
     * @throws InterruptedException If the current thread is interrupted while waiting for an element to be available.
     */
    public T take() throws InterruptedException {
        // Poll each BlockingQueue in the queueMap until a non-empty queue is found
        T element = null;
        while (element == null) {
            // Try to acquire the lock for each BlockingQueue without blocking the current thread
            for (var entry : queueMap.entrySet()) {
                ReentrantLock lock = entry.getValue().getLock();

                // If the lock is acquired, try to poll the BlockingQueue
                if (lock.tryLock()) {
                    try {
                        BlockingQueue<T> queue = entry.getValue().getQueue();
                        element = queue.poll();
                    } finally {
                        // Release the lock
                        lock.unlock();
                    }

                    // If a non-empty queue was found, break out of the loop
                    if (element != null) {
                        break;
                    }
                }
                // if the lock can't be acquired, then the queue is being modified by another thread
            }

            // If no non-empty queue was found, block the current thread until any queue has an element available
            if (element == null) {
                synchronized (queueMap) {
                    // In the take method, the call to wait on the queueMap monitor is unconditional because it is
                    // only called if no non-empty queue was found in the map. This means that there will only be one
                    // thread waiting on the queueMap monitor at any given time, and the put method will always call
                    // notify (or notifyAll) to wake up that thread when a new element is added to any queue in the map.

                    // Using an unconditional wait in this way is safe because the put method always calls notify (or
                    // notifyAll) to wake up the waiting thread. This ensures that the waiting thread will not be blocked
                    // indefinitely, and that it will only wait for as long as it takes for a new element to be added to
                    // one of the queues in the map.
                    //noinspection WaitOrAwaitWithoutTimeout,UnconditionalWait
                    queueMap.wait(); // NOSONAR
                }
            }
        }

        // Return the element from the non-empty queue
        return element;
    }

    private List<WorkContainer<K, V>> findFirstAvailableEntry(int requestedMaxWorkToRetrieve) {
        return sm.getWorkIfAvailable(requestedMaxWorkToRetrieve);
//        return queueMap.values().stream().filter(ts -> !ts.isEmpty()).findFirst().orElse(EMPTY_QUEUE).poll();
    }

    public int size() {
        return queueMap.values().stream().mapToInt(value -> value.queue.size()).sum();
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
