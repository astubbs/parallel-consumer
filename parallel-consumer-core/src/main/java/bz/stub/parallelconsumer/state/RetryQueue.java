package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.kafka.common.utils.CloseableIterator;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Custom Sorted Set implementation for the retry queue. Difference from standard Sorted Set is that it allows
 * uniqueness constraint to be based on different set of fields than sorting logic. Uniqueness is based on Topic,
 * Partition and Offset of the WorkContainer while sorting is done based on RetryDueAt, Topic, Partition and Offset.
 * <p>
 * To enable that - Set is implemented using two Maps - uniqueness map and sorted map - uniqueness map is used to link
 * the unique keys to sorting keys while sorted map is used to store the sorted elements.
 * <p>
 * Implementation is thread safe and uses ReadWriteLock to allow multiple readers or single writer. Due to use of the
 * locks - it is important to close the Iterator in timely fashion to release the lock and prevent deadlocks.
 * <p>
 * Only a subset of Set methods are implemented - add, remove, clear and iterator - as those are only methods used by
 * the Parallel Consumer code.
 */
public class RetryQueue {

    // No accessors: these are lock-protected, and handing them out bypasses every lock in this class.
    private final Map<WorkContainerKey, WorkContainerSortKey> unique = new HashMap<>();
    private final NavigableMap<WorkContainerSortKey, WorkContainer<?, ?>> sorted;

    private final Comparator<WorkContainerSortKey> comparator = Comparator
            .comparing(WorkContainerSortKey::getRetryDueAt)
            .thenComparing(WorkContainerKey::getTopic)
            .thenComparing(WorkContainerKey::getPartition)
            .thenComparing(WorkContainerSortKey::getOffset);

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    public RetryQueue() {
        sorted = new TreeMap<>(comparator);
    }

    /**
     * Get the size of the set
     *
     * @return size of the set
     */
    public int size() {
        lock.readLock().lock();
        try {
            return unique.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Check if the set is empty
     *
     * @return true if the set is empty
     */
    public boolean isEmpty() {
        lock.readLock().lock();
        try {
            return unique.isEmpty();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Check if the set contains a work container - based on Topic, Partition and Offset
     */
    public boolean contains(final WorkContainer<?, ?> wc) {
        lock.readLock().lock();
        try {
            return unique.containsKey(WorkContainerKey.of(wc));
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Clear the set
     */
    public void clear() {
        lock.writeLock().lock();
        try {
            unique.clear();
            sorted.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Iterator over the sorted set. Access is guarded by Read lock - so it is really important for it to be closed in
     * timely fashion to release the lock.
     *
     * @return iterator
     */
    public RetryQueueIterator iterator() {
        lock.readLock().lock();
        return new RetryQueueIterator(lock, sorted.values().iterator());
    }

    /**
     * Add a work container to the set. Method follows Set.add() behaviour, returning true if the element was not
     * already present.
     *
     * @param workContainer to add
     * @return true if the element was not already present
     */
    public boolean add(final WorkContainer<?, ?> workContainer) {
        lock.writeLock().lock();
        try {
            WorkContainerKey newKey = WorkContainerKey.of(workContainer);
            WorkContainerSortKey newSortKey = WorkContainerSortKey.of(workContainer);

            WorkContainerSortKey existing = unique.put(newKey, newSortKey);
            if (existing != null) {
                sorted.remove(existing);
            }
            sorted.put(newSortKey, workContainer);
            // interface is set based, so return boolean indicating if element was not present.
            return existing == null;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Remove a work container from the set, WAITING for the write lock. Method follows Set.remove() behaviour,
     * returning true if the element was present.
     * <p>
     * <b>Only for threads that are allowed to wait</b> - which here means the controller thread. The broker-poll
     * thread must use {@link #tryRemove(String, int, long)} instead; {@link #tryRemove} carries the reasoning and
     * {@code ArchitectureTest.rebalanceCallbacksMustNotBlock} is the check that this method stays off the
     * rebalance callbacks.
     *
     * @param workContainer the container to remove
     * @return true if the element was present
     */
    public boolean remove(final WorkContainer<?, ?> workContainer) {
        lock.writeLock().lock();
        try {
            return removeWhileWriteLocked(WorkContainerKey.of(workContainer));
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Remove the entry for one record, but ONLY if the write lock is free at this instant - never waiting for it.
     * <p>
     * <b>This exists for the broker-poll thread inside a Kafka rebalance callback</b>, which runs inside
     * {@code consumer.poll()} with the whole group waiting on it: a wait there is spent out of
     * {@code max.poll.interval.ms} and can evict the member. {@link #remove} waits, and the wait is not
     * theoretical - {@link #iterator()} hands the READ lock to the controller thread for a whole scan, and this
     * lock is constructed fair, so an arriving writer queues behind that scan rather than interleaving with it.
     * {@code AbstractParallelEoSStreamProcessor.tryCommitOffsetsOnRevoke} is the same decision taken for the
     * commit lock, for the same reason (confluentinc#857).
     * <p>
     * <b>{@code tryLock()} barges deliberately.</b> {@link java.util.concurrent.locks.ReentrantReadWriteLock}'s
     * {@code tryLock()} ignores the fairness policy and takes a free lock even with waiters queued ahead of it -
     * documented behaviour, and the property wanted here. It returns immediately either way, which is the whole
     * contract: this method never waits.
     * <p>
     * <b>What barging costs, on the record</b> (review of astubbs/parallel-consumer#431): one rebalance
     * callback sweeps every revoked record in a loop, so a burst of these can repeatedly cut in front of a
     * controller thread already queued on the fair, BLOCKING {@link #remove}/{@link #add}. That inversion is
     * bounded by the burst - one callback's worth of records - and it is the trade taken knowingly, because the
     * alternative is the poll thread waiting, which is the defect this method exists to remove.
     * <p>
     * <b>Keyed by the record's coordinates rather than by a container</b>, so a caller can ask this queue FIRST
     * and only then take the record out of its shard. That ordering is what makes a refusal safe - see the
     * callers in {@code ShardManager.removeWorkFromShardFor} and
     * {@link ProcessingShard#removeStaleWorkContainersFromShard}, which own the reasoning about what a refusal
     * means for the shard.
     *
     * @return true if the lock was taken and the removal ran - whether or not an entry was actually present;
     *         false if the lock was contended, in which case NOTHING was changed
     */
    public boolean tryRemove(final String topic, final int partition, final long offset) {
        return tryRemove(WorkContainerKey.of(topic, partition, offset));
    }

    /**
     * @see #tryRemove(String, int, long)
     */
    public boolean tryRemove(final WorkContainer<?, ?> workContainer) {
        return tryRemove(WorkContainerKey.of(workContainer));
    }

    private boolean tryRemove(final WorkContainerKey key) {
        if (!lock.writeLock().tryLock()) {
            return false;
        }
        try {
            // Presence is deliberately not reported. Every caller is deciding whether it may go on to its own
            // paired removal, and for that decision "there was no entry" and "I removed the entry" are the same
            // answer - only "I could not look" differs.
            boolean ignoredWasPresent = removeWhileWriteLocked(key);
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * The two maps move together or not at all - callers hold the write lock.
     */
    private boolean removeWhileWriteLocked(final WorkContainerKey key) {
        WorkContainerSortKey existing = unique.remove(key);
        if (existing != null) {
            sorted.remove(existing);
        }
        return existing != null;
    }

    /**
     * Remove all specified work containers from the set. Method follows Set.removeAll() behaviour, returning true if
     * the set was modified.
     *
     * @param toRemove collection of work containers to remove
     * @return true if the set was modified
     */
    public <K, V> boolean removeAll(List<WorkContainer<K, V>> toRemove) {
        // GUARD ON THE CALLER'S OWN LIST, never on `unique`. The original fast path read
        // `unique.isEmpty()` with no lock held while writers mutate it under the write lock, so the
        // JMM permitted a stale `true` and this method could return false having removed nothing -
        // leaving a container in the retry queue while it was also in flight.
        //
        // Deleting it outright was the first fix and it was wrong about the cost: the claim was that
        // the guard "only saved one uncontended lock acquisition on an empty queue". It is not
        // uncontended and it is not once. `ProcessingShard.getWorkIfAvailable` calls this
        // unconditionally, `ShardManager` calls that in a loop over shards, and that runs every
        // control-loop tick - so it is one acquisition per shard per tick, bounded by key
        // cardinality under KEY ordering. The lock is FAIR, so it queues rather than barges and
        // blocks readers behind it, including `iterator()`, which holds the read lock for a whole
        // iteration.
        //
        // `toRemove.isEmpty()` restores the fast path with none of the hazard: the list is the
        // caller's, freshly built by `workTaken.stream().filter(...)`, shared with no other thread.
        // It is also the more precise question - nothing to remove means nothing to do, whatever the
        // queue currently holds.
        if (toRemove == null || toRemove.isEmpty()) {
            return false;
        }
        lock.writeLock().lock();
        try {
            List<WorkContainerKey> keysToRemove = toRemove.stream().map(WorkContainerKey::of).collect(Collectors.toList());
            boolean modified = false;
            for (WorkContainerKey wcKey : keysToRemove) {
                WorkContainerSortKey existing = unique.remove(wcKey);
                if (existing != null) {
                    sorted.remove(existing);
                    modified = true;
                }
            }
            return modified;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public WorkContainer<?, ?> last() {
        lock.readLock().lock();
        try {
            return sorted.isEmpty() ? null : sorted.lastEntry().getValue();
        } finally {
            lock.readLock().unlock();
        }
    }

    public WorkContainer<?, ?> first() {
        lock.readLock().lock();
        try {
            return sorted.isEmpty() ? null : sorted.firstEntry().getValue();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns a pair of values - current retry queue size and number of work containers that are ready to be retried
     * Method is combined to provide consistent view of the queue - both values calculated while locked with same read
     * lock preventing racing updates between two reads.
     *
     * @return pair of values - current retry queue size and number of work containers that are ready to be retried
     */
    public ParallelConsumer.Tuple<Integer, Long> getQueueSizeAndNumberReadyToBeRetried() {
        lock.readLock().lock();
        try {
            return new ParallelConsumer.Tuple<>(sorted.size(), getNumberOfFailedWorkReadyToBeRetried());
        } finally {
            lock.readLock().unlock();
        }
    }

    private long getNumberOfFailedWorkReadyToBeRetried() {
        long count = 0;
        //First check if last element is ready to be retried - in that case all before it are ready too
        if (Optional.ofNullable(sorted.isEmpty() ? null : sorted.lastEntry().getValue()).map(WorkContainer::isDelayPassed).orElse(false)) {
            return sorted.size();
        }
        Iterator<WorkContainer<?, ?>> iterator = sorted.values().iterator();
        while (iterator.hasNext()) {
            WorkContainer<?, ?> workContainer = iterator.next();
            //count all work containers that are ready to be retried but not inflight yet
            if (workContainer.isDelayPassed()) {
                count++;
            } else {
                // early stop since retryQueue is sorted by retryDueAt
                break;
            }
        }
        return count;
    }


    @Getter
    @EqualsAndHashCode
    static class WorkContainerKey {
        private final String topic;
        private final Integer partition;
        private final Long offset;

        private WorkContainerKey(String topic, Integer partition, Long offset) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
        }

        static WorkContainerKey of(WorkContainer<?, ?> workContainer) {
            return new WorkContainerKey(workContainer.getTopicPartition().topic(),
                    workContainer.getTopicPartition().partition(),
                    workContainer.getCr().offset());
        }

        /**
         * The same key, built from a record's coordinates rather than from a container - uniqueness here has
         * never been by container identity, so the two are interchangeable by construction.
         */
        static WorkContainerKey of(String topic, int partition, long offset) {
            return new WorkContainerKey(topic, partition, offset);
        }
    }

    @Getter
    @EqualsAndHashCode(callSuper = true)
    static class WorkContainerSortKey extends WorkContainerKey {
        private final Instant retryDueAt;

        private WorkContainerSortKey(final String topic, final Integer partition, final Long offset, Instant retryDueAt) {
            super(topic, partition, offset);
            this.retryDueAt = retryDueAt;
        }

        static WorkContainerSortKey of(WorkContainer<?, ?> workContainer) {
            return new WorkContainerSortKey(workContainer.getTopicPartition().topic(),
                    workContainer.getTopicPartition().partition(),
                    workContainer.getCr().offset(),
                    workContainer.getRetryDueAt());
        }
    }

    public static class RetryQueueIterator implements CloseableIterator<WorkContainer<?, ?>> {
        private final ReentrantReadWriteLock lock;
        private final Iterator<WorkContainer<?, ?>> wrapped;
        private boolean closed;

        public RetryQueueIterator(ReentrantReadWriteLock lock, Iterator<WorkContainer<?, ?>> wrapped) {
            this.lock = lock;
            this.wrapped = wrapped;
            this.closed = false;
        }

        @Override
        public void close() {
            lock.readLock().unlock();
            this.closed = true;
        }

        @Override
        public boolean hasNext() {
            if (closed) {
                throw new IllegalStateException("RetryQueueIterator is closed");
            }
            return wrapped.hasNext();
        }

        @Override
        public WorkContainer<?, ?> next() {
            if (closed) {
                throw new IllegalStateException("RetryQueueIterator is closed");
            }
            return wrapped.next();
        }
    }
}
