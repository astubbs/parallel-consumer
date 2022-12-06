package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

import static io.confluent.csid.utils.JavaUtils.isGreaterThan;
import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static lombok.AccessLevel.PRIVATE;

/**
 * Models the queue of work to be processed, based on the {@link ProcessingOrder} modes.
 *
 * @author Antony Stubbs
 */
@Slf4j
@RequiredArgsConstructor
@EqualsAndHashCode
public class ProcessingShard<K, V> implements Comparable<ProcessingShard<K, V>> {

    /**
     * Map of offset to WorkUnits.
     * <p>
     * Uses a ConcurrentSkipListMap instead of a TreeMap as under high pressure there appears to be some concurrency
     * errors (missing WorkContainers). This is addressed in PR#270.
     * <p>
     * Is a Map because need random access into collection, as records don't always complete in order (i.e. UNORDERED
     * mode).
     */
    @Getter
    private final NavigableMap<Long, WorkContainer<K, V>> entries = new ConcurrentSkipListMap<>();

    // todo generics
    @Getter(PRIVATE)
    private final ShardKey<Object> key;

    private final ParallelConsumerOptions<?, ?> options;

    private final PartitionStateManager<K, V> pm;

    public boolean workIsWaitingToBeProcessed() {
        return entries.values().parallelStream()
                .anyMatch(kvWorkContainer -> kvWorkContainer.isAvailableToTakeAsWork());
    }

    public void addWorkContainer(WorkContainer<K, V> wc) {
        long key = wc.offset();
        if (entries.containsKey(key)) {
            log.debug("Entry for {} already exists in shard queue, dropping record", wc);
        } else {
            entries.put(key, wc);
        }
    }

    public void onSuccess(WorkContainer<?, ?> wc) {
        // remove work from shard's queue
        entries.remove(wc.offset());
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public long getCountOfWorkAwaitingSelection() {
        return entries.values().stream()
                // todo missing pm.isBlocked(topicPartition) ?
                .filter(WorkContainer::isAvailableToTakeAsWork)
                .count();
    }

    public long getCountOfWorkTracked() {
        return entries.size();
    }

    public long getCountWorkInFlight() {
        return entries.values().stream()
                .filter(WorkContainer::isInFlight)
                .count();
    }

    public WorkContainer<K, V> remove(long offset) {
        return entries.remove(offset);
    }

//    public Batch<K, V> pollBatch() {
//        var quantity = options.getBatchSize() * 100;
//        var workIfAvailable = getWorkIfAvailable(quantity);
//
//        if (workIfAvailable.isEmpty()) {
//            return null;
//        }
//
//        return new Batch<>(workIfAvailable);
//    }

    private void addToSlowWorkMaybe(Set<WorkContainer<?, ?>> slowWork, WorkContainer<?, ?> workContainer) {
        var msgTemplate = "Can't take as work: Work ({}). Must all be true: Delay passed= {}. Is not in flight= {}. Has not succeeded already= {}. Time spent in execution queue: {}.";
        Duration timeInFlight = workContainer.getTimeInFlight();
        var msg = msg(msgTemplate, workContainer, workContainer.isDelayPassed(), workContainer.isNotInFlight(), !workContainer.isUserFunctionSucceeded(), timeInFlight);
        Duration slowThreshold = options.getThresholdForTimeSpendInQueueWarning();
        if (isGreaterThan(timeInFlight, slowThreshold)) {
            slowWork.add(workContainer);
            log.trace("Work has spent over " + slowThreshold + " in queue! " + msg);
        } else {
            log.trace(msg);
        }
    }

    private boolean isOrderRestricted() {
        return options.getOrdering() != UNORDERED;
    }

    // audit synchronized usage
    // todo remove synchronized
    public synchronized WorkRequestResult getWorkIfAvailable(int workToGetDelta) {
        log.trace("Looking for work on shardQueueEntry: {}", getKey());

        var slowWork = new HashSet<WorkContainer<?, ?>>();
        var workTaken = new ArrayList<WorkContainer<K, V>>();

        var iterator = entries.entrySet().iterator();
        while (workTaken.size() < workToGetDelta && iterator.hasNext()) {
            var workContainer = iterator.next().getValue();

            if (pm.couldBeTakenAsWork(workContainer)) {

                if (workContainer.isAvailableToTakeAsWork()) {
                    log.trace("Taking {} as work", workContainer);
                    workContainer.onQueueingForExecution();
                    workTaken.add(workContainer);
                } else {
                    addToSlowWorkMaybe(slowWork, workContainer);
                }

                if (isOrderRestricted()) {
                    // can't take any more work from this shard, due to ordering restrictions
                    // processing blocked on this shard, continue to next shard
                    log.trace("Processing by {}, so have cannot get more messages on this ({}) shardEntry.", this.options.getOrdering(), getKey());
                    break;
                }
            }
        }

        if (workTaken.size() > workToGetDelta) {
            log.trace("Work taken ({}) exceeds max ({})", workTaken.size(), workToGetDelta);
        }

        return new WorkRequestResult(workTaken, slowWork);
    }

    @Value
    public class WorkRequestResult {

        ArrayList<WorkContainer<K, V>> workTaken;

        HashSet<WorkContainer<?, ?>> slowWork;

        public boolean hasWork() {
            return !workTaken.isEmpty();
        }
    }

    @Override
    public int compareTo(@NonNull ProcessingShard<K, V> o) {
        return Comparator.nullsFirst(Comparator.<ProcessingShard<K, V>, ShardKey<Object>>comparing(ProcessingShard::getKey))
                .compare(this, o);
    }


    //    @Override
//    public Queue<WorkContainer<K, V>> queue() {
//        return null;
//    }
//
//    @Override
//    public Queue<WorkContainer<K, V>> queue() {
//        return new NoOpQueue<K, V>() {
//
//            @Override
//            public WorkContainer<K, V> poll() {
//                var quantity = options.getBatchSize();
//                return getWorkIfAvailable(quantity);
//            }
//        };
//    }

//        return new Queue<>() {
//
//            @Override
//            public boolean containsAll(Collection<?> c) {
//                return false;
//            }
//
//            @Override
//            public boolean removeAll(Collection<?> c) {
//                return false;
//            }
//
//            @Override
//            public boolean retainAll(Collection<?> c) {
//                return false;
//            }
//
//            private interface MyExcludes {
//
//                boolean containsAll(Collection<?> c);
//
//                boolean removeAll(Collection<?> c);
//
//                boolean retainAll(Collection<?> c);
//
//                WorkContainer toArray(WorkContainer[] a);
//
//            }
//
//
//            // Use Lombok's dynamic delegate feature to implement the MyInterface interface
//            @Delegate(excludes = MyExcludes.class)
//            final Queue<WorkContainer<K, V>> delegate = new ArrayDeque<>();
//
//        };


}
