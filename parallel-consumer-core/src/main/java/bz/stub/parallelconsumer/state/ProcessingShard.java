package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2025 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.internal.RateLimiter;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static bz.stub.parallelconsumer.internal.utils.BackportUtils.toSeconds;
import static bz.stub.parallelconsumer.internal.utils.JavaUtils.isGreaterThan;
import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static lombok.AccessLevel.PRIVATE;

/**
 * Models the queue of work to be processed, based on the {@link ProcessingOrder} modes.
 *
 * @author Antony Stubbs
 * @see ShardManager
 */
@Slf4j
@RequiredArgsConstructor
public class ProcessingShard<K, V> {

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


    @Getter(PRIVATE)
    private final ShardKey key;

    private final ParallelConsumerOptions<?, ?> options;

    private final PartitionStateManager<K, V> pm;

    private final RateLimiter slowWarningRateLimit = new RateLimiter(5);

    private final AtomicLong availableWorkContainerCnt = new AtomicLong(0);

    void addWorkContainer(WorkContainer<K, V> wc) {
        long key = wc.offset();
        WorkContainer<K, V> existing = entries.get(key);
        if (existing != null) {
            // Check if the existing entry is stale and should be replaced
            if (isWorkContainerStale(existing)) {
                log.debug("Replacing stale entry (epoch {}) for offset {} with fresh one (epoch {})",
                        existing.getEpoch(), key, wc.getEpoch());
                entries.put(key, wc);
                // availableWorkContainerCnt stays the same since we're replacing, not adding. Two paths
                // reach here having already spent this offset's decrement, and neither re-increments:
                // the stale entry had been taken as work (getWorkIfAvailable() decremented at take time),
                // or the poller's removeStaleContainers() sweep removed it between the get and this put.
                // Either way the shard undercounts its available work, and NOT only "until the next add" -
                // the next add increments for its own new entry, so the deficit survives it and can
                // accumulate across replacements. It resyncs only when the shard drains far enough for the
                // clamp in dcrAvailableWorkContainerCntByDelta() to floor the counter at zero, or when the
                // shard is removed. That is a backpressure-gauge inaccuracy only, and it errs towards
                // fetching sooner rather than starving: no record is lost, because getWorkIfAvailable()
                // scans entries directly rather than gating on the count, and handleFutureResult() drops a
                // stale in-flight result without touching the shard, so it cannot remove this fresh entry.
            } else {
                log.debug("Entry for {} already exists in shard queue, dropping record", wc);
            }
        } else {
            entries.put(key, wc);
            availableWorkContainerCnt.incrementAndGet();
        }
    }

    /**
     * Which container currently occupies an offset, or null.
     * <p>
     * Read-only, and package-private for tests that need to assert WHICH container won a contested offset rather
     * than merely how many are tracked. A read cannot break the invariants that keep {@link #entries} private -
     * only a write can, which is why there is no corresponding setter and why {@link #addWorkContainer} remains
     * the only way in.
     */
    WorkContainer<K, V> getWorkContainerAt(long offset) {
        return entries.get(offset);
    }

    public void onSuccess(WorkContainer<?, ?> wc) {
        // remove work from shard's queue
        entries.remove(wc.offset());
    }

    public void onFailure() {
        // increase available cnt first to let retry expired calculated later
        availableWorkContainerCnt.incrementAndGet();
    }


    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public long getCountOfWorkAwaitingSelection() {
        return availableWorkContainerCnt.get();
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
        // from onPartitionsRemoved callback, need to deduce the available worker count for the revoked partition
        WorkContainer<K, V> toRemovedWorker = entries.get(offset);
        if (toRemovedWorker != null && toRemovedWorker.isAvailableToTakeAsWork()) {
            dcrAvailableWorkContainerCntByDelta(1);
        }
        return entries.remove(offset);
    }



    // remove staled WorkContainer otherwise when the partition is reassigned, the staled messages will:
    // 1. block the new work containers to be picked and processed
    // 2. will cause the consumer to paused consuming new messages indefinitely
    public List<WorkContainer<K, V>> removeStaleWorkContainersFromShard() {
        List<WorkContainer<K, V>> staleContainers = new ArrayList<>();
        var iterator = entries.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, WorkContainer<K, V>> entry = iterator.next();
            if (isWorkContainerStale(entry.getValue())) {
                iterator.remove();  // Safe even on ConcurrentSkipListMap
                dcrAvailableWorkContainerCntByDelta(1);
                staleContainers.add(entry.getValue());
            }
        }
        return staleContainers;
    }

    ArrayList<WorkContainer<K, V>> getWorkIfAvailable(int workToGetDelta, RetryQueue retryQueue) {
        log.trace("Looking for work on shardQueueEntry: {}", getKey());

        var slowWork = new HashSet<WorkContainer<?, ?>>();
        var workTaken = new ArrayList<WorkContainer<K, V>>();

        var iterator = entries.entrySet().iterator();
        boolean hasStaleWorkContainer = false;
        while (workTaken.size() < workToGetDelta && iterator.hasNext()) {
            var workContainer = iterator.next().getValue();

            if (pm.couldBeTakenAsWork(workContainer)) {
                // ONE call, deliberately. This used to read `isAvailableToTakeAsWork()` and then call
                // onQueueingForExecution() separately, and the gap between the two is what could let a record be
                // delivered twice: the check read three terms and the act re-validated none of them, so a decision
                // made before another worker completed the record could still win. onQueueingForExecution() now
                // evaluates the whole decision and claims from the state it evaluated. Do not reintroduce a guard
                // in front of it.
                if (workContainer.onQueueingForExecution()) {
                    log.trace("Taking {} as work", workContainer);

                    workTaken.add(workContainer);
                } else {
                    log.trace("Skipping {} as work, not available to take as work", workContainer);
                    addToSlowWorkMaybe(slowWork, workContainer);
                }

                if (isOrderRestricted()) {
                    // can't take any more work from this shard, due to ordering restrictions
                    // processing blocked on this shard, continue to next shard
                    log.trace("Processing by {}, so have cannot get more messages on this ({}) shardEntry.", this.options.getOrdering(), getKey());
                    break;
                }
            } else {
                // break, assuming all work in this shard, is for the same ShardKey, which is always on the same
                //  partition (regardless of ordering mode - KEY, PARTITION or UNORDERED (which is parallel PARTITIONs)),
                //  so no point continuing shard scanning. This only isn't true if a non standard partitioner produced the
                //  recrods of the same key to different partitions. In which case, there's no way PC can make sure all
                //  records of that belong to the shard are able to even be processed by the same PC instance, so it doesn't
                //  matter.

                if (isWorkContainerStale(workContainer)) {
                    // remove stale container and deduct on availableWorkContainerCnt
                    log.debug("shard {} there are still stale work container, need to remove container : {}", this, workContainer);
                    dcrAvailableWorkContainerCntByDelta(1);
                    iterator.remove();
                } else {
                    log.trace("Partition for shard {} is blocked for work taking, stopping shard scan", this);
                    break;
                }
            }
        }

        if (workTaken.size() == workToGetDelta) {
            log.trace("Work taken ({}) exceeds max ({})", workTaken.size(), workToGetDelta);
        }

        logSlowWork(slowWork);

        // Remove from retry queue as picked for submission to work pool - filter to only remove work containers that have
        // previously failed - as retry queue won't have any that didn't previously fail.
        retryQueue.removeAll(workTaken.stream().filter(WorkContainer::hasPreviouslyFailed).collect(Collectors.toList()));

        dcrAvailableWorkContainerCntByDelta(workTaken.size());

        return workTaken;
    }

    private void logSlowWork(Set<WorkContainer<?, ?>> slowWork) {
        // log
        if (!slowWork.isEmpty()) {
            List<String> slowTopics = slowWork.parallelStream()
                    .map(x -> x.getTopicPartition().toString()).distinct()
                    .collect(Collectors.toList());
            slowWarningRateLimit.performIfNotLimited(() ->
                    log.warn("Warning: {} records in the queue have been waiting longer than {}s for following topics {}.",
                            slowWork.size(), toSeconds(options.getThresholdForTimeSpendInQueueWarning()), slowTopics));
        }
    }

    private void addToSlowWorkMaybe(Set<WorkContainer<?, ?>> slowWork, WorkContainer<?, ?> workContainer) {
        Duration timeInFlight = workContainer.getTimeInFlight();
        Duration slowThreshold = options.getThresholdForTimeSpendInQueueWarning();
        if (isGreaterThan(timeInFlight, slowThreshold)) {
            if (!slowWork.contains(workContainer)) {
                pm.incrementSlowWorkCounter(workContainer.getTopicPartition());
            }
            slowWork.add(workContainer);
            if (log.isTraceEnabled()) {
                log.trace("Work has spent over " + slowThreshold + " in queue! " + cantTakeAsWorkMsg(workContainer, timeInFlight));
            }
        } else {
            if (log.isTraceEnabled()) {
                log.trace(cantTakeAsWorkMsg(workContainer, timeInFlight));
            }
        }
    }

    private static String cantTakeAsWorkMsg(WorkContainer<?, ?> workContainer, Duration timeInFlight) {
        var msgTemplate = "Can't take as work: Work ({}). Must all be true: Delay passed= {}. Is not in flight= {}. Has not succeeded already= {}. Time spent in execution queue: {}.";
        return msg(msgTemplate, workContainer, workContainer.isDelayPassed(), workContainer.isNotInFlight(), !workContainer.isUserFunctionSucceeded(), timeInFlight);
    }

    private boolean isOrderRestricted() {
        return options.getOrdering() != UNORDERED;
    }

    // check if the work container is stale
    private boolean isWorkContainerStale(WorkContainer<K, V> workContainer) {
        return pm.getPartitionState(workContainer).checkIfWorkIsStale(workContainer);
    }

    private void dcrAvailableWorkContainerCntByDelta(int ByNum) {
        availableWorkContainerCnt.getAndAdd(-1 * ByNum);
        // in case of possible race condition
        if (availableWorkContainerCnt.get() < 0L) {
            availableWorkContainerCnt.set(0L);
        }
    }
}
