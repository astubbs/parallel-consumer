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

    /**
     * Offset the next {@code UNORDERED} scan starts from, or null to start at the head.
     * <p>
     * Read and advanced by the control loop's dispatch call, which is single threaded. Cleared by
     * {@link #markAvailableAgain()}, which runs on worker threads - hence volatile. A stale read there costs at
     * most one extra pass before the record is seen.
     */
    private volatile Long unorderedResumePoint = null;


    @Getter(PRIVATE)
    private final ShardKey key;

    private final ParallelConsumerOptions<?, ?> options;

    private final PartitionStateManager<K, V> pm;

    private final RateLimiter slowWarningRateLimit = new RateLimiter(5);

    private final AtomicLong availableWorkContainerCnt = new AtomicLong(0);

    public void addWorkContainer(WorkContainer<K, V> wc) {
        long key = wc.offset();
        if (entries.containsKey(key)) {
            log.debug("Entry for {} already exists in shard queue, dropping record", wc);
        } else {
            entries.put(key, wc);
            availableWorkContainerCnt.incrementAndGet();
        }
    }

    public void onSuccess(WorkContainer<?, ?> wc) {
        // remove work from shard's queue
        entries.remove(wc.offset());
    }

    /**
     * The work is out of flight and selectable again - whether it failed, or came back with no verdict at all.
     * Increase the available cnt first, to let retry expiry be calculated later.
     * <p>
     * Whether a retry is <em>also</em> scheduled is {@link ShardManager}'s decision, not this shard's.
     */
    public void markAvailableAgain() {
        // Work has become selectable again BEHIND the dispatch scan's resume point, and this is the only
        // way that can happen: records are registered in ascending offset order, so nothing else ever
        // lands earlier than where the scan has already reached. Clearing the point sends the next scan
        // back to the head so this record is seen.
        //
        // Without this, the wrap in scanResuming is not enough on its own. It only fires when the tail
        // cannot fill the request, and on a live partition new records keep arriving at higher offsets -
        // so the tail never empties, the resume point advances forever, and a retried record is starved
        // while the partition sits blocked behind it. Note that a test which fails one record and looks
        // for it does NOT catch this, because an idle tail resets the point and hides the bug;
        // UnorderedShardScanResumeTest keeps the shard fed on purpose.
        unorderedResumePoint = null;
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

        if (isOrderRestricted()) {
            // KEY and PARTITION must always start at the lowest offset, because the lowest offset IS the
            // next record to run. There is nothing to resume from and nothing to gain: the scan takes at
            // most one container and stops.
            scan(entries, workToGetDelta, workTaken, slowWork);
        } else {
            scanResuming(workToGetDelta, workTaken, slowWork);
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

    /**
     * Scans from where the last scan stopped, then wraps to cover what it skipped.
     * <p>
     * WHY THIS EXISTS. {@link #entries} keeps a record until it SUCCEEDS, not until it is dispatched - so
     * every record currently out for processing is still sitting in this map, ahead of the selectable ones.
     * In {@code UNORDERED} the scan does not stop early, so restarting at the head each pass meant walking
     * past every in-flight container before reaching any work. That is O(in-flight) per dispatch pass, over
     * a skip list, and it gets worse exactly as concurrency rises - the shape a measured throughput ceiling
     * should never have.
     * <p>
     * Measured: {@code UNORDERED} ran 21% slower than {@code KEY} at a 100ms handler and 1,000 concurrent -
     * 7,318 against 8,875 msg/s - despite {@code KEY} being the mode with the stricter guarantee. The
     * difference is the walk: {@code UNORDERED} shards by topic-partition, so ten shards each held a
     * thousand records, while {@code KEY} shards by record key, so each shard held one and there was
     * nothing to walk past. See docs/inflight/perf-throughput-regression-since-0-3.md.
     * <p>
     * WHY IT WRAPS RATHER THAN JUST SKIPPING. A record that fails, or returns no verdict, becomes
     * selectable again via {@link #markAvailableAgain()} - and it sits BEHIND the resume point. Advancing
     * without wrapping would starve it until the point happened to reset. The second pass covers exactly
     * the range the first one skipped, so every selectable record is still reachable in one call.
     * <p>
     * This is the same fairness argument {@link ShardManager} already makes across shards with
     * {@code LoopingResumingIterator}; this is that idea applied within one shard. It is deliberately NOT
     * that class: it takes {@code map.size()} up front, which on a {@link ConcurrentSkipListMap} is O(n)
     * and would reintroduce the cost being removed here. {@link NavigableMap#tailMap} positions in
     * O(log n) instead.
     */
    private void scanResuming(int workToGetDelta,
                              ArrayList<WorkContainer<K, V>> workTaken,
                              Set<WorkContainer<?, ?>> slowWork) {
        Long resume = unorderedResumePoint;

        boolean blocked = scan(resume == null ? entries : entries.tailMap(resume, true),
                workToGetDelta, workTaken, slowWork);

        // Only wrap if we actually skipped something, and only when the first pass did not stop because the
        // partition is blocked - if it is blocked, the head of the map is blocked too.
        if (!blocked && resume != null && workTaken.size() < workToGetDelta) {
            scan(entries.headMap(resume, false), workToGetDelta, workTaken, slowWork);
        }

        if (workTaken.isEmpty()) {
            // Nothing selectable from here - start over rather than creeping forward.
            unorderedResumePoint = null;
        } else {
            unorderedResumePoint = workTaken.get(workTaken.size() - 1).offset() + 1;
        }
    }

    /**
     * @return true if the scan stopped because the partition cannot take work right now
     */
    private boolean scan(NavigableMap<Long, WorkContainer<K, V>> range,
                         int workToGetDelta,
                         ArrayList<WorkContainer<K, V>> workTaken,
                         Set<WorkContainer<?, ?>> slowWork) {
        var iterator = range.entrySet().iterator();
        while (workTaken.size() < workToGetDelta && iterator.hasNext()) {
            var workContainer = iterator.next().getValue();

            if (pm.couldBeTakenAsWork(workContainer)) {
                if (workContainer.isAvailableToTakeAsWork()) {
                    log.trace("Taking {} as work", workContainer);

                    workContainer.onQueueingForExecution();
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
                    return true;
                }
            }
        }
        return false;
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
