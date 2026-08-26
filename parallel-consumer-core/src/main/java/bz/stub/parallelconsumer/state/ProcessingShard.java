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
     * <p>
     * <b>Deliberately not exposed.</b> Every insertion and removal has to be paired with a
     * {@link RecordPopulation} admission or retirement, and that pairing is only enforceable while this class is
     * the only thing that can touch the map. Read-only totals are available through
     * {@link #getCountOfWorkTracked()}.
     */
    private final NavigableMap<Long, WorkContainer<K, V>> entries = new ConcurrentSkipListMap<>();


    @Getter(PRIVATE)
    private final ShardKey key;

    private final ParallelConsumerOptions<?, ?> options;

    private final PartitionStateManager<K, V> pm;

    /**
     * The conservation-derived count of records held across <em>all</em> shards, which this shard contributes its
     * admissions and retirements to. Shared instance, owned by {@link ShardManager}.
     */
    private final RecordPopulation population;

    private final RateLimiter slowWarningRateLimit = new RateLimiter(5);

    /**
     * Approximately how many of this shard's {@link #entries} are selectable as work right now.
     * <p>
     * <b>This no longer gates record intake</b> - see {@link WorkManager#isSufficientlyLoaded()}, which reads the
     * conservation figure instead. It survives as the input to the {@code WAITING_RECORDS} metric, the
     * under-served-retrieval diagnostic, and the shutdown drain check, and it remains an <em>approximation</em>:
     * it is incremented when a record is put back for retry, which is before its retry delay has passed, and
     * {@link ShardManager#getNumberOfWorkQueuedInShardsAwaitingSelection()} nets that out against the retry queue
     * rather than this shard doing so.
     * <p>
     * There is deliberately no clamp on it. A clamp is only defensible while something depends on the value being
     * non-negative, and once the load gate stopped reading it, nothing does - the aggregate in
     * {@link ShardManager#getNumberOfWorkQueuedInShardsAwaitingSelection()} floors its own result. Hiding a
     * negative here only hid one direction of drift, and made the other direction impossible to see at all.
     */
    private final AtomicLong availableWorkContainerCnt = new AtomicLong(0);

    void addWorkContainer(WorkContainer<K, V> wc) {
        long offset = wc.offset();
        WorkContainer<K, V> resident = entries.get(offset);
        if (resident != null && !isWorkContainerStale(resident)) {
            log.debug("Entry for {} already exists in shard queue, dropping record", wc);
            return;
        }
        if (resident != null) {
            log.debug("Replacing stale entry (epoch {}) for offset {} with fresh one (epoch {})",
                    resident.getEpoch(), offset, wc.getEpoch());
        }

        // ADMIT FIRST, then let the map itself say what happened - never the read above.
        //
        // By the time the insertion runs, `resident` is only advice: a stale sweep on the other thread can
        // have removed it and retired it in between, which turns what looks like a replacement into an
        // insertion. Deciding from `resident` would then skip the admission for the only container now at
        // this offset, while its eventual departure still retires - and the population sits permanently
        // below what the shards hold, with no clamp and nothing to reconcile it. Reading low
        // under-throttles, so the drift over-fetches from the broker rather than stalling it, but it never
        // self-corrects in either direction.
        //
        // Admitting before the put also preserves RecordPopulation's ordering invariant - a retirement can
        // never be observed against an admission that has not been committed yet - which is what lets
        // getInSystem() be non-negative by construction instead of by clamp.
        population.onAdmitted();
        WorkContainer<K, V> displaced = entries.put(offset, wc);
        if (displaced == null) {
            availableWorkContainerCnt.incrementAndGet();
        } else {
            // A real replacement after all: one container left the map as this one entered it, so the
            // speculative admission is balanced by the displaced container's retirement and the shard's
            // population is unchanged.
            population.onRetired();
            // availableWorkContainerCnt is deliberately NOT incremented. If the displaced container was
            // still selectable it was already holding this offset's unit, and if it had been taken as work
            // that unit was spent at selection and is not owed back - which leaves the shard reading one
            // low until it is removed. That is the approximation tracked in
            // docs/inflight/bug-available-work-counter-is-still-an-approximation.md, not a population
            // defect: getWorkIfAvailable() scans entries directly rather than gating on this count, and
            // handleFutureResult() drops a stale in-flight result without touching the shard, so no record
            // is lost either way.
        }
    }

    /**
     * Which container currently occupies an offset, or null.
     * <p>
     * Read-only, and package-private for tests that need to assert WHICH container won a contested offset rather
     * than merely how many are tracked. A read cannot break the admission/retirement pairing that keeps
     * {@link #entries} private - only a write can, which is why there is no corresponding setter and why
     * {@link #addWorkContainer} remains the only way in.
     */
    WorkContainer<K, V> getWorkContainerAt(long offset) {
        return entries.get(offset);
    }

    public void onSuccess(WorkContainer<?, ?> wc) {
        // remove work from shard's queue
        retireAlreadyDeducted(entries.remove(wc.offset()));
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

    /**
     * Removes the record at this offset, if it is still held. Reached from the partition revocation sweep.
     */
    public WorkContainer<K, V> remove(long offset) {
        return removeAndRetire(offset);
    }

    /**
     * Takes whatever is at this offset out of the shard, and retires exactly what the map gave up.
     * <p>
     * <b>The retirement has to be driven by the map's own return value, never by the container the caller is
     * holding.</b> Three removal paths run across two threads - the revocation sweep, the epoch-change stale
     * sweep, and {@link #getWorkIfAvailable}'s last-resort one - and when two of them collide on the same
     * offset only one removes anything. Retiring on both retires a single admission twice, and since
     * {@link RecordPopulation} has no clamp and nothing reconciles it against the shards, the deficit is
     * permanent: the load gate then believes fewer records are held than really are, and over-fetches for
     * the life of the consumer.
     * <p>
     * A value-conditional {@code entries.remove(offset, container)} would not do instead.
     * {@link WorkContainer#equals(Object)} is topic, partition and offset only, so a <em>fresh</em> container
     * that replaced this one at the same offset compares equal to it and would be removed as though it were
     * the stale one.
     */
    private WorkContainer<K, V> removeAndRetire(long offset) {
        WorkContainer<K, V> removed = entries.remove(offset);
        retireAndDeductIfStillCounted(removed);
        return removed;
    }



    // remove staled WorkContainer otherwise when the partition is reassigned, the staled messages will:
    // 1. block the new work containers to be picked and processed
    // 2. will cause the consumer to paused consuming new messages indefinitely
    public List<WorkContainer<K, V>> removeStaleWorkContainersFromShard() {
        List<WorkContainer<K, V>> staleContainers = new ArrayList<>();
        for (Map.Entry<Long, WorkContainer<K, V>> entry : entries.entrySet()) {
            if (isWorkContainerStale(entry.getValue())) {
                // Not iterator.remove(): it discards the map's return value, so it cannot tell "this call
                // removed the record" from "another thread had already removed it" - and the retirement has
                // to know which. See removeAndRetire.
                WorkContainer<K, V> removed = removeAndRetire(entry.getKey());
                if (removed != null) {
                    staleContainers.add(removed);
                }
            }
        }
        return staleContainers;
    }

    ArrayList<WorkContainer<K, V>> getWorkIfAvailable(int workToGetDelta, RetryQueue retryQueue) {
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
                    // last-resort sweep, for a container that went stale without either epoch-change sweep
                    // having reached it - it still has to be retired like every other departure, and taken
                    // out of the retry queue like ShardManager.removeStaleContainers() does. Leaving the
                    // queue entry behind orphans it forever: nothing else removes an entry whose container
                    // is no longer in any shard, and the workable figure the load gate reads subtracts a
                    // parked-for-retry count that would then include a record the population no longer does.
                    log.debug("shard {} there are still stale work container, need to remove container : {}", this, workContainer);
                    WorkContainer<K, V> removed = removeAndRetire(workContainer.offset());
                    if (removed != null) {
                        retryQueue.remove(removed);
                    }
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

    private void dcrAvailableWorkContainerCntByDelta(int byNum) {
        availableWorkContainerCnt.getAndAdd(-byNum);
    }

    /**
     * The record has left this shard, and the available-work counter had <em>already</em> deducted it when it was
     * selected as work - so only the population is updated.
     */
    private void retireAlreadyDeducted(WorkContainer<?, ?> removed) {
        if (removed != null) {
            population.onRetired();
        }
    }

    /**
     * The record has left this shard without being processed to a conclusion - revoked, or swept as stale.
     * <p>
     * Whether the available-work counter still holds a unit for it depends on where the record was when it went:
     * one out at a worker was deducted at selection, one sitting in the shard (including one waiting out a retry
     * delay, which {@link #onFailure()} counted back in) was not. {@link WorkContainer#isNotInFlight()}
     * is what separates the two.
     * <p>
     * The old test here was {@link WorkContainer#isAvailableToTakeAsWork()}, which additionally requires the retry
     * delay to have passed. That made revoking a record parked in retry back-off leave its increment behind
     * permanently, high, in the direction the clamp never caught.
     * <p>
     * <b>It is still an inference, and it has a known open window</b>: the predicate is read after the map has
     * already given the container up, so a controller-side {@code endFlight()} for a revoked record can land in
     * between and deduct a unit selection already spent. That is a defect in this counter, not in the
     * population - {@link RecordPopulation} is retired above on the map's own return value, never on a
     * predicate - and closing it means giving the container an ownership flag rather than patching a removal
     * site. Tracked in {@code docs/inflight/bug-available-work-counter-is-still-an-approximation.md}.
     */
    private void retireAndDeductIfStillCounted(WorkContainer<?, ?> removed) {
        if (removed == null) {
            return;
        }
        population.onRetired();
        if (removed.isNotInFlight()) {
            dcrAvailableWorkContainerCntByDelta(1);
        }
    }
}
