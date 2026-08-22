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
import java.util.concurrent.atomic.LongAdder;
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

    /**
     * Counts what the dispatch scan looks at, so a change that makes one shard shape quadratic is detectable.
     * Shared across every shard of one {@link ShardManager} - see {@link DispatchScanMeter} for why it is not
     * per-shard, and why it is a count rather than a timing.
     */
    private final DispatchScanMeter scanMeter;

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

    /**
     * How many of this shard's records are out at a worker right now.
     * <p>
     * <b>What it buys.</b> Under an ordered mode a shard may have at most one record out at a time, so a shard
     * with anything in flight can hand out nothing - and the only way to discover that used to be to enter the
     * shard and walk to its head, paying an iterator, a set, a list and a scan to learn one bit. This answers the
     * same question with one comparison, and lets {@link ShardManager#getUpperBoundOnSelectableWork()} count
     * shards that can actually yield rather than estimating with the shard total.
     * <p>
     * <b>It is an optimisation and never the guarantee.</b> The ordering invariant is still enforced where it
     * always was: by the per-record claim in {@link WorkContainer#onQueueingForExecution()} and by the
     * {@code isOrderRestricted()} break below. So a reading that is transiently low costs a wasted scan, never a
     * second record out of an ordered shard.
     * <p>
     * <b>Why it cannot leak.</b> The charge is taken and released by the two halves of one state transition on
     * the record itself - claimed, then landed - rather than by the paths that add and remove entries. A record
     * removed from this shard while still out at a worker therefore still releases its charge when it lands, and
     * a record whose shard is discarded first releases into a meter nobody reads. There is no removal site whose
     * condition can be got wrong, which is the failure mode {@link #availableWorkContainerCnt} documents.
     * <p>
     * {@link LongAdder} for the same reasons as {@link RecordPopulation}: written twice per delivery from
     * whichever worker took the record, read at most once per shard per dispatch pass, and never read at all
     * under {@code UNORDERED}, where no shard is ever blocked.
     *
     * @see #getCountOfWorkInFlight()
     * @see #countWorkInFlightByScan()
     */
    private final LongAdder inFlightWorkContainerCnt = new LongAdder();

    void addWorkContainer(WorkContainer<K, V> wc) {
        long key = wc.offset();
        // before any publication into entries, so every thread that can reach the container has seen it
        wc.onAdmittedToShard(inFlightWorkContainerCnt);
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
            population.onAdmitted();
            availableWorkContainerCnt.incrementAndGet();
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

    /**
     * The work is out of flight and selectable again - whether it failed, or came back with no verdict at all.
     * Increase the available cnt first, to let retry expiry be calculated later.
     * <p>
     * Whether a retry is <em>also</em> scheduled is {@link ShardManager}'s decision, not this shard's.
     */
    public void markAvailableAgain() {
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

    /**
     * How many of this shard's records are out at a worker, in O(1).
     *
     * @see #inFlightWorkContainerCnt
     */
    public long getCountOfWorkInFlight() {
        return inFlightWorkContainerCnt.sum();
    }

    /**
     * Ground truth for {@link #getCountOfWorkInFlight()}: counts in-flight records by scanning the entries.
     * <p>
     * O(n) and deliberately derived from the containers rather than the counter, so a test can hold the two
     * against each other - the same arrangement as {@link ShardManager#countRecordsInShardsByScan()}. Not for
     * production use.
     */
    // visible for testing
    long countWorkInFlightByScan() {
        return entries.values().stream()
                .filter(WorkContainer::isInFlight)
                .count();
    }

    /**
     * Whether an ordered shard is closed for business because it already has a record out at a worker.
     * <p>
     * Always false under {@code UNORDERED}, where shards are never blocked - the check costs one comparison that
     * is never true, and making it help {@code UNORDERED} is a separate question
     * ({@code docs/inflight/next-direct-pull-unordered-selection.md}).
     */
    boolean isBlockedByWorkInFlight() {
        return isOrderRestricted() && getCountOfWorkInFlight() > 0;
    }

    /**
     * Removes the record at this offset, if it is still held. Reached from the partition revocation sweep.
     */
    public WorkContainer<K, V> remove(long offset) {
        WorkContainer<K, V> removed = entries.remove(offset);
        retireAndDeductIfStillCounted(removed);
        return removed;
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
                retireAndDeductIfStillCounted(entry.getValue());
                staleContainers.add(entry.getValue());
            }
        }
        return staleContainers;
    }

    List<WorkContainer<K, V>> getWorkIfAvailable(int workToGetDelta, RetryQueue retryQueue) {
        if (isBlockedByWorkInFlight()) {
            // An ordered shard with a record out at a worker can hand out nothing, and this is the cheapest way
            // to know it: one comparison, against an iterator, a set, a list and a walk to the head. The scan
            // below still enforces the invariant for every shard it does enter - this only skips work that would
            // certainly have found nothing. The stale-container sweep the scan also performs is skipped with it,
            // and is picked up on the next pass once the shard is free (or by the poller's own sweep).
            log.trace("Shard {} has work in flight and is order restricted, so cannot hand out more", getKey());
            return Collections.emptyList();
        }

        log.trace("Looking for work on shardQueueEntry: {}", getKey());

        var slowWork = new HashSet<WorkContainer<?, ?>>();
        var workTaken = new ArrayList<WorkContainer<K, V>>();

        var iterator = entries.entrySet().iterator();
        boolean hasStaleWorkContainer = false;
        while (workTaken.size() < workToGetDelta && iterator.hasNext()) {
            var workContainer = iterator.next().getValue();
            scanMeter.onEntryExamined();

            if (pm.couldBeTakenAsWork(workContainer)) {
                // ONE call, deliberately. This used to read `isAvailableToTakeAsWork() && onQueueingForExecution()`,
                // and the gap between the two is what let a record be delivered twice: the check read three terms
                // and the claim re-validated only one of them, so a decision made before another worker completed
                // the record could still win. onQueueingForExecution() now evaluates the whole decision and claims
                // from the state it evaluated. Do not reintroduce a guard in front of it.
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
                    // last-resort sweep, for a container that went stale without either epoch-change sweep
                    // having reached it - it still has to be retired like every other departure
                    log.debug("shard {} there are still stale work container, need to remove container : {}", this, workContainer);
                    iterator.remove();
                    retireAndDeductIfStillCounted(workContainer);
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
        availableWorkContainerCnt.getAndAdd(-1L * byNum);
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
     * delay, which {@link #markAvailableAgain()} counted back in) was not. {@link WorkContainer#isNotInFlight()}
     * is what separates the two.
     * <p>
     * The old test here was {@link WorkContainer#isAvailableToTakeAsWork()}, which additionally requires the retry
     * delay to have passed. That made revoking a record parked in retry back-off leave its increment behind
     * permanently, high, in the direction the clamp never caught.
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
