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
     * The records this shard can offer as work, by offset.
     * <p>
     * Uses a ConcurrentSkipListMap instead of a TreeMap as under high pressure there appears to be some concurrency
     * errors (missing WorkContainers). This is addressed in PR#270.
     * <p>
     * Is a Map because need random access into collection, as records don't always complete in order (i.e. UNORDERED
     * mode).
     *
     * <h2>What is in it, which differs by ordering mode and is the whole point</h2>
     *
     * Under an <b>ordered</b> mode it holds every record of the shard, in-flight ones included, and that is
     * load-bearing: a scanner meeting an occupied head falls into the skip branch and breaks, which is what makes
     * an ordered shard refuse a second taker. Removing them from this walk's view breaks ordering silently.
     * <p>
     * Under <b>{@code UNORDERED}</b> it holds only the records that are <em>not</em> out at a worker. A record
     * leaves when it is TAKEN and comes back when its delivery lands. Nothing orders an unordered shard, so an
     * in-flight record sitting here would be pure obstruction - the scan used to walk the whole in-flight prefix
     * to reach a claimable entry, at roughly {@code in-flight / shards} examinations per record dispatched, and
     * that is what collapsed the direct-pull engine at 5,000 workers. Departure-on-take removes the prefix rather
     * than indexing around it, so the walk is one examination per record by construction.
     * <p>
     * <b>Re-entry is by offset, not by arrival.</b> This is a sorted map, so a failed or abandoned record returns
     * to its own place among the records still waiting, ahead of newer ones - exactly where the old
     * never-departs design left it. That is not an aesthetic: PC commits the lowest incomplete offset and encodes
     * the incompletes above it, so the committed payload grows with the SPREAD of in-flight offsets and has a hard
     * broker metadata ceiling. Appending a retry to a tail would widen that spread on the path that is already
     * unhappy. See {@code UnorderedRetryOffsetOrderTest}.
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
     * the record itself - claimed ({@link #onFlightBegan}), then landed ({@link #onFlightEnded}) - rather than by
     * the paths that add and remove entries. A record removed from this shard while still out at a worker
     * therefore still releases its charge when it lands. There is no removal site whose condition can be got
     * wrong, which is the failure mode {@link #availableWorkContainerCnt} documents.
     * <p>
     * <b>Under {@code UNORDERED} it is also the other half of the shard's contents</b>, because a record out at a
     * worker has left {@link #entries} - so the records this shard is responsible for are the entries plus this
     * count, which is what {@link #getCountOfWorkTracked()} returns.
     * <p>
     * {@link LongAdder} for the same reasons as {@link RecordPopulation}: written twice per delivery from
     * whichever worker took the record, read at most once per shard per dispatch pass.
     *
     * @see #getCountOfWorkInFlight()
     * @see #countWorkInFlightByScan()
     */
    private final LongAdder inFlightWorkContainerCnt = new LongAdder();

    void addWorkContainer(WorkContainer<K, V> wc) {
        long key = wc.offset();
        // before any publication into entries, so every thread that can reach the container has seen it
        wc.onAdmittedToShard(this);
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
            // Under UNORDERED this branch is also reached when the offset's PREVIOUS container is out at a worker,
            // because an in-flight record is not in entries to be found above. The fresh container is admitted
            // here, and the displaced one retires when its delivery lands and finds the offset taken - see
            // onFlightEnded(). Net population is one per offset either way.
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
        // remove work from shard's queue.
        //
        // Identity-matched, not offset-matched: under UNORDERED a record re-enters the entries when its delivery
        // lands, and a rebalance can have put a FRESH container at the same offset in the meantime. Removing by
        // offset alone would take the fresh one out and retire it, stranding a live record. The staleness check in
        // WorkManager#handleFutureResult already stops a stale result reaching here, so this is belt and braces -
        // but it is a one-word difference and the failure it prevents is silent.
        if (entries.remove(wc.offset(), wc)) {
            // the available-work counter already deducted this record when it was selected as work
            population.onRetired();
        }
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
        return getCountOfWorkTracked() == 0;
    }

    public long getCountOfWorkAwaitingSelection() {
        return availableWorkContainerCnt.get();
    }

    /**
     * How many records this shard is responsible for - offerable, out at a worker, or waiting out a retry delay.
     * <p>
     * Under an ordered mode that is exactly the entry map. Under {@code UNORDERED} a record out at a worker has
     * left the entry map, so the responsibility is the entries PLUS the in-flight count - the two disjoint halves
     * of the shard rather than one collection and an index over it.
     */
    public long getCountOfWorkTracked() {
        return entries.size() + (isOrderRestricted() ? 0 : getCountOfWorkInFlight());
    }

    /**
     * How many of this shard's records it can offer right now without waiting for a delivery to land. O(n) on a
     * skip list, so for tests and diagnostics only.
     */
    // visible for testing
    long countOfferable() {
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
     * <p>
     * <b>Meaningful under an ordered mode only.</b> Under {@code UNORDERED} an in-flight record is not in the
     * entries to be counted, so this reads zero in quiescence and that IS the invariant - see
     * {@code UnorderedAvailableQueueTest}, which asserts exactly that rather than an agreement between two
     * structures that no longer overlap.
     */
    // visible for testing
    long countWorkInFlightByScan() {
        return entries.values().stream()
                .filter(WorkContainer::isInFlight)
                .count();
    }

    /**
     * A worker has won the claim on this record: it is out, and under {@code UNORDERED} it leaves the offerable
     * set until its delivery lands.
     * <p>
     * The charge is taken first and the entry removed second, so a concurrent reader of
     * {@link #getCountOfWorkTracked()} can transiently double-count a record but can never miss one. High
     * throttles record intake for one loop; low would let the consumer over-fetch.
     *
     * @see WorkContainer#onQueueingForExecution()
     */
    void onFlightBegan(WorkContainer<K, V> wc) {
        inFlightWorkContainerCnt.increment();
        if (!isOrderRestricted()) {
            // identity-matched: a rebalance may already have replaced this offset with a fresh container, which
            // must stay offerable
            entries.remove(wc.offset(), wc);
        }
    }

    /**
     * The delivery has landed - succeeded, failed, or came back with no verdict at all - so under {@code UNORDERED}
     * the record returns to the offerable set, <b>at its own offset</b>.
     * <p>
     * <b>It returns even when it succeeded</b>, and that is deliberate rather than an oversight: retiring it here
     * would make "the record left the entries" no longer the single condition under which a record is retired from
     * {@link RecordPopulation}, and a second retirement rule is exactly the shape of drift that conservation
     * counting exists to rule out. {@link ShardManager#onSuccess} removes it a moment later on the same thread,
     * which is where it has always been removed. The visible cost is one map insert and one removal per successful
     * record, on the control thread.
     *
     * @see WorkContainer#endFlight()
     */
    void onFlightEnded(WorkContainer<K, V> wc) {
        if (isOrderRestricted()) {
            inFlightWorkContainerCnt.decrement();
            return;
        }
        if (isWorkContainerStale(wc)) {
            // The partition was revoked or re-assigned while this delivery was out, so the record must never be
            // offered again - putting it back would leave a stale container in the offerable set for the scan to
            // meet, which is the confluentinc#909 hazard, and the epoch sweep that would otherwise have taken it
            // has already run. This IS its departure, so it retires here. Compare
            // WorkManager#handleFutureResult's own staleness branch, which is the same decision one level up.
            log.debug("Landing container for offset {} is from a revoked generation, retiring it rather than "
                    + "returning it to shard {}", wc.offset(), getKey());
            inFlightWorkContainerCnt.decrement();
            // retired but NOT deducted from availableWorkContainerCnt: a record out at a worker had its unit
            // deducted when it was selected, exactly as retireAlreadyDeducted() used to record for a success
            population.onRetired();
            return;
        }
        // re-enter BEFORE releasing the charge, for the same reason onFlightBegan charges before removing: a
        // concurrent reader of getCountOfWorkTracked() must never see the record in neither half
        WorkContainer<K, V> occupant = entries.putIfAbsent(wc.offset(), wc);
        inFlightWorkContainerCnt.decrement();
        if (occupant != null && occupant != wc) {
            // A fresh container took this offset while the delivery was out - confluentinc#909's rebalance case.
            // The returning container has nowhere to go and will never be offered again, so its population unit
            // has to be released here; the fresh one carries its own.
            log.debug("Landing container for offset {} has been replaced in shard {}, retiring it", wc.offset(), getKey());
            population.onRetired();
        }
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
     * <p>
     * <b>Under {@code UNORDERED} this returns null for a record that is out at a worker</b>, because such a record
     * is not in the shard to be removed. That is not a leak: the delivery lands, {@link #onFlightEnded} puts the
     * record back, and the stale sweep - the scan's own last-resort branch, or the poller's
     * {@link ShardManager#removeStaleContainers()} - retires it then, exactly as it retires a record that was
     * revoked while merely waiting. The only difference from an ordered mode is WHEN the population figure drops,
     * and it drops late rather than early, which throttles record intake for the interval rather than loosening
     * it.
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

        takeFromEntries(workToGetDelta, slowWork, workTaken);

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
     * The walk, over the entry map from its lowest offset. <b>One walk for every ordering mode</b>, which is the
     * point: what differs between the modes is what is IN the map, not how it is read.
     * <p>
     * <b>Under an ordered mode, in-flight records staying visible to this walk is how ordering is enforced.</b> A
     * scanner that meets an occupied head falls into the skip branch and breaks without ever reaching the next
     * offset, which is what makes an ordered shard self-excluding under concurrent selection. Removing them from
     * its view broke ten tests on {@code perf/split-shard-inflight}
     * ({@code docs/inflight/parked-resume-shard-dispatch-scan.md}). It costs at most one examination per shard per
     * pass anyway: the {@code break} fires whether the head was taken or skipped.
     * <p>
     * <b>Under {@code UNORDERED} there is no break, and no in-flight prefix to need one.</b> A record leaves
     * {@link #entries} when it is taken, so all this walk can meet is work it can offer - one examination per
     * record dispatched, flat at any concurrency, without an index to keep in step. What it CAN still meet is a
     * record parked in retry back-off, which is refused by the claim and stepped over exactly as before; that is
     * bounded by the number of records in back-off, not by concurrency.
     */
    private void takeFromEntries(int workToGetDelta,
                                 Set<WorkContainer<?, ?>> slowWork,
                                 List<WorkContainer<K, V>> workTaken) {
        var iterator = entries.entrySet().iterator();
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
                    // can't take any more work from this shard, due to ordering restrictions - and the break is
                    // outside the take/skip branch on purpose: a scanner that found the head occupied must stop
                    // here rather than walk on to the next offset. That placement IS the ordered-mode guarantee.
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
