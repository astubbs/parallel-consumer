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
     * {@link #getCountOfWorkTracked()}; a test that needs a resident planted white-box goes through
     * {@link #plantResident(WorkContainer)}, which keeps the pairing.
     */
    private final NavigableMap<Long, WorkContainer<K, V>> workMap = new ConcurrentSkipListMap<>();


    @Getter(PRIVATE)
    private final ShardKey key;

    private final ParallelConsumerOptions<?, ?> options;

    private final PartitionStateManager<K, V> pm;

    /**
     * The conservation-derived count of records held across <em>all</em> shards, which this shard contributes its
     * admissions and retirements to. Shared instance, owned by {@link ShardManager}.
     * <p>
     * <b>Independent of {@link #workAwaitingSelectionCount} below, and the two must not be conditioned on each
     * other.</b> This one counts what the shard <em>holds</em> and is driven by the map's own mutations - the
     * value {@code put} displaced, the value {@code remove} gave up. That one counts what is <em>selectable</em>
     * and is driven by the compare-and-set on a container's selection claim. A record can leave the selection
     * population without leaving the shard (it was taken as work) and can leave the shard while holding no claim
     * (it was revoked at a worker), so neither figure is derivable from the other.
     */
    private final RecordPopulation population;

    private final RateLimiter slowWarningRateLimit = new RateLimiter(5);

    /**
     * How many of this shard's entries are counted as awaiting selection.
     * <p>
     * <b>Invariant: this equals the number of resident entries that hold a selection claim</b>
     * ({@link #countSelectionClaimedByScan()}), and is non-negative. Both hold <em>between</em> operations rather
     * than at every instant, and by construction rather than by clamping: every adjustment is made by the winner of
     * a compare-and-set on
     * {@link WorkContainer#claimSelection()} / {@link WorkContainer#releaseSelection()} - so a claim is taken
     * exactly once, by the party that owns the transition, and no site has to infer from observable state whether
     * it was already taken. {@code ShardAvailableCountOwnershipTest} is the check.
     * <p>
     * "Between operations" is not a hedge: {@link #includeInSelection(WorkContainer)} takes the claim and then
     * increments, which is two atomics rather than one, so a reader interleaving there can see the scan one ahead
     * of the counter - and, if a concurrent {@link #excludeFromSelection(WorkContainer)} wins the release inside
     * that window, can see the counter momentarily at -1. Every such interleaving still settles correct, and every
     * consumer that DRIVES ANYTHING reads the aggregate in
     * {@link ShardManager#getNumberOfWorkQueuedInShardsAwaitingSelection()}, which floors at zero - that is the one
     * behind {@code drain()}. It is no longer the one behind {@link WorkManager#isSufficientlyLoaded()}: the load
     * gate now reads {@link ShardManager#getWorkableRecords()}, which is derived from {@link #population} and
     * never touches this counter. The single exception reads nothing: the
     * under-served-retrieval diagnostic in {@link ShardManager#getWorkIfAvailable(int)} sums this counter unfloored
     * behind {@code log.isDebugEnabled()}, so the transient can surface as {@code awaitingSelection=-1} in one debug
     * line. Left unfloored deliberately - that line exists to show a human what the accounting actually says, and a
     * clamp there would hide the very drift it was added to expose. Collapsing the two atomics into one is the
     * follow-up that arrives with astubbs/parallel-consumer#335's {@code Execution} transition, and it removes the
     * transient rather than masking it.
     * <p>
     * This counts a failed record back into selection before its retry delay has passed
     * ({@link #onFailure(WorkContainer)}), so {@link ShardManager#getNumberOfWorkQueuedInShardsAwaitingSelection()}
     * nets that out against the retry queue - only the aggregate is meaningful.
     */
    private final AtomicLong workAwaitingSelectionCount = new AtomicLong(0);

    void addWorkContainer(WorkContainer<K, V> incomingWorkContainer) {
        long offset = incomingWorkContainer.offset();
        WorkContainer<K, V> residentBeforePut = workMap.get(offset);
        if (residentBeforePut != null && !isWorkContainerStale(residentBeforePut)) {
            log.debug("Entry for {} already exists in shard queue, dropping record", incomingWorkContainer);
            return;
        }
        if (residentBeforePut != null) {
            log.debug("Replacing stale entry (epoch {}) for offset {} with fresh one (epoch {})",
                    residentBeforePut.getEpoch(), offset, incomingWorkContainer.getEpoch());
        }

        // ADMIT FIRST, then let the map itself say what happened - never the read above.
        //
        // By the time the insertion runs, `residentBeforePut` is only advice: a stale sweep on the other thread
        // can have removed it and retired it in between, which turns what looks like a replacement into an
        // insertion. Deciding from `residentBeforePut` would then skip the admission for the only container now
        // at this offset, while its eventual departure still retires - and the population sits permanently below
        // what the shards hold, with no clamp and nothing to reconcile it. Reading low under-throttles, so the
        // drift over-fetches from the broker rather than stalling it, but it never self-corrects.
        //
        // Admitting before the put also preserves RecordPopulation's ordering invariant - a retirement can never
        // be observed against an admission that has not been committed yet - which is what lets getInSystem() be
        // non-negative by construction instead of by clamp.
        population.onAdmitted();
        WorkContainer<K, V> displaced = workMap.put(offset, incomingWorkContainer);

        // The claim protocol is separate accounting, and deliberately reads NOTHING from the branch above: the
        // arrival is offered a claim because it is now resident, and the displaced container gives one back
        // because it is not. Each is settled by its own compare-and-set, so neither can double-count when the
        // other thread reaches the same container first. includeInSelection also rechecks residency, which is
        // what covers the arrival being swept between the put and here.
        includeInSelection(incomingWorkContainer);
        if (displaced != null) {
            // A real replacement after all: one container left the map as this one entered it, so the
            // speculative admission is balanced by the displaced container's retirement and the shard's
            // population is unchanged.
            population.onRetired();
            // The displaced container gives back its claim IF it still holds one. It does not when it was
            // already taken as work, and does when it was only ever queued - the compare-and-set tells those
            // apart from the container's own record instead of guessing, which is what used to leave this
            // branch a claim short every time a taken entry was replaced.
            excludeFromSelection(displaced);
        }
    }

    /**
     * Plants a container as a resident of this shard, paired with its {@link RecordPopulation} admission but
     * <em>without</em> offering it a selection claim.
     * <p>
     * For white-box tests that need a resident already in place - typically a stale one, which the poller's sweep
     * normally removes, so a test that lets the sweep run never reaches the branch it is aiming at. It exists
     * rather than a getter for {@link #workMap} because a raw map handle lets a test insert without admitting,
     * which drifts the population silently and fails nothing.
     * <p>
     * No claim is offered because the containers planted this way have generally already spent theirs by being
     * taken as work; offering one here would count a container the shard is asserting is uncounted. Use
     * {@link #addWorkContainer} for anything modelling a genuine arrival.
     */
    // visible for testing
    void plantResident(WorkContainer<K, V> wc) {
        population.onAdmitted();
        workMap.put(wc.offset(), wc);
    }

    /**
     * Which container currently occupies an offset, if any.
     * <p>
     * <b>{@link Optional}, not a nullable reference.</b> "No container here" is an ordinary answer - the record
     * succeeded and left the shard, or was swept as stale - so it is the return type's job to say so rather than
     * the caller's job to remember (astubbs#335 review). This is the only accessor on the shard that can be
     * legitimately empty, which is exactly why an implicit null here would not be noticed.
     * <p>
     * Read-only, and package-private for tests that need to assert WHICH container won a contested offset rather
     * than merely how many are tracked. A read cannot break the invariants that keep {@link #workMap} private -
     * only a write can, which is why there is no corresponding setter and why {@link #addWorkContainer} and
     * {@link #plantResident} are the only ways in.
     */
    Optional<WorkContainer<K, V>> getWorkContainerAtOffset(long offset) {
        return Optional.ofNullable(workMap.get(offset));
    }

    public void onSuccess(WorkContainer<?, ?> successfulWork) {
        // remove work from shard's queue
        retire(workMap.remove(successfulWork.offset()));
    }

    /**
     * Idempotent - a failed record is selectable again (once its retry delay passes), so it re-joins the selection
     * population. Calling this twice for the same container includes it once.
     */
    public void onFailure(WorkContainer<?, ?> failedWork) {
        // include in selection first to let retry expired calculated later
        includeInSelection(failedWork);
    }

    /**
     * Work returned without a verdict. It never failed, so no retry is scheduled and no attempt is consumed - but
     * it must become selectable again, so it re-joins the selection population exactly as a failure would.
     * <p>
     * Deliberately its own entry point rather than a call to {@link #onFailure}: the two say different things
     * about the record, and it is {@link ShardManager} that acts on the difference by skipping the retry queue.
     * Naming the shard-level step after the failure it is not would put the one seam that distinguishes them
     * behind a method whose name denies it.
     * <p>
     * Idempotent for the same reason {@link #onFailure} is - the claim is a compare-and-set, so calling this twice
     * for the same container includes it once.
     */
    public void onAbandoned(WorkContainer<?, ?> abandonedWork) {
        includeInSelection(abandonedWork);
    }


    public boolean isEmpty() {
        return workMap.isEmpty();
    }

    public long getCountOfWorkAwaitingSelection() {
        return workAwaitingSelectionCount.get();
    }

    public long getCountOfWorkTracked() {
        return workMap.size();
    }

    public long getCountWorkInFlight() {
        return workMap.values().stream()
                .filter(WorkContainer::isInFlight)
                .count();
    }

    /**
     * From the {@code onPartitionsRemoved} callback: the revoked record leaves the shard, and gives back its
     * selection claim if it is still holding one.
     * <p>
     * This used to ask {@link WorkContainer#isAvailableToTakeAsWork()} whether to deduct, which is unanswerable:
     * a record out at a worker whose stale result the controller has just dropped ({@code handleFutureResult} ->
     * {@link WorkContainer#endFlight()}) reads as available again, and the shard would deduct a second time for a
     * claim selection had already taken. The deficit was permanent, and hid later queued records from
     * {@code WAITING_RECORDS} and from {@code drain()}'s check that nothing is still awaiting processing.
     */
    public WorkContainer<K, V> removeWorkAtOffset(long offset) {
        return retire(workMap.remove(offset));
    }

    /**
     * The one exit path: whatever the map actually gave up is retired from the population and gives back its
     * selection claim.
     * <p>
     * <b>Both halves are driven by the map's own return value, never by the container the caller is holding.</b>
     * Three removal paths run across two threads - the revocation sweep, the epoch-change stale sweep, and
     * {@link #getWorkIfAvailable}'s last-resort one - and when two of them collide on the same offset only one
     * removes anything. Retiring on both retires a single admission twice, and since {@link RecordPopulation} has
     * no clamp and nothing reconciles it against the shards, the deficit is permanent: the load gate then
     * believes fewer records are held than really are, and over-fetches for the life of the consumer.
     * <p>
     * The claim release is unconditional rather than predicated on the container's observable state, and normally
     * a no-op - the claim was already given back when the record was taken as work. Done anyway so that "a
     * container that has left the shard holds no claim" is an invariant of every exit path rather than a property
     * of the paths somebody remembered; only the caller that wins the compare-and-set moves the counter.
     * <p>
     * A value-conditional {@code workMap.remove(offset, container)} would not do instead.
     * {@link WorkContainer#equals(Object)} is topic, partition and offset only, so a <em>fresh</em> container
     * that replaced this one at the same offset compares equal to it and would be removed as though it were the
     * stale one.
     */
    private WorkContainer<K, V> retire(WorkContainer<K, V> removed) {
        if (removed != null) {
            population.onRetired();
            excludeFromSelection(removed);
        }
        return removed;
    }



    // remove staled WorkContainer otherwise when the partition is reassigned, the staled messages will:
    // 1. block the new work containers to be picked and processed
    // 2. will cause the consumer to paused consuming new messages indefinitely
    public List<WorkContainer<K, V>> removeStaleWorkContainersFromShard() {
        List<WorkContainer<K, V>> staleContainers = new ArrayList<>();
        for (Map.Entry<Long, WorkContainer<K, V>> entry : workMap.entrySet()) {
            if (isWorkContainerStale(entry.getValue())) {
                // Not iterator.remove(): it discards the map's return value, so it cannot tell "this call removed
                // the record" from "another thread had already removed it" - and both the retirement and the
                // claim release have to know which. See retire().
                //
                // This still removes by KEY, so a fresh container the controller put here since next() returned
                // is what actually leaves. Accounting for what LEFT rather than for what was inspected is half of
                // that defect closed: the population and the claim now follow the evicted object, and the caller
                // is handed it rather than the container the sweep was looking at. The eviction of the fresh
                // record itself is untouched and still open, with the decision it needs, in
                // docs/inflight/bug-stale-sweep-iterator-evicts-fresh-replacement.md.
                WorkContainer<K, V> removed = removeWorkAtOffset(entry.getKey());
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

        var iterator = workMap.entrySet().iterator();
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

                    // Release this container's selection claim here, at the moment it stops being selectable -
                    // and only for the caller that WON the claim above, which is why this sits inside the branch.
                    // Only the caller that wins the release moves the counter, so a concurrent revocation removing
                    // the same container cannot release it twice.
                    excludeFromSelection(workContainer);
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
                    // last-resort sweep, for a container that went stale without either epoch-change sweep having
                    // reached it - it still has to be retired and released like every other departure, and taken
                    // out of the retry queue like ShardManager.removeStaleContainers() does. Leaving the queue
                    // entry behind orphans it forever: nothing else removes an entry whose container is no longer
                    // in any shard, and the workable figure the load gate reads subtracts a parked-for-retry
                    // count that would then include a record the population no longer does.
                    log.debug("shard {} there are still stale work container, need to remove container : {}", this, workContainer);
                    WorkContainer<K, V> removed = removeWorkAtOffset(workContainer.offset());
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

    /**
     * Include {@code wc} in selection, if it is not included already.
     * <p>
     * Takes the claim first and confirms residency second, deliberately: the reverse order is a check-then-act, and
     * inferring "is this still mine to count" from a separate read is the mistake this class was fixed to remove.
     * If the container left the shard concurrently, the claim is handed straight back here - and if the removing
     * site's own release got there first, its compare-and-set lost and this one wins, so the claim is returned
     * exactly once whichever way the two interleave. That branch therefore nets to zero rather than counting a
     * departed container: the increment and the {@link #excludeFromSelection(WorkContainer)} that follows it
     * cancel, and the container leaves holding nothing.
     * <p>
     * Residency is tested by <b>reference</b> identity, not {@code equals}: {@link WorkContainer#equals(Object)} is
     * topic/partition/offset only, so a fresh container that replaced a stale one at the same offset compares equal
     * to it. Equality here would let a departed container keep the claim its replacement is now holding.
     */
    private void includeInSelection(WorkContainer<?, ?> wc) {
        if (wc.claimSelection()) {
            workAwaitingSelectionCount.incrementAndGet();
            if (workMap.get(wc.offset()) != wc) {
                excludeFromSelection(wc);
            }
        }
    }

    /**
     * Exclude {@code wc} from selection, if it is still included.
     * <p>
     * The compare-and-set is what makes the deduction owned: at most one caller can win it per claim, so the
     * counter settles non-negative and needs no clamp (see the field for the one transient this does not cover).
     * That matters beyond tidiness - the floor-at-zero clamp this replaces is
     * what let a conditional-decrement defect sit here unnoticed, by absorbing exactly the drift that would have
     * exposed it.
     */
    private void excludeFromSelection(WorkContainer<?, ?> wc) {
        if (wc.releaseSelection()) {
            workAwaitingSelectionCount.decrementAndGet();
        }
    }

    /**
     * Ground truth for the counter, for tests: the containers resident in this shard that hold a selection claim.
     * {@link #getCountOfWorkAwaitingSelection()} must always agree with this.
     */
    long countSelectionClaimedByScan() {
        return workMap.values().stream().filter(WorkContainer::isSelectionClaimed).count();
    }
}
