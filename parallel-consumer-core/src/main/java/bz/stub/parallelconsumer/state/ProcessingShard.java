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
    private final NavigableMap<Long, WorkContainer<K, V>> workMap = new ConcurrentSkipListMap<>();


    @Getter(PRIVATE)
    private final ShardKey key;

    private final ParallelConsumerOptions<?, ?> options;

    private final PartitionStateManager<K, V> pm;

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
     * behind {@code isSufficientlyLoaded()} and {@code drain()}. The single exception reads nothing: the
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
        long key = incomingWorkContainer.offset();
        WorkContainer<K, V> existingWorkContainer = workMap.get(key);
        if (existingWorkContainer != null) {
            // Check if the existing entry is stale and should be replaced
            if (isWorkContainerStale(existingWorkContainer)) {
                log.debug("Replacing stale entry (epoch {}) for offset {} with fresh one (epoch {})",
                        existingWorkContainer.getEpoch(), key, incomingWorkContainer.getEpoch());
                workMap.put(key, incomingWorkContainer);
                // The stale entry has left the shard, so it gives back its claim - if it still holds one. It does
                // not when it was already taken as work, and did when it was only ever queued; the counter now
                // tells those apart from the container's own record instead of guessing, which is what used to
                // leave this branch a claim short every time a taken entry was replaced.
                excludeFromSelection(existingWorkContainer);
                includeInSelection(incomingWorkContainer);
            } else {
                log.debug("Entry for {} already exists in shard queue, dropping record", incomingWorkContainer);
            }
        } else {
            workMap.put(key, incomingWorkContainer);
            includeInSelection(incomingWorkContainer);
        }
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
     * only a write can, which is why there is no corresponding setter and why {@link #addWorkContainer} remains
     * the only way in.
     */
    Optional<WorkContainer<K, V>> getWorkContainerAt(long offset) {
        return Optional.ofNullable(workMap.get(offset));
    }

    public void onSuccess(WorkContainer<?, ?> successfulWork) {
        // remove work from shard's queue
        WorkContainer<K, V> removedContainer = workMap.remove(successfulWork.offset());
        if (removedContainer != null) {
            // Normally a no-op: the claim was taken when the record was taken as work. Done unconditionally anyway
            // so that "a container that has left the shard holds no claim" is an invariant of every exit path
            // rather than a property of the paths somebody remembered.
            excludeFromSelection(removedContainer);
        }
    }

    /**
     * Idempotent - a failed record is selectable again (once its retry delay passes), so it re-joins the selection
     * population. Calling this twice for the same container includes it once.
     */
    public void onFailure(WorkContainer<?, ?> failedWork) {
        // include in selection first to let retry expired calculated later
        includeInSelection(failedWork);
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
        WorkContainer<K, V> removed = workMap.remove(offset);
        if (removed != null) {
            excludeFromSelection(removed);
        }
        return removed;
    }



    // remove staled WorkContainer otherwise when the partition is reassigned, the staled messages will:
    // 1. block the new work containers to be picked and processed
    // 2. will cause the consumer to paused consuming new messages indefinitely
    public List<WorkContainer<K, V>> removeStaleWorkContainersFromShard() {
        List<WorkContainer<K, V>> staleContainers = new ArrayList<>();
        var iterator = workMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, WorkContainer<K, V>> entry = iterator.next();
            if (isWorkContainerStale(entry.getValue())) {
                // Safe to remove during iteration on a ConcurrentSkipListMap - but this removes by KEY, so a fresh
                // container the controller put here since next() returned is what actually leaves, and the
                // excludeFromSelection below then releases the wrong object. Open, with the decision it needs, in
                // docs/inflight/bug-stale-sweep-iterator-evicts-fresh-replacement.md.
                iterator.remove();
                excludeFromSelection(entry.getValue());
                staleContainers.add(entry.getValue());
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
                    // remove stale container and deduct on workAwaitingSelectionCount
                    log.debug("shard {} there are still stale work container, need to remove container : {}", this, workContainer);
                    iterator.remove();
                    excludeFromSelection(workContainer);
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
