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
import org.apache.kafka.clients.consumer.ConsumerRecord;

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
     * <p>
     * <b>The container is stored directly, and {@link Map#remove(Object, Object)} on this map is a true
     * compare-and-remove</b> - because {@link WorkContainer}'s equality is reference identity. The JDK's
     * compare-and-remove decides "still mapped to the value I inspected" with {@code equals}, so what it means is
     * a property of the value type and not of the map; identity equality is what makes it mean "this container".
     * See {@link #evictIfStillResident} and the class javadoc of {@link WorkContainer}.
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

    /**
     * Counts what the dispatch scan looks at, so a change that makes one shard shape quadratic is detectable.
     * Shared across every shard of one {@link ShardManager} - see {@link DispatchScanMeter} for why it is not
     * per-shard, and why it is a count rather than a timing.
     */
    private final DispatchScanMeter scanMeter;

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
            //
            // KNOWN GAP, not fixed here: a container leaving a shard has to be taken out of the retry queue
            // too, and this branch cannot do it - the shard holds no reference to the RetryQueue, which is
            // passed in per-call to getWorkIfAvailable and nowhere else. A displaced container that was parked
            // for retry therefore leaves its queue entry behind, and ShardManager.purgeDepartedRetryEntries()
            // is what collects it: residency is reference identity, so a displaced container is resident in no
            // shard from the moment its replacement takes its offset.
            //
            // THAT ENTRY IS NOT PERMANENT, and an earlier version of this comment said it was. RetryQueue keys
            // by topic, partition and offset alone (WorkContainerKey.of), never by container identity, and
            // ShardManager.onSuccess removes by that key unconditionally - so the replacement admitted here,
            // which carries the same coordinates, clears the entry at its own first terminal event: success
            // removes it, failure re-adds the same key (add() replaces rather than duplicates), and a sweep
            // that finds the replacement removes it by key. What is wrong meanwhile is the FIGURE - the
            // surviving entry carries the DISPLACED container's retry-due time, so the ready-to-retry count
            // and RetryQueue.getLowestRetryTime read one entry high until then. Bounded misdirection, not the
            // stall this was first written up as.
            //
            // CLEARED 2026-09-08 - the purge above bounds the harm, and this says the case does not arise at
            // all. SUSPECTED: that production can reach this branch with a queue-resident displaced container.
            // It cannot, and note the argument turns on RESIDENCE, not on the queue - which is why it is
            // unaffected by the rebalance callbacks no longer touching the queue.
            // A container enters the retry queue only through ShardManager.onFailure, which needs it NOT stale
            // (couldBeTakenAsWork refuses a stale container, so a stale one is never selected, never fails and
            // never re-queues) and, since astubbs#437, still resident. The DISCRIMINATOR is what happens next:
            // only three transitions can then make it stale - the removed-state swap and the putAll in
            // PartitionStateManager, each immediately followed on the same thread by the sweep that takes it
            // OUT OF ITS SHARD, and fenceForRevocation, which sweeps nothing but cannot be followed by a second
            // container at the same offset, because a duplicate offset needs a re-assignment and that is the
            // swept path. A container the sweep has removed is not a resident, so there is nothing here to
            // displace. Bumping the epoch map alone changes no answer - PartitionState's epoch is final and
            // staleness is only asked through the state object.
            // WHAT WOULD REOPEN IT, and nothing goes red for any of it: an in-generation replay of an
            // already-registered offset (a seek - there are none in main today - or an offset-reset/truncation
            // replay), a topic-scoped shard key, or a second insertion site on workMap. The last leg is a
            // property of the consumer's fetch position, not of this class, so the pairing gap is one arrival
            // away rather than absent - and the purge is what makes that a bounded misdirection rather than a
            // surprise, which is why both records are kept.
            // Proof, control arms and ablation matrix: ShardDisplacementOrphanReachabilityTest and
            // docs/solutions/logic-errors/the-shard-displacement-orphan-is-unreachable-and-the-guard-is-outside-the-class-2026-09-08.md.
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
     * A delivery that never started - see {@link ShardManager#onAbandonedBeforeStarting}. The same re-inclusion a
     * failure gets, without the retry queue, because nothing failed.
     */
    public void onAbandonedBeforeStarting(WorkContainer<?, ?> abandonedWork) {
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
     * The unconditional removal: whatever occupies the offset leaves the shard, and gives back its selection
     * claim if it is still holding one.
     * <p>
     * <b>It has no production caller any more.</b> It was the {@code onPartitionsRemoved} sweep's removal until
     * that moved to {@link #removeWorkForRevokedRecord}, which names the registration it means; what is left are
     * the tests that use this as a modelling primitive for "a container departs its shard". Kept for them rather
     * than deleted, and named here so the next reader does not go looking for the caller this sentence used to
     * claim. <b>Do not reach for it from main code</b> - a site that has judged a particular container wants
     * {@link #evictIfStillResident}, and one that was handed a record wants {@link #removeWorkForRevokedRecord}.
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
     * <b>This one is by KEY and takes whatever is there, which is correct for its callers and wrong for a
     * caller that inspected a container first</b> - {@link #onSuccess} is removing a container that by
     * construction cannot have been replaced (a non-stale resident is never replaced by
     * {@link #addWorkContainer}). A caller that judged a <em>particular</em> container and now wants that one
     * gone must use {@link #evictIfStillResident} instead, because between the judgement and the removal a
     * replacement can arrive.
     * <p>
     * <b>The revocation sweep used to be listed here as the other correct caller, on the grounds that it is
     * emptying an offset regardless of who occupies it. That was wrong</b>, and it is
     * {@link #removeWorkForRevokedRecord} now: the sweep is handed the records ONE generation was still
     * carrying as incomplete, so "whoever occupies it" is exactly the distinction it needs to draw.
     */
    private WorkContainer<K, V> retire(WorkContainer<K, V> removed) {
        if (removed == null) {
            return null;
        }
        population.onRetired();
        excludeFromSelection(removed);
        return removed;
    }

    /**
     * Removes an offset's occupant <b>only if it is still the exact container the caller inspected</b>, and
     * retires it if so.
     * <p>
     * This is the removal for any site that looked at a container, made a decision about <em>that</em> container,
     * and now wants it gone: the decision and the removal are two steps, and the writer on the other thread can
     * land a replacement between them. {@link Map#remove(Object, Object)} settles it in one atomic step, and
     * {@link WorkContainer}'s identity equality is what makes the comparison the map performs the one the caller
     * meant - a compare-and-remove is defined by the value type's {@code equals}, so with coordinate equality no
     * map API could express which of two containers at one offset was intended. That class javadoc carries the
     * reasoning; {@code docs/solutions/logic-errors/a-by-key-removal-cannot-say-which-container-it-meant-2026-09-07.md}
     * carries the measurement, including why {@code computeIfPresent} with an identity check in the remapping
     * function does not do instead.
     *
     * @return the container this call evicted, or {@code null} if the offset had already been taken over or
     *         emptied - in which case this call changed nothing and must account for nothing
     */
    private WorkContainer<K, V> evictIfStillResident(long offset, WorkContainer<K, V> inspected) {
        return workMap.remove(offset, inspected)
                ? retire(inspected)
                : null;
    }

    /**
     * The revocation and lost sweep's removal: empties the offset of the container the revoked generation
     * registered, and leaves a LIVE container that a later registration put there.
     * <p>
     * <b>The sweep is handed a {@link ConsumerRecord}, not a container, and that record IS the identifier.</b>
     * {@code PartitionState.maybeRegisterNewPollBatchAsWork} puts the same record instance into its
     * {@code incompleteOffsets} and into the {@link WorkContainer} it builds, so "the container this generation
     * registered for this record" is a reference comparison. A later generation re-delivering the same offset
     * supplies a DIFFERENT record object, from a different fetch, which is what tells the two apart.
     * <p>
     * <b>That identity has one named exception, and the second condition below is what covers it.</b>
     * {@code addNewIncompleteRecord} puts unconditionally while {@link #addWorkContainer} DROPS an arrival whose
     * resident is not stale, so an in-generation replay of an already-registered offset - a seek, an offset-reset
     * or a truncation replay - leaves the partition naming record B while this shard still holds the container
     * over record A. Then the comparison is false about a container that IS the right one. It is the same
     * reopener {@link #addWorkContainer}'s own cleared suspicion names; nothing in main calls {@code seek} today.
     * <p>
     * <b>The removal used to be {@code removeWorkAtOffset(record.offset())}</b>, which takes whatever occupies
     * the offset when it lands - astubbs/parallel-consumer#468's defect class, reported at this site by
     * astubbs/parallel-consumer#483's sweep and fixed here. If a live container has taken the offset, evicting it
     * loses the record: it is gone from the shard while its own {@link PartitionState} still carries the offset as
     * incomplete, so nothing selects it and the commit high-water mark cannot pass it until the partition is
     * re-polled. The eviction is therefore conditional on the container this method inspected, settled in one
     * atomic step by {@link #evictIfStillResident} - a compare-and-remove, which means "this container" only
     * because {@link WorkContainer}'s equality is identity.
     * <p>
     * <b>Two conditions, because registration identity alone declines in a case where declining is wrong.</b> An
     * occupant from another registration that is STALE has to go: leaving it is the state this sweep exists to
     * prevent, where a stale container holds an offset of a revoked partition against the next assignment's work.
     * So the only thing declined is a live container from another registration, which is exactly the defect case
     * and nothing else - every other caller and every existing harness sees the behaviour it saw before.
     * <p>
     * <b>ON TODAY'S ONLY CALLER THE DECLINE BRANCH CANNOT FIRE, and that is a stronger unreachability argument
     * than the thread one.</b> {@code PartitionStateManager.resetOffsetMapAndRemoveWork} installs
     * {@code RemovedPartitionState} for the partition BEFORE it calls in, and that state answers
     * {@code isPartitionRemovedOrNeverAssigned}, so {@code checkIfWorkIsStale} is true for EVERY occupant and the
     * second condition carries all of them - this method is behaviourally identical to a by-key removal on the
     * shipped path. The branch is defence for a caller that sweeps BEFORE the swap, which is exactly what
     * {@code ShardManagerLincheckTest.revokeSweep} does. Structural, and it does not rest on which thread runs
     * what; the thread-ordering argument in {@code ShardRevokeSweepReplacementEvictionTest}'s javadoc is the
     * weaker, caller-shaped one, and neither is checked by anything.
     * <p>
     * CLEARED 2026-09-09 - SUSPECTED: that the staleness question can NPE out of a rebalance callback, the way
     * confluentinc#757 and astubbs#345 both did on this path, because
     * {@code PartitionStateManager.getPartitionState} answers null for a partition that was never assigned. Note
     * the state is resolved from the OCCUPANT, not from {@code revokedRecord}, and in the only branch that asks
     * the occupant is by definition a different registration - so the discriminator has to be about the occupant.
     * It is TWO facts: the state is installed before the call (and the {@code partition == null} case
     * {@code continue}s without sweeping at all), and every {@link ShardKey} variant is partition-scoped -
     * {@code KeyOrderedKey} holds a {@code TopicPartition}, {@code TopicPartitionKey} is one - so an occupant of
     * this shard necessarily belongs to the revoked partition. WHAT WOULD REOPEN IT: a second caller from a path
     * that has not installed a state, or <b>a topic-scoped shard key</b>, which would admit an occupant from a
     * partition that may never have been assigned. That second reopener is the one
     * {@link #addWorkContainer}'s cleared suspicion already lists for this class, and it reopens both at once.
     * Nothing goes red for either - the open null-safety decision is
     * {@code docs/inflight/core-stale-arrival-guard-needs-a-null-safety-decision.md}.
     *
     * @param revokedRecord the record the revoked generation was still carrying as incomplete
     * @return the container this call evicted, or {@code null} if the offset was already empty or is now held by a
     *         live container from a later registration - in which case this call changed nothing and must account
     *         for nothing
     */
    WorkContainer<K, V> removeWorkForRevokedRecord(ConsumerRecord<K, V> revokedRecord) {
        long offset = revokedRecord.offset();
        WorkContainer<K, V> occupant = workMap.get(offset);
        if (occupant == null) {
            return null;
        }
        // Reference identity is the SUBJECT here: same coordinates, different registration is the case this
        // whole method exists to tell apart. No @SuppressWarnings("ReferenceEquality") - measured, it suppresses
        // nothing: neither WorkContainer nor Kafka's ConsumerRecord overrides equals, so Error Prone does not
        // fire, which is also why isResident's == below carries no suppression either.
        boolean isTheRegistrationBeingRevoked = occupant.getCr() == revokedRecord;
        if (!isTheRegistrationBeingRevoked && !isWorkContainerStale(occupant)) {
            log.debug("Revoke/lost sweep leaves offset {} alone: it is held by a live container from a later " +
                    "registration ({}), not by the record this revocation named", offset, occupant);
            return null;
        }
        return evictIfStillResident(offset, occupant);
    }



    // remove staled WorkContainer otherwise when the partition is reassigned, the staled messages will:
    // 1. block the new work containers to be picked and processed
    // 2. will cause the consumer to paused consuming new messages indefinitely
    public List<WorkContainer<K, V>> removeStaleWorkContainersFromShard() {
        List<WorkContainer<K, V>> staleContainers = new ArrayList<>();
        for (Map.Entry<Long, WorkContainer<K, V>> entry : workMap.entrySet()) {
            WorkContainer<K, V> inspected = entry.getValue();
            if (isWorkContainerStale(inspected)) {
                // Not iterator.remove(), and not removeWorkAtOffset(key) either: both remove whatever occupies the
                // offset when they land, and this thread is the broker poller inside a rebalance callback while
                // addWorkContainer's stale-replacement branch runs on the controller. Nothing orders the two, so
                // the container answered "stale" above can have been replaced by a FRESH one carrying the current
                // epoch by the time the removal runs - and a by-key removal then evicts the replacement. The
                // record is lost: PartitionState still carries its offset as incomplete, so nothing selects it
                // again until the partition is re-polled.
                //
                // The removal is therefore conditional on the container this loop inspected, settled in one
                // atomic step. Nothing evicted means the replacement won, which is the correct outcome and not a
                // failure: this call changed nothing, so it retires nothing and reports nothing - and the
                // downstream retry-queue removal in ShardManager.removeStaleContainers stays gated on a real
                // shard removal, which is the invariant astubbs/parallel-consumer#437 pinned.
                WorkContainer<K, V> evicted = evictIfStillResident(entry.getKey(), inspected);
                if (evicted != null) {
                    staleContainers.add(evicted);
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
            scanMeter.onEntryExamined();

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
                    // reached it - it still has to be retired and released like every other departure. The queue
                    // removal below is on the CONTROLLER thread (this whole method is), so it may wait for the
                    // write lock, and it is kept because it costs one already-uncontended acquisition to retire
                    // the pair in one step. It is no longer the only thing standing between this container and a
                    // permanent orphan: ShardManager.purgeDepartedRetryEntries() collects an entry whose
                    // container is resident in no shard, on the pass after this one.
                    //
                    // Conditional on the container this scan inspected, for the same reason
                    // removeStaleWorkContainersFromShard is: a by-key removal here would evict a fresh
                    // replacement.
                    //
                    // CLEARED SUSPICION, 2026-09-07: unlike the epoch-change sweep, this site is not
                    // reachable by that race today. Suspected because it has the same shape - inspect a
                    // container, then remove its offset. The discriminator is that addWorkContainer is the
                    // ONLY writer of workMap outside tests and runs on the controller, which is also the
                    // thread that runs this scan, so no replacement can land between the two statements.
                    // It reopens the moment anything puts into a shard off the controller thread, and
                    // nothing would go red if that happened - hence the conditional form anyway: it costs
                    // the same, and it does not rest on a thread-confinement claim nothing checks.
                    //
                    // That discriminator is the SAME single-writer fact the displacement branch's own cleared
                    // suspicion rests on (see addWorkContainer), reached independently from the other side. So
                    // "anything puts into a shard off the controller thread" reopens both at once, and
                    // ShardDisplacementOrphanReachabilityTest is the only thing that would notice.
                    log.debug("shard {} there are still stale work container, need to remove container : {}", this, workContainer);
                    WorkContainer<K, V> removed = evictIfStillResident(workContainer.offset(), workContainer);
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

    /**
     * A container the scan could not take, considered for the slow-work warning and counter.
     * <p>
     * <b>A parked record is skipped</b>, and that is not a cosmetic exclusion. "Slow" means work that should have
     * moved and has not; a parked record is work the definition deliberately stopped, so counting it says an
     * instance is struggling when it is doing exactly what it was told (R27, KTD14). Every parked record on a
     * partition would otherwise be re-counted on every shard scan, which is a warning per pass for a set nobody is
     * waiting on.
     */
    private void addToSlowWorkMaybe(Set<WorkContainer<?, ?>> slowWork, WorkContainer<?, ?> workContainer) {
        if (workContainer.isParked()) {
            return;
        }
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
     * Is {@code wc} the container this shard currently holds at its offset?
     * <p>
     * <b>Reference identity, spelled out with {@code ==}.</b> Every caller here is asking "has THIS container left
     * the shard", and a fresh container that replaced a stale one occupies the same offset - so the comparison has
     * to be about the object. {@link WorkContainer}'s equality is now identity too, so the {@code ==} is a
     * restatement rather than a workaround; it stays explicit because this is the one question in the class that
     * must never silently start meaning "a container at the same coordinates".
     * <p>
     * <b>A residency answer is only ever true about the instant it was taken</b>, so a caller may not use it as
     * a guard in front of an action that must not happen to a departed container - that is a check-then-act, and
     * it is the shape this class keeps being fixed to remove. It is safe in the other order: act first, then ask
     * this, then undo if the answer is no. {@link #includeInSelection(WorkContainer)} and
     * {@link ShardManager#onFailure(WorkContainer)} are both built that way, and each says why it closes.
     */
    boolean isResident(WorkContainer<?, ?> wc) {
        return workMap.get(wc.offset()) == wc;
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
     * Whether it is still resident is tested by <b>reference</b> identity: a fresh container that replaced a stale one occupies the same
     * offset, and a comparison by coordinates would let a departed container keep the claim its replacement is now
     * holding.
     */
    private void includeInSelection(WorkContainer<?, ?> wc) {
        if (wc.claimSelection()) {
            workAwaitingSelectionCount.incrementAndGet();
            if (!isResident(wc)) {
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
