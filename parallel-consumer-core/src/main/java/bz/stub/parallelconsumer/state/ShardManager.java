package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2025 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.LoopingResumingIterator;
import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.internal.BrokerPollSystem;
import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.metrics.PCMetrics;
import bz.stub.parallelconsumer.metrics.PCMetricsDef;
import io.micrometer.core.instrument.Gauge;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static java.util.Optional.empty;
import static java.util.Optional.of;

/**
 * Shards are local queues of work to be processed.
 * <p>
 * Generally they are keyed by one of the corresponding {@link ProcessingOrder} modes - key, partition etc...
 * <p>
 * This state is shared between the {@link BrokerPollSystem} thread (write - adding and removing shards and work)  and
 * the {@link AbstractParallelEoSStreamProcessor} Controller thread (read - how many records are in the shards?), so
 * must be thread safe.
 *
 * @author Antony Stubbs
 */
// metrics: number of queues, average queue length
@Slf4j
public class ShardManager<K, V> {

    private final PCModule<K, V> module;


    @Getter
    private final ParallelConsumerOptions<?, ?> options;

    private final WorkManager<K, V> wm;

    /**
     * Map of Object keys to Shard
     * <p>
     * Object Type is either the K key type, or it is a {@link TopicPartition}
     * <p>
     * Used to collate together a queue of work units for each unique key consumed
     *
     * @see ProcessingShard
     * @see K
     * @see WorkManager#getWorkIfAvailable()
     */
    // performance: could disable/remove if using partition order - but probably not worth the added complexity in the code to handle an extra special case
    @Getter(AccessLevel.PRIVATE)
    @Setter(AccessLevel.PACKAGE)
    private Map<ShardKey, ProcessingShard<K, V>> processingShards = new ConcurrentHashMap<>();


    /**
     * How many records the shards are currently holding, derived by conservation rather than counted.
     * <p>
     * Shared with every {@link ProcessingShard} this manager creates, so that the admissions and retirements of
     * all shards reduce to one figure that can be read in O(1) from the control thread.
     *
     * @see #getNumberOfRecordsInShards()
     */
    private final RecordPopulation recordPopulation = new RecordPopulation();

    /**
     * Shared by every shard this manager creates, so it survives the removal of emptied shards.
     *
     * @see DispatchScanMeter
     */
    @Getter(AccessLevel.PACKAGE) // visible for testing
    private final DispatchScanMeter dispatchScanMeter = new DispatchScanMeter();

    /**
     * View of {@link WorkContainer}s that need retrying sorted by retryDue.
     */
    @Getter(AccessLevel.PACKAGE) // visible for testing
    private final RetryQueue retryQueue = new RetryQueue();

    /**
     * Iteration resume point, to ensure fairness (prevent shard starvation) when we can't process messages from every
     * shard.
     */
    private Optional<ShardKey> iterationResumePoint = Optional.empty();

    private Gauge shardsSizeGauge;
    private Gauge shardsMaxSizeGauge;
    private Gauge numberOfShardsGauge;

    private final PCMetrics pcMetrics;

    public ShardManager(final PCModule<K, V> module, final WorkManager<K, V> wm) {
        this.module = module;
        this.wm = wm;
        this.options = module.options();
        this.pcMetrics = module.pcMetrics();
        initMetrics();
    }

    /**
     * The shard belonging to the given key
     *
     * @return may return empty if the shard has since been removed
     */
    Optional<ProcessingShard<K, V>> getShard(ShardKey key) {
        return Optional.ofNullable(processingShards.get(key));
    }

    ShardKey computeShardKey(WorkContainer<?, ?> wc) {
        return ShardKey.of(wc, options.getOrdering());
    }

    ShardKey computeShardKey(ConsumerRecord<?, ?> wc) {
        return ShardKey.of(wc, options.getOrdering());
    }

        /**
         * @return Work ready in the processing shards, awaiting selection as work to do
         */
    public long getNumberOfWorkQueuedInShardsAwaitingSelection() {
        // all available container count - (still pending for running retry containers count)
        // => all_available_count - (retryCnt - all_expired_retry_cnt)
        // order matters as there is a race between getting those numbers and state updates - we should err on the higher
        // number - so read retry queue size before shards size.
        //
        // The write order this reads against CHANGED when the shard counter stopped being adjusted in one batch at
        // the end of ProcessingShard#getWorkIfAvailable and started being released per container inside its selection
        // loop: the shard counter now drops FIRST and retryQueue.removeAll runs afterwards, so the window a reader
        // can land in spans the rest of the shard scan rather than two statements. The read order above is still the
        // right one, but no longer for the reason previously written here ("retry queue is updated before shard
        // counters"). Re-derived for a previously-failed container being taken, where S is the shard sum, Q the retry
        // size and r the ready-to-retry count: a reader that sees the retry queue still holding it and the shard sum
        // already without it computes (r+1) + max(0, S-(Q+1)), which equals the settled r + max(0, S-Q) when
        // S-Q >= 1, and exceeds it by one otherwise. So the skew is either nil or high - never low, which is the
        // direction that matters, because reading low is what closes drain() early.
        // it can still be negative due to race between marking containers inflight, updating counters in shards and updates to retryQueue
        // this value should not be used in isolation though - but as part of overall buffer size calculation - which takes into account
        // this number and number of work containers queued in work thread pool.
        // it is safe though to set it to 0 for negative value of shards size - retry queue size portion.

        ParallelConsumer.Tuple<Integer,Long> retryQueueSizeAndNumberReadyToBeRetried = retryQueue.getQueueSizeAndNumberReadyToBeRetried();
        long diffBetweenShardsAndRetrySize = -retryQueueSizeAndNumberReadyToBeRetried.getLeft() + sumOfShardAvailableCounters();
        return retryQueueSizeAndNumberReadyToBeRetried.getRight() + (diffBetweenShardsAndRetrySize < 0 ? 0 : diffBetweenShardsAndRetrySize);
    }

    /**
     * How many records the shards currently hold - selectable, out at a worker, or waiting out a retry delay.
     * <p>
     * Derived by conservation ({@code admitted - retired}) rather than counted, so it cannot disagree with the
     * shards' contents the way a separately maintained running total can, and it is O(1) to read.
     *
     * @see RecordPopulation
     */
    public long getNumberOfRecordsInShards() {
        return recordPopulation.getInSystem();
    }

    /**
     * How many records the shards hold that are parked waiting out a retry delay, and so cannot be worked on yet
     * however much capacity there is.
     * <p>
     * Subtracted from {@link #getNumberOfRecordsInShards()} to get the figure that gates record intake: a
     * consumer whose entire buffer is in retry back-off should keep fetching, or it would idle its workers
     * waiting on delays.
     */
    public long getNumberOfRecordsParkedForRetry() {
        var sizeAndReady = retryQueue.getQueueSizeAndNumberReadyToBeRetried();
        return sizeAndReady.getLeft() - sizeAndReady.getRight();
    }

    /**
     * The record-intake gate's figure, together with the two operands it is derived from, taken as close together
     * as the two structures holding them allow.
     * <p>
     * The subtraction lives here because both operands do: {@link WorkManager} reaching across for
     * {@link #getNumberOfRecordsInShards()} and {@link #getNumberOfRecordsParkedForRetry()} to do the arithmetic
     * itself only spread one figure's definition across two classes. The operands come back as well as the
     * difference so that the diagnostic in {@link WorkManager#isSufficientlyLoaded()} can print the equation the
     * decision was actually made on, rather than re-reading and printing one that never held.
     * <p>
     * <b>This is NOT an atomic snapshot, and no arrangement of these two reads makes it one.</b>
     * {@link #getNumberOfRecordsInShards()} reduces two {@link java.util.concurrent.atomic.LongAdder}s in
     * {@link RecordPopulation}; {@link #getNumberOfRecordsParkedForRetry()} reads {@link RetryQueue} under that
     * queue's own fair read/write lock. No lock spans both, and adding one would put the broker-poll thread's
     * admission path behind the retry queue's fair lock - a redesign, not a tidy-up, and one this figure does not
     * need. What follows is what the skew actually is.
     * <p>
     * <b>Retry-queue movement in the window costs nothing.</b> Parking a record for retry, and its delay
     * expiring, both leave the population untouched - the container stays in its shard throughout. So a retry
     * queue that moves between the two reads does not make the difference wrong: it is exactly right as of the
     * later read.
     * <p>
     * <b>Population movement in the window is the whole of the skew.</b> Reading the population first makes it the
     * stale operand, by however many records another thread admits or retires while the retry-queue read is in
     * progress - a fair read-lock acquisition plus a scan of the queue's head, O(n) in the worst case. An
     * admission missed this way reads the figure <em>low</em>, which fetches sooner than needed. A retirement
     * missed this way reads it <em>high</em>, which is the direction that matters: high is what pauses the poller,
     * and a poller that stays paused is the silent stall of confluentinc#857.
     * <p>
     * <b>It cannot accumulate, which is what makes it tolerable.</b> The gate is resampled every control-loop
     * tick, and once mutation stops both figures are exact - so a skewed sample can only bring one fetch forward
     * or hold it back by one tick, and never at a threshold distance that a tick of real work would not have
     * crossed anyway. That is the difference between this and the defect this figure replaced: separately
     * maintained counters drifted <em>permanently</em>, so the gate could sit wrong forever with nothing to
     * reconcile it.
     */
    public WorkableRecords getWorkableRecords() {
        // Population FIRST, retry queue second - see the class javadoc above for why the order is the one that
        // makes retry-queue movement free rather than merely cheap.
        long inShards = getNumberOfRecordsInShards();
        long parkedForRetry = getNumberOfRecordsParkedForRetry();
        return new WorkableRecords(inShards, parkedForRetry);
    }

    /**
     * The load gate's figure and its two operands, from one call to {@link #getWorkableRecords()}.
     * <p>
     * It exists so a caller that needs the difference <em>and</em> the operands - the gate, which decides on one
     * and logs the others - gets them from a single read rather than reading each twice.
     */
    @Value
    public static class WorkableRecords {

        /**
         * @see ShardManager#getNumberOfRecordsInShards()
         */
        long inShards;

        /**
         * @see ShardManager#getNumberOfRecordsParkedForRetry()
         */
        long parkedForRetry;

        /**
         * @return records held that work capacity can actually advance - the figure the intake gate compares
         *         against its threshold
         */
        public long getWorkable() {
            return inShards - parkedForRetry;
        }
    }

    /**
     * The conservation counters themselves, for tests that need to assert on both sides of the balance.
     */
    // visible for testing
    RecordPopulation getRecordPopulation() {
        return recordPopulation;
    }

    /**
     * Counts the shards' contents by scanning them - O(n), and deliberately independent of the conservation
     * counters, so a test can hold {@link #getNumberOfRecordsInShards()} against it.
     * <p>
     * Read on the debug-only under-served-retrieval path in {@link #getWorkIfAvailable}; the O(1) conservation
     * figure is what anything on a hot path should be reading.
     */
    long countRecordsInShardsByScan() {
        return processingShards.values().stream()
                .mapToLong(ProcessingShard::getCountOfWorkTracked)
                .sum();
    }

    /**
     * The raw sum of the per-shard available-work counters, with no flooring applied.
     * <p>
     * {@link #getNumberOfWorkQueuedInShardsAwaitingSelection()} floors its result, which hides both directions of
     * counter drift from any test that reads it. This is the unfloored figure that method starts from, exposed
     * so drift can be asserted on directly.
     */
    long sumOfShardAvailableCounters() {
        return processingShards.values().stream()
                .mapToLong(ProcessingShard::getCountOfWorkAwaitingSelection)
                .sum();
    }

    public boolean workIsWaitingToBeProcessed() {
        return getNumberOfWorkQueuedInShardsAwaitingSelection() > 0L;
    }

    /**
     * Remove only the work shards which are referenced from work from revoked partitions
     *
     * @param recordsFromRemovedPartition collection of work to scan to get keys of shards to remove
     */
    void removeAnyShardEntriesReferencedFrom(Collection<Optional<ConsumerRecord<K, V>>> recordsFromRemovedPartition) {
        List<ConsumerRecord<K, V>> polledRecordsFromPartition = recordsFromRemovedPartition.stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
        for (ConsumerRecord<K, V> consumerRecord : polledRecordsFromPartition) {
            removeWorkFromShardFor(consumerRecord);
        }
    }

    /**
     * Removes any tracked work for this record, and removes the shard if it is empty.
     * <p>
     * <b>Shards only - this runs on the broker-poll thread inside a rebalance callback, and it must not touch
     * the retry queue.</b> It used to, with {@code retryQueue.remove(removedWC)}, which takes that queue's
     * unbounded FAIR write lock; the controller thread holds the matching READ lock for the whole of
     * {@link #getLowestRetryTime()}'s scan, so the callback could wait out a full scan inside
     * {@code consumer.poll()} with the group waiting on it. The shard map is a
     * {@link java.util.concurrent.ConcurrentHashMap} of {@link java.util.concurrent.ConcurrentSkipListMap}s, so
     * the removal below waits for nothing and there is no lock to decline.
     * <p>
     * <b>What that leaves, and who collects it.</b> A retry-queue entry whose container is now resident in no
     * shard - garbage, and {@link #purgeDepartedRetryEntries()} is what collects it, on the controller thread
     * where waiting is allowed. That method states the invariant and its bound; do not restate them here.
     * <p>
     * <b>The removal is CONDITIONAL on the container the revoked record was registered as</b>, not by key -
     * {@link ProcessingShard#removeWorkForRevokedRecord} owns why, and holds the cleared suspicion about the
     * staleness question it asks. This method's own contribution is the single-read {@code getShard} above it and
     * the shard garbage collection below.
     */
    private void removeWorkFromShardFor(ConsumerRecord<K, V> consumerRecord) {
        ShardKey shardKey = computeShardKey(consumerRecord);

        // single read - a check-then-get pair here tears against removeShardIfEmpty racing on the control thread
        // (KEY ordering removes empty shards), NPE-ing out of the rebalance listener into consumer.poll
        Optional<ProcessingShard<K, V>> shardOpt = getShard(shardKey);
        if (shardOpt.isPresent()) {
            // LOGGED rather than parked in an ignored local, which is a dead store SpotBugs reports in main
            // code - the same trade already settled at onFailure below. What this answer USED to decide was
            // whether to pair a retry-queue removal with it, and that is exactly what moved to the controller
            // thread; null now means EITHER the container had already gone OR a live container from a later
            // registration owns the offset, and neither is an error on this path.
            WorkContainer<K, V> removedFromTheShard = shardOpt.get().removeWorkForRevokedRecord(consumerRecord);
            log.trace("Revoke/lost sweep removed {} from shard {} - the retry queue is deliberately untouched here",
                    removedFromTheShard, shardKey);

            // remove the shard if empty
            removeShardIfEmpty(shardKey);
        } else {
            // covers both already-removed-before-the-sweep and removed-against-this-read; the third null
            // on this path, after the shard's own long-standing guard and confluentinc#757's retryQueue one
            log.trace("Shard referenced by WC: {} with shard key: {} already removed", consumerRecord, shardKey);
        }

    }

    void addWorkContainer(long epochOfInboundRecords, ConsumerRecord<K, V> aRecord) {
        var wc = new WorkContainer<>(epochOfInboundRecords, aRecord, module);
        ShardKey shardKey = computeShardKey(wc);

        // Choosing the shard and writing to it have to be ONE step, not two.
        //
        // computeIfAbsent followed by shard.addWorkContainer() hands the caller a shard and then lets go of
        // the map: under KEY ordering removeShardIfEmpty() can garbage-collect that very shard in between,
        // on the control thread, and the record is then admitted into a shard no scan will ever reach. The
        // record is lost either way - that part is not new - but the admission is not, and nothing ever
        // retires it, so getNumberOfRecordsInShards() reads permanently high and eventually holds the
        // broker poller paused for good. The old gate summed only the shards still IN this map, so an
        // orphan simply disappeared from it; a conservation figure cannot forget.
        //
        // compute() here and computeIfPresent() in removeShardIfEmpty() take the same per-key lock, so a
        // shard can no longer be dropped between being chosen and being written to.
        processingShards.compute(shardKey, (ignore, existingShard) -> {
            var shard = (existingShard == null)
                    ? new ProcessingShard<>(shardKey, options, wm.getPm(), recordPopulation, dispatchScanMeter)
                    : existingShard;
            shard.addWorkContainer(wc);
            return shard;
        });
    }

    void removeShardIfEmpty(ShardKey key) {
        // If using KEY ordering, where the shard key is a message key, garbage collect old shard keys (i.e. KEY ordering we may never see a message for this key again)
        // If not, no point to remove the shard, as it will be reused for the next message from the same partition
        if (!options.getOrdering().equals(KEY)) {
            return;
        }
        // The emptiness test and the removal are one step, against the same per-key lock addWorkContainer()
        // takes - see there for what a shard dropped mid-insertion costs.
        processingShards.computeIfPresent(key, (ignore, shard) -> {
            if (shard.isEmpty()) {
                log.trace("Removing empty shard (key: {})", key);
                return null;
            }
            return shard;
        });
    }

    public void onSuccess(WorkContainer<?, ?> wc) {
        // remove from the retry queue if it's contained
        this.retryQueue.remove(wc);

        // remove from processing queues
        var key = computeShardKey(wc);
        var shardOptional = getShard(key);

        if (shardOptional.isPresent()) {
            //
            shardOptional.get().onSuccess(wc);
            removeShardIfEmpty(key);
        } else {
            log.trace("Dropping successful result for revoked partition {}. Record in question was: {}", key, wc.getCr());
        }
    }

    /**
     * Idempotent - work may have not been removed, either way it's put back.
     * <p>
     * <b>The queue entry is added FIRST and its shard residency confirmed SECOND</b>, and that order is the
     * whole of this method's thread safety. {@link WorkManager#onFailureResult} re-validates the epoch against
     * the live partition map immediately before calling this, but says at the site that no epoch check can ever
     * be atomic with the actions that follow it - so the revoke sweep on the broker-poll thread can complete in
     * the gap. It removes this container from its shard; under PARTITION or UNORDERED ordering the emptied
     * shard object survives (only KEY ordering garbage-collects one), so {@link #getShard} still answers present
     * and the add below goes through anyway. What that used to leave is a <b>queue-only orphan</b>: work is
     * handed out by scanning shards, so a container in no shard is never selected, never completed and never
     * swept - and every route that removed a retry-queue entry reached it THROUGH shard contents, so nothing
     * could ever take it out again. {@link #purgeDepartedRetryEntries()} is now that route, and it does not go
     * through the shards; the confirmation below is what keeps the common case from needing it.
     * <p>
     * <b>Asking about residency before adding would only narrow the window</b> - it is another check-then-act,
     * and the sweep can land between that answer and the add exactly as it lands between the epoch check and
     * this call. Reversing the order closes it instead, because the last thing to happen is a REMOVAL driven by
     * a read taken after the add, and the sweep's own action is also a removal.
     * <p>
     * <b>THE SWEEP NO LONGER REMOVES FROM THE QUEUE AT ALL, so this confirmation narrows rather than closes -
     * and {@link #purgeDepartedRetryEntries()} is what closes it.</b> {@link #removeWorkFromShardFor} and
     * {@link #removeStaleContainers} run on the broker-poll thread and touch the shards only, which is what
     * keeps them off this queue's fair write lock inside {@code poll()}. Against a shard-only sweep:
     * <ul>
     * <li>the sweep completes before the add - the residency read below sees a departed container and undoes
     *     the add, so this method still closes that half on its own;</li>
     * <li>the sweep lands between the add and the residency read - its shard removal has already happened, so
     *     the read again sees a departed container and undoes the add;</li>
     * <li><b>the sweep starts after the residency read</b> - the read saw a resident container, the add stands,
     *     and the sweep then removes the container from its shard. Nothing here can undo that, because the
     *     residency answer this method took was true when it was taken. The entry is a departed one from that
     *     moment, and the purge collects it on the controller's next pass - one control-loop tick.</li>
     * </ul>
     * Once the read below sees a departed container the answer cannot go stale in the dangerous direction:
     * residency is by reference, and nothing ever re-inserts the same container instance.
     * <p>
     * <b>So the confirmation below is now belt-and-braces, and is kept deliberately.</b> The purge would
     * collect every orphan it prevents, a tick later; what this keeps is the tick, on the common interleaving,
     * for the price of one reference comparison. Whether it earns that is a live question - see the PR that
     * introduced the purge - and the answer must not be assumed from the fact that both exist.
     * <p>
     * <b>The superseded alternative, named because the history reads as though it were the plan.</b>
     * astubbs/parallel-consumer#431 kept the poll thread on the queue and made it DECLINE, asking the queue
     * first with a non-blocking {@code tryRemove} so a refusal abandoned the paired shard removal and the pair
     * never split. That ordering defeats a one-shot confirmation - the sweep's queue removal passes over an
     * empty queue, the controller then adds and reads residency while the container is still resident - so it
     * had to repeat the queue removal after the shard removal. Correct, and more machinery than a controller
     * that collects garbage; {@code docs/solutions/runtime-errors/retry-queue-write-lock-on-the-rebalance-path.md}
     * carries both designs.
     * <p>
     * <b>This adds no lock and makes no thread wait.</b> The alternative - moving the shard map and the queue
     * under one lock - would put the broker-poll thread's rebalance callbacks behind the retry queue's fair
     * lock, which is the wait the purge exists to keep off that thread. Nothing here touches the poll thread's
     * side of the interleaving at all.
     * <p>
     * <b>What else the purge now backstops.</b> {@link ProcessingShard#addWorkContainer} displaces a stale
     * resident without removing its queue entry, because the shard has no handle on the queue - a separate,
     * tracked defect whose entry the purge also collects, since a displaced container is resident in no shard
     * by reference identity. Its own note owns the rest.
     * <p>
     * <b>And it has nothing to collect there, because that branch was proven UNREACHABLE with a queue-resident
     * container on 2026-09-08</b> - so the backstop above is a genuine belt-and-braces rather than the thing
     * standing between that branch and an orphan. The two answers are independent and both worth keeping: the
     * purge bounds the harm whatever happens, and the proof says the case does not arise. The cleared
     * suspicion, its discriminator and what would reopen it are recorded on {@code addWorkContainer} itself;
     * {@code ShardDisplacementOrphanReachabilityTest} is the durable form.
     *
     * @see ProcessingShard#isResident(WorkContainer)
     */
    public void onFailure(WorkContainer<?, ?> wc) {
        log.debug("Work FAILED");

        var key = computeShardKey(wc);
        var shardOptional = getShard(key);

        if (shardOptional.isPresent()) {
            var shard = shardOptional.get();
            shard.onFailure(wc);
            this.retryQueue.add(wc);

            // Confirm residency AFTER the add, and undo it if the container has left - see the javadoc for why
            // this order closes the window that asking first only narrows.
            if (!shard.isResident(wc)) {
                // The removal's answer is LOGGED rather than dropped, because a bare call here cannot be told
                // from a forgotten check. What it MEANS changed when the sweep stopped touching the queue: it
                // used to say which of two racing removals won, and both answers were correct. Now nothing
                // races it. The add a few lines above put the entry there, the sweep never removes from this
                // queue, and every other removal - purgeDepartedRetryEntries, ProcessingShard's removeAll and
                // its last-resort stale branch - is on this same controller thread and later in the pass. So
                // TRUE is the only outcome production can now produce.
                //
                // FALSE IS THEREFORE A SIGNAL, not an alternative: it would mean a second writer of the retry
                // queue exists, which is the one thing that would invalidate purgeDepartedRetryEntries()'s
                // scan-then-remove. Nothing asserts it here - this is main code on a hot path - so it is logged
                // in the form a reader can act on. (Logged rather than assigned to an ignored local: a dead
                // store in main code trades one static-analysis finding for another - see
                // docs/inflight/static-error-prone-rule-registry.md, `ReturnValueIgnored`.)
                boolean thisCallRemovedIt = this.retryQueue.remove(wc);
                log.debug("Failed work left its shard while it was being re-queued (its partition was revoked); " +
                        "taking the retry queue entry back out so it cannot be orphaned - this call removed it: " +
                        "{} (FALSE would mean something else removed it, i.e. a second writer of the retry " +
                        "queue, which nothing should be). {}",
                        thisCallRemovedIt, wc);
            }
        }

    }

    /**
     * Work returned without a verdict - restores shard availability but, unlike {@link #onFailure}, does
     * <em>not</em> insert into the retry queue. There is nothing to retry: the record was never attempted to a
     * conclusion, so it becomes immediately selectable rather than waiting out a retry delay it never earned.
     * <p>
     * Idempotent in the same sense as {@link #onFailure} - work may or may not have been removed already, and the
     * shard's selection claim is a compare-and-set, so a repeat call counts the container once.
     */
    public void onAbandoned(WorkContainer<?, ?> wc) {
        log.debug("Work ABANDONED without verdict");

        var key = computeShardKey(wc);
        getShard(key).ifPresent(shard -> shard.onAbandoned(wc));
    }

    /**
     * @return none if there are no messages to retry
     */
    public Optional<Duration> getLowestRetryTime() {
        // find the first in the queue
        try (RetryQueue.RetryQueueIterator retryQueueIterator = this.retryQueue.iterator()) {
            while (retryQueueIterator.hasNext()) {
                WorkContainer<?, ?> workContainer = retryQueueIterator.next();
                // Would only be in edge case of race between picking container for work (when its marked in-flight) and
                // updating retryQueue - so still double-checking here to only consider not inflight ones.
                if (workContainer.isNotInFlight())
                    return of(workContainer.getDelayUntilRetryDue());
            }
            return empty();
        }
    }

    public List<WorkContainer<K, V>> getWorkIfAvailable(final int requestedMaxWorkToRetrieve) {
        // FIRST, and before the shard scan below: this is the once-per-control-loop-pass point the purge's
        // one-tick bound is expressed in, and drain() reads the awaiting-selection figure later in the same
        // pass.
        purgeDepartedRetryEntries();

        LoopingResumingIterator<ShardKey, ProcessingShard<K, V>> shardQueueIterator =
                new LoopingResumingIterator<>(iterationResumePoint, this.processingShards);

        //
        List<WorkContainer<K, V>> workFromAllShards = new ArrayList<>();

        // loop over shards, and get work from each
        Optional<Map.Entry<ShardKey, ProcessingShard<K, V>>> next = shardQueueIterator.next();
        while (workFromAllShards.size() < requestedMaxWorkToRetrieve && next.isPresent()) {
            var shardEntry = next;
            ProcessingShard<K, V> shard = shardEntry.get().getValue();

            //
            int remainingToGet = requestedMaxWorkToRetrieve - workFromAllShards.size();
            var work = shard.getWorkIfAvailable(remainingToGet, retryQueue);
            workFromAllShards.addAll(work);

            // next
            next = shardQueueIterator.next();
        }

        // log
        if (workFromAllShards.size() >= requestedMaxWorkToRetrieve) {
            log.debug("Work taken is now over max (iteration resume point is {})", iterationResumePoint);
        }

        // Silent-stall diagnostic (confluentinc#857): the control loop asked for work but we handed back less than
        // requested even though work is still tracked in the shards. Break down WHY so a stall can be told
        // apart from normal back-pressure. See docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md
        if (log.isDebugEnabled() && workFromAllShards.size() < requestedMaxWorkToRetrieve) {
            long tracked = countRecordsInShardsByScan();
            if (tracked > 0) {
                long awaitingSelection = sumOfShardAvailableCounters();
                long inFlight = processingShards.values().stream().mapToLong(ProcessingShard::getCountWorkInFlight).sum();
                var retry = retryQueue.getQueueSizeAndNumberReadyToBeRetried();
                // Interpretation guide:
                //  - returned 0 with awaitingSelection > 0  => STALL: selectable work exists but was not handed out (a real bug)
                //  - tracked all inFlight                   => normal: worker pool is just busy
                //  - tracked all in retryQueue, none ready  => normal: retry back-off (records failed, waiting to retry)
                //  - tracked > awaitingSelection+inFlight and none ready to retry => work is "missing"/stuck (candidate leak)
                log.debug("Work retrieval under-served: requested {}, returned {}, but {} tracked across {} shard(s) " +
                                "[awaitingSelection={}, inFlight={}, retryQueue.size={}, retryQueue.readyToRetry={}]",
                        requestedMaxWorkToRetrieve, workFromAllShards.size(), tracked, processingShards.size(),
                        awaitingSelection, inFlight, retry.getLeft(), retry.getRight());
            }
        }

        //
        updateResumePoint(next);

        return workFromAllShards;
    }

    /**
     * The epoch-change stale sweep: retire from the SHARDS every container whose partition has moved on.
     * <p>
     * <b>Shards only, and for the same reason as {@link #removeWorkFromShardFor}</b> - this is reached from
     * {@code onPartitionsAssigned} as well as from {@code onPartitionsRemoved}, so it runs on the broker-poll
     * thread inside a rebalance callback. It used to finish with {@code .map(retryQueue::remove)}, a METHOD
     * REFERENCE rather than a call, which is why {@code ArchitectureTest.rebalanceCallbacksMustNotBlock} could
     * not see it until that rule learned to follow references.
     * <p>
     * The retry-queue entries the sweep leaves behind are collected by {@link #purgeDepartedRetryEntries()} on
     * the controller thread.
     *
     * @return how many containers were retired from shards
     */
    public long removeStaleContainers() {
        return processingShards.values().stream()
                .map(ProcessingShard::removeStaleWorkContainersFromShard)
                .flatMap(Collection::stream)
                .count();
    }

    /**
     * Collect every retry-queue entry whose container is resident in no shard.
     * <p>
     * <b>The invariant, stated once, here.</b> A retry-queue entry whose container is resident in no shard is
     * <em>garbage the controller collects</em>. It may exist for at most one control-loop tick, and the
     * rebalance callbacks are free to create it - that freedom is the whole point, because it is what lets them
     * remove from the shards alone and never wait on this queue's fair write lock inside {@code poll()}.
     * <p>
     * <b>Why an entry with no resident container is garbage.</b> Work is handed out by scanning shards, so a
     * container in no shard is never selected, never completed and never swept - and every other route that
     * removes a retry-queue entry reaches it THROUGH shard contents. Left alone, the entry counts in
     * {@link #getNumberOfWorkQueuedInShardsAwaitingSelection()} forever once its retry delay elapses, which is
     * the figure behind {@code AbstractParallelEoSStreamProcessor#drain()} - so one entry holds a draining
     * close open to its timeout with nothing in the system (astubbs/parallel-consumer#437 measured that).
     * <p>
     * <b>The bound is one control-loop tick, in every ordering mode.</b> This runs at the top of
     * {@link #getWorkIfAvailable(int)}, which the control loop reaches once per pass through
     * {@code retrieveAndDistributeNewWork} in both {@code RUNNING} and {@code DRAINING}, and before
     * {@code drain()} reads the figure above in that same pass. It scans the QUEUE rather than the shards, so
     * the ordered modes' shard-scan break - which stops the scan at the first container taken - cannot delay
     * it. (The superseded astubbs/parallel-consumer#431 design retired abandoned pairs from inside that scan,
     * and so had the weaker bound "until the takeable head in front of it leaves the shard".)
     * <p>
     * <b>Why the scan and the removal can be two steps.</b> The entries are read under the read lock and
     * removed afterwards under the write lock, which is a check-then-act - and it is safe here for two
     * independent reasons, both of which have to hold:
     * <ul>
     * <li><b>Departure is monotonic.</b> {@link ProcessingShard#isResident(WorkContainer)} is reference
     *     identity, and nothing ever re-inserts the same container instance into a shard, so an answer of "not
     *     resident" cannot go stale in the dangerous direction.</li>
     * <li><b>Nothing else can put an entry at those keys in between.</b> This matters because
     *     {@link RetryQueue} removes BY KEY - topic, partition and offset ({@code WorkContainerKey.of}) - so
     *     the removal below cannot say which container it meant, and a fresh entry arriving at a departed
     *     container's coordinates would be taken out instead. That is the defect class recorded in
     *     {@code git show 7c95b75ce^:docs/inflight/bug-stale-sweep-iterator-evicts-fresh-replacement.md} (retired when astubbs/parallel-consumer#468 fixed it) and fixed on the shard's
     *     side by astubbs/parallel-consumer#468, where the two sides really were different threads. Here they are
     *     not: {@link RetryQueue#add} is {@link ControllerThreadOnly}, its only production caller is
     *     {@link #onFailure}, and both run on this same thread - so there is no add to race and no window to
     *     narrow. <b>What would reopen it is a second writer</b>, which is the same condition as the bullet
     *     below.</li>
     * </ul>
     * <b>What would reopen it</b>: an add to {@link #retryQueue} from any thread but the controller. That is
     * what {@link ControllerThreadOnly} on {@code add} declares and what
     * {@code ArchitectureTest.rebalanceCallbacksMustNotBlock} reports a rebalance-callback reach into - but
     * nothing checks the general case, and the runtime ownership guard that would is tracked in
     * {@code docs/inflight/core-retry-queue-needs-a-runtime-controller-ownership-guard.md}.
     *
     * <b>It returns nothing on purpose.</b> Nothing in production reads a count, and a caller that named one
     * only to drop it would be a dead store - the same finding already avoided at {@link #onFailure} by
     * logging instead. What is worth knowing is logged below; a meter, if the scan's cost is ever measured,
     * belongs inside here rather than at the one call site.
     */
    @ControllerThreadOnly
    void purgeDepartedRetryEntries() {
        List<WorkContainer<?, ?>> departed = new ArrayList<>();
        try (RetryQueue.RetryQueueIterator entries = this.retryQueue.iterator()) {
            while (entries.hasNext()) {
                WorkContainer<?, ?> entry = entries.next();
                if (!isResidentInItsShard(entry)) {
                    departed.add(entry);
                }
            }
        }

        // OUTSIDE the read lock: the iterator holds it until it is closed, and this needs the write lock.
        // removeAll's own fast path returns without acquiring anything when the list is empty, which is the
        // common case on every tick.
        boolean modified = this.retryQueue.removeAll(departed);
        if (!departed.isEmpty()) {
            // `modified` is read here rather than dropped, and it is the same signal as the one at onFailure:
            // this thread is the queue's only writer, so it can only be false if something else removed these
            // entries first - i.e. a second writer, which is exactly what would invalidate the scan-then-remove
            // above.
            log.debug("Collected {} retry queue entries whose containers are resident in no shard - a rebalance " +
                    "callback removed them from their shards and deliberately left the queue alone. Queue " +
                    "reported modified={} (FALSE would mean a second writer of the retry queue, which nothing " +
                    "should be). {}",
                    departed.size(), modified, departed);
        }
    }

    private boolean isResidentInItsShard(WorkContainer<?, ?> wc) {
        // An absent shard is a departure too: under KEY ordering removeShardIfEmpty garbage-collects a shard
        // once it is empty, so "no shard" and "shard without it" are the same answer to the only question here.
        return getShard(computeShardKey(wc))
                .map(shard -> shard.isResident(wc))
                .orElse(false);
    }

    private void updateResumePoint(Optional<Map.Entry<ShardKey, ProcessingShard<K, V>>> lastShard) {
        // if empty, iteration was exhausted and no resume point is needed
        iterationResumePoint = lastShard.map(Map.Entry::getKey);
        if (iterationResumePoint.isPresent()) {
            log.debug("Work taken is now over max, stopping (saving iteration resume point {})", iterationResumePoint);
        }
    }

    /**
     * Per-shard queue depths, as a fresh stream. Only {@code SHARDS_MAX_SIZE} needs it: the total is the
     * conservation figure, which is O(1) and cannot disagree with the shards the way a scan of drifting
     * per-shard counters can.
     */
    private LongStream shardEntryCounts() {
        return processingShards.values().stream().mapToLong(ProcessingShard::getCountOfWorkTracked);
    }

    private void initMetrics() {
        shardsSizeGauge = pcMetrics.gaugeFromMetricDef(PCMetricsDef.SHARDS_SIZE,
                this, ShardManager::getNumberOfRecordsInShards);
        // TODO(refactor): walks every shard queue, and ConcurrentSkipListMap.size() is O(n), so each
        // scrape is O(total queued records). SHARDS_SIZE above no longer pays that - it reads the O(1)
        // conservation figure - so this is now the only scan per scrape rather than one of two.
        // Triaged as negligible; docs/refactoring.md owns the assessment and the fix under
        // "state/ShardManager.java", with the upstream shard-count-caching design under "Performance".
        // Do not restate the fix here - two copies of it had already drifted apart once.
        shardsMaxSizeGauge = pcMetrics.gaugeFromMetricDef(PCMetricsDef.SHARDS_MAX_SIZE,
                this, shardManager -> shardManager.shardEntryCounts().max().orElse(0));


        numberOfShardsGauge = pcMetrics.gaugeFromMetricDef(PCMetricsDef.NUMBER_OF_SHARDS,
                this, shardManager -> shardManager.processingShards.size());
    }
}
