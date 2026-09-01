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
     * Removes any tracked work for this record, and removes the shard if it is empty
     */
    private void removeWorkFromShardFor(ConsumerRecord<K, V> consumerRecord) {
        ShardKey shardKey = computeShardKey(consumerRecord);

        // single read - a check-then-get pair here tears against removeShardIfEmpty racing on the control thread
        // (KEY ordering removes empty shards), NPE-ing out of the rebalance listener into consumer.poll
        Optional<ProcessingShard<K, V>> shardOpt = getShard(shardKey);
        if (shardOpt.isPresent()) {
            // remove the work
            WorkContainer<K, V> removedWC = shardOpt.get().removeWorkAtOffset(consumerRecord.offset());

            // remove if in retry queue
            // check null to avoid race condition
            if (Objects.nonNull(removedWC)) {
                this.retryQueue.remove(removedWC);
            }

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
                    ? new ProcessingShard<>(shardKey, options, wm.getPm(), recordPopulation)
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
     * Idempotent - work may have not been removed, either way it's put back
     */
    public void onFailure(WorkContainer<?, ?> wc) {
        log.debug("Work FAILED");

        var key = computeShardKey(wc);
        var shardOptional = getShard(key);

        if (shardOptional.isPresent()) {
            shardOptional.get().onFailure(wc);
            this.retryQueue.add(wc);
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

    // remove stale containers from both processingShards and retryQueue
    public long removeStaleContainers() {
        return processingShards.values().stream()
                .map(ProcessingShard::removeStaleWorkContainersFromShard)
                .flatMap(Collection::stream)
                .map(retryQueue::remove)
                .count();
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
