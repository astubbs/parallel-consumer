package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.RiderContext;
import bz.stub.parallelconsumer.internal.BrokerPollSystem;
import bz.stub.parallelconsumer.internal.EpochAndRecordsMap;
import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.internal.utils.ThrowableUtils;
import bz.stub.parallelconsumer.metrics.PCMetrics;
import bz.stub.parallelconsumer.metrics.PCMetricsDef;
import bz.stub.parallelconsumer.offsets.NoEncodingPossibleException;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager;
import bz.stub.parallelconsumer.offsets.OffsetRiderEnvelope;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tag;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import static bz.stub.parallelconsumer.internal.utils.JavaUtils.*;
import static bz.stub.parallelconsumer.offsets.OffsetMapCodecManager.DefaultMaxMetadataSize;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static lombok.AccessLevel.*;

/**
 * Our view of the state of the partitions that we've been assigned.
 *
 * @author Antony Stubbs
 * @see PartitionStateManager
 */
@ToString
@Slf4j
public class PartitionState<K, V> {

    /**
     * Symbolic value for a parameter which is initialised as having an offset absent (instead of using Optional or
     * null)
     */
    public static final long KAFKA_OFFSET_ABSENCE = -1L;

    private final PCModule<K, V> module;

    @NonNull
    @Getter
    private final TopicPartition tp;

    /**
     * Offsets beyond the highest committable offset (see {@link #getOffsetHighestSequentialSucceeded()}) which haven't
     * totally succeeded. Based on decoded metadata and polled records (not offset ranges).
     * <p>
     * Mapped to the corresponding {@link ConsumerRecord}, once it's been polled from the broker.
     * <p>
     * Initially mapped to an empty optional, until the record is polled from the broker, because we initially get only
     * the incomplete offsets decoded from the metadata payload first, before receiving the records from poll requests.
     * <p>
     * <p>
     * <h2>How does this handle gaps in the offsets in the source partitions?:</h2>
     * <p>
     * We track per record acknowledgement, by only storing the offsets of records <em>OF WHICH WE'VE RECEIVED</em>
     * through {@link KafkaConsumer#poll} calls.
     * <p>
     * This is as explicitly opposed to looking at the lowest offset we've polled, and synthetically creating a list of
     * EXPECTED offsets from the range from it to the highest polled. If we were to construct this offset range
     * synthetically like this, then we would need to expect to process/receive records which might not exist, for
     * whatever reason, usually due to compaction.
     * <p>
     * Instead, the offsets tracked are only determined from the records we've given to process from the broker - we
     * make no assumptions about which offsets exist. This way we don't have to worry about gaps in the offsets. Also, a
     * nice outcome of this is that a gap in the offsets is effectively the same as, as far as we're concerned, an
     * offset which has succeeded - because either way we have no action to take.
     * <p>
     * This is independent of the actual queued {@link WorkContainer}s. This is because to start with, data about
     * incomplete offsets come from the encoded metadata payload that gets committed along with the highest committable
     * offset ({@link #getOffsetHighestSequentialSucceeded()}) and so we don't yet have ConsumerRecord's for those
     * offsets until we start polling for them. And so they are not always in sync.
     * <p>
     * <p>
     * <h2>Concurrency:</h2>
     * <p>
     * Needs to be concurrent because, the committer requesting the data to commit may be another thread - the broker
     * polling sub system - {@link BrokerPollSystem#maybeDoCommit}. The alternative to having this as a concurrent
     * collection, would be to have the control thread prepare possible commit data on every cycle, and park that data
     * so that the broker polling thread can grab it, if it wants to commit - i.e. the poller would not prepare/query
     * the data for itself. This requirement is removed in the upcoming confluentinc PR #200 Refactor: Consider a shared nothing
     * architecture.
     *
     * @see bz.stub.parallelconsumer.offsets.BitSetEncoder for disucssion on how this is impacts per record ack
     *         storage
     */
    @NonNull
    @Setter(PACKAGE)
    private ConcurrentSkipListMap<Long, Optional<ConsumerRecord<K, V>>> incompleteOffsets;

    /**
     * Marks whether any {@link WorkContainer}s have been added yet or not. Used for some initial poll analysis.
     */
    private boolean bootstrapPhase = true;

    /**
     * Cache view of the state of the partition. Is set dirty when the incomplete state of any offset changes. Is set
     * clean after a successful commit of the state.
     * <p>
     * {@code volatile} because it crosses threads with no other fence: written on the control thread
     * ({@code onSuccess} via the mailbox), read on the broker-poll thread by the commit path's
     * dirty-partition collection in the default {@code PERIODIC_CONSUMER_ASYNCHRONOUS} mode. As a plain
     * field, jcstress measured the reader observing {@code dirty} set while {@code offsetHighestSucceeded}
     * was still stale at ~1.4e-7 per sample even with the real surrounding {@code ConcurrentSkipListMap}
     * accesses on both sides - a burnt commit cycle, which on a partition that then goes idle holds the
     * committed offset back until the next rebalance. With only this flag volatile the anomaly was 0 in
     * 4.29e9 samples with the outcome declared FORBIDDEN: the release store on the write publishes the
     * preceding plain writes, and the acquire load on the read observes them. The {@code long}s stay
     * plain deliberately - fencing them too buys nothing the flag does not already provide, at extra
     * cost on every read. Evidence, re-runnable: the {@code jcstress-poc/} module
     * (astubbs/parallel-consumer#348), whose {@code CommitPathVisibilityProbes} models this exact pair -
     * the arm carrying that FORBIDDEN outcome is
     * {@code CommitPathVisibilityProbes.VolatileDirtyPublishesPlainSucceeded}. What a probe's zero and its
     * rate are each worth, with these figures in its results table:
     * docs/solutions/best-practices/a-stress-probe-is-an-instrument-you-built-not-a-test.md.
     */
    @Setter(PRIVATE)
    @Getter(PACKAGE)
    private volatile boolean dirty;

    /**
     * The highest seen offset for a partition.
     * <p>
     * Starts off as -1 - no data. Offsets in Kafka are never negative, so this is fine.
     */
    // visible for testing
    @Getter(PUBLIC)
    private long offsetHighestSeen;

    /**
     * Highest offset which has completed successfully ("succeeded").
     * <p>
     * Note that this may in some conditions, there may be a gap between this and the next offset to poll - that being,
     * there may be some number of transaction marker records above it, and the next offset to poll.
     * <p>
     * Note that as we only encode our offset map up to the highest succeeded offset (as encoding higher has no value),
     * upon bootstrap, this will always start off as the same as the {@link #offsetHighestSeen}.
     */
    @Getter(PUBLIC)
    private long offsetHighestSucceeded = KAFKA_OFFSET_ABSENCE;

    /**
     * If true, more messages are allowed to process for this partition.
     * <p>
     * If false, we have calculated that we can't record any more offsets for this partition, as our best performing
     * encoder requires nearly as much space is available for this partitions allocation of the maximum offset metadata
     * size.
     * <p>
     * Default (missing elements) is true - more messages can be processed.
     * <p>
     * AKA high watermark (which is a deprecated description).
     *
     * @see OffsetMapCodecManager#DefaultMaxMetadataSize
     */
    @Getter(PACKAGE)
    @Setter(PRIVATE)
    private boolean allowedMoreRecords = true;

    /**
     * The Epoch of the generation of partition assignment, for fencing off invalid work.
     * <p>
     * Will unified actor partition assignment messages, epochs may no longer be needed.
     */
    @Getter
    private final long partitionsAssignmentEpoch;

    private long lastCommittedOffset;
    private Gauge lastCommittedOffsetGauge;
    private Gauge highestSeenOffsetGauge;
    private Gauge highestCompletedOffsetGauge;
    private Gauge highestSequentialSucceededOffsetGauge;
    private Gauge numberOfIncompletesGauge;
    private Gauge ephochGauge;
    private DistributionSummary ratioPayloadUsedDistributionSummary;
    private DistributionSummary ratioMetadataSpaceUsedDistributionSummary;
    /**
     * The rider's four series (KTD14/R20). The rider is opaque to PC, so PC cannot tell an embedder whether its
     * feature works - only whether the bytes it was handed reached the wire. These say when they did not.
     */
    private DistributionSummary riderSizeDistributionSummary;
    private Counter riderDroppedCounter;
    private Counter payloadStrippedCounter;
    private Counter riderSupplierFailedCounter;
    private final PCMetrics pcMetrics;
    private final OffsetMapCodecManager<K, V> om;

    /**
     * Additional flag to prevent overwriting dirty state that was updated during commit execution window - so that any
     * subsequent offsets completed while commit is being performed could mark state as dirty and retain the dirty state
     * on commit completion. In tight race condition - it may be set just before offset is completed and included in
     * commit data collection - so it is a little bit pessimistic - that may cause an additional unnecessary commit on
     * next commit cycle - but it is highly unlikely as throughput has to be high for this to occur - but with high
     * throughput there will be other offsets ready to commit anyway.
     */
    private boolean stateChangedSinceCommitStart = false;


    public PartitionState(long newEpoch,
                          PCModule<K, V> pcModule,
                          TopicPartition topicPartition,
                          OffsetMapCodecManager.HighestOffsetAndIncompletes offsetData) {
        this.module = pcModule;

        this.tp = topicPartition;
        this.partitionsAssignmentEpoch = newEpoch;
        this.pcMetrics = module.pcMetrics();
        initStateFromOffsetData(offsetData);
        initMetrics();
        this.om = new OffsetMapCodecManager<>(pcModule);
    }

    private void initStateFromOffsetData(OffsetMapCodecManager.HighestOffsetAndIncompletes offsetData) {
        this.offsetHighestSeen = offsetData.getHighestSeenOffset().orElse(KAFKA_OFFSET_ABSENCE);

        this.incompleteOffsets = new ConcurrentSkipListMap<>();
        offsetData.getIncompleteOffsets()
                .forEach(offset -> incompleteOffsets.put(offset, Optional.empty()));

        this.offsetHighestSucceeded = this.offsetHighestSeen; // by definition, as we only encode up to the highest seen offset (inclusive)
    }

    private void maybeRaiseHighestSeenOffset(final long offset) {
        // rise the highest seen offset
        if (offset >= offsetHighestSeen) {
            log.trace("Updating highest seen - was: {} now: {}", offsetHighestSeen, offset);
            offsetHighestSeen = offset;
        }
    }

    public void onOffsetCommitSuccess(OffsetAndMetadata committed) { //NOSONAR
        lastCommittedOffset = committed.offset();
        setClean();
    }

    private void setClean() {
        if (!stateChangedSinceCommitStart) {
            setDirty(false);
        }
    }

    private void setDirty() {
        stateChangedSinceCommitStart = true;
        setDirty(true);
    }

    // todo rename isRecordComplete()
    // todo add support for this to TruthGen
    public boolean isRecordPreviouslyCompleted(final ConsumerRecord<K, V> rec) {
        long recOffset = rec.offset();
        if (incompleteOffsets.containsKey(recOffset)) {
            // we haven't recorded this far up, so must not have been processed yet
            return false;
        } else {
            // if within the range of tracked offsets, must have been previously completed, as it's not in the incomplete set
            return recOffset <= offsetHighestSucceeded;
        }
    }

    public boolean hasIncompleteOffsets() {
        return !incompleteOffsets.isEmpty();
    }

    public int getNumberOfIncompleteOffsets() {
        return incompleteOffsets.size();
    }

    public void onSuccess(long offset) {
        //noinspection OptionalAssignedToNull - null check to see if key existed
        boolean removedFromIncompletes = this.incompleteOffsets.remove(offset) != null; // NOSONAR
        assert (removedFromIncompletes);

        updateHighestSucceededOffsetSoFar(offset);

        setDirty();
    }

    public void onFailure(WorkContainer<K, V> work) {
        // no-op
    }

    /**
     * Update highest Succeeded seen so far
     */
    private void updateHighestSucceededOffsetSoFar(long thisOffset) {
        long highestSucceeded = getOffsetHighestSucceeded();
        if (thisOffset > highestSucceeded) {
            log.trace("Updating highest completed - was: {} now: {}", highestSucceeded, thisOffset);
            this.offsetHighestSucceeded = thisOffset;
        }
    }

    /**
     * First of THREE staleness checkpoints. Read this before concluding any one of them is broken -
     * each is deliberately partial, and they are only sound together.
     *
     * <ol>
     *   <li><b>Here, on register</b> - proactive and best-effort. Checked ONCE per poll batch, which is
     *       correct rather than a shortcut: {@link EpochAndRecordsMap.RecordsAndEpoch} carries a single
     *       {@code epochOfPartitionAtPoll} for its whole record list, so a per-record check would return
     *       the identical answer.</li>
     *   <li><b>On take</b> - {@link #couldBeTakenAsWork}, per container against LIVE state. This is the
     *       authoritative one: nothing stale is ever executed, however it got into a shard.</li>
     *   <li><b>On completion</b> - {@code WorkManager.handleFutureResult}, per container against state
     *       resolved ONCE: the staleness answer and the acting reads share a single lookup, and the
     *       success action mutates exactly the state object that was validated (two lookups were a torn
     *       read - a rebalance in the gap meant validating the old state and acting on its replacement).
     *       The failure path additionally re-validates against the live map immediately before its retry
     *       re-queue, whose target structures the revoke sweep cleans. A result returning from work that
     *       went stale mid-flight is dropped.</li>
     * </ol>
     *
     * <b>This checkpoint is knowingly racy, and that is not a defect.</b> The caller
     * ({@code PartitionStateManager.maybeRegisterNewRecordAsWork}) looks this state up live and calls
     * straight in, but a rebalance on the broker-poll thread can land between that lookup and the
     * inserts this method then performs - registration runs on the control thread and nothing
     * serialises the two. Inside that window the guard can pass wrongly, either because the epoch
     * bumped after a correct check, or because this state object itself went stale (its
     * {@code partitionsAssignmentEpoch} is a {@code final long} captured at construction, so a stale
     * state compares its own old epoch against the batch's old epoch and they match). Either way
     * old-epoch containers reach live shards. Checkpoint 2 is what makes that safe.
     *
     * <b>Do not "fix" this by re-checking per record, or by consulting the live epoch here.</b>
     * Neither closes the window - both are still check-then-act against a concurrent rebalance - and
     * the lock that would close it was deliberately removed in {@code 9a966860b} (confluentinc#219),
     * on the grounds that epoch tracking replaced it. That reasoning holds for the scheme as a whole,
     * not for this checkpoint alone.
     *
     * <b>What was genuinely broken</b> was never the admission of stale containers but what the shard
     * did on a collision: it preferred a stale RESIDENT over a fresh ARRIVAL at the same offset and
     * dropped the arrival, which is lost for good since checkpoint 2 only removes the resident.
     * See {@code ProcessingShard.addWorkContainer} and confluentinc#909.
     *
     * @see #couldBeTakenAsWork
     */
    private boolean epochIsStale(EpochAndRecordsMap<K, V>.RecordsAndEpoch recordsAndEpoch) {
        // do epochs still match? do a proactive check, but the epoch will be checked again at work completion as well
        var currentPartitionEpoch = getPartitionsAssignmentEpoch();
        Long epochOfInboundRecords = recordsAndEpoch.getEpochOfPartitionAtPoll();

        return !Objects.equals(epochOfInboundRecords, currentPartitionEpoch);
    }

    public void maybeRegisterNewPollBatchAsWork(@NonNull EpochAndRecordsMap<K, V>.RecordsAndEpoch recordsAndEpoch) {
        if (epochIsStale(recordsAndEpoch)) {
            // Expected during any rebalance: a rebalance between poll() and registration means these
            // records belong to an assignment we no longer hold, and their new owner will receive
            // them. This is the epoch fencing working as designed, so it stays at debug.
            //
            // It was briefly a WARN calling itself "the primary suspect for the silent stall". The
            // same investigation disproved that (see "Verified: epoch mismatch is NOT the cause" in
            // docs/BUG_857_INVESTIGATION.md (deleted 2026-08-18; retrieve with `git show 262629aab:docs/BUG_857_INVESTIGATION.md`)), and the 2026-08-18 A/B soak established the commit-path
            // deadlock as the cause instead. A WARN on every rebalance buries the lines worth acting on.
            log.debug("Inbound record of work has epoch ({}) not matching currently assigned epoch for the applicable partition ({}), skipping",
                    recordsAndEpoch.getEpochOfPartitionAtPoll(), getPartitionsAssignmentEpoch());
            return;
        }

        //
        maybeTruncateOrPruneTrackedOffsets(recordsAndEpoch);

        //
        long epochOfInboundRecords = recordsAndEpoch.getEpochOfPartitionAtPoll();
        List<ConsumerRecord<K, V>> recordPollBatch = recordsAndEpoch.getRecords();
        for (var aRecord : recordPollBatch) {
            if (isRecordPreviouslyCompleted(aRecord)) {
                log.trace("Record previously completed, skipping. offset: {}", aRecord.offset());
            } else {
                getShardManager().addWorkContainer(epochOfInboundRecords, aRecord);
                addNewIncompleteRecord(aRecord);
            }
        }

    }

    /**
     * Used for adding work to, if it's been successfully added to our tracked state
     *
     * @see #maybeRegisterNewPollBatchAsWork
     */
    private ShardManager<K, V> getShardManager() {
        return module.workManager().getSm();
    }

    public boolean isPartitionRemovedOrNeverAssigned() {
        return false;
    }

    // visible for legacy testing
    public void addNewIncompleteRecord(ConsumerRecord<K, V> record) {
        long offset = record.offset();
        maybeRaiseHighestSeenOffset(offset);

        // idempotently add the offset to our incompletes track - if it was already there from loading our metadata on startup, there is no affect
        incompleteOffsets.put(offset, Optional.of(record));
    }


    /**
     * If the offset is higher than expected, according to the previously committed / polled offset, truncate up to it.
     * If lower, reset down to it.
     * <p>
     * Only runs if this is the first {@link ConsumerRecord} to be added since instantiation.
     * <p>
     * Can be caused by the offset reset policy of the underlying consumer.
     */
    private void maybeTruncateBelowOrAbove(long bootstrapPolledOffset) {
        if (bootstrapPhase) {
            bootstrapPhase = false;
        } else {
            // Not bootstrap phase anymore, so not checking for truncation
            return;
        }

        // during bootstrap, getOffsetToCommit() will return the offset of the last record committed, so we can use that to determine if we need to truncate
        long expectedBootstrapRecordOffset = getOffsetToCommit();

        boolean pollAboveExpected = bootstrapPolledOffset > expectedBootstrapRecordOffset;

        boolean pollBelowExpected = bootstrapPolledOffset < expectedBootstrapRecordOffset;

        if (pollAboveExpected) {
            // previously committed offset record has been removed from the topic, so we need to truncate up to it
            log.warn("Truncating state - removing records lower than {} from partition {} of topic {}. Offsets have been removed from the partition " +
                            "by the broker or committed offset has been raised. Bootstrap polled {} but expected {} from loaded commit data. " +
                            "Could be caused by record retention or compaction and offset reset policy LATEST.",
                    bootstrapPolledOffset,
                    this.tp.partition(),
                    this.tp.topic(),
                    bootstrapPolledOffset,
                    expectedBootstrapRecordOffset);

            // truncate
            final NavigableSet<Long> incompletesToPrune = incompleteOffsets.keySet().headSet(bootstrapPolledOffset, false);
            incompletesToPrune.forEach(incompleteOffsets::remove);
        } else if (pollBelowExpected) {
            // reset to lower offset detected, so we need to reset our state to match
            log.warn("Bootstrap polled offset has been reset to an earlier offset ({}) for partition {} of topic {} - truncating state - all records " +
                            "above (including this) will be replayed. Was expecting {} but bootstrap poll was {}. " +
                            "Could be caused by record retention or compaction and offset reset policy EARLIEST.",
                    bootstrapPolledOffset,
                    this.tp.partition(),
                    this.tp.topic(),
                    expectedBootstrapRecordOffset,
                    bootstrapPolledOffset
            );

            // reset
            var offsetData = OffsetMapCodecManager.HighestOffsetAndIncompletes.of();
            initStateFromOffsetData(offsetData);
        }
    }

    /**
     * Has this partition been removed? No.
     *
     * @return by definition false in this implementation
     */
    public boolean isRemoved() {
        return false;
    }

    public Optional<OffsetAndMetadata> getCommitDataIfDirty() {
        if (isDirty()) {
            // setting the flag so that any subsequent offset completed while commit is being performed could mark state as dirty
            // and retain the dirty state on commit completion.
            stateChangedSinceCommitStart = false;
            return of(createOffsetAndMetadata());
        }
        return empty();
    }

    // visible for testing
    protected OffsetAndMetadata createOffsetAndMetadata() {
        // use tuple to make sure getOffsetToCommit is invoked only once to avoid dirty read
        // and commit the wrong offset
        ParallelConsumer.Tuple<Optional<String>, Long> tuple = tryToEncodeOffsets();
        Optional<String> payloadOpt = tuple.getLeft();
        long nextOffset = tuple.getRight();
        return payloadOpt
                .map(encodedOffsets -> new OffsetAndMetadata(nextOffset, encodedOffsets))
                .orElseGet(() -> new OffsetAndMetadata(nextOffset));
    }

    /**
     * Next offset expected to be polled, upon freshly connecting to a broker.
     * <p>
     * Defined as the offset one ABOVE the highest sequentially succeeded offset - Kafka commits the next
     * offset to read, not the last one processed.
     */
    // visible for testing
    protected long getOffsetToCommit() {
        return getOffsetHighestSequentialSucceeded() + 1;
    }

    /**
     * @return all incomplete offsets of buffered work in this shard, even if higher than the highest succeeded
     */
    public List<Long> getAllIncompleteOffsets() {
        //noinspection FuseStreamOperations - only in java 10
        return Collections.unmodifiableList(incompleteOffsets.keySet().parallelStream().collect(Collectors.toList()));
    }

    /**
     * @return incomplete offsets which are lower than the highest succeeded
     */
    public SortedSet<Long> getIncompleteOffsetsBelowHighestSucceeded() {
        return getIncompleteOffsetsBelow(getOffsetHighestSucceeded());
    }

    /**
     * The bounded filter behind {@link #getIncompleteOffsetsBelowHighestSucceeded()}, taking the bound as a parameter
     * so a caller that also needs the bound itself can sample it <em>once</em> and use the same value for both -
     * {@code OffsetMapCodecManager#encodeOffsetsCompressed} must, because its encoder marks every offset in
     * {@code [base, bound]} that is absent from this set as completed, so deriving set and bound from two separate
     * reads of the moving {@code offsetHighestSucceeded} let a concurrent completion above the mark widen the range
     * around a stale set (see {@code OffsetEncoderWidenedRangeRaceTest}).
     *
     * @param highestSucceededBound the exclusive upper bound - a caller's single sample of
     *                              {@link #getOffsetHighestSucceeded()}
     * @return incomplete offsets which are lower than the given bound
     */
    public SortedSet<Long> getIncompleteOffsetsBelow(long highestSucceededBound) {
        return incompleteOffsets.keySet().parallelStream()
                .filter(x -> x < highestSucceededBound)
                .collect(toTreeSet());
    }

    /**
     * The offset which is itself, and all before, all successfully completed (or skipped).
     * <p>
     * Defined for our purpose (as only used in definition of what offset to poll for next), as the offset one below the
     * lowest incomplete offset.
     */
    public long getOffsetHighestSequentialSucceeded() {
        /*
         * Capture the current value in case it's changed during this operation - because if more records are added to
         * the queue, after looking at the incompleteOffsets, offsetHighestSeen could increase drastically and will be
         * incorrect for the value of getOffsetHighestSequentialSucceeded. So this is a ~pessimistic solution - as in a
         * race case, there may be a higher getOffsetHighestSequentialSucceeded from the incompleteOffsets collection,
         * but it will always at lease be pessimistically correct in terms of committing offsets to the broker.
         *
         * See confluentinc#200 for the complete correct solution.
         */
        // use offsetHighestSucceeded instead of offsetHighestSeen to fix confluentinc issue #826
        long currentOffsetHighestSeen = offsetHighestSucceeded;
        Long firstIncompleteOffset = incompleteOffsets.keySet().ceiling(KAFKA_OFFSET_ABSENCE);
        boolean incompleteOffsetsWasEmpty = firstIncompleteOffset == null;

        if (incompleteOffsetsWasEmpty) {
            return currentOffsetHighestSeen;
        } else {
            return firstIncompleteOffset - 1;
        }
    }


    /**
     * Tries to encode the incomplete offsets for this partition. This may not be possible if there are none, or if no
     * encodings are possible ({@link NoEncodingPossibleException}. Encoding may not be possible of - see
     * {@link OffsetMapCodecManager#makeOffsetMetadataPayload}.
     * <p>
     * <b>This method is the whole commit snapshot, and everything the payload says is sampled inside it</b>: the
     * offset, the offset map, and - when one is configured - the embedder's rider (see
     * {@link #riderFromSupplier}). Reading any of them again elsewhere is the confluentinc#893 defect class, so
     * the write side is two steps rather than one: encode the offset map once, then assemble the string around
     * whatever the rider slot turned out to hold.
     *
     * @return the encoded offset map if one was possible, paired with the offset it was encoded
     *         against. The two travel together deliberately: committing the payload against a
     *         later offset than the one it describes is the confluentinc#893 defect, so the caller
     *         must never re-derive the offset.
     */
    private ParallelConsumer.Tuple<Optional<String>, Long> tryToEncodeOffsets() {
        long offsetOfNextExpectedMessage = getOffsetToCommit();

        if (incompleteOffsets.isEmpty()) {
            // KTD6: this early return is the ONLY place a partition blocked by back pressure unblocks once it has
            // caught up, so a rider must ride on it rather than replace it - otherwise configuring a rider could
            // leave a partition with no work left to complete permanently blocked.
            setAllowedMoreRecords(true);
            var caughtUpRider = riderFromSupplier(offsetOfNextExpectedMessage, NO_INNER_BYTES.length);
            if (caughtUpRider.getState() == OffsetRiderEnvelope.RiderState.NONE) {
                return ParallelConsumer.Tuple.pairOf(empty(), offsetOfNextExpectedMessage);
            }
            // KTD14: neither ratio takes a sample here, deliberately. There is no offset map encoding to report a
            // density for, and the offset range is zero or negative on this path - Micrometer records -0.0 and
            // 0.0 as samples, and a positive numerator over a zero range is Infinity, so a sample would drag
            // both distributions off their meaning on every commit of a healthy consumer. The rider itself is
            // still measured: a caught-up commit is the one a restart reads back.
            recordRiderSizeIfWritten(caughtUpRider);
            return ParallelConsumer.Tuple.pairOf(of(om.assembleMetadataPayload(NO_INNER_BYTES, caughtUpRider)),
                    offsetOfNextExpectedMessage);
        }

        try {
            // todo refactor use of null shouldn't be needed. Is OffsetMapCodecManager stateful? remove null - confluentinc#233
            var offsetRange = getOffsetHighestSucceeded() - offsetOfNextExpectedMessage;
            // KTD9: encode the offset map ONCE, then ask for the rider, then assemble. A second encode pass here would
            // snapshot a later offset map (the confluentinc#894 tear class) and double-count the encoding meters.
            byte[] innerBytes = om.encodeOffsetsToInnerBytes(offsetOfNextExpectedMessage, this);
            var offered = riderFromSupplier(offsetOfNextExpectedMessage, innerBytes.length);
            // KTD4/R9: the ladder picks its rung by PREDICTED length and only then assembles, so the outer codec
            // runs once on the winner rather than once per rung.
            var rider = fitRiderToBudget(offered, innerBytes.length);
            String offsetMapPayload = om.assembleMetadataPayload(innerBytes, rider);
            // KTD4: two lengths, both in encoded characters. With no envelope the assembled string IS the inner
            // encoding's string, so its own length is today's number byte for byte; with one, the inner length is
            // derived from the byte count by Base64's closed form rather than by a second encode.
            int innerEncodingCharacterLength = rider.getState() == OffsetRiderEnvelope.RiderState.NONE
                    ? offsetMapPayload.length()
                    : RiderBudgetRung.base64Characters(innerBytes.length);
            recordEncodingRatios(innerEncodingCharacterLength, offsetMapPayload.length(), offsetRange);
            recordRiderSizeIfWritten(rider);
            boolean mustStrip = updateBlockFromEncodingResult(innerEncodingCharacterLength, offsetMapPayload.length());
            if (mustStrip) {
                return ParallelConsumer.Tuple.pairOf(empty(), offsetOfNextExpectedMessage);
            } else {
                return ParallelConsumer.Tuple.pairOf(of(offsetMapPayload), offsetOfNextExpectedMessage);
            }
        } catch (NoEncodingPossibleException e) {
            setAllowedMoreRecords(false);
            // KTD14: the stripped-payload counter only. This escapes the inner-bytes step, which under KTD9 runs
            // BEFORE the supplier is called, so on this path there is no rider to have discarded and nothing to
            // count as dropped - the ladder's own strip rung is where both are counted.
            payloadStrippedCounter.increment();
            log.warn("No encodings could be used to encode the offset map, skipping. Warning: messages might be replayed on rebalance.", e);
            return ParallelConsumer.Tuple.pairOf(empty(), offsetOfNextExpectedMessage);
        }
    }

    /**
     * The two ratios, which measure <b>different lengths</b> and answer different questions (KTD14).
     * <p>
     * {@link PCMetricsDef#PAYLOAD_RATIO_USED} is <em>density</em>: how many encoded characters the offset map
     * spends per offset it describes, so it records the encoded offset map's own length and is unmoved by a rider.
     * {@link PCMetricsDef#METADATA_SPACE_USED} is <em>headroom</em>: how close this commit came to the broker's
     * metadata limit, so it records the string that actually goes to the broker, rider included - which is what
     * keeps its description true now that a payload can carry more than the offset map.
     * <p>
     * <b>Neither divisor may be zero or negative.</b> Micrometer's own sign check drops {@code NaN} but records
     * {@code -0.0} and {@code 0.0} as samples, and a positive numerator over a zero divisor is {@code Infinity},
     * which poisons a distribution's total for the life of the process. The caught-up path never reaches here at
     * all (its range is zero or negative by definition); the guard is for the remaining shapes - a partition
     * whose highest succeeded offset has not yet passed the offset being committed, and a metadata limit
     * configured to zero.
     *
     * @param innerEncodingCharacterLength the encoded offset map's own length, in characters
     * @param assembledPayloadLength       the length of the string that will actually be committed
     * @param offsetRange                  how many offsets the offset map describes
     */
    private void recordEncodingRatios(int innerEncodingCharacterLength, int assembledPayloadLength, long offsetRange) {
        if (offsetRange > 0) {
            ratioPayloadUsedDistributionSummary.record(innerEncodingCharacterLength / (double) offsetRange);
        }
        if (DefaultMaxMetadataSize > 0) {
            ratioMetadataSpaceUsedDistributionSummary.record(assembledPayloadLength / (double) DefaultMaxMetadataSize);
        }
    }

    /**
     * The size of the rider this commit is about to write, in the bytes the embedder handed over rather than the
     * characters they cost - bytes are the unit {@link RiderContext#getMaxRiderBytes()} gives the supplier its
     * budget in, so a distribution in any other unit could not be read against it.
     * <p>
     * Only a {@link OffsetRiderEnvelope.RiderState#PRESENT} rider is a sample: the drop marker carries no bytes,
     * and a rider shed anywhere above this never reached the wire, which is what the dropped counter is for.
     */
    private void recordRiderSizeIfWritten(OffsetRiderEnvelope.Rider rider) {
        if (rider.getState() == OffsetRiderEnvelope.RiderState.PRESENT) {
            riderSizeDistributionSummary.record(rider.getByteLength());
        }
    }

    /**
     * The two size checks a commit makes, and they are deliberately made against <b>different lengths</b> (KTD4,
     * R7).
     * <p>
     * <b>Back pressure measures the offset map alone</b>, because back pressure exists so that a payload can
     * <em>shrink</em> as work completes. Rider bytes do not shrink - the embedder hands over whatever it hands
     * over, whatever the offset map is doing - so charging them here would make a rider a floor back pressure can
     * never relieve, and on a caught-up partition a permanent block.
     * <p>
     * <b>The hard limit measures the whole assembled string</b>, rider included, because that is what actually
     * goes to the broker.
     * <p>
     * With no rider configured the two numbers are the same string's length, so this is byte for byte the check
     * this build has always made, and the point at which back pressure engages does not move.
     *
     * @param innerEncodingCharacterLength the offset map's own encoded length, in characters
     * @param assembledPayloadLength       the length of the string that will actually be committed
     * @return true if the payload is too large and must be stripped
     */
    private boolean updateBlockFromEncodingResult(int innerEncodingCharacterLength, int assembledPayloadLength) {
        boolean mustStrip = false;

        if (assembledPayloadLength > DefaultMaxMetadataSize) {
            // exceeded maximum API allowed, strip the payload
            mustStrip = stripPayloadForSize(assembledPayloadLength);
        } else if (innerEncodingCharacterLength > getPressureThresholdValue()) { // payload within the hard limit
            // try to turn on back pressure before max size is reached
            setAllowedMoreRecords(false);
            log.warn("Offset map size {} higher than threshold {}, but the payload of {} is still lower than max {}. " +
                            "Will write payload, but will " +
                            "not allow further messages, in order to allow the offset data to shrink (via succeeding messages).",
                    innerEncodingCharacterLength, getPressureThresholdValue(), assembledPayloadLength,
                    DefaultMaxMetadataSize);

        } else { // and thus (innerEncodingCharacterLength <= pressureThresholdValue)
            if (allowedMoreRecords == false) {
                // guard is useful for debugging to catch the transition from false to true
                setAllowedMoreRecords(true);
            }
            log.debug("Offset map size {} within threshold {}", innerEncodingCharacterLength,
                    getPressureThresholdValue());
        }

        return mustStrip;
    }

    /**
     * The bottom rung of the budget ladder, and the only one that predates the rider: not even the bare offset map
     * fits the metadata field, so the commit carries a bare offset and the partition is blocked.
     * <p>
     * A single method so that it is one place, not three: the ladder above it has already shed the rider and the
     * envelope by the time this is reached, so what is stripped here is the offset map itself.
     *
     * @return always true - the caller's {@code mustStrip}, named rather than assumed
     */
    private boolean stripPayloadForSize(int assembledPayloadLength) {
        setAllowedMoreRecords(false);
        // the only strip site on the ladder path, so this is the whole of what the counter means: a commit that
        // wrote no payload at all, which is the half of
        // docs/inflight/bug-no-metric-for-discarded-offset-metadata.md that a write-side counter can close
        payloadStrippedCounter.increment();
        log.warn("Offset map data too large (size: {}) to fit in metadata payload hard limit of {} - cannot " +
                        "include in commit. Warning: messages might be replayed on rebalance. " +
                        "See kafka.coordinator.group.OffsetConfig#DefaultMaxMetadataSize = {} and confluentinc#47.",
                assembledPayloadLength, DefaultMaxMetadataSize, DefaultMaxMetadataSize);
        return true;
    }

    private double getPressureThresholdValue() {
        return DefaultMaxMetadataSize * PartitionStateManager.getUSED_PAYLOAD_THRESHOLD_MULTIPLIER();
    }

    /**
     * What a caught-up partition has to encode: nothing. The rider, if there is one, rides alone.
     */
    private static final byte[] NO_INNER_BYTES = new byte[0];

    /**
     * Asks the embedder's {@link ParallelConsumerOptions#getRiderSupplier() riderSupplier} for this commit's
     * rider, and turns every way it can be unhelpful into a rider slot the rest of the write side can trust.
     * <p>
     * <b>This is user code on an engine thread</b> - the broker-poll thread under the consumer commit modes, the
     * control thread under the produce write lock under transactions - so nothing it does may escape. A throw is
     * caught (including an {@link Error}: a supplier that throws {@code NoClassDefFoundError} from a
     * half-deployed embedder must still cost only the rider), logged through a rate limiter and treated as no
     * rider. The shape and the reasoning are {@code WorkContainer#getRetryDelayConfig}'s, and the failure it
     * guards against is not hypothetical - a throwing meter registry took the poll thread down and stranded
     * {@code close()}:
     * {@code docs/solutions/runtime-errors/a-throwing-meter-registry-kills-the-poll-thread-and-strands-close.md}.
     * <p>
     * <b>KTD3 - normalisation happens here and nowhere else.</b> {@code null} and a zero-length array both become
     * {@link OffsetRiderEnvelope.Rider#none()} before anything below sees them, so the only writer of a
     * zero-length envelope is the drop below and an embedder cannot forge that marker.
     *
     * @param offsetToCommit              the offset this rider will be committed against - sampled once, by the
     *                                    caller, in the same snapshot as everything else about this commit (R11)
     * @param innerEncodingByteLength     how many bytes the encoded offset map took, or zero when the partition is
     *                                    caught up and there is no map to write
     * @return never {@code null}; {@code NONE} when there is no rider to carry
     */
    private OffsetRiderEnvelope.Rider riderFromSupplier(long offsetToCommit, int innerEncodingByteLength) {
        var supplier = module.options().getRiderSupplier();
        if (supplier == null) {
            return OffsetRiderEnvelope.Rider.none();
        }

        int allowance = maxRiderBytes(innerEncodingByteLength,
                DefaultMaxMetadataSize,
                PartitionStateManager.getUSED_PAYLOAD_THRESHOLD_MULTIPLIER());

        byte[] theirs;
        try {
            theirs = supplier.apply(new RiderContext(tp, offsetToCommit, allowance));
        } catch (Throwable theirSupplierThrew) {
            warnBrokenRiderSupplier(theirSupplierThrew);
            // KTD8 makes this silent by design - the commit proceeds - so the counter is the ONLY continuous
            // signal that a rider-based feature has stopped working. The warning beside it is rate limited and
            // may be half a minute away.
            riderSupplierFailedCounter.increment();
            return OffsetRiderEnvelope.Rider.none();
        }

        if (theirs == null || theirs.length == 0) {
            return OffsetRiderEnvelope.Rider.none();
        }

        if (theirs.length > allowance) {
            warnOversizedRider(theirs.length, allowance, innerEncodingByteLength > 0);
            // a dropped rider like any other from an operator's point of view - the embedder's bytes did not
            // reach the wire. Counted here rather than below the ladder because a rider refused at the write side
            // never descends it; both spellings of the same loss belong in one series.
            riderDroppedCounter.increment();
            // KTD4: a caught-up partition whose rider will not fit writes no metadata at all, rather than an
            // envelope whose only content is the marker saying it is empty. With an offset map to sit beside, the
            // marker is worth its three bytes - it is how a reader tells a rider that was shed from one that was
            // never configured (R6).
            return innerEncodingByteLength > 0
                    ? OffsetRiderEnvelope.Rider.dropped()
                    : OffsetRiderEnvelope.Rider.none();
        }

        return OffsetRiderEnvelope.Rider.present(theirs);
    }

    /**
     * The most rider bytes that may be carried alongside an offset map of {@code innerEncodingByteLength} bytes,
     * per KTD4 of the rider plan. Pure, so it can be asserted directly.
     * <p>
     * Two independent limits, and the answer is the smaller:
     * <ol>
     *     <li><b>The rider cap</b> - {@code maxMetadataSizeInCharacters * (1 - multiplier)} encoded characters,
     *     the slice of the metadata field that back pressure deliberately never uses. Independent of the offset
     *     map, and it is what buys the property an embedder depends on: a rider at its cap can only push the
     *     assembled payload over the hard limit once the offset map has already crossed the back-pressure
     *     threshold, so configuring a rider cannot cost a partition metadata it would otherwise have
     *     committed.</li>
     *     <li><b>What is actually left</b> in this commit once the offset map and the envelope's own header are
     *     accounted for. Without this a small cap plus a large map would promise room that does not exist.</li>
     * </ol>
     * Both start life in <em>encoded characters</em>, because that is the unit the broker's limit is in and the
     * unit the existing checks use, and are converted to raw bytes by inverting Base64's closed form: {@code n}
     * bytes encode to {@code 4*ceil(n/3)} characters, so the largest {@code n} fitting in {@code c} characters is
     * {@code 3*floor(c/4)}. Base64 is the more expansive of the outer codecs in play, so a rider that fits under
     * it fits under the alternatives too.
     *
     * @param innerEncodingByteLength         bytes of encoded offset map this rider shares the payload with; zero
     *                                        for a caught-up partition
     * @param maxMetadataSizeInCharacters     the hard metadata limit, in characters
     *                                        ({@link OffsetMapCodecManager#DefaultMaxMetadataSize})
     * @param usedPayloadThresholdMultiplier  the back-pressure threshold as a fraction of that limit
     *                                        ({@link PartitionStateManager#getUSED_PAYLOAD_THRESHOLD_MULTIPLIER()})
     * @return zero or more bytes - zero meaning there is no room for a rider on this commit at all, which is what
     *         a multiplier at or above 1 produces for every limit
     */
    // visible for testing - the budget ladder asserts these values directly rather than through a commit
    static int maxRiderBytes(int innerEncodingByteLength,
                             int maxMetadataSizeInCharacters,
                             double usedPayloadThresholdMultiplier) {
        int riderCapInCharacters = (int) Math.floor(maxMetadataSizeInCharacters * (1 - usedPayloadThresholdMultiplier));
        int riderCap = base64CapacityInBytes(riderCapInCharacters);

        int remaining = base64CapacityInBytes(maxMetadataSizeInCharacters)
                - OffsetRiderEnvelope.HEADER_BYTES
                - innerEncodingByteLength;

        int allowed = Math.min(riderCap, remaining);
        // the format's own ceiling: the length field is 16 bits, so no derived cap may promise more than it can
        // describe, however large the metadata limit is set
        return Math.max(0, Math.min(allowed, OffsetRiderEnvelope.MAX_RIDER_BYTES));
    }

    /**
     * The largest number of raw bytes whose Base64 encoding fits in {@code characters} characters - the inverse of
     * {@code 4*ceil(n/3)}. Negative inputs (a threshold multiplier above 1) answer zero rather than a negative
     * capacity.
     */
    private static int base64CapacityInBytes(int characters) {
        return characters < 4 ? 0 : (characters / 4) * 3;
    }

    /**
     * Walks the ladder for this commit and returns the rider slot the payload will actually carry.
     * <p>
     * Nothing is encoded here: the rung is chosen from predicted lengths and the caller assembles once, on the
     * winner. A rider already over its own derived cap was turned into the marker by {@link #riderFromSupplier}
     * before this point, so what descends here is a rider that fits its cap but not this particular payload.
     */
    private OffsetRiderEnvelope.Rider fitRiderToBudget(OffsetRiderEnvelope.Rider offered, int innerEncodingByteLength) {
        var state = offered.getState();
        if (state == OffsetRiderEnvelope.RiderState.NONE) {
            // no envelope to shed - today's payload, and today's strip below it if even that does not fit
            return offered;
        }

        int riderByteLength = offered.getByteLength();
        var rung = RiderBudgetRung.choose(state, riderByteLength, innerEncodingByteLength, DefaultMaxMetadataSize);
        switch (rung) {
            case RIDER:
                return offered;
            case MARKER:
                return state == OffsetRiderEnvelope.RiderState.PRESENT
                        ? shedRiderForSize(riderByteLength, innerEncodingByteLength)
                        : offered; // already the marker, and the guard has already warned about it
            default:
                return shedEnvelopeForSize(state, innerEncodingByteLength);
        }
    }

    /**
     * The ladder's first descent: the rider does not fit beside this offset map, so the envelope carries the
     * zero-length marker instead. The offset map is untouched - that is R9.
     */
    private OffsetRiderEnvelope.Rider shedRiderForSize(int riderByteLength, int innerEncodingByteLength) {
        // one increment per commit: the ladder chooses a rung by predicted length and jumps straight to it, so a
        // commit reaches this OR shedEnvelopeForSize below, never both
        riderDroppedCounter.increment();
        var limiter = module.riderBudgetLadderWarnLimiter();
        limiter.performIfNotLimited(() ->
                log.warn("Dropping the {} bytes your {} returned for partition {}: with the {}-byte offset map they " +
                                "would need {} characters of metadata and the limit is {}. The offset map is " +
                                "committed regardless, carrying the marker that tells a reader the rider was " +
                                "dropped rather than never configured. This warning is rate limited to once per {}.",
                        riderByteLength,
                        ParallelConsumerOptions.Fields.riderSupplier,
                        tp,
                        innerEncodingByteLength,
                        RiderBudgetRung.RIDER.predictedCharacters(riderByteLength, innerEncodingByteLength),
                        DefaultMaxMetadataSize,
                        limiter.getRate()));
        return OffsetRiderEnvelope.Rider.dropped();
    }

    /**
     * The ladder's second descent: the marker itself does not fit, so the envelope goes and the payload becomes
     * the one this build writes with no rider configured - which is exactly how it reads back (R6). The alternative
     * would be dropping an offset map that fits, which R9 forbids.
     *
     * @param offeredState             what the rider slot held before this rung - {@code PRESENT} means an
     *                                 embedder's bytes are being lost here and this commit is the first place
     *                                 that has been counted; {@code DROPPED} means the write-time guard already
     *                                 counted the same loss, and counting it again would report two riders lost
     *                                 on a commit that only ever had one
     * @param innerEncodingByteLength  bytes of encoded offset map that keeps the payload to itself
     */
    private OffsetRiderEnvelope.Rider shedEnvelopeForSize(OffsetRiderEnvelope.RiderState offeredState,
                                                          int innerEncodingByteLength) {
        if (offeredState == OffsetRiderEnvelope.RiderState.PRESENT) {
            riderDroppedCounter.increment();
        }
        var limiter = module.riderBudgetLadderWarnLimiter();
        limiter.performIfNotLimited(() ->
                log.warn("Dropping the rider envelope entirely for partition {}: the {}-byte offset map is within " +
                                "the envelope's own {} bytes of the {}-character metadata limit, so keeping the " +
                                "envelope would cost the offset map. This commit reads back as though no {} were " +
                                "configured - not as one whose rider was dropped. This warning is rate limited to " +
                                "once per {}.",
                        tp,
                        innerEncodingByteLength,
                        OffsetRiderEnvelope.HEADER_BYTES,
                        DefaultMaxMetadataSize,
                        ParallelConsumerOptions.Fields.riderSupplier,
                        limiter.getRate()));
        return OffsetRiderEnvelope.Rider.none();
    }

    /**
     * One rate-limited warning for a supplier that threw.
     * <p>
     * Rate limited because this is a coding error rather than a transient: a supplier broken once is broken on
     * every commit of every partition, so an unlimited warning turns one bad lambda into a log nobody can read.
     * The counterpart to that is that the warning has to be self-contained - it says which option, which
     * partition, and what PC did instead - because the next one may be half a minute away.
     */
    private void warnBrokenRiderSupplier(Throwable theirs) {
        var limiter = module.brokenRiderSupplierWarnLimiter();
        limiter.performIfNotLimited(() ->
                ThrowableUtils.logWithoutEscaping(theirs, () ->
                        log.warn("Your {} threw for partition {} - committing without a rider while it keeps " +
                                        "happening. Offsets are unaffected and still committed, but nothing is " +
                                        "being carried in the offset metadata, so whatever reads the rider back " +
                                        "will find none. Fix the supplier. This warning is rate limited to once " +
                                        "per {}. Cause: {}",
                                ParallelConsumerOptions.Fields.riderSupplier,
                                tp,
                                limiter.getRate(),
                                ThrowableUtils.describeWithRootCause(theirs),
                                theirs)));
    }

    /**
     * One rate-limited warning for a supplier that returned more than the {@link RiderContext#getMaxRiderBytes()}
     * it was handed (R8).
     * <p>
     * Its own limiter rather than the broken-supplier one: a supplier that returns too much and a supplier that
     * throws are different faults with different fixes, and sharing a limiter would let whichever happened first
     * silence the other for its whole window.
     * <p>
     * The one warning has to describe two different outcomes, because {@link #riderFromSupplier}
     * produces two: beside an offset map the rider becomes the dropped marker, and a reader sees
     * {@link OffsetRiderEnvelope.RiderState#DROPPED}; on a caught-up partition there is no map for the marker to
     * sit beside, no metadata is written at all, and a reader sees {@link OffsetRiderEnvelope.RiderState#NONE}. A
     * message that promised the marker in both cases sent an operator looking for a dropped-rider state the
     * caught-up commit never wrote.
     *
     * @param besideAnOffsetMap whether this commit carries an offset map for the marker to sit beside
     */
    private void warnOversizedRider(int riderLength, int allowance, boolean besideAnOffsetMap) {
        var limiter = module.oversizedRiderWarnLimiter();
        String whatIsWritten = besideAnOffsetMap
                ? "The offset map is still committed; the rider is not, so whatever reads it back will see " +
                "that one existed and was dropped. "
                : "The partition is caught up, so with no offset map to carry the marker no metadata is " +
                "written for this commit at all, and whatever reads it back will see no rider. ";
        limiter.performIfNotLimited(() ->
                log.warn("Your {} returned {} bytes for partition {}, more than the {} it was given room for in " +
                                "this commit - dropping the rider. {}" +
                                "The allowance is in RiderContext and moves with how much of the metadata the " +
                                "offset map is using, so size the rider for the crowded case. This warning is " +
                                "rate limited to once per {}.",
                        ParallelConsumerOptions.Fields.riderSupplier,
                        riderLength,
                        tp,
                        allowance,
                        whatIsWritten,
                        limiter.getRate()));
    }

    public void onPartitionsRemoved(ShardManager<K, V> sm) {
        sm.removeAnyShardEntriesReferencedFrom(incompleteOffsets.values());
        deregisterMetrics();
    }

    /**
     * Convenience method for readability
     *
     * @return true if {@link #isAllowedMoreRecords()} is false
     * @see #isAllowedMoreRecords()
     */
    public boolean isBlocked() {
        return !isAllowedMoreRecords();
    }

    /**
     * Each time we poll a patch of records, check to see that as expected our tracked incomplete offsets exist in the
     * set, otherwise they must have been removed from the underlying partition and should be removed from our tracking
     * as we'll ever be given the record again to retry.
     * <p>
     * <p>
     * Also, does {@link #maybeTruncateBelowOrAbove}.
     */
    @SuppressWarnings("OptionalGetWithoutIsPresent") // checked with isEmpty
    private void maybeTruncateOrPruneTrackedOffsets(EpochAndRecordsMap<?, ?>.RecordsAndEpoch polledRecordBatch) {
        var records = polledRecordBatch.getRecords();

        if (records.isEmpty()) {
            log.warn("Polled an empty batch of records? {}", polledRecordBatch);
            return;
        }

        var offsetOfLowestRecord = getFirst(records).get().offset(); // NOSONAR see #isEmpty

        maybeTruncateBelowOrAbove(offsetOfLowestRecord);

        // build the hash set once, so we can do random access checks of our tracked incompletes
        var polledOffsets = records.stream()
                .map(ConsumerRecord::offset)
                .collect(Collectors.toSet());

        var offsetOfHighestRecord = getLast(records).get().offset(); // NOSONAR see #isEmpty

        // for the incomplete offsets within this range of poll batch
        var offsetsToRemoveFromTracking = new ArrayList<Long>();
        var trackedIncompletesWithinPolledBatch = incompleteOffsets.keySet().subSet(offsetOfLowestRecord, true, offsetOfHighestRecord, true);
        for (long trackedIncomplete : trackedIncompletesWithinPolledBatch) {
            boolean incompleteMissingFromPolledRecords = !polledOffsets.contains(trackedIncomplete);

            if (incompleteMissingFromPolledRecords) {
                offsetsToRemoveFromTracking.add(trackedIncomplete);
                // don't need to remove it from the #commitQueue, as it would never have been added
            }
        }
        if (!offsetsToRemoveFromTracking.isEmpty()) {
            log.warn("Offsets {} have been removed from partition {} (as they were not been returned within a polled batch " +
                            "which should have contained them - batch offset range is {} to {}), so they be removed " +
                            "from tracking state, as they will never be sent again to be retried. " +
                            "This can be caused by PC rebalancing across a partition which has been compacted on offsets above the committed " +
                            "base offset, after initial load and before a rebalance.",
                    offsetsToRemoveFromTracking,
                    getTp(),
                    offsetOfLowestRecord,
                    offsetOfHighestRecord
            );
            offsetsToRemoveFromTracking.forEach(incompleteOffsets::remove);
        }
    }

    /**
     * If the record is below the highest succeeded offset, then it is or will be represented in the current offset
     * encoding.
     * <p>
     * This may in fact be THE message holding up the partition - so must be retried.
     * <p>
     * In which case - don't want to skip it.
     * <p>
     * Generally speaking, completing more offsets below the highest succeeded (and thus the set represented in the
     * encoded payload), should usually reduce the payload size requirements.
     */
    private boolean isBlockingProgress(WorkContainer<?, ?> workContainer) {
        return workContainer.offset() < getOffsetHighestSucceeded();
    }

    /**
     * Checks if this record be taken from its partition as work.
     * <p>
     * It checks that the work is not stale, and that the partition ok to allow more records to be processed, or if the
     * record is actually blocking our progress.
     *
     * @return true if this record be taken from its partition as work.
     */
    /**
     * Second and AUTHORITATIVE of the three staleness checkpoints - per container, against live state.
     * Nothing stale is ever executed, however it reached a shard, which is what lets checkpoint 1 be
     * best-effort. {@link #epochIsStale} documents the scheme; do not duplicate it here.
     *
     * @see #epochIsStale
     */
    public boolean couldBeTakenAsWork(WorkContainer<K, V> workContainer) {
        if (checkIfWorkIsStale(workContainer)) {
            log.debug("Work is in queue with stale epoch or no longer assigned. Skipping. Shard it came from will/was removed during partition revocation. WC: {}", workContainer);
            return false;
        } else if (isAllowedMoreRecords()) {
            log.debug("Partition is allowed more records. Taking work. WC: {}", workContainer);
            return true;
        } else if (isBlockingProgress(workContainer)) {
            // allow record to be taken, even if partition is blocked, as this record completion may reduce payload size requirement
            log.debug("Partition is blocked, but this record is blocking progress. Taking work. WC: {}", workContainer);
            return true;
        } else {
            log.debug("Not allowed more records for the partition ({}) as set from previous encode run (blocked), that this " +
                            "record ({}) belongs to, due to offset encoding back pressure, is within the encoded payload already (offset lower than highest succeeded, " +
                            "not in flight ({}), continuing on to next container in shardEntry.",
                    workContainer.getTopicPartition(), workContainer.offset(), workContainer.isNotInFlight());
            return false;
        }
    }

    /**
     * Have our partitions been revoked?
     * <p>
     * This state is rare, as shards or work get removed upon partition revocation, although under busy load it might
     * occur we don't synchronize over PartitionState here so it's a bit racey, but is handled and eventually settles.
     *
     * @return true if epoch doesn't match, false if ok
     */
    boolean checkIfWorkIsStale(final WorkContainer<?, ?> workContainer) {
        Long currentPartitionEpoch = getPartitionsAssignmentEpoch();
        long workEpoch = workContainer.getEpoch();

        boolean partitionNotAssigned = isPartitionRemovedOrNeverAssigned();

        boolean epochMissMatch = currentPartitionEpoch != workEpoch;

        if (epochMissMatch || partitionNotAssigned) {
            log.debug("Epoch mismatch {} vs {} for record {}. Skipping message - it's partition has already assigned to a different consumer.",
                    workEpoch, currentPartitionEpoch, workContainer);
            return true;
        }
        return false;
    }

    private void initMetrics() {
        TopicPartition topicPartition = getTp();
        if (topicPartition == null) {
            return;
        }
        Tag[] partitionStateTags = new Tag[]{Tag.of("topic", topicPartition.topic()), Tag.of("partition", String.valueOf(topicPartition.partition()))};
        lastCommittedOffsetGauge = pcMetrics.gaugeFromMetricDef(PCMetricsDef.PARTITION_LAST_COMMITTED_OFFSET,
                this, partitionState -> partitionState.lastCommittedOffset, partitionStateTags);
        highestSeenOffsetGauge = pcMetrics.gaugeFromMetricDef(PCMetricsDef.PARTITION_HIGHEST_SEEN_OFFSET,
                this, PartitionState::getOffsetHighestSeen, partitionStateTags);
        highestCompletedOffsetGauge = pcMetrics.gaugeFromMetricDef(PCMetricsDef.PARTITION_HIGHEST_COMPLETED_OFFSET,
                this, PartitionState::getOffsetHighestSucceeded, partitionStateTags);
        highestSequentialSucceededOffsetGauge = pcMetrics.gaugeFromMetricDef(PCMetricsDef.PARTITION_HIGHEST_SEQUENTIAL_SUCCEEDED_OFFSET,
                this, PartitionState::getOffsetHighestSequentialSucceeded, partitionStateTags);
        numberOfIncompletesGauge = pcMetrics.gaugeFromMetricDef(PCMetricsDef.PARTITION_INCOMPLETE_OFFSETS,
                this, partitionState -> partitionState.incompleteOffsets.size(), partitionStateTags);
        ephochGauge = pcMetrics.gaugeFromMetricDef(PCMetricsDef.PARTITION_ASSIGNMENT_EPOCH,
                this, PartitionState::getPartitionsAssignmentEpoch, partitionStateTags);
        ratioMetadataSpaceUsedDistributionSummary = pcMetrics.getDistributionSummaryFromMetricDef(PCMetricsDef.METADATA_SPACE_USED, partitionStateTags);
        ratioPayloadUsedDistributionSummary = pcMetrics.getDistributionSummaryFromMetricDef(PCMetricsDef.PAYLOAD_RATIO_USED, partitionStateTags);
        riderSizeDistributionSummary = pcMetrics.getDistributionSummaryFromMetricDef(
                PCMetricsDef.OFFSETS_RIDER_SIZE, partitionStateTags);
        riderDroppedCounter = pcMetrics.getCounterFromMetricDef(
                PCMetricsDef.OFFSETS_RIDER_DROPPED, partitionStateTags);
        payloadStrippedCounter = pcMetrics.getCounterFromMetricDef(
                PCMetricsDef.OFFSETS_PAYLOAD_STRIPPED, partitionStateTags);
        riderSupplierFailedCounter = pcMetrics.getCounterFromMetricDef(
                PCMetricsDef.OFFSETS_RIDER_SUPPLIER_FAILED, partitionStateTags);
    }

    private void deregisterMetrics() {
        pcMetrics.removeMeter(lastCommittedOffsetGauge);
        pcMetrics.removeMeter(highestSeenOffsetGauge);
        pcMetrics.removeMeter(highestCompletedOffsetGauge);
        pcMetrics.removeMeter(highestSequentialSucceededOffsetGauge);
        pcMetrics.removeMeter(numberOfIncompletesGauge);
        pcMetrics.removeMeter(ephochGauge);
        pcMetrics.removeMeter(ratioMetadataSpaceUsedDistributionSummary);
        pcMetrics.removeMeter(ratioPayloadUsedDistributionSummary);
        // the same guarded path as everything above it: removal runs inside onPartitionsRevoked on the
        // broker-poll thread, where a throw from the user's registry would stop every commit
        pcMetrics.removeMeter(riderSizeDistributionSummary);
        pcMetrics.removeMeter(riderDroppedCounter);
        pcMetrics.removeMeter(payloadStrippedCounter);
        pcMetrics.removeMeter(riderSupplierFailedCounter);
    }
}
