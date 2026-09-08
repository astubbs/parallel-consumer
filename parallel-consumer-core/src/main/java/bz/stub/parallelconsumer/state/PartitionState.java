package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import com.facebook.infer.annotation.ThreadConfined;
import bz.stub.parallelconsumer.internal.BrokerPollSystem;
import bz.stub.parallelconsumer.internal.EpochAndRecordsMap;
import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.metrics.PCMetrics;
import bz.stub.parallelconsumer.metrics.PCMetricsDef;
import bz.stub.parallelconsumer.offsets.NoEncodingPossibleException;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager;
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
     * The thread the aborted-transaction replay is confined to, as {@code ThreadConfined} names it: the control
     * thread, which also stamps the replay generation on every dispatch, so the two are totally ordered.
     */
    public static final String CONTROL_THREAD = "pc-control";

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

    /**
     * Set once a revocation's commit has drained this partition's completed work and is about to commit it, and
     * never cleared - the state is replaced at truncation. From then on every container of this partition reads as
     * stale ({@link #checkIfWorkIsStale}), so no worker starts one, no worker produces for one, and a completion
     * that arrives anyway is dropped like any other stale result.
     * <p>
     * <b>Why the epoch cannot do this job.</b> {@link #partitionsAssignmentEpoch} is final, captured at
     * construction; bumping the manager's epoch map leaves this state comparing containers against its own old
     * epoch, and they match - the fact
     * {@code docs/solutions/logic-errors/stale-container-blocks-fresh-work-same-offset-after-rebalance-2026-08-07.md}
     * established from the other direction ("a final long set at construction"). Truncation replaces the state, which is what makes the epoch scheme work - but
     * truncation runs on the broker-poll thread after the revocation commit returns, and the gap between that
     * commit releasing the producer write lock and the truncation is wide enough for a worker parked on the
     * produce lock to start a record of the revoked partition, produce its output into the next transaction,
     * and have its completion dropped as stale at truncation: output published, offset never committed, the
     * partition's next owner reprocesses it. {@code RebalanceEoSDeadlockTest} saw two or three such duplicates
     * per rebalance with the drain fixed and this fence absent. The fence is set on the control thread INSIDE
     * the write lock - after the drain, before the commit - so there is no such gap: a worker that acquires the
     * produce lock after the commit finds the partition fenced
     * ({@code ParallelEoSStreamProcessor#acquireProduceLockRefusingRevokedWork}).
     * <p>
     * volatile: written by the control thread, read by the workers and the poll thread.
     */
    private volatile boolean fencedForRevocation;

    /**
     * The highest offset the broker has acknowledged a commit for on this partition - what
     * {@code pc.partition.latest.committed.offset} reads.
     * <p>
     * <b>It only ever rises</b>, which {@link #recordCommittedOffset} is what enforces. Under
     * {@code PERIODIC_CONSUMER_ASYNCHRONOUS} two commits can be in flight at once and their acknowledgements can
     * arrive in either order, so an answer carrying an offset a later one has already passed reaches here after the
     * higher one. Recording it would walk the commit watermark, and the gauge, BACKWARDS.
     * <p>
     * Package-private getter because this is the only way a test can read it - the gauge is the sole production
     * reader.
     */
    @Getter(PACKAGE)
    private long lastCommittedOffset;

    /**
     * The offer this partition last made for commit - the {@link OffsetAndMetadata} {@link #getCommitDataIfDirty()}
     * handed to the committer, held WHOLE.
     * <p>
     * <b>Whole, because the offset alone does not identify an offer.</b> The offered offset is
     * {@link #getOffsetToCommit()}, one above the highest <em>sequentially</em> succeeded offset, while the metadata
     * is the encoded set of incomplete offsets above it. A record completing above the lowest incomplete one
     * therefore changes the metadata and leaves the offset exactly where it was, so two requests can be in flight
     * carrying the same offset and different metadata. Comparing offsets alone, the answer to the older one would
     * match and mark the partition clean - and the newer request's metadata, the only record that the higher record
     * is done, would never be re-sent if that request then failed or was dropped, replaying records after a
     * reassignment that the encoded offset map exists to stop being replayed. Found by the Codex review on
     * astubbs/parallel-consumer#470, and pinned by
     * {@code PartitionStateAcknowledgedCommitOffsetTest.anAcknowledgementOfAnOfferWithSupersededMetadataAtTheSameOffsetDoesNotCleanThePartition}.
     * <p>
     * Comparing whole is exact rather than merely tighter: {@code Consumer#commitAsync} hands the
     * {@code OffsetCommitCallback} the very map it was given, and the transactional path calls
     * {@link #onOffsetCommitSuccess} inline with the map it just collected, so the acknowledgement of an offer
     * carries that offer's own object. Anything that did not come back identical stays dirty, which costs one extra
     * commit and cannot under-report - the same edge this whole rule lands on.
     * <p>
     * <b>It is the whole of the clean-mark rule.</b> {@link #onOffsetCommitSuccess} records every acknowledgement,
     * because an acknowledgement is true - the broker really did commit up to the offset it names - but it marks the
     * partition CLEAN only when what was acknowledged is this offer. Under {@code PERIODIC_CONSUMER_ASYNCHRONOUS} two
     * commits can be in flight at once, so an answer can arrive for an offer a later one has already passed; marking
     * clean on that answer is what would leave nothing dirty to re-send the offsets in between if the later request
     * then failed or was dropped - the very defect
     * {@code docs/solutions/logic-errors/an-async-commit-was-recorded-on-send-not-on-acknowledgement-2026-09-07.md}
     * removed, re-entered through the door that fix opened. The committer needs to know none of this: the partition
     * made the offer, so the partition is what can recognise its own answer.
     * <p>
     * <b>Which thread writes and reads it.</b> Written in the commit path ({@code getCommitDataIfDirty}, reached from
     * {@code collectCommitDataForDirtyPartitions}) and read back in {@code onOffsetCommitSuccess}, by the SAME thread
     * in every commit mode. Under both consumer commit modes that is the broker-poll thread: it sends the request,
     * and Kafka delivers a commit callback from the {@code poll()} that same thread drives. Under
     * {@code PERIODIC_TRANSACTIONAL_PRODUCER} it is the control thread, where the commit blocks and the
     * acknowledgement is recorded inline - uniformly so since astubbs/parallel-consumer#466, which moved that
     * mode's revocation-time commit off the broker-poll thread and onto the control thread by posting a request
     * to it. The revocation commit in the CONSUMER commit modes stays inline on the poll thread, which is the
     * same thread their ordinary commits already run on, so both modes remain self-consistent either way. The one
     * hand-over is {@code Consumer#close()} flushing a pending callback
     * on the closing thread, which happens only after the poll loop has finished - a hand-over with a happens-before
     * edge, not an overlap. So this is a plain field, for the reason {@link #dirty} states for the {@code long}s
     * here: that flag is the fence, and fencing these too buys nothing it does not already provide. It has the same
     * lifecycle as {@link #stateChangedSinceCommitStart} - written where the commit window opens, read where it
     * closes.
     * <p>
     * <b>Replaced, not accumulated.</b> Only the latest offer is kept, because only the latest offer may end the
     * story; every earlier one is by definition superseded. Keeping a set of outstanding offers would have to be
     * pruned by something, and the downward reset {@code maybeTruncateBelowOrAbove} performs on a bootstrap poll is
     * exactly the event no pruning rule would see - after which the partition could never match an offer again:
     * dirty forever, committing nothing.
     * <p>
     * A partition state freshly built by a rebalance starts {@code null}, so an acknowledgement for the assignment
     * before it cannot mark it clean. That is a change for the better - the previous code marked clean
     * unconditionally - and it costs at most one extra commit.
     */
    private OffsetAndMetadata offerLastMadeForCommit;

    private Gauge lastCommittedOffsetGauge;
    private Gauge highestSeenOffsetGauge;
    private Gauge highestCompletedOffsetGauge;
    private Gauge highestSequentialSucceededOffsetGauge;
    private Gauge numberOfIncompletesGauge;
    private Gauge ephochGauge;
    private DistributionSummary ratioPayloadUsedDistributionSummary;
    private DistributionSummary ratioMetadataSpaceUsedDistributionSummary;
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

    /**
     * The completed-but-uncommitted ledger (R13, KTD5): what {@link #onSuccess(long)} would otherwise drop, kept until
     * the commit carrying it succeeds, so an aborted transaction's work can be put back. The retaining kind in
     * transactional commit mode, the no-op kind otherwise - chosen once here rather than tested per completion. Its
     * own class carries its monitor and its thread-safety invariant.
     */
    private final UncommittedCompletions<K, V> uncommittedCompletions;


    public PartitionState(long newEpoch,
                          PCModule<K, V> pcModule,
                          TopicPartition topicPartition,
                          OffsetMapCodecManager.HighestOffsetAndIncompletes offsetData) {
        this.module = pcModule;
        this.uncommittedCompletions = module.options().isUsingTransactionCommitMode()
                ? new UncommittedCompletions<>()
                : UncommittedCompletions.none();

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

    /**
     * The broker acknowledged a commit for this partition: <b>record the offset always, mark the partition clean only
     * if this is the answer to what the partition last offered.</b>
     * <p>
     * The two halves are separate because an acknowledgement carries two different things. Its offset is TRUE - the
     * broker really did commit up to it - so it is recorded whether or not a later request has since passed it, and
     * {@link #recordCommittedOffset} keeps the higher of the two if the answers arrive out of order. What it may not
     * do, unless it is the answer to the latest offer, is end the story: see {@link #offerLastMadeForCommit},
     * which owns the rule and the reasoning.
     * <p>
     * The clean mark is still subject to the existing protocol - {@link #setClean()} declines when the partition's
     * state changed again while the commit was in flight.
     */
    public void onOffsetCommitSuccess(OffsetAndMetadata committed) { //NOSONAR
        recordCommittedOffset(committed);
        if (committed.equals(offerLastMadeForCommit)) {
            setClean();
            // the ledger is trimmed on the answer to the offer that carried it - a stale answer to an older
            // offer leaves the newer snapshot in place, so a replay can only over-replay, never under
            uncommittedCompletions.onCommitSuccess();
        } else {
            log.debug("Acknowledged commit for {} is {}, not the {} this partition last offered - the offset is " +
                            "recorded, but the partition stays dirty until the newer offer is answered",
                    tp, committed, offerLastMadeForCommit);
        }
    }

    /**
     * Advances {@link #lastCommittedOffset} monotonically - see that field for why it may not move backwards.
     */
    private void recordCommittedOffset(OffsetAndMetadata committed) {
        if (committed.offset() > lastCommittedOffset) {
            lastCommittedOffset = committed.offset();
        } else {
            log.debug("Acknowledged commit for {} carries offset {}, at or below the {} already recorded - keeping " +
                            "the higher one, as the commit watermark only rises",
                    tp, committed.offset(), lastCommittedOffset);
        }
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

    /**
     * Records a completed offset: it leaves {@link #incompleteOffsets}, and the high-water mark and dirty flag
     * follow.
     * <p>
     * <b>If you are here from the assert below, it has fired for three shapes, and all three are closed and
     * pinned.</b> Do not read it as proof of a double delivery - that was the right inference for the 2026-08-22
     * sightings because every one carried {@code deliveryCount == 2}, and it was the evidence that made it
     * right, not the assert. The shapes: a work claim decided before another selector's completion (closed by
     * astubbs#335, pinned by {@code WorkClaimStateMachineTest}); a completion acting on a partition state
     * swapped by a rebalance after its staleness check (closed by astubbs#346, pinned by
     * {@code WorkManagerStaleCheckDoubleLookupTest}); and a record selected and completed between being
     * published to its shard and its offset being registered - the one shape with no double delivery at all
     * (closed by astubbs#370, pinned by {@code PartitionStateRegistrationOrder370Test}). A fourth would need
     * an offset removed from the incomplete set by some route other than this method, or a completion for an
     * offset this state never registered.
     * <p>
     * @see #onSuccess(WorkContainer) which is what the engine calls; this overload keeps the record out of the ledger
     *         and exists for the tests that drive offsets directly
     */
    public void onSuccess(long offset) {
        //noinspection OptionalAssignedToNull - null check to see if key existed
        boolean removedFromIncompletes = this.incompleteOffsets.remove(offset) != null; // NOSONAR
        assert (removedFromIncompletes);

        updateHighestSucceededOffsetSoFar(offset);

        setDirty();
    }

    /**
     * Records a completed container: the offset leaves the incomplete set, and in transactional commit mode the
     * record enters the completed-but-uncommitted ledger until the commit carrying it succeeds.
     */
    public void onSuccess(WorkContainer<K, V> work) {
        onSuccess(work.offset());
        uncommittedCompletions.record(work.offset(), work.getCr());
    }

    /**
     * Puts every completed-but-uncommitted record back into processing, after the transaction that carried its
     * output was aborted (R13, KTD5). Each record goes back through the pair
     * {@link #maybeRegisterNewPollBatchAsWork} uses for a fresh poll batch - a new {@link WorkContainer} at the
     * partition's current epoch into its shard, then the record back into {@link #incompleteOffsets} - so
     * {@link #isRecordPreviouslyCompleted} answers false for it again, {@link #getOffsetHighestSequentialSucceeded()}
     * drops below it, and no offset from the aborted transaction can be committed. The partition is marked dirty so
     * the next commit publishes the lowered frontier.
     * <p>
     * <b>Cleared suspicion, 2026-09-02: a container completing after the replay cannot target a restored offset.</b>
     * The suspicion: {@link #onSuccess(long)} asserts the offset was still incomplete, so a late-arriving completion
     * for a restored offset would either trip that assert or silently re-complete a record the replay just put back.
     * The discriminator is what the caller does before calling this: recovery holds the producer write lock, so no
     * worker is between {@code beginProducing} and {@code cleanUpContext}, and it drains the mailbox first - so every
     * container that produced into the aborted transaction has already reached {@code handleFutureResult} and is
     * either in this ledger (success) or the retry queue (failure) before the replay runs. A record on the
     * {@code poll} flow takes no produce lock, but produced nothing, so nothing of it was discarded and its late
     * completion targets an offset that is not in the ledger. What would reopen it: calling this outside the write
     * lock, or before the drain - nothing gates that but the one call site in
     * {@code AbstractParallelEoSStreamProcessor}.
     * <p>
     * Confined to the control thread - declared for RacerD by the annotation, and asserted at the one entry point,
     * {@code AbstractParallelEoSStreamProcessor#replayWorkDiscardedByAbortedTransaction}. The ledger's monitor is
     * not held across the replay itself, which enters the shard map's per-key lock and must not do so while holding
     * it.
     *
     * @return how many records were put back
     */
    @ThreadConfined(PartitionState.CONTROL_THREAD)
    public int restoreCompletedButUncommittedWork() {
        Map<Long, ConsumerRecord<K, V>> discarded = uncommittedCompletions.snapshotInOffsetOrder();
        if (discarded.isEmpty()) {
            return 0;
        }
        long epoch = getPartitionsAssignmentEpoch();
        for (ConsumerRecord<K, V> record : discarded.values()) {
            // register, then publish - the order maybeRegisterNewPollBatchAsWork keeps (astubbs#370), for the same
            // reason: a scanner may select and complete the container the instant it is reachable through its
            // shard, and that completion must find the offset already in the incomplete set. Pinned by
            // PartitionStateAbortedTransactionReplayTest's completion-on-publish case.
            addNewIncompleteRecord(record);
            getShardManager().addWorkContainer(epoch, record);
        }
        // forgotten only once every entry is back in processing: a throw mid-loop leaves the ledger intact for the
        // next pass, and both registrations tolerate a repeat (the incomplete set is a put, the shard keeps its
        // resident), so replaying an entry twice costs nothing
        uncommittedCompletions.forget(discarded.keySet());
        setDirty();
        log.debug("Restored {} completed-but-uncommitted record(s) to processing for {} after an aborted transaction; commit frontier is now {}",
                discarded.size(), tp, getOffsetToCommit());
        return discarded.size();
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

    /**
     * Registers a polled batch: each record not already complete goes into {@link #incompleteOffsets} and then
     * into its shard, in that order - see the loop body for why the order is the contract.
     * <p>
     * <b>Thread model, derived from the callers rather than declared (2026-09-05).</b> The one production route
     * here is {@code AbstractParallelEoSStreamProcessor#processWorkCompleteMailBox}, on the control thread: the
     * broker-poll thread's {@code registerWork} only posts the batch to the mailbox, and the same drain loop
     * that calls {@code WorkManager#registerWork} also calls {@code WorkManager#handleFutureResult}, so on the
     * shipped engine a registration and a completion never overlap. That is what made the old publish-first
     * order safe by accident. It is <em>not</em> asserted here: the Lincheck harnesses
     * ({@code PartitionStateLincheckTest}, {@code WorkManagerLincheckTest}) deliberately drive
     * {@link #onSuccess(long)} and {@code handleFutureResult} from several threads to measure exactly the
     * interleavings a guard would refuse, and the direct-pull engine will select from worker threads. The
     * register-then-publish order is the invariant that survives all of those; the plain {@code long}s
     * are the residue that still assumes one writer. There are two, and the second is the one with teeth:
     * {@code offsetHighestSeen}, written here through {@link #addNewIncompleteRecord}, and
     * {@code offsetHighestSucceeded}, which {@link #onSuccess(long)} read-modify-writes and which
     * {@link #getOffsetHighestSequentialSucceeded()} returns <em>directly</em> whenever
     * {@code incompleteOffsets} is empty - so a stale read of it is an offset committed to the broker, not
     * only bookkeeping. The {@code dirty} field's own javadoc records jcstress measuring that exact
     * staleness (the reader seeing {@code dirty} set while {@code offsetHighestSucceeded} was still stale),
     * and what closes it is the release/acquire pair that field's {@code volatile} provides - nothing on
     * this method.
     * <p>
     * <b>What would reopen this, and what would catch it (2026-09-05).</b> A second writer of either
     * {@code long} - the direct-pull engine selecting from worker threads, or a completion path moved off
     * the control thread - reopens it, and the {@code volatile} on {@code dirty} does not cover a
     * write/write pair. <b>Nothing in this repository would catch that today.</b> The
     * {@code jcstress-poc} module's {@code SeenSucceededOrderingProbes} owns the question, and that module
     * is absent from the root {@code pom.xml}'s {@code <modules>} list, so no reactor build reaches it; no
     * workflow and no script in {@code bin/} names it either - {@code grep -rn jcstress .github/ bin/}
     * returns nothing. It is run by hand or not at all.
     */
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
                // REGISTER, then PUBLISH - the order is the invariant, and it is the only thing here that
                // keeps a completion valid whichever thread selects the record. The offset enters
                // incompleteOffsets first, so by the time a shard scan can reach the container, the state
                // its completion removes from already holds it. Publishing first (the order until
                // astubbs#370) left a gap in which the container was selectable and its offset absent:
                // a completion landing there tripped onSuccess's assert, and without -ea it silently
                // re-registered an already-completed offset that nothing could ever complete again, pinning
                // the commit frontier below it. Both maps are concurrent, so the put here happens-before
                // the shard's put and that happens-before any scanner's get - no thread model required.
                // Pinned by PartitionStateRegistrationOrder370Test.
                addNewIncompleteRecord(aRecord);
                getShardManager().addWorkContainer(epochOfInboundRecords, aRecord);
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

    /**
     * Marks every container of this partition stale from now on - see {@link #fencedForRevocation} for when, on
     * which thread, and why the epoch cannot do it.
     */
    public void fenceForRevocation() {
        log.debug("Fencing {} for revocation: its completed work is drained and about to be committed, nothing " +
                "further may start or produce for it", getTp());
        this.fencedForRevocation = true;
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
            uncommittedCompletions.snapshotForCommit(); // the same guard for the ledger: only what this commit carries is trimmed on its success
            OffsetAndMetadata offered = createOffsetAndMetadata();
            // remembering the offer WHOLE is what lets onOffsetCommitSuccess recognise the answer to it, and decline
            // to mark clean on the answer to an older one - the offset alone does not identify an offer, because
            // completing a record above the lowest incomplete one changes only the metadata. See offerLastMadeForCommit
            offerLastMadeForCommit = offered;
            return of(offered);
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
     *
     * @return the encoded offset map if one was possible, paired with the offset it was encoded
     *         against. The two travel together deliberately: committing the payload against a
     *         later offset than the one it describes is the confluentinc#893 defect, so the caller
     *         must never re-derive the offset.
     */
    private ParallelConsumer.Tuple<Optional<String>, Long> tryToEncodeOffsets() {
        long offsetOfNextExpectedMessage = getOffsetToCommit();

        if (incompleteOffsets.isEmpty()) {
            setAllowedMoreRecords(true);
            return ParallelConsumer.Tuple.pairOf(empty(), offsetOfNextExpectedMessage);
        }

        try {
            // todo refactor use of null shouldn't be needed. Is OffsetMapCodecManager stateful? remove null - confluentinc#233
            var offsetRange = getOffsetHighestSucceeded() - offsetOfNextExpectedMessage;
            String offsetMapPayload = om.makeOffsetMetadataPayload(offsetOfNextExpectedMessage, this);
            ratioPayloadUsedDistributionSummary.record(offsetMapPayload.length() / (double) offsetRange);
            ratioMetadataSpaceUsedDistributionSummary.record(offsetMapPayload.length() / (double) OffsetMapCodecManager.DefaultMaxMetadataSize);
            boolean mustStrip = updateBlockFromEncodingResult(offsetMapPayload);
            if (mustStrip) {
                return ParallelConsumer.Tuple.pairOf(empty(), offsetOfNextExpectedMessage);
            } else {
                return ParallelConsumer.Tuple.pairOf(of(offsetMapPayload), offsetOfNextExpectedMessage);
            }
        } catch (NoEncodingPossibleException e) {
            setAllowedMoreRecords(false);
            log.warn("No encodings could be used to encode the offset map, skipping. Warning: messages might be replayed on rebalance.", e);
            return ParallelConsumer.Tuple.pairOf(empty(), offsetOfNextExpectedMessage);
        }
    }

    /**
     * @return true if the payload is too large and must be stripped
     */
    private boolean updateBlockFromEncodingResult(String offsetMapPayload) {
        int metaPayloadLength = offsetMapPayload.length();
        boolean mustStrip = false;

        if (metaPayloadLength > DefaultMaxMetadataSize) {
            // exceeded maximum API allowed, strip the payload
            mustStrip = true;
            setAllowedMoreRecords(false);
            log.warn("Offset map data too large (size: {}) to fit in metadata payload hard limit of {} - cannot include in commit. " +
                            "Warning: messages might be replayed on rebalance. " +
                            "See kafka.coordinator.group.OffsetConfig#DefaultMaxMetadataSize = {} and confluentinc issue #47.",
                    metaPayloadLength, DefaultMaxMetadataSize, DefaultMaxMetadataSize);
        } else if (metaPayloadLength > getPressureThresholdValue()) { // and thus metaPayloadLength <= DefaultMaxMetadataSize
            // try to turn on back pressure before max size is reached
            setAllowedMoreRecords(false);
            log.warn("Payload size {} higher than threshold {}, but still lower than max {}. Will write payload, but will " +
                            "not allow further messages, in order to allow the offset data to shrink (via succeeding messages).",
                    metaPayloadLength, getPressureThresholdValue(), DefaultMaxMetadataSize);

        } else { // and thus (metaPayloadLength <= pressureThresholdValue)
            if (allowedMoreRecords == false) {
                // guard is useful for debugging to catch the transition from false to true
                setAllowedMoreRecords(true);
            }
            log.debug("Payload size {} within threshold {}", metaPayloadLength, getPressureThresholdValue());
        }

        return mustStrip;
    }

    private double getPressureThresholdValue() {
        return DefaultMaxMetadataSize * PartitionStateManager.getUSED_PAYLOAD_THRESHOLD_MULTIPLIER();
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

        if (fencedForRevocation) {
            log.debug("Partition {} is fenced for revocation - its work is stale whatever its epoch. Skipping {}",
                    getTp(), workContainer);
            return true;
        }
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
    }
}
