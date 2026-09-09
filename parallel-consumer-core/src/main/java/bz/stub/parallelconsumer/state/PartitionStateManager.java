package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2025 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import com.facebook.infer.annotation.ThreadConfined;
import bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.internal.BrokerPollSystem;
import bz.stub.parallelconsumer.internal.EpochAndRecordsMap;
import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.metrics.PCMetrics;
import bz.stub.parallelconsumer.metrics.PCMetricsDef;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tag;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * In charge of managing {@link PartitionState}s.
 * <p>
 * This state is shared between the {@link BrokerPollSystem} thread and the {@link AbstractParallelEoSStreamProcessor}.
 *
 * @author Antony Stubbs
 * @see PartitionState
 */
// metrics: assigned partitions and their epochs, number of assigned partitions,
@Slf4j
public class PartitionStateManager<K, V> implements ConsumerRebalanceListener {

    public static final double USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT = 0.75;

    /**
     * Best efforts attempt to prevent usage of offset payload beyond X% - as encoding size test is currently only done
     * per batch, we need to leave some buffer for the required space to overrun before hitting the hard limit where we
     * have to drop the offset payload entirely.
     */
    @Getter
    @Setter
    // todo remove static
    private static double USED_PAYLOAD_THRESHOLD_MULTIPLIER = USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT;

    private final ShardManager<K, V> sm;

    /**
     * Hold the tracking state for each of our managed partitions.
     */
    private final Map<TopicPartition, PartitionState<K, V>> partitionStates = new ConcurrentHashMap<>();

    /**
     * Record the generations of partition assignment, for fencing off invalid work.
     * <p>
     * NOTE: This must live outside of {@link PartitionState}, as it must be tracked across partition lifecycles.
     * <p>
     * Starts at zero.
     * <p>
     * NOTE: Must be concurrent because it can be set by one thread, but read by another.
     */
    private final Map<TopicPartition, Long> partitionsAssignmentEpochs = new ConcurrentHashMap<>();

    private final PCModule<K, V> module;

    private Gauge numberOfPartitionsGauge;
    private Gauge totalIncompletesGauge;
    /**
     * NOTE: Must be concurrent because it can be set by one thread, but read by another - the same reason
     * {@link #partitionsAssignmentEpochs} above says so. Written on the broker-poll thread by the rebalance
     * callbacks, read on the control thread by {@link #incrementSlowWorkCounter} as work is retrieved.
     */
    private final Map<TopicPartition, Counter> slowWorkCounters = new ConcurrentHashMap<>();

    private final PCMetrics pcMetrics;

    /**
     * Cached instance — creating throwaway OffsetMapCodecManagers on every partition assignment
     * leaked metrics (each instance registered duplicate timers/counters). See <a href="https://github.com/confluentinc/parallel-consumer/issues/859">confluentinc#859</a>, <a href="https://github.com/confluentinc/parallel-consumer/issues/233">confluentinc#233</a>.
     */
    // TODO(refactor): decode-only + single-threaded today, so sharing one instance is safe; NOT
    // thread-safe if confluentinc#200 parallelises encoding. Broader confluentinc#233 (split encode/decode, de-static) remains: https://github.com/confluentinc/parallel-consumer/issues/233
    // See docs/refactoring.md.
    private final OffsetMapCodecManager<K, V> offsetMapCodecManager;

    public PartitionStateManager(PCModule<K, V> module, ShardManager<K, V> sm) {
        this.sm = sm;
        this.module = module;
        this.pcMetrics = module.pcMetrics();
        this.offsetMapCodecManager = new OffsetMapCodecManager<>(module);
        initMetrics();
    }

    public PartitionState<K, V> getPartitionState(TopicPartition tp) {
        return partitionStates.get(tp);
    }

    private PartitionState<K, V> getPartitionState(EpochAndRecordsMap<K, V>.RecordsAndEpoch recordsAndEpoch) {
        return getPartitionState(recordsAndEpoch.getTopicPartition());
    }

    protected PartitionState<K, V> getPartitionState(WorkContainer<K, V> workContainer) {
        TopicPartition topicPartition = workContainer.getTopicPartition();
        return getPartitionState(topicPartition);
    }

    /**
     * Load offset map for assigned assignedPartitions
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> assignedPartitions) {
        log.debug("Partitions assigned: {}", assignedPartitions);
        log.trace("Epoch map before assignment: {}", partitionsAssignmentEpochs);

        for (final TopicPartition partitionAssignment : assignedPartitions) {
            boolean isAlreadyAssigned = this.partitionStates.containsKey(partitionAssignment);
            if (isAlreadyAssigned) {
                PartitionState<K, V> previouslyAssignedState = partitionStates.get(partitionAssignment);
                if (previouslyAssignedState.isRemoved()) {
                    log.trace("Reassignment of previously revoked partition {} - state: {}", partitionAssignment, previouslyAssignedState);
                } else {
                    log.warn("New assignment of partition which already exists and isn't recorded as removed in " +
                            "partition state. Could be a state bug - was the partition revocation somehow missed, " +
                            "or is this a race? Please file a GH issue. Partition: {}, state: {}", partitionAssignment, previouslyAssignedState);
                }
            }
        }

        incrementPartitionAssignmentEpoch(assignedPartitions);

        try {
            var partitionStates = offsetMapCodecManager.loadPartitionStateForAssignment(assignedPartitions);
            this.partitionStates.putAll(partitionStates);
            initPartitionCounters(assignedPartitions);

            // remove stale work containers after partition epoch changed
            // because we will judge if container is stale or not by comparing between
            // epoch from WorkContainer to partitionsAssignmentEpoch in PartitionState
            long staleContainerCnt = sm.removeStaleContainers();
            log.debug("removed stale container count : {}", staleContainerCnt);
        } catch (Exception e) {
            log.error("Error in onPartitionsAssigned", e);
            throw e;
        }
    }

    private void initPartitionCounters(Collection<TopicPartition> assignedPartitions) {
        assignedPartitions.forEach(topicPartition -> {
            slowWorkCounters.computeIfAbsent(topicPartition, tp -> pcMetrics
                    .getCounterFromMetricDef(PCMetricsDef.SLOW_RECORDS,
                            Tag.of("topic", tp.topic()),
                            Tag.of("partition", String.valueOf(tp.partition())))
            );
        });
    }

    /**
     * Metrics de-registration for revoked partitions - and it must NEVER throw.
     * <p>
     * This runs inside {@code onPartitionsRevoked}, which runs on the broker-poll thread inside
     * {@code poll()}. The meter registry is usually the USER'S, so this is third-party code on the
     * rebalance path: an exception here escapes the callback and kills the poll thread, which is the
     * only producer of commit responses, so every later commit blocks until it times out. That is the
     * confluentinc#857 family's worst failure shape, reached from a reporting concern.
     * <p>
     * No try/catch here on purpose: {@link PCMetrics#removeMeter} carries the never-throws contract,
     * guarded once at the source because this is one of eleven teardown call sites and a guard at each
     * is a guard someone will miss. A second one here could never fire, and defensive code that cannot
     * fire is worse than none - it implies the contract is doubted. Losing a meter is an acceptable
     * outcome; losing the poll thread is not.
     */
    private void deregisterPartitionCounters(Collection<TopicPartition> removedPartitions) {
        removedPartitions.forEach(topicPartition -> {
            Counter counter = slowWorkCounters.remove(topicPartition);
            if (counter != null) {
                pcMetrics.removeMeter(counter);
            }
        });
    }

    public void incrementSlowWorkCounter(TopicPartition topicPartition) {
        Optional.ofNullable(slowWorkCounters.get(topicPartition)).ifPresent(Counter::increment);
    }

    /**
     * Clear offset map for revoked partitions
     * <p>
     * {@link AbstractParallelEoSStreamProcessor#onPartitionsRevoked} handles committing off offsets upon revoke
     *
     * @see AbstractParallelEoSStreamProcessor#onPartitionsRevoked
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.info("Partitions revoked: {}", partitions);

        try {
            onPartitionsRemoved(partitions);
        } catch (Exception e) {
            log.error("Error in onPartitionsRevoked", e);
            throw e;
        }
    }

    /**
     * The step between a revocation commit's drain and its commit, on the control thread inside the producer
     * write lock: from here on nothing may start or produce for these partitions, so that the offsets about to be
     * committed are the last word this instance has on them. {@link PartitionState#fenceForRevocation} owns the
     * reasoning; truncation ({@link #onPartitionsRevoked}) follows on the poll thread once the commit has returned.
     * <p>
     * <b>A fence belongs to one assignment generation, which is why the caller passes epochs and not partitions.</b>
     * The pass that serves a revocation can run after the revocation's waiter gave up: the poll thread then
     * truncates, and the partition can come back to this instance under a new epoch with a fresh state before the
     * late pass reaches this point. Fencing whatever state occupies the key at that moment would fence the NEW
     * generation, silently, until the next rebalance - the independent cross-model review of the fix found it. So a
     * partition is fenced only while its live epoch still equals the one the request was posted with; a state that
     * is missing (a failed assignment, astubbs#451) or already removed is left alone, the removed one because it is
     * the shared {@link RemovedPartitionState} singleton and already reads as stale.
     *
     * @param partitionEpochsAtRequest the revoked partitions, each with the assignment epoch it had when the revocation
     *                                 was posted
     */
    public void fenceForRevocation(Map<TopicPartition, Long> partitionEpochsAtRequest) {
        // A loop, not a forEach lambda: Infer keys its findings on Class.method, and a lambda added here renumbers
        // the synthetic lambda$... names of every method after it - which renamed a known finding in
        // onOffsetCommitSuccess and read as a new one to the ratchet.
        for (Map.Entry<TopicPartition, Long> entry : partitionEpochsAtRequest.entrySet()) {
            TopicPartition partition = entry.getKey();
            Long epochAtRequest = entry.getValue();
            var state = getPartitionState(partition);
            if (state == null || state.isRemoved()) {
                log.debug("No state to fence for {} - never assigned, its assignment failed, or already truncated", partition);
                continue;
            }
            Long liveEpoch = getEpochOfPartition(partition);
            if (!Objects.equals(liveEpoch, epochAtRequest)) {
                log.info("Not fencing {}: it was revoked at epoch {} but is now assigned at epoch {}, so the fence " +
                        "belongs to a generation that has already been truncated", partition, epochAtRequest, liveEpoch);
                continue;
            }
            state.fenceForRevocation();
        }
    }

    void onPartitionsRemoved(final Collection<TopicPartition> partitions) {
        incrementPartitionAssignmentEpoch(partitions);
        resetOffsetMapAndRemoveWork(partitions);
        deregisterPartitionCounters(partitions);

        // remove stale work containers after partition epoch changed
        // because we will judge if container is stale or not by comparing between
        // epoch from WorkContainer to partitionsAssignmentEpoch in PartitionState
        sm.removeStaleContainers();
    }

    /**
     * Clear offset map for lost partitions
     */
    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        try {
            log.info("Lost partitions: {}", partitions);
            onPartitionsRemoved(partitions);
        } catch (Exception e) {
            log.error("Error in onPartitionsLost", e);
            throw e;
        }
    }

    /**
     * Records that a commit succeeded, for each partition that was committed.
     * <p>
     * Per partition, this delegates to {@link PartitionState#onOffsetCommitSuccess}, which stores the newly committed
     * offset as the partition's last committed offset and marks the partition clean - unless the acknowledgement is
     * to an offer a later one has passed, or its state changed again while the commit was in flight, in either of
     * which cases it stays dirty and will be committed again.
     * <p>
     * <b>No offsets are discarded here.</b> Earlier versions of this javadoc described truncating tracked offsets below
     * the committed offset once a commit landed. That does not happen, and cannot: {@link PartitionState} tracks only
     * <em>incomplete</em> offsets, and the offset committed is the lowest incomplete one - so there is nothing below it
     * left to throw away.
     * <p>
     * Truncation of tracked state does still exist, but it happens on the <b>bootstrap poll</b> rather than on commit -
     * see {@link PartitionState}'s {@code maybeTruncateBelowOrAbove}, reached from its
     * {@code maybeTruncateOrPruneTrackedOffsets}. That is where records removed by retention or compaction, or a
     * committed offset raised externally, get reconciled against the offsets we track.
     *
     * @param committed the offsets just successfully committed to the broker, by partition
     */
    public void onOffsetCommitSuccess(Map<TopicPartition, OffsetAndMetadata> committed) {
        committed.forEach((tp, meta) -> {
            var partition = getPartitionState(tp);
            partition.onOffsetCommitSuccess(meta);
        });
    }

    /**
     * Remove work from removed partition.
     * <p>
     *
     * <b>On shard removal:</b>
     *
     * <li>{@link  ProcessingOrder#PARTITION} ordering, work shards and partition queues are the same,
     * so remove all from referenced shards
     *
     * <li>{@link ProcessingOrder#KEY} ordering, all records in a shard will be of
     * the same key, so by definition all records with this key should be removed - i.e. the entire shard
     *
     * <li>{@link ProcessingOrder#UNORDERED} ordering, {@link WorkContainer}s go into shards keyed by partition, so
     * falls back to the {@link ProcessingOrder#PARTITION} case
     *
     * <p>
     * <b>A revoked partition may have no state, and that is survivable.</b> {@link #onPartitionsAssigned} records
     * the epoch before it loads the state, so anything thrown by the load - {@code consumer.committed()} failing, or
     * {@code invalidOffsetMetadataPolicy(FAIL)} rejecting metadata - leaves the partition with an epoch and no
     * entry here. Kafka keeps it assigned regardless: the assignment is applied before the listener runs, and the
     * exception is rethrown out of {@code poll()} with the member STABLE.
     * <p>
     * <b>How this sweep is then reached - not on "the next rebalance".</b> That exception propagates the whole way:
     * {@link #onPartitionsAssigned} logs and rethrows, {@code AbstractParallelEoSStreamProcessor.onPartitionsAssigned}
     * has no catch, and {@code BrokerPollSystem.controlLoop}'s catch notifies the committer and rethrows, so the
     * broker-poll thread ends and there is no next poll. The route left is the close sequence, on the
     * <em>control</em> thread: {@code supervise()} surfaces the dead poller, {@code doClose} runs, and
     * {@code maybeCloseConsumer} closes the consumer, whose {@code onLeavePrepare} drives {@code onPartitionsRevoked}
     * (or {@code onPartitionsLost}) into this sweep before it sends LeaveGroup. <b>That is a live path in every
     * commit mode</b>, and was not always: the step was gated on {@code committer instanceof ProducerManager}, so
     * after a poller death the consumer-commit modes closed nothing and this branch was insurance there.
     * {@code maybeCloseConsumer} now also fires when the poll thread ended without closing the consumer, which is
     * exactly this scenario - see
     * {@code docs/solutions/logic-errors/a-duty-assigned-by-role-is-unassigned-when-the-role-holder-dies-2026-09-08.md}.
     * <p>
     * On the live route a throw here is expensive twice over: Kafka's close throws out of {@code onLeavePrepare}
     * before {@code maybeLeaveGroup}, so the member's departure is left to the session timeout, and the {@code for}
     * loop below aborts, leaving every partition after this one in the same revoke unswept. There is nothing to
     * sweep for a state that was never installed - no work was registered against it, no shard references it - and
     * {@link #incrementPartitionAssignmentEpoch} has already fenced anything that somehow carried the old epoch. So
     * the partition is marked removed, the gap is logged, and the sweep moves on.
     * {@code PartitionStateManagerRevokeAfterFailedAssignmentTest} drives both routes into the partial state and the
     * mixed revoke of a stateless partition alongside a stateful one.
     */
    private void resetOffsetMapAndRemoveWork(Collection<TopicPartition> allRemovedPartitions) {
        for (TopicPartition removedPartition : allRemovedPartitions) {
            // by replacing with a no op implementation, we protect for stale messages still in queues which reference it
            // however it means the map will only grow, but only it's key set
            var partition = this.partitionStates.put(removedPartition, RemovedPartitionState.getSingleton());

            if (partition == null) {
                log.warn("Partition {} revoked with no tracked state: its assignment must have failed after the epoch "
                        + "was recorded and before its state was installed (see the earlier onPartitionsAssigned error). "
                        + "Nothing to remove; the epoch has been advanced so no work referencing it can be taken.",
                        removedPartition);
                continue;
            }

            partition.onPartitionsRemoved(sm);
        }
    }

    /**
     * The current assignment epoch of the partition, or empty if the assignment callback has not fired for it yet.
     * <p>
     * Absence only ever means "not yet assigned": epochs are written by {@link #incrementPartitionAssignmentEpoch}
     * on every assignment and every revocation, and nothing removes one. The two production readers consume an
     * absent epoch in opposite ways, and the type makes each choose at the call site:
     * <ul>
     *   <li><b>Skip, on the poll path</b> - {@link bz.stub.parallelconsumer.internal.EpochAndRecordsMap} can be
     *   handed a poll's records for a partition before its assignment callback fires (the eager-protocol race), and
     *   it skips them: they are uncommitted, so Kafka re-delivers them once the callback has run.</li>
     *   <li><b>Fail closed, on the assignment path</b> - {@code OffsetMapCodecManager.epochOfPartitionBeingAssigned}
     *   builds {@link PartitionState} from this epoch and must never see it absent, because
     *   {@link #onPartitionsAssigned} writes every epoch before it loads any state. It throws, naming that
     *   ordering; its javadoc carries the trace.</li>
     * </ul>
     *
     * @return the current epoch of the partition, or empty if the assignment callback has not fired for it
     * @see #getEpochOfPartition the nullable form of the same lookup
     */
    public Optional<Long> epochOfPartitionIfAssigned(TopicPartition partition) {
        return Optional.ofNullable(partitionsAssignmentEpochs.get(partition));
    }

    /**
     * The legacy nullable form of {@link #epochOfPartitionIfAssigned}: the same lookup, with null carrying exactly the
     * meaning empty carries there - the assignment callback has not fired for this partition yet.
     * <p>
     * New callers should prefer the {@link Optional} form, so that absence is handled at the call site rather than by
     * an accidental unbox: this method's null was consumed by an auto-unbox into a primitive on the assignment path
     * until {@code OffsetMapCodecManager.epochOfPartitionBeingAssigned} was made to fail closed, and a nullable
     * boxed return reads identically whether the caller decided about the null or forgot it. The return type is not
     * narrowed to {@code long} because absence is a legitimate outcome on the poll path (see the Optional form's
     * javadoc). Existing callers - the test suite unboxes this in many places - can migrate when they touch the site.
     *
     * @return the current epoch of the partition, or null if the assignment callback has not fired for it
     */
    public Long getEpochOfPartition(TopicPartition partition) {
        return epochOfPartitionIfAssigned(partition).orElse(null);
    }


    private void incrementPartitionAssignmentEpoch(final Collection<TopicPartition> partitions) {
        for (final TopicPartition partition : partitions) {
            Long oldEpoch = partitionsAssignmentEpochs.getOrDefault(partition, PartitionState.KAFKA_OFFSET_ABSENCE);
            Long newEpoch = oldEpoch + 1;
            partitionsAssignmentEpochs.put(partition, newEpoch);
            log.trace("Epoch for {} incremented: {} -> {}", partition, oldEpoch, newEpoch);
        }
    }

    /**
     * Check we have capacity in offset storage to process more messages
     */
    public boolean isAllowedMoreRecords(TopicPartition tp) {
        PartitionState<K, V> partitionState = getPartitionState(tp);
        return partitionState.isAllowedMoreRecords();
    }

    /**
     * @see #isAllowedMoreRecords(TopicPartition)
     */
    public boolean isAllowedMoreRecords(WorkContainer<?, ?> wc) {
        return isAllowedMoreRecords(wc.getTopicPartition());
    }

    public boolean hasIncompleteOffsets() {
        for (var partition : getAssignedPartitions().values()) {
            if (partition.hasIncompleteOffsets())
                return true;
        }
        return false;
    }

    public long getNumberOfIncompleteOffsets() {
        Collection<PartitionState<K, V>> values = getAssignedPartitions().values();
        return values.stream()
                .mapToLong(PartitionState::getNumberOfIncompleteOffsets)
                .reduce(Long::sum)
                .orElse(0);
    }

    public long getHighestSeenOffset(final TopicPartition tp) {
        return getPartitionState(tp).getOffsetHighestSeen();
    }

    /**
     * Applies the completion to the given, ALREADY-RESOLVED state - the caller resolves the state once, checks
     * staleness against it, and passes the same reference here, so the state a staleness check validated can
     * never diverge from the state the completion then mutates. Resolving again here was half of the
     * checkpoint-3 torn read (see {@code WorkManager#handleFutureResult}).
     */
    public void onSuccess(WorkContainer<K, V> wc, PartitionState<K, V> partitionState) {
        partitionState.onSuccess(wc);
    }

    /**
     * @return how many completed-but-uncommitted records were put back into processing across the assigned partitions
     * @see PartitionState#restoreCompletedButUncommittedWork()
     */
    @ThreadConfined(PartitionState.CONTROL_THREAD)
    public int restoreCompletedButUncommittedWork() {
        int restored = 0;
        for (var state : getAssignedPartitions().values()) {
            restored += state.restoreCompletedButUncommittedWork();
        }
        return restored;
    }

    /**
     * Same single-resolution contract as {@link #onSuccess(WorkContainer, PartitionState)}.
     */
    public void onFailure(WorkContainer<K, V> wc, PartitionState<K, V> partitionState) {
        partitionState.onFailure(wc);
    }

    /**
     * Takes a record as work and puts it into internal queues, unless it's been previously recorded as completed as per
     * loaded records.
     */
    void maybeRegisterNewRecordAsWork(final EpochAndRecordsMap<K, V> recordsMap) {
        log.debug("Incoming {} new records...", recordsMap.count());
        for (var recordsAndEpoch : recordsMap.getRecordMap().values()) {
            PartitionState<K, V> partitionState = getPartitionState(recordsAndEpoch);
            partitionState.maybeRegisterNewPollBatchAsWork(recordsAndEpoch);
        }
    }

    public Map<TopicPartition, OffsetAndMetadata> collectDirtyCommitData() {
        var dirties = new HashMap<TopicPartition, OffsetAndMetadata>();
        for (var state : getAssignedPartitions().values()) {
            var offsetAndMetadata = state.getCommitDataIfDirty();
            //noinspection ObjectAllocationInLoop
            offsetAndMetadata.ifPresent(andMetadata -> dirties.put(state.getTp(), andMetadata));
        }
        return dirties;
    }

    private Map<TopicPartition, PartitionState<K, V>> getAssignedPartitions() {
        return Collections.unmodifiableMap(this.partitionStates.entrySet().stream()
                .filter(e -> !e.getValue().isRemoved())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    /**
     * @return true if this record be taken from its partition as work.
     */
    public boolean couldBeTakenAsWork(WorkContainer<K, V> workContainer) {
        return getPartitionState(workContainer)
                .couldBeTakenAsWork(workContainer);
    }

    public boolean isDirty() {
        return this.partitionStates.values().stream()
                .anyMatch(PartitionState::isDirty);
    }

    private void initMetrics() {
        numberOfPartitionsGauge = pcMetrics.gaugeFromMetricDef(PCMetricsDef.NUMBER_OF_PARTITIONS, this, pm -> getAssignedPartitions().size());
        totalIncompletesGauge = pcMetrics.gaugeFromMetricDef(PCMetricsDef.INCOMPLETE_OFFSETS_TOTAL,
                this, partitionStateManager -> partitionStateManager.getAssignedPartitions().values().stream()
                        .mapToInt(PartitionState::getNumberOfIncompleteOffsets)
                        .sum()
        );
    }
}
