package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.internal.EpochAndRecordsMap;
import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * R13 / KTD5: what {@link PartitionState} retains so that a record whose output an aborted transaction discarded can
 * run again, and what the replay puts back.
 */
class PartitionStateAbortedTransactionReplayTest {

    private final TopicPartition tp = new TopicPartition("topic", 0);

    private WorkManager<String, String> workManagerIn(CommitMode mode, ProcessingOrder ordering) {
        var module = new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder()
                .commitMode(mode)
                .ordering(ordering)
                .build());
        var wm = module.workManager();
        wm.onPartitionsAssigned(UniLists.of(tp));
        return wm;
    }

    private WorkManager<String, String> transactionalWorkManager() {
        return workManagerIn(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER, ProcessingOrder.UNORDERED);
    }

    private void register(WorkManager<String, String> wm, long fromOffset, long toOffsetInclusive) {
        List<ConsumerRecord<String, String>> records = LongStream.rangeClosed(fromOffset, toOffsetInclusive)
                .mapToObj(offset -> new ConsumerRecord<>(tp.topic(), tp.partition(), offset, "key-" + offset, "value-" + offset))
                .collect(Collectors.toList());
        wm.registerWork(new EpochAndRecordsMap<>(new ConsumerRecords<>(UniMaps.of(tp, records)), wm.getPm()));
    }

    /** Takes every selectable container and completes it successfully, the way a worker and the control thread do. */
    private List<WorkContainer<String, String>> takeAndSucceedAll(WorkManager<String, String> wm) {
        List<WorkContainer<String, String>> taken = new ArrayList<>(wm.getWorkIfAvailable());
        for (var wc : taken) {
            wc.onUserFunctionSuccess();
            wm.handleFutureResult(wc);
        }
        return taken;
    }

    private Map<TopicPartition, OffsetAndMetadata> collectAndCommit(WorkManager<String, String> wm) {
        var collected = wm.collectCommitDataForDirtyPartitions();
        wm.onOffsetCommitSuccess(collected);
        return collected;
    }

    private PartitionState<String, String> stateOf(WorkManager<String, String> wm) {
        return wm.getPm().getPartitionState(tp);
    }

    /**
     * A {@link ShardManager} that plays a concurrent scanner on the replay: the container the replay has just
     * published is selected and completed before control returns to the loop, the seam
     * {@code PartitionStateRegistrationOrder370Test} uses on the poll path.
     */
    static class CompletingOnPublishShardManager extends ShardManager<String, String> {
        private final WorkManager<String, String> wm;
        private boolean armed;
        private boolean fired;

        CompletingOnPublishShardManager(PCModule<String, String> module, WorkManager<String, String> wm) {
            super(module, wm);
            this.wm = wm;
        }

        @Override
        void addWorkContainer(long epochOfInboundRecords, ConsumerRecord<String, String> aRecord) {
            super.addWorkContainer(epochOfInboundRecords, aRecord);
            if (!armed) {
                return;
            }
            armed = false;
            fired = true;
            List<WorkContainer<String, String>> taken = wm.getWorkIfAvailable(1);
            assertWithMessage("seam: the container the replay just published must be the one the scan hands out")
                    .that(taken).hasSize(1);
            WorkContainer<String, String> wc = taken.get(0);
            wc.onUserFunctionSuccess();
            wm.handleFutureResult(wc);
        }
    }

    /**
     * The register-then-publish order astubbs#370 established on the poll path (astubbs#450) holds on the replay
     * too: a record put back into processing is selected and completed the instant it is reachable through its
     * shard, before the loop moves on. On the publish-then-register order {@link PartitionState#onSuccess(long)}'s
     * assert fires from inside the replay - a completion for an offset the partition had not yet re-registered -
     * and without {@code -ea} the offset ends complete and incomplete at once. Latent today for the same reason it
     * was latent on the poll path: one control thread does both. The seam is checked to have fired, so a test that
     * never raced would not pass by accident.
     */
    @Test
    void aCompletionTheInstantAReplayedRecordIsPublishedIsAValidCompletion() {
        var installed = new CompletingOnPublishShardManager[1];
        var module = new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder()
                .commitMode(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER)
                .ordering(ProcessingOrder.UNORDERED)
                .build()) {
            @Override
            protected ShardManager<String, String> createShardManager(WorkManager<String, String> workManagerInstance) {
                installed[0] = new CompletingOnPublishShardManager(this, workManagerInstance);
                return installed[0];
            }
        };
        var wm = module.workManager();
        wm.onPartitionsAssigned(UniLists.of(tp));
        assertWithMessage("fixture: the module built the racing shard manager").that(installed[0]).isNotNull();
        register(wm, 0, 0);
        takeAndSucceedAll(wm);
        assertWithMessage("fixture: the record is in the ledger, uncommitted").that(stateOf(wm).getAllIncompleteOffsets()).isEmpty();
        installed[0].armed = true;

        int restored = wm.restoreWorkDiscardedByAbortedTransaction();

        assertWithMessage("seam fired: the scanner completed the record inside the replay").that(installed[0].fired).isTrue();
        assertThat(restored).isEqualTo(1);
        assertWithMessage("completed during the replay, so not incomplete after it").that(stateOf(wm).getAllIncompleteOffsets()).isEmpty();
        assertWithMessage("and its completion was registered where the commit reads it")
                .that(stateOf(wm).getOffsetHighestSucceeded()).isEqualTo(0L);
    }

    /**
     * Covers AE8.
     */
    @Test
    void replayPutsEveryCompletedButUncommittedRecordBackAsIncompleteAndSelectableWork() {
        var wm = transactionalWorkManager();
        register(wm, 0, 4);
        var originals = takeAndSucceedAll(wm);
        assertWithMessage("fixture: all five completed").that(stateOf(wm).getAllIncompleteOffsets()).isEmpty();
        assertThat(stateOf(wm).getOffsetToCommit()).isEqualTo(5);

        int restored = wm.restoreWorkDiscardedByAbortedTransaction();

        assertThat(restored).isEqualTo(5);
        assertThat(stateOf(wm).getAllIncompleteOffsets()).containsExactly(0L, 1L, 2L, 3L, 4L);
        assertWithMessage("no offset from the aborted transaction can be committed")
                .that(stateOf(wm).getOffsetToCommit()).isEqualTo(0);
        assertThat(stateOf(wm).isDirty()).isTrue();

        var replayed = wm.getWorkIfAvailable();
        assertThat(replayed.stream().map(WorkContainer::offset).collect(Collectors.toList())).containsExactly(0L, 1L, 2L, 3L, 4L);
        long currentEpoch = wm.getPm().getEpochOfPartition(tp);
        for (var wc : replayed) {
            assertWithMessage("a fresh container at the partition's current epoch, not the retired one")
                    .that(wc.getEpoch()).isEqualTo(currentEpoch);
            // identity, not equals: WorkContainer.equals compares by offset, which a replacement shares by design
            assertWithMessage("the retired container is not reused").that(originals.stream().anyMatch(o -> o == wc)).isFalse();
        }
    }

    @Test
    void replayRestoresOnlyWhatCompletedAfterTheLastSuccessfulCommit() {
        var wm = transactionalWorkManager();
        register(wm, 0, 4);
        takeAndSucceedAll(wm);
        collectAndCommit(wm);
        register(wm, 5, 6);
        takeAndSucceedAll(wm);

        int restored = wm.restoreWorkDiscardedByAbortedTransaction();

        assertThat(restored).isEqualTo(2);
        assertThat(stateOf(wm).getAllIncompleteOffsets()).containsExactly(5L, 6L);
        assertThat(stateOf(wm).getOffsetToCommit()).isEqualTo(5);
    }

    @Test
    void aCompletionLandingBetweenCollectionAndCommitSuccessSurvivesForALaterReplay() {
        var wm = transactionalWorkManager();
        register(wm, 0, 3);
        var taken = new ArrayList<>(wm.getWorkIfAvailable());
        for (var wc : taken.subList(0, 3)) {
            wc.onUserFunctionSuccess();
            wm.handleFutureResult(wc);
        }
        var collected = wm.collectCommitDataForDirtyPartitions();
        // offset 3 completes while the commit is in flight - the revoke-path commit runs on the poll thread while
        // the control thread drains its mailbox
        taken.get(3).onUserFunctionSuccess();
        wm.handleFutureResult(taken.get(3));
        wm.onOffsetCommitSuccess(collected);

        int restored = wm.restoreWorkDiscardedByAbortedTransaction();

        assertThat(restored).isEqualTo(1);
        assertThat(stateOf(wm).getAllIncompleteOffsets()).containsExactly(3L);
    }

    @Test
    void replayOnAnEmptyLedgerIsANoOp() {
        var wm = transactionalWorkManager();
        register(wm, 0, 1);
        takeAndSucceedAll(wm);
        collectAndCommit(wm);
        assertThat(stateOf(wm).isDirty()).isFalse();

        int restored = wm.restoreWorkDiscardedByAbortedTransaction();

        assertThat(restored).isEqualTo(0);
        assertThat(stateOf(wm).getAllIncompleteOffsets()).isEmpty();
        assertThat(stateOf(wm).isDirty()).isFalse();
        assertThat(wm.getWorkIfAvailable()).isEmpty();
    }

    @Test
    void nothingIsRetainedOutsideTransactionalCommitMode() {
        var wm = workManagerIn(CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS, ProcessingOrder.UNORDERED);
        register(wm, 0, 4);
        takeAndSucceedAll(wm);

        int restored = wm.restoreWorkDiscardedByAbortedTransaction();

        assertThat(restored).isEqualTo(0);
        assertThat(stateOf(wm).getAllIncompleteOffsets()).isEmpty();
    }

    @Test
    void underKeyOrderingAShardRemovedAfterTheSuccessIsRecreatedByTheReplay() {
        var wm = workManagerIn(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER, ProcessingOrder.KEY);
        register(wm, 0, 2);
        takeAndSucceedAll(wm);
        assertWithMessage("fixture: KEY ordering garbage-collects the emptied per-key shards")
                .that(wm.getSm().getNumberOfRecordsInShards()).isEqualTo(0);

        int restored = wm.restoreWorkDiscardedByAbortedTransaction();

        assertThat(restored).isEqualTo(3);
        assertThat(wm.getWorkIfAvailable().stream().map(WorkContainer::offset).collect(Collectors.toList()))
                .containsExactly(0L, 1L, 2L);
    }
}
