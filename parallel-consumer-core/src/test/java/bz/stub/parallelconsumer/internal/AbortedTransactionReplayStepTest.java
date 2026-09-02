package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.PollContextInternal;
import bz.stub.parallelconsumer.state.WorkContainer;
import bz.stub.parallelconsumer.state.WorkManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The write-locked step of a recovery, driven by hand (KTD4, KTD5): the mailbox is drained before the ledger is
 * replayed, so a result a worker mailboxed after producing into the aborted transaction is put back too. Without the
 * drain that record's offset stays complete and is committed by the replacement for output the broker discarded -
 * and every recovery test that fences on the control thread's own commit is green either way, because their
 * mailboxes are empty when recovery begins.
 */
class AbortedTransactionReplayStepTest {

    private final TopicPartition tp = new TopicPartition("topic", 0);

    @Test
    void theDrainLandsAMailboxedResultBeforeTheReplayPutsTheLedgerBack() {
        var module = PCModuleTestEnv.withHandDrivenProcessor(ParallelConsumerOptions.<String, String>builder()
                .commitMode(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER)
                .build(), false);
        AbstractParallelEoSStreamProcessor<String, String> pc = module.pc();
        WorkManager<String, String> wm = module.workManager();
        wm.onPartitionsAssigned(UniLists.of(tp));
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        for (long offset = 0; offset <= 1; offset++) {
            records.add(new ConsumerRecord<>(tp.topic(), tp.partition(), offset, "key-" + offset, "value-" + offset));
        }
        wm.registerWork(new EpochAndRecordsMap<>(new ConsumerRecords<>(UniMaps.of(tp, records)), wm.getPm()));
        List<WorkContainer<String, String>> taken = new ArrayList<>(wm.getWorkIfAvailable());
        assertWithMessage("fixture").that(taken).hasSize(2);
        taken.forEach(WorkContainer::onUserFunctionSuccess);
        // offset 0's result was landed by an earlier pass; offset 1's is still in the mailbox when recovery begins
        wm.handleFutureResult(taken.get(0));
        pc.addToMailbox(new PollContextInternal<>(UniLists.of(taken.get(1))), taken.get(1));
        assertWithMessage("fixture: offset 1 is in flight until the drain lands it").that(taken.get(1).isInFlight()).isTrue();

        int restored = pc.replayWorkDiscardedByAbortedTransaction();

        assertWithMessage("both records went back, the mailboxed one included").that(restored).isEqualTo(2);
        assertThat(wm.getPm().getPartitionState(tp).getAllIncompleteOffsets()).containsExactly(0L, 1L);
        assertWithMessage("no offset of the aborted transaction can be committed")
                .that(wm.collectCommitDataForDirtyPartitions().get(tp).offset()).isEqualTo(0);
    }
}
