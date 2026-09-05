package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.EpochAndRecordsMap;
import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.List;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * A record's offset is registered as incomplete BEFORE the record is published to a shard, so a scanner that
 * selects and completes it the instant it becomes reachable finds the state that makes that completion valid
 * (astubbs/parallel-consumer#370).
 * <p>
 * <b>The defect.</b> {@link PartitionState#maybeRegisterNewPollBatchAsWork} used to call
 * {@code ShardManager#addWorkContainer} first and {@link PartitionState#addNewIncompleteRecord} second, so
 * between the two the container was visible to every shard scan while its offset was absent from
 * {@code incompleteOffsets}. A completion landing in that gap hits {@link PartitionState#onSuccess(long)}'s
 * {@code assert removedFromIncompletes} - the one shape that fires it with no double delivery at all. Without
 * {@code -ea} it is worse than a crash: the removal silently removes nothing, the second registration then
 * puts the already-completed offset INTO the incomplete set, and nothing will ever complete it again, because
 * the shard removed its container on success. The commit frontier sits below that offset for the life of the
 * assignment, and the next rebalance redelivers everything above it.
 * <p>
 * <b>Why it was latent.</b> On the shipped engine registration and completion both reach {@link PartitionState}
 * from the control thread's mailbox drain, one after the other, so no completion can land inside a
 * registration - the gap exists in program order but no thread can enter it. That is a property of today's
 * callers, not of the class: the direct-pull engine gives every worker its own scanner, and there the gap is
 * reachable. The fix removes the gap rather than defending the invariant that happened to close it.
 * <p>
 * <b>The seam.</b> {@link CompletingOnPublishShardManager} is the {@link ShardManager} the module builds, and
 * on the armed insert it does what a concurrent scanner would: selects the container it has just published
 * through the real selection path and completes it through {@code WorkManager#handleFutureResult}, all before
 * returning to the registration loop. Same shape as {@code WorkManagerStaleCheckDoubleLookupTest}'s racing
 * double - the interleaving played out by hand on one thread, exact and in milliseconds, where a timing test
 * would need a second engine to reach it at all. Installed through {@link PCModule#createShardManager} so the
 * production class carries no test hook.
 * <p>
 * <b>RED on the unfixed code</b>: {@code wm.registerWork} throws the {@link AssertionError} from
 * {@link PartitionState#onSuccess(long)} out of the registration loop. GREEN with the order swapped, with the
 * seam still firing - {@link #aCompletionTheInstantARecordIsPublishedIsAValidCompletion} checks that, so a
 * seam that goes dead cannot pass by never running.
 *
 * @author Antony Stubbs
 * @see PartitionState#maybeRegisterNewPollBatchAsWork
 */
@Slf4j
class PartitionStateRegistrationOrder370Test {

    static final String TOPIC = "registration-order-topic";
    static final TopicPartition TP = new TopicPartition(TOPIC, 0);

    /**
     * A {@link ShardManager} that, once armed, plays the concurrent scanner: the container it has just published
     * is selected and completed before control returns to the registration loop. Firing is tracked in an explicit
     * boolean set at firing time, because a cleared armed-slot cannot tell "armed, then fired" from "never
     * armed", and a guard built on it would pass a test that forgot to arm.
     */
    static class CompletingOnPublishShardManager extends ShardManager<String, String> {

        private final WorkManager<String, String> wm;
        private boolean armed;
        private boolean fired;
        private WorkContainer<String, String> completed;

        CompletingOnPublishShardManager(PCModule<String, String> module, WorkManager<String, String> wm) {
            super(module, wm);
            this.wm = wm;
        }

        void armCompletionOnNextPublish() {
            this.armed = true;
            this.fired = false;
            this.completed = null;
        }

        boolean completionFired() {
            return fired;
        }

        WorkContainer<String, String> completedContainer() {
            return completed;
        }

        @Override
        void addWorkContainer(long epochOfInboundRecords, ConsumerRecord<String, String> aRecord) {
            super.addWorkContainer(epochOfInboundRecords, aRecord);
            if (!armed) {
                return;
            }
            armed = false;
            fired = true;

            // the scanner: the record is reachable now, so a selector takes it through the real path
            List<WorkContainer<String, String>> taken = wm.getWorkIfAvailable(1);
            assertWithMessage("seam: the container just published must be the one the scan hands out, or the "
                    + "interleaving under test never starts")
                    .that(taken).hasSize(1);
            WorkContainer<String, String> wc = taken.get(0);
            assertThat(wc.offset()).isEqualTo(aRecord.offset());

            // and its verdict comes back before the registration loop has moved on
            wc.onUserFunctionSuccess();
            wm.handleFutureResult(wc);
            completed = wc;
        }
    }

    final CompletingOnPublishShardManager[] installed = new CompletingOnPublishShardManager[1];

    final PCModuleTestEnv module = new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder()
            .ordering(UNORDERED)
            .consumer(new MockConsumer<>(OffsetResetStrategy.EARLIEST))
            .build()) {
        @Override
        protected ShardManager<String, String> createShardManager(WorkManager<String, String> workManagerInstance) {
            var sm = new CompletingOnPublishShardManager(this, workManagerInstance);
            installed[0] = sm;
            return sm;
        }
    };

    final WorkManager<String, String> wm = module.workManager();

    final ConsumerRecord<String, String> record = new ConsumerRecord<>(TOPIC, 0, 0L, "key-0", "value-0");

    {
        wm.onPartitionsAssigned(UniLists.of(TP));
    }

    private void registerTheRecord() {
        var records = new ConsumerRecords<>(UniMaps.of(TP, UniLists.of(record)));
        wm.registerWork(new EpochAndRecordsMap<>(records, wm.getPm()));
    }

    /**
     * THE INTERLEAVING: the record is selected and completed the instant it is reachable through a shard, before
     * {@link PartitionState#maybeRegisterNewPollBatchAsWork} has finished registering it. RED on the unfixed
     * order - the {@link AssertionError} from {@link PartitionState#onSuccess(long)} escapes
     * {@code registerWork}; without {@code -ea} the assertions below fail instead, on an offset that is complete
     * and incomplete at once.
     */
    @Test
    void aCompletionTheInstantARecordIsPublishedIsAValidCompletion() {
        CompletingOnPublishShardManager sm = installed[0];
        assertWithMessage("fixture: the module must have built the racing shard manager").that(sm).isNotNull();
        sm.armCompletionOnNextPublish();

        registerTheRecord();

        assertWithMessage("the armed completion must actually have fired inside registration - a seam that "
                + "never runs proves nothing")
                .that(sm.completionFired()).isTrue();

        PartitionState<String, String> state = wm.getPm().getPartitionState(TP);
        assertWithMessage("a record completed once is complete: it must not be back in the incomplete set "
                + "because its registration finished after its completion")
                .that(state.getNumberOfIncompleteOffsets()).isEqualTo(0);
        assertWithMessage("the completion moved the commit frontier past the record")
                .that(state.getOffsetHighestSequentialSucceeded()).isEqualTo(record.offset());
        assertWithMessage("a second registration of the same record must be recognised as already done")
                .that(state.isRecordPreviouslyCompleted(record)).isTrue();
        assertWithMessage("delivered exactly once")
                .that(sm.completedContainer().getDeliveryCount()).isEqualTo(1L);
        assertWithMessage("nothing is left in flight")
                .that(wm.getNumberRecordsOutForProcessing()).isEqualTo(0);
    }

    /**
     * The control: with the seam unarmed the same registration goes through the same double untouched, and the
     * record is incomplete and selectable, as a fresh record should be. Without this a fix that made the seam
     * vacuous - a shard manager that dropped the record, say - would pass the test above.
     */
    @Test
    void aRecordRegisteredWithoutInterferenceIsIncompleteAndSelectable() {
        CompletingOnPublishShardManager sm = installed[0];
        assertWithMessage("fixture: the module must have built the racing shard manager").that(sm).isNotNull();

        registerTheRecord();

        assertWithMessage("control: nothing was armed, so nothing may have fired")
                .that(sm.completionFired()).isFalse();
        PartitionState<String, String> state = wm.getPm().getPartitionState(TP);
        assertThat(state.getNumberOfIncompleteOffsets()).isEqualTo(1);
        assertThat(state.isRecordPreviouslyCompleted(record)).isFalse();

        List<WorkContainer<String, String>> taken = wm.getWorkIfAvailable(1);
        assertWithMessage("the registered record is the one selectable container")
                .that(taken).hasSize(1);
        assertThat(taken.get(0).offset()).isEqualTo(record.offset());
    }
}
