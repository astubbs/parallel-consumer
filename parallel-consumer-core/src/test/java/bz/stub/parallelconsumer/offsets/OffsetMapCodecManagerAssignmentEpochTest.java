package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Pins the contract between {@code PartitionStateManager.getEpochOfPartition}, which is documented "or null if not
 * yet assigned", and the two places {@link OffsetMapCodecManager#loadPartitionStateForAssignment} unboxes that value
 * into {@link bz.stub.parallelconsumer.state.PartitionState}'s primitive {@code long} epoch.
 * <p>
 * <b>Null is unreachable there from the production caller</b> - {@code PartitionStateManager.onPartitionsAssigned}
 * writes every partition's epoch, unconditionally and on the same thread, before it calls
 * {@code loadPartitionStateForAssignment} with the same collection, and nothing ever removes an epoch. Both unbox
 * sites only look up partitions from that collection: {@code Consumer.committed(Set)} returns keys drawn from the set
 * it was asked about, and the default-entry pass iterates the assignment itself. The contract is therefore narrowed
 * rather than made lenient: a caller that reaches either site without an epoch has broken the assignment ordering,
 * and the failure names that ordering instead of surfacing as an auto-unbox {@code NullPointerException} whose
 * message names a local variable.
 * <p>
 * <b>Before the fix, both fail-closed tests were RED</b>: the same {@code NullPointerException} type was thrown, but
 * from the unbox, with the JDK's helpful message ({@code Cannot invoke "java.lang.Long.longValue()" because "epoch"
 * is null}) - which says nothing about which invariant was broken or by whom. The control test is green either way:
 * it proves the fixture reaches both sites and that the guard is silent when the epoch exists.
 * <p>
 * Reaching the sites without an epoch is deliberate misuse - calling the codec manager for a partition the state
 * manager was never told about - which is exactly the shape a future refactor of the assignment order would
 * produce, and exactly what the guard exists to name.
 *
 * @author Antony Stubbs
 * @see OffsetMapCodecManager#epochOfPartitionBeingAssigned
 */
class OffsetMapCodecManagerAssignmentEpochTest {

    static final TopicPartition TP = new TopicPartition("myTopic", 7);

    /**
     * A committed offset with an empty metadata field: decodes to "nothing incomplete below the committed offset"
     * without touching any codec, so the only thing that can fail in {@code decodePartitionState} is the epoch.
     */
    static final OffsetAndMetadata COMMITTED_WITH_NO_OFFSET_MAP = new OffsetAndMetadata(100L);

    final MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

    final PCModuleTestEnv module = new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder()
            .consumer(consumer)
            .build());

    final OffsetMapCodecManager<String, String> codecManager = new OffsetMapCodecManager<>(module);

    /**
     * The default-entry pass - a partition with no commit history, which is the one the retired Infer identity
     * {@code lambda$loadPartitionStateForAssignment$2} named. Nothing is committed on the mock consumer, so
     * {@code committed(Set)} returns nothing for the partition and the default entry is the path taken.
     */
    @Test
    void theDefaultEntryFailsClosedNamingTheInvariantWhenNoEpochWasRecorded() {
        assertWithMessage("precondition: the partition has no epoch, because onPartitionsAssigned never ran")
                .that(module.workManager().getPm().getEpochOfPartition(TP)).isNull();

        var thrown = assertThrows(NullPointerException.class,
                () -> codecManager.loadPartitionStateForAssignment(UniLists.of(TP)));

        assertInvariantIsNamed(thrown);
    }

    /**
     * The decode pass - a partition WITH commit history, the site Infer originally reported. Reached directly,
     * because the mock consumer only answers {@code committed(Set)} for partitions it has been assigned, and the
     * point of this test is a partition the state manager has NOT been told about.
     */
    @Test
    void theDecodedEntryFailsClosedNamingTheInvariantWhenNoEpochWasRecorded() {
        var thrown = assertThrows(NullPointerException.class,
                () -> codecManager.decodePartitionState(TP, COMMITTED_WITH_NO_OFFSET_MAP));

        assertInvariantIsNamed(thrown);
    }

    /**
     * Control: with the epoch written the way {@code onPartitionsAssigned} writes it, both sites build a state carrying
     * that epoch and the guard is silent. Green before and after the fix, so a failure above is the guard's and not the
     * fixture's.
     */
    @Test
    void bothSitesBuildStateCarryingTheEpochOnceItIsRecorded() throws OffsetDecodingError {
        module.workManager().onPartitionsAssigned(UniLists.of(TP));
        long epoch = module.workManager().getPm().getEpochOfPartition(TP);

        var loaded = codecManager.loadPartitionStateForAssignment(UniLists.of(TP));
        assertWithMessage("the default entry carries the recorded epoch")
                .that(loaded.get(TP).getPartitionsAssignmentEpoch()).isEqualTo(epoch);

        var decoded = codecManager.decodePartitionState(TP, COMMITTED_WITH_NO_OFFSET_MAP);
        assertWithMessage("the decoded entry carries the recorded epoch")
                .that(decoded.getPartitionsAssignmentEpoch()).isEqualTo(epoch);
    }

    private static void assertInvariantIsNamed(NullPointerException thrown) {
        assertWithMessage("the failure must name the partition, not a local variable")
                .that(thrown).hasMessageThat().contains(TP.toString());
        assertWithMessage("the failure must name the ordering that was broken")
                .that(thrown).hasMessageThat().contains("PartitionStateManager.onPartitionsAssigned");
        assertThat(thrown).hasMessageThat().contains("epoch");
    }
}
