package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.EpochAndRecordsMap;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.ArrayList;
import java.util.List;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Is {@link ProcessingShard#addWorkContainer}'s displacement branch reachable in production <b>with the
 * displaced container holding a retry-queue entry</b>?
 * <p>
 * <b>No, and the discriminator is not in this class.</b> The branch itself is reachable - confluentinc#909's
 * late drain puts a stale resident in a shard and the next poll's fresh record displaces it, which is what
 * {@link ProcessingShardStaleReplacement909Test} covers. What cannot happen is that resident also being in
 * the {@link RetryQueue}, and the three arms below are the control-armed form of that claim: the shape that
 * production drives (green, and the sweep is what makes it green), the shape that reaches the branch without
 * a queue entry (so the first arm's green is not "the branch never fires"), and the shape that <em>would</em>
 * orphan an entry (so the first arm's assertions are known to be able to see one).
 * <p>
 * <b>The argument, each leg checked in source rather than inferred.</b> An orphan needs four things true at
 * once when a container {@code B} arrives at {@code (topic, partition, offset)}: a resident {@code A} at that
 * offset; {@code A} observed <em>stale</em> (otherwise the arrival is dropped); {@code A} holding a retry-queue
 * entry; and {@code B} existing at all.
 * <ol>
 * <li><b>The displaced container is always the one that was inspected.</b> {@code workMap} has two insertion
 *     sites - {@code addWorkContainer}, which is control-thread-only (the poll thread's {@code registerWork}
 *     only posts to the mailbox; {@code maybeRegisterNewPollBatchAsWork}'s thread-model javadoc states it),
 *     and {@code plantResident}, which has no production caller. Only removals can interleave between the
 *     {@code get} and the {@code put}, and a removal makes the {@code put} return null. So the staleness read
 *     is about the container that is actually displaced. This is the one leg astubbs#468's defect class does
 *     NOT reach here: a by-key removal can hit a different occupant, but a by-key <em>insertion</em> cannot,
 *     because there is only one inserting thread.</li>
 * <li><b>A queue entry implies non-stale-and-resident when it was made.</b> {@code RetryQueue.add} has exactly
 *     one production caller, {@link ShardManager#onFailure}, reached only from {@code WorkManager.onFailureResult}
 *     behind its live {@code checkIfWorkIsStale} check - and since astubbs#437 followed by a residency
 *     confirmation that undoes the add if the container has left.</li>
 * <li><b>Staleness is monotone per container.</b> {@code WorkContainer.epoch} and
 *     {@code PartitionState.partitionsAssignmentEpoch} are both {@code final long}, {@code fencedForRevocation}
 *     is set and never cleared, and a replaced state carries a strictly higher epoch or is the removed
 *     singleton.</li>
 * <li><b>So A must cross the staleness boundary between (2) and the displacement, and exactly three
 *     transitions can do that</b> - the removed-singleton swap in {@code resetOffsetMapAndRemoveWork}, the
 *     {@code putAll} in {@code onPartitionsAssigned}, and {@link PartitionState#fenceForRevocation}. Bumping
 *     the manager's epoch map alone does not: the state's own epoch is final and is only ever consulted
 *     through the state object.</li>
 * <li><b>The first two each carry a paired sweep, on the same thread, before the callback returns</b> -
 *     {@code onPartitionsRemoved} does the swap and then {@code partition.onPartitionsRemoved(sm)} and
 *     {@code sm.removeStaleContainers()}; {@code onPartitionsAssigned} ends with
 *     {@code sm.removeStaleContainers()}. Arm one asserts exactly this.</li>
 * <li><b>The fence carries no sweep at all</b> - which is what arm three exploits - but a second container at
 *     the same coordinates requires the offset to be delivered twice, and within one assignment generation the
 *     consumer's position never goes backwards: nothing in main calls {@code seek}, and shards are
 *     partition-scoped in every ordering mode ({@code ShardKey.KeyOrderedKey} owns that reasoning). A
 *     re-delivery therefore needs a re-assignment, which is transitions one and two, sweeps included.</li>
 * </ol>
 * <b>What would reopen it</b>, and none of it is guarded here: any in-generation replay of an offset - a
 * {@code seek}, an offset-reset or truncation replay that re-registers already-registered offsets, or a
 * topic-scoped shard key. The guard on the last leg is a property of the Kafka consumer's fetch position, not
 * of {@link ProcessingShard}, so the pairing gap in that branch is one arrival away rather than absent.
 *
 * @author Antony Stubbs
 * @see ProcessingShard#addWorkContainer(WorkContainer)
 * @see ProcessingShardStaleReplacement909Test
 * @see RetryQueueRequeueWindowTest
 */
@Slf4j
class ShardDisplacementOrphanReachabilityTest {

    static final String TOPIC = "myTopic";

    static final long OFFSET = 0L;

    final TopicPartition tp = new TopicPartition(TOPIC, 0);

    final PCModuleTestEnv module = new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder()
            .ordering(PARTITION)
            .consumer(new MockConsumer<>(OffsetResetStrategy.EARLIEST))
            .build());

    final WorkManager<String, String> wm = module.workManager();

    ShardManager<String, String> sm() {
        return wm.getSm();
    }

    PartitionStateManager<String, String> pm() {
        return wm.getPm();
    }

    private ConsumerRecord<String, String> recordAt(long offset) {
        return new ConsumerRecord<>(TOPIC, tp.partition(), offset, "key-" + offset, "value-" + offset);
    }

    private ProcessingShard<String, String> shard() {
        var found = sm().getShard(sm().computeShardKey(recordAt(OFFSET)));
        assertWithMessage("FIXTURE: the shard must exist - under PARTITION ordering it survives being "
                + "emptied, so its absence means the fixture never registered anything")
                .that(found.isPresent())
                .isTrue();
        return found.get();
    }

    private WorkContainer<String, String> residentAtOffset() {
        return shard().getWorkContainerAtOffset(OFFSET).orElse(null);
    }

    /** Every container the retry queue currently holds, by reference, so an orphan can be told from a replacement. */
    private List<WorkContainer<?, ?>> retryQueueContents() {
        var contents = new ArrayList<WorkContainer<?, ?>>();
        try (RetryQueue.RetryQueueIterator it = sm().getRetryQueue().iterator()) {
            while (it.hasNext()) {
                contents.add(it.next());
            }
        }
        return contents;
    }

    /**
     * The state an orphan needs, minus the staleness: a container that is resident in its shard, parked in the
     * retry queue, and not stale. Built through the production failure path, not planted.
     */
    private WorkContainer<String, String> aFailedRecordRestingInBothStructures() {
        WorkContainer<String, String> wc = ModelUtils.registerOneRecordAndTakeIt(wm, tp);
        wc.onUserFunctionFailure(new RuntimeException("simulated user function failure"));
        wm.handleFutureResult(wc);

        assertWithMessage("FIXTURE: the failed container must still be the shard's resident at its offset")
                .that(residentAtOffset())
                .isSameInstanceAs(wc);
        assertWithMessage("FIXTURE: the failed container must be parked in the retry queue")
                .that(retryQueueContents())
                .containsExactly(wc);
        assertWithMessage("FIXTURE: it must NOT be stale yet - a stale container can never be taken as work "
                + "(PartitionState.couldBeTakenAsWork) and so can never reach the retry queue at all")
                .that(wm.checkIfWorkIsStale(wc))
                .isFalse();
        return wc;
    }

    /**
     * <b>The disproof.</b> Drive the only production sequence that can turn a queued, resident container
     * stale, and assert that it leaves both structures before the poll that could supply a replacement can
     * even run.
     * <p>
     * The load-bearing assertion is the middle one, and it is about ordering rather than about outcomes: the
     * transition that makes {@code A} stale and the sweep that removes it are the same method on the same
     * thread, inside the rebalance callback, so a fresh record at the same offset - which can only be
     * delivered after the partition is re-assigned - cannot arrive while {@code A} is still there to displace.
     */
    @Test
    void aRebalanceClearsBothStructuresBeforeAnyReplacementCanArrive() {
        WorkContainer<String, String> a = aFailedRecordRestingInBothStructures();

        wm.onPartitionsRevoked(UniLists.of(tp));

        assertWithMessage("the revocation must have taken the container out of its shard - the removed-state "
                + "swap that makes it stale and the sweep that removes it are the same callback")
                .that(residentAtOffset())
                .isNull();
        assertWithMessage("and out of the retry queue in the same callback - this is the pairing the "
                + "displacement branch cannot do for itself, done here by the sweep that owns the departure")
                .that(retryQueueContents())
                .isEmpty();

        wm.onPartitionsAssigned(UniLists.of(tp));

        assertWithMessage("the re-assignment must not resurrect either half")
                .that(residentAtOffset())
                .isNull();
        assertWithMessage("the re-assignment must not resurrect the queue entry")
                .that(retryQueueContents())
                .isEmpty();

        // only NOW can the partition's next poll redeliver the offset - and it finds nothing to displace
        wm.registerWork(new EpochAndRecordsMap<>(ModelUtils.pollOf(tp, OFFSET), pm()));

        assertWithMessage("the redelivered record is an ordinary insertion, not a displacement")
                .that(residentAtOffset())
                .isNotSameInstanceAs(a);
        assertWithMessage("PRECONDITION: the redelivery must actually have registered, or this arm proves "
                + "nothing about what the replacement finds")
                .that(residentAtOffset())
                .isNotNull();
        assertWithMessage("no orphan: nothing was displaced, so nothing was left behind in the queue")
                .that(retryQueueContents())
                .isEmpty();
    }

    /**
     * <b>Control arm - the branch DOES fire in production, just never with a queued container.</b> Without
     * this, the arm above is green for two indistinguishable reasons: the sweep works, or the displacement
     * branch is simply unreachable and the whole question is moot.
     * <p>
     * This is confluentinc#909's late drain, entered at {@link ShardManager#addWorkContainer} because that is
     * the layer the race delivers to - {@code maybeRegisterNewPollBatchAsWork}'s {@code epochIsStale} guard is
     * precisely what the race defeats, by being called on a {@link PartitionState} that has itself gone stale
     * and therefore compares its own captured epoch against the batch's matching one.
     */
    @Test
    void aStaleLateArrivalIsDisplacedButHasNoQueueEntryToOrphan() {
        wm.onPartitionsAssigned(UniLists.of(tp));
        long epochBefore = pm().getEpochOfPartition(tp);

        wm.onPartitionsRevoked(UniLists.of(tp));
        wm.onPartitionsAssigned(UniLists.of(tp));
        long epochAfter = pm().getEpochOfPartition(tp);
        assertWithMessage("PRECONDITION: the rebalance must have advanced the epoch, or nothing here is stale")
                .that(epochAfter)
                .isGreaterThan(epochBefore);

        // the late drain: a batch snapshotted before the rebalance, inserted after both sweeps have passed
        sm().addWorkContainer(epochBefore, recordAt(OFFSET));
        WorkContainer<String, String> staleResident = residentAtOffset();

        assertWithMessage("PRECONDITION: the late arrival must be resident and stale, or the next add is not "
                + "a displacement")
                .that(wm.checkIfWorkIsStale(staleResident))
                .isTrue();
        assertWithMessage("a container that has never been selected as work cannot be in the retry queue - "
                + "PartitionState.couldBeTakenAsWork refuses a stale container, so it never will be either")
                .that(retryQueueContents())
                .isEmpty();

        sm().addWorkContainer(epochAfter, recordAt(OFFSET));

        assertWithMessage("the displacement branch fired: the fresh container replaced the stale resident")
                .that(residentAtOffset())
                .isNotSameInstanceAs(staleResident);
        assertWithMessage("and there was no entry to orphan")
                .that(retryQueueContents())
                .isEmpty();
    }

    /**
     * <b>Positive control - the orphan the note describes, and the exact production input that is missing.</b>
     * <p>
     * {@link PartitionState#fenceForRevocation} is the one staleness transition in the engine with no paired
     * sweep: it is set on the control thread inside the produce lock, and truncation follows later on the poll
     * thread. So (stale AND resident AND queued) is genuinely reachable through the production API - this arm
     * builds it with no white-box planting at all. What production cannot then supply is the second container
     * at the same coordinates, because the offset has already been delivered in this assignment generation and
     * the consumer's position does not go backwards without a re-assignment.
     * <p>
     * Injecting that one arrival is what this arm adds, and the orphan appears immediately: the retry queue
     * goes on holding the <em>displaced</em> instance while the shard holds its replacement. That makes the
     * first arm's assertions demonstrably able to see an orphan, and it names what would make this reachable -
     * an in-generation replay of an already-registered offset.
     */
    @Test
    void injectingTheOneArrivalProductionCannotSupplyDoesOrphanTheEntry() {
        WorkContainer<String, String> a = aFailedRecordRestingInBothStructures();
        long epoch = pm().getEpochOfPartition(tp);

        wm.fenceForRevocation(UniMaps.of(tp, epoch));

        assertWithMessage("PRECONDITION: the fence must make the resident stale without any sweep having run")
                .that(wm.checkIfWorkIsStale(a))
                .isTrue();
        assertWithMessage("PRECONDITION: and it must still be resident - the fence removes nothing")
                .that(residentAtOffset())
                .isSameInstanceAs(a);
        assertWithMessage("PRECONDITION: and still parked in the retry queue")
                .that(retryQueueContents())
                .containsExactly(a);

        // the arrival production cannot deliver here: a second container at coordinates already delivered
        sm().addWorkContainer(epoch, recordAt(OFFSET));

        assertWithMessage("PRECONDITION: the displacement branch must have fired")
                .that(residentAtOffset())
                .isNotSameInstanceAs(a);
        assertWithMessage("THE ORPHAN: the retry queue still holds the DISPLACED instance, which is now "
                + "resident in no shard - the displacement branch retires it and releases its selection claim "
                + "but cannot remove it from a queue it has no handle on")
                .that(retryQueueContents())
                .containsExactly(a);
        assertWithMessage("and the entry is by reference the departed container, not the replacement - which "
                + "is why the ready-to-retry figure reads the DISPLACED container's retry-due time")
                .that(shard().isResident(retryQueueContents().get(0)))
                .isFalse();
    }
}
