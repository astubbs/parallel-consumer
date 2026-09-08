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
 *     because there is only one inserting thread. <b>The same single-writer fact is recorded independently on
 *     {@code getWorkIfAvailable}'s last-resort sweep</b>, whose own cleared suspicion names the identical
 *     discriminator - so both clearances reopen together the moment anything puts into a shard off the
 *     controller thread, and this class is the only thing that would notice.</li>
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
 * <li><b>The first two each carry a sweep that takes the container out of its SHARD, on the same thread,
 *     before the callback returns</b> - {@code onPartitionsRemoved} does the swap and then
 *     {@code partition.onPartitionsRemoved(sm)} and {@code sm.removeStaleContainers()};
 *     {@code onPartitionsAssigned} ends with {@code sm.removeStaleContainers()}. Arm one asserts exactly
 *     this, and residence is all the argument needs - what is not resident cannot be displaced.</li>
 * <li><b>The fence carries no sweep at all</b> - which is what arm three exploits - but a second container at
 *     the same coordinates requires the offset to be delivered twice, and within one assignment generation the
 *     consumer's position never goes backwards: nothing in main calls {@code seek}, and shards are
 *     partition-scoped in every ordering mode ({@code ShardKey.KeyOrderedKey} owns that reasoning). A
 *     re-delivery therefore needs a re-assignment, which is transitions one and two, sweeps included.</li>
 * </ol>
 * <b>Why astubbs/parallel-consumer#481 does not move any of this, and is a second answer besides.</b> That PR
 * takes the rebalance callbacks off the retry queue - they remove from the shards only, and
 * {@code ShardManager.purgeDepartedRetryEntries()} collects departed entries on the controller thread one
 * pass later. Leg five was originally written as "removes it from both structures"; the queue half was never
 * the part carrying it, and arm one now asserts the two removals in the order that PR establishes. Coming the
 * other way, the purge means that even a displacement orphan would be collected within one control-loop tick
 * - so the harm is bounded whatever happens, and this class says the case does not arise. Both are worth
 * keeping: arm three asserts them together.
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
     * stale, and assert it stops being a RESIDENT before the poll that could supply a replacement can run.
     * <p>
     * <b>Residence is what the assertions are about, and that is the point.</b> Since
     * astubbs/parallel-consumer#481 the rebalance callbacks touch the shards only - the queue entry survives
     * them and is collected by {@code ShardManager.purgeDepartedRetryEntries()} on the controller's next pass.
     * This arm asserts both halves in the order that PR establishes: gone from the shard inside the callback,
     * gone from the queue after one controller pass. The displacement branch needs a resident to displace, so
     * the one-tick queue-only window is not a way into it.
     * <p>
     * The load-bearing assertion is the first, and it is about ordering rather than outcomes: the transition
     * that makes {@code A} stale and the sweep that unseats it are the same method on the same thread, so a
     * fresh record at the same offset - deliverable only after the partition is re-assigned - cannot arrive
     * while {@code A} is still there to displace.
     */
    @Test
    void aRebalanceUnseatsTheResidentBeforeAnyReplacementCanArrive() {
        WorkContainer<String, String> a = aFailedRecordRestingInBothStructures();

        wm.onPartitionsRevoked(UniLists.of(tp));

        assertWithMessage("the revocation must have taken the container out of its shard - the removed-state "
                + "swap that makes it stale and the sweep that unseats it are the same callback. This is the "
                + "whole disproof: what is not resident cannot be displaced")
                .that(residentAtOffset())
                .isNull();
        assertWithMessage("PRECONDITION for the next assertion, and the shape astubbs/parallel-consumer#481 "
                + "establishes: the callback leaves the queue entry alone rather than pairing the removal, so "
                + "it is still here and is the controller's to collect")
                .that(retryQueueContents())
                .containsExactly(a);

        wm.onPartitionsAssigned(UniLists.of(tp));

        assertWithMessage("the re-assignment must not put the container back in a shard")
                .that(residentAtOffset())
                .isNull();

        // one controller pass - purgeDepartedRetryEntries() runs at the top of it
        List<WorkContainer<String, String>> takenOnTheControllerPass = sm().getWorkIfAvailable(100);

        assertWithMessage("FIXTURE: the controller pass must find nothing to hand out, or the purge is being "
                + "credited with a removal that selection actually made")
                .that(takenOnTheControllerPass)
                .isEmpty();
        assertWithMessage("one controller pass collects the departed entry - the bound astubbs/parallel-"
                + "consumer#481 states, asserted here because this arm's argument reads it as the reason the "
                + "queue is empty by the time a replacement can arrive")
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

        // The other half of the answer, asserted here because this is the only place the orphan exists to
        // collect: it does not survive one control-loop pass. Unreachability and the bound are independent
        // results, and this arm is where they meet.
        List<WorkContainer<String, String>> takenOnTheControllerPass = sm().getWorkIfAvailable(100);

        assertWithMessage("PRECONDITION, and NOT what was predicted before running this: the fence makes the "
                + "whole partition stale, so the replacement is stale on arrival too and couldBeTakenAsWork "
                + "refuses it. Nothing is handed out, which is why the assertion below is about the bound and "
                + "not about which mechanism delivers it")
                .that(takenOnTheControllerPass)
                .isEmpty();
        assertWithMessage("even the orphan production cannot create does not survive one control-loop pass - "
                + "so the harm is bounded whatever happens, and the arms above say the case does not arise. "
                + "Two independent results, neither implying the other. NOT attributed to a single mechanism: "
                + "under a fence astubbs/parallel-consumer#481's purge collects it at the top of this call AND "
                + "the last-resort stale sweep below would take the same key, so this arm cannot tell them "
                + "apart and does not pretend to - it asserts the bound, which is what the note claims")
                .that(retryQueueContents())
                .isEmpty();
    }
}
