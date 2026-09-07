package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.mockito.Mockito.spy;

/**
 * The poller's stale sweep must evict the container it inspected and nothing else - never a fresh container the
 * controller put at that offset while the sweep was deciding.
 * <p>
 * <b>It used to remove by KEY</b> ({@code removeWorkAtOffset(entry.getKey())}), so whatever occupied the offset
 * when the removal landed is what left the shard. The sweep now removes conditionally on the
 * {@code ProcessingShard.Residency} it inspected, in one atomic step.
 * <p>
 * <b>The two sides are genuinely different threads.</b> {@link ProcessingShard#removeStaleWorkContainersFromShard}
 * is reached from {@code PartitionStateManager.onPartitionsRemoved} / {@code onPartitionsAssigned}, i.e. inside a
 * rebalance callback on the broker-poll thread; {@link ProcessingShard#addWorkContainer}'s stale-replacement branch
 * runs on the controller. Nothing orders them.
 * <p>
 * <b>The harm is the lost record, not the counter.</b> The accounting half of this defect is already closed - every
 * exit path retires and releases the claim of whatever the map actually gave up, never of the container the caller
 * was looking at - so the figures below settle correct either way, and are asserted precisely so that a future
 * change cannot trade one half of the defect for the other. What is lost is the record: the fresh container is gone
 * from the shard while {@code PartitionState} still carries its offset as incomplete, so nothing ever selects it
 * again until the partition is re-polled.
 *
 * @author Antony Stubbs
 * @see ShardSeamTestBase
 */
class ShardStaleSweepReplacementEvictionTest extends ShardSeamTestBase {

    private static final long CONTESTED_OFFSET = 100L;

    /**
     * The sweep must remove the container it inspected, and only that one.
     * <p>
     * The interleaving is the one the sweep cannot see: it answers "stale" about the resident, the controller
     * replaces that resident with a fresh container carrying the current epoch, and the sweep's removal - keyed on
     * the offset alone - takes the fresh one out instead.
     */
    @Test
    void theStaleSweepMustNotEvictAFreshReplacementThatLandedInsideIt() {
        var population = new RecordPopulation();
        var seam = spy(wm.getPm());
        var record = recordAt(CONTESTED_OFFSET);
        var shard = shardWith(seam, population, record);

        wm.onPartitionsAssigned(UniLists.of(TP));
        long firstEpoch = wm.getPm().getEpochOfPartition(TP);
        var stale = new WorkContainer<>(firstEpoch, record, module);
        shard.addWorkContainer(stale);

        // the partition is taken away and handed back, so what the shard is still holding is now stale
        wm.onPartitionsRevoked(UniLists.of(TP));
        wm.onPartitionsAssigned(UniLists.of(TP));
        long laterEpoch = wm.getPm().getEpochOfPartition(TP);
        assertWithMessage("PRECONDITION: the rebalance must actually have made the resident stale")
                .that(laterEpoch).isGreaterThan(firstEpoch);

        var fresh = new WorkContainer<>(laterEpoch, record, module);
        var replacementLanded = new AtomicBoolean();

        // THE INTERLEAVING: the controller's stale-replacement lands between the sweep's staleness answer and the
        // removal it drives
        onNextStalenessCheck(seam, () -> {
            shard.addWorkContainer(fresh);
            replacementLanded.set(true);
        });

        var swept = shard.removeStaleWorkContainersFromShard();

        assertWithMessage("PRECONDITION: the replacement must have landed inside the sweep, or this test "
                + "exercises nothing")
                .that(replacementLanded.get()).isTrue();
        assertWithMessage("PRECONDITION: the replacement must have reached the shard, or the sweep had nothing "
                + "to race with")
                .that(fresh.getEpoch()).isEqualTo(laterEpoch);

        // IDENTITY, never equality, in every assertion below. WorkContainer.equals is topic/partition/offset
        // only, so the fresh container compares EQUAL to the stale one it replaced - which is the very reason
        // this defect exists. Truth's hasValue and containsExactly both use equals, so written that way these
        // assertions would pass on the defective behaviour and this test would assert nothing.
        assertWithMessage("the fresh container is the only thing at this offset that is not stale, and the "
                + "partition still carries its offset as incomplete - evicting it loses the record until the "
                + "partition is re-polled")
                .that(shard.getWorkContainerAtOffset(CONTESTED_OFFSET).orElse(null))
                .isSameInstanceAs(fresh);
        assertWithMessage("the sweep reports what IT evicted, and it evicted nothing - the replacement won the "
                + "offset. ShardManager.removeStaleContainers feeds this list to the retry queue, and "
                + "astubbs/parallel-consumer#437 pins that the queue removal is reached only through a real "
                + "shard removal; reporting a container this call did not remove would break that gating and "
                + "would take the entry at coordinates the FRESH container now owns")
                .that(swept).isEmpty();

        assertThat(shard.getCountOfWorkTracked()).isEqualTo(1L);
        assertWithMessage("the counter agrees with the units actually held - the accounting half of this defect "
                + "is already closed and must not be traded back for the eviction half")
                .that(shard.getCountOfWorkAwaitingSelection()).isEqualTo(shard.countSelectionClaimedByScan());
        assertThat(shard.getCountOfWorkAwaitingSelection()).isEqualTo(1L);
        assertWithMessage("one admission survives, because one container is still held")
                .that(population.getInSystem()).isEqualTo(1L);
    }

    /**
     * The control: the same sweep over the same stale resident, with nothing racing it.
     * <p>
     * Without this, "the stale container left the shard" above would also be satisfied by a sweep that removed
     * everything it touched for the wrong reason, and by one that never ran at all. Same magnitude, different
     * position: the only term that changes between the two tests is whether the replacement lands inside the
     * sweep.
     */
    @Test
    void aSweepWithNothingRacingItRemovesTheStaleContainerAndOnlyThat() {
        var population = new RecordPopulation();
        var record = recordAt(CONTESTED_OFFSET);
        var shard = shardWith(wm.getPm(), population, record);

        wm.onPartitionsAssigned(UniLists.of(TP));
        long firstEpoch = wm.getPm().getEpochOfPartition(TP);
        var stale = new WorkContainer<>(firstEpoch, record, module);
        shard.addWorkContainer(stale);

        wm.onPartitionsRevoked(UniLists.of(TP));
        wm.onPartitionsAssigned(UniLists.of(TP));
        assertWithMessage("PRECONDITION: the rebalance must actually have made the resident stale")
                .that(wm.getPm().getEpochOfPartition(TP)).isGreaterThan(firstEpoch);

        var swept = shard.removeStaleWorkContainersFromShard();

        assertThat(swept).hasSize(1);
        assertThat(swept.get(0)).isSameInstanceAs(stale);
        assertThat(shard.getWorkContainerAtOffset(CONTESTED_OFFSET)).isEmpty();
        assertThat(shard.getCountOfWorkTracked()).isEqualTo(0L);
        assertThat(shard.getCountOfWorkAwaitingSelection()).isEqualTo(shard.countSelectionClaimedByScan());
        assertThat(population.getInSystem()).isEqualTo(0L);
    }

    /**
     * The premise the fix rests on, measured rather than assumed: <b>no value-conditional removal keyed on
     * {@link WorkContainer} equality can express "remove the one I inspected"</b>, so the shard stores a
     * {@link ProcessingShard.Residency} token whose equality is reference identity.
     * <p>
     * This is a characterisation of the primitive, not of the JDK for its own sake. Both forms below are the
     * obvious fixes for the defect above, both read as correct, and both silently take the replacement -
     * <b>including the identity check inside {@code computeIfPresent}</b>, because that method commits its
     * removal through {@code doRemove(key, v)}, which re-reads the node value and gates on
     * {@code v.equals(reRead)} before its compare-and-set. The identity test does not survive to the commit.
     * <p>
     * It is also the tripwire for the day {@link WorkContainer#equals(Object)} becomes identity-based: this test
     * goes red, and the token can then be deleted.
     */
    @Test
    // reference equality is the SUBJECT of this test, not an accident of it: the whole point is that the
    // identity comparison below does not survive to the map's commit while equality does
    @SuppressWarnings("ReferenceEquality")
    void noRemovalKeyedOnContainerEqualityCanExpressWhichContainerToRemove() {
        var record = recordAt(CONTESTED_OFFSET);
        var stale = new WorkContainer<>(0L, record, module);
        var fresh = new WorkContainer<>(1L, record, module);
        assertWithMessage("PRECONDITION: the two containers are different objects that compare EQUAL - "
                + "WorkContainer.equals is topic/partition/offset only, which is the whole difficulty")
                .that(stale).isEqualTo(fresh);
        assertThat(stale).isNotSameInstanceAs(fresh);

        // FORM 1: the JDK's compare-and-remove, asked about the stale container after the replacement landed
        var byEquality = new ConcurrentSkipListMap<Long, WorkContainer<String, String>>();
        byEquality.put(CONTESTED_OFFSET, fresh);
        assertWithMessage("remove(key, value) compares with equals, so it answers YES about a container that is "
                + "not there and takes the replacement instead")
                .that(byEquality.remove(CONTESTED_OFFSET, stale)).isTrue();
        assertThat(byEquality.get(CONTESTED_OFFSET)).isNull();

        // FORM 2: computeIfPresent with an identity check in the function - the fix that looks airtight. The put
        // inside the function stands in for the controller's replacement landing after the function has decided
        // and before ConcurrentSkipListMap commits the removal; that gap is inside the JDK and cannot be reached
        // from the production seam, which is why this arm exists at all.
        var byIdentityInTheFunction = new ConcurrentSkipListMap<Long, WorkContainer<String, String>>();
        byIdentityInTheFunction.put(CONTESTED_OFFSET, stale);
        byIdentityInTheFunction.computeIfPresent(CONTESTED_OFFSET, (offset, resident) -> {
            byIdentityInTheFunction.put(offset, fresh);
            return resident == stale ? null : resident;
        });
        assertWithMessage("the identity check in the remapping function does not survive to the commit: "
                + "computeIfPresent removes through doRemove(key, v), which re-reads the value and gates on "
                + "equals - so this narrows the window instead of closing it, and loses the record just the same")
                .that(byIdentityInTheFunction.get(CONTESTED_OFFSET)).isNull();

        // FORM 3: the token the shard actually stores
        var byResidency = new ConcurrentSkipListMap<Long, ProcessingShard.Residency<String, String>>();
        var staleResidency = new ProcessingShard.Residency<>(stale);
        var freshResidency = new ProcessingShard.Residency<>(fresh);
        byResidency.put(CONTESTED_OFFSET, staleResidency);
        byResidency.put(CONTESTED_OFFSET, freshResidency);
        assertWithMessage("Residency overrides neither equals nor hashCode, so the comparison the map performs "
                + "is reference identity and the compare-and-remove declines")
                .that(byResidency.remove(CONTESTED_OFFSET, staleResidency)).isFalse();
        assertThat(byResidency.get(CONTESTED_OFFSET)).isSameInstanceAs(freshResidency);
        assertWithMessage("and it still removes the residency it really was asked about")
                .that(byResidency.remove(CONTESTED_OFFSET, freshResidency)).isTrue();
    }
}
