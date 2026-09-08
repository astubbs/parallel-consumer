package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;
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
 * when the removal landed is what left the shard. The sweep now removes conditionally on the container it
 * inspected, in one atomic step - which only means anything because {@link WorkContainer}'s equality is identity.
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

    private RecordPopulation population;

    private PartitionStateManager<String, String> seam;

    private ProcessingShard<String, String> shard;

    private ConsumerRecord<String, String> record;

    private WorkContainer<String, String> stale;

    private long currentEpoch;

    /**
     * A shard holding one stale container at {@link #CONTESTED_OFFSET}, which is where both sweep arms start.
     * <p>
     * <b>Deliberately called from each test rather than run as a {@code @BeforeEach}</b>: the third test in this
     * class needs none of it, and a fixture that builds itself for a test that does not use it reads as though it
     * matters there.
     * <p>
     * <b>The seam is spied in BOTH arms, and that strengthens the control rather than contaminating it.</b> Only
     * the defect arm arms it with {@link ShardSeamTestBase#onNextStalenessCheck}, so after this hoist the single
     * term that differs between the two sweeps is whether a replacement lands inside one - which is what "same
     * magnitude, different position" claims and what the control is for. An unspied control would leave the
     * collaborator as a second difference, and a reader could not tell which of the two produced the outcome.
     */
    private void givenAStaleResidentAtTheContestedOffset() {
        population = new RecordPopulation();
        seam = spy(wm.getPm());
        record = recordAt(CONTESTED_OFFSET);
        shard = shardWith(seam, population, record);

        wm.onPartitionsAssigned(UniLists.of(TP));
        long firstEpoch = wm.getPm().getEpochOfPartition(TP);
        stale = new WorkContainer<>(firstEpoch, record, module);
        shard.addWorkContainer(stale);

        // the partition is taken away and handed back, so what the shard is still holding is now stale
        wm.onPartitionsRevoked(UniLists.of(TP));
        wm.onPartitionsAssigned(UniLists.of(TP));
        currentEpoch = wm.getPm().getEpochOfPartition(TP);
        assertWithMessage("PRECONDITION: the rebalance must actually have made the resident stale")
                .that(currentEpoch).isGreaterThan(firstEpoch);
    }

    /**
     * The one assertion that is identical in both arms, and it is identical because it is about neither of them:
     * the accounting half of this defect is already closed, and it must not be traded back for the eviction half
     * whichever way the sweep goes. The counts either arm expects DO differ, and stay written out where they are.
     */
    private void assertTheCountersAgreeWithWhatIsHeld() {
        assertWithMessage("the counter agrees with the units actually held - the accounting half of this defect "
                + "is already closed and must not be traded back for the eviction half")
                .that(shard.getCountOfWorkAwaitingSelection()).isEqualTo(shard.countSelectionClaimedByScan());
    }

    /**
     * The sweep must remove the container it inspected, and only that one.
     * <p>
     * The interleaving is the one the sweep cannot see: it answers "stale" about the resident, the controller
     * replaces that resident with a fresh container carrying the current epoch, and the sweep's removal - keyed on
     * the offset alone - takes the fresh one out instead.
     */
    @Test
    void theStaleSweepMustNotEvictAFreshReplacementThatLandedInsideIt() {
        givenAStaleResidentAtTheContestedOffset();

        var fresh = new WorkContainer<>(currentEpoch, record, module);
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
                .that(fresh.getEpoch()).isEqualTo(currentEpoch);

        // IDENTITY, never equality, in every assertion below - spelled out rather than relying on
        // WorkContainer's equality being identity today. The question here is WHICH OBJECT survived, and it must
        // keep being asked that way whatever the equality contract does next: written with Truth's hasValue or
        // containsExactly, these assertions would pass on the defective behaviour under the coordinate-based
        // equals this class was written against, and this test would assert nothing.
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
        assertTheCountersAgreeWithWhatIsHeld();
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
        givenAStaleResidentAtTheContestedOffset();

        // NOTHING is armed on the seam here, and that absence is the whole of what this arm varies.
        var swept = shard.removeStaleWorkContainersFromShard();

        assertThat(swept).hasSize(1);
        assertThat(swept.get(0)).isSameInstanceAs(stale);
        assertThat(shard.getWorkContainerAtOffset(CONTESTED_OFFSET)).isEmpty();
        assertThat(shard.getCountOfWorkTracked()).isEqualTo(0L);
        assertTheCountersAgreeWithWhatIsHeld();
        assertThat(population.getInSystem()).isEqualTo(0L);
    }

    /**
     * The premise the fix rests on: <b>{@link WorkContainer}'s equality is IDENTITY, which is what lets
     * {@code Map.remove(key, value)} express "remove the container I inspected"</b> rather than "remove whatever
     * is at this offset".
     * <p>
     * A compare-and-remove decides "still mapped to the value I inspected" with {@code equals}, so what it means
     * is a property of the value type and not of the map - no map API can rescue a value type whose equality
     * cannot tell two occupants of one offset apart. The two halves below are therefore asserted together: the
     * equality contract itself, then the map behaviour that follows from it.
     * <p>
     * <b>This is the tripwire for a reintroduced coordinate-based {@code equals}/{@code hashCode}.</b> Restore
     * the topic/partition/offset pair {@code WorkContainer} used to carry and every assertion here inverts:
     * the two containers compare equal, the compare-and-remove asked about the departed one answers yes, and the
     * replacement is destroyed. It also records the JDK measurement that ruled out the near-miss fix -
     * {@code computeIfPresent} with an identity check in the remapping function - which was airtight-looking and
     * closed nothing while equality was by coordinates, because that method commits through
     * {@code doRemove(key, v)}, which re-reads the node value and gates on {@code v.equals(reRead)}. Under
     * identity equality that gate now declines, which is the third form below.
     */
    @Test
    // reference equality is the SUBJECT of this test, not an accident of it
    @SuppressWarnings("ReferenceEquality")
    void twoContainersAtOneOffsetMustNotBeInterchangeable() {
        var record = recordAt(CONTESTED_OFFSET);
        var stale = new WorkContainer<>(0L, record, module);
        var fresh = new WorkContainer<>(1L, record, module);

        // THE CONTRACT. Two containers for one record at two epochs are two different flights, and nothing may
        // treat one as a stand-in for the other.
        assertThat(stale).isNotSameInstanceAs(fresh);
        assertWithMessage("WorkContainer equality is identity - a coordinate-based equals would make the stale "
                + "container and its fresh replacement interchangeable in every value-conditional collection "
                + "operation, which is the defect this class exists for")
                .that(stale).isNotEqualTo(fresh);
        assertWithMessage("hashCode is the identity hash, inherited rather than overridden - anything else is a "
                + "coordinate-derived hashCode, which is half of the same defect")
                .that(stale.hashCode()).isEqualTo(System.identityHashCode(stale));
        assertThat(fresh.hashCode()).isEqualTo(System.identityHashCode(fresh));
        assertWithMessage("and so the two hash to different buckets, instead of colliding by construction")
                .that(stale.hashCode()).isNotEqualTo(fresh.hashCode());

        // FORM 1: the JDK's compare-and-remove, asked about the stale container after the replacement landed. It
        // must DECLINE - this is the exact call the sweep makes, and the one that used to destroy the record.
        var byIdentity = new ConcurrentSkipListMap<Long, WorkContainer<String, String>>();
        byIdentity.put(CONTESTED_OFFSET, fresh);
        assertWithMessage("remove(key, value) compares with equals, so identity equality is what makes it answer "
                + "NO about a container that is no longer there")
                .that(byIdentity.remove(CONTESTED_OFFSET, stale)).isFalse();
        assertThat(byIdentity.get(CONTESTED_OFFSET)).isSameInstanceAs(fresh);

        // FORM 2: and it still removes the container it really was asked about, so the conditional form is not
        // merely safe but useful - a removal that never removed anything would satisfy FORM 1 alone.
        assertWithMessage("the same call names the container it means, and removes exactly that one")
                .that(byIdentity.remove(CONTESTED_OFFSET, fresh)).isTrue();
        assertThat(byIdentity.get(CONTESTED_OFFSET)).isNull();

        // FORM 3: computeIfPresent with an identity check in the remapping function - the near-miss fix, kept
        // because its failure mode was invisible at the production seam. The put inside the function stands in
        // for the controller's replacement landing after the function has decided and before
        // ConcurrentSkipListMap commits the removal; that gap is inside the JDK and cannot be reached from the
        // product. Under coordinate equality the re-read gate passed and the replacement was destroyed; under
        // identity equality it declines.
        var byIdentityInTheFunction = new ConcurrentSkipListMap<Long, WorkContainer<String, String>>();
        byIdentityInTheFunction.put(CONTESTED_OFFSET, stale);
        byIdentityInTheFunction.computeIfPresent(CONTESTED_OFFSET, (offset, resident) -> {
            byIdentityInTheFunction.put(offset, fresh);
            return resident == stale ? null : resident;
        });
        assertWithMessage("computeIfPresent commits through doRemove(key, v), which re-reads the value and gates "
                + "on equals - so with identity equality the replacement survives the gap it used to be lost in")
                .that(byIdentityInTheFunction.get(CONTESTED_OFFSET)).isSameInstanceAs(fresh);
    }
}
