package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The revocation sweep must empty the offsets of the generation being revoked, and never take a LIVE container a
 * later registration put at one of those offsets.
 * <p>
 * <b>The sibling of {@link ShardStaleSweepReplacementEvictionTest}, one site along.</b> astubbs/parallel-consumer#468
 * fixed the poller's stale sweep, which asked each occupant whether it was stale and then removed by KEY;
 * astubbs/parallel-consumer#483's defect-class sweep reported a second instance of the same shape and left it -
 * {@code ShardManager.removeWorkFromShardFor}, the revoke and lost path's {@code removeWorkAtOffset}. It is the
 * same defect class stated the other way round: there the removal could not name the container it had judged, here
 * it cannot name the <em>registration</em> it was handed. The sweep's argument is a {@link ConsumerRecord} the
 * revoked {@link PartitionState} was still carrying as incomplete; what it used to remove was whatever occupied
 * that offset when the removal landed.
 * <p>
 * <b>The harm, if it is ever reached, is the lost record</b> - the same one astubbs#468 measured. The live
 * container is gone from the shard while its own {@code PartitionState} still carries its offset as incomplete, so
 * nothing selects it and nothing completes it, and the commit high-water mark cannot pass it until the partition
 * is re-polled.
 * <p>
 * <b>Whether production reaches it, stated plainly.</b> Not today, and the discriminator is the thread: both
 * rebalance callbacks run on the broker-poll thread, so the revoke sweep for a generation completes before the
 * assignment that could register a replacement at one of its offsets even begins - the same single-poll-thread
 * argument astubbs#483 used for {@code addWorkContainer}'s displacement branch. That is an argument about
 * callers, and nothing checks it; the conditional form costs one reference comparison and does not rest on it,
 * which is exactly why astubbs#468 wrote {@code getWorkIfAvailable}'s last-resort sweep conditionally against a
 * race it had shown was unreachable there. These arms therefore drive the sweep directly rather than through a
 * rebalance, and say so.
 *
 * @author Antony Stubbs
 * @see ShardStaleSweepReplacementEvictionTest for the same defect at the stale sweep, with its production seam
 */
class ShardRevokeSweepReplacementEvictionTest {

    private static final String TOPIC = "topic";

    private static final TopicPartition TP = new TopicPartition(TOPIC, 0);

    private static final long CONTESTED_OFFSET = 100L;

    private final ModelUtils mu = new ModelUtils();

    private final WorkManager<String, String> wm = mu.getModule().workManager();

    private final ShardManager<String, String> sm = wm.getSm();

    /** The record the revoked generation registered, and the one its sweep is handed. */
    private final ConsumerRecord<String, String> revokedRecord = recordAt(CONTESTED_OFFSET, "revoked");

    /**
     * The state of the generation being revoked - captured before it is replaced, because the sweep's argument
     * list is built from <em>its</em> incomplete offsets and not from whatever is assigned now.
     */
    private PartitionState<String, String> revokedGeneration;

    private long revokedEpoch;

    private ProcessingShard<String, String> shard;

    /**
     * One generation assigned, one record registered through the production path, then a second assignment - so
     * the shard exists, the revoked generation still carries {@link #CONTESTED_OFFSET} as incomplete, and the
     * container it registered has already been swept as stale by the reassignment.
     * <p>
     * The reassignment is what makes a LIVE container at that offset possible at all: staleness is asked through
     * the {@link PartitionState} object, so a container is fresh only against the state currently installed.
     */
    @BeforeEach
    void givenARevokedGenerationStillNamingTheContestedOffset() {
        wm.onPartitionsAssigned(UniLists.of(TP));
        revokedGeneration = wm.getPm().getPartitionState(TP);
        revokedEpoch = wm.getPm().getEpochOfPartition(TP);

        // the production registration order - the offset enters the partition's tracking, then its shard
        revokedGeneration.addNewIncompleteRecord(revokedRecord);
        sm.addWorkContainer(revokedEpoch, revokedRecord);

        // the partition comes back under a new generation; its epoch-change sweep takes the old container out
        wm.onPartitionsAssigned(UniLists.of(TP));
        assertWithMessage("PRECONDITION: the reassignment must have moved the epoch on, or nothing here is stale")
                .that(currentEpoch()).isGreaterThan(revokedEpoch);

        shard = sm.getShard(sm.computeShardKey(revokedRecord))
                .orElseThrow(() -> new AssertionError("PRECONDITION: the shard must survive being emptied - "
                        + "only KEY ordering garbage-collects one, and this fixture is not in KEY ordering"));
        assertWithMessage("PRECONDITION: the reassignment's stale sweep must have emptied the contested offset")
                .that(shard.getWorkContainerAtOffset(CONTESTED_OFFSET)).isEmpty();
    }

    private long currentEpoch() {
        return wm.getPm().getEpochOfPartition(TP);
    }

    private ConsumerRecord<String, String> recordAt(long offset, String generation) {
        return new ConsumerRecord<>(TOPIC, TP.partition(), offset, "a-key", generation + "-" + offset);
    }

    /**
     * Drives the sweep the way production does - {@link PartitionState#onPartitionsRemoved} builds the argument
     * from the revoked generation's own incomplete offsets, so nothing here hand-picks what the sweep is told.
     */
    private void revokeSweepForTheRevokedGeneration() {
        assertWithMessage("PRECONDITION: the revoked generation must still be tracking the contested offset, or "
                + "its sweep is handed nothing and every assertion below is vacuous")
                .that(revokedGeneration.getAllIncompleteOffsets())
                .contains(CONTESTED_OFFSET);
        revokedGeneration.onPartitionsRemoved(sm);
    }

    /**
     * THE DEFECT. A stale container from the revoked generation is displaced by a fresh one the current
     * generation registered at the same offset, and the revoked generation's sweep - which meant the stale one -
     * must not take the replacement.
     * <p>
     * RED against the unconditional {@code removeWorkAtOffset(consumerRecord.offset())}: the fresh container is
     * evicted, and the current generation still carries its offset as incomplete.
     */
    @Test
    void theRevokeSweepMustNotEvictAFreshContainerThatDisplacedTheStaleOne() {
        // the revoked generation's container is still resident when the current generation's record arrives -
        // planted rather than left in place, because the reassignment's own sweep removes it first
        var stale = new WorkContainer<>(revokedEpoch, revokedRecord, mu.getModule());
        shard.plantResident(stale);

        // the current generation's poll delivers the same offset again: a DIFFERENT ConsumerRecord object, from a
        // different fetch, which is what a re-delivery after a rebalance actually is
        var freshRecord = recordAt(CONTESTED_OFFSET, "current");
        sm.addWorkContainer(currentEpoch(), freshRecord);
        var fresh = shard.getWorkContainerAtOffset(CONTESTED_OFFSET).orElseThrow(AssertionError::new);
        assertWithMessage("PRECONDITION: the arrival must have displaced the stale resident, not been dropped")
                .that(fresh).isNotSameInstanceAs(stale);
        assertWithMessage("PRECONDITION: the replacement must be live against the state installed now, or the "
                + "sweep is being asked about two stale containers and this arm proves nothing")
                .that(fresh.getEpoch()).isEqualTo(currentEpoch());

        revokeSweepForTheRevokedGeneration();

        // IDENTITY, never Truth's hasValue or containsExactly: the question is WHICH OBJECT survived, and
        // WorkContainer's equality being identity today is what this asks about rather than what it relies on
        assertWithMessage("the replacement is the only live container at this offset, and the CURRENT "
                + "generation carries its offset as incomplete - evicting it loses the record until the "
                + "partition is re-polled")
                .that(shard.getWorkContainerAtOffset(CONTESTED_OFFSET).orElse(null))
                .isSameInstanceAs(fresh);
    }

    /**
     * CONTROL, and the tripwire for the REGISTRATION-IDENTITY leg of the guard: a container built from the very
     * record the sweep was handed is removed even though it is live.
     * <p>
     * Not a contrived shape - it is what a revoke sweep driven before the partition's state has been swapped
     * sees, which is what {@code ShardManagerLincheckTest.revokeSweep} models on every invocation. A guard that
     * asked staleness alone would decline here, and that harness's removal operation would quietly become a
     * no-op while staying green.
     */
    @Test
    void aRevokeSweepStillRemovesALiveContainerBuiltFromTheRecordItWasHanded() {
        var itsOwn = new WorkContainer<>(currentEpoch(), revokedRecord, mu.getModule());
        shard.plantResident(itsOwn);
        assertWithMessage("PRECONDITION: this container must be LIVE, or the staleness leg would carry the arm "
                + "and it would stop pinning registration identity")
                .that(wm.checkIfWorkIsStale(itsOwn)).isFalse();

        revokeSweepForTheRevokedGeneration();

        assertWithMessage("the sweep is emptying this offset for the registration it names, and that is what it "
                + "must go on doing whatever the epoch says")
                .that(shard.getWorkContainerAtOffset(CONTESTED_OFFSET)).isEmpty();
    }

    /**
     * SECOND CONTROL, and the tripwire for the STALENESS leg: a container at the offset from a
     * <em>different</em> registration that is nonetheless STALE is still swept out.
     * <p>
     * Registration identity alone would decline here, and declining would leave a stale container occupying an
     * offset of a revoked partition - the state the sweep exists to prevent. So the guard declines for exactly
     * one thing, a live container from another registration, and this arm is what pins the difference.
     */
    @Test
    void aStaleContainerFromAnotherRegistrationIsStillSweptOut() {
        var anotherRecord = recordAt(CONTESTED_OFFSET, "another");
        var staleButNotTheOneNamed = new WorkContainer<>(revokedEpoch, anotherRecord, mu.getModule());
        shard.plantResident(staleButNotTheOneNamed);
        assertWithMessage("PRECONDITION: this container must be from a registration the sweep was NOT handed")
                .that(staleButNotTheOneNamed.getCr()).isNotSameInstanceAs(revokedRecord);

        revokeSweepForTheRevokedGeneration();

        assertThat(shard.getWorkContainerAtOffset(CONTESTED_OFFSET)).isEmpty();
    }
}
