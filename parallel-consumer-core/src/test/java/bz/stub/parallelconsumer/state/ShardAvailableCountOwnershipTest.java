package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The shard's available-work count must be spent and returned by whoever <em>owns</em> the transition, never by a
 * site that infers "was this already counted?" from the container's observable state.
 * <p>
 * Extracted from a review finding on astubbs/parallel-consumer#336 (thread 3862087445).
 * {@link ProcessingShard#remove(long)} used to deduct a unit when
 * {@link WorkContainer#isAvailableToTakeAsWork()} was true at removal time. That predicate cannot answer the
 * question being asked of it: it describes what the container <em>is</em> at an instant, and two containers with
 * opposite accounting histories - one never taken as work, one taken and then released by
 * {@link WorkContainer#endFlight()} - are indistinguishable through it. So the deduction was made twice for some
 * records and not at all for others, and the resulting drift was permanent.
 * <p>
 * <b>Every test here drives the interleaving through a seam rather than racing for it.</b> The window is between
 * two calls made from different threads in production - the controller's stale-result handling and the poller's
 * revocation sweep - so calling them in the damaging order, in one thread, reproduces it exactly and every time.
 * The threading is not what makes the defect; the ordering is.
 *
 * @author Antony Stubbs
 * @see ProcessingShard
 * @see WorkContainer#claimShardAvailableUnit()
 */
@Slf4j
class ShardAvailableCountOwnershipTest {

    ModelUtils mu = new ModelUtils();
    WorkManager<String, String> wm;
    ShardManager<String, String> sm;
    PartitionStateManager<String, String> pm;

    static final String TOPIC = "topic";
    /** Default ordering is KEY, so one key means one shard. */
    static final String KEY = "K";

    TopicPartition tp = new TopicPartition(TOPIC, 0);

    @BeforeEach
    void setup() {
        PCModuleTestEnv module = mu.getModule();
        wm = module.workManager();
        sm = wm.getSm();
        pm = wm.getPm();
        wm.onPartitionsAssigned(UniLists.of(tp));
    }

    /**
     * The finding itself. A record is out at a worker, so selection has already spent its unit. Its partition is
     * revoked; the controller drops the now-stale result, which calls {@link WorkContainer#endFlight()} and tells
     * the shard nothing. The poller's revocation sweep then removes the entry - and used to deduct a <b>second</b>
     * unit, because the container now reads as available again.
     * <p>
     * A second record is queued behind it purely so the deficit is visible: with one record the counter is already
     * at zero and the old clamp swallowed the second deduction, which is precisely how this survived.
     */
    @Test
    void aStaleResultEndingTheFlightMustNotLetRevocationDeductASecondTime() {
        var outAtWorker = registerAndTake(100);
        register(101);
        assertCount("one taken, one still queued", 1);

        // The worker failed the record and its retry delay has since passed - so nothing in the container's state
        // distinguishes it from a record that was never taken, once the flight ends.
        outAtWorker.onUserFunctionFailure(new RuntimeException("failed at the worker"));
        mu.getModule().getMutableClock().add(Duration.ofSeconds(10));

        // Controller thread: WorkManager#handleFutureResult finds the result stale and drops it, ending the flight
        // without touching the shard.
        outAtWorker.endFlight();

        assertWithMessage("PRECONDITION: the container must read as available again, or this test no longer "
                + "exercises the window it was written for")
                .that(outAtWorker.isAvailableToTakeAsWork()).isTrue();

        // Poller thread: the revocation sweep now removes the entry.
        shard().remove(100);

        assertCount("offset 101 is still queued and selectable, so it must still be counted. A second deduction "
                + "for offset 100 hides it from WAITING_RECORDS and from drain()'s check", 1);
    }

    /**
     * The same unsound inference with the sign reversed, and reachable without any stale result at all.
     * <p>
     * A failed record holds a unit again ({@link ProcessingShard#onFailure}) but is <em>not</em>
     * {@link WorkContainer#isAvailableToTakeAsWork()} until its retry delay passes. Revoking it inside that window
     * used to deduct nothing, leaving the count permanently one too high - which inflates the poller's load gate
     * and can leave {@code drain()} believing work is still waiting forever.
     */
    @Test
    void revokingAFailedRecordInsideItsRetryDelayMustGiveItsUnitBack() {
        var failed = registerAndTake(100);
        assertCount("taken as work", 0);

        failed.onUserFunctionFailure(new RuntimeException("failed at the worker"));
        failed.endFlight();
        sm.onFailure(failed);
        assertCount("failed, so selectable again once the delay passes", 1);

        assertWithMessage("PRECONDITION: the retry delay must NOT have passed, or this test exercises nothing")
                .that(failed.isDelayPassed()).isFalse();

        shard().remove(100);

        assertCount("the shard is empty, so nothing can be awaiting selection", 0);
    }

    /**
     * The accumulating deficit recorded in {@code docs/inflight/bug-processing-shard-available-work-undercount.md}
     * - the same defect at the stale-replacement site, where the old code deducted nothing on the grounds that a
     * replacement is not an addition. That is true of {@code entries} and false of the counter: the replaced entry
     * had already spent its unit when it was taken as work, so the fresh entry arrived uncounted.
     */
    @Test
    void replacingAStaleEntryThatWasAlreadyTakenAsWorkMustLeaveTheFreshEntryCounted() {
        var takenThenStale = registerAndTake(100);
        assertCount("taken as work, so its unit is spent", 0);

        // Rebalance: the container's epoch is now old, so it is stale.
        wm.onPartitionsRevoked(UniLists.of(tp));
        wm.onPartitionsAssigned(UniLists.of(tp));

        // Re-establish the shard, then plant the stale resident white-box - the same technique
        // ProcessingShardStaleReplacement909Test uses, and for the same reason: the poller's sweep is what
        // normally removes a stale resident, so a test that lets the sweep run never reaches this branch.
        register(101);
        shard().getEntries().put(100L, takenThenStale);
        assertCount("only offset 101 is counted - the planted resident spent its unit when it was taken", 1);

        register(100);

        assertCount("the fresh container at offset 100 replaced the stale one and is selectable, so it must be "
                + "counted too. Leaving it uncounted is a deficit that survives every later add", 2);
    }

    /**
     * The claim's <b>residency recheck</b> - the half of {@code ProcessingShard.countAsSelectable} that makes the
     * claim-then-confirm order safe, and the only line in the design that defends against the two threads actually
     * interleaving rather than merely running in the damaging order.
     * <p>
     * Reached through its production seam rather than a race: a revocation lands between
     * {@code WorkManager.handleFutureResult}'s staleness check and {@code sm.onFailure}, so the controller counts a
     * container selectable again after the poller has already taken it out of the shard. Without the recheck the
     * claim sticks and the shard counts a container it no longer holds - an overcount that survives every later
     * add, which is the same permanent drift with the opposite sign to the reported defect.
     */
    @Test
    void countingBackInARecordThatHasAlreadyLeftTheShardMustHandTheUnitStraightBack() {
        var revokedThenFailed = registerAndTake(100);
        register(101);
        assertCount("one taken, one still queued", 1);

        // Poller thread: the revocation sweep removes the entry while the record is still out at a worker.
        shard().remove(100);
        assertCount("offset 100 has left the shard; offset 101 is untouched", 1);

        revokedThenFailed.onUserFunctionFailure(new RuntimeException("failed at the worker"));
        revokedThenFailed.endFlight();

        // Controller thread: the result comes back a failure, so the shard is asked to count it selectable again.
        sm.onFailure(revokedThenFailed);

        assertCount("a container that is no longer resident cannot be awaiting selection in this shard, so the "
                + "claim must be given back rather than left standing", 1);
    }

    /**
     * The same unsound inference at the <b>poller's sweep</b>, which is the second site that deducted a unit on the
     * strength of a container's observable state rather than on whether it held one.
     * <p>
     * A container taken as work and then gone stale across a rebalance has already spent its unit, so the sweep
     * that evicts it must deduct nothing. The old code deducted unconditionally here, which is the same double
     * deduction as the reported defect reached by a different path - and the clamp is what stopped it showing.
     */
    @Test
    void thePollersStaleSweepMustNotDeductForAnEntryThatAlreadySpentItsUnit() {
        var takenThenStale = registerAndTake(100);
        assertCount("taken as work, so its unit is spent", 0);

        wm.onPartitionsRevoked(UniLists.of(tp));
        wm.onPartitionsAssigned(UniLists.of(tp));

        // Same white-box plant as the stale-replacement case, and for the same reason: a stale resident is
        // normally removed by this very sweep, so a test that lets it run first never reaches the branch.
        register(101);
        shard().getEntries().put(100L, takenThenStale);
        assertCount("only offset 101 is counted - the planted resident spent its unit when it was taken", 1);

        long swept = sm.removeStaleContainers();

        assertWithMessage("PRECONDITION: the sweep must actually have removed the planted stale resident, or this "
                + "test exercises nothing")
                .that(swept).isAtLeast(1L);
        assertCount("the swept entry held no unit, so the sweep must deduct nothing - offset 101 is still queued "
                + "and selectable, and deducting for offset 100 would hide it", 1);
    }

    /**
     * The invariant the design rests on, exercised over a sequence that mixes every accounting path. It is not
     * documentation: a counter that can disagree with the units actually held, or go negative, fails here.
     */
    @Test
    void theCounterAgreesWithTheUnitsActuallyHeldAcrossEveryPath() {
        register(100);
        register(101);
        register(102);
        assertCount("three queued", 3);

        var taken = wm.getWorkIfAvailable(1).get(0);
        assertCount("one taken", 2);

        taken.onUserFunctionFailure(new RuntimeException("boom"));
        taken.endFlight();
        sm.onFailure(taken);
        assertCount("and put back", 3);

        // Idempotent - ShardManager#onFailure says so, and a second call must not count it twice.
        sm.onFailure(taken);
        assertCount("failure handling is idempotent", 3);

        // Deliberately a FRESH container rather than the resident one, and the only coverage anywhere of the case:
        // ProcessingShard#onSuccess removes by offset, so the object it uncounts is the one it actually removed,
        // not the one passed. WorkContainer#equals is topic/partition/offset only, so the two compare equal and a
        // future reader can swap in the resident container without any test going red - losing the coverage.
        sm.onSuccess(new WorkContainer<>(0, recordAt(101), mu.getModule()));
        assertCount("one succeeded and left the shard", 2);

        shard().remove(102);
        assertCount("one revoked", 1);

        shard().remove(102);
        assertCount("removing an absent offset changes nothing", 1);
    }

    // ---- helpers ----

    private ProcessingShard<String, String> shard() {
        return sm.getShard(ShardKey.ofKey(recordAt(0))).orElseThrow(AssertionError::new);
    }

    private void register(long offset) {
        sm.addWorkContainer(pm.getEpochOfPartition(tp), recordAt(offset));
    }

    private WorkContainer<String, String> registerAndTake(long offset) {
        register(offset);
        var work = wm.getWorkIfAvailable(1);
        assertWithMessage("PRECONDITION: offset " + offset + " must be handed out as work")
                .that(work.stream().anyMatch(wc -> wc.offset() == offset)).isTrue();
        return work.stream().filter(wc -> wc.offset() == offset).findFirst().orElseThrow(AssertionError::new);
    }

    private ConsumerRecord<String, String> recordAt(long offset) {
        return new ConsumerRecord<>(TOPIC, 0, offset, KEY, "v-" + offset);
    }

    /**
     * Asserts the counter against the expectation AND against ground truth - the units the resident containers
     * actually hold. Asserting only the number would let a counter that happens to be right for the wrong reason
     * through, which is the failure mode this whole class is about.
     */
    private void assertCount(String why, long expected) {
        var shard = shard();
        assertWithMessage(why)
                .that(shard.getCountOfWorkAwaitingSelection()).isEqualTo(expected);
        assertWithMessage("the counter must equal the units the shard's resident containers hold (" + why + ")")
                .that(shard.getCountOfWorkAwaitingSelection()).isEqualTo(shard.countHeldUnitsByScan());
        assertWithMessage("the counter must never go negative, and must not rely on a clamp to stay non-negative "
                + "(" + why + ")")
                .that(shard.getCountOfWorkAwaitingSelection()).isAtLeast(0L);
    }
}
