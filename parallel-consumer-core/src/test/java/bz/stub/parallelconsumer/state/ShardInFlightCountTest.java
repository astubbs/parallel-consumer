package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.FakeRuntimeException;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.internal.EpochAndRecordsMap;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import pl.tlinkowski.unij.api.UniLists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The per-shard in-flight count, and the two things it is for.
 * <p>
 * WHY IT EXISTS. Dispatch used to enter <em>every</em> shard on every pass with no guard at all - not even
 * {@code isEmpty()} - paying an iterator, a set, a list and a walk to the head purely to discover that an
 * ordered shard already had a record out at a worker, and then breaking. An ordered shard may have at most one
 * record out at a time, so that is one bit of information, and
 * {@link ProcessingShard#getCountOfWorkInFlight()} answers it with one comparison.
 * <p>
 * It also replaces an estimate. {@link ShardManager#getUpperBoundOnSelectableWork()} - which decides how many
 * parked workers the direct-pull engine wakes - could previously do no better than
 * {@code min(awaitingSelection, shardCount)}, counting an occupied shard as offering one record when it offers
 * none. With the per-shard truth it counts the shards that can actually yield.
 * <p>
 * WHAT THIS IS NOT. It is not the ordering guarantee, and no test here may be read as making it one. The
 * guarantee is still the per-record claim plus the {@code isOrderRestricted()} break inside the scan, which
 * {@link DirectPullConcurrentSelectionTest} pins under concurrency. The count is an optimisation on top: a
 * reading that is transiently low costs a wasted scan, never a second record out of an ordered shard. The
 * count-versus-scan assertions below are what would catch it drifting the other way, which is the direction
 * that would silently stall a shard.
 *
 * @author Antony Stubbs
 * @see ProcessingShard#getCountOfWorkInFlight()
 * @see ShardManager#getUpperBoundOnSelectableWork()
 */
@Slf4j
class ShardInFlightCountTest {

    static final String TOPIC = "shard-in-flight-topic";
    static final TopicPartition TP = new TopicPartition(TOPIC, 0);

    PCModuleTestEnv module;
    WorkManager<String, String> wm;

    void setup(ProcessingOrder ordering) {
        module = new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder()
                .ordering(ordering)
                .build());
        wm = module.workManager();
        wm.onPartitionsAssigned(UniLists.of(TP));
    }

    /**
     * @param key the record key, which is what {@link ProcessingOrder#KEY} shards on
     */
    void register(String key, int fromOffset, int count) {
        List<ConsumerRecord<String, String>> recs = new ArrayList<>(count);
        for (int i = fromOffset; i < fromOffset + count; i++) {
            recs.add(new ConsumerRecord<>(TOPIC, 0, i, key, "value-" + i));
        }
        Map<TopicPartition, List<ConsumerRecord<String, String>>> m = new HashMap<>();
        m.put(TP, recs);
        wm.registerWork(new EpochAndRecordsMap<>(new ConsumerRecords<>(m), wm.getPm()));
    }

    ProcessingShard<String, String> shardFor(String key) {
        var sm = wm.getSm();
        var shard = sm.getShard(sm.computeShardKey(new ConsumerRecord<>(TOPIC, 0, 0, key, "v")));
        assertWithMessage("shard for key %s must exist", key).that(shard.isPresent()).isTrue();
        return shard.get();
    }

    long entriesExamined() {
        return wm.getSm().getDispatchScanMeter().getEntriesExamined();
    }

    /**
     * Held against the O(n) scan of the containers on every check, never on its own. A counter that agrees with
     * itself proves nothing; the failure this guards against is the counter and the shard disagreeing.
     */
    void assertInFlightCountAgreesWithScan(ProcessingShard<String, String> shard, long expected) {
        assertWithMessage("the O(1) in-flight count").that(shard.getCountOfWorkInFlight()).isEqualTo(expected);
        assertWithMessage("and an independent O(n) scan of the shard's containers agrees with it")
                .that(shard.countWorkInFlightByScan()).isEqualTo(expected);
    }

    // -----------------------------------------------------------------------------------------------------
    // The scan the count avoids
    // -----------------------------------------------------------------------------------------------------

    /**
     * The whole point of the count: an ordered shard with a record out at a worker is not entered at all, so it
     * examines nothing.
     * <p>
     * Asserted with {@link DispatchScanMeter}, which counts entries the scan looked at - the same instrument
     * {@link OrderingModeDispatchParityTest} uses, and for the same reason: it is a count, so machine load
     * cannot move it. "Not entered" is exactly "examined nothing".
     */
    @ParameterizedTest
    @EnumSource(value = ProcessingOrder.class, names = {"KEY", "PARTITION"})
    void anOrderedShardWithWorkInFlightIsNotEnteredAtAll(ProcessingOrder ordering) {
        setup(ordering);
        register("the-key", 0, 5);
        var shard = shardFor("the-key");

        assertInFlightCountAgreesWithScan(shard, 0);

        var taken = wm.getWorkIfAvailable(1);
        assertThat(taken).hasSize(1);
        assertInFlightCountAgreesWithScan(shard, 1);
        assertThat(shard.isBlockedByWorkInFlight()).isTrue();

        long examinedBefore = entriesExamined();
        var second = wm.getWorkIfAvailable(5);
        long examinedDuring = entriesExamined() - examinedBefore;

        assertWithMessage("an occupied ordered shard hands out nothing - that is the pre-existing invariant")
                .that(second).isEmpty();
        assertWithMessage("and it now does so without being entered: %s shard entries were examined to learn "
                + "something one comparison already knew", examinedDuring)
                .that(examinedDuring).isEqualTo(0L);
    }

    /**
     * The other half: once the record lands, the shard is entered again. Without this, a count that never came
     * back down would look identical to a working one right up until the consumer stalled.
     */
    @ParameterizedTest
    @EnumSource(value = ProcessingOrder.class, names = {"KEY", "PARTITION"})
    void theShardIsEnteredAgainAsSoonAsItsRecordLands(ProcessingOrder ordering) {
        setup(ordering);
        register("the-key", 0, 5);
        var shard = shardFor("the-key");

        var taken = wm.getWorkIfAvailable(1).get(0);
        taken.onUserFunctionSuccess();
        wm.handleFutureResult(taken);

        assertInFlightCountAgreesWithScan(shard, 0);
        assertThat(shard.isBlockedByWorkInFlight()).isFalse();

        long examinedBefore = entriesExamined();
        var next = wm.getWorkIfAvailable(5);

        assertWithMessage("the shard is open for business again").that(next).hasSize(1);
        assertWithMessage("and it was really entered, rather than the count merely reading zero")
                .that(entriesExamined() - examinedBefore).isGreaterThan(0L);
    }

    /**
     * {@code UNORDERED} shards are never blocked, so the check must cost one comparison that is never true and
     * change nothing. Making the count help {@code UNORDERED} is a separate open question - see
     * {@code docs/inflight/next-direct-pull-unordered-selection.md} - and this pins that it has not been done by
     * accident.
     */
    @Test
    void anUnorderedShardIsUnaffectedByWorkBeingInFlight() {
        setup(ProcessingOrder.UNORDERED);
        register("any-key", 0, 5);
        var shard = shardFor("any-key");

        var taken = wm.getWorkIfAvailable(1);
        assertThat(taken).hasSize(1);
        assertInFlightCountAgreesWithScan(shard, 1);

        assertWithMessage("an unordered shard is never blocked, whatever it has in flight")
                .that(shard.isBlockedByWorkInFlight()).isFalse();

        long examinedBefore = entriesExamined();
        var rest = wm.getWorkIfAvailable(4);

        assertWithMessage("and it keeps handing out the rest of its records")
                .that(rest).hasSize(4);
        assertThat(entriesExamined() - examinedBefore).isGreaterThan(0L);
        assertInFlightCountAgreesWithScan(shard, 5);
    }

    // -----------------------------------------------------------------------------------------------------
    // The charge is released by every way a delivery can end
    // -----------------------------------------------------------------------------------------------------

    /**
     * Success, failure and abandonment are three different return paths through {@link WorkManager}, and each
     * one has to give the shard its capacity back. A count that only came down on success would leave a shard
     * that failed one record blocked for the life of the consumer.
     */
    @Test
    void everyWayADeliveryEndsReleasesTheShardsCharge() {
        setup(ProcessingOrder.KEY);
        register("succeeds", 0, 2);
        register("fails", 10, 2);
        register("abandoned", 20, 2);

        var taken = wm.getWorkIfAvailable(3);
        assertThat(taken).hasSize(3);
        assertInFlightCountAgreesWithScan(shardFor("succeeds"), 1);
        assertInFlightCountAgreesWithScan(shardFor("fails"), 1);
        assertInFlightCountAgreesWithScan(shardFor("abandoned"), 1);

        for (var wc : taken) {
            String key = wc.getCr().key();
            if ("succeeds".equals(key)) {
                wc.onUserFunctionSuccess();
            } else if ("fails".equals(key)) {
                wc.onUserFunctionFailure(new FakeRuntimeException("deliberate"));
            } else {
                wc.markAbandoned(wc.getDeliveryCount());
            }
            wm.handleFutureResult(wc);
        }

        assertInFlightCountAgreesWithScan(shardFor("succeeds"), 0);
        assertInFlightCountAgreesWithScan(shardFor("fails"), 0);
        assertInFlightCountAgreesWithScan(shardFor("abandoned"), 0);
        assertWithMessage("a shard that failed a record is not blocked by it - it is parked in retry, not held")
                .that(shardFor("fails").isBlockedByWorkInFlight()).isFalse();
    }

    /**
     * The path with no shard call in it at all: a result returning for a partition that has since been revoked
     * takes {@link WorkManager#handleFutureResult}'s stale branch, which ends the flight and tells the shard
     * nothing. The charge is released by the record's own transition rather than by a removal site, which is
     * exactly why that path needs no special case - and this is the test that would fail if the charge were ever
     * moved onto the removal sites instead.
     */
    @Test
    void aResultReturningForARevokedPartitionStillReleasesItsCharge() {
        setup(ProcessingOrder.KEY);
        register("the-key", 0, 2);
        var shard = shardFor("the-key");

        var wc = wm.getWorkIfAvailable(1).get(0);
        assertThat(shard.getCountOfWorkInFlight()).isEqualTo(1L);

        wm.onPartitionsRevoked(UniLists.of(TP));

        wc.onUserFunctionSuccess();
        wm.handleFutureResult(wc);

        assertWithMessage("the stale branch ends the flight without touching any shard, and the charge still "
                + "comes back - it belongs to the record's transition, not to the removal")
                .that(shard.getCountOfWorkInFlight()).isEqualTo(0L);
    }

    // -----------------------------------------------------------------------------------------------------
    // The estimate the count replaces
    // -----------------------------------------------------------------------------------------------------

    /**
     * {@link ShardManager#getUpperBoundOnSelectableWork()} is what the direct-pull engine uses to decide how many
     * parked workers to wake, so over-reporting sends threads to contend over records that are not there.
     * <p>
     * Three shards of four records each. The old expression was {@code min(awaitingSelection, shardCount)}, which
     * answers 3 at every step below however many shards are occupied - so the middle two assertions are the ones
     * that only pass with the per-shard count.
     */
    @Test
    void theUpperBoundCountsShardsThatCanActuallyYield() {
        setup(ProcessingOrder.KEY);
        register("a", 0, 4);
        register("b", 10, 4);
        register("c", 20, 4);

        assertWithMessage("three unoccupied shards, so three records could be handed out right now")
                .that(wm.getUpperBoundOnSelectableWork()).isEqualTo(3L);

        var first = wm.getWorkIfAvailable(1).get(0);
        assertWithMessage("one shard is now occupied and offers nothing, so the bound is two - not three, "
                + "which is what counting shards rather than yielding shards gives")
                .that(wm.getUpperBoundOnSelectableWork()).isEqualTo(2L);

        var rest = wm.getWorkIfAvailable(2);
        assertThat(rest).hasSize(2);
        assertWithMessage("every shard occupied: nothing at all can be handed out")
                .that(wm.getUpperBoundOnSelectableWork()).isEqualTo(0L);

        first.onUserFunctionSuccess();
        wm.handleFutureResult(first);
        assertWithMessage("one shard freed, and it still holds three more records - but it can only yield one")
                .that(wm.getUpperBoundOnSelectableWork()).isEqualTo(1L);
    }

    /**
     * An empty shard offers nothing either, and under {@code KEY} an emptied shard is removed - so this is really
     * asserting that the bound never counts a shard that has run out of work but has not been collected yet.
     */
    @Test
    void theUpperBoundIgnoresAShardWithNothingLeftToOffer() {
        setup(ProcessingOrder.PARTITION);
        register("the-key", 0, 1);

        var wc = wm.getWorkIfAvailable(1).get(0);
        wc.onUserFunctionSuccess();
        wm.handleFutureResult(wc);

        assertWithMessage("the shard survives under PARTITION ordering but has nothing to give")
                .that(wm.getUpperBoundOnSelectableWork()).isEqualTo(0L);
    }

    /**
     * {@code UNORDERED} is bounded by the queued count, not by the shard count, and the in-flight count must not
     * have crept into that path.
     */
    @Test
    void theUpperBoundUnderUnorderedIsStillTheQueuedCount() {
        setup(ProcessingOrder.UNORDERED);
        register("any-key", 0, 6);

        assertThat(wm.getUpperBoundOnSelectableWork()).isEqualTo(6L);

        var taken = wm.getWorkIfAvailable(2);
        assertThat(taken).hasSize(2);
        assertWithMessage("an unordered shard offers everything still queued, whatever it has in flight")
                .that(wm.getUpperBoundOnSelectableWork()).isEqualTo(4L);
    }
}
