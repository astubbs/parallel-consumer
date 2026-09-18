package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.internal.EpochAndRecordsMap;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.List;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * astubbs#178 (confluentinc#843): a record of one key must not be handed to a second worker while an earlier
 * delivery of that key is still running - <b>including when a rebalance separates the two</b>.
 * <p>
 * <b>The route, deterministic rather than raced for.</b> A container is taken as work and a worker is inside the
 * user function. The partition is revoked and handed straight back: the epoch moves on, and the stale sweep
 * ({@link ProcessingShard#removeStaleWorkContainersFromShard}, reached from both callbacks) evicts the container
 * from its shard on staleness alone, never asking whether it is in flight. The offset was never committed, so
 * the partition re-delivers it; the fresh container lands in a shard whose {@code workMap} no longer holds the
 * old one, and the ordering restriction consults only that map - so the fresh container is takeable, and the key
 * is now running on two threads. PC's bookkeeping stays right ({@code WorkManager#handleFutureResult} drops the
 * old result as stale), which is what kept this invisible: nothing PC counts goes wrong, only the user's
 * "strong ordering by key" does.
 * <p>
 * Every arm is single-threaded. The two callbacks run inline, the scan runs inline, and "still in flight" is a
 * container whose claim was won and never returned - the same shape the repo's confluentinc#857 probes use to
 * force a window open rather than replaying a randomised run and reporting a rate. The chaos cell
 * {@code ChaosRevokeUnderWorkKeyOrderIT} corroborates under real churn; this is the evidence.
 * <p>
 * <b>The ruling this pins:</b> an undrained old-epoch delivery running concurrently with the same key's new-epoch
 * delivery IS a key-ordering violation, and the fix is on the selection side - the re-delivered record waits
 * for the old flight to end, exactly as it would have waited had there been no rebalance. Not a drain on revoke:
 * that is the confluentinc#857 recipe ({@code docs/inflight/bug-857-family.md}), and it spends the poll-interval
 * budget inside a callback.
 *
 * @author Antony Stubbs
 */
class KeyOrderAcrossRebalanceTest {

    private static final String TOPIC = "topic";

    private static final TopicPartition TP = new TopicPartition(TOPIC, 0);

    private static final String THE_KEY = "k";

    private PCModuleTestEnv module;

    private WorkManager<String, String> wm;

    private ShardManager<String, String> sm;

    private PartitionStateManager<String, String> pm;

    private void givenAnAssignedPartitionUnder(ProcessingOrder ordering) {
        module = new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder().ordering(ordering).build());
        wm = module.workManager();
        sm = wm.getSm();
        pm = wm.getPm();
        wm.onPartitionsAssigned(UniLists.of(TP));
    }

    /**
     * THE defect, through the real registration path, in both orderings that make a promise.
     * <p>
     * The registration path matters under KEY: {@code PartitionState} carries the offset as incomplete, so the
     * revoke sweep both evicts the in-flight container AND garbage-collects its now-empty shard - the fresh
     * arrival then builds a brand-new shard with no memory of the flight it is queued behind. A fixture that
     * calls {@code ShardManager#addWorkContainer} directly never registers the offset and never reaches that
     * collection, so it cannot tell a fix that survives it from one that does not.
     */
    @ParameterizedTest
    @EnumSource(value = ProcessingOrder.class, names = {"KEY", "PARTITION"})
    void aRedeliveredKeyWaitsForItsOldEpochFlightToEnd(ProcessingOrder ordering) {
        givenAnAssignedPartitionUnder(ordering);
        long epochBefore = pm.getEpochOfPartition(TP);

        wm.registerWork(pollOf(recordAt(0, THE_KEY)));
        List<WorkContainer<String, String>> first = wm.getWorkIfAvailable(10);
        assertThat(first).hasSize(1);
        WorkContainer<String, String> oldFlight = first.get(0);
        assertWithMessage("PRECONDITION: the worker is inside the user function")
                .that(oldFlight.isInFlight()).isTrue();

        // the partition is taken away and handed straight back, with the worker still running
        wm.onPartitionsRevoked(UniLists.of(TP));
        wm.onPartitionsAssigned(UniLists.of(TP));
        assertWithMessage("PRECONDITION: the rebalance moved the epoch, so the old flight is stale")
                .that(pm.getEpochOfPartition(TP)).isGreaterThan(epochBefore);
        assertWithMessage("PRECONDITION: nothing drained the worker - the old delivery is still running")
                .that(oldFlight.isInFlight()).isTrue();

        // the offset was never committed, so the partition re-delivers it, plus the one behind it
        wm.registerWork(pollOf(recordAt(0, THE_KEY), recordAt(1, THE_KEY)));

        List<WorkContainer<String, String>> takenWhileOldFlightRuns = wm.getWorkIfAvailable(10);

        assertWithMessage("KEY ORDER VIOLATED: the re-delivered record of key '%s' was handed to a second worker "
                + "while the old-epoch delivery of the same key is still running (astubbs#178). Taken: %s",
                THE_KEY, takenWhileOldFlightRuns)
                .that(takenWhileOldFlightRuns).isEmpty();

        // the old flight returns; its result is dropped as stale, and the key is free again
        wm.handleFutureResult(oldFlight);
        assertThat(oldFlight.isInFlight()).isFalse();

        List<WorkContainer<String, String>> takenAfterwards = wm.getWorkIfAvailable(10);
        assertWithMessage("once the old flight has ended the re-delivered record must be takeable - the wait is "
                + "for the flight, not for the epoch")
                .that(takenAfterwards).hasSize(1);
        assertThat(takenAfterwards.get(0).offset()).isEqualTo(0L);
        assertThat(takenAfterwards.get(0).getEpoch()).isEqualTo(pm.getEpochOfPartition(TP));
        assertWithMessage("the shards hold the two re-delivered records and nothing else - the wait must not "
                + "disturb the population figure the load gate reads")
                .that(sm.getNumberOfRecordsInShards()).isEqualTo(2);
    }

    /**
     * The second way into the same state, named in the inflight note: the {@code Replacing stale entry} branch
     * of {@link ProcessingShard#addWorkContainer} frees the slot when the sweep missed the container. Reached
     * white-box, the way {@code ProcessingShardStaleReplacement909Test} reaches the same branch: an in-flight
     * container from the old epoch is planted as the resident, and the fresh arrival displaces it.
     */
    @Test
    void theDisplacementRouteWaitsForTheOldEpochFlightToo() {
        givenAnAssignedPartitionUnder(KEY);
        long epochBefore = pm.getEpochOfPartition(TP);
        ConsumerRecord<String, String> record = recordAt(0, THE_KEY);

        // establish the shard through the normal path, then make everything in it stale
        sm.addWorkContainer(epochBefore, record);
        wm.onPartitionsRevoked(UniLists.of(TP));
        wm.onPartitionsAssigned(UniLists.of(TP));
        long epochAfter = pm.getEpochOfPartition(TP);
        var shard = sm.getShard(ShardKey.of(record, KEY));
        assertWithMessage("PRECONDITION: the shard survives the sweep - it is emptied, not removed")
                .that(shard.isPresent()).isTrue();

        // a stale container the sweep missed, whose worker is still running
        var oldFlight = new WorkContainer<>(epochBefore, record, module);
        assertThat(oldFlight.onQueueingForExecution()).isTrue();
        shard.get().plantResident(oldFlight);

        // the fresh arrival displaces it
        sm.addWorkContainer(epochAfter, recordAt(0, THE_KEY));
        assertWithMessage("PRECONDITION: the displacement branch fired - the fresh container holds the offset")
                .that(shard.get().getWorkContainerAtOffset(0L).get().getEpoch()).isEqualTo(epochAfter);

        assertWithMessage("KEY ORDER VIOLATED through the displacement branch: the fresh container was handed "
                + "out while the displaced one is still running on a worker (astubbs#178)")
                .that(wm.getWorkIfAvailable(10)).isEmpty();

        // the old flight ends the way a dropped stale result ends it
        oldFlight.endFlight();
        assertThat(wm.getWorkIfAvailable(10)).hasSize(1);
    }

    /**
     * The state every control arm starts from: one record of {@link #THE_KEY} taken and still out at a worker,
     * and the partition revoked and handed straight back so that flight is stale but running.
     */
    private WorkContainer<String, String> givenAnOldEpochFlightStillRunningAfterARevokeAndReassign() {
        wm.registerWork(pollOf(recordAt(0, THE_KEY)));
        WorkContainer<String, String> oldFlight = wm.getWorkIfAvailable(10).get(0);
        wm.onPartitionsRevoked(UniLists.of(TP));
        wm.onPartitionsAssigned(UniLists.of(TP));
        assertWithMessage("PRECONDITION: nothing drained the worker").that(oldFlight.isInFlight()).isTrue();
        return oldFlight;
    }

    /** Control: the wait is per shard. Another key's re-delivery is not held back by this key's old flight. */
    @Test
    void anotherKeyIsNotHeldBackByThisKeysOldEpochFlight() {
        givenAnAssignedPartitionUnder(KEY);
        givenAnOldEpochFlightStillRunningAfterARevokeAndReassign();

        wm.registerWork(pollOf(recordAt(0, THE_KEY), recordAt(1, "another-key")));

        List<WorkContainer<String, String>> taken = wm.getWorkIfAvailable(10);
        assertWithMessage("only the key with a flight outstanding waits; the other key's record is free to go")
                .that(taken).hasSize(1);
        assertThat(taken.get(0).getCr().key()).isEqualTo("another-key");
    }

    /**
     * The same control under PARTITION, where "another shard" means another partition. The departure set is per
     * shard and the scan asks only the shard it is reading, so partition 1's record goes out while partition 0's
     * old-epoch flight is still running. This is the arm the KEY control above cannot reach - under KEY both keys
     * share the partition - and it is what pins that the gate withholds one shard's work rather than the
     * instance's: a starvation across partitions with no revoke in play is NOT something this gate can cause.
     */
    @Test
    void anotherPartitionIsNotHeldBackByThisPartitionsOldEpochFlight() {
        givenAnAssignedPartitionUnder(PARTITION);
        TopicPartition otherPartition = new TopicPartition(TOPIC, 1);
        wm.onPartitionsAssigned(UniLists.of(otherPartition));
        givenAnOldEpochFlightStillRunningAfterARevokeAndReassign();

        var otherPartitionsRecord = new ConsumerRecord<>(TOPIC, 1, 0L, "other-key", "v-other");
        var bothPartitions = new ConsumerRecords<>(UniMaps.of(
                TP, UniLists.of(recordAt(0, THE_KEY)),
                otherPartition, UniLists.of(otherPartitionsRecord)));
        wm.registerWork(new EpochAndRecordsMap<>(bothPartitions, pm));

        List<WorkContainer<String, String>> taken = wm.getWorkIfAvailable(10);
        assertWithMessage("only the partition with a flight outstanding waits; the other partition's record is free "
                + "to go")
                .that(taken).hasSize(1);
        assertThat(taken.get(0).getTopicPartition()).isEqualTo(otherPartition);
    }

    /**
     * Control: {@link ProcessingOrder#UNORDERED} promises no order, so there is nothing to wait for - the fresh
     * container is handed out at once, exactly as before. This is what keeps the fix from costing the mode that
     * exists to have no such constraint.
     */
    @Test
    void unorderedMakesNoPromiseAndIsNotHeldBack() {
        givenAnAssignedPartitionUnder(UNORDERED);
        givenAnOldEpochFlightStillRunningAfterARevokeAndReassign();

        wm.registerWork(pollOf(recordAt(0, THE_KEY)));

        assertWithMessage("UNORDERED made no ordering promise, so the re-delivery is not held back")
                .that(wm.getWorkIfAvailable(10)).hasSize(1);
    }

    /**
     * Under KEY ordering an empty shard is garbage-collected. A shard that is empty but has an in-flight departure
     * must survive that collection - or the memory of the flight goes with it, which is the first arm - and
     * must then be collected once the flight has ended, or every key in flight at a revoke that is never
     * re-delivered here leaks one shard for the life of the instance.
     */
    @Test
    void aShardWithAnInFlightDepartureSurvivesCollectionUntilTheFlightEnds() {
        givenAnAssignedPartitionUnder(KEY);
        ShardKey key = ShardKey.of(recordAt(0, THE_KEY), KEY);
        WorkContainer<String, String> oldFlight = givenAnOldEpochFlightStillRunningAfterARevokeAndReassign();

        assertWithMessage("the shard is empty of work but the old flight is still out, so it must not be collected")
                .that(sm.getShard(key).isPresent()).isTrue();
        assertThat(sm.getShard(key).get().getCountOfWorkTracked()).isEqualTo(0);

        // no re-delivery ever arrives here (the partition could equally have gone elsewhere); the flight ends
        wm.handleFutureResult(oldFlight);
        // the scan is what clears the departure, and there is nothing for it to hand out
        assertThat(wm.getWorkIfAvailable(10)).isEmpty();

        assertWithMessage("once the flight has ended and a scan has seen that, the empty shard is collected")
                .that(sm.getShard(key).isPresent()).isFalse();
    }

    private static ConsumerRecord<String, String> recordAt(long offset, String key) {
        return new ConsumerRecord<>(TOPIC, 0, offset, key, "v-" + offset);
    }

    /** A poll batch capturing the CURRENT epoch, the way a real poll does. */
    @SafeVarargs
    private final EpochAndRecordsMap<String, String> pollOf(ConsumerRecord<String, String>... records) {
        var consumerRecords = new ConsumerRecords<>(UniMaps.of(TP, UniLists.of(records)));
        return new EpochAndRecordsMap<>(consumerRecords, pm);
    }
}
