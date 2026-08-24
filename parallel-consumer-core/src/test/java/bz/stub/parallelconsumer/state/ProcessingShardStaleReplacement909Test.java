package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.EpochAndRecordsMap;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Regression test for confluentinc#909: a stale {@link WorkContainer} at the same offset
 * blocks fresh work from being added after rebalance.
 * <p>
 * The race condition: {@code removeStaleContainers()} is a point-in-time sweep, and nothing
 * orders it against a concurrent add on the control thread. A control thread still draining the
 * pre-rebalance batch adds a container carrying the OLD epoch after the sweep has passed - into
 * an existing shard, or one {@code computeIfAbsent} creates. The next poll's fresh container for
 * the same offset is then dropped because {@code addWorkContainer} sees "entry already exists."
 * <p>
 * Note the shard does NOT have to be absent: {@code removeStaleContainers()} empties shards
 * without removing them, so this test reproduces the bug with the shard already present.
 *
 * @see <a href="https://github.com/confluentinc/parallel-consumer/pull/909">confluentinc/parallel-consumer#909</a>
 * @see ProcessingShard#addWorkContainer(WorkContainer)
 */
@Slf4j
class ProcessingShardStaleReplacement909Test {

    ModelUtils mu = new ModelUtils();
    WorkManager<String, String> wm;
    ShardManager<String, String> sm;
    PartitionStateManager<String, String> pm;

    String topic = "topic";
    TopicPartition tp = new TopicPartition(topic, 0);

    @BeforeEach
    void setup() {
        PCModuleTestEnv module = mu.getModule();
        wm = module.workManager();
        sm = wm.getSm();
        pm = wm.getPm();

        // initial assignment at epoch 0
        wm.onPartitionsAssigned(UniLists.of(tp));
    }

    /**
     * Exact reproduction of the confluentinc#909 race condition timeline:
     * <ol>
     *   <li>Control thread adds work at offset 200 for key "K_B" at epoch N</li>
     *   <li>Rebalance happens (epoch advances to N+2)</li>
     *   <li>Control thread (still processing old batch) adds STALE work at offset 300
     *       for key "K_B" at OLD epoch N — shard K_B still exists, now holding a stale entry</li>
     *   <li>New poll adds FRESH work at offset 300 for key "K_B" at NEW epoch N+2</li>
     *   <li>BUG (before fix): fresh work is DROPPED because offset 300 already exists</li>
     *   <li>FIX: stale entry is replaced with fresh one</li>
     * </ol>
     */
    @Test
    void staleContainerAtSameOffsetShouldBeReplacedByFreshOne() {
        long epoch0 = pm.getEpochOfPartition(tp);

        // Step 1: add initial work at epoch 0
        addWork(epoch0, 100, "K_A");
        addWork(epoch0, 200, "K_B");

        // Step 2: rebalance (epoch 0 → epoch 2: revoke increments, assign increments again)
        wm.onPartitionsRevoked(UniLists.of(tp));
        wm.onPartitionsAssigned(UniLists.of(tp));
        long epoch2 = pm.getEpochOfPartition(tp);
        // deterministic under eager rebalancing: revoke increments, assign increments again
        assertThat(epoch2).isEqualTo(epoch0 + 2);

        // Step 3: late-arriving stale work at offset 300 from old epoch
        // (control thread still processing old batch after rebalance)
        addWork(epoch0, 300, "K_B");

        // Step 4: new poll adds fresh work at the SAME offset 300, new epoch
        addWork(epoch2, 300, "K_B");

        // Step 5: verify the fresh work replaced the stale entry
        List<WorkContainer<String, String>> work = sm.getWorkIfAvailable(100);
        var offset300 = work.stream().filter(wc -> wc.offset() == 300).findFirst();

        assertWithMessage("Fresh work at offset 300 should be available (not blocked by stale entry). " +
                "See https://github.com/confluentinc/parallel-consumer/pull/909")
                .that(offset300.isPresent()).isTrue();
        assertWithMessage("Work at offset 300 should be from the new epoch, not the stale one")
                .that(offset300.get().getEpoch()).isEqualTo(epoch2);
    }

    /**
     * Verify that a non-stale duplicate at the same offset is still correctly dropped
     * (preserving original behavior).
     */
    @Test
    void nonStaleDuplicateAtSameOffsetShouldStillBeDropped() {
        long epoch0 = pm.getEpochOfPartition(tp);

        // The two adds carry DISTINGUISHABLE values, because entries is keyed by offset: asserting only
        // that one entry survives passes whether the duplicate was dropped or replaced, so it cannot tell
        // the intended behaviour from an "always replace" regression. The value is what discriminates.
        addWork(epoch0, 100, "K_A", "first");

        // Try to add duplicate at same offset, same epoch — should be dropped
        addWork(epoch0, 100, "K_A", "second");

        List<WorkContainer<String, String>> work = sm.getWorkIfAvailable(100);
        var offset100 = work.stream().filter(wc -> wc.offset() == 100).collect(Collectors.toList());

        assertThat(offset100).hasSize(1);
        assertWithMessage("The FIRST container must survive - a non-stale duplicate is dropped, not replaced")
                .that(offset100.get(0).getCr().value()).isEqualTo("first");
    }


    /**
     * The same defect driven through the REAL registration path, rather than by calling
     * {@link ShardManager#addWorkContainer} directly.
     * <p>
     * This is the stronger arm: {@code maybeRegisterNewPollBatchAsWork} guards the batch with
     * {@code epochIsStale()}, and that guard is exactly what fails to fence the late add. It cannot
     * fence it once the state it is called on has itself gone stale: {@code partitionsAssignmentEpoch}
     * is a {@code final long} captured when the {@link PartitionState} is constructed, so a stale state
     * compares its own old epoch against the batch's old epoch, finds them equal, and waves the whole
     * batch through.
     * <p>
     * There are TWO sub-cases, and this test drives the first. (a) The state the lookup returned has
     * itself gone stale, so the check passes against its own captured epoch. (b) The check passes
     * LEGITIMATELY against the then-live state, and the rebalance lands mid-loop, after the check but
     * before the remaining inserts - which is the timeline the confluentinc#909 reporter described.
     * Both are the same check-then-act race; (a) merely widens how late a doomed batch can still be
     * waved through. The fix is indifferent to which one occurred, because it acts at the insert.
     * <p>
     * The window is real but TEMPORAL, not structural. {@code maybeRegisterNewRecordAsWork} looks the
     * state up live ({@code partitionStates.get(tp)}) and then holds it in a local across its
     * per-record loop: a rebalance landing before that lookup fences correctly, while one landing
     * after it leaves the loop inserting old-epoch containers into the LIVE shard manager. Holding
     * the reference across the rebalance here is a faithful stand-in for that local - it models the
     * window deterministically rather than racing for it, which is why no latch or hook is needed.
     */
    @Test
    void staleBatchRegisteredThroughTheRealGuardMustNotBlockFreshWork() {
        long epoch0 = pm.getEpochOfPartition(tp);

        // The control thread has polled a batch at epoch 0 and is about to drain it.
        var stalePoll = pollBatchAt(500);
        // ...and the live lookup handed it the state current at that moment - the same reference
        // maybeRegisterNewRecordAsWork holds in a local across its per-record loop.
        PartitionState<String, String> preRebalanceState = pm.getPartitionState(tp);

        // Rebalance happens on the poller thread while that drain is still pending.
        wm.onPartitionsRevoked(UniLists.of(tp));
        wm.onPartitionsAssigned(UniLists.of(tp));
        long epoch2 = pm.getEpochOfPartition(tp);
        assertThat(epoch2).isEqualTo(epoch0 + 2);

        // The drain completes against the stale state. The guard does NOT fence it - assert that,
        // so this test fails loudly if the mechanism it is built on ever changes.
        preRebalanceState.maybeRegisterNewPollBatchAsWork(stalePoll.records(tp));
        assertWithMessage("PRECONDITION: the stale batch must actually reach the shard. If the guard "
                + "ever starts fencing it, this test stops exercising the confluentinc#909 window and "
                + "must be rewritten rather than left silently passing")
                .that(sm.getNumberOfWorkQueuedInShardsAwaitingSelection()).isEqualTo(1);

        // Next poll delivers the SAME offset at the new epoch.
        wm.registerWork(pollBatchAt(500));

        List<WorkContainer<String, String>> work = sm.getWorkIfAvailable(100);
        var offset500 = work.stream().filter(wc -> wc.offset() == 500).findFirst();

        assertWithMessage("Fresh work at offset 500 must survive a stale batch registered through the "
                + "real guard. See https://github.com/confluentinc/parallel-consumer/pull/909")
                .that(offset500.isPresent()).isTrue();
        assertWithMessage("Surviving work at offset 500 must carry the NEW epoch")
                .that(offset500.get().getEpoch()).isEqualTo(epoch2);
    }



    /**
     * The stale-RESIDENT replacement branch, reached white-box.
     * <p>
     * The other tests here reach this branch through {@link ShardManager#addWorkContainer}, which is
     * faithful but leaves the branch's coverage dependent on stale arrivals being admitted at all. This
     * one plants the resident directly in {@code entries} and then offers the fresh record through the
     * normal call, so the branch stays covered on its own terms. Without it the fresh record is dropped
     * and the offset stays wedged - confluentinc#909 exactly.
     */
    @Test
    void staleResidentPlantedDirectlyIsReplacedByFreshWork() {
        long epoch0 = pm.getEpochOfPartition(tp);

        // Establish the shard through the normal path, at the current epoch.
        addWork(epoch0, 700, "K_D");

        wm.onPartitionsRevoked(UniLists.of(tp));
        wm.onPartitionsAssigned(UniLists.of(tp));
        long epoch2 = pm.getEpochOfPartition(tp);

        var shard = sm.getShard(ShardKey.of(recordAt(700, "K_D"), mu.getModule().options().getOrdering()));
        assertWithMessage("PRECONDITION: the shard must still exist - removeStaleContainers empties "
                + "shards without removing them, which is what leaves this window open at all")
                .that(shard.isPresent()).isTrue();

        var staleWc = new WorkContainer<String, String>(epoch0, recordAt(700, "K_D"), mu.getModule());
        shard.get().getEntries().put(700L, staleWc);
        assertWithMessage("PRECONDITION: the stale resident is in place")
                .that(shard.get().getEntries().get(700L).getEpoch()).isEqualTo(epoch0);

        addWork(epoch2, 700, "K_D");

        assertWithMessage("The fresh container must have REPLACED the stale resident, not been dropped. "
                + "See https://github.com/confluentinc/parallel-consumer/pull/909")
                .that(shard.get().getEntries().get(700L).getEpoch()).isEqualTo(epoch2);
    }

    private ConsumerRecord<String, String> recordAt(long offset, String key) {
        return new ConsumerRecord<>(topic, 0, offset, key, "v-" + offset);
    }

    /** A poll batch for a single offset, capturing the CURRENT epoch the way a real poll does. */
    private EpochAndRecordsMap<String, String> pollBatchAt(long offset) {
        var record = new ConsumerRecord<>(topic, 0, offset, "K_C", "v-" + offset);
        var records = new ConsumerRecords<>(UniMaps.of(tp, UniLists.of(record)));
        return new EpochAndRecordsMap<>(records, pm);
    }

    private void addWork(long epoch, long offset, String key) {
        addWork(epoch, offset, key, "value");
    }

    private void addWork(long epoch, long offset, String key, String value) {
        var record = new ConsumerRecord<>(topic, 0, offset, key, value);
        sm.addWorkContainer(epoch, record);
    }
}
