package bz.stub.parallelconsumer.state;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.EpochAndRecordsMap;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.truth.Truth.assertWithMessage;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * Reproduces candidate 2 of the torn-read family dossier: {@code ShardManager.removeWorkFromShardFor} checks
 * {@code containsKey} and then calls {@code get}, dereferencing the result unconditionally. Under KEY ordering the
 * control thread's {@code onSuccess -> removeShardIfEmpty} removes empty shards from the same map, and the revoke
 * sweep ({@code PartitionState.onPartitionsRemoved -> removeAnyShardEntriesReferencedFrom}) runs on the
 * broker-poll thread inside {@code consumer.poll}'s rebalance callback - nothing serialises the two (the only
 * shared monitor on the revoke path is {@code commitCommand}, which covers commit collection, not the sweep; see
 * docs/solutions/runtime-errors/revoke-path-commit-deadlock-between-poll-and-control-threads.md). A removal
 * landing between the two adjacent map reads turns {@code get} into {@code null} and the next line into an NPE
 * that flies out of the {@link org.apache.kafka.clients.consumer.ConsumerRebalanceListener} into
 * {@code consumer.poll} - the poller-death family.
 * <p>
 * Every other access in the file already uses the single-read {@code getShard(key)} Optional idiom; this is the
 * one remaining check-then-get pair - and it sits inside the guard that was itself added for confluentinc#757
 * ({@code ShardManagerTest.testAssignedQuickRevokeNPE}), which closed the shard-already-gone case but left the
 * window between its own two reads.
 * <p>
 * <b>The forced race.</b> The shard map is replaced (production package setter, as
 * {@code ShardManagerTest.testAssignedQuickRevokeNPE} already does) with a double that fires the interfering
 * mutation from the sweep's first read of the armed key - and the mutation is the real production path, not a hand-rolled map
 * edit: {@link ShardManager#onSuccess} on the shard's last container, exactly what the control thread does when
 * that container's result arrives. One logical operation, two reads, the second contradicting the first.
 * <p>
 * <b>Before the fix, {@link #revokeSweepMustSurviveAShardRemovedBetweenItsTwoReads} was RED</b> - the NPE was the
 * defect under test, reproduced deterministically on the hunt branch {@code test/torn-read-candidates-reproduction}
 * before the fix existed. The control test - the identical mutation fired <em>before</em> the sweep instead of
 * against its read - is green either way, so a failure is the interleaving's, not the fixture's. With
 * {@code removeWorkFromShardFor} on the single-read idiom, both are green and this file is the fix's regression
 * test.
 *
 * @author Antony Stubbs
 */
@Slf4j
class ShardManagerRevokeSweepNpeTest {

    static final String TOPIC = "myTopic";
    final TopicPartition tp = new TopicPartition(TOPIC, 0);

    final PCModuleTestEnv module = new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder()
            .ordering(KEY)
            .consumer(new MockConsumer<>(OffsetResetStrategy.EARLIEST))
            .build());

    final WorkManager<String, String> wm = module.workManager();
    final ShardManager<String, String> sm = wm.getSm();

    /**
     * A shard map that fires an interfering mutation from inside the sweep's first read of the armed key -
     * {@code containsKey} on the original check-then-get code (the instant between its two reads), or {@code get}
     * on the fixed single-read code (the instant immediately after the one read, whose returned reference the
     * sweep must then tolerate operating on). Firing on whichever read comes first keeps the seam live across the
     * fix: the defective code still tears (containsKey true, then get null), while the fixed code must survive
     * the same removal landing against its single read. Tracks firing in an explicit {@code raceFired} boolean: a
     * cleared armed-flag cannot tell "armed, then fired" from "never armed", so the guard would pass on a test
     * that forgot to arm.
     */
    static class RacingShardMap extends ConcurrentHashMap<ShardKey, ProcessingShard<String, String>> {
        private transient Runnable interference;
        private transient ShardKey armedKey;
        private boolean raceFired;

        void arm(ShardKey key, Runnable interference) {
            this.armedKey = key;
            this.interference = interference;
        }

        boolean raceHasFired() {
            return raceFired;
        }

        @Override
        public boolean containsKey(Object key) {
            boolean present = super.containsKey(key);
            if (present && armedKey != null && armedKey.equals(key)) {
                fire();
            }
            return present;
        }

        @Override
        public ProcessingShard<String, String> get(Object key) {
            // read first, then fire: the interference models a removal landing AFTER this read completes, so the
            // caller is handed the pre-removal reference and must cope with the map having moved on
            ProcessingShard<String, String> shard = super.get(key);
            if (shard != null && armedKey != null && armedKey.equals(key)) {
                fire();
            }
            return shard;
        }

        private void fire() {
            armedKey = null;
            raceFired = true;
            interference.run();
        }
    }

    final RacingShardMap racingShardMap = new RacingShardMap();

    /** Registers one record through the production path, so shard and incomplete tracking are both real. */
    private WorkContainer<String, String> registerOneRecordAndTakeIt() {
        sm.setProcessingShards(racingShardMap);
        wm.onPartitionsAssigned(UniLists.of(tp));

        var record = new ConsumerRecord<>(TOPIC, tp.partition(), 0, "key-0", "value");
        var records = new ConsumerRecords<>(UniMaps.of(tp, UniLists.of(record)));
        wm.registerWork(new EpochAndRecordsMap<>(records, wm.getPm()));

        List<WorkContainer<String, String>> taken = wm.getWorkIfAvailable();
        assertWithMessage("fixture: exactly the one registered container must be selectable")
                .that(taken).hasSize(1);
        return taken.get(0);
    }

    /**
     * The interleaving under test. The last container's success lands between {@code containsKey} and
     * {@code get}; under KEY ordering that removes the now-empty shard, and the sweep dereferences null.
     * <p>
     * The contract asserted is the trivially correct one - a revoke sweep must never throw out of the rebalance
     * listener, whatever completes concurrently. RED on master: the NPE from
     * {@code ShardManager.removeWorkFromShardFor}'s unconditional dereference.
     */
    @Test
    void revokeSweepMustSurviveAShardRemovedBetweenItsTwoReads() {
        WorkContainer<String, String> wc = registerOneRecordAndTakeIt();

        // the control thread's production mutation, scheduled to land between the sweep's two map reads
        racingShardMap.arm(sm.computeShardKey(wc), () -> sm.onSuccess(wc));

        assertDoesNotThrow(() -> wm.onPartitionsRevoked(UniLists.of(tp)),
                "candidate 2: a shard removal landing between removeWorkFromShardFor's containsKey and get "
                        + "must not escape the rebalance listener as an NPE - in production this kills "
                        + "consumer.poll on the broker-poll thread");

        assertWithMessage("the armed race must actually have fired - otherwise this test exercised nothing")
                .that(racingShardMap.raceHasFired())
                .isTrue();
    }

    /**
     * Control arm: the identical mutation - same container, same production {@code onSuccess} path, same
     * shard-removal consequence - fired immediately BEFORE the sweep instead of between its two reads. The sweep
     * then sees {@code containsKey == false} and takes its already-removed branch gracefully. Green on master:
     * the fixture is innocent, the interleaving is the failure.
     */
    @Test
    void sameShardRemovalBeforeTheSweepIsHandledGracefully() {
        WorkContainer<String, String> wc = registerOneRecordAndTakeIt();

        // same magnitude, different position: the completion lands wholly before the sweep starts
        sm.onSuccess(wc);
        assertWithMessage("fixture: the completion must have removed the now-empty shard, as KEY ordering does")
                .that(sm.getShard(sm.computeShardKey(wc)).isPresent())
                .isFalse();

        assertDoesNotThrow(() -> wm.onPartitionsRevoked(UniLists.of(tp)),
                "the already-removed-shard branch is guarded (confluentinc#757) and must stay graceful");

        assertWithMessage("control arm must not have armed the race")
                .that(racingShardMap.raceHasFired())
                .isFalse();
    }
}
