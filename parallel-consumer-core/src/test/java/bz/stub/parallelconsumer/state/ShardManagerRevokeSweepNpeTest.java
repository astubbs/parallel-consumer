package bz.stub.parallelconsumer.state;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

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
 * one remaining check-then-get pair.
 * <p>
 * <b>Which guard belongs to which NPE, because the method now carries three.</b> The {@code containsKey} test
 * has been in {@code removeWorkFromShardFor} since upstream's 2022 batching work, long predating
 * confluentinc#757. What confluentinc#757 - "NullPointerException on partitions revoked", closed by upstream's
 * PR 758 - actually added is the {@code Objects.nonNull(removedWC)} check one line further down, against a
 * {@code retryQueue.remove(null)}, with {@code ShardManagerTest.testAssignedQuickRevokeNPE} for it. So the
 * already-removed-<em>work</em> case is guarded and the already-removed-<em>shard</em> case is guarded, and
 * this is the third null on the same revoke path: the shard the {@code containsKey} test just saw, gone by
 * the time {@code get} runs.
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
        // inherited from ConcurrentHashMap, which is Serializable; this double is never serialised, but a
        // Serializable class without one is a real finding rather than a special case worth arguing
        private static final long serialVersionUID = 1L;

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

        // The three fields above are race scaffolding, not value state: two of these holding the same shards
        // are equal as maps whatever either has armed, so ConcurrentHashMap's entry-wise equality is already
        // the right answer. Declared rather than inherited because subclassing a collection and adding
        // fields otherwise reads as a forgotten equals - which is exactly what SpotBugs
        // EQ_DOESNT_OVERRIDE_EQUALS is for, and it cannot tell scaffolding from state.
        //
        // hashCode is deliberately NOT declared alongside it. The usual "override both together" rule guards
        // against an equals paired with Object's identity hashCode; here both come from AbstractMap and are
        // already consistent with each other, so redeclaring hashCode would change nothing and SpotBugs
        // rightly flags the no-op (COM_PARENT_DELEGATED_CALL). equals earns its declaration by answering a
        // real question; hashCode would only be echoing its parent.
        @Override
        public boolean equals(Object other) {
            return super.equals(other);
        }
    }

    final RacingShardMap racingShardMap = new RacingShardMap();

    /** Registers one record through the production path, so shard and incomplete tracking are both real. */
    private WorkContainer<String, String> registerOneRecordAndTakeIt() {
        // the racing map must be in place BEFORE the assignment that creates the shard
        sm.setProcessingShards(racingShardMap);
        return ModelUtils.registerOneRecordAndTakeIt(wm, tp);
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
                "the already-removed-shard branch has been guarded since 2022 and must stay graceful");

        assertWithMessage("control arm must not have armed the race")
                .that(racingShardMap.raceHasFired())
                .isFalse();
    }
}
