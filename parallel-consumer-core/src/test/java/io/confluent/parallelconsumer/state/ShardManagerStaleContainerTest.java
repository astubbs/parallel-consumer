package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2026 Confluent, Inc. and contributors
 */

import io.confluent.parallelconsumer.internal.PCModuleTestEnv;
import io.confluent.parallelconsumer.offsets.OffsetMapCodecManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Deterministic unit tests for stale container handling across rebalances.
 * <p>
 * These tests construct the exact scenarios suspected in
 * <a href="https://github.com/confluentinc/parallel-consumer/issues/857">#857</a>
 * without requiring a broker, so they run fast and fail deterministically.
 *
 * @see ShardManager
 * @see ProcessingShard
 * @see PartitionStateManager
 */
@Slf4j
class ShardManagerStaleContainerTest {

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
     * Core reproduction scenario for #857: after a revoke+reassign cycle, stale work containers
     * from the old epoch should not block new work from being taken.
     */
    @Test
    void staleContainerShouldNotBlockNewWorkAfterRebalance() {
        long initialEpoch = pm.getEpochOfPartition(tp);

        // Add work at the initial epoch
        for (int i = 0; i < 5; i++) {
            sm.addWorkContainer(initialEpoch, new ConsumerRecord<>(topic, 0, i, "key-" + i, "value"));
        }

        // Verify we can take work (sanity check)
        List<WorkContainer<String, String>> initialWork = sm.getWorkIfAvailable(10);
        assertThat(initialWork).hasSize(5);

        // Simulate revoke → reassign (epoch goes from N to N+2)
        wm.onPartitionsRevoked(UniLists.of(tp));
        wm.onPartitionsAssigned(UniLists.of(tp));

        long newEpoch = pm.getEpochOfPartition(tp);
        assertWithMessage("Epoch should have advanced past initial")
                .that(newEpoch).isGreaterThan(initialEpoch);

        // Add new work at the new epoch
        for (int i = 10; i < 15; i++) {
            sm.addWorkContainer(newEpoch, new ConsumerRecord<>(topic, 0, i, "key-" + i, "value"));
        }

        // Also inject a "late arriving" container with the OLD epoch — simulates a poll
        // that was in-flight during the rebalance and arrived after reassignment
        sm.addWorkContainer(initialEpoch, new ConsumerRecord<>(topic, 0, 99, "key-stale", "value"));

        // Now try to take work — the new epoch's containers should be returned,
        // and the stale one should not block them
        List<WorkContainer<String, String>> workAfterRebalance = sm.getWorkIfAvailable(10);

        assertWithMessage("Should be able to take new-epoch work after rebalance. " +
                "If this fails with 0, stale containers are blocking the shard — this is bug #857.")
                .that(workAfterRebalance).isNotEmpty();

        // Verify the returned work is from the new epoch
        for (var wc : workAfterRebalance) {
            assertWithMessage("Returned work should be from new epoch, not stale")
                    .that(wc.getEpoch()).isEqualTo(newEpoch);
        }
    }

    /**
     * Rapid successive rebalances (revoke→assign→revoke→assign) should clean up all stale
     * containers and not leave any behind to block future work.
     */
    @Test
    void multipleRapidRebalancesShouldNotLeaveStaleContainers() {
        long epoch0 = pm.getEpochOfPartition(tp);

        // Add work at epoch 0
        sm.addWorkContainer(epoch0, new ConsumerRecord<>(topic, 0, 0, "key-0", "value"));
        sm.addWorkContainer(epoch0, new ConsumerRecord<>(topic, 0, 1, "key-1", "value"));

        // Rapid rebalance cycle 1
        wm.onPartitionsRevoked(UniLists.of(tp));
        wm.onPartitionsAssigned(UniLists.of(tp));

        long epoch1 = pm.getEpochOfPartition(tp);
        sm.addWorkContainer(epoch1, new ConsumerRecord<>(topic, 0, 2, "key-2", "value"));

        // Rapid rebalance cycle 2
        wm.onPartitionsRevoked(UniLists.of(tp));
        wm.onPartitionsAssigned(UniLists.of(tp));

        long epoch2 = pm.getEpochOfPartition(tp);
        sm.addWorkContainer(epoch2, new ConsumerRecord<>(topic, 0, 3, "key-3", "value"));

        // Rapid rebalance cycle 3
        wm.onPartitionsRevoked(UniLists.of(tp));
        wm.onPartitionsAssigned(UniLists.of(tp));

        long finalEpoch = pm.getEpochOfPartition(tp);
        assertThat(finalEpoch).isGreaterThan(epoch2);

        // Add work at the final epoch
        sm.addWorkContainer(finalEpoch, new ConsumerRecord<>(topic, 0, 10, "key-fresh", "value"));

        // Explicitly run stale removal
        long staleCount = sm.removeStaleContainers();
        log.info("Removed {} stale containers after rapid rebalances", staleCount);

        // Take work — should get the fresh container
        List<WorkContainer<String, String>> work = sm.getWorkIfAvailable(10);
        assertWithMessage("Should get fresh work after rapid rebalances")
                .that(work).isNotEmpty();

        for (var wc : work) {
            assertWithMessage("All returned work should be from final epoch")
                    .that(wc.getEpoch()).isEqualTo(finalEpoch);
        }
    }

    /**
     * Verify that the stale container removal actually removes containers from all prior epochs,
     * not just the immediately previous one.
     */
    @Test
    void staleRemovalShouldCatchContainersFromAllPriorEpochs() {
        long epoch0 = pm.getEpochOfPartition(tp);
        sm.addWorkContainer(epoch0, new ConsumerRecord<>(topic, 0, 0, "key-e0", "value"));

        wm.onPartitionsRevoked(UniLists.of(tp));
        wm.onPartitionsAssigned(UniLists.of(tp));

        long epoch1 = pm.getEpochOfPartition(tp);
        sm.addWorkContainer(epoch1, new ConsumerRecord<>(topic, 0, 1, "key-e1", "value"));

        wm.onPartitionsRevoked(UniLists.of(tp));
        wm.onPartitionsAssigned(UniLists.of(tp));

        long epoch2 = pm.getEpochOfPartition(tp);

        // Now inject containers from BOTH old epochs (simulating two separate late polls)
        sm.addWorkContainer(epoch0, new ConsumerRecord<>(topic, 0, 90, "key-stale-e0", "value"));
        sm.addWorkContainer(epoch1, new ConsumerRecord<>(topic, 0, 91, "key-stale-e1", "value"));

        // Add fresh work
        sm.addWorkContainer(epoch2, new ConsumerRecord<>(topic, 0, 10, "key-fresh", "value"));

        // Take work — stale containers from both old epochs should not block
        List<WorkContainer<String, String>> work = sm.getWorkIfAvailable(10);

        assertWithMessage("Fresh work should be available despite stale containers from multiple epochs")
                .that(work).isNotEmpty();

        for (var wc : work) {
            assertWithMessage("No stale work should be returned")
                    .that(wc.getEpoch()).isEqualTo(epoch2);
        }
    }
}
