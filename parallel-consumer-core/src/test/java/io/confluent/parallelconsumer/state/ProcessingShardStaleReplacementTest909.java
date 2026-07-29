package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2026 Confluent, Inc. and contributors
 */

import io.confluent.parallelconsumer.internal.PCModuleTestEnv;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.util.List;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Regression test for upstream PR #909: a stale {@link WorkContainer} at the same offset
 * blocks fresh work from being added after rebalance.
 * <p>
 * The race condition: during rebalance, {@code removeStaleContainers()} runs but can't clean
 * shards that don't exist yet. When the control thread resumes processing the old batch, it
 * adds a stale container to a newly-created shard. The next poll's fresh container for the
 * same offset is then dropped because {@code addWorkContainer} sees "entry already exists."
 *
 * @see <a href="https://github.com/confluentinc/parallel-consumer/pull/909">confluentinc/parallel-consumer#909</a>
 * @see ProcessingShard#addWorkContainer(WorkContainer)
 */
@Slf4j
class ProcessingShardStaleReplacementTest909 {

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
     * Exact reproduction of the PR #909 race condition timeline:
     * <ol>
     *   <li>Control thread adds work at offset 200 for key "K_B" at epoch N</li>
     *   <li>Rebalance happens (epoch advances to N+2)</li>
     *   <li>Control thread (still processing old batch) adds STALE work at offset 300
     *       for key "K_B" at OLD epoch N — this creates shard K_B with a stale entry</li>
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
        assertThat(epoch2).isGreaterThan(epoch0);

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

        // Add work at offset 100
        addWork(epoch0, 100, "K_A");

        // Try to add duplicate at same offset, same epoch — should be dropped
        addWork(epoch0, 100, "K_A");

        // Should only have one entry
        List<WorkContainer<String, String>> work = sm.getWorkIfAvailable(100);
        long count = work.stream().filter(wc -> wc.offset() == 100).count();
        assertThat(count).isEqualTo(1);
    }

    private void addWork(long epoch, long offset, String key) {
        var record = new ConsumerRecord<>(topic, 0, offset, key, "value");
        sm.addWorkContainer(epoch, record);
    }
}
