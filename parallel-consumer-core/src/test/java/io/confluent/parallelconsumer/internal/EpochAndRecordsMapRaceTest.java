package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2026 Confluent, Inc. and contributors
 */

import io.confluent.parallelconsumer.state.ModelUtils;
import io.confluent.parallelconsumer.state.PartitionStateManager;
import io.confluent.parallelconsumer.state.ShardManager;
import io.confluent.parallelconsumer.state.WorkContainer;
import io.confluent.parallelconsumer.state.WorkManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.List;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Verifies that the epoch initialization race is handled safely:
 * <ol>
 *   <li>poll() returns records for a partition before onPartitionsAssigned() fires</li>
 *   <li>Records are safely skipped (no NPE crash)</li>
 *   <li>onPartitionsAssigned() fires, establishing the epoch and partition state</li>
 *   <li>Next poll creates valid work at the correct epoch</li>
 * </ol>
 * This race is more likely with Kafka 2.x's eager rebalance protocol.
 */
@Slf4j
class EpochAndRecordsMapRaceTest {

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
        // Deliberately NOT calling onPartitionsAssigned — simulating the race
    }

    /**
     * Core race scenario: poll returns records before onPartitionsAssigned fires.
     * Records should be safely skipped (no NPE), and the map should be empty.
     */
    @Test
    void pollBeforeAssignmentShouldSkipRecordsNotCrash() {
        // No onPartitionsAssigned called — epoch map is empty
        assertThat(pm.getEpochOfPartition(tp)).isNull();

        // poll() returns records for the unassigned partition
        ConsumerRecords<String, String> poll = new ConsumerRecords<>(UniMaps.of(tp, UniLists.of(
                new ConsumerRecord<>(topic, 0, 0, "key", "value"),
                new ConsumerRecord<>(topic, 0, 1, "key", "value")
        )));

        // This should NOT throw NPE — records are skipped
        EpochAndRecordsMap<String, String> recordsMap = new EpochAndRecordsMap<>(poll, pm);

        // Records should NOT be in the map (skipped due to missing epoch)
        assertThat(recordsMap.count()).isEqualTo(0);
        assertThat(recordsMap.partitions()).isEmpty();
    }

    /**
     * Full lifecycle: poll before assignment (skipped) → assignment fires → re-poll succeeds.
     * Proves records are recovered after the assignment callback completes.
     */
    @Test
    void fullLifecycleRecordsRecoveredAfterAssignment() {
        // Step 1: poll returns records before onPartitionsAssigned — safely skipped
        ConsumerRecords<String, String> firstPoll = new ConsumerRecords<>(UniMaps.of(tp, UniLists.of(
                new ConsumerRecord<>(topic, 0, 0, "key-0", "value"),
                new ConsumerRecord<>(topic, 0, 1, "key-1", "value")
        )));
        EpochAndRecordsMap<String, String> firstRecords = new EpochAndRecordsMap<>(firstPoll, pm);

        // Records were skipped — nothing to register
        assertThat(firstRecords.count()).isEqualTo(0);

        // Step 2: onPartitionsAssigned fires (late) — epoch and partition state established
        wm.onPartitionsAssigned(UniLists.of(tp));
        long epoch = pm.getEpochOfPartition(tp);
        assertThat(epoch).isEqualTo(0L);

        // Step 3: Re-poll — Kafka re-delivers the same records (they were never committed)
        ConsumerRecords<String, String> secondPoll = new ConsumerRecords<>(UniMaps.of(tp, UniLists.of(
                new ConsumerRecord<>(topic, 0, 0, "key-0", "value"),
                new ConsumerRecord<>(topic, 0, 1, "key-1", "value")
        )));
        EpochAndRecordsMap<String, String> secondRecords = new EpochAndRecordsMap<>(secondPoll, pm);

        // Records should now be accepted with the correct epoch
        assertThat(secondRecords.count()).isEqualTo(2);
        assertThat(secondRecords.records(tp).getEpochOfPartitionAtPoll()).isEqualTo(0L);

        // Step 4: Register and verify work is created
        wm.registerWork(secondRecords);
        List<WorkContainer<String, String>> work = sm.getWorkIfAvailable(10);
        assertWithMessage("Work should be available after assignment + re-poll")
                .that(work).hasSize(2);
        for (var wc : work) {
            assertThat(wc.getEpoch()).isEqualTo(0L);
        }
    }

    /**
     * When epoch is already present (normal case), records are processed normally.
     */
    @Test
    void normalCaseWithPreExistingEpochIsUnaffected() {
        // Normal flow: onPartitionsAssigned first
        wm.onPartitionsAssigned(UniLists.of(tp));
        assertThat(pm.getEpochOfPartition(tp)).isEqualTo(0L);

        // poll returns records — should use existing epoch
        ConsumerRecords<String, String> poll = new ConsumerRecords<>(UniMaps.of(tp, UniLists.of(
                new ConsumerRecord<>(topic, 0, 0, "key", "value")
        )));
        EpochAndRecordsMap<String, String> recordsMap = new EpochAndRecordsMap<>(poll, pm);

        assertThat(recordsMap.count()).isEqualTo(1);
        assertThat(recordsMap.records(tp).getEpochOfPartitionAtPoll()).isEqualTo(0L);
    }
}
