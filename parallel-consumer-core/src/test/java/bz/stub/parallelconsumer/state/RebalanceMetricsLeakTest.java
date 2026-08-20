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
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import pl.tlinkowski.unij.api.UniLists;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Regression for the <a href="https://github.com/confluentinc/parallel-consumer/issues/859">confluentinc#859</a> meter leak via the real rebalance path: repeated partition assign/revoke
 * cycles must not grow {@link bz.stub.parallelconsumer.metrics.PCMetrics}'s registered-meter
 * tracking set. Before the fix the tracking collection was a {@code List}, so the fixed-id
 * offset-encoding meter re-registered on every assignment accumulated duplicates unbounded; the fix
 * makes it a de-duplicating {@code Set} (<a href="https://github.com/confluentinc/parallel-consumer/issues/859">confluentinc#859</a>). Verified: this test fails when the {@code Set} is
 * reverted to a {@code List}.
 *
 * <p><strong>Scope:</strong> this guards the <a href="https://github.com/confluentinc/parallel-consumer/issues/859">confluentinc#859</a> tracking-set de-duplication only. It does
 * <em>not</em> verify the astubbs#57 {@code PartitionStateManager} codec-manager caching, and cannot do so
 * through the tracked-meter count: the {@code Set} de-dup already collapses the duplicate
 * registrations the caching would avoid, and {@link PartitionState}'s own per-instance
 * {@code OffsetMapCodecManager} (constructed per assignment) re-registers the same fixed-id meter
 * regardless. Reverting the caching leaves this test green. The caching is an allocation/confluentinc#233
 * optimization, redundant for the leak, with no clean behavioural seam to assert here - see
 * {@code docs/refactoring.md}.
 *
 * <p>Runs {@link ExecutionMode#SAME_THREAD} for the same reason {@link WorkManagerTest} does - these
 * tests share process-wide module state
 * (same reason as {@link WorkManagerTest}).
 *
 * @see PartitionStateManager
 * @see bz.stub.parallelconsumer.metrics.PCMetrics859Test
 */
@Execution(ExecutionMode.SAME_THREAD)
@Slf4j
class RebalanceMetricsLeakTest {

    static final String INPUT_TOPIC = "input";

    @Test
    void repeatedRebalancesDoNotGrowRegisteredMeters() {
        var mockConsumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        var options = ParallelConsumerOptions.<String, String>builder().consumer(mockConsumer).build();
        var module = new PCModuleTestEnv(options);
        var wm = module.workManager();

        var tp = UniLists.of(new TopicPartition(INPUT_TOPIC, 0));

        // One full cycle to reach steady state (first assign registers per-partition + codec meters).
        wm.onPartitionsAssigned(tp);
        wm.onPartitionsRevoked(tp);
        int steadyState = module.pcMetrics().registeredMeterCount();
        log.info("Registered meters at steady state: {}", steadyState);

        // Many more rebalances must not add to the tracking set.
        for (int i = 0; i < 50; i++) {
            wm.onPartitionsAssigned(tp);
            wm.onPartitionsRevoked(tp);
        }

        assertWithMessage("registeredMeters grew across 50 assign/revoke cycles - PCMetrics leak (confluentinc#859) has regressed")
                .that(module.pcMetrics().registeredMeterCount())
                .isEqualTo(steadyState);
    }
}
