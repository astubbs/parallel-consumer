package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.internal.TestParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager;
import bz.stub.parallelconsumer.state.ModelUtils;
import bz.stub.parallelconsumer.state.PartitionState;
import bz.stub.parallelconsumer.state.WorkContainer;
import bz.stub.parallelconsumer.state.WorkManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Tests to verify the protected and internal methods of
 * {@link bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor} work as expected.
 * <p>
 *
 * @author Jonathon Koyle
 */
@Slf4j
class AbstractParallelEoSStreamProcessorConfigurationTest {
    final MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
    final ParallelConsumerOptions<String, String> testOptions = ParallelConsumerOptions.<String, String>builder()
            .consumer(consumer)
            .build();

    ModelUtils mu = new ModelUtils();
    PartitionState<String, String> state;
    WorkManager<String, String> wm;

    String topic = "myTopic";
    int partition = 0;

    TopicPartition tp = new TopicPartition(topic, partition);
    PCModule module = new PCModuleTestEnv();

    @BeforeEach
    public void setup() {
        state = new PartitionState<>(0, mu.getModule(), tp, OffsetMapCodecManager.HighestOffsetAndIncompletes.of());
        wm = mu.getModule().workManager();
        wm.onPartitionsAssigned(UniLists.of(tp));
    }

    /**
     * Test that the {@link bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor#getQueueTargetLoaded}
     */
    @Test
    void queueTargetLoad() {
        final int batchSize = 10;
        final int concurrency = 2;
        final MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
        final ParallelConsumerOptions<String, String> testOptions = ParallelConsumerOptions.<String, String>builder()
                .batchSize(batchSize)
                .maxConcurrency(concurrency)
                .consumer(consumer)
                // The multiplication asserted below is specific to the pre-loaded-queue engine: the load factor
                // sizes the executor's QUEUE, so a factor of 2 means twice as many records buffered, not twice as
                // many running. A virtual-thread pool has no queue, so it deliberately does not multiply, and this
                // assertion would be asserting a defect there. The virtual-thread half of the contract is pinned
                // in VirtualThreadExecutionModeTest#theInFlightTargetIsNotMultipliedByTheLoadFactorInThisMode.
                .useVirtualThreads(false)
                .build();
        try (final TestParallelEoSStreamProcessor<String, String> testInstance = new TestParallelEoSStreamProcessor<>(testOptions)) {
            final int defaultLoad = 2;
            final int expectedTargetLoad = batchSize * concurrency * defaultLoad;

            final int actualTargetLoad = testInstance.getTargetLoad();

            Assertions.assertEquals(expectedTargetLoad, actualTargetLoad);
        }
    }

    @Test
    void testHandleStaleWorkSplit() {
        List<WorkContainer<String, String>> workContainers = new ArrayList<>();

        workContainers.add(new WorkContainer<String, String>(0, new ConsumerRecord<>(topic, partition, 0, "test_k", "test_v1"), module));
        workContainers.add(new WorkContainer<String, String>(1, new ConsumerRecord<>(topic, partition, 1, "test_k", "test_v2"), module));

        try (final TestParallelEoSStreamProcessor<String, String> testInstance = new TestParallelEoSStreamProcessor<>(testOptions)) {
            testInstance.setWm(wm);
            Function<PollContextInternal<String, String>, List<String>> dummyFunction = (contextInternal) -> new ArrayList<>();
            Consumer<String> callback = (res) -> {
            };

            testInstance.runUserFunc(dummyFunction, callback, workContainers);


            Assertions.assertEquals(testInstance.getMailBoxSuccessCnt(), 1);
            Assertions.assertEquals(testInstance.getMailBoxFailedCnt(), 1);
        }
    }

    @Test
    void testHandleStaleWorkNoSplit() {
        List<WorkContainer<String, String>> workContainers = new ArrayList<>();

        workContainers.add(new WorkContainer<String, String>(0, new ConsumerRecord<>(topic, partition, 0, "test_k", "test_v1"), module));
        workContainers.add(new WorkContainer<String, String>(0, new ConsumerRecord<>(topic, partition, 1, "test_k", "test_v2"), module));

        try (final TestParallelEoSStreamProcessor<String, String> testInstance = new TestParallelEoSStreamProcessor<>(testOptions)) {
            testInstance.setWm(wm);
            Function<PollContextInternal<String, String>, List<String>> dummyFunction = (contextInternal) -> new ArrayList<>();
            Consumer<String> callback = (res) -> {
            };

            testInstance.runUserFunc(dummyFunction, callback, workContainers);


            Assertions.assertEquals(testInstance.getMailBoxSuccessCnt(), 2);
            Assertions.assertEquals(testInstance.getMailBoxFailedCnt(), 0);
        }
    }
}
