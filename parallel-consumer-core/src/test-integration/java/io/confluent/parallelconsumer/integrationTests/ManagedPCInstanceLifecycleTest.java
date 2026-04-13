package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2026 Confluent, Inc. and contributors
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.confluent.parallelconsumer.integrationTests.utils.ManagedPCInstance;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Focused lifecycle test for {@link ManagedPCInstance} — verifies that rapid stop/start
 * cycles don't create duplicate PC instances or cause ConcurrentModificationException.
 * <p>
 * This is a targeted regression test for the double-submission bug found during the
 * <a href="https://github.com/confluentinc/parallel-consumer/issues/857">#857</a> investigation.
 */
@Slf4j
class ManagedPCInstanceLifecycleTest extends BrokerIntegrationTest<String, String> {

    /**
     * Rapidly toggle a single instance stop→start multiple times.
     * If the started flag isn't set correctly, run() will be submitted multiple times,
     * creating duplicate PCs in the same consumer group → ConcurrentModificationException.
     */
    @RepeatedTest(5)
    void rapidToggleShouldNotCreateDuplicateInstances() throws Exception {
        numPartitions = 4;
        String inputName = setupTopic("lifecycle-test");

        ExecutorService executor = Executors.newCachedThreadPool();
        AtomicInteger consumeCount = new AtomicInteger();

        ManagedPCInstance.Config config = ManagedPCInstance.Config.builder()
                .commitMode(CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS)
                .order(ProcessingOrder.UNORDERED)
                .inputTopic(inputName)
                .build();

        ManagedPCInstance instance = new ManagedPCInstance(config, getKcu(), key -> consumeCount.incrementAndGet());

        // Start the instance
        executor.submit(instance);
        Thread.sleep(2000); // let it start and join the group

        // Rapid toggle cycles — the bug: toggle() calls start() which submits run()
        // before the previous run() has set started=true, causing double-submission
        for (int i = 0; i < 10; i++) {
            log.info("Toggle cycle {}", i);
            instance.toggle(executor);
            Thread.sleep(100); // very short — enough for stop, not enough for run() to start
            instance.toggle(executor);
        }

        // Let it settle
        Thread.sleep(3000);

        // The instance should still be functional — produce and consume a message
        getKcu().produceMessages(inputName, 10);
        Thread.sleep(5000);

        instance.stop();
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        // If duplicate PCs were created, we'd see ConcurrentModificationException in the logs
        // and the PC would be dead. Check that it actually consumed something.
        log.info("Consumed {} messages after rapid toggles", consumeCount.get());
        assertThat(consumeCount.get())
                .as("Should have consumed messages — if 0, the PC died from CME during rapid toggles")
                .isGreaterThan(0);
    }
}
