package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;

/**
 * Tests that PC works fine with the plain vanilla {@link MockConsumer}, as opposed to the
 * {@link LongPollingMockConsumer}.
 * <p>
 * This is the baseline of the {@link MockConsumerTestBase} family: no injected failure, so what remains is the
 * setup the other scenarios build on. That setup is also the demonstration of why {@link MockConsumer} is
 * awkward to use with PC, and why {@link LongPollingMockConsumer} should be used instead - read
 * {@link MockConsumerTestBase#setupMockConsumerAndParallelConsumer()} and its class javadoc for the manual
 * rebalance dance this test depends on.
 *
 * @author Antony Stubbs
 * @see LongPollingMockConsumer#revokeAssignment
 */
class MockConsumerTest extends MockConsumerTestBase {

    private static final int RECORDS = 3;

    /**
     * With nothing injected to go wrong, the backlog reaches the user function - the baseline the failure
     * scenarios are measured against.
     */
    @Test
    void backlogIsProcessedWithAVanillaMockConsumer() {
        addRecords(RECORDS);

        startProcessing();

        Awaitility.await().untilAsserted(() -> assertThat(processedRecords).hasSize(RECORDS));
    }

}
