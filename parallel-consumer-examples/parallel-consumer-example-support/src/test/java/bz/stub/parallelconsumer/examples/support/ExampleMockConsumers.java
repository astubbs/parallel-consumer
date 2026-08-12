package bz.stub.parallelconsumer.examples.support;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.mockito.Mockito;

import static org.mockito.Mockito.when;

/**
 * The mock consumer every example test drives its app with, built once here instead of copy-pasted into
 * each of them.
 * <p>
 * It lives in this module's test-jar rather than in parallel-consumer-core's, which is signed and
 * published to Maven Central: examples-only scaffolding does not belong on a published artifact's
 * surface, where removing it later becomes a compatibility question.
 */
public final class ExampleMockConsumers {

    /**
     * The group id the examples' mock consumers report. Arbitrary - no example asserts on it.
     */
    public static final String DEFAULT_GROUP_ID = "groupid";

    private ExampleMockConsumers() {
    }

    /**
     * @see #spiedLongPollingMockConsumer(String)
     */
    public static <K, V> LongPollingMockConsumer<K, V> spiedLongPollingMockConsumer() {
        return spiedLongPollingMockConsumer(DEFAULT_GROUP_ID);
    }

    /**
     * A spied {@link LongPollingMockConsumer} with its group metadata stubbed, which the Apache Kafka
     * mock consumer does not supply itself.
     *
     * @param groupId the consumer group id to report
     */
    public static <K, V> LongPollingMockConsumer<K, V> spiedLongPollingMockConsumer(String groupId) {
        LongPollingMockConsumer<K, V> mockConsumer =
                Mockito.spy(new LongPollingMockConsumer<K, V>(OffsetResetStrategy.EARLIEST));
        when(mockConsumer.groupMetadata())
                .thenReturn(new ConsumerGroupMetadata(groupId)); // todo fix AK mock consumer
        return mockConsumer;
    }
}
