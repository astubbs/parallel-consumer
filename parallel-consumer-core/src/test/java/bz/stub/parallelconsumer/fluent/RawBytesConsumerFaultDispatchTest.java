package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A pre-built consumer that is not configured for raw bytes: the definition fault nothing can detect at define
 * time, because type parameters are erased and a byte-array consumer is the same class as a string one (KTD3, R1).
 * <p>
 * <b>It stops the instance rather than retrying.</b> Every record would meet the same cast failure, so retrying is
 * a hot loop that never succeeds and never reports why - the shape of failure the fluent API exists to remove.
 * <p>
 * This test wires its clients by hand rather than through {@link RecordingClientRuntime}'s helper, because the
 * whole subject is a consumer of the <em>wrong</em> type: a helper that handed out a correct one could not express
 * it.
 */
@Timeout(60)
class RawBytesConsumerFaultDispatchTest {

    private static final String TOPIC = "orders";

    private final LongPollingMockConsumer<String, String> stringConsumer =
            new LongPollingMockConsumer<>(OffsetResetStrategy.EARLIEST);

    private ConsumerHandle handle;

    @AfterEach
    void closeTheInstance() {
        if (handle != null) {
            RecordingClientRuntime.closeWithoutDraining(handle);
        }
    }

    private static Properties props() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "raw-bytes-fault-dispatch-test");
        return properties;
    }

    /**
     * The erasure that makes this undetectable is the same erasure that lets a test express it: a
     * {@code Consumer<String, String>} is assignable to {@code Consumer<byte[], byte[]>} with one unchecked cast,
     * which is exactly what a user does by accident when they build their consumer with string deserialisers.
     */
    @SuppressWarnings("unchecked")
    private Consumer<byte[], byte[]> asIfItWereRawBytes() {
        return (Consumer<byte[], byte[]>) (Consumer<?, ?>) stringConsumer;
    }

    @Test
    void theFirstRecordStopsTheInstanceAndTheHandlesAwaitSurfacesTheFault() {
        var ran = new AtomicInteger();
        var pc = ParallelConsumer.connect(props()).consumer(asIfItWereRawBytes());
        pc.string(TOPIC)
                .retryDelay(Duration.ofMillis(10))
                .process(context -> {
                    ran.incrementAndGet();
                    return Outcome.succeeded();
                });

        stringConsumer.updateBeginningOffsets(Collections.singletonMap(new TopicPartition(TOPIC, 0), 0L));
        handle = pc.start(new RecordingClientRuntime());
        stringConsumer.subscribeWithRebalanceAndAssignment(Collections.singletonList(TOPIC), 1);
        stringConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, "key-0", "an order"));

        var thrown = assertThrows(RawBytesConsumerFaultException.class,
                () -> handle.awaitShutdown(Duration.ofSeconds(30)));

        assertThat(thrown).hasMessageThat().contains("not configured for raw bytes");
        assertThat(thrown).hasMessageThat().contains("definition fault");
        assertThat(thrown).hasMessageThat().contains("ByteArrayDeserializer");
        // The record never reached the function: decoding is what failed.
        assertThat(ran.get()).isEqualTo(0);
        // Not a retry, and not a park: the instance stopped, and the fault is on the handle for a caller that did
        // not happen to be awaiting.
        assertThat(handle.failureCause().get()).isSameInstanceAs(thrown);
        assertThat(pc.dispatcher().parkedCount()).isEqualTo(0);
    }
}
