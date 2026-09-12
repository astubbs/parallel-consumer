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
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
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
class RawBytesConsumerFaultDispatchTest extends AbstractFluentEngineTest {

    private final LongPollingMockConsumer<String, String> stringConsumer =
            new LongPollingMockConsumer<>(OffsetResetStrategy.EARLIEST);

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
        var pc = ParallelConsumer.connect(props()).withConsumer(asIfItWereRawBytes());
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

    /**
     * The same words, a different culprit: a route's own deserialiser casting a {@code String} to {@code byte[]}
     * internally throws a {@link ClassCastException} naming a byte array exactly as the facade's own cast does - on
     * a definition whose supplied consumer is a correct raw-bytes one.
     * <p>
     * Matching on the message alone read that as a bad pre-built consumer and stopped the whole instance, when R12
     * calls a deserialiser's failure transient and gives it a retry path. It parks after its attempts now, and the
     * instance stays up.
     */
    @Test
    void aDeserialisersOwnByteArrayCastIsRetriedAndParkedRatherThanStoppingTheInstance() {
        LongPollingMockConsumer<byte[], byte[]> rawBytesConsumer =
                new LongPollingMockConsumer<>(OffsetResetStrategy.EARLIEST);
        var pc = ParallelConsumer.connect(props()).withConsumer(rawBytesConsumer);
        pc.topic(TOPIC)
                .consumed(Consumed.with(Serdes.String(), Format.reading(castingItsOwnValueToBytes())))
                .retryLimit(1)
                .retryDelay(Duration.ofMillis(10))
                .process(context -> Outcome.succeeded());

        rawBytesConsumer.updateBeginningOffsets(Collections.singletonMap(new TopicPartition(TOPIC, 0), 0L));
        handle = pc.start(new RecordingClientRuntime());
        rawBytesConsumer.subscribeWithRebalanceAndAssignment(Collections.singletonList(TOPIC), 1);
        rawBytesConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, "key-0".getBytes(StandardCharsets.UTF_8),
                "an order".getBytes(StandardCharsets.UTF_8)));

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(pc.dispatcher().parkedCount()).isEqualTo(1));

        assertWithMessage("a deserialiser's bad cast is the record's problem, not the consumer's")
                .that(handle.processor().isClosedOrFailed()).isFalse();
        assertThat(handle.failureCause().isPresent()).isFalse();
    }

    /**
     * A deserialiser that casts the value it was handed to {@code byte[]} - which it already is, declared - through
     * an {@code Object} so the compiler inserts the checkcast inside this class rather than refusing it. It is the
     * shape of an ordinary bug in user code, and it fails in the deserialiser's own frame.
     */
    private static Deserializer<String> castingItsOwnValueToBytes() {
        return (topic, data) -> {
            Object notAByteArray = "this is a String, and the next line insists it is not";
            byte[] ignoredForcedCast = (byte[]) notAByteArray;
            return new String(ignoredForcedCast, StandardCharsets.UTF_8);
        };
    }
}
