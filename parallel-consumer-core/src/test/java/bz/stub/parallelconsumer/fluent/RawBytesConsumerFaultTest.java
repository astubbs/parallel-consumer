package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static com.google.common.truth.Truth.assertThat;

/**
 * KTD3's undetectable definition fault: a pre-built consumer that is not configured for raw bytes.
 * <p>
 * Its type parameters are erased, so nothing at definition time can tell a byte-array consumer from a string one -
 * the two are the same class with different constructor arguments. This unit owns the <em>hook</em>: the
 * deserialisers are read off the consumer once, at the moment it is supplied, and kept as a string. The dispatch
 * wrapper is what will catch the first cast failure and stop the instance with them; what is proved here is that the
 * message it will carry is worth reading.
 */
class RawBytesConsumerFaultTest {

    private static Properties props() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "raw-bytes-fault-test");
        return properties;
    }

    /**
     * A real consumer, built offline - {@code KafkaConsumer} connects on its first poll, not in its constructor - so
     * the probe is measured against the client internals it will actually meet rather than a mock's.
     */
    @Test
    void theDeserialisersOfAPreBuiltConsumerAreReadOffItWhenItIsSupplied() {
        Properties consumerProperties = props();
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        try (KafkaConsumer<String, String> stringConsumer = new KafkaConsumer<>(consumerProperties)) {
            String description = RawBytesConsumerFaultException.describe(stringConsumer);

            assertThat(description).isNotNull();
            assertThat(description).contains("StringDeserializer");
        }
    }

    @Test
    void theDescriptionIsKeptOnTheDefinitionForTheMessageTheWrapperWillThrow() {
        var pc = ParallelConsumer.define(props())
                .consumer(new LongPollingMockConsumer<>(OffsetResetStrategy.EARLIEST));
        pc.string("orders").process(context -> Outcome.succeeded());

        // A mock consumer's deserialisers may not be readable at all, and a probe that guessed would be worse than
        // one that admits it does not know - so the only claim here is that the definition asked.
        assertThat(pc.topics()).containsExactly("orders");
        var fault = RawBytesConsumerFaultException.from(
                new ClassCastException("class java.lang.String cannot be cast to class [B"),
                pc.preBuiltConsumerDescription());

        assertThat(fault).hasMessageThat().contains("not configured for raw bytes");
        assertThat(fault).hasMessageThat().contains("ByteArrayDeserializer");
        assertThat(fault).hasMessageThat().contains("definition fault");
    }

    @Test
    void theMessageNamesTheDeserialisersWhenTheyAreKnown() {
        var fault = RawBytesConsumerFaultException.from(
                new ClassCastException("class java.lang.String cannot be cast to class [B"),
                "[StringDeserializer, StringDeserializer]");

        assertThat(fault).hasMessageThat().contains("StringDeserializer");
    }

    /**
     * The classification the dispatch wrapper will apply. It is narrow on purpose: a false negative costs the clear
     * message and leaves the ordinary retry path, while a false positive would stop a running instance.
     */
    @Test
    void onlyACastFailureNamingAByteArrayCountsAsThisFault() {
        assertThat(RawBytesConsumerFaultException.isRawBytesCastFailure(
                new ClassCastException("class java.lang.String cannot be cast to class [B"))).isTrue();
        assertThat(RawBytesConsumerFaultException.isRawBytesCastFailure(
                new ClassCastException("class Order cannot be cast to class Parcel"))).isFalse();
        assertThat(RawBytesConsumerFaultException.isRawBytesCastFailure(new ClassCastException())).isFalse();
    }
}
