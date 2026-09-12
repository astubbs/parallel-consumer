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
class RawBytesConsumerFaultTest extends AbstractFluentEngineTest {

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
        var pc = ParallelConsumer.connect(props())
                .withConsumer(new LongPollingMockConsumer<>(OffsetResetStrategy.EARLIEST));
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
     * <p>
     * Two conditions, and the second is here because the first alone was not enough - see
     * {@link #aDeserialiserMakingItsOwnByteArrayCastIsNotThisFault()} for the case it accepted wrongly.
     */
    @Test
    void onlyACastFailureNamingAByteArrayFromTheWrappersOwnCastCountsAsThisFault() {
        assertThat(RawBytesConsumerFaultException.isRawBytesCastFailure(
                thrownBy(RouteDispatcher.class, "class java.lang.String cannot be cast to class [B"))).isTrue();
        assertThat(RawBytesConsumerFaultException.isRawBytesCastFailure(
                thrownBy(RouteDispatcher.class, "class Order cannot be cast to class Parcel"))).isFalse();
        assertThat(RawBytesConsumerFaultException.isRawBytesCastFailure(
                thrownBy(RouteDispatcher.class, null))).isFalse();
    }

    /**
     * The throw-site half on its own: the same message, from somebody else's frame, is not this fault. A route's
     * deserialiser casting a {@code String} to {@code byte[]} internally words its failure identically, and being
     * classified as a bad pre-built consumer stopped the whole instance instead of retrying a decode failure (R12).
     */
    @Test
    void aDeserialiserMakingItsOwnByteArrayCastIsNotThisFault() {
        assertThat(RawBytesConsumerFaultException.isRawBytesCastFailure(
                thrownBy(RawBytesConsumerFaultTest.class, "class java.lang.String cannot be cast to class [B")))
                .isFalse();
    }

    /**
     * A cast failure stamped with the frame it would have been thrown from, which is what the classifier reads.
     * Building it rather than provoking it keeps the two conditions separable: a real throw could only ever carry
     * one frame at a time.
     */
    private static ClassCastException thrownBy(Class<?> thrower, String message) {
        ClassCastException castFailed = new ClassCastException(message);
        castFailed.setStackTrace(new StackTraceElement[]{
                new StackTraceElement(thrower.getName(), "someMethod", thrower.getSimpleName() + ".java", 1)});
        return castFailed;
    }
}
