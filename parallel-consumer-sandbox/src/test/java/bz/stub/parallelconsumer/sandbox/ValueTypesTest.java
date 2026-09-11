package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.fluent.Format;
import bz.stub.parallelconsumer.sandbox.demo.Order;
import org.apache.kafka.common.serialization.BooleanDeserializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.FloatDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.ShortDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.UUIDDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.ByteBuffer;
import java.util.UUID;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The three answers {@link ValueTypes} gives, and the table behind the second one.
 * <p>
 * <b>Why the table needs a test of its own.</b> It is eleven class names written as strings, and a wrong one does
 * not fail anything at build time - it fails at run time as a route the sandbox refuses for having a type nobody
 * can name, which reads as a fault in the user's definition rather than in this table. Referring to each
 * deserialiser as a class here is what makes a typo or a Kafka-side rename a compile error. The table stays keyed
 * by name on purpose: a name matches a deserialiser loaded by another classloader, which is an ordinary shape for
 * a library embedded in a container, and a {@code Class} key would not.
 */
@Timeout(60)
class ValueTypesTest {

    @Test
    void aFormatThatWasToldItsTypeAnswersWithIt() {
        assertThat(ValueTypes.of(Format.reading(new StringDeserializer(), String.class))).isEqualTo(String.class);
    }

    @Test
    void everyDeserialiserKafkaShipsResolvesToTheTypeItReads() {
        assertResolves(new StringDeserializer(), String.class);
        assertResolves(new ByteArrayDeserializer(), byte[].class);
        assertResolves(new ByteBufferDeserializer(), ByteBuffer.class);
        assertResolves(new BytesDeserializer(), Bytes.class);
        assertResolves(new IntegerDeserializer(), Integer.class);
        assertResolves(new ShortDeserializer(), Short.class);
        assertResolves(new LongDeserializer(), Long.class);
        assertResolves(new FloatDeserializer(), Float.class);
        assertResolves(new DoubleDeserializer(), Double.class);
        assertResolves(new BooleanDeserializer(), Boolean.class);
        assertResolves(new UUIDDeserializer(), UUID.class);
    }

    /**
     * The third answer, which is the one a user meets: a hand-written deserialiser over a hand-written class is
     * not in the table and was never declared, so nothing can name the type and the sandbox has to say so rather
     * than invent a value it cannot read back.
     */
    @Test
    void aHandWrittenDeserialiserIsNotGuessedAt() {
        Deserializer<Order> handWritten = (topic, data) -> new Order();

        assertWithMessage("an unrecognised deserialiser must answer null - the caller turns that into a refusal "
                + "naming the topic, and a guess here would surface one poll later as a payload error")
                .that(ValueTypes.of(Format.reading(handWritten))).isNull();
    }

    private static void assertResolves(Deserializer<?> deserializer, Class<?> expected) {
        assertWithMessage("%s is one of Kafka's own, so a route declared with it needs no type of its own",
                deserializer.getClass().getSimpleName())
                .that(ValueTypes.of(Format.reading(deserializer))).isEqualTo(expected);
    }
}
