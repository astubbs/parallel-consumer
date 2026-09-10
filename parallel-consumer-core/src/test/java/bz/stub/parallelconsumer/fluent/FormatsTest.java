package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The format helpers (R4, KTD7): what resolves, what refuses, and how a route says a decode failure is permanent.
 */
class FormatsTest {

    private static final byte[] NOT_JSON = "not json".getBytes(StandardCharsets.UTF_8);

    public static class Order {

        public String customerId = "";

        public long amount;
    }

    /**
     * The refusal that matters most, because the alternative is a library that fails on the first record of a
     * production topic: neither Confluent serialiser is on Maven Central, so neither is on this project's classpath,
     * and the helper says which one it wanted and where it comes from.
     */
    @Test
    void aFormatHelperForALibraryThatIsNotOnTheClasspathRefusesNamingIt() {
        var avro = assertThrows(IllegalStateException.class, () -> Formats.avro(Order.class));
        assertThat(avro).hasMessageThat().contains("avro(Order)");
        assertThat(avro).hasMessageThat().contains("io.confluent.kafka.serializers.KafkaAvroDeserializer");
        assertThat(avro).hasMessageThat().contains("kafka-avro-serializer");
        assertThat(avro).hasMessageThat().contains("Consumed.with");

        var protobuf = assertThrows(IllegalStateException.class, () -> Formats.protobuf(Order.class));
        assertThat(protobuf).hasMessageThat().contains("protobuf(Order)");
        assertThat(protobuf).hasMessageThat()
                .contains("io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer");
    }

    /**
     * Jackson is core's one declared format dependency, and optional, so the JSON helpers resolve here.
     */
    @Test
    void theJsonHelperReadsAndWritesTheDeclaredClass() {
        Format<Order> format = Formats.json(Order.class);

        Order order = new Order();
        order.customerId = "c1";
        order.amount = 42;
        byte[] bytes = format.serializer().serialize("orders", order);
        Order read = format.deserializer().deserialize("orders", bytes);

        assertThat(read.customerId).isEqualTo("c1");
        assertThat(read.amount).isEqualTo(42);
        assertThat(format.hasSerializer()).isTrue();
    }

    /**
     * The class-free form: a topic nobody has a class for, inspected by field name with nothing declared (R4).
     */
    @Test
    void theJsonHelperWithNoClassYieldsAMapOfFieldNamesToValues() {
        Format<Map<String, Object>> format = Formats.json();

        byte[] payload = "{\"type\":\"dispatched\",\"parcels\":3}".getBytes(StandardCharsets.UTF_8);
        Map<String, Object> read = format.deserializer().deserialize("events", payload);

        assertThat(read.get("type")).isEqualTo("dispatched");
        assertThat(read.get("parcels")).isEqualTo(3);
    }

    @Test
    void bytesAndStringCanBothReadAndWrite() {
        assertThat(Formats.bytes().hasSerializer()).isTrue();
        assertThat(Formats.bytes().hasDeserializer()).isTrue();
        assertThat(Formats.string().hasSerializer()).isTrue();
        assertThat(Formats.string().deserializer().deserialize("t", "hello".getBytes(StandardCharsets.UTF_8)))
                .isEqualTo("hello");
    }

    /**
     * R12: a stock deserialiser that throws is transient by default, because it cannot tell a corrupt payload from a
     * registry outage. The wrapper is how a route that <em>can</em> tell says so.
     */
    @Test
    void classifyDecodeFailuresTurnsTheVerdictIntoATerminalThrowOnlyWhenItSaysPermanent() {
        Format<Order> permanent = Formats.classifyDecodeFailures(Formats.json(Order.class),
                failure -> Decode.permanentFailure(failure));
        Format<Order> stillTransient = Formats.classifyDecodeFailures(Formats.json(Order.class),
                failure -> Decode.transientFailure(failure));

        var terminal = assertThrows(PermanentDecodeFailureException.class,
                () -> permanent.deserializer().deserialize("orders", NOT_JSON));
        assertThat(terminal).hasMessageThat().contains("orders");
        assertThat(terminal).hasCauseThat().isNotNull();

        // Transient keeps the original exception, so it takes the ordinary retry path (R9, R10).
        var retried = assertThrows(RuntimeException.class,
                () -> stillTransient.deserializer().deserialize("orders", NOT_JSON));
        assertThat(retried).isNotInstanceOf(PermanentDecodeFailureException.class);
    }

    @Test
    void anUnwrappedDeserialiserFailureStaysTransient() {
        var thrown = assertThrows(RuntimeException.class,
                () -> Formats.json(Order.class).deserializer().deserialize("orders", NOT_JSON));

        assertThat(thrown).isNotInstanceOf(PermanentDecodeFailureException.class);
    }

    /**
     * A hand-written deserialiser can read a topic nothing here can write, and the produced side is where that
     * becomes a problem - so that is where it is refused, rather than on the first produced record.
     */
    @Test
    void aReadOnlyFormatIsRefusedOnTheProducedSideNamingIt() {
        Format<String> readOnly = Format.reading(Serdes.String().deserializer());
        assertThat(readOnly.hasSerializer()).isFalse();

        var thrown = assertThrows(IllegalArgumentException.class,
                () -> Produced.with(Serdes.String(), readOnly));

        assertThat(thrown).hasMessageThat().contains("no serializer");
        assertThat(thrown).hasMessageThat().contains("value");
    }

    /**
     * KTD9: the sandbox has to encode what it generates, so a format that can only read is visible as such rather
     * than discovered when a generator tries to write it.
     */
    @Test
    void aFormatSaysWhetherItCanWrite() {
        assertThat(Format.of(Serdes.Long()).hasSerializer()).isTrue();
        assertThat(Format.reading(Serdes.Long().deserializer()).hasSerializer()).isFalse();
        assertThat(Format.writing(Serdes.Long().serializer()).hasDeserializer()).isFalse();
    }
}
