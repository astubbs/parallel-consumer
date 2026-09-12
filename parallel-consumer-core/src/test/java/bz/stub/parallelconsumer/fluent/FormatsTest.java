package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Deserializer;
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
     * The shape that a bare Jackson mapper refuses: a {@code java.time} field. Ordinary in a Kafka payload, and the
     * first thing a returning developer puts on one.
     */
    public static class OrderWithATimestamp {

        public String customerId = "";

        public java.time.Instant placedAt;
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
     * A bare {@code ObjectMapper} refuses every {@code java.time} type - "not supported by default: add Module
     * com.fasterxml.jackson.datatype:jackson-datatype-jsr310" - and a user who wrote {@code json(Order.class)} has
     * nowhere to add that module. {@code JacksonFormats} calls {@code findAndRegisterModules()} for exactly this,
     * which picks up whatever datatype modules the user's classpath carries.
     */
    @Test
    void theJsonHelperReadsAndWritesAJavaTimeField() {
        Format<OrderWithATimestamp> format = Formats.json(OrderWithATimestamp.class);

        OrderWithATimestamp order = new OrderWithATimestamp();
        order.customerId = "c1";
        order.placedAt = java.time.Instant.parse("2026-09-10T11:22:33Z");

        byte[] bytes = format.serializer().serialize("orders", order);
        OrderWithATimestamp read = format.deserializer().deserialize("orders", bytes);

        assertThat(read.placedAt).isEqualTo(order.placedAt);
        assertThat(read.customerId).isEqualTo("c1");
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

    /**
     * A wrapper must delegate the <b>headers-aware</b> form, which is the one the dispatch wrapper calls.
     * <p>
     * {@link org.apache.kafka.common.serialization.Deserializer}'s default 3-arg method discards the headers and
     * calls the 2-arg one, so a wrapper overriding only the 2-arg form hides them from the deserialiser it wraps
     * and nothing goes red - the payload still decodes, it just decodes without the headers. The registry
     * deserialisers this facade is built to wrap are exactly the ones that resolve a schema from a header, so the
     * loss would land on the formats a user cannot test here.
     * <p>
     * {@code ConfigMergingDeserializer} and {@code ConfigMergingSerializer} carried the same defect and were fixed
     * with it; neither is reachable from a test, because both are built only by the Avro and Protobuf helpers and
     * neither library is on this project's classpath.
     */
    @Test
    void aWrappedDeserialiserIsGivenTheRecordsHeaders() {
        Deserializer<String> headerDriven = new Deserializer<String>() {

            @Override
            public String deserialize(String topic, byte[] data) {
                return "the headers-aware form was not called";
            }

            @Override
            public String deserialize(String topic, Headers headers, byte[] data) {
                Header schema = headers.lastHeader("schema");
                return schema == null ? "no schema header" : new String(schema.value(), StandardCharsets.UTF_8);
            }
        };
        Format<String> wrapped = Formats.classifyDecodeFailures(
                Serdes.serdeFrom(Serdes.String().serializer(), headerDriven), Decode::permanentFailure);

        Headers headers = new RecordHeaders().add("schema", "v7".getBytes(StandardCharsets.UTF_8));
        String read = wrapped.deserializer()
                .deserialize("orders", headers, "payload".getBytes(StandardCharsets.UTF_8));

        assertThat(read).isEqualTo("v7");
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
