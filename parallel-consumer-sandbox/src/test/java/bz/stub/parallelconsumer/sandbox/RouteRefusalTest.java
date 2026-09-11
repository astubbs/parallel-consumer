package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.fluent.Consumed;
import bz.stub.parallelconsumer.fluent.ConsumerHandle;
import bz.stub.parallelconsumer.fluent.Format;
import bz.stub.parallelconsumer.fluent.Formats;
import bz.stub.parallelconsumer.fluent.Outcome;
import bz.stub.parallelconsumer.fluent.ParallelConsumerDefinition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * What the sandbox refuses, and when.
 * <p>
 * The generator has to <b>encode</b> what it makes with the same format the route <b>decodes</b> it with, because
 * the engine under the facade reads raw bytes. That gives it two requirements a route need not otherwise meet: the
 * format has to be able to write, and something has to name the Java type to fill. A route missing either is
 * refused at start, naming the topic - the alternative being a record the route cannot read, failing one poll
 * later with a message about the payload rather than about the definition.
 */
@Timeout(60)
class RouteRefusalTest {

    /**
     * A deserialiser nothing in Kafka or this library has heard of: not in {@code Serdes}, so the type cannot be
     * recognised from its class either.
     */
    private static final Deserializer<String> SHOUTY_READER =
            (topic, data) -> data == null ? null : new String(data, StandardCharsets.UTF_8).toLowerCase();

    private static final Serializer<String> SHOUTY_WRITER =
            (topic, data) -> data == null ? null : data.toUpperCase().getBytes(StandardCharsets.UTF_8);

    @Test
    void aRouteWhoseFormatCanOnlyReadIsRefusedNamingItsTopic() {
        ParallelConsumerDefinition definition = ParallelConsumer.connect(new Properties());
        definition.topic("legacy")
                .consumed(Consumed.with(Formats.string(), Format.reading(SHOUTY_READER)))
                .process(context -> Outcome.succeeded());

        IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
                () -> definition.start(Sandbox.builder().build()));

        assertWithMessage("the refusal has to name the topic - a definition with a dozen routes is otherwise a "
                + "hunt").that(refusal).hasMessageThat().contains("legacy");
        assertThat(refusal).hasMessageThat().contains("can only read");
    }

    @Test
    void theSameRouteWithASerialiserAndATypeGeneratesAndEncodes() {
        ParallelConsumerDefinition definition = ParallelConsumer.connect(new Properties());
        definition.topic("legacy")
                .consumed(Consumed.with(Formats.string(), Format.of(SHOUTY_READER, SHOUTY_WRITER, String.class)))
                .process(context -> Outcome.succeeded());

        Sandbox sandbox = Sandbox.builder()
                .perSecond(500)
                .bound(Bound.afterRecords(5))
                .build();

        try (ConsumerHandle handle = definition.start(sandbox)) {
            assertThat(sandbox.awaitBound(Duration.ofSeconds(30))).isTrue();
            handle.awaitShutdown();
        }

        assertThat(sandbox.generatedRecords()).isEqualTo(5);
    }

    /**
     * The middle case, and the one that would be easy to get wrong in the other direction: the format can write,
     * so the first check passes, but nothing names the type to fill. Refused with the cure in the message.
     */
    @Test
    void aRouteWithASerialiserButNoNamedTypeIsRefusedAndTheBuilderIsTheWayOut() {
        Sandbox refusing = Sandbox.builder().build();

        IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
                () -> untypedRoute().start(refusing));

        assertThat(refusal).hasMessageThat().contains("legacy");
        assertThat(refusal).hasMessageThat().contains("does not name a Java type");
        assertWithMessage("a refusal that does not say what to do about it is only half a message")
                .that(refusal).hasMessageThat().contains("generating(\"legacy\"");

        Sandbox told = Sandbox.builder()
                .perSecond(500)
                .bound(Bound.afterRecords(3))
                .generating("legacy", String.class)
                .build();
        try (ConsumerHandle handle = untypedRoute().start(told)) {
            assertThat(told.awaitBound(Duration.ofSeconds(30))).isTrue();
            handle.awaitShutdown();
        }
        assertThat(told.generatedRecords()).isEqualTo(3);
    }

    /**
     * A definition may only be started once, so each arm of the test above needs its own.
     */
    private static ParallelConsumerDefinition untypedRoute() {
        ParallelConsumerDefinition definition = ParallelConsumer.connect(new Properties());
        definition.topic("legacy")
                .consumed(Consumed.with(Formats.string(), Format.of(SHOUTY_READER, SHOUTY_WRITER)))
                .process(context -> Outcome.succeeded());
        return definition;
    }
}
