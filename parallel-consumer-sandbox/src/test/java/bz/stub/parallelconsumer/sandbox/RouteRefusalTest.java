package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.fluent.Consumed;
import bz.stub.parallelconsumer.fluent.ParallelConsumerInstance;
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

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * What the sandbox refuses, and when.
 * <p>
 * The sandbox has to <b>encode</b> what it publishes with the same format the route <b>decodes</b> it with,
 * because the engine under the facade reads raw bytes. That gives a route one requirement it need not otherwise
 * meet - its format has to be able to write - and a driven sandbox one more: something has to name the Java type
 * the hydration is to fill, unless its caller says what a record contains instead. Both are refused at start,
 * naming the topic; the alternative is a record the route cannot read, failing one poll later with a message
 * about the payload rather than about the definition, or a route that silently never fires.
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
        ParallelConsumerDefinition definition = SandboxFixtures.definition();
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
    void theSameRouteWithASerialiserPublishesAndEncodes() {
        ParallelConsumerDefinition definition = SandboxFixtures.definition();
        definition.topic("legacy")
                .consumed(Consumed.with(Formats.string(), Format.of(SHOUTY_READER, SHOUTY_WRITER, String.class)))
                .process(context -> Outcome.succeeded());

        Sandbox sandbox = Sandbox.builder()
                .perSecond(500)
                .bound(Bound.afterRecords(5))
                .feeding("legacy", index -> "scan-" + index)
                .build();

        try (ParallelConsumerInstance instance = definition.start(sandbox)) {
            assertThat(sandbox.awaitBound(Duration.ofSeconds(30))).isTrue();
            instance.awaitShutdown();
        }

        assertThat(sandbox.generatedRecords()).isEqualTo(5);
    }

    /**
     * The middle case, and the one that would be easy to get wrong in the other direction: the format can write,
     * so the first check passes, but nothing names the type to fill. Refused with the cure in the message.
     */
    /**
     * A driven route whose Java type nothing can name is refused at start, naming the topic - and the refusal
     * lists every way out, because which one a caller wants depends on what they were trying to do.
     * <p>
     * <b>Up front rather than at the first record.</b> A driver that skipped a topic the hydration could not fill
     * would present as a definition whose route never fires, which is a far harder thing to diagnose.
     */
    @Test
    void aDrivenRouteWithNoNamedTypeIsRefusedAndTheRefusalListsEveryWayOut() {
        Sandbox refusing = Sandbox.builder().build();

        IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
                () -> untypedRoute().start(refusing));

        assertThat(refusal).hasMessageThat().contains("legacy");
        assertThat(refusal).hasMessageThat().contains("does not name a Java type");
        assertWithMessage("a refusal that does not say what to do about it is only half a message")
                .that(refusal).hasMessageThat().contains("generating(\"legacy\"");
        assertWithMessage("and saying what a record contains is the other answer, for a caller who never wanted "
                + "fake data in the first place")
                .that(refusal).hasMessageThat().contains("feeding(\"legacy\"");
    }

    /**
     * The type declared on the builder, which is what the refusal above points at: the hydration then fills it.
     */
    @Test
    void aDeclaredTypeIsWhatTheHydrationFills() {
        Sandbox told = Sandbox.builder()
                .perSecond(500)
                .bound(Bound.afterRecords(3))
                .generating("legacy", String.class)
                .build();
        try (ParallelConsumerInstance instance = untypedRoute().start(told)) {
            assertThat(told.awaitBound(Duration.ofSeconds(30))).isTrue();
            instance.awaitShutdown();
        }
        assertThat(told.generatedRecords()).isEqualTo(3);
    }

    /**
     * The other answer: the caller says what a record contains, and the route needs no nameable type at all
     * because nothing is going to fill one.
     */
    @Test
    void aValueFunctionMakesTheNamedTypeUnnecessary() {
        Sandbox told = Sandbox.builder()
                .perSecond(500)
                .bound(Bound.afterRecords(3))
                .feeding("legacy", index -> "scan-" + index)
                .build();
        try (ParallelConsumerInstance instance = untypedRoute().start(told)) {
            assertThat(told.awaitBound(Duration.ofSeconds(30))).isTrue();
            instance.awaitShutdown();
        }
        assertThat(told.generatedRecords()).isEqualTo(3);
    }

    /**
     * The same route on a hand-published sandbox is <b>not</b> refused: nothing is driving it, so nothing needs to
     * know what a record contains until the caller says, by publishing one.
     */
    @Test
    void aHandPublishedSandboxNeedsNoValueFunctionAtAll() {
        Sandbox handPublished = Sandbox.builder().handPublished().build();

        try (ParallelConsumerInstance instance = untypedRoute().start(handPublished)) {
            var ignoredOffset = handPublished.publish("legacy", "cust-1", "scan-1");
            handPublished.awaitSettled();
            assertThat(instance.parkedAllTopics().count()).isEqualTo(0);
        }
        assertWithMessage("the driver published nothing, because there is no driver")
                .that(handPublished.generatedRecords()).isEqualTo(0);
    }

    /**
     * A definition may only be started once, so each arm of the test above needs its own.
     */
    private static ParallelConsumerDefinition untypedRoute() {
        ParallelConsumerDefinition definition = SandboxFixtures.definition();
        definition.topic("legacy")
                .consumed(Consumed.with(Formats.string(), Format.of(SHOUTY_READER, SHOUTY_WRITER)))
                .process(context -> Outcome.succeeded());
        return definition;
    }
}
