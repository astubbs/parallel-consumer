package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Properties;

import static bz.stub.parallelconsumer.fluent.AfterRetries.dlqImmediately;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * What a definition actually builds, and when (R4, KTD2, KTD3).
 * <p>
 * Two of these claims are about something that does <b>not</b> happen - a definition that is never started opens
 * nothing, and a definition needing no producer never opens one - so they are asserted through
 * {@link RecordingClientRuntime}, which counts what it was asked for. A producer opened for a definition that has
 * nothing to produce is not a visible fault: it is an idle connection, a transactional id nobody uses, and a
 * dependency on broker permissions the definition did not need.
 */
class ClientConstructionTest {

    private final RecordingClientRuntime runtime = new RecordingClientRuntime();

    private static Properties props() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "client-construction-test");
        return properties;
    }

    @Test
    void aDefinitionThatIsNeverStartedConstructsNoClientAtAll() {
        var pc = ParallelConsumer.define(props());
        pc.json("orders", RouteTypingAndDefaultsTest.Order.class)
                .retryLimit(3)
                .afterRetries(AfterRetries.park())
                .process(context -> Outcome.succeeded());
        pc.validate();

        assertThat(runtime.builtNothing()).isTrue();
    }

    @Test
    void aDefinitionWithNothingToProduceOpensNoProducerAndAsksForNone() {
        var pc = ParallelConsumer.define(props()).commitMode(CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS);
        pc.string("orders").process(context -> Outcome.succeeded());

        assertThat(pc.requiresProducer()).isFalse();
        ParallelConsumerOptions<byte[], byte[]> options = pc.buildOptions(runtime);

        assertThat(runtime.consumerCalls).isEqualTo(1);
        assertThat(runtime.producerCalls).isEqualTo(0);
        assertThat(options.getProducer()).isNull();
        assertThat(options.getProducerConfig()).isNull();
    }

    @Test
    void aDeadLetterDestinationIsEnoughToNeedAProducer() {
        var pc = ParallelConsumer.define(props());
        pc.string("orders").afterRetries(dlqImmediately("orders.dlq")).process(context -> Outcome.succeeded());

        assertThat(pc.requiresProducer()).isTrue();
        ParallelConsumerOptions<byte[], byte[]> options = pc.buildOptions(runtime);

        assertThat(runtime.producerCalls).isEqualTo(1);
        assertThat(options.getProducer()).isNotNull();
    }

    @Test
    void aRouteThatDeclaresProducedTypesNeedsAProducer() {
        var pc = ParallelConsumer.define(props());
        pc.string("orders")
                .produced(Produced.with(Serdes.String(), Serdes.String()))
                .process(context -> Outcome.succeeded());

        assertThat(pc.requiresProducer()).isTrue();
    }

    @Test
    void theTransactionalCommitModeNeedsAProducerEvenWithNothingToProduce() {
        Properties properties = props();
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "client-construction-test");
        var pc = new ParallelConsumerDefinition(properties)
                .commitMode(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER);
        pc.string("orders").process(context -> Outcome.succeeded());

        assertThat(pc.requiresProducer()).isTrue();
    }

    /**
     * R1 and astubbs#410: an instance built from a producer <em>configuration</em> can rebuild its producer, and one
     * handed a finished instance cannot. So when the runtime declines to supply one, the definition hands over the
     * configuration rather than building a producer itself.
     */
    @Test
    void aRuntimeThatSuppliesNoProducerGetsTheConfigurationInsteadWithRawByteSerialisers() {
        var declining = RecordingClientRuntime.decliningToSupplyAProducer();
        var pc = ParallelConsumer.define(props());
        pc.string("orders").afterRetries(dlqImmediately("orders.dlq")).process(context -> Outcome.succeeded());

        ParallelConsumerOptions<byte[], byte[]> options = pc.buildOptions(declining);

        assertThat(options.getProducer()).isNull();
        assertThat(options.getProducerConfig())
                .containsEntry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        assertThat(options.getProducerConfig())
                .containsEntry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        // Consumer-only keys would only earn an unknown-configuration warning from the producer.
        assertThat(options.getProducerConfig()).doesNotContainKey(ConsumerConfig.GROUP_ID_CONFIG);
    }

    /**
     * R23: routes do not compete for one shared limit, so the engine's total admission is the sum of the routes'
     * targets rather than the instance default.
     */
    @Test
    void theEnginesAdmissionTargetIsTheSumOfTheRoutesTargets() {
        var pc = ParallelConsumer.define(props()).defaultConcurrency(10);
        pc.string("orders").process(context -> Outcome.succeeded());
        pc.string("audit").concurrency(100).process(context -> Outcome.succeeded());

        assertThat(pc.buildOptions(runtime).getMaxConcurrency()).isEqualTo(110);
    }

    @Test
    void aPreBuiltConsumerIsUsedInsteadOfAskingTheRuntime() {
        var pc = ParallelConsumer.define(props())
                .consumer(new LongPollingMockConsumer<>(OffsetResetStrategy.EARLIEST));
        pc.string("orders").process(context -> Outcome.succeeded());

        ParallelConsumerOptions<byte[], byte[]> options = pc.buildOptions(runtime);

        assertThat(runtime.consumerCalls).isEqualTo(0);
        assertThat(options.getConsumer()).isNotNull();
    }

    /**
     * KTD3: the engine already refuses a consumer whose subscription it does not own, and the fluent API inherits
     * that rather than adding a second check that could drift from it. Refused in the processor's constructor, so
     * nothing is started and there is no instance to close.
     * <p>
     * The consumer here is a Mockito mock rather than the shipped mock consumer because the engine's check
     * <b>exempts every {@code MockConsumer}</b> by design - "disabled for unit tests which don't test rebalancing".
     * A test written over the shipped mock passes whatever the fluent API does, which is how this one was caught.
     */
    @Test
    @SuppressWarnings("unchecked")
    void aPreBuiltConsumerThatIsAlreadySubscribedIsRefusedAtStart() {
        Consumer<byte[], byte[]> subscribed = Mockito.mock(Consumer.class);
        Mockito.when(subscribed.subscription()).thenReturn(Collections.singleton("orders"));
        Mockito.when(subscribed.assignment()).thenReturn(Collections.emptySet());
        // The engine checks the group id first, so without this the test would prove that check instead.
        Mockito.when(subscribed.groupMetadata())
                .thenReturn(new ConsumerGroupMetadata("client-construction-test"));
        var pc = ParallelConsumer.define(props()).consumer(subscribed);
        pc.string("orders").process(context -> Outcome.succeeded());

        var thrown = assertThrows(IllegalStateException.class, () -> pc.start(runtime));

        assertThat(thrown).hasMessageThat().contains("subscription must be managed by the Parallel Consumer");
    }

    @Test
    void aDefinitionCanOnlyBeStartedOnce() {
        var pc = ParallelConsumer.define(props());
        pc.string("orders").process(context -> Outcome.succeeded());
        pc.buildOptions(runtime);

        var thrown = assertThrows(IllegalStateException.class, () -> pc.buildOptions(runtime));

        assertThat(thrown).hasMessageThat().contains("already been started");
    }

    /**
     * The runtime seam sees the definition, which is what lets the sandbox generate records of the right types for
     * the right topics without the definition changing (KTD9).
     */
    @Test
    void theRuntimeSeamIsHandedTheValidatedDefinition() {
        var pc = ParallelConsumer.define(props());
        pc.json("orders", RouteTypingAndDefaultsTest.Order.class).process(context -> Outcome.succeeded());
        pc.buildOptions(runtime);

        DefinitionView seen = runtime.definitionsSeen.get(0);
        assertThat(seen.topics()).containsExactly("orders");
        assertThat(seen.commitMode()).isEqualTo(CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS);
        assertThat(seen.route("orders").consumedValue().hasSerializer()).isTrue();
    }
}
