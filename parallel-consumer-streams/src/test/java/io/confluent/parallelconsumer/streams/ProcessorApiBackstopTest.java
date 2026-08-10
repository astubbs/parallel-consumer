package io.confluent.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.StoreBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.Arrays;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Layer 3: the backstop in {@code StreamTask}'s constructor, reached the way the DSL refusals cannot be.
 * <p>
 * <b>This is the case that justifies layer 3 existing at all.</b> Every topology here is built with the
 * Processor API: {@code addStateStore} with a window-store builder, connected to a plain
 * {@code Processor}. No {@code KStream} is created, no {@code windowedBy} is called, so
 * {@link RefusedDslConstructsTest}'s guards are never consulted - and without this layer the topology would
 * run and quietly produce wrong window contents.
 * <p>
 * <b>{@code TopologyTestDriver} is the vehicle because it constructs a real {@code StreamTask}</b> (verified
 * against the Kafka 3.9.2 bytecode) with no broker. That makes it a genuine test of the patched constructor
 * rather than of a helper, which is the distinction {@link PcSupportedEnvelopeTest} deliberately cannot
 * make.
 * <p>
 * The seam-on cases only <em>construct</em> the driver and never pipe a record. Construction is the whole
 * claim - the refusal happens there - and driving records through {@code TopologyTestDriver} on the PC
 * dispatch path is a different subject with a different test.
 *
 * @author Antony Stubbs
 */
// PcDispatchSwitch is process-wide static state; concurrent execution would have these methods toggling
// each other's switch, and a seam-off control arm that is only a control by accident is not one.
@Execution(ExecutionMode.SAME_THREAD)
@Isolated
class ProcessorApiBackstopTest {

    private static final String INPUT_TOPIC = "backstop-in";
    private static final String SOURCE = "source";
    private static final String PROCESSOR = "processor";

    @AfterEach
    void restoreSwitch() {
        PcDispatchSwitch.resetToDefault();
    }

    // ---------------------------------------------------------------------------------------------------
    // The Processor API route - unreachable from the DSL guards.
    // ---------------------------------------------------------------------------------------------------

    @Test
    void aWindowStoreBuiltThroughTheProcessorApiIsRefused() {
        PcDispatchSwitch.enable(2);

        assertThatThrownBy(() -> openAndClose(topologyWith(RefusedStoreFixtures.windowStoreBuilder()), atLeastOnce()))
                .as("a window store reached without ever touching KStream must still be refused - this is the "
                        + "hole layer 3 exists to close")
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(PcUnsupportedConstruct.WINDOW_STORE.getDisplayName())
                .hasMessageContaining(PcDispatchSwitch.ENABLED_PROPERTY + "=false");
    }

    @Test
    void aSessionStoreBuiltThroughTheProcessorApiIsRefused() {
        PcDispatchSwitch.enable(2);

        assertThatThrownBy(() -> openAndClose(topologyWith(RefusedStoreFixtures.sessionStoreBuilder()), atLeastOnce()))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(PcUnsupportedConstruct.SESSION_STORE.getDisplayName())
                .hasMessageContaining(PcDispatchSwitch.ENABLED_PROPERTY + "=false");
    }

    @Test
    void aVersionedKeyValueStoreBuiltThroughTheProcessorApiIsRefused() {
        PcDispatchSwitch.enable(2);

        assertThatThrownBy(() -> openAndClose(
                topologyWith(RefusedStoreFixtures.versionedKeyValueStoreBuilder()), atLeastOnce()))
                .as("a versioned store is not a WindowStore and so is the easiest of these to leave unguarded - "
                        + "and it is the one that drops writes rather than merely reordering them")
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(PcUnsupportedConstruct.VERSIONED_KEY_VALUE_STORE.getDisplayName())
                .hasMessageContaining(PcDispatchSwitch.ENABLED_PROPERTY + "=false");
    }

    @Test
    void aSuppressionBufferBuiltThroughTheProcessorApiIsRefused() {
        PcDispatchSwitch.enable(2);

        assertThatThrownBy(() -> openAndClose(
                topologyWith(RefusedStoreFixtures.suppressionBufferBuilder()), atLeastOnce()))
                .as("the suppression buffer is a state store like the other three, so it has to be proven "
                        + "through a real StreamTask too - classifying it correctly in a unit test says nothing "
                        + "about whether the backstop sees it")
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(PcUnsupportedConstruct.SUPPRESSION_BUFFER.getDisplayName())
                .hasMessageContaining(PcDispatchSwitch.ENABLED_PROPERTY + "=false");
    }

    // ---------------------------------------------------------------------------------------------------
    // EOS - configuration, not topology shape.
    // ---------------------------------------------------------------------------------------------------

    @Test
    void exactlyOnceIsRefusedOnAnOtherwiseSupportedTopology() {
        PcDispatchSwitch.enable(2);

        assertThatThrownBy(() -> openAndClose(topologyWith(RefusedStoreFixtures.keyValueStoreBuilder()), exactlyOnce()))
                .as("EOS cannot be inferred from the topology - it has to come off the task config, and this "
                        + "is the only test that proves that wiring")
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(PcUnsupportedConstruct.EXACTLY_ONCE.getDisplayName())
                .hasMessageContaining(PcDispatchSwitch.ENABLED_PROPERTY + "=false");
    }

    // ---------------------------------------------------------------------------------------------------
    // The ordering the refusal depends on.
    // ---------------------------------------------------------------------------------------------------

    /**
     * A refused task must build no dispatcher, which is why {@code PcSupportedEnvelope.checkTask} is called
     * before {@code PcTaskDispatcher.createIfEnabled} in {@code StreamTask}'s constructor and not after.
     * <p>
     * Without this, that ordering is asserted only by a comment. Swapping the two lines would leave every
     * other assertion in this class green while orphaning a worker pool, a {@code WorkManager} and a
     * {@code PcWorkSignal} registration per refused task - and a refused task is one a rebalance will try to
     * create again.
     */
    @Test
    void aRefusedTaskBuildsNoDispatcher() {
        PcDispatchSwitch.enable(2);

        final int before = PcTaskDispatcher.activeCount();

        for (final StoreBuilder<?> refused : Arrays.asList(
                RefusedStoreFixtures.windowStoreBuilder(),
                RefusedStoreFixtures.sessionStoreBuilder(),
                RefusedStoreFixtures.suppressionBufferBuilder(),
                RefusedStoreFixtures.versionedKeyValueStoreBuilder())) {
            assertThatThrownBy(() -> openAndClose(topologyWith(refused), atLeastOnce()))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
        assertThatThrownBy(() -> openAndClose(topologyWith(RefusedStoreFixtures.keyValueStoreBuilder()),
                exactlyOnce()))
                .isInstanceOf(UnsupportedOperationException.class);

        assertThat(PcTaskDispatcher.activeCount())
                .as("every refusal above must happen before the dispatcher is constructed; a live dispatcher "
                        + "here is a worker pool nothing will ever shut down")
                .isEqualTo(before);
    }

    // ---------------------------------------------------------------------------------------------------
    // Control arms. The seam-off pair is what distinguishes a conditional guard from an unconditional one.
    // ---------------------------------------------------------------------------------------------------

    @Test
    void everythingRefusedAboveConstructsNormallyWithTheSeamOff() {
        PcDispatchSwitch.disable();

        assertThatCode(() -> {
            openAndClose(topologyWith(RefusedStoreFixtures.windowStoreBuilder()), atLeastOnce());
            openAndClose(topologyWith(RefusedStoreFixtures.sessionStoreBuilder()), atLeastOnce());
            openAndClose(topologyWith(RefusedStoreFixtures.suppressionBufferBuilder()), atLeastOnce());
            openAndClose(topologyWith(RefusedStoreFixtures.versionedKeyValueStoreBuilder()), atLeastOnce());
            openAndClose(topologyWith(RefusedStoreFixtures.keyValueStoreBuilder()), exactlyOnce());
        }).as("with the seam off the patched StreamTask must construct exactly as stock does - this is the "
                + "assertion that would fail if the backstop were unconditional, and Apache Kafka's own "
                + "StreamTaskTest would fail with it")
                .doesNotThrowAnyException();
    }

    @Test
    void aPlainKeyValueStoreTopologyStillConstructsWithTheSeamOn() {
        PcDispatchSwitch.enable(2);

        // Non-windowed stateful work is inside the supported envelope (KTD3). Without this, a backstop that
        // refused every state store would satisfy every other assertion in this class.
        assertThatCode(() -> openAndClose(topologyWith(RefusedStoreFixtures.keyValueStoreBuilder()), atLeastOnce()))
                .doesNotThrowAnyException();
    }

    @Test
    void aStatelessTopologyStillConstructsWithTheSeamOn() {
        PcDispatchSwitch.enable(2);

        assertThatCode(() -> openAndClose(topologyWith(null), atLeastOnce()))
                .doesNotThrowAnyException();
    }

    // ---------------------------------------------------------------------------------------------------

    /**
     * A source plus one Processor API node, optionally with a state store attached. No DSL anywhere, which is
     * the point.
     */
    private static Topology topologyWith(final StoreBuilder<?> store) {
        final Topology topology = new Topology();
        topology.addSource(SOURCE, Serdes.String().deserializer(), Serdes.String().deserializer(), INPUT_TOPIC);
        topology.addProcessor(PROCESSOR, DoNothingProcessor::new, SOURCE);
        if (store != null) {
            topology.addStateStore(store, PROCESSOR);
        }
        return topology;
    }

    /**
     * Construct the driver and close it again. Construction is the whole claim - the refusal happens in
     * {@code StreamTask}'s constructor - and closing on the way out matters most in the cases that are
     * <em>expected</em> to throw: if the refusal ever stops happening, a leaked driver would turn one clean
     * failure into a cascade of state-directory-lock noise from every test after it.
     */
    private static void openAndClose(final Topology topology, final Properties config) {
        final TopologyTestDriver driver = new TopologyTestDriver(topology, config);
        driver.close();
    }

    private static Properties baseConfig() {
        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "pc-streams-backstop");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        return config;
    }

    private static Properties atLeastOnce() {
        return baseConfig();
    }

    private static Properties exactlyOnce() {
        final Properties config = baseConfig();
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        return config;
    }

    /**
     * The topology has to have a processor for the store to attach to; what it does is irrelevant, because
     * every assertion here is about construction.
     */
    private static final class DoNothingProcessor implements Processor<String, String, Void, Void> {
        @Override
        public void process(final Record<String, String> record) {
            // Deliberately empty - no record is ever piped in.
        }
    }
}
