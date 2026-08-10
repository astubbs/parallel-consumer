package io.confluent.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.Isolated;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Layer 2: the DSL refuses at topology construction, seam on - and is completely inert, seam off.
 * <p>
 * <b>The seam-off arm is not padding.</b> Every refusal test here would pass just as well against an
 * unconditional guard, and an unconditional guard would break Apache Kafka's own suite - the 419-test
 * behaviour-preservation claim this whole module rests on. So each refusal is paired with the same topology
 * built with the seam off, which is the only assertion that can tell those two implementations apart.
 * <p>
 * Only the topology is built; no {@code KafkaStreams} is started and no broker is involved, because layer 2
 * fires at the DSL call. That the same constructs are also caught later, at task construction, is
 * {@link ProcessorApiBackstopTest}'s subject.
 * <p>
 * Every method calls methods this module has deliberately deprecated, hence the class-level suppression -
 * the deprecation is the product, and warning about it here would be warning about the feature working.
 *
 * @author Antony Stubbs
 * @see PcUnsupportedConstruct
 */
@SuppressWarnings("deprecation")
// PcDispatchSwitch is process-wide static state and this module inherits concurrent JUnit execution from
// core's junit-platform.properties; left concurrent these methods would toggle each other's switch.
@Execution(ExecutionMode.SAME_THREAD)
@Isolated
class RefusedDslConstructsTest {

    private static final String LEFT_TOPIC = "left";
    private static final String RIGHT_TOPIC = "right";

    private static final Consumed<String, String> STRINGS = Consumed.with(Serdes.String(), Serdes.String());

    @AfterEach
    void restoreSwitch() {
        PcDispatchSwitch.resetToDefault();
    }

    // ---------------------------------------------------------------------------------------------------
    // Joins
    // ---------------------------------------------------------------------------------------------------

    @Test
    void streamStreamJoinRefusesWithTheSeamOnAndBuildsWithItOff() {
        assertRefusedThenAllowed(PcUnsupportedConstruct.KSTREAM_KSTREAM_JOIN, builder -> {
            final KStream<String, String> left = builder.stream(LEFT_TOPIC, STRINGS);
            final KStream<String, String> right = builder.stream(RIGHT_TOPIC, STRINGS);
            left.join(right, (a, b) -> a + b, JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1)));
        });
    }

    @Test
    void streamStreamLeftJoinRefusesWithTheSeamOnAndBuildsWithItOff() {
        assertRefusedThenAllowed(PcUnsupportedConstruct.KSTREAM_KSTREAM_JOIN, builder -> {
            final KStream<String, String> left = builder.stream(LEFT_TOPIC, STRINGS);
            final KStream<String, String> right = builder.stream(RIGHT_TOPIC, STRINGS);
            left.leftJoin(right, (a, b) -> a + b, JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1)));
        });
    }

    @Test
    void streamStreamOuterJoinRefusesWithTheSeamOnAndBuildsWithItOff() {
        assertRefusedThenAllowed(PcUnsupportedConstruct.KSTREAM_KSTREAM_JOIN, builder -> {
            final KStream<String, String> left = builder.stream(LEFT_TOPIC, STRINGS);
            final KStream<String, String> right = builder.stream(RIGHT_TOPIC, STRINGS);
            left.outerJoin(right, (a, b) -> a + b, JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1)));
        });
    }

    @Test
    void streamTableJoinRefusesWithTheSeamOnAndBuildsWithItOff() {
        assertRefusedThenAllowed(PcUnsupportedConstruct.KSTREAM_KTABLE_JOIN, builder -> {
            final KStream<String, String> stream = builder.stream(LEFT_TOPIC, STRINGS);
            final KTable<String, String> table = builder.table(RIGHT_TOPIC, STRINGS);
            stream.join(table, (a, b) -> a + b);
        });
    }

    @Test
    void streamGlobalTableJoinRefusesWithTheSeamOnAndBuildsWithItOff() {
        assertRefusedThenAllowed(PcUnsupportedConstruct.KSTREAM_GLOBALKTABLE_JOIN, builder ->
                builder.stream(LEFT_TOPIC, STRINGS)
                        .join(builder.globalTable(RIGHT_TOPIC, STRINGS), (k, v) -> k, (a, b) -> a + b));
    }

    @Test
    void tableTableJoinRefusesWithTheSeamOnAndBuildsWithItOff() {
        assertRefusedThenAllowed(PcUnsupportedConstruct.KTABLE_KTABLE_JOIN, builder -> {
            final KTable<String, String> left = builder.table(LEFT_TOPIC, STRINGS);
            final KTable<String, String> right = builder.table(RIGHT_TOPIC, STRINGS);
            left.join(right, (a, b) -> a + b);
        });
    }

    @Test
    void tableTableForeignKeyJoinRefusesWithTheSeamOnAndBuildsWithItOff() {
        assertRefusedThenAllowed(PcUnsupportedConstruct.KTABLE_KTABLE_FOREIGN_KEY_JOIN, builder -> {
            final KTable<String, String> left = builder.table(LEFT_TOPIC, STRINGS);
            final KTable<String, String> right = builder.table(RIGHT_TOPIC, STRINGS);
            left.join(right, value -> value, (a, b) -> a + b,
                    Materialized.with(Serdes.String(), Serdes.String()));
        });
    }

    // ---------------------------------------------------------------------------------------------------
    // Windowing
    // ---------------------------------------------------------------------------------------------------

    @Test
    void timeWindowedAggregationRefusesWithTheSeamOnAndBuildsWithItOff() {
        assertRefusedThenAllowed(PcUnsupportedConstruct.WINDOWED_AGGREGATION, builder ->
                builder.stream(LEFT_TOPIC, STRINGS)
                        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1))));
    }

    @Test
    void slidingWindowedAggregationRefusesWithTheSeamOnAndBuildsWithItOff() {
        assertRefusedThenAllowed(PcUnsupportedConstruct.WINDOWED_AGGREGATION, builder ->
                builder.stream(LEFT_TOPIC, STRINGS)
                        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                        .windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1))));
    }

    @Test
    void sessionWindowedAggregationRefusesWithTheSeamOnAndBuildsWithItOff() {
        assertRefusedThenAllowed(PcUnsupportedConstruct.WINDOWED_AGGREGATION, builder ->
                builder.stream(LEFT_TOPIC, STRINGS)
                        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                        .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(1))));
    }

    @Test
    void windowedCogroupedAggregationRefusesWithTheSeamOnAndBuildsWithItOff() {
        // The cogroup route reaches windowing through a different interface, so it needs its own guard and
        // its own proof - CogroupedKStream is not a KGroupedStream.
        assertRefusedThenAllowed(PcUnsupportedConstruct.WINDOWED_COGROUPED_AGGREGATION, builder ->
                builder.stream(LEFT_TOPIC, STRINGS)
                        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                        .cogroup((key, value, aggregate) -> aggregate + value)
                        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1))));
    }

    // ---------------------------------------------------------------------------------------------------
    // Suppression
    // ---------------------------------------------------------------------------------------------------

    @Test
    void suppressionRefusesWithTheSeamOnAndBuildsWithItOff() {
        assertRefusedThenAllowed(PcUnsupportedConstruct.SUPPRESSION, builder ->
                builder.table(LEFT_TOPIC, STRINGS)
                        .suppress(Suppressed.untilTimeLimit(Duration.ofMinutes(1),
                                Suppressed.BufferConfig.unbounded())));
    }

    // ---------------------------------------------------------------------------------------------------
    // The positive control. Without it, refusing absolutely everything would pass every test above.
    // ---------------------------------------------------------------------------------------------------

    @Test
    void aSupportedTopologyStillBuildsWithTheSeamOn() {
        PcDispatchSwitch.enable(2);

        assertThatCode(() -> {
            final StreamsBuilder builder = new StreamsBuilder();
            builder.stream(LEFT_TOPIC, STRINGS)
                    .filter((k, v) -> v != null)
                    // A one-argument lambda rather than a method reference: String::toUpperCase matches both
                    // ValueMapper and ValueMapperWithKey, and the arity here picks ValueMapper unambiguously.
                    .mapValues(value -> value.toUpperCase())
                    .to("out");
            builder.build();
        }).as("stateless stream -> mapValues -> to is the module's proven envelope and must not be refused")
                .doesNotThrowAnyException();
    }

    @Test
    void aNonWindowedAggregationStillBuildsWithTheSeamOn() {
        PcDispatchSwitch.enable(2);

        assertThatCode(() -> {
            final StreamsBuilder builder = new StreamsBuilder();
            builder.stream(LEFT_TOPIC, STRINGS)
                    .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                    .count();
            builder.build();
        }).as("non-windowed stateful aggregation is supported (KTD3) and is what PcDrivenStatefulProofTest "
                + "exercises - refusing it here would delete that proof")
                .doesNotThrowAnyException();
    }

    // ---------------------------------------------------------------------------------------------------

    /**
     * Build the same topology twice: once with the seam on, expecting refusal that names the construct, and
     * once with it off, expecting silence.
     */
    private static void assertRefusedThenAllowed(final PcUnsupportedConstruct expected,
                                                 final TopologyFragment fragment) {
        PcDispatchSwitch.enable(2);
        assertThatThrownBy(() -> fragment.applyTo(new StreamsBuilder()))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(expected.getDisplayName())
                .hasMessageContaining("astubbs#255")
                .hasMessageContaining(PcDispatchSwitch.ENABLED_PROPERTY + "=false");

        PcDispatchSwitch.disable();
        assertThatCode(() -> fragment.applyTo(new StreamsBuilder()))
                .as("with the seam off this module must behave exactly like stock Kafka Streams, which is what "
                        + "lets Apache Kafka's own suite keep passing unmodified")
                .doesNotThrowAnyException();
    }

    @FunctionalInterface
    private interface TopologyFragment {
        void applyTo(StreamsBuilder builder);
    }
}
