package io.confluent.parallelconsumer.streams.integrationTests;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import io.confluent.parallelconsumer.streams.PcDispatchCounters;
import io.confluent.parallelconsumer.streams.integrationTests.BaselineFixture.Row;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The machinery both PC-driven proof arms share: proving the patched classes are the ones running, replaying
 * a stock fixture's inputs, starting a one-StreamThread topology, draining an output topic, and the
 * equality vocabulary the comparison is stated in.
 * <p>
 * <b>The equality vocabulary is deliberate.</b> Across the whole run, output is compared as a
 * <em>multiset</em> - global emission order necessarily differs once records are dispatched to N threads, and
 * an ordered whole-run assertion would go red for exactly the concurrency being demonstrated. Order is
 * asserted where it is actually claimed: <em>within each key</em>, where PC's KEY ordering is what is
 * supposed to preserve it.
 *
 * @author Antony Stubbs
 * @see PcDrivenStreamsProofTest
 * @see PcDrivenStatefulProofTest
 */
@Slf4j
abstract class PcDrivenProofSupport extends BrokerStreamsIntegrationTest {

    static final int POOL_SIZE = 4;

    /**
     * Enough that records genuinely overlap inside the chain. Without a cost the pool would drain faster than
     * it fills and the concurrency these tests claim to exercise would be an accident of scheduling.
     */
    static final Duration PROCESSING_COST = Duration.ofMillis(150);

    /**
     * Polls made after the expected output count is reached, purely to catch a surplus record. Three at 500ms
     * is 1.5 seconds of silence against a run whose last record was emitted a moment earlier - not a proof
     * that no extra record will ever appear, but enough to catch the duplicate a re-dispatched chain would
     * produce.
     */
    static final int SURPLUS_DRAIN_POLLS = 3;

    /**
     * The classes this module patches. Asserted in every arm because this is where a false positive would be
     * most expensive: if the jar's copies won, these tests would be comparing stock Kafka Streams against a
     * stock Kafka Streams fixture and passing beautifully.
     */
    private static final List<String> PATCHED_CLASSES = Arrays.asList(
            "org.apache.kafka.streams.processor.internals.StreamTask",
            "org.apache.kafka.streams.processor.internals.AbstractProcessorContext",
            "org.apache.kafka.streams.processor.internals.ProcessorContextImpl",
            "org.apache.kafka.streams.processor.internals.RecordCollectorImpl");

    /**
     * Half of the premise, asserted from this side. The other half - that the fixture's JVM had none
     * of this - is asserted by the {@code Stock*BaselineFixtureTest} classes in
     * {@code parallel-consumer-example-streams}.
     */
    static void assertThisJvmRunsThePatchedClasses() throws ClassNotFoundException {
        for (String name : PATCHED_CLASSES) {
            URL location = Class.forName(name).getProtectionDomain().getCodeSource().getLocation();
            log.info("{} loaded from {}", name, location);
            // Deliberately not asserting on "/classes/" the way the surefire-side ShadowedClassLoadingTest
            // does: failsafe puts the module's packaged JAR on the classpath rather than target/classes, so
            // the patched classes legitimately arrive from parallel-consumer-streams-*.jar here. What
            // matters either way is which ARTIFACT they came from, not its packaging.
            assertThat(location.toString())
                    .as("%s must load from this module's own build output. If it loads from the kafka-streams "
                            + "jar, the comparison is stock-versus-stock and passes for the wrong reason.", name)
                    .contains("parallel-consumer-streams")
                    .doesNotContain("kafka-streams");
        }
    }

    /**
     * Starts a single-StreamThread topology, so the only concurrency any probe can observe is the one this
     * module introduced.
     *
     * @param topology the builder callback, so a test can keep its topology next to the assertions about it
     */
    KafkaStreams startTopology(final String applicationId, final StockTopology topology) {
        StreamsBuilder builder = new StreamsBuilder();
        topology.define(builder);

        Properties props = baseStreamsProps(applicationId);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        return startAndAwaitRunning(builder, props, LOG_AND_SHUT_DOWN_CLIENT);
    }

    /**
     * Replays the fixture's inputs verbatim - key, value, timestamp and probe header - rather than rebuilding
     * them from a second copy of the generation rules. The two modules cannot share code, so replaying is
     * what stops the arms drifting apart in what they were fed.
     * <p>
     * Each send is awaited so the broker-assigned offset can be recorded against the value: that map is the
     * probe's only anchor for {@code context.offset()} that does not itself come from the record context.
     * <p>
     * <b>Call this before starting the topology.</b> The offset can only be recorded once the send is acked,
     * so a topology that is already RUNNING can fetch and process a record in the window between that ack and
     * the bookkeeping - the probe then finds no offset for a record it is holding and reports one "the test
     * never sent". It is a narrow window, widest on the very first record because an idle StreamThread returns
     * from its poll the instant that record lands, and it fails a run roughly once in eight on a loaded CI
     * machine. Producing everything first closes it by construction; the arms set
     * {@code auto.offset.reset=earliest}, so nothing produced ahead of start is missed.
     */
    @SneakyThrows
    void replayFixtureInputs(final String inputTopic, final BaselineFixture fixture, final AmbientContextProbe probe) {
        try (KafkaProducer<String, String> producer = getKcu().createNewProducer(KafkaClientUtils.ProducerMode.NOT_TRANSACTIONAL)) {
            for (Row in : fixture.getInputs()) {
                List<Header> headers = Collections.singletonList(
                        new RecordHeader(fixture.getProbeHeader(), in.getValue().getBytes(StandardCharsets.UTF_8)));
                RecordMetadata metadata = producer.send(new ProducerRecord<>(
                        inputTopic, null, in.getTimestamp(), in.getKey(), in.getValue(), headers)).get();
                probe.recordProducedOffset(in.getValue(), metadata.offset());
            }
            producer.flush();
        }
        log.info("Replayed {} fixture input records into {}", fixture.getInputs().size(), inputTopic);
    }

    /**
     * Reads the expected number of records, and then keeps reading for a while longer.
     * <p>
     * Stopping the moment the count is reached is what makes "no duplicates" an assertion that cannot fail: a
     * run that emitted one record too many would hand back the first N and, if those N happened to be the
     * right ones, look perfect. The extra polls are the only place a surplus record can be observed at all.
     *
     * @param renderValue how to write this topic's value type into a fixture {@link Row} - fixtures carry
     *                    values as strings so one grammar covers both a String topology and a Long one
     */
    <V> List<Row> drain(final String topic,
                        final int expected,
                        final Properties consumerOverrides,
                        final Function<V, String> renderValue) {
        List<Row> rows = new ArrayList<>();
        try (KafkaConsumer<String, V> consumer = getKcu().createNewConsumer(true, consumerOverrides)) {
            consumer.subscribe(Collections.singletonList(topic));

            await().atMost(Duration.ofSeconds(120)).until(() -> {
                pollInto(consumer, rows, renderValue);
                return rows.size() >= expected;
            });

            for (int quietPoll = 0; quietPoll < SURPLUS_DRAIN_POLLS; quietPoll++) {
                pollInto(consumer, rows, renderValue);
            }
        }
        return rows;
    }

    private static <V> void pollInto(final KafkaConsumer<String, V> consumer,
                                     final List<Row> rows,
                                     final Function<V, String> renderValue) {
        ConsumerRecords<String, V> polled = consumer.poll(Duration.ofMillis(500));
        for (ConsumerRecord<String, V> record : polled) {
            rows.add(new Row(record.key(), renderValue.apply(record.value()), record.timestamp()));
        }
    }

    /**
     * Multiset equality across the run, sequence equality within each key. See the class javadoc for why
     * those two and not a single ordered comparison.
     */
    static void assertMatchesBaseline(final List<Row> outputs, final BaselineFixture fixture) {
        assertThat(outputs)
                .as("no record lost and none duplicated: exactly one output per input")
                .hasSameSizeAs(fixture.getInputs());

        assertThat(outputs)
                .as("MULTISET equality across the whole run - same records, same multiplicities, order "
                        + "unconstrained. Global order is what parallel dispatch is allowed to change; "
                        + "identity, timestamps and multiplicity are not.")
                .containsExactlyInAnyOrderElementsOf(fixture.getOutputs());

        Map<String, List<Row>> expectedByKey = BaselineFixture.groupByKey(fixture.getOutputs());
        Map<String, List<Row>> actualByKey = BaselineFixture.groupByKey(outputs);
        assertThat(actualByKey.keySet()).as("every key stock emitted must be emitted here").isEqualTo(expectedByKey.keySet());
        for (Map.Entry<String, List<Row>> entry : expectedByKey.entrySet()) {
            assertThat(actualByKey.get(entry.getKey()))
                    .as("SEQUENCE equality within key %s - this is where order is actually claimed, and PC's "
                            + "KEY ordering is the only reason it survives %s workers", entry.getKey(), POOL_SIZE)
                    .containsExactlyElementsOf(entry.getValue());
        }
    }

    static void assertAmbientContextWasNeverCrossed(final AmbientContextProbe probe, final int expectedRecords) {
        assertThat(probe.getViolations())
                .as("every ambient read off the per-task record context must describe the record actually "
                        + "being processed. A violation here is the confinement failing: one worker reading "
                        + "another's topic, offset, timestamp or headers.")
                .isEmpty();
        assertThat(probe.getObservationCount())
                .as("the probe must have seen every record - a probe that observed nothing would report no "
                        + "violations and look identical to a pass")
                .isEqualTo(expectedRecords);
        assertThat(probe.getValuesSeenTwice())
                .as("and must have seen each input record exactly once - a repeat means the chain ran twice "
                        + "for one record, which is the duplicate PC's retries would otherwise produce")
                .isEmpty();
    }

    static void logEvidence(final String arm, final AmbientContextProbe probe) {
        log.info("=== {} arm | offered={} accepted={} dispatchedToPool={} succeeded={} failed={} "
                        + "peakChainConcurrency={} probeObservations={} violations={}",
                arm,
                PcDispatchCounters.getRecordsOfferedToWorkManager(),
                PcDispatchCounters.getRecordsAcceptedByWorkManager(),
                PcDispatchCounters.getRecordsDispatchedToPool(),
                PcDispatchCounters.getRecordsCompletedSuccessfully(),
                PcDispatchCounters.getRecordsFailed(),
                probe.getPeakConcurrency(),
                probe.getObservationCount(),
                probe.getViolations());
    }

    /**
     * Defines the topology under test. A lambda rather than an overridden method so a test can keep its
     * topology next to the assertions about it.
     */
    interface StockTopology {
        void define(StreamsBuilder builder);
    }
}
