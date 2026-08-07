package io.confluent.parallelconsumer.examples.streams.integrationTests;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.integrationTests.BrokerIntegrationTest;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Generates - and, on every subsequent run, re-verifies - the <b>stock Kafka Streams baseline</b> that
 * {@code PcDrivenStreamsProofTest} in {@code parallel-consumer-streams-spike} asserts against.
 * <p>
 * <b>Why this class lives here, in an examples module, rather than beside the test that consumes it.</b> The
 * spike module generates patched copies of four {@code org.apache.kafka.streams.processor.internals} classes
 * and compiles them into its own {@code target/classes}, which precedes the {@code kafka-streams} jar on the
 * classpath. Every {@code KafkaStreams} instance in that JVM therefore runs the patched classes - including
 * one a test called "the stock arm". Both arms would then share every defect the patch introduced, and
 * comparing them would prove exactly nothing. {@code parallel-consumer-example-streams} does not depend on
 * the spike module (and, being earlier in the reactor, the spike could not depend on it either), so the
 * Kafka Streams that produced the rows below came from the published jar. {@link #theBaselineIsGeneratedByGenuinelyStockKafkaStreams()}
 * asserts that rather than assuming it.
 * <p>
 * <b>The fixture is the contract between the two modules.</b> It carries the inputs as well as the outputs,
 * and the spike-side test replays those inputs rather than reconstructing them - so the two arms cannot
 * drift apart in what they were fed, which is the failure mode that would quietly make the comparison
 * meaningless. The two modules cannot share code (no shared test artifact spans them, in this reactor
 * order), so the file format is the only thing duplicated, and it is deliberately trivial.
 * <p>
 * <b>Default mode is verify, not regenerate.</b> A generator that overwrites its own expectation can never
 * fail. Pass {@code -Dpc.spike.fixture.regenerate=true} to rewrite the tracked file - and then read the diff
 * before committing it.
 *
 * @author Antony Stubbs
 */
@Slf4j
class StockBaselineFixtureTest extends BrokerIntegrationTest<String, String> {

    // --- the shape of the run. Mirrored, by necessity, in the spike-side test; the fixture carries the
    // --- resulting data so only these declarations are duplicated, never the records themselves.

    static final int KEYS = 6;
    static final int RECORDS_PER_KEY = 5;
    static final int TOTAL = KEYS * RECORDS_PER_KEY;

    /**
     * Fixed, so regenerating the fixture produces a byte-identical file when nothing has changed - a moving
     * base would make every regeneration look like a behaviour change. Comfortably in the past but well
     * inside the active log segment, which is never eligible for retention deletion.
     */
    static final long TIMESTAMP_BASE = 1_700_000_000_000L;
    static final long TIMESTAMP_STEP = 10L;

    static final String SUFFIX = "-processed";

    /**
     * Every input record carries this header, whose value is the record's own value in UTF-8. It exists for
     * the spike-side probe: {@code context.headers()} is an <em>ambient</em> read off the per-task record
     * context, so a header that matches the value handed to the processor is evidence that the ambient slot
     * belonged to this record and not to a sibling running on another worker thread.
     */
    static final String PROBE_HEADER = "pc-probe-id";

    static final String TOPOLOGY = "stream -> mapValues((key, value) -> value + \"" + SUFFIX + "\") -> to";

    static final String FIXTURE_VERSION = "1";

    static final String REGENERATE_PROPERTY = "pc.spike.fixture.regenerate";

    static final String FIXTURE_RELATIVE_PATH =
            "parallel-consumer-streams-spike/src/test/resources/stock-baseline-fixture.tsv";

    /**
     * The classes the spike patches. If any of these loads from anywhere but the jar in this JVM, this module
     * has acquired a dependency on the spike and the baseline is contaminated.
     */
    private static final List<String> SPIKE_PATCHED_CLASSES = Arrays.asList(
            "org.apache.kafka.streams.processor.internals.StreamTask",
            "org.apache.kafka.streams.processor.internals.AbstractProcessorContext",
            "org.apache.kafka.streams.processor.internals.ProcessorContextImpl",
            "org.apache.kafka.streams.processor.internals.RecordCollectorImpl");

    private static final String SPIKE_MARKER_CLASS = "io.confluent.parallelconsumer.streamsspike.PcDispatchSwitch";

    /**
     * The load-bearing assertion of this class, and the reason it is not simply a second arm inside the spike
     * module. Everything else here is only as meaningful as this is.
     */
    @Test
    void theBaselineIsGeneratedByGenuinelyStockKafkaStreams() throws Exception {
        assertClasspathIsStock();
    }

    @SneakyThrows
    @Test
    void stockStreamsRunProducesTheTrackedBaselineFixture() {
        assertClasspathIsStock();

        List<InputRecord> inputs = buildInputs();

        String inputTopic = setupTopic("stock-baseline-in");
        String outputTopic = setupTopic("stock-baseline-out");
        ensureTopic(inputTopic, 1);
        ensureTopic(outputTopic, 1);

        KafkaStreams streams = startTopology(inputTopic, outputTopic);
        List<OutputRecord> outputs;
        try {
            produce(inputTopic, inputs);
            outputs = consume(outputTopic);
        } finally {
            streams.close(Duration.ofSeconds(60));
        }

        assertBaselineIsSane(inputs, outputs);

        String rendered = render(inputs, outputs);
        Path tracked = locateTrackedFixture();

        if (Boolean.getBoolean(REGENERATE_PROPERTY)) {
            Files.createDirectories(tracked.getParent());
            Files.write(tracked, rendered.getBytes(StandardCharsets.UTF_8));
            log.warn("Regenerated the stock baseline fixture at {} - READ THE DIFF before committing it. "
                    + "A changed fixture means stock Kafka Streams behaved differently, or the run shape "
                    + "was edited; neither should pass unexamined.", tracked);
            return;
        }

        Path scratch = Paths.get(System.getProperty("user.dir"), "target", "stock-baseline-fixture.tsv");
        Files.createDirectories(scratch.getParent());
        Files.write(scratch, rendered.getBytes(StandardCharsets.UTF_8));

        assertThat(tracked)
                .as("the tracked fixture must exist - the spike-side proof test asserts against it, and "
                        + "without it that test has no baseline at all. Regenerate with -D%s=true; this run's "
                        + "output was written to %s", REGENERATE_PROPERTY, scratch)
                .exists();

        String trackedText = new String(Files.readAllBytes(tracked), StandardCharsets.UTF_8);
        assertThat(rendered)
                .as("stock Kafka Streams must still produce exactly the tracked baseline. If this fails, the "
                        + "baseline the spike is being judged against no longer describes stock behaviour - "
                        + "do NOT regenerate until you know which of the two changed. This run's output: %s",
                        scratch)
                .isEqualTo(trackedText);

        log.info("Stock baseline re-verified against {} ({} inputs, {} outputs)", tracked, inputs.size(), outputs.size());
    }

    // ---------------------------------------------------------------------------------------------------

    private static void assertClasspathIsStock() throws ClassNotFoundException {
        for (String name : SPIKE_PATCHED_CLASSES) {
            Class<?> loaded = Class.forName(name);
            URL location = loaded.getProtectionDomain().getCodeSource().getLocation();
            log.info("{} loaded from {}", name, location);

            assertThat(location.toString())
                    .as("%s must load from the published kafka-streams jar in THIS module. If it loads from a "
                                    + "directory, this JVM is running the spike's patched copy and the 'stock' "
                                    + "baseline it generates is not stock - it shares every defect the patch "
                                    + "introduced, and comparing the two arms proves nothing.",
                            name)
                    .contains("kafka-streams")
                    .endsWith(".jar");
        }

        assertThatThrownBy(() -> Class.forName(SPIKE_MARKER_CLASS))
                .as("the spike module must not be on this module's classpath at all - its presence is the "
                        + "single way this baseline could be silently contaminated")
                .isInstanceOf(ClassNotFoundException.class);
    }

    private static List<InputRecord> buildInputs() {
        List<InputRecord> inputs = new ArrayList<>();
        int index = 0;
        for (int seq = 0; seq < RECORDS_PER_KEY; seq++) {
            for (int k = 0; k < KEYS; k++) {
                long timestamp = TIMESTAMP_BASE + index * TIMESTAMP_STEP;
                // The value encodes its own expected timestamp on purpose. The spike-side probe compares
                // context.timestamp() - an ambient read - against a number carried by the value it was
                // handed as a method argument, so the comparison has an anchor outside the record context.
                // Comparing two ambient reads to each other would agree happily on the wrong record.
                String value = "k" + k + "-s" + seq + "-t" + timestamp;
                inputs.add(new InputRecord("key-" + k, value, timestamp));
                index++;
            }
        }
        return inputs;
    }

    private KafkaStreams startTopology(final String inputTopic, final String outputTopic) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(inputTopic);
        stream.mapValues((key, value) -> value + SUFFIX).to(outputTopic);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stock-baseline-" + System.nanoTime());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.consumerPrefix("auto.offset.reset"), "earliest");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.setUncaughtExceptionHandler(throwable -> {
            log.error("Streams thread died", throwable);
            return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });
        streams.start();

        AtomicInteger polls = new AtomicInteger();
        await().atMost(Duration.ofSeconds(60)).until(() -> {
            KafkaStreams.State state = streams.state();
            if (polls.getAndIncrement() % 10 == 0) {
                log.info("Waiting for stock Streams to run, state={}", state);
            }
            return state == KafkaStreams.State.RUNNING;
        });
        return streams;
    }

    @SneakyThrows
    private void produce(final String inputTopic, final List<InputRecord> inputs) {
        try (KafkaProducer<String, String> producer = getKcu().createNewProducer(KafkaClientUtils.ProducerMode.NOT_TRANSACTIONAL)) {
            for (InputRecord in : inputs) {
                // Sent and awaited one at a time so the log order is the produce order by construction
                // rather than by assumption about in-flight batching - the fixture records an ORDER, and
                // the per-key sequence assertion downstream is only as good as that order is real.
                producer.send(in.toProducerRecord(inputTopic)).get();
            }
            producer.flush();
        }
        log.info("Produced {} records across {} keys to {}", inputs.size(), KEYS, inputTopic);
    }

    private List<OutputRecord> consume(final String outputTopic) {
        List<OutputRecord> outputs = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = getKcu().createNewConsumer(KafkaClientUtils.GroupOption.NEW_GROUP)) {
            consumer.subscribe(Collections.singletonList(outputTopic));

            await().atMost(Duration.ofSeconds(120)).until(() -> {
                ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : polled) {
                    outputs.add(new OutputRecord(record.key(), record.value(), record.timestamp()));
                }
                return outputs.size() >= TOTAL;
            });
        }
        return outputs;
    }

    private static void assertBaselineIsSane(final List<InputRecord> inputs, final List<OutputRecord> outputs) {
        assertThat(inputs).as("the run shape must produce %s input records", TOTAL).hasSize(TOTAL);
        assertThat(outputs)
                .as("stock Kafka Streams must emit exactly one output per input - a baseline recorded from a "
                        + "run that dropped or duplicated records would be worse than no baseline")
                .hasSize(TOTAL);

        Map<String, List<String>> expectedByKey = new LinkedHashMap<>();
        for (InputRecord in : inputs) {
            expectedByKey.computeIfAbsent(in.key, k -> new ArrayList<>()).add(in.value + SUFFIX);
        }
        Map<String, List<String>> actualByKey = new LinkedHashMap<>();
        for (OutputRecord out : outputs) {
            actualByKey.computeIfAbsent(out.key, k -> new ArrayList<>()).add(out.value);
        }
        assertThat(actualByKey.keySet()).as("every key that went in must come out").isEqualTo(expectedByKey.keySet());
        for (Map.Entry<String, List<String>> entry : expectedByKey.entrySet()) {
            assertThat(actualByKey.get(entry.getKey()))
                    .as("key %s must come out in the order it went in", entry.getKey())
                    .containsExactlyElementsOf(entry.getValue());
        }

        Map<String, Long> inputTimestamps = new LinkedHashMap<>();
        for (InputRecord in : inputs) {
            inputTimestamps.put(in.value + SUFFIX, in.timestamp);
        }
        for (OutputRecord out : outputs) {
            assertThat(out.timestamp)
                    .as("stock Kafka Streams carries the input record's timestamp to the sink for %s - the "
                            + "spike asserts that too, and it is the property that breaks first if the "
                            + "per-record context leaks between worker threads", out.value)
                    .isEqualTo(inputTimestamps.get(out.value));
        }
    }

    private static String render(final List<InputRecord> inputs, final List<OutputRecord> outputs) {
        StringBuilder sb = new StringBuilder();
        sb.append("# Stock Kafka Streams baseline for the astubbs#255 PC-on-Streams spike.\n");
        sb.append("#\n");
        sb.append("# GENERATED - do not hand-edit. Written by StockBaselineFixtureTest in\n");
        sb.append("# parallel-consumer-example-streams, a module that does NOT depend on\n");
        sb.append("# parallel-consumer-streams-spike, so these rows come from the published kafka-streams jar\n");
        sb.append("# and not from the spike's patched target/classes. That independence is what makes this a\n");
        sb.append("# baseline rather than a second copy of the thing under test.\n");
        sb.append("#\n");
        sb.append("# Regenerate (and then READ THE DIFF):\n");
        sb.append("#   ./mvnw -pl parallel-consumer-examples/parallel-consumer-example-streams -am clean verify \\\n");
        sb.append("#     -DskipUTs=true -Dit.test=StockBaselineFixtureTest -Dfailsafe.failIfNoSpecifiedTests=false \\\n");
        sb.append("#     -Dcopyright.skip=true -D").append(REGENERATE_PROPERTY).append("=true\n");
        sb.append("#\n");
        sb.append("# 'in' rows are the inputs in produce order, and the spike-side test replays exactly these\n");
        sb.append("# rather than rebuilding them, so the two arms cannot drift in what they were fed. Every\n");
        sb.append("# input also carries a header named by probeHeader below, whose value is the record's own\n");
        sb.append("# value in UTF-8.\n");
        sb.append("# 'out' rows are the outputs in the order stock Streams emitted them.\n");
        sb.append("# Columns: type<TAB>key<TAB>value<TAB>timestamp\n");
        sb.append("fixtureVersion\t").append(FIXTURE_VERSION).append('\n');
        sb.append("topology\t").append(TOPOLOGY).append('\n');
        sb.append("probeHeader\t").append(PROBE_HEADER).append('\n');
        for (InputRecord in : inputs) {
            sb.append("in\t").append(in.key).append('\t').append(in.value).append('\t').append(in.timestamp).append('\n');
        }
        for (OutputRecord out : outputs) {
            sb.append("out\t").append(out.key).append('\t').append(out.value).append('\t').append(out.timestamp).append('\n');
        }
        return sb.toString();
    }

    /**
     * The fixture is tracked inside the spike module, not this one, because both this class and the fixture
     * are spike scaffolding with the same lifetime - when the throwaway module goes, they go with it, and
     * nothing is left dangling in a shipped example module. The cost is this walk up the tree, which fails
     * loudly rather than silently skipping if the spike module is not there.
     */
    private static Path locateTrackedFixture() throws IOException {
        File dir = new File(System.getProperty("user.dir")).getCanonicalFile();
        while (dir != null) {
            File candidate = new File(dir, FIXTURE_RELATIVE_PATH);
            if (new File(dir, "parallel-consumer-streams-spike").isDirectory()) {
                return candidate.toPath();
            }
            dir = dir.getParentFile();
        }
        throw new IllegalStateException("Could not find parallel-consumer-streams-spike above "
                + System.getProperty("user.dir") + " - this test exists only to feed that module's proof "
                + "test, so if the spike module is gone this class should go with it.");
    }

    // ---------------------------------------------------------------------------------------------------

    private static final class InputRecord {
        private final String key;
        private final String value;
        private final long timestamp;

        private InputRecord(final String key, final String value, final long timestamp) {
            this.key = key;
            this.value = value;
            this.timestamp = timestamp;
        }

        private ProducerRecord<String, String> toProducerRecord(final String topic) {
            List<Header> headers = Collections.singletonList(
                    new RecordHeader(PROBE_HEADER, value.getBytes(StandardCharsets.UTF_8)));
            return new ProducerRecord<>(topic, null, timestamp, key, value, headers);
        }
    }

    private static final class OutputRecord {
        private final String key;
        private final String value;
        private final long timestamp;

        private OutputRecord(final String key, final String value, final long timestamp) {
            this.key = key;
            this.value = value;
            this.timestamp = timestamp;
        }
    }
}
