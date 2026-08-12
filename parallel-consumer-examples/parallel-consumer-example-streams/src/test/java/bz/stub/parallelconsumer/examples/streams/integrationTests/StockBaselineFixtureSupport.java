package bz.stub.parallelconsumer.examples.streams.integrationTests;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.integrationTests.BrokerIntegrationTest;
import bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;

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
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Everything two stock-baseline generators have in common: the guarantee that they are running genuinely
 * stock Kafka Streams, the shape of the input run, the produce/start/verify machinery, and the fixture file
 * format.
 * <p>
 * <b>Why generators for the astubbs#255 module live in an examples module at all.</b>
 * {@code parallel-consumer-streams} compiles patched copies of four
 * {@code org.apache.kafka.streams.processor.internals} classes into its own {@code target/classes}, which
 * precedes the {@code kafka-streams} jar on the classpath. Every {@code KafkaStreams} instance in that JVM
 * therefore runs the patched classes - including one a test called "the stock arm". Both arms would then
 * share every defect the patch introduced, and comparing them would prove exactly nothing.
 * {@code parallel-consumer-example-streams} does not depend on the module module (and, being earlier in the
 * reactor, the module could not depend on it either), so Kafka Streams here comes from the published jar.
 * {@link #assertClasspathIsStock()} asserts that rather than assuming it, and every generator calls it before
 * recording a single row.
 * <p>
 * <b>The fixture is the contract between the two modules.</b> It carries the inputs as well as the outputs,
 * and the module-side tests replay those inputs rather than reconstructing them - so the two arms cannot drift
 * apart in what they were fed, which is the failure mode that would quietly make the comparison meaningless.
 * The two modules cannot share code in this reactor order, so the file format is the only thing duplicated,
 * and it is deliberately trivial.
 * <p>
 * <b>Default mode is verify, not regenerate.</b> A generator that overwrites its own expectation can never
 * fail. Pass {@code -Dpc.module.fixture.regenerate=true} to rewrite the tracked files - and then read the diff
 * before committing them.
 *
 * @author Antony Stubbs
 * @see StockBaselineFixtureTest
 * @see StockStatefulBaselineFixtureTest
 */
@Slf4j
abstract class StockBaselineFixtureSupport extends BrokerIntegrationTest<String, String> {

    // --- the shape of the run. Mirrored, by necessity, in the module-side tests; the fixture carries the
    // --- resulting data so only these declarations are duplicated, never the records themselves.

    static final int KEYS = 6;
    static final int RECORDS_PER_KEY = 5;
    static final int TOTAL = KEYS * RECORDS_PER_KEY;

    /**
     * Fixed, so regenerating a fixture produces a byte-identical file when nothing has changed - a moving
     * base would make every regeneration look like a behaviour change. Comfortably in the past but well
     * inside the active log segment, which is never eligible for retention deletion.
     */
    static final long TIMESTAMP_BASE = 1_700_000_000_000L;
    static final long TIMESTAMP_STEP = 10L;

    /**
     * Every input record carries this header, whose value is the record's own value in UTF-8. It exists for
     * the module-side probe: {@code context.headers()} is an <em>ambient</em> read off the per-task record
     * context, so a header that matches the value handed to the processor is evidence that the ambient slot
     * belonged to this record and not to a sibling running on another worker thread.
     */
    static final String PROBE_HEADER = "pc-probe-id";

    static final String FIXTURE_VERSION = "1";

    static final String REGENERATE_PROPERTY = "pc.module.fixture.regenerate";

    static final String PATCHED_MODULE = "parallel-consumer-streams";

    /**
     * The classes the module patches. If any of these loads from anywhere but the jar in this JVM, this module
     * has acquired a dependency on the module and the baseline is contaminated.
     */
    private static final List<String> PATCHED_CLASSES = Arrays.asList(
            "org.apache.kafka.streams.processor.internals.StreamTask",
            "org.apache.kafka.streams.processor.internals.AbstractProcessorContext",
            "org.apache.kafka.streams.processor.internals.ProcessorContextImpl",
            "org.apache.kafka.streams.processor.internals.RecordCollectorImpl");

    private static final String PATCHED_MODULE_MARKER_CLASS = "bz.stub.parallelconsumer.streams.PcDispatchSwitch";

    /**
     * Defines the topology under test. A lambda rather than an overridden method so a generator can keep its
     * topology declaration next to the string that describes it in the fixture header.
     */
    interface TopologyDefinition {
        void define(StreamsBuilder builder);
    }

    /**
     * The load-bearing assertion of this whole arrangement, and the reason these generators are not simply a
     * second arm inside the module module. Everything else is only as meaningful as this is.
     */
    static void assertClasspathIsStock() throws ClassNotFoundException {
        for (String name : PATCHED_CLASSES) {
            Class<?> loaded = Class.forName(name);
            URL location = loaded.getProtectionDomain().getCodeSource().getLocation();
            log.info("{} loaded from {}", name, location);

            assertThat(location.toString())
                    .as("%s must load from the published kafka-streams jar in THIS module. If it loads from a "
                                    + "directory, this JVM is running the module's patched copy and the 'stock' "
                                    + "baseline it generates is not stock - it shares every defect the patch "
                                    + "introduced, and comparing the two arms proves nothing.",
                            name)
                    .contains("kafka-streams")
                    .endsWith(".jar");
        }

        assertThatThrownBy(() -> Class.forName(PATCHED_MODULE_MARKER_CLASS))
                .as("the module module must not be on this module's classpath at all - its presence is the "
                        + "single way this baseline could be silently contaminated")
                .isInstanceOf(ClassNotFoundException.class);
    }

    /**
     * The input run, identical for every generator so that one set of records exercises both the stateless
     * and the stateful topology. Keys are interleaved, so a per-key sequence is never a contiguous block of
     * the log and per-key ordering is a real constraint rather than an accident of layout.
     */
    static List<Row> buildInputs() {
        List<Row> inputs = new ArrayList<>();
        int index = 0;
        for (int seq = 0; seq < RECORDS_PER_KEY; seq++) {
            for (int k = 0; k < KEYS; k++) {
                long timestamp = TIMESTAMP_BASE + index * TIMESTAMP_STEP;
                // The value encodes its own expected timestamp on purpose. The module-side probe compares
                // context.timestamp() - an ambient read - against a number carried by the value it was
                // handed as a method argument, so the comparison has an anchor outside the record context.
                // Comparing two ambient reads to each other would agree happily on the wrong record.
                String value = "k" + k + "-s" + seq + "-t" + timestamp;
                inputs.add(new Row("key-" + k, value, timestamp));
                index++;
            }
        }
        return inputs;
    }

    KafkaStreams startTopology(final String applicationIdPrefix, final TopologyDefinition topology) {
        StreamsBuilder builder = new StreamsBuilder();
        topology.define(builder);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationIdPrefix + "-" + System.nanoTime());
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
    void produce(final String inputTopic, final List<Row> inputs) {
        try (KafkaProducer<String, String> producer = getKcu().createNewProducer(KafkaClientUtils.ProducerMode.NOT_TRANSACTIONAL)) {
            for (Row in : inputs) {
                // Sent and awaited one at a time so the log order is the produce order by construction
                // rather than by assumption about in-flight batching - the fixture records an ORDER, and
                // the per-key sequence assertion downstream is only as good as that order is real.
                List<Header> headers = Collections.singletonList(
                        new RecordHeader(PROBE_HEADER, in.value.getBytes(StandardCharsets.UTF_8)));
                producer.send(new ProducerRecord<>(inputTopic, null, in.timestamp, in.key, in.value, headers)).get();
            }
            producer.flush();
        }
        log.info("Produced {} records across {} keys to {}", inputs.size(), KEYS, inputTopic);
    }

    /**
     * The fixture file body. Both generators emit the same row grammar, so the only thing a generator has to
     * decide is what its outputs mean.
     */
    static String render(final Class<?> generator,
                         final String fixtureRelativePath,
                         final String topology,
                         final List<Row> inputs,
                         final List<Row> outputs,
                         final String[] descriptionLines) {
        StringBuilder sb = new StringBuilder();
        sb.append("# Stock Kafka Streams baseline for the astubbs#255 PC-on-Streams module.\n");
        sb.append("#\n");
        sb.append("# GENERATED - do not hand-edit. Written by ").append(generator.getSimpleName()).append(" in\n");
        sb.append("# parallel-consumer-example-streams, a module that does NOT depend on\n");
        sb.append("# ").append(PATCHED_MODULE).append(", so these rows come from the published kafka-streams jar\n");
        sb.append("# and not from the module's patched target/classes. That independence is what makes this a\n");
        sb.append("# baseline rather than a second copy of the thing under test.\n");
        sb.append("#\n");
        for (String line : descriptionLines) {
            sb.append("# ").append(line).append('\n');
        }
        sb.append("#\n");
        sb.append("# Regenerate (and then READ THE DIFF):\n");
        sb.append("#   ./mvnw -pl parallel-consumer-examples/parallel-consumer-example-streams -am clean verify \\\n");
        sb.append("#     -DskipUTs=true -Dit.test=").append(generator.getSimpleName())
                .append(" -Dfailsafe.failIfNoSpecifiedTests=false \\\n");
        sb.append("#     -Dcopyright.skip=true -D").append(REGENERATE_PROPERTY).append("=true\n");
        sb.append("#\n");
        sb.append("# Tracked at ").append(fixtureRelativePath).append('\n');
        sb.append("# 'in' rows are the inputs in produce order, and the module-side test replays exactly these\n");
        sb.append("# rather than rebuilding them, so the two arms cannot drift in what they were fed. Every\n");
        sb.append("# input also carries a header named by probeHeader below, whose value is the record's own\n");
        sb.append("# value in UTF-8.\n");
        sb.append("# 'out' rows are the outputs in the order stock Streams emitted them.\n");
        sb.append("# Columns: type<TAB>key<TAB>value<TAB>timestamp\n");
        sb.append("fixtureVersion\t").append(FIXTURE_VERSION).append('\n');
        sb.append("topology\t").append(topology).append('\n');
        sb.append("probeHeader\t").append(PROBE_HEADER).append('\n');
        for (Row in : inputs) {
            sb.append("in\t").append(in.key).append('\t').append(in.value).append('\t').append(in.timestamp).append('\n');
        }
        for (Row out : outputs) {
            sb.append("out\t").append(out.key).append('\t').append(out.value).append('\t').append(out.timestamp).append('\n');
        }
        return sb.toString();
    }

    /**
     * Rewrites the tracked fixture when explicitly asked, and otherwise asserts that stock Kafka Streams
     * still produces exactly it. The scratch copy is written either way, so a failure can be diffed without
     * re-running the broker.
     */
    @SneakyThrows
    void verifyOrRegenerate(final String rendered, final String fixtureRelativePath) {
        Path tracked = locateTrackedFixture(fixtureRelativePath);

        if (Boolean.getBoolean(REGENERATE_PROPERTY)) {
            Files.createDirectories(tracked.getParent());
            Files.write(tracked, rendered.getBytes(StandardCharsets.UTF_8));
            log.warn("Regenerated the stock baseline fixture at {} - READ THE DIFF before committing it. "
                    + "A changed fixture means stock Kafka Streams behaved differently, or the run shape "
                    + "was edited; neither should pass unexamined.", tracked);
            return;
        }

        Path scratch = Paths.get(System.getProperty("user.dir"), "target", tracked.getFileName().toString());
        Files.createDirectories(scratch.getParent());
        Files.write(scratch, rendered.getBytes(StandardCharsets.UTF_8));

        assertThat(tracked)
                .as("the tracked fixture must exist - the module-side proof test asserts against it, and "
                        + "without it that test has no baseline at all. Regenerate with -D%s=true; this run's "
                        + "output was written to %s", REGENERATE_PROPERTY, scratch)
                .exists();

        String trackedText = new String(Files.readAllBytes(tracked), StandardCharsets.UTF_8);
        assertThat(rendered)
                .as("stock Kafka Streams must still produce exactly the tracked baseline. If this fails, the "
                        + "baseline the module is being judged against no longer describes stock behaviour - "
                        + "do NOT regenerate until you know which of the two changed. This run's output: %s",
                        scratch)
                .isEqualTo(trackedText);

        log.info("Stock baseline re-verified against {}", tracked);
    }

    /**
     * The fixtures are tracked inside the module module, not this one, because both these generators and the
     * fixtures are module scaffolding with the same lifetime - when the throwaway module goes, they go with
     * it, and nothing is left dangling in a shipped example module. The cost is this walk up the tree, which
     * fails loudly rather than silently skipping if the module module is not there.
     */
    static Path locateTrackedFixture(final String relativePath) throws IOException {
        File dir = new File(System.getProperty("user.dir")).getCanonicalFile();
        while (dir != null) {
            if (new File(dir, PATCHED_MODULE).isDirectory()) {
                return new File(dir, relativePath).toPath();
            }
            dir = dir.getParentFile();
        }
        throw new IllegalStateException("Could not find " + PATCHED_MODULE + " above "
                + System.getProperty("user.dir") + " - these generators exist only to feed that module's "
                + "proof tests, so if the module module is gone they should go with it.");
    }

    /**
     * One record, in or out. Values are strings even when the topology emits longs, so both fixtures share a
     * single row grammar; the module-side reader parses whatever the topology's serde produced.
     */
    static final class Row {
        final String key;
        final String value;
        final long timestamp;

        Row(final String key, final String value, final long timestamp) {
            this.key = key;
            this.value = value;
            this.timestamp = timestamp;
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof Row)) {
                return false;
            }
            Row that = (Row) other;
            return timestamp == that.timestamp && key.equals(that.key) && value.equals(that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value, timestamp);
        }

        @Override
        public String toString() {
            return key + "=" + value + "@" + timestamp;
        }
    }
}
