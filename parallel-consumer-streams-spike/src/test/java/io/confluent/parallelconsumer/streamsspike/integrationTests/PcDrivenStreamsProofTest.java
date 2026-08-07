package io.confluent.parallelconsumer.streamsspike.integrationTests;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.integrationTests.BrokerIntegrationTest;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import io.confluent.parallelconsumer.streamsspike.PcDispatchCounters;
import io.confluent.parallelconsumer.streamsspike.PcDispatchSwitch;
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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * U6, the stateless proof: the same topology and the same records that stock Kafka Streams was fed in
 * {@code StockBaselineFixtureTest}, run instead through Parallel Consumer's {@code WorkManager} and a worker
 * pool - and asserted against what stock produced.
 * <p>
 * <b>The baseline is external on purpose.</b> Nothing in this JVM can generate it: this module compiles
 * patched copies of four {@code processor.internals} classes into {@code target/classes}, which precedes the
 * {@code kafka-streams} jar, so a "stock arm" written here would run the patch too and both arms would share
 * every defect it introduced. The fixture on the classpath was produced by
 * {@code parallel-consumer-example-streams}, which does not depend on this module.
 * {@link #thisJvmRunsThePatchedClassesWhileTheBaselineDidNot()} asserts the two halves of that claim from
 * this side; the fixture test asserts them from the other.
 * <p>
 * <b>The equality vocabulary is deliberate.</b> Across the whole run, output is compared as a
 * <em>multiset</em> - global emission order necessarily differs once records are dispatched to four threads,
 * and an ordered whole-run assertion would go red for exactly the concurrency being demonstrated. Order is
 * asserted where it is actually claimed: <em>within each key</em>, where PC's KEY ordering is what is
 * supposed to preserve it.
 * <p>
 * <b>The probe is the R9 partial substitute.</b> A stateless topology instantiates none of the store
 * wrappers that read the per-task record context ambiently, so without a probe a green result could mean
 * "thread confinement works" or "nothing ever read the confined fields". The transformer below reads
 * {@code context.topic()}, {@code partition()}, {@code offset()}, {@code timestamp()} and {@code headers()}
 * for every record while four workers run concurrently, and compares each against the value it was handed as
 * a method argument - an anchor outside the record context. Comparing two ambient reads against each other
 * would agree perfectly on the wrong record.
 * <p>
 * {@code @Isolated} is load-bearing: {@link PcDispatchSwitch} is process-wide, so an arm running concurrently
 * with the flag-off arm here or with {@link ShadowedStreamsControlTest} would silently destroy the control.
 *
 * @author Antony Stubbs
 * @see ShadowedStreamsControlTest
 * @see PcDrivenStreamsDispatchTest
 */
@Slf4j
@Isolated
class PcDrivenStreamsProofTest extends BrokerIntegrationTest<String, String> {

    private static final String FIXTURE_RESOURCE = "/stock-baseline-fixture.tsv";

    private static final int POOL_SIZE = 4;

    /**
     * Enough that records genuinely overlap inside the chain. Without a cost the pool would drain faster than
     * it fills and the concurrency this test claims to exercise would be an accident of scheduling.
     */
    private static final Duration PROCESSING_COST = Duration.ofMillis(150);

    private static final String SUFFIX = "-processed";

    /**
     * Polls made after the expected output count is reached, purely to catch a surplus record. Three at 500ms
     * is 1.5 seconds of silence against a run whose last record was emitted a moment earlier - not a proof
     * that no thirty-first record will ever appear, but enough to catch the duplicate a re-dispatched chain
     * would produce.
     */
    private static final int SURPLUS_DRAIN_POLLS = 3;

    /** Mirrors {@code StockBaselineFixtureTest.TOPOLOGY}; asserted against the fixture, not assumed. */
    private static final String TOPOLOGY = "stream -> mapValues((key, value) -> value + \"" + SUFFIX + "\") -> to";

    /**
     * The classes this module patches. Asserted here as well as in {@code ShadowedClassLoadingTest} because
     * this is where a false positive would be most expensive: if the jar's copies won, this test would be
     * comparing stock Kafka Streams against a stock Kafka Streams fixture and passing beautifully.
     */
    private static final List<String> PATCHED_CLASSES = Arrays.asList(
            "org.apache.kafka.streams.processor.internals.StreamTask",
            "org.apache.kafka.streams.processor.internals.AbstractProcessorContext",
            "org.apache.kafka.streams.processor.internals.ProcessorContextImpl",
            "org.apache.kafka.streams.processor.internals.RecordCollectorImpl");

    private static Fixture fixture;

    private final AmbientContextProbe probe = new AmbientContextProbe();

    @BeforeAll
    static void loadFixture() throws IOException {
        fixture = Fixture.load(FIXTURE_RESOURCE);
        log.info("Loaded stock baseline fixture: {} inputs, {} outputs, topology '{}'",
                fixture.inputs.size(), fixture.outputs.size(), fixture.topology);
    }

    @BeforeEach
    void resetState() {
        PcDispatchCounters.reset();
        probe.reset();
    }

    @AfterEach
    void turnDispatchOff() {
        PcDispatchSwitch.disable();
    }

    /**
     * The premise, asserted from this side. The other half - that the fixture's JVM had none of this - is
     * asserted by {@code StockBaselineFixtureTest.theBaselineIsGeneratedByGenuinelyStockKafkaStreams}.
     */
    @Test
    void thisJvmRunsThePatchedClassesWhileTheBaselineDidNot() throws Exception {
        for (String name : PATCHED_CLASSES) {
            URL location = Class.forName(name).getProtectionDomain().getCodeSource().getLocation();
            log.info("{} loaded from {}", name, location);
            // Deliberately not asserting on "/classes/" the way the surefire-side ShadowedClassLoadingTest
            // does: failsafe puts the module's packaged JAR on the classpath rather than target/classes, so
            // the patched classes legitimately arrive from parallel-consumer-streams-spike-*.jar here. What
            // matters either way is which ARTIFACT they came from, not its packaging.
            assertThat(location.toString())
                    .as("%s must load from this module's own build output. If it loads from the kafka-streams "
                            + "jar, the comparison below is stock-versus-stock and passes for the wrong "
                            + "reason.", name)
                    .contains("parallel-consumer-streams-spike")
                    .doesNotContain("kafka-streams");
        }

        assertThat(fixture.topology)
                .as("the fixture must have been generated from the same topology this test runs, or the two "
                        + "arms are not comparable however equal their output looks")
                .isEqualTo(TOPOLOGY);
        assertThat(fixture.inputs).as("the fixture must carry the inputs this test replays").isNotEmpty();
        assertThat(fixture.outputs)
                .as("stock produced one output per input; anything else means the baseline itself is broken")
                .hasSameSizeAs(fixture.inputs);
    }

    /**
     * The proof, repeated. One green run of a concurrency experiment is a coin toss with the schedule; the
     * claim being made is that the seam holds, and that is a claim about repetition.
     */
    @RepeatedTest(3)
    void pcDrivenOutputMatchesTheStockBaseline() {
        PcDispatchSwitch.enable(POOL_SIZE);

        List<Row> outputs = runTopology("pc-proof");
        logEvidence("PC ON");

        assertThat(PcDispatchCounters.getRecordsDispatchedToPool())
                .as("records must have reached the chain through PcTaskDispatcher's worker pool. Output "
                        + "equality alone would be satisfied by the stock path, so this counter is what makes "
                        + "the rest of this test mean anything.")
                .isGreaterThanOrEqualTo(fixture.inputs.size());
        assertThat(PcDispatchCounters.getRecordsAcceptedByWorkManager())
                .as("and none may be dropped on the way in for want of a partition-assignment epoch - that "
                        + "drop is logged at warn and is otherwise silent")
                .isEqualTo(PcDispatchCounters.getRecordsOfferedToWorkManager());

        assertThat(probe.peakConcurrency.get())
                .as("with a pool of %s and a %s processor, at least 3 records must be inside the chain at "
                        + "once - a proof taken under serial dispatch would prove nothing about this seam",
                        POOL_SIZE, PROCESSING_COST)
                .isGreaterThanOrEqualTo(3);

        assertAmbientContextWasNeverCrossed();
        assertMatchesBaseline(outputs);
    }

    /**
     * The control for this test rather than for the spike: same harness, same patched classes, PC dispatch
     * off. It separates "the patch changed the output" from "parallel dispatch changed the output" - the
     * two diagnoses a failure of the arm above would otherwise be ambiguous between.
     */
    @Test
    void withDispatchOffTheSameHarnessStillMatchesTheStockBaseline() {
        PcDispatchSwitch.disable();

        List<Row> outputs = runTopology("pc-proof-off");
        logEvidence("PC OFF");

        assertThat(PcDispatchCounters.getRecordsDispatchedToPool())
                .as("with the switch off no record may reach the worker pool")
                .isZero();
        assertThat(probe.peakConcurrency.get())
                .as("and the chain must be walked one record at a time, on the StreamThread")
                .isEqualTo(1);

        assertAmbientContextWasNeverCrossed();
        assertMatchesBaseline(outputs);
    }

    // ---------------------------------------------------------------------------------------------------

    private void assertMatchesBaseline(final List<Row> outputs) {
        assertThat(outputs)
                .as("no record lost and none duplicated: exactly one output per input")
                .hasSameSizeAs(fixture.inputs);

        assertThat(outputs)
                .as("MULTISET equality across the whole run - same records, same multiplicities, order "
                        + "unconstrained. Global order is what parallel dispatch is allowed to change; "
                        + "identity, timestamps and multiplicity are not.")
                .containsExactlyInAnyOrderElementsOf(fixture.outputs);

        Map<String, List<Row>> expectedByKey = groupByKey(fixture.outputs);
        Map<String, List<Row>> actualByKey = groupByKey(outputs);
        assertThat(actualByKey.keySet()).as("every key stock emitted must be emitted here").isEqualTo(expectedByKey.keySet());
        for (Map.Entry<String, List<Row>> entry : expectedByKey.entrySet()) {
            assertThat(actualByKey.get(entry.getKey()))
                    .as("SEQUENCE equality within key %s - this is where order is actually claimed, and PC's "
                            + "KEY ordering is the only reason it survives four workers", entry.getKey())
                    .containsExactlyElementsOf(entry.getValue());
        }
    }

    private void assertAmbientContextWasNeverCrossed() {
        assertThat(probe.violations)
                .as("every ambient read off the per-task record context must describe the record actually "
                        + "being processed. A violation here is the confinement failing: one worker reading "
                        + "another's topic, offset, timestamp or headers.")
                .isEmpty();
        assertThat(probe.observations.get())
                .as("the probe must have seen every record - a probe that observed nothing would report no "
                        + "violations and look identical to a pass")
                .isEqualTo(fixture.inputs.size());
        assertThat(probe.valuesSeenTwice)
                .as("and must have seen each input record exactly once - a repeat means the chain ran twice "
                        + "for one record, which is the duplicate PC's retries would otherwise produce")
                .isEmpty();
    }

    private void logEvidence(final String arm) {
        log.info("=== {} arm | offered={} accepted={} dispatchedToPool={} succeeded={} failed={} "
                        + "peakChainConcurrency={} probeObservations={} violations={}",
                arm,
                PcDispatchCounters.getRecordsOfferedToWorkManager(),
                PcDispatchCounters.getRecordsAcceptedByWorkManager(),
                PcDispatchCounters.getRecordsDispatchedToPool(),
                PcDispatchCounters.getRecordsCompletedSuccessfully(),
                PcDispatchCounters.getRecordsFailed(),
                probe.peakConcurrency.get(),
                probe.observations.get(),
                probe.violations);
    }

    private List<Row> runTopology(final String namePrefix) {
        String inputTopic = setupTopic(namePrefix + "-in");
        String outputTopic = setupTopic(namePrefix + "-out");
        ensureTopic(inputTopic, 1);
        ensureTopic(outputTopic, 1);

        probe.expectedTopic = inputTopic;

        KafkaStreams streams = startTopology(namePrefix, inputTopic, outputTopic);
        try {
            produce(inputTopic);
            return consume(outputTopic);
        } finally {
            streams.close(Duration.ofSeconds(60));
        }
    }

    private KafkaStreams startTopology(final String namePrefix, final String inputTopic, final String outputTopic) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream(inputTopic);
        // The probe node is value-, key- and order-transparent: it returns the value it was given and emits
        // nothing of its own, so its presence cannot change what reaches the sink. It is a reader, added
        // where the fixture topology has nothing, precisely so that the emitted records stay comparable.
        // A fresh transformer per get() - Kafka Streams' ApiUtils.checkSupplier rejects a supplier that hands
        // back the same instance twice. They all share one AmbientContextProbe, which is where the state and
        // the expectations live; only the per-instance ProcessorContext differs, which is the correct split.
        ValueTransformerWithKeySupplier<String, String, String> probeSupplier = probe::newTransformer;
        stream.transformValues(probeSupplier)
                .mapValues((key, value) -> value + SUFFIX)
                .to(outputTopic);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, namePrefix + "-" + System.nanoTime());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.consumerPrefix("auto.offset.reset"), "earliest");
        // One StreamThread, so the only concurrency the probe can observe is the one this spike introduced.
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
                log.info("Waiting for Streams to run, state={}", state);
            }
            return state == KafkaStreams.State.RUNNING;
        });
        return streams;
    }

    /**
     * Replays the fixture's inputs verbatim - key, value, timestamp and probe header - rather than rebuilding
     * them from a second copy of the generation rules. The two modules cannot share code, so replaying is what
     * stops the arms drifting apart in what they were fed.
     * <p>
     * Each send is awaited so the broker-assigned offset can be recorded against the value: that map is the
     * probe's only anchor for {@code context.offset()} that does not itself come from the record context.
     */
    @SneakyThrows
    private void produce(final String inputTopic) {
        try (KafkaProducer<String, String> producer = getKcu().createNewProducer(KafkaClientUtils.ProducerMode.NOT_TRANSACTIONAL)) {
            for (Row in : fixture.inputs) {
                List<Header> headers = Collections.singletonList(
                        new RecordHeader(fixture.probeHeader, in.value.getBytes(StandardCharsets.UTF_8)));
                RecordMetadata metadata = producer.send(
                        new ProducerRecord<>(inputTopic, null, in.timestamp, in.key, in.value, headers)).get();
                probe.expectedOffsetByValue.put(in.value, metadata.offset());
            }
            producer.flush();
        }
        log.info("Replayed {} fixture input records into {}", fixture.inputs.size(), inputTopic);
    }

    /**
     * Reads the expected number of outputs, and then keeps reading for a while longer.
     * <p>
     * Stopping the moment the count is reached is what makes "no duplicates" an assertion that cannot fail:
     * a run that emitted 31 records would hand back the first 30 and, if those 30 happened to be the right
     * ones, look perfect. The extra polls are the only place a surplus record can be observed at all.
     */
    private List<Row> consume(final String outputTopic) {
        List<Row> outputs = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = getKcu().createNewConsumer(KafkaClientUtils.GroupOption.NEW_GROUP)) {
            consumer.subscribe(Collections.singletonList(outputTopic));

            await().atMost(Duration.ofSeconds(120)).until(() -> {
                ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : polled) {
                    outputs.add(new Row(record.key(), record.value(), record.timestamp()));
                }
                return outputs.size() >= fixture.outputs.size();
            });

            for (int quietPoll = 0; quietPoll < SURPLUS_DRAIN_POLLS; quietPoll++) {
                ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : polled) {
                    outputs.add(new Row(record.key(), record.value(), record.timestamp()));
                }
            }
        }
        return outputs;
    }

    private static Map<String, List<Row>> groupByKey(final List<Row> rows) {
        Map<String, List<Row>> byKey = new LinkedHashMap<>();
        for (Row row : rows) {
            byKey.computeIfAbsent(row.key, k -> new ArrayList<>()).add(row);
        }
        return byKey;
    }

    // ---------------------------------------------------------------------------------------------------

    /**
     * One emitted or expected record. Value equality is what makes the multiset comparison a multiset
     * comparison; the timestamp is in it because carrying the input record's timestamp to the sink is an
     * ambient read of the confined context, and dropping it from the comparison would drop the assertion.
     */
    private static final class Row {
        private final String key;
        private final String value;
        private final long timestamp;

        private Row(final String key, final String value, final long timestamp) {
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

    private static final class Fixture {
        private String topology;
        private String probeHeader;
        private final List<Row> inputs = new ArrayList<>();
        private final List<Row> outputs = new ArrayList<>();

        private static Fixture load(final String resource) throws IOException {
            Fixture fixture = new Fixture();
            try (InputStream in = PcDrivenStreamsProofTest.class.getResourceAsStream(resource)) {
                if (in == null) {
                    throw new IllegalStateException("Stock baseline fixture " + resource + " is not on the test "
                            + "classpath. It is generated by StockBaselineFixtureTest in "
                            + "parallel-consumer-example-streams - see that class for the command. This test "
                            + "cannot fabricate it locally: a baseline produced in this JVM would run the "
                            + "patched classes and would not be a baseline at all.");
                }
                BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.isEmpty() || line.charAt(0) == '#') {
                        continue;
                    }
                    String[] parts = line.split("\t", -1);
                    switch (parts[0]) {
                        case "fixtureVersion":
                            if (!"1".equals(parts[1])) {
                                throw new IllegalStateException("Unsupported fixture version " + parts[1]);
                            }
                            break;
                        case "topology":
                            fixture.topology = parts[1];
                            break;
                        case "probeHeader":
                            fixture.probeHeader = parts[1];
                            break;
                        case "in":
                            fixture.inputs.add(new Row(parts[1], parts[2], Long.parseLong(parts[3])));
                            break;
                        case "out":
                            fixture.outputs.add(new Row(parts[1], parts[2], Long.parseLong(parts[3])));
                            break;
                        default:
                            throw new IllegalStateException("Unrecognised fixture row type '" + parts[0] + "'");
                    }
                }
            }
            if (fixture.topology == null || fixture.probeHeader == null) {
                throw new IllegalStateException("Fixture " + resource + " is missing its topology/probeHeader header rows");
            }
            return fixture;
        }
    }

    /**
     * The shared expectations and findings behind the {@code transformValues} node - one instance for the
     * whole run, however many transformer objects Kafka Streams asks the supplier for.
     * <p>
     * Its transformers read the deprecated {@code ProcessorContext}, the last remaining <em>ambient</em>
     * reader in a stateless topology: {@code topic()}, {@code partition()}, {@code offset()},
     * {@code timestamp()} and {@code headers()} all resolve through the per-task record context slot that U4
     * thread-confined. Under stock serial dispatch there is nothing there to get wrong; under four workers
     * sharing one task, a slot that was not confined would hand one worker another worker's record.
     * <p>
     * Violations are collected, not thrown. Throwing here would kill the StreamThread and shut the client
     * down, and the test would then fail on a timeout that says nothing about which read was wrong.
     */
    private static final class AmbientContextProbe {

        private volatile String expectedTopic;
        private final Map<String, Long> expectedOffsetByValue = new ConcurrentHashMap<>();

        private final AtomicInteger inFlight = new AtomicInteger();
        private final AtomicInteger peakConcurrency = new AtomicInteger();
        private final AtomicInteger observations = new AtomicInteger();
        private final Map<String, Boolean> valuesSeen = new ConcurrentHashMap<>();
        private final Map<String, Boolean> valuesSeenTwice = new ConcurrentHashMap<>();
        private final ConcurrentLinkedQueue<String> violations = new ConcurrentLinkedQueue<>();

        private void reset() {
            expectedTopic = null;
            expectedOffsetByValue.clear();
            inFlight.set(0);
            peakConcurrency.set(0);
            observations.set(0);
            valuesSeen.clear();
            valuesSeenTwice.clear();
            violations.clear();
        }

        private ValueTransformerWithKey<String, String, String> newTransformer() {
            return new ProbeTransformer(this);
        }

        private String observe(final ProcessorContext context, final String readOnlyKey, final String value) {
            // Duplicate detection keyed on the value, which arrives as a method argument - deliberately not
            // on context.offset(), which is the very thing under suspicion.
            if (valuesSeen.put(value, Boolean.TRUE) != null) {
                valuesSeenTwice.put(value, Boolean.TRUE);
            }
            int concurrent = inFlight.incrementAndGet();
            peakConcurrency.accumulateAndGet(concurrent, Math::max);
            try {
                inspectAmbientContext(context, readOnlyKey, value);
                Thread.sleep(PROCESSING_COST.toMillis());
                // Read a second time, AFTER the window in which siblings were dispatched. The first read
                // could be right by luck on a slot that is later overwritten; the interesting failure is a
                // context that was correct on entry and belongs to someone else by the time it is used.
                inspectAmbientContext(context, readOnlyKey, value);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                inFlight.decrementAndGet();
            }
            observations.incrementAndGet();
            return value;
        }

        private void inspectAmbientContext(final ProcessorContext context, final String key, final String value) {
            if (!expectedTopic.equals(context.topic())) {
                violations.add("topic for " + key + "/" + value + ": expected " + expectedTopic
                        + " but the ambient context said " + context.topic());
            }
            if (context.partition() != 0) {
                violations.add("partition for " + key + "/" + value + ": expected 0 but the ambient context said "
                        + context.partition());
            }

            Long expectedOffset = expectedOffsetByValue.get(value);
            if (expectedOffset == null) {
                violations.add("no produced offset recorded for value " + value + " - the probe saw a record "
                        + "the test never sent");
            } else if (context.offset() != expectedOffset) {
                violations.add("offset for " + key + "/" + value + ": expected " + expectedOffset
                        + " but the ambient context said " + context.offset());
            }

            // The expected timestamp is carried INSIDE the value, so this compares an ambient read against
            // an argument rather than against another ambient read.
            long expectedTimestamp = Long.parseLong(value.substring(value.lastIndexOf("-t") + 2));
            if (context.timestamp() != expectedTimestamp) {
                violations.add("timestamp for " + key + "/" + value + ": expected " + expectedTimestamp
                        + " but the ambient context said " + context.timestamp());
            }

            Header probeHeader = context.headers().lastHeader(fixture.probeHeader);
            if (probeHeader == null) {
                violations.add("no " + fixture.probeHeader + " header in the ambient context for " + key + "/" + value);
            } else {
                String carried = new String(probeHeader.value(), StandardCharsets.UTF_8);
                if (!value.equals(carried)) {
                    violations.add("headers for " + key + "/" + value + ": the ambient context carried the "
                            + "header of " + carried);
                }
            }
        }
    }

    /**
     * The per-instance half of the probe: it owns nothing but the {@code ProcessorContext} it was initialised
     * with, and delegates every finding to the one shared {@link AmbientContextProbe}. Kafka Streams demands
     * a new object from the supplier on each call, so state that must survive the run cannot live here.
     */
    private static final class ProbeTransformer implements ValueTransformerWithKey<String, String, String> {

        private final AmbientContextProbe probe;

        private ProcessorContext context;

        private ProbeTransformer(final AmbientContextProbe probe) {
            this.probe = probe;
        }

        @Override
        public void init(final ProcessorContext processorContext) {
            this.context = processorContext;
        }

        @Override
        public String transform(final String readOnlyKey, final String value) {
            return probe.observe(context, readOnlyKey, value);
        }

        @Override
        public void close() {
            // nothing to release
        }
    }
}
