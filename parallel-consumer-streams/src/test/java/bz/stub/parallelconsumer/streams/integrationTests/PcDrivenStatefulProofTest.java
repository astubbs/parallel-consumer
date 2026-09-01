package bz.stub.parallelconsumer.streams.integrationTests;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.streams.PcDispatchCounters;
import bz.stub.parallelconsumer.streams.PcDispatchSwitch;
import bz.stub.parallelconsumer.streams.integrationTests.BaselineFixture.Row;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * U7, the stateful proof - and the unit that decides whether R9 is answered or merely undisturbed.
 *
 * <h2>Why a stateless green result was not enough</h2>
 * The report's single load-bearing change is thread-confining the per-task {@code recordContext} slot. That
 * change is only load-bearing where something reads the slot <em>ambiently</em>, and in Kafka Streams every
 * such reader is a state-store class: {@code MeteredKeyValueStore} (end-to-end latency),
 * {@code ChangeLoggingKeyValueBytesStore} (the timestamp it stamps on every changelog record),
 * {@code StoreQueryUtils} (the store's IQv2 {@code Position}). A {@code stream -> mapValues -> to} topology
 * instantiates none of them, so every green result before this one was compatible with both "the confinement
 * works" and "the confinement was never needed here".
 *
 * <h2>What this topology adds, and why exactly this much</h2>
 * <ul>
 *   <li>A non-windowed {@code count} over a {@code withCachingDisabled()} KV store. This is the operator the
 *       route choice turns on: the origin report ranks forking Streams internals above building a PC-native
 *       DSL <em>solely</em> because this route inherits state stores for free, so an implementation that never
 *       materialises one does not test that ranking at all.</li>
 *   <li>A second, explicitly registered <b>non-timestamped</b> KV store, written by the probe node. This one
 *       is not decoration. The DSL's {@code count} materialises a <em>timestamped</em> store, and
 *       {@code ChangeLoggingTimestampedKeyValueBytesStore} takes the changelog record's timestamp from the
 *       timestamp embedded in the value - which {@code KStreamAggregate} computed from the {@code Record}
 *       argument, not from the ambient slot. So the count's changelog timestamp is <em>not</em> an ambient
 *       read and asserting on it would prove nothing about confinement. The plain
 *       {@code ChangeLoggingKeyValueBytesStore} stamps {@code context.timestamp()} instead, which is the
 *       ambient read, observable from outside the JVM as a record timestamp on a changelog topic. Finding
 *       that distinction is itself a result: the obvious assertion is the vacuous one.</li>
 * </ul>
 * Windowed operators, joins and suppression are deliberately out: they change their own semantics under
 * out-of-order processing, so a failure could not be attributed to the confinement.
 * {@code CachingKeyValueStore} is out too, by the same scope decision that disables caching.
 *
 * <h2>The assertions, in order of what they are worth</h2>
 * <ol>
 *   <li><b>Per-key aggregate values are correct - no lost updates.</b> Derived from the fixture's inputs, not
 *       from its outputs, so it is a statement about arithmetic rather than a second comparison.</li>
 *   <li>Output equality against the stock fixture: multiset across the run, sequence within each key.</li>
 *   <li>Changelog records carry the timestamp of the record that produced them - the confinement property,
 *       observed through Kafka's own store stack rather than through a probe written here.</li>
 *   <li>The ambient probe, repeated from U6, now running alongside real store traffic.</li>
 * </ol>
 *
 * {@code @Isolated} is load-bearing: {@link PcDispatchSwitch} is process-wide, so an arm running concurrently
 * with the flag-off arm here or with {@link ShadowedStreamsControlTest} would silently destroy the control.
 *
 * @author Antony Stubbs
 * @see PcDrivenStreamsProofTest
 */
@Slf4j
@Isolated
class PcDrivenStatefulProofTest extends PcDrivenProofSupport {

    private static final String FIXTURE_RESOURCE = "/stock-stateful-baseline-fixture.tsv";

    /** Mirrors {@code StockStatefulBaselineFixtureTest.COUNT_STORE}; asserted via the topology string. */
    private static final String COUNT_STORE = "pc-streams-counts";

    /**
     * The module-side-only store whose changelog timestamp IS an ambient read - see the class javadoc. It has
     * no counterpart in the stock fixture because it emits nothing downstream; stock needs no evidence that
     * its own single thread saw its own record.
     */
    private static final String PROBE_STORE = "pc-module-probe-store";

    private static final String TOPOLOGY =
            "stream -> groupByKey -> count(Materialized.as(\"" + COUNT_STORE + "\").withCachingDisabled()) -> toStream -> to";

    private static BaselineFixture fixture;

    private AmbientContextProbe probe;

    @BeforeAll
    static void loadFixture() throws IOException {
        fixture = BaselineFixture.load(FIXTURE_RESOURCE);
        log.info("Loaded stock stateful baseline fixture: {} inputs, {} outputs, topology '{}'",
                fixture.getInputs().size(), fixture.getOutputs().size(), fixture.getTopology());
    }

    @BeforeEach
    void resetState() {
        PcDispatchCounters.reset();
        probe = new AmbientContextProbe(fixture.getProbeHeader(), PROCESSING_COST.toMillis(), PROBE_STORE);
    }

    /** Hand the JVM back at the artifact's default (on), so the next test states its own requirement. */
    @AfterEach
    void restoreDefaultDispatch() {
        PcDispatchSwitch.resetToDefault();
    }

    @Test
    void thisJvmRunsThePatchedClassesWhileTheBaselineDidNot() throws Exception {
        assertThisJvmRunsThePatchedClasses();

        assertThat(fixture.getTopology())
                .as("the fixture must have been generated from the same aggregation this test runs, or the "
                        + "two arms are not comparable however equal their output looks")
                .isEqualTo(TOPOLOGY);
        assertThat(fixture.getOutputs())
                .as("with caching disabled stock emits one update per input; anything else means the "
                        + "baseline itself is broken")
                .hasSameSizeAs(fixture.getInputs());
    }

    /**
     * The proof, repeated. One green run of a concurrency experiment is a coin toss with the schedule.
     */
    @RepeatedTest(3)
    void pcDrivenAggregationMatchesTheStockBaseline() {
        PcDispatchSwitch.enable(POOL_SIZE);

        Run run = runTopology("pc-stateful");
        logEvidence("PC ON stateful", probe);
        logChangelogEvidence(run);

        assertThat(PcDispatchCounters.getRecordsDispatchedToPool())
                .as("records must have reached the chain - and therefore the store stack - through "
                        + "PcTaskDispatcher's worker pool. Output equality alone would be satisfied by the "
                        + "stock path, so this counter is what makes the rest of this test mean anything.")
                .isGreaterThanOrEqualTo(fixture.getInputs().size());
        assertThat(PcDispatchCounters.getRecordsAcceptedByWorkManager())
                .as("and none may be dropped on the way in for want of a partition-assignment epoch")
                .isEqualTo(PcDispatchCounters.getRecordsOfferedToWorkManager());

        assertThat(probe.getPeakConcurrency())
                .as("with a pool of %s and a %s processor, at least 3 records must be inside the chain at "
                        + "once - an aggregation proven under serial dispatch proves nothing about this seam",
                        POOL_SIZE, PROCESSING_COST)
                .isGreaterThanOrEqualTo(3);

        // Deliberate order: the aggregate first because it is the claim this arm exists to make, then the
        // two confinement observations, and only then whole-run equality. Equality is the SYMPTOM of a
        // context leak; reporting it first buries the cause under a 30-element diff.
        assertAggregatesAreCorrect(run.outputs);
        assertAmbientContextWasNeverCrossed(probe, fixture.getInputs().size());
        assertChangelogTimestampsBelongToTheirOwnRecord(run.probeChangelog);
        assertCountChangelogRecordedEveryUpdate(run.countChangelog);
        assertMatchesBaseline(run.outputs, fixture);
    }

    /**
     * Same harness, same patched classes, same stores - PC dispatch off. Separates "the patch changed the
     * aggregation" from "parallel dispatch changed the aggregation".
     */
    @Test
    void withDispatchOffTheSameHarnessStillMatchesTheStockBaseline() {
        // Explicit, and load-bearing: the seam defaults ON, so without this line this arm is not a control.
        PcDispatchSwitch.disable();

        Run run = runTopology("pc-stateful-off");
        logEvidence("PC OFF stateful", probe);
        logChangelogEvidence(run);

        assertThat(PcDispatchCounters.getRecordsDispatchedToPool())
                .as("with the switch off no record may reach the worker pool")
                .isZero();
        assertThat(probe.getPeakConcurrency())
                .as("and the chain must be walked one record at a time, on the StreamThread")
                .isEqualTo(1);

        // Deliberate order: the aggregate first because it is the claim this arm exists to make, then the
        // two confinement observations, and only then whole-run equality. Equality is the SYMPTOM of a
        // context leak; reporting it first buries the cause under a 30-element diff.
        assertAggregatesAreCorrect(run.outputs);
        assertAmbientContextWasNeverCrossed(probe, fixture.getInputs().size());
        assertChangelogTimestampsBelongToTheirOwnRecord(run.probeChangelog);
        assertCountChangelogRecordedEveryUpdate(run.countChangelog);
        assertMatchesBaseline(run.outputs, fixture);
    }

    // ---------------------------------------------------------------------------------------------------

    /**
     * Reports the store-stack verdict as a log line as well as an assertion.
     * <p>
     * The assertions below fire in cause-before-symptom order and the first one to fail ends the test, so
     * without this a run that trips the probe would never say what happened at the changelog - and the
     * changelog is where Kafka's OWN ambient read is observable. A diagnostic, not a substitute: the
     * assertions still have to pass.
     */
    private static void logChangelogEvidence(final Run run) {
        int probeMismatches = 0;
        for (Row logged : run.probeChangelog) {
            if (logged.getTimestamp() != AmbientContextProbe.expectedTimestampOf(logged.getKey())) {
                probeMismatches++;
            }
        }
        log.info("=== changelog evidence | probeStore records={} timestampsBelongingToAnotherRecord={} "
                        + "| countStore records={}",
                run.probeChangelog.size(), probeMismatches, run.countChangelog.size());
    }

    /**
     * The one that matters. A lost update under concurrent dispatch shows up here as a count that skips a
     * number, or a final count below the number of records for the key.
     * <p>
     * The expectation is computed from the fixture's <em>inputs</em>. Comparing against the fixture's outputs
     * as well is worth doing - {@link PcDrivenProofSupport#assertMatchesBaseline} does - but on its own it
     * would only say "the same as stock", and if the arithmetic were wrong in both arms it would say it
     * cheerfully.
     */
    private static void assertAggregatesAreCorrect(final List<Row> outputs) {
        Map<String, Integer> recordsPerKey = new LinkedHashMap<>();
        for (Row in : fixture.getInputs()) {
            recordsPerKey.merge(in.getKey(), 1, Integer::sum);
        }

        Map<String, List<Row>> emittedByKey = BaselineFixture.groupByKey(outputs);
        assertThat(emittedByKey.keySet())
                .as("every key that was fed in must have produced counts")
                .isEqualTo(recordsPerKey.keySet());

        for (Map.Entry<String, Integer> entry : recordsPerKey.entrySet()) {
            String key = entry.getKey();
            int expectedRecords = entry.getValue();

            List<String> expectedCounts = new ArrayList<>();
            for (int n = 1; n <= expectedRecords; n++) {
                expectedCounts.add(Long.toString(n));
            }

            List<String> actualCounts = new ArrayList<>();
            for (Row emitted : emittedByKey.get(key)) {
                actualCounts.add(emitted.getValue());
            }

            assertThat(actualCounts)
                    .as("key %s was fed %s records, so its aggregate must climb 1..%s with no value missed "
                            + "and none repeated. A gap here is a LOST UPDATE: two workers read the same "
                            + "prior count and one of their writes was thrown away. This is the assertion "
                            + "the whole stateful arm exists to make.", key, expectedRecords, expectedRecords)
                    .containsExactlyElementsOf(expectedCounts);
        }
    }

    /**
     * The confinement property, observed through Kafka's own store stack.
     * <p>
     * Each changelog record's <b>key</b> is the value of the input record that wrote it - an argument the
     * processor was handed. Each changelog record's <b>timestamp</b> was taken by
     * {@code ChangeLoggingKeyValueBytesStore.put} from {@code context.timestamp()}, an ambient read of the
     * per-task slot, on a worker thread, after that worker had already slept long enough for every sibling to
     * be dispatched. If the slot were shared rather than confined, this is where one record's write would
     * carry another record's timestamp - and it would be invisible in the output topic, because the sink
     * takes its timestamp from the {@code Record} instead.
     */
    private static void assertChangelogTimestampsBelongToTheirOwnRecord(final List<Row> probeChangelog) {
        assertThat(probeChangelog)
                .as("the probe store must have logged exactly one changelog record per input - a changelog "
                        + "that recorded nothing would produce no mismatches and look identical to a pass")
                .hasSameSizeAs(fixture.getInputs());

        for (Row logged : probeChangelog) {
            long expected = AmbientContextProbe.expectedTimestampOf(logged.getKey());
            assertThat(logged.getTimestamp())
                    .as("the changelog record written for %s must carry ITS OWN record's timestamp. Kafka "
                            + "reads that timestamp ambiently off the per-task record context inside "
                            + "ChangeLoggingKeyValueBytesStore.put; a mismatch here is that context having "
                            + "been overwritten by a sibling worker - which is precisely the failure the "
                            + "thread-confinement exists to prevent.", logged.getKey())
                    .isEqualTo(expected);
            assertThat(logged.getValue())
                    .as("and the changelog entry's value must be the record that wrote it")
                    .isEqualTo(logged.getKey());
        }
    }

    /**
     * The aggregation store's own changelog. Weaker evidence about confinement than the probe store's - a
     * timestamped store's changelog timestamp comes from the value, not the ambient slot - but it is the
     * place where a lost update would be visible <em>at the store</em> rather than only downstream of it.
     */
    private static void assertCountChangelogRecordedEveryUpdate(final List<Row> countChangelog) {
        assertThat(countChangelog)
                .as("with caching disabled every update is logged, so the count store's changelog must carry "
                        + "one record per input")
                .hasSameSizeAs(fixture.getInputs());

        Map<String, List<Row>> byKey = BaselineFixture.groupByKey(countChangelog);
        for (Map.Entry<String, List<Row>> entry : byKey.entrySet()) {
            List<String> counts = new ArrayList<>();
            for (Row logged : entry.getValue()) {
                counts.add(logged.getValue());
            }
            List<String> expected = new ArrayList<>();
            for (int n = 1; n <= entry.getValue().size(); n++) {
                expected.add(Long.toString(n));
            }
            assertThat(counts)
                    .as("the store itself must have seen key %s climb 1..%s in order. Asserting on the "
                            + "output topic alone would leave open the possibility that the store lost an "
                            + "update and the sink happened to emit a plausible sequence anyway.",
                            entry.getKey(), entry.getValue().size())
                    .containsExactlyElementsOf(expected);
        }
    }

    private Run runTopology(final String namePrefix) {
        String applicationId = namePrefix + "-" + System.nanoTime();
        String inputTopic = setupTopic(namePrefix + "-in");
        String outputTopic = setupTopic(namePrefix + "-out");
        ensureTopic(inputTopic, 1);
        ensureTopic(outputTopic, 1);

        probe.setExpectedTopic(inputTopic);

        // Replayed BEFORE the topology starts, and the ordering is load-bearing - see replayFixtureInputs.
        // Starting first leaves a window in which a running StreamThread can process a record before the test
        // has recorded that record's offset, and the probe then reports a record "the test never sent".
        replayFixtureInputs(inputTopic, fixture, probe);

        // The probe node is value-, key- and order-transparent: it returns the value it was given and emits
        // nothing of its own, so its presence cannot change what the count sees or what reaches the sink.
        // transformValues cannot change the key, so groupByKey below still needs no repartition.
        ValueTransformerWithKeySupplier<String, String, String> probeSupplier = this::newProbeTransformer;
        StoreBuilder<KeyValueStore<String, String>> probeStore = Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(PROBE_STORE), Serdes.String(), Serdes.String())
                .withLoggingEnabled(Collections.emptyMap())
                // Explicit, though it is also the default for a hand-built store: with the record cache in
                // front, the changelog write would happen at flush time on the StreamThread and the ambient
                // read this test turns on would never happen on a worker at all.
                .withCachingDisabled();

        KafkaStreams streams = startTopology(applicationId, builder -> {
            builder.addStateStore(probeStore);
            KStream<String, String> stream = builder.stream(inputTopic);
            stream.transformValues(probeSupplier, PROBE_STORE)
                    .groupByKey()
                    .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(COUNT_STORE)
                            .withCachingDisabled())
                    .toStream()
                    .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));
        });

        List<Row> outputs;
        try {
            outputs = drain(outputTopic, fixture.getOutputs().size(), longValueConsumerProps(),
                    (Long count) -> Long.toString(count));
        } finally {
            // Closed before the changelogs are read: close flushes the Streams producer, so what is on those
            // topics afterwards is the complete record of what the stores were told.
            streams.close(Duration.ofSeconds(60));
        }

        int expected = fixture.getInputs().size();
        List<Row> probeChangelog = drain(changelogTopic(applicationId, PROBE_STORE), expected,
                new Properties(), Function.identity());
        List<Row> countChangelog = drain(changelogTopic(applicationId, COUNT_STORE), expected,
                longValueConsumerProps(), (Long count) -> Long.toString(count));

        return new Run(outputs, probeChangelog, countChangelog);
    }

    private static String changelogTopic(final String applicationId, final String storeName) {
        return applicationId + "-" + storeName + "-changelog";
    }

    /**
     * The count topology and the count store's changelog both carry {@code Long} values, so the shared
     * consumer factory's String deserializer has to be overridden. Reading them as strings would "work" and
     * would silently stop comparing what the topology emitted.
     */
    private static Properties longValueConsumerProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        return props;
    }

    private ValueTransformerWithKey<String, String, String> newProbeTransformer() {
        return probe.newTransformer();
    }

    /** What one execution of the topology produced, across the output topic and both changelogs. */
    private static final class Run {
        private final List<Row> outputs;
        private final List<Row> probeChangelog;
        private final List<Row> countChangelog;

        private Run(final List<Row> outputs, final List<Row> probeChangelog, final List<Row> countChangelog) {
            this.outputs = outputs;
            this.probeChangelog = probeChangelog;
            this.countChangelog = countChangelog;
        }
    }
}
