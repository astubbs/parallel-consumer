package bz.stub.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.streams.protocol.v1alpha1.Aggregate;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.CombineKind;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.DataType;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.HandleKind;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.HandleType;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.TimeWindowSpec;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The assembler turns a host's replayed builder calls into a running topology.
 *
 * <p>The end-to-end case here is the definition-path oracle in unit form: the topology is built only from calls a
 * host issued, and then actually counts. Nothing in this class hardcodes the chain's shape.
 */
class TopologyAssemblerTest {

    /** Echoes the value back, so the assembly is what is under test rather than any mapping behaviour. */
    private final TopologyAssembler.MapperFactory echo = token -> (key, value) -> value;

    /** Concatenates aggregate and value, so a reduction's result depends on BOTH arguments crossing correctly. */
    private final TopologyAssembler.ReducerFactory concat = token -> (aggregate, value) -> {
        byte[] joined = new byte[aggregate.length + value.length];
        System.arraycopy(aggregate, 0, joined, 0, aggregate.length);
        System.arraycopy(value, 0, joined, aggregate.length, value.length);
        return joined;
    };

    /** Joins stream value to table value with a separator, so a transposed pair is visible rather than plausible. */
    private final TopologyAssembler.JoinerFactory joining =
            token -> (streamValue, tableValue) -> bytes(new String(streamValue, StandardCharsets.UTF_8) + ">"
                    + new String(tableValue, StandardCharsets.UTF_8));

    /** Appends each value to the accumulator, so the initializer's bytes and every crossing show in the result. */
    private final TopologyAssembler.AggregatorFactory appending = token -> (key, value, aggregate) -> {
        byte[] joined = new byte[aggregate.length + value.length];
        System.arraycopy(aggregate, 0, joined, 0, aggregate.length);
        System.arraycopy(value, 0, joined, aggregate.length, value.length);
        return joined;
    };

    @Test
    void eachCallReturnsAHandleTheNextCallCanName() {
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, appending);

        long source = assembler.source("in");
        long mapped = assembler.mapValues(source, 42);
        long grouped = assembler.groupByKey(mapped);
        long counted = assembler.count(grouped, "counts");

        assertThat(Stream.of(source, mapped, grouped, counted).distinct().count()).isEqualTo(4);
        assertThat(source).isGreaterThan(0L);
    }

    @Test
    void aCallNamingAnUnknownHandleIsRefusedAndTheErrorNamesIt() {
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, appending);
        assembler.source("in");

        TopologyDescriptionException thrown =
                assertThrows(TopologyDescriptionException.class, () -> assembler.groupByKey(9999));

        assertThat(thrown).hasMessageThat().contains("9999");
        assertThat(thrown).hasMessageThat().contains("groupByKey");
    }

    @Test
    void aCallAppliedToTheWrongKindOfHandleIsRefusedInProtocolVocabulary() {
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, appending);
        long source = assembler.source("in");

        // count needs a grouped stream; a source is not one. The refusal speaks the protocol's vocabulary -
        // "stream", "grouped stream" - never a Kafka Streams implementation class name, which means nothing to a
        // host that has never seen a JVM.
        TopologyDescriptionException thrown =
                assertThrows(TopologyDescriptionException.class, () -> assembler.count(source, "counts"));

        assertThat(thrown).hasMessageThat().contains("count");
        assertThat(thrown).hasMessageThat().contains("it names a stream");
        assertThat(thrown).hasMessageThat().contains("grouped stream");
        assertThat(thrown).hasMessageThat().doesNotContain("KStream");
    }

    @Test
    void eachMintRecordsItsKindAndItsKeyAndValueTypes() {
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, appending);

        long source = assembler.source("in");
        long mapped = assembler.mapValues(source, 42);
        long grouped = assembler.groupByKey(mapped);
        long counted = assembler.count(grouped, "counts");

        assertThat(assembler.typeOf(source)).isEqualTo(type(
                HandleKind.HANDLE_KIND_STREAM, DataType.DATA_TYPE_BYTES, DataType.DATA_TYPE_BYTES));
        assertThat(assembler.typeOf(mapped)).isEqualTo(type(
                HandleKind.HANDLE_KIND_STREAM, DataType.DATA_TYPE_BYTES, DataType.DATA_TYPE_BYTES));
        assertThat(assembler.typeOf(grouped)).isEqualTo(type(
                HandleKind.HANDLE_KIND_GROUPED_STREAM, DataType.DATA_TYPE_BYTES, DataType.DATA_TYPE_BYTES));
        // The one mint whose value the host never supplied - the whole reason types travel at all.
        assertThat(assembler.typeOf(counted)).isEqualTo(type(
                HandleKind.HANDLE_KIND_TABLE, DataType.DATA_TYPE_BYTES, DataType.DATA_TYPE_LONG));
    }

    /**
     * The refusal matrix: every method applied to every wrong kind of handle it can meet, each refused by the
     * RECORDED kind. One passing case per method is not enough - a resolver that reported every mismatch as
     * "grouped stream" would pass a single-case test.
     *
     * <p>The time-windowed rows are what pin the protocol string "time-windowed stream": before them no test
     * fed a time-windowed handle into any refusing method, so its recorded name was unenforced. Red-proofed,
     * two separate sabotages of {@code TopologyAssembler}: with {@code kindName}'s time-windowed branch changed
     * to return "time windowed stream" (hyphen dropped), the first time-windowed row failed with
     * {@code expected to contain: it names a time-windowed stream / but was: ... time windowed stream ...};
     * with {@code windowedAggregate}'s expected kind changed to {@code HANDLE_KIND_GROUPED_STREAM}, the
     * aggregate-on-stream row failed with {@code expected to contain: needs a time-windowed stream / but was:
     * ... aggregate needs a grouped stream} (and every aggregate happy-path test in the class errored). Both
     * sabotages restored.
     */
    @Test
    void everyMethodRefusesEveryWrongKindByItsRecordedName() {
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, appending);
        long stream = assembler.source("in");
        long grouped = assembler.groupByKey(stream);
        long table = assembler.count(grouped, "counts");
        long windowed = assembler.windowedBy(grouped, tumblingHour());

        assertRefusedNaming(() -> assembler.mapValues(grouped, 42), "mapValues", "grouped stream", "stream");
        assertRefusedNaming(() -> assembler.mapValues(table, 42), "mapValues", "table", "stream");
        assertRefusedNaming(() -> assembler.mapValues(windowed, 42), "mapValues", "time-windowed stream", "stream");
        assertRefusedNaming(() -> assembler.groupByKey(grouped), "groupByKey", "grouped stream", "stream");
        assertRefusedNaming(() -> assembler.groupByKey(table), "groupByKey", "table", "stream");
        assertRefusedNaming(() -> assembler.groupByKey(windowed), "groupByKey", "time-windowed stream", "stream");
        assertRefusedNaming(() -> assembler.count(stream, "s"), "count", "stream", "grouped stream");
        assertRefusedNaming(() -> assembler.count(table, "s"), "count", "table", "grouped stream");
        assertRefusedNaming(() -> assembler.count(windowed, "s"), "count", "time-windowed stream",
                "grouped stream");
        assertRefusedNaming(() -> assembler.reduce(windowed, 7, "s"), "reduce", "time-windowed stream",
                "grouped stream");
        assertRefusedNaming(() -> assembler.sink(grouped, "out"), "sink", "grouped stream", "stream or a table");
        assertRefusedNaming(() -> assembler.sink(windowed, "out"), "sink", "time-windowed stream",
                "stream or a table");
        // Both of join's positions, because each resolves by its own expected kind.
        assertRefusedNaming(() -> assembler.join(windowed, table, 7), "join", "time-windowed stream", "stream");
        assertRefusedNaming(() -> assembler.join(stream, windowed, 7), "join", "time-windowed stream", "table");
        assertRefusedNaming(() -> assembler.windowedBy(windowed, tumblingHour()), "windowed_by",
                "time-windowed stream", "grouped stream");
        // to_stream accepts windowed TABLES, not windowed streams - the near-miss its refusal must name.
        assertRefusedNaming(() -> assembler.toStream(windowed), "to_stream", "time-windowed stream",
                "windowed table");
        // aggregate is the one method whose CONTRACT is the time-windowed stream: everything else refuses it.
        assertRefusedNaming(() -> assembler.aggregate(stream, bytes("i"), 7, "s"), "aggregate", "stream",
                "time-windowed stream");
        assertRefusedNaming(() -> assembler.aggregate(grouped, bytes("i"), 7, "s"), "aggregate", "grouped stream",
                "time-windowed stream");
        assertRefusedNaming(() -> assembler.aggregate(table, bytes("i"), 7, "s"), "aggregate", "table",
                "time-windowed stream");
    }

    /**
     * Sinking a grouped stream is refused as what it IS. Before types were recorded, this case fell through the
     * instanceof chain and was misreported as a handle that "does not exist" - a lie about a handle the engine
     * itself minted.
     */
    @Test
    void sinkingAGroupedStreamIsRefusedByItsKindNotAsUnknown() {
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, appending);
        long grouped = assembler.groupByKey(assembler.source("in"));

        TopologyDescriptionException thrown =
                assertThrows(TopologyDescriptionException.class, () -> assembler.sink(grouped, "out"));

        assertThat(thrown).hasMessageThat().contains("grouped stream");
        assertThat(thrown).hasMessageThat().doesNotContain("does not exist");
    }

    /**
     * A type with no serde branch is refused by name, naming the handle and the axis - never silently written as
     * bytes. Driven through the selector directly because no current operator can mint an unspecified-typed
     * handle; the pipeline tests prove the sink writes through this same selector. Both axes, because the sink
     * selects a serde for each.
     */
    @Test
    void aTypeWithNoSerdeIsRefusedNamingTheHandleTheAxisAndTheType() {
        TopologyDescriptionException value = assertThrows(TopologyDescriptionException.class,
                () -> TopologyAssembler.serdeFor(DataType.DATA_TYPE_UNSPECIFIED, "value type", 7));
        TopologyDescriptionException key = assertThrows(TopologyDescriptionException.class,
                () -> TopologyAssembler.serdeFor(DataType.DATA_TYPE_UNSPECIFIED, "key type", 9));

        assertThat(value).hasMessageThat().contains("7");
        assertThat(value).hasMessageThat().contains("value type unspecified");
        assertThat(value).hasMessageThat().contains("no serde");
        assertThat(key).hasMessageThat().contains("9");
        assertThat(key).hasMessageThat().contains("key type unspecified");
    }

    /**
     * A mint that pairs a node with the wrong recorded kind is an engine bug, and it fails at the mint that made
     * it - not one call later as a ClassCastException naming a Kafka Streams implementation class to the host.
     * This validation is what makes the assembler's erased casts safe against future operators.
     */
    @Test
    void aMintPairingANodeWithTheWrongKindFailsAtTheMint() {
        var builder = new org.apache.kafka.streams.StreamsBuilder();
        var stream = builder.stream("in");

        IllegalArgumentException mismatched = assertThrows(IllegalArgumentException.class,
                () -> new TopologyAssembler.Minted(stream, type(
                        HandleKind.HANDLE_KIND_TABLE, DataType.DATA_TYPE_BYTES, DataType.DATA_TYPE_LONG)));
        IllegalArgumentException unspecified = assertThrows(IllegalArgumentException.class,
                () -> new TopologyAssembler.Minted(stream, type(
                        HandleKind.HANDLE_KIND_UNSPECIFIED, DataType.DATA_TYPE_BYTES, DataType.DATA_TYPE_BYTES)));

        assertThat(mismatched).hasMessageThat().contains("engine bug");
        assertThat(mismatched).hasMessageThat().contains("table");
        assertThat(unspecified).hasMessageThat().contains("known kind");
    }

    /**
     * A plain byte stream sinks as bytes. Paired with the count test below, this is what pins the serde selection
     * to the recorded type: a selector hardcoded to longs fails here, one hardcoded to bytes fails there.
     */
    @Test
    void aByteStreamSinksItsBytesUnchanged(@TempDir Path stateDir) {
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, appending);
        long source = assembler.source("in");
        assembler.sink(assembler.mapValues(source, 42), "out");

        try (TopologyTestDriver driver = new TopologyTestDriver(assembler.build(), config(stateDir))) {
            TestInputTopic<byte[], byte[]> in = driver.createInputTopic(
                    "in", new ByteArraySerializer(), new ByteArraySerializer());
            TestOutputTopic<byte[], byte[]> out = driver.createOutputTopic(
                    "out", new ByteArrayDeserializer(), new ByteArrayDeserializer());

            in.pipeInput(bytes("k"), bytes("payload"));

            var record = out.readKeyValue();
            assertThat(new String(record.key, StandardCharsets.UTF_8)).isEqualTo("k");
            assertThat(new String(record.value, StandardCharsets.UTF_8)).isEqualTo("payload");
        }
    }

    private static void assertRefusedNaming(
            org.junit.jupiter.api.function.Executable call, String method, String actualKind, String neededKind) {
        TopologyDescriptionException thrown = assertThrows(TopologyDescriptionException.class, call);
        assertThat(thrown).hasMessageThat().contains(method);
        assertThat(thrown).hasMessageThat().contains("it names a " + actualKind);
        // The full phrase, not a bare contains(neededKind): for the "stream" rows a bare contains is already
        // satisfied by the "grouped stream" in the actual-kind clause, which would let the needed-kind half of
        // the message regress unnoticed.
        assertThat(thrown).hasMessageThat().contains("needs a " + neededKind);
    }

    private static HandleType type(HandleKind kind, DataType keyType, DataType valueType) {
        return HandleType.newBuilder().setKind(kind).setKeyType(keyType).setValueType(valueType).build();
    }

    @Test
    void describingAfterTheTopologyIsBuiltIsRefused() {
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, appending);
        long source = assembler.source("in");
        assembler.sink(source, "out");
        assembler.build();

        TopologyDescriptionException thrown =
                assertThrows(TopologyDescriptionException.class, () -> assembler.source("late"));

        assertThat(thrown).hasMessageThat().ignoringCase().contains("once");
    }

    @Test
    void theDescribedTopologyCountsPerKeyAndTheSinkCarriesTheCounts(@TempDir Path stateDir) {
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, appending);
        long source = assembler.source("in");
        long mapped = assembler.mapValues(source, 42);
        long grouped = assembler.groupByKey(mapped);
        long counted = assembler.count(grouped, "counts");
        assembler.sink(counted, "out");
        Topology topology = assembler.build();

        Map<String, Long> lastPerKey = new LinkedHashMap<>();
        try (TopologyTestDriver driver = new TopologyTestDriver(topology, config(stateDir))) {
            TestInputTopic<byte[], byte[]> in = driver.createInputTopic(
                    "in", new ByteArraySerializer(), new ByteArraySerializer());
            TestOutputTopic<byte[], Long> out = driver.createOutputTopic(
                    "out", new ByteArrayDeserializer(), new LongDeserializer());

            in.pipeInput(bytes("a"), bytes("1"));
            in.pipeInput(bytes("b"), bytes("2"));
            in.pipeInput(bytes("a"), bytes("3"));

            // The sink of a count is a changelog: it carries every intermediate value per key, so the reader takes
            // the last value per key rather than summing what it sees. The demo reads it the same way.
            out.readKeyValuesToList().forEach(kv ->
                    lastPerKey.put(new String(kv.key, StandardCharsets.UTF_8), kv.value));
        }

        assertThat(lastPerKey).containsExactly("a", 2L, "b", 1L);
    }

    @Test
    void theAggregationUsesNoRocksDb(@TempDir Path stateDir) throws IOException {
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, appending);
        long grouped = assembler.groupByKey(assembler.source("in"));
        assembler.sink(assembler.count(grouped, "counts"), "out");

        List<String> stateFiles;
        try (TopologyTestDriver driver = new TopologyTestDriver(assembler.build(), config(stateDir))) {
            driver.createInputTopic("in", new ByteArraySerializer(), new ByteArraySerializer())
                    .pipeInput(bytes("a"), bytes("1"));

            // Inspected while the driver is OPEN. Closing it deletes the state directory, so the same walk after
            // the try block reports an empty tree for a persistent store too - a check that cannot fail, which is
            // what the first version of this test was.
            try (Stream<Path> tree = Files.walk(stateDir)) {
                stateFiles = tree.filter(Files::isRegularFile)
                        .map(path -> path.getFileName().toString())
                        .toList();
            }
        }

        // A persistent store writes RocksDB artifacts; an in-memory one writes none. This is the observable
        // difference, and the whole reason the store choice is a decision rather than a detail.
        assertThat(stateFiles.stream()
                .filter(name -> name.endsWith(".sst") || name.equals("CURRENT") || name.startsWith("MANIFEST-"))
                .toList()).isEmpty();
    }

    private static Properties config(Path stateDir) {
        Properties properties = new Properties();
        properties.putAll(new HashMap<>(Map.of(
                StreamsConfig.APPLICATION_ID_CONFIG, "assembler-test",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                StreamsConfig.STATE_DIR_CONFIG, stateDir.toString())));
        return properties;
    }

    @Test
    void reduceCombinesEachKeysValuesThroughTheForeignFunction(@TempDir Path stateDir) {
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, appending);
        long grouped = assembler.groupByKey(assembler.source("in"));
        assembler.sink(assembler.reduce(grouped, 7, "reduced"), "out");

        Map<String, String> lastPerKey = new LinkedHashMap<>();
        try (TopologyTestDriver driver = new TopologyTestDriver(assembler.build(), config(stateDir))) {
            TestInputTopic<byte[], byte[]> in = driver.createInputTopic(
                    "in", new ByteArraySerializer(), new ByteArraySerializer());
            // Bytes out, NOT longs. A reduction preserves the value type, which is the whole reason the recorded
            // handle type must distinguish it from count - a LongDeserializer here would fail outright.
            TestOutputTopic<byte[], byte[]> out = driver.createOutputTopic(
                    "out", new ByteArrayDeserializer(), new ByteArrayDeserializer());

            in.pipeInput(bytes("a"), bytes("x"));
            in.pipeInput(bytes("b"), bytes("p"));
            in.pipeInput(bytes("a"), bytes("y"));
            in.pipeInput(bytes("a"), bytes("z"));

            out.readKeyValuesToList().forEach(kv -> lastPerKey.put(
                    new String(kv.key, StandardCharsets.UTF_8), new String(kv.value, StandardCharsets.UTF_8)));
        }

        // "xyz" can only be produced by the STORED aggregate crossing to the reducer and back on the second and
        // third values for "a". A reducer that ignored its aggregate would leave "z"; one never called would leave
        // "x". The assertion distinguishes all three failures.
        assertThat(lastPerKey).containsExactly("a", "xyz", "b", "p");
    }

    @Test
    void theFirstValueForAKeyNeverReachesTheReducer(@TempDir Path stateDir) {
        // Kafka does not call a reducer for a key's first value - that value becomes the aggregate untouched. The
        // wire depends on this: it uses the aggregate's PRESENCE to mean "combine", so a first value that invoked
        // the reducer would have to invent an aggregate, and an empty one is a different and wrong thing to send.
        List<String> aggregatesSeen = new ArrayList<>();
        TopologyAssembler.ReducerFactory recording = token -> (aggregate, value) -> {
            aggregatesSeen.add(new String(aggregate, StandardCharsets.UTF_8));
            return value;
        };
        TopologyAssembler assembler = new TopologyAssembler(echo, recording, joining, appending);
        long grouped = assembler.groupByKey(assembler.source("in"));
        assembler.sink(assembler.reduce(grouped, 7, "reduced"), "out");

        try (TopologyTestDriver driver = new TopologyTestDriver(assembler.build(), config(stateDir))) {
            driver.createInputTopic("in", new ByteArraySerializer(), new ByteArraySerializer())
                    .pipeInput(bytes("a"), bytes("first"));
            driver.createInputTopic("in", new ByteArraySerializer(), new ByteArraySerializer())
                    .pipeInput(bytes("a"), bytes("second"));
        }

        assertThat(aggregatesSeen).containsExactly("first");
    }

    @Test
    void reduceMintsATableOfBytesWhereCountMintsLongs() {
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, appending);
        long grouped = assembler.groupByKey(assembler.source("in"));
        long counted = assembler.count(grouped, "counted");
        long reduced = assembler.reduce(grouped, 7, "reduced");

        // Both are tables and the difference is exactly the value type. Getting it wrong is silent: the sink would
        // pick a Long serde for reduced bytes and write plausible nonsense.
        assertThat(assembler.typeOf(counted).getValueType()).isEqualTo(DataType.DATA_TYPE_LONG);
        assertThat(assembler.typeOf(reduced).getValueType()).isEqualTo(DataType.DATA_TYPE_BYTES);
        assertThat(assembler.typeOf(reduced).getKind()).isEqualTo(HandleKind.HANDLE_KIND_TABLE);
    }

    @Test
    void aMapAndAReduceCoexistAndEachGetsItsOwnFunction(@TempDir Path stateDir) {
        // Two foreign operators of DIFFERENT kinds in one topology - the first time the engine holds more than
        // one, and the first time it must route two token shapes at once. Two mappers would not test this: the
        // interesting failure is a reducer being handed what a mapper expects, or one factory serving both.
        List<String> mapped = new ArrayList<>();
        List<String> combined = new ArrayList<>();
        TopologyAssembler.MapperFactory upper = token -> (key, value) -> {
            mapped.add(new String(value, StandardCharsets.UTF_8));
            return new String(value, StandardCharsets.UTF_8).toUpperCase(Locale.ROOT)
                    .getBytes(StandardCharsets.UTF_8);
        };
        TopologyAssembler.ReducerFactory join = token -> (aggregate, value) -> {
            combined.add(new String(aggregate, StandardCharsets.UTF_8) + "+"
                    + new String(value, StandardCharsets.UTF_8));
            byte[] joined = new byte[aggregate.length + value.length];
            System.arraycopy(aggregate, 0, joined, 0, aggregate.length);
            System.arraycopy(value, 0, joined, aggregate.length, value.length);
            return joined;
        };

        TopologyAssembler assembler = new TopologyAssembler(upper, join, joining, appending);
        long source = assembler.source("in");
        long transformed = assembler.mapValues(source, 1);
        long grouped = assembler.groupByKey(transformed);
        assembler.sink(assembler.reduce(grouped, 2, "reduced"), "out");

        Map<String, String> lastPerKey = new LinkedHashMap<>();
        try (TopologyTestDriver driver = new TopologyTestDriver(assembler.build(), config(stateDir))) {
            TestInputTopic<byte[], byte[]> in = driver.createInputTopic(
                    "in", new ByteArraySerializer(), new ByteArraySerializer());
            TestOutputTopic<byte[], byte[]> out = driver.createOutputTopic(
                    "out", new ByteArrayDeserializer(), new ByteArrayDeserializer());

            in.pipeInput(bytes("a"), bytes("x"));
            in.pipeInput(bytes("a"), bytes("y"));
            in.pipeInput(bytes("b"), bytes("z"));

            out.readKeyValuesToList().forEach(kv -> lastPerKey.put(
                    new String(kv.key, StandardCharsets.UTF_8), new String(kv.value, StandardCharsets.UTF_8)));
        }

        // Every record crossed for the map; only the second value for "a" crossed for the reduce. That the
        // reducer saw "X+Y" and not "x+y" proves the two ran in order rather than racing or bypassing.
        assertThat(mapped).containsExactly("x", "y", "z").inOrder();
        assertThat(combined).containsExactly("X+Y");
        assertThat(lastPerKey).containsExactly("a", "XY", "b", "Z");
    }

    /**
     * The first topology here that is not a straight line: two sources converge on one host function.
     *
     * <p>A join is the case that would let a transposed pair pass unnoticed - both sides are bytes, so swapping
     * them still compiles and still produces output. The separator in the joined value is what makes the order
     * observable, and the two sides carry deliberately different alphabets so a transposition cannot read as a pass.
     */
    @Test
    void aStreamJoinedToATableCallsTheHostWithTheStreamValueFirst(@TempDir Path stateDir) {
        List<String> pairs = new ArrayList<>();
        TopologyAssembler.JoinerFactory recording =
                token -> (streamValue, tableValue) -> {
                    pairs.add(new String(streamValue, StandardCharsets.UTF_8) + ">"
                            + new String(tableValue, StandardCharsets.UTF_8));
                    return bytes(new String(streamValue, StandardCharsets.UTF_8) + ">"
                            + new String(tableValue, StandardCharsets.UTF_8));
                };

        TopologyAssembler assembler = new TopologyAssembler(echo, concat, recording, appending);
        long facts = assembler.source("facts");
        long table = assembler.reduce(assembler.groupByKey(facts), 1, "facts-store");
        long events = assembler.source("events");
        assembler.sink(assembler.join(events, table, 2), "out");

        List<String> emitted = new ArrayList<>();
        try (TopologyTestDriver driver = new TopologyTestDriver(assembler.build(), config(stateDir))) {
            TestInputTopic<byte[], byte[]> factsIn = driver.createInputTopic(
                    "facts", new ByteArraySerializer(), new ByteArraySerializer());
            TestInputTopic<byte[], byte[]> eventsIn = driver.createInputTopic(
                    "events", new ByteArraySerializer(), new ByteArraySerializer());
            TestOutputTopic<byte[], byte[]> out = driver.createOutputTopic(
                    "out", new ByteArrayDeserializer(), new ByteArrayDeserializer());

            factsIn.pipeInput(bytes("k"), bytes("LEFT"));
            eventsIn.pipeInput(bytes("k"), bytes("one"));
            eventsIn.pipeInput(bytes("k"), bytes("two"));
            // No fact for this key, so the join finds no match and the host is never called for it.
            eventsIn.pipeInput(bytes("absent"), bytes("three"));

            out.readValuesToList().forEach(v -> emitted.add(new String(v, StandardCharsets.UTF_8)));
        }

        assertThat(pairs).containsExactly("one>LEFT", "two>LEFT").inOrder();
        assertThat(emitted).containsExactly("one>LEFT", "two>LEFT").inOrder();
    }

    @Test
    void aJoinAgainstACountsTableIsRefusedAtTheCallThatDescribedIt() {
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, appending);
        long facts = assembler.source("facts");
        long counts = assembler.count(assembler.groupByKey(facts), "counts-store");
        long events = assembler.source("events");

        // Longs on one side, a host joiner handed bytes on the other: erasure would let this reach a running
        // topology and fail one record in.
        TopologyDescriptionException refused = assertThrows(TopologyDescriptionException.class,
                () -> assembler.join(events, counts, 1));
        assertThat(refused).hasMessageThat().contains("long");
    }

    @Test
    void aJoinNamingAGroupedStreamAsItsTableIsRefused() {
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, appending);
        long events = assembler.source("events");
        long grouped = assembler.groupByKey(assembler.source("facts"));

        TopologyDescriptionException refused = assertThrows(TopologyDescriptionException.class,
                () -> assembler.join(events, grouped, 1));
        assertThat(refused).hasMessageThat().contains("needs a table");
    }

    // ---- the windowed aggregation: windowed_by, aggregate, to_stream ----

    private static final long ONE_HOUR_MS = 3_600_000L;
    private static final long TWO_HOURS_MS = 7_200_000L;

    /** A base timestamp well past the epoch, so no window under test brushes against time zero. */
    private static final Instant BASE = Instant.parse("2026-01-01T00:00:00Z");

    private static TimeWindowSpec window(long sizeMs, long advanceMs, long graceMs, long retentionMs) {
        return TimeWindowSpec.newBuilder()
                .setSizeMs(sizeMs).setAdvanceMs(advanceMs).setGraceMs(graceMs).setRetentionMs(retentionMs)
                .build();
    }

    /** A one-hour tumbling window: advance equals size, zero grace, retention comfortably above the minimum. */
    private static TimeWindowSpec tumblingHour() {
        return window(ONE_HOUR_MS, ONE_HOUR_MS, 0, TWO_HOURS_MS);
    }

    @Test
    void windowedByMintsATimeWindowedStreamWhoseRecordedTypeCarriesTheSpecification() {
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, appending);
        long grouped = assembler.groupByKey(assembler.source("in"));

        long windowed = assembler.windowedBy(grouped, tumblingHour());

        HandleType recorded = assembler.typeOf(windowed);
        assertThat(recorded.getKind()).isEqualTo(HandleKind.HANDLE_KIND_TIME_WINDOWED_STREAM);
        assertThat(recorded.getKeyType()).isEqualTo(DataType.DATA_TYPE_BYTES);
        assertThat(recorded.getValueType()).isEqualTo(DataType.DATA_TYPE_BYTES);
        assertThat(recorded.hasWindow()).isTrue();
        assertThat(recorded.getWindow()).isEqualTo(tumblingHour());
    }

    @Test
    void aggregateMintsAWindowedTableOfBytesCarryingTheWindow() {
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, appending);
        long windowed = assembler.windowedBy(assembler.groupByKey(assembler.source("in")), tumblingHour());

        long table = assembler.aggregate(windowed, bytes("init"), 7, "agg-store");

        HandleType recorded = assembler.typeOf(table);
        assertThat(recorded.getKind()).isEqualTo(HandleKind.HANDLE_KIND_TABLE);
        assertThat(recorded.getValueType()).isEqualTo(DataType.DATA_TYPE_BYTES);
        assertThat(recorded.hasWindow()).isTrue();
        assertThat(recorded.getWindow()).isEqualTo(tumblingHour());
    }

    @Test
    void windowedByOnAStreamOrATableIsRefusedInProtocolVocabulary() {
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, appending);
        long stream = assembler.source("in");
        long table = assembler.count(assembler.groupByKey(stream), "counts");

        assertRefusedNaming(() -> assembler.windowedBy(stream, tumblingHour()),
                "windowed_by", "stream", "grouped stream");
        assertRefusedNaming(() -> assembler.windowedBy(table, tumblingHour()),
                "windowed_by", "table", "grouped stream");
        // Protocol vocabulary only: never a Kafka Streams implementation class name, which means nothing to a
        // host that has never seen a JVM.
        TopologyDescriptionException refused = assertThrows(TopologyDescriptionException.class,
                () -> assembler.windowedBy(stream, tumblingHour()));
        assertThat(refused).hasMessageThat().doesNotContain("KStream");
        assertThat(refused).hasMessageThat().doesNotContain("KGroupedStream");
    }

    /**
     * R17: the refusal is on the windowed KEY, not on the table, and it names the sanctioned way out. Without the
     * to_stream pointer a host is told only what it cannot do, when one call would have unblocked it.
     */
    @Test
    void sinkingAWindowedTableIsRefusedNamingTheWindowedKeyAndToStreamAsTheWayOut() {
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, appending);
        long windowed = assembler.windowedBy(assembler.groupByKey(assembler.source("in")), tumblingHour());
        long table = assembler.aggregate(windowed, bytes("i"), 7, "agg-store");

        TopologyDescriptionException refused = assertThrows(TopologyDescriptionException.class,
                () -> assembler.sink(table, "out"));

        assertThat(refused).hasMessageThat().contains("handle " + table);
        assertThat(refused).hasMessageThat().contains("windowed table");
        assertThat(refused).hasMessageThat().contains("window");
        assertThat(refused).hasMessageThat().contains("to_stream");
    }

    /**
     * The behaviour {@code reduce} can never give (R15): Kafka skips a reducer for a key's first value, but an
     * aggregator is called for every record because the engine supplies the initializer's bytes itself. The window
     * store is read while the driver is OPEN - closing it deletes the state directory, which has produced a green
     * test asserting nothing in this module before.
     */
    @Test
    void theFirstValueForAKeyReachesTheHostAggregator(@TempDir Path stateDir) {
        List<String> crossings = new ArrayList<>();
        TopologyAssembler.AggregatorFactory recording = token -> (key, value, aggregate) -> {
            crossings.add(new String(aggregate, StandardCharsets.UTF_8) + "+"
                    + new String(value, StandardCharsets.UTF_8));
            byte[] joined = new byte[aggregate.length + value.length];
            System.arraycopy(aggregate, 0, joined, 0, aggregate.length);
            System.arraycopy(value, 0, joined, aggregate.length, value.length);
            return joined;
        };
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, recording);
        long windowed = assembler.windowedBy(assembler.groupByKey(assembler.source("in")), tumblingHour());
        assembler.aggregate(windowed, bytes("i"), 7, "agg-store");

        String stored;
        try (TopologyTestDriver driver = new TopologyTestDriver(assembler.build(), config(stateDir))) {
            TestInputTopic<byte[], byte[]> in = driver.createInputTopic(
                    "in", new ByteArraySerializer(), new ByteArraySerializer(), BASE, Duration.ZERO);
            in.pipeInput(bytes("a"), bytes("x"));
            in.pipeInput(bytes("a"), bytes("y"));
            in.pipeInput(bytes("a"), bytes("z"));

            WindowStore<byte[], byte[]> store = driver.getWindowStore("agg-store");
            try (WindowStoreIterator<byte[]> iterator = store.fetch(
                    bytes("a"), BASE.minusMillis(ONE_HOUR_MS), BASE.plusMillis(ONE_HOUR_MS))) {
                assertThat(iterator.hasNext()).isTrue();
                stored = new String(iterator.next().value, StandardCharsets.UTF_8);
                assertThat(iterator.hasNext()).isFalse();
            }
        }

        // "i+x" is the whole point: the FIRST value for the key crossed, with the initializer's bytes as its
        // accumulator - the record a reduce silently swallows.
        assertThat(crossings).containsExactly("i+x", "ix+y", "ixy+z").inOrder();
        assertThat(stored).isEqualTo("ixyz");
    }

    /**
     * KTD8: the initializer runs once per new window per key, and hands out a DEFENSIVE COPY each time. The host
     * aggregator here vandalises the accumulator array in place; with one shared array the second key's initial
     * accumulator would arrive as "###" instead of "id-". Red-proofed (R4): with the per-call copy in
     * {@code TopologyAssembler.aggregate} changed to hand out the captured array itself, this test fails with
     * {@code initials: ["id-", "###"]} - so it can see the defect it guards against.
     */
    @Test
    void eachKeyOpeningAWindowGetsItsOwnCopyOfTheInitializerBytes(@TempDir Path stateDir) {
        List<String> initials = new ArrayList<>();
        TopologyAssembler.AggregatorFactory vandalising = token -> (key, value, aggregate) -> {
            initials.add(new String(aggregate, StandardCharsets.UTF_8));
            Arrays.fill(aggregate, (byte) '#');
            return value;
        };
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, vandalising);
        long windowed = assembler.windowedBy(assembler.groupByKey(assembler.source("in")), tumblingHour());
        assembler.aggregate(windowed, bytes("id-"), 7, "agg-store");

        try (TopologyTestDriver driver = new TopologyTestDriver(assembler.build(), config(stateDir))) {
            TestInputTopic<byte[], byte[]> in = driver.createInputTopic(
                    "in", new ByteArraySerializer(), new ByteArraySerializer(), BASE, Duration.ZERO);
            in.pipeInput(bytes("a"), bytes("1"));
            in.pipeInput(bytes("b"), bytes("2"));
        }

        assertThat(initials).containsExactly("id-", "id-").inOrder();
    }

    /**
     * All four fields have an arm, and each asserts the DISTINCTIVE clause "missing &lt;field&gt;". A bare
     * {@code contains("advance_ms")} proved unable to detect a wrong field name: the refusal's enumeration tail
     * ("size_ms, advance_ms, grace_ms and retention_ms must all be present") contains every field name, so the
     * old assertion passed whichever field the message blamed. Red-proofed: with the {@code hasSizeMs} and
     * {@code hasAdvanceMs} labels swapped at their {@code requireWindowField} call sites, this test failed with
     * {@code expected to contain: missing size_ms / but was: ... missing advance_ms ...}; sabotage restored.
     */
    @Test
    void aWindowSpecificationMissingAFieldIsRefusedByName() {
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, appending);
        long grouped = assembler.groupByKey(assembler.source("in"));
        TimeWindowSpec noSize = TimeWindowSpec.newBuilder()
                .setAdvanceMs(ONE_HOUR_MS).setGraceMs(0).setRetentionMs(TWO_HOURS_MS).build();
        TimeWindowSpec noAdvance = TimeWindowSpec.newBuilder()
                .setSizeMs(ONE_HOUR_MS).setGraceMs(0).setRetentionMs(TWO_HOURS_MS).build();
        TimeWindowSpec noGrace = TimeWindowSpec.newBuilder()
                .setSizeMs(ONE_HOUR_MS).setAdvanceMs(ONE_HOUR_MS).setRetentionMs(TWO_HOURS_MS).build();
        TimeWindowSpec noRetention = TimeWindowSpec.newBuilder()
                .setSizeMs(ONE_HOUR_MS).setAdvanceMs(ONE_HOUR_MS).setGraceMs(0).build();

        TopologyDescriptionException missingSize = assertThrows(TopologyDescriptionException.class,
                () -> assembler.windowedBy(grouped, noSize));
        TopologyDescriptionException missingAdvance = assertThrows(TopologyDescriptionException.class,
                () -> assembler.windowedBy(grouped, noAdvance));
        TopologyDescriptionException missingGrace = assertThrows(TopologyDescriptionException.class,
                () -> assembler.windowedBy(grouped, noGrace));
        TopologyDescriptionException missingRetention = assertThrows(TopologyDescriptionException.class,
                () -> assembler.windowedBy(grouped, noRetention));

        // Refused by NAME, never defaulted: proto3's zero would silently turn a tumbling window into a point
        // (advance) or hand the store Kafka's own size+grace default (retention), each wrong in a different way.
        assertThat(missingSize).hasMessageThat().contains("missing size_ms");
        assertThat(missingAdvance).hasMessageThat().contains("missing advance_ms");
        assertThat(missingGrace).hasMessageThat().contains("missing grace_ms");
        assertThat(missingRetention).hasMessageThat().contains("missing retention_ms");
    }

    @Test
    void aRetentionBelowSizePlusGraceIsRefusedNamingTheMinimum() {
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, appending);
        long grouped = assembler.groupByKey(assembler.source("in"));
        long graceMs = 1_800_000L;
        TimeWindowSpec tooShort = window(ONE_HOUR_MS, ONE_HOUR_MS, graceMs, ONE_HOUR_MS);

        TopologyDescriptionException refused = assertThrows(TopologyDescriptionException.class,
                () -> assembler.windowedBy(grouped, tooShort));

        // The minimum is named (R20), rather than left to surface as Kafka's own exception from inside the
        // engine one build step later.
        assertThat(refused).hasMessageThat().contains(String.valueOf(ONE_HOUR_MS + graceMs));
        assertThat(refused).hasMessageThat().contains("size_ms + grace_ms");
    }

    /**
     * Present-but-invalid window values are refused by the engine's own named refusals, in protocol vocabulary -
     * never left to reach Kafka's window constructors, whose exceptions surface to the host as unnamed engine
     * failures quoting classes it has never heard of. Each arm names the offending field and its bound.
     *
     * <p>Red-proofed: with all three bound checks removed from {@code TopologyAssembler.windowedBy}, every arm
     * here failed with "Unexpected exception type ... expected TopologyDescriptionException but was
     * IllegalArgumentException" - Kafka's own exception leaking, which is exactly the defect these refusals
     * close. Sabotage restored.
     */
    @Test
    void aWindowOfZeroSizeIsRefusedNamingTheField() {
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, appending);
        long grouped = assembler.groupByKey(assembler.source("in"));
        TimeWindowSpec zeroSize = window(0, ONE_HOUR_MS, 0, TWO_HOURS_MS);

        TopologyDescriptionException refused = assertThrows(TopologyDescriptionException.class,
                () -> assembler.windowedBy(grouped, zeroSize));

        assertThat(refused).hasMessageThat().contains("size_ms 0");
        assertThat(refused).hasMessageThat().contains("minimum 1");
        assertThat(refused).hasMessageThat().doesNotContain("TimeWindows");
    }

    @Test
    void aWindowWhoseAdvanceIsZeroIsRefusedNamingTheField() {
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, appending);
        long grouped = assembler.groupByKey(assembler.source("in"));
        TimeWindowSpec zeroAdvance = window(ONE_HOUR_MS, 0, 0, TWO_HOURS_MS);

        TopologyDescriptionException refused = assertThrows(TopologyDescriptionException.class,
                () -> assembler.windowedBy(grouped, zeroAdvance));

        assertThat(refused).hasMessageThat().contains("advance_ms 0");
        assertThat(refused).hasMessageThat().contains("minimum 1");
        assertThat(refused).hasMessageThat().doesNotContain("TimeWindows");
    }

    @Test
    void aWindowWhoseAdvanceExceedsItsSizeIsRefusedNamingBothFields() {
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, appending);
        long grouped = assembler.groupByKey(assembler.source("in"));
        TimeWindowSpec gappy = window(ONE_HOUR_MS, TWO_HOURS_MS, 0, TWO_HOURS_MS);

        TopologyDescriptionException refused = assertThrows(TopologyDescriptionException.class,
                () -> assembler.windowedBy(grouped, gappy));

        assertThat(refused).hasMessageThat().contains("advance_ms " + TWO_HOURS_MS);
        assertThat(refused).hasMessageThat().contains("above size_ms " + ONE_HOUR_MS);
        assertThat(refused).hasMessageThat().doesNotContain("TimeWindows");
    }

    /** Reachable because the wire field is int64: nothing before this refusal rejects a negative. */
    @Test
    void aNegativeGraceIsRefusedNamingTheField() {
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, appending);
        long grouped = assembler.groupByKey(assembler.source("in"));
        TimeWindowSpec negativeGrace = window(ONE_HOUR_MS, ONE_HOUR_MS, -1, TWO_HOURS_MS);

        TopologyDescriptionException refused = assertThrows(TopologyDescriptionException.class,
                () -> assembler.windowedBy(grouped, negativeGrace));

        assertThat(refused).hasMessageThat().contains("grace_ms -1");
        assertThat(refused).hasMessageThat().contains("minimum 0");
        assertThat(refused).hasMessageThat().doesNotContain("TimeWindows");
    }

    /**
     * The chain every arm in U6 runs through, asserted here rather than discovered there (R29, KTD19): to_stream
     * re-keys the windowed table to its INNER key, so map_values and sink accept the result unchanged and no
     * window bytes ever reach the topic.
     */
    @Test
    void toStreamThenMapValuesThenSinkCarriesTheInnerKeyWithNoWindowBytes(@TempDir Path stateDir) {
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, appending);
        long windowed = assembler.windowedBy(assembler.groupByKey(assembler.source("in")), tumblingHour());
        long table = assembler.aggregate(windowed, bytes(""), 7, "agg-store");

        long restreamed = assembler.toStream(table);
        HandleType recorded = assembler.typeOf(restreamed);
        assertThat(recorded.getKind()).isEqualTo(HandleKind.HANDLE_KIND_STREAM);
        assertThat(recorded.getKeyType()).isEqualTo(DataType.DATA_TYPE_BYTES);
        assertThat(recorded.getValueType()).isEqualTo(DataType.DATA_TYPE_BYTES);
        // The window is DROPPED at the re-key, not carried: a windowed type here would send the next operator
        // hunting for window bytes that no longer exist.
        assertThat(recorded.hasWindow()).isFalse();

        assembler.sink(assembler.mapValues(restreamed, 42), "out");

        try (TopologyTestDriver driver = new TopologyTestDriver(assembler.build(), config(stateDir))) {
            TestInputTopic<byte[], byte[]> in = driver.createInputTopic(
                    "in", new ByteArraySerializer(), new ByteArraySerializer(), BASE, Duration.ZERO);
            TestOutputTopic<byte[], byte[]> out = driver.createOutputTopic(
                    "out", new ByteArrayDeserializer(), new ByteArrayDeserializer());

            in.pipeInput(bytes("k"), bytes("v"));

            var record = out.readKeyValue();
            // The INNER key, byte for byte: a windowed key would carry an 8-byte big-endian start suffix, so
            // length alone would betray it. Asserting the exact bytes covers both the value and the length.
            assertThat(record.key).isEqualTo(bytes("k"));
            assertThat(new String(record.value, StandardCharsets.UTF_8)).isEqualTo("v");
        }
    }

    @Test
    void toStreamOnANonWindowedHandleIsRefusedByName() {
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, appending);
        long stream = assembler.source("in");
        long grouped = assembler.groupByKey(stream);
        long plainTable = assembler.count(grouped, "counts");

        assertRefusedNaming(() -> assembler.toStream(stream), "to_stream", "stream", "windowed table");
        assertRefusedNaming(() -> assembler.toStream(grouped), "to_stream", "grouped stream", "windowed table");
        // A plain table is the near-miss worth naming separately: it IS a table, just not a windowed one, and
        // Kafka's own KTable.toStream would happily take it - the refusal is this wire's scope choice.
        assertRefusedNaming(() -> assembler.toStream(plainTable), "to_stream", "table", "windowed table");
    }

    // ---- U5, the P2 placement: a declared JVM-side combine on the same aggregate call ----

    /** An aggregate call naming a combine kind and nothing the host would run. */
    private static Aggregate combineCall(long handle, CombineKind kind, String storeName) {
        return Aggregate.newBuilder().setHandle(handle).setCombine(kind).setStoreName(storeName).build();
    }

    /**
     * U5 scenario 1: the combine placement mints a handle of exactly the shape the token placement mints - same
     * kind, same types, same window, and the same sink refusal - so nothing downstream of the mint can tell the
     * two placements apart. That sameness is what makes a comparison of them a comparison of the placement only.
     */
    @Test
    void aggregateWithACombineAndNoTokenMintsATableOfTheTokenPathsShape() {
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, appending);
        long windowed = assembler.windowedBy(assembler.groupByKey(assembler.source("in")), tumblingHour());

        long table = assembler.aggregate(
                combineCall(windowed, CombineKind.COMBINE_KIND_APPEND_BYTES, "agg-store"));

        HandleType recorded = assembler.typeOf(table);
        assertThat(recorded.getKind()).isEqualTo(HandleKind.HANDLE_KIND_TABLE);
        assertThat(recorded.getKeyType()).isEqualTo(DataType.DATA_TYPE_BYTES);
        assertThat(recorded.getValueType()).isEqualTo(DataType.DATA_TYPE_BYTES);
        assertThat(recorded.hasWindow()).isTrue();
        assertThat(recorded.getWindow()).isEqualTo(tumblingHour());
        // The sink refusal reads the recorded type, so the combine-minted table is refused exactly as the
        // token-minted one is, naming to_stream as the way out.
        TopologyDescriptionException refused = assertThrows(TopologyDescriptionException.class,
                () -> assembler.sink(table, "out"));
        assertThat(refused).hasMessageThat().contains("windowed table");
        assertThat(refused).hasMessageThat().contains("to_stream");
    }

    /**
     * U5 scenario 2: both set is refused by name, saying they are alternatives. Precedence would silently
     * discard half of what the host said - either the function it registered or the combine it named.
     */
    @Test
    void anAggregateCarryingBothACombineAndAFunctionTokenIsRefusedNamingThemAsAlternatives() {
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, appending);
        long windowed = assembler.windowedBy(assembler.groupByKey(assembler.source("in")), tumblingHour());
        Aggregate both = Aggregate.newBuilder().setHandle(windowed).setFunctionToken(7)
                .setCombine(CombineKind.COMBINE_KIND_APPEND_BYTES).setStoreName("agg-store").build();

        TopologyDescriptionException refused = assertThrows(TopologyDescriptionException.class,
                () -> assembler.aggregate(both));

        assertThat(refused).hasMessageThat().contains("function_token");
        assertThat(refused).hasMessageThat().contains("combine");
        assertThat(refused).hasMessageThat().contains("alternatives");
    }

    /** U5 scenario 3: neither set is refused by name - an aggregation with no combining step is not one. */
    @Test
    void anAggregateCarryingNeitherACombineNorAFunctionTokenIsRefusedByName() {
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, appending);
        long windowed = assembler.windowedBy(assembler.groupByKey(assembler.source("in")), tumblingHour());
        Aggregate neither = Aggregate.newBuilder().setHandle(windowed).setStoreName("agg-store").build();

        TopologyDescriptionException refused = assertThrows(TopologyDescriptionException.class,
                () -> assembler.aggregate(neither));

        assertThat(refused).hasMessageThat().contains("neither");
        assertThat(refused).hasMessageThat().contains("function_token");
        assertThat(refused).hasMessageThat().contains("combine");
    }

    /**
     * A combine kind defines its own empty accumulator, so initial bytes beside a combine have nowhere to go.
     * Refused rather than dropped: a host that supplied a seed and got a topology that silently ignored it
     * would read its own aggregates as wrong for a reason it could not see.
     */
    @Test
    void anAggregateCarryingInitialAlongsideACombineIsRefusedRatherThanDropped() {
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, appending);
        long windowed = assembler.windowedBy(assembler.groupByKey(assembler.source("in")), tumblingHour());
        Aggregate seeded = combineCall(windowed, CombineKind.COMBINE_KIND_APPEND_BYTES, "agg-store")
                .toBuilder().setInitial(com.google.protobuf.ByteString.copyFromUtf8("seed")).build();

        TopologyDescriptionException refused = assertThrows(TopologyDescriptionException.class,
                () -> assembler.aggregate(seeded));

        assertThat(refused).hasMessageThat().contains("initial");
        assertThat(refused).hasMessageThat().contains("combine");
    }

    /** Proto3's zero member selects nothing; defaulting it to a real kind would choose an accumulator shape
     * the host never asked for, so it is refused naming the two real kinds. */
    @Test
    void anUnspecifiedCombineKindIsRefusedNamingTheRealKinds() {
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, appending);
        long windowed = assembler.windowedBy(assembler.groupByKey(assembler.source("in")), tumblingHour());

        TopologyDescriptionException refused = assertThrows(TopologyDescriptionException.class,
                () -> assembler.aggregate(
                        combineCall(windowed, CombineKind.COMBINE_KIND_UNSPECIFIED, "agg-store")));

        assertThat(refused).hasMessageThat().contains("COMBINE_KIND_APPEND_BYTES");
        assertThat(refused).hasMessageThat().contains("COMBINE_KIND_LAST_BYTES");
    }

    /**
     * U5 scenario 6: the appended accumulator is length-prefixed and splits back into the original values in
     * arrival order, so a host fold over the collection never guesses at boundaries. Distinct value lengths on
     * purpose - equal lengths would let a wrong prefix or a transposed order still parse.
     *
     * <p>Red-proofed (R4): with {@code appendLengthPrefixed} sabotaged to write the value's length AFTER its
     * bytes, this test failed with {@code BufferUnderflowException} inside the split - so the assertion does
     * walk the declared boundaries, and a mis-framed store value cannot pass. Sabotage removed after the red run.
     */
    @Test
    void anAppendedWindowStoresLengthPrefixedValuesThatSplitBackInArrivalOrder(@TempDir Path stateDir) {
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, appending);
        long windowed = assembler.windowedBy(assembler.groupByKey(assembler.source("in")), tumblingHour());
        assembler.aggregate(combineCall(windowed, CombineKind.COMBINE_KIND_APPEND_BYTES, "agg-store"));

        byte[] stored;
        try (TopologyTestDriver driver = new TopologyTestDriver(assembler.build(), config(stateDir))) {
            TestInputTopic<byte[], byte[]> in = driver.createInputTopic(
                    "in", new ByteArraySerializer(), new ByteArraySerializer(), BASE, Duration.ZERO);
            in.pipeInput(bytes("a"), bytes("x"));
            in.pipeInput(bytes("a"), bytes("yy"));
            in.pipeInput(bytes("a"), bytes("zzz"));

            WindowStore<byte[], byte[]> store = driver.getWindowStore("agg-store");
            try (WindowStoreIterator<byte[]> iterator = store.fetch(
                    bytes("a"), BASE.minusMillis(ONE_HOUR_MS), BASE.plusMillis(ONE_HOUR_MS))) {
                assertThat(iterator.hasNext()).isTrue();
                stored = iterator.next().value;
                assertThat(iterator.hasNext()).isFalse();
            }
        }

        assertThat(splitLengthPrefixed(stored)).containsExactly("x", "yy", "zzz").inOrder();
    }

    /**
     * U5 scenario 8: {@code LAST_BYTES} crosses zero times and keeps a BOUNDED accumulator - after three records
     * the stored value is one value's size, not three. That bound is what qualifies it as the placement
     * comparison's control arm (KTD16): against a host-fold arm it changes exactly one term, the crossing,
     * where an appending accumulator would also change the store, changelog and emit volumes.
     */
    @Test
    void aLastBytesCombineCrossesZeroTimesAndItsAccumulatorDoesNotGrowWithRecords(@TempDir Path stateDir) {
        AtomicInteger crossings = new AtomicInteger();
        TopologyAssembler.AggregatorFactory counting = token -> (key, value, aggregate) -> {
            crossings.incrementAndGet();
            return aggregate;
        };
        TopologyAssembler assembler = new TopologyAssembler(echo, concat, joining, counting);
        long windowed = assembler.windowedBy(assembler.groupByKey(assembler.source("in")), tumblingHour());
        assembler.aggregate(combineCall(windowed, CombineKind.COMBINE_KIND_LAST_BYTES, "agg-store"));

        byte[] stored;
        try (TopologyTestDriver driver = new TopologyTestDriver(assembler.build(), config(stateDir))) {
            TestInputTopic<byte[], byte[]> in = driver.createInputTopic(
                    "in", new ByteArraySerializer(), new ByteArraySerializer(), BASE, Duration.ZERO);
            in.pipeInput(bytes("a"), bytes("v1"));
            in.pipeInput(bytes("a"), bytes("v2"));
            in.pipeInput(bytes("a"), bytes("v3"));

            WindowStore<byte[], byte[]> store = driver.getWindowStore("agg-store");
            try (WindowStoreIterator<byte[]> iterator = store.fetch(
                    bytes("a"), BASE.minusMillis(ONE_HOUR_MS), BASE.plusMillis(ONE_HOUR_MS))) {
                assertThat(iterator.hasNext()).isTrue();
                stored = iterator.next().value;
                assertThat(iterator.hasNext()).isFalse();
            }
        }

        // The newest value only, at one value's size after three same-sized records: the accumulator is bounded.
        assertThat(new String(stored, StandardCharsets.UTF_8)).isEqualTo("v3");
        assertThat(stored.length).isEqualTo(bytes("v1").length);
        // Zero crossings: the counting factory's aggregator was never called - the engine ran its own.
        assertThat(crossings.get()).isEqualTo(0);
    }

    /** Splits a length-prefixed collection on its declared boundaries: four big-endian length bytes, then the
     * value, repeated. The reader-side half of the engine's {@code COMBINE_KIND_APPEND_BYTES} format. */
    private static List<String> splitLengthPrefixed(byte[] appended) {
        ByteBuffer buffer = ByteBuffer.wrap(appended);
        List<String> values = new ArrayList<>();
        while (buffer.hasRemaining()) {
            byte[] value = new byte[buffer.getInt()];
            buffer.get(value);
            values.add(new String(value, StandardCharsets.UTF_8));
        }
        return values;
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
