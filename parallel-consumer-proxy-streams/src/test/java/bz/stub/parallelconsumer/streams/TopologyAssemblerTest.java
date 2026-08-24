package bz.stub.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.streams.protocol.v1alpha1.DataType;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.HandleKind;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.HandleType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
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

    @Test
    void eachCallReturnsAHandleTheNextCallCanName() {
        TopologyAssembler assembler = new TopologyAssembler(echo);

        long source = assembler.source("in");
        long mapped = assembler.mapValues(source, 42);
        long grouped = assembler.groupByKey(mapped);
        long counted = assembler.count(grouped, "counts");

        assertThat(Stream.of(source, mapped, grouped, counted).distinct().count()).isEqualTo(4);
        assertThat(source).isGreaterThan(0L);
    }

    @Test
    void aCallNamingAnUnknownHandleIsRefusedAndTheErrorNamesIt() {
        TopologyAssembler assembler = new TopologyAssembler(echo);
        assembler.source("in");

        TopologyDescriptionException thrown =
                assertThrows(TopologyDescriptionException.class, () -> assembler.groupByKey(9999));

        assertThat(thrown).hasMessageThat().contains("9999");
        assertThat(thrown).hasMessageThat().contains("groupByKey");
    }

    @Test
    void aCallAppliedToTheWrongKindOfHandleIsRefusedInProtocolVocabulary() {
        TopologyAssembler assembler = new TopologyAssembler(echo);
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
        TopologyAssembler assembler = new TopologyAssembler(echo);

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
     */
    @Test
    void everyMethodRefusesEveryWrongKindByItsRecordedName() {
        TopologyAssembler assembler = new TopologyAssembler(echo);
        long stream = assembler.source("in");
        long grouped = assembler.groupByKey(stream);
        long table = assembler.count(grouped, "counts");

        assertRefusedNaming(() -> assembler.mapValues(grouped, 42), "mapValues", "grouped stream", "stream");
        assertRefusedNaming(() -> assembler.mapValues(table, 42), "mapValues", "table", "stream");
        assertRefusedNaming(() -> assembler.groupByKey(grouped), "groupByKey", "grouped stream", "stream");
        assertRefusedNaming(() -> assembler.groupByKey(table), "groupByKey", "table", "stream");
        assertRefusedNaming(() -> assembler.count(stream, "s"), "count", "stream", "grouped stream");
        assertRefusedNaming(() -> assembler.count(table, "s"), "count", "table", "grouped stream");
        assertRefusedNaming(() -> assembler.sink(grouped, "out"), "sink", "grouped stream", "stream or a table");
    }

    /**
     * Sinking a grouped stream is refused as what it IS. Before types were recorded, this case fell through the
     * instanceof chain and was misreported as a handle that "does not exist" - a lie about a handle the engine
     * itself minted.
     */
    @Test
    void sinkingAGroupedStreamIsRefusedByItsKindNotAsUnknown() {
        TopologyAssembler assembler = new TopologyAssembler(echo);
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
        TopologyAssembler assembler = new TopologyAssembler(echo);
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
        TopologyAssembler assembler = new TopologyAssembler(echo);
        long source = assembler.source("in");
        assembler.sink(source, "out");
        assembler.build();

        TopologyDescriptionException thrown =
                assertThrows(TopologyDescriptionException.class, () -> assembler.source("late"));

        assertThat(thrown).hasMessageThat().ignoringCase().contains("once");
    }

    @Test
    void theDescribedTopologyCountsPerKeyAndTheSinkCarriesTheCounts(@TempDir Path stateDir) {
        TopologyAssembler assembler = new TopologyAssembler(echo);
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
        TopologyAssembler assembler = new TopologyAssembler(echo);
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

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
