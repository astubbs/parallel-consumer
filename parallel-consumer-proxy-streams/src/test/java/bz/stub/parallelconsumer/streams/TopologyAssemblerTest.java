package bz.stub.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

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
    void aCallAppliedToTheWrongKindOfHandleIsRefusedAndTheErrorSaysWhy() {
        TopologyAssembler assembler = new TopologyAssembler(echo);
        long source = assembler.source("in");

        // count needs a grouped stream; a source is not one.
        TopologyDescriptionException thrown =
                assertThrows(TopologyDescriptionException.class, () -> assembler.count(source, "counts"));

        assertThat(thrown).hasMessageThat().contains("count");
        assertThat(thrown).hasMessageThat().contains("KGroupedStream");
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
