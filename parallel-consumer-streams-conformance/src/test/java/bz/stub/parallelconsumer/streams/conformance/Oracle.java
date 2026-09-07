package bz.stub.parallelconsumer.streams.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.test.TestRecord;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.TreeMap;

/**
 * The live oracle (R5): it turns a loaded {@link ConformanceCase} into the {@link FinalState} plain Apache Kafka
 * Streams computes for it, by translating the case's operation list into a {@link StreamsBuilder} topology, piping
 * the case's records through a {@link TopologyTestDriver}, and snapshotting every observable <em>inside</em> the
 * driver's open scope.
 * <p>
 * Nothing here is hand-written and nothing is committed: the expected outcome of a case is whatever Kafka Streams
 * does with it on this run. The accepted price is that a {@code kafka-streams} bump changing Kafka's own behaviour
 * is invisible, because both sides of a later comparison move together.
 *
 * <h2>The snapshot is inside the scope, and that is the whole design (KTD3)</h2>
 *
 * {@link TopologyTestDriver#close()} suspends the task and cleans the state directory, so a store read afterwards
 * observes nothing - and a test asserting over nothing is green. Every read in {@link #snapshot} therefore happens
 * before the try-with-resources closes, and {@code OracleTest} carries the negative control that pins what a
 * post-close read actually does.
 *
 * <h2>Byte arrays everywhere, rendered before anything is keyed or sorted</h2>
 *
 * Serdes are {@link Serdes#ByteArray()} throughout, so the oracle asserts nothing about serialisation - except for
 * {@code count}, whose result Kafka defines as a {@code Long}, and whose store and sink therefore carry
 * {@link Serdes#Long()}. Every key and value is turned into its rendered string by {@link #render} <em>before</em>
 * any map or sort is built, because byte arrays compare by identity: two sink records under one logical key would
 * otherwise stay two entries and the determinism proof would red for a Java reason (KTD3). {@link FinalState}'s
 * javadoc owns the rendering format, which is part of the contract a later driver has to meet.
 *
 * <h2>Caching is off, deliberately</h2>
 *
 * {@link StreamsConfig#STATESTORE_CACHE_MAX_BYTES_CONFIG} is {@code 0}. With the record cache on, a KTable's
 * downstream emissions are deduplicated until a flush, so what reaches a sink depends on when the driver happens to
 * commit rather than on the case - and the sink observable would stop being a function of the case's inputs. With it
 * off, every update reaches the sink, which is what makes a sink's fold deterministic. The one emission rule that is
 * <em>not</em> cache-driven is close-driven suppression, which is why {@code emit: on-window-close} is the one
 * attribute that changes what a sink sees (KTD5).
 *
 * <h2>Every record names its own topic</h2>
 *
 * {@link ConformanceCase.InputRecord#topic()} is always resolved by the time a case reaches here - the loader
 * defaults it to the single source's topic when a topology declares exactly one, and refuses a record that names
 * none when it declares any other number. So the oracle pipes each record to exactly the topic it names, in list
 * order, at its absolute timestamp, and nothing about which side of a join a record feeds is decided here.
 *
 * <h2>One thing the case format leaves open, decided here</h2>
 *
 * <b>{@code to-stream} on a windowed table drops the window</b> - the key becomes the inner key, which is what R3
 * already assumes when it says last-per-key would keep one record of many. The window survives in the store
 * observable, where its bounds are part of every entry's rendering.
 */
public final class Oracle {

    /**
     * Fixed, so nothing about a run depends on which case is running. The state directory is a fresh temporary one
     * per run, which is what actually isolates two runs from each other.
     */
    private static final String APPLICATION_ID = "pc-streams-conformance-oracle";

    /**
     * The one byte {@code concat-sides} puts between the stream-side and table-side values (KTD13). A separator at
     * all is what makes a transposed binding produce a different outcome rather than an ambiguous one.
     */
    private static final byte JOIN_SEPARATOR = (byte) '|';

    private Oracle() {
    }

    /**
     * Computes the case's final state from its declared inputs.
     *
     * @throws OracleExecutionException if the topology cannot be built, a record cannot be piped, or a store or sink
     *                                  cannot be snapshotted - always naming the case (KTD10)
     */
    public static FinalState run(ConformanceCase conformanceCase) {
        return execute(conformanceCase, conformanceCase.inputs(), "inputs");
    }

    /**
     * Computes the case's final state from its author-chosen perturbed twin, through the identical path (R8).
     * <p>
     * It exists as its own entry point rather than as a flag because the positive control's whole claim is that the
     * pipeline from <em>execution</em> to comparison is sensitive to its input - perturbing a copy of an outcome
     * already computed would prove only that the comparison works.
     */
    public static FinalState runPerturbation(ConformanceCase conformanceCase) {
        return execute(conformanceCase, conformanceCase.perturbation(), "perturbation");
    }

    private static FinalState execute(ConformanceCase conformanceCase,
                                      List<ConformanceCase.InputRecord> records,
                                      String which) {
        if (conformanceCase.refusalClass()) {
            throw new OracleExecutionException(conformanceCase.name(), "is a refusal-class case, which declares the "
                    + "fault the wire must raise for an invalid specification and is never executed - plain Kafka "
                    + "Streams never refuses what the wire invented, so there is no oracle row to compute (R15)",
                    null);
        }

        Path stateDirectory = null;
        try {
            Translation translation = translate(conformanceCase);
            stateDirectory = Files.createTempDirectory("pc-streams-conformance-oracle-");
            try (TopologyTestDriver driver =
                         new TopologyTestDriver(translation.topology, configuration(stateDirectory))) {
                pipe(conformanceCase, driver, translation, records);
                // Inside the scope, always: close() cleans the state directory, and a post-close read observes
                // nothing at all while every assertion over it still passes (KTD3).
                return snapshot(driver, translation);
            }
        } catch (OracleExecutionException e) {
            throw e;
        } catch (RuntimeException | IOException e) {
            throw new OracleExecutionException(conformanceCase.name(),
                    "threw while executing its " + which + ": " + e, e);
        } finally {
            deleteRecursively(stateDirectory);
        }
    }

    // ------------------------------------------------------------------------------- the topology translation

    /** What one translation produced: the topology, plus what the snapshot has to go looking for. */
    private static final class Translation {

        private final Topology topology;

        private final String description;

        /** Source topics, in declaration order - what a record's own topic is resolved against. */
        private final List<String> sourceTopics = new ArrayList<>();

        /** Store name to whether it is a window store, in declaration order. */
        private final Map<String, Boolean> stores = new LinkedHashMap<>();

        /** Sink topic to its spec, in declaration order. */
        private final Map<String, Sink> sinks = new LinkedHashMap<>();

        Translation(Topology topology, String description) {
            this.topology = topology;
            this.description = description;
        }
    }

    /** A sink's value serde (a count's is {@code Long}) and whether the handle feeding it came from a window. */
    private static final class Sink {

        private final Serde<?> valueSerde;

        private final boolean windowedFed;

        Sink(Serde<?> valueSerde, boolean windowedFed) {
            this.valueSerde = valueSerde;
            this.windowedFed = windowedFed;
        }
    }

    private enum HandleKind {
        STREAM, GROUPED, WINDOWED, TABLE, WINDOWED_TABLE
    }

    /** Kafka fixes {@code count}'s result type, so not every handle carries byte arrays. */
    private enum ValueType {
        BYTES, LONG;

        Serde<?> serde() {
            return this == LONG ? Serdes.Long() : Serdes.ByteArray();
        }
    }

    /** One entry in the handle table: what a case's id resolves to while the topology is being built. */
    private static final class Handle {

        private final HandleKind kind;

        private final Object node;

        private final ValueType valueType;

        /** Whether this handle's lineage passes through a windowed aggregation - what R3 keys the sink fold off. */
        private final boolean windowDerived;

        @Nullable
        private final ConformanceCase.WindowSpec window;

        Handle(HandleKind kind,
               Object node,
               ValueType valueType,
               boolean windowDerived,
               @Nullable ConformanceCase.WindowSpec window) {
            this.kind = kind;
            this.node = node;
            this.valueType = valueType;
            this.windowDerived = windowDerived;
            this.window = window;
        }
    }

    @SuppressWarnings("unchecked")
    private static Translation translate(ConformanceCase conformanceCase) {
        StreamsBuilder builder = new StreamsBuilder();
        Map<String, Handle> handles = new LinkedHashMap<>();
        List<String> sourceTopics = new ArrayList<>();
        Map<String, Boolean> stores = new LinkedHashMap<>();
        Map<String, Sink> sinks = new LinkedHashMap<>();
        boolean suppressUntilWindowCloses = conformanceCase.emit() == ConformanceCase.EmitRule.ON_WINDOW_CLOSE;

        for (ConformanceCase.Operation operation : conformanceCase.topology()) {
            switch (operation.kind()) {
                case SOURCE: {
                    String topic = required(conformanceCase, operation, operation.topic(), "topic");
                    sourceTopics.add(topic);
                    put(conformanceCase, handles, operation, new Handle(HandleKind.STREAM,
                            builder.stream(topic, Consumed.with(Serdes.ByteArray(), Serdes.ByteArray())),
                            ValueType.BYTES, false, null));
                    break;
                }
                case MAP_VALUES: {
                    Handle input = resolveOnly(conformanceCase, handles, operation);
                    String function = required(conformanceCase, operation, operation.function(), "fn");
                    requireBytes(conformanceCase, operation, input);
                    Object mapped;
                    if (input.kind == HandleKind.STREAM) {
                        mapped = ((KStream<byte[], byte[]>) input.node)
                                .mapValues(value -> mapValues(function, value));
                    } else if (input.kind == HandleKind.TABLE) {
                        mapped = ((KTable<byte[], byte[]>) input.node)
                                .mapValues(value -> mapValues(function, value));
                    } else {
                        throw wrongKind(conformanceCase, operation, input, "a stream or a table");
                    }
                    put(conformanceCase, handles, operation,
                            new Handle(input.kind, mapped, ValueType.BYTES, input.windowDerived, null));
                    break;
                }
                case GROUP_BY_KEY: {
                    Handle input = resolveOnly(conformanceCase, handles, operation);
                    if (input.kind != HandleKind.STREAM) {
                        throw wrongKind(conformanceCase, operation, input, "a stream");
                    }
                    requireBytes(conformanceCase, operation, input);
                    put(conformanceCase, handles, operation, new Handle(HandleKind.GROUPED,
                            ((KStream<byte[], byte[]>) input.node)
                                    .groupByKey(Grouped.with(Serdes.ByteArray(), Serdes.ByteArray())),
                            ValueType.BYTES, input.windowDerived, null));
                    break;
                }
                case WINDOWED_BY: {
                    Handle input = resolveOnly(conformanceCase, handles, operation);
                    if (input.kind != HandleKind.GROUPED) {
                        throw wrongKind(conformanceCase, operation, input, "a grouped stream");
                    }
                    ConformanceCase.WindowSpec window = Objects.requireNonNull(operation.window());
                    put(conformanceCase, handles, operation, new Handle(HandleKind.WINDOWED,
                            ((KGroupedStream<byte[], byte[]>) input.node).windowedBy(timeWindows(window)),
                            ValueType.BYTES, true, window));
                    break;
                }
                case COUNT:
                case REDUCE:
                case AGGREGATE: {
                    Handle input = resolveOnly(conformanceCase, handles, operation);
                    String store = required(conformanceCase, operation, operation.store(), "store");
                    if (operation.combine() != null) {
                        throw new OracleExecutionException(conformanceCase.name(), operation + " names the engine "
                                + "combine " + operation.combine() + "; a combine is something the wire invented and "
                                + "plain Kafka Streams has no opinion about, so no oracle row exists for it (R15)",
                                null);
                    }
                    Handle aggregated = aggregate(conformanceCase, operation, input, store,
                            suppressUntilWindowCloses);
                    Boolean previous = stores.put(store, aggregated.kind == HandleKind.WINDOWED_TABLE);
                    if (previous != null) {
                        throw new OracleExecutionException(conformanceCase.name(), "declares two stores named "
                                + store + "; a store name is how an observable is named in a red (R11)", null);
                    }
                    put(conformanceCase, handles, operation, aggregated);
                    break;
                }
                case TO_STREAM: {
                    Handle input = resolveOnly(conformanceCase, handles, operation);
                    if (input.kind == HandleKind.TABLE) {
                        put(conformanceCase, handles, operation, new Handle(HandleKind.STREAM,
                                ((KTable<byte[], Object>) input.node).toStream(),
                                input.valueType, input.windowDerived, null));
                    } else if (input.kind == HandleKind.WINDOWED_TABLE) {
                        // The window is dropped here, exactly as R3 assumes; it survives in the store observable.
                        put(conformanceCase, handles, operation, new Handle(HandleKind.STREAM,
                                ((KTable<Windowed<byte[]>, Object>) input.node)
                                        .toStream((key, ignoredValue) -> key.key()),
                                input.valueType, true, null));
                    } else {
                        throw wrongKind(conformanceCase, operation, input, "a table");
                    }
                    break;
                }
                case JOIN: {
                    Handle streamSide = resolve(conformanceCase, handles, operation,
                            Objects.requireNonNull(operation.streamInput()));
                    Handle tableSide = resolve(conformanceCase, handles, operation,
                            Objects.requireNonNull(operation.tableInput()));
                    if (streamSide.kind != HandleKind.STREAM) {
                        throw wrongKind(conformanceCase, operation, streamSide, "a stream on its stream side");
                    }
                    if (tableSide.kind != HandleKind.TABLE) {
                        throw wrongKind(conformanceCase, operation, tableSide, "a table on its table side");
                    }
                    requireBytes(conformanceCase, operation, streamSide);
                    requireBytes(conformanceCase, operation, tableSide);
                    String function = required(conformanceCase, operation, operation.function(), "fn");
                    put(conformanceCase, handles, operation, new Handle(HandleKind.STREAM,
                            ((KStream<byte[], byte[]>) streamSide.node).join(
                                    (KTable<byte[], byte[]>) tableSide.node,
                                    (streamValue, tableValue) -> join(function, streamValue, tableValue),
                                    Joined.with(Serdes.ByteArray(), Serdes.ByteArray(), Serdes.ByteArray())),
                            ValueType.BYTES, streamSide.windowDerived || tableSide.windowDerived, null));
                    break;
                }
                case SINK: {
                    Handle input = resolveOnly(conformanceCase, handles, operation);
                    if (input.kind != HandleKind.STREAM) {
                        throw wrongKind(conformanceCase, operation, input, "a stream");
                    }
                    String topic = required(conformanceCase, operation, operation.topic(), "topic");
                    Serde<?> valueSerde = input.valueType.serde();
                    ((KStream<byte[], Object>) input.node)
                            .to(topic, Produced.with(Serdes.ByteArray(), castSerde(valueSerde)));
                    Sink previous = sinks.put(topic, new Sink(valueSerde, input.windowDerived));
                    if (previous != null) {
                        throw new OracleExecutionException(conformanceCase.name(), "writes two sinks to topic "
                                + topic + "; a sink topic is how an observable is named in a red (R11)", null);
                    }
                    break;
                }
                default:
                    throw new OracleExecutionException(conformanceCase.name(),
                            "names operation " + operation.kind() + ", which the oracle does not translate", null);
            }
        }

        Topology topology = builder.build();
        Translation translation = new Translation(topology, topology.describe().toString());
        translation.sourceTopics.addAll(sourceTopics);
        translation.stores.putAll(stores);
        translation.sinks.putAll(sinks);
        return translation;
    }

    @SuppressWarnings("unchecked")
    private static Handle aggregate(ConformanceCase conformanceCase,
                                    ConformanceCase.Operation operation,
                                    Handle input,
                                    String store,
                                    boolean suppressUntilWindowCloses) {
        boolean windowed = input.kind == HandleKind.WINDOWED;
        if (!windowed && input.kind != HandleKind.GROUPED) {
            throw wrongKind(conformanceCase, operation, input, "a grouped stream or a windowed grouped stream");
        }

        if (windowed) {
            ConformanceCase.WindowSpec window = Objects.requireNonNull(input.window);
            TimeWindowedKStream<byte[], byte[]> grouped = (TimeWindowedKStream<byte[], byte[]>) input.node;
            KTable<Windowed<byte[]>, ?> table;
            ValueType valueType;
            switch (operation.kind()) {
                case COUNT:
                    table = grouped.count(windowMaterialized(store, window, Serdes.Long()));
                    valueType = ValueType.LONG;
                    break;
                case REDUCE: {
                    String function = required(conformanceCase, operation, operation.function(), "fn");
                    table = grouped.reduce((left, right) -> reduce(function, left, right),
                            windowMaterialized(store, window, Serdes.ByteArray()));
                    valueType = ValueType.BYTES;
                    break;
                }
                default: {
                    String function = required(conformanceCase, operation, operation.function(), "fn");
                    table = grouped.aggregate(() -> initial(function),
                            (key, value, aggregate) -> aggregate(function, value, aggregate),
                            windowMaterialized(store, window, Serdes.ByteArray()));
                    valueType = ValueType.BYTES;
                    break;
                }
            }
            if (suppressUntilWindowCloses) {
                // KTD5. The driver never advances stream time on close(), so this emits only when a later record
                // pushes stream time past the window's end plus grace - which is why every pinned-emit case the
                // loader accepts carries a trailing record past that point.
                table = ((KTable<Windowed<byte[]>, Object>) table)
                        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));
            }
            return new Handle(HandleKind.WINDOWED_TABLE, table, valueType, true, window);
        }

        KGroupedStream<byte[], byte[]> grouped = (KGroupedStream<byte[], byte[]>) input.node;
        switch (operation.kind()) {
            case COUNT:
                return new Handle(HandleKind.TABLE, grouped.count(keyValueMaterialized(store, Serdes.Long())),
                        ValueType.LONG, input.windowDerived, null);
            case REDUCE: {
                String function = required(conformanceCase, operation, operation.function(), "fn");
                return new Handle(HandleKind.TABLE,
                        grouped.reduce((left, right) -> reduce(function, left, right),
                                keyValueMaterialized(store, Serdes.ByteArray())),
                        ValueType.BYTES, input.windowDerived, null);
            }
            default: {
                String function = required(conformanceCase, operation, operation.function(), "fn");
                return new Handle(HandleKind.TABLE,
                        grouped.aggregate(() -> initial(function),
                                (key, value, aggregate) -> aggregate(function, value, aggregate),
                                keyValueMaterialized(store, Serdes.ByteArray())),
                        ValueType.BYTES, input.windowDerived, null);
            }
        }
    }

    private static TimeWindows timeWindows(ConformanceCase.WindowSpec window) {
        // ofSizeAndGrace, never the deprecated TimeWindows.of, which silently carries max(24h - size, 0) grace.
        return TimeWindows.ofSizeAndGrace(Duration.ofMillis(window.sizeMs()), Duration.ofMillis(window.graceMs()))
                .advanceBy(Duration.ofMillis(window.advanceMs()));
    }

    private static <V> Materialized<byte[], V, org.apache.kafka.streams.state.KeyValueStore<
            org.apache.kafka.common.utils.Bytes, byte[]>> keyValueMaterialized(String store, Serde<V> valueSerde) {
        // In-memory, matching the wrapper module's convention: RocksDB buys nothing here and costs native libraries.
        return Materialized.<byte[], V>as(Stores.inMemoryKeyValueStore(store))
                .withKeySerde(Serdes.ByteArray())
                .withValueSerde(valueSerde);
    }

    private static <V> Materialized<byte[], V, org.apache.kafka.streams.state.WindowStore<
            org.apache.kafka.common.utils.Bytes, byte[]>> windowMaterialized(String store,
                                                                             ConformanceCase.WindowSpec window,
                                                                             Serde<V> valueSerde) {
        return Materialized.<byte[], V>as(Stores.inMemoryWindowStore(store,
                        Duration.ofMillis(window.retentionMs()), Duration.ofMillis(window.sizeMs()), false))
                .withKeySerde(Serdes.ByteArray())
                .withValueSerde(valueSerde);
    }

    @SuppressWarnings("unchecked")
    private static <V> Serde<V> castSerde(Serde<?> serde) {
        return (Serde<V>) serde;
    }

    // ------------------------------------------------------------------------------------- KTD13's functions

    @Nullable
    private static byte[] mapValues(String function, @Nullable byte[] value) {
        if (value == null || "identity".equals(function)) {
            return value;
        }
        if (!"upper".equals(function)) {
            throw new IllegalArgumentException("map-values names function " + function + ", which is not identity "
                    + "or upper");
        }
        byte[] uppered = new byte[value.length];
        for (int index = 0; index < value.length; index++) {
            byte current = value[index];
            uppered[index] = current >= 'a' && current <= 'z' ? (byte) (current - ('a' - 'A')) : current;
        }
        return uppered;
    }

    private static byte[] reduce(String function, byte[] accumulated, byte[] added) {
        if ("last-wins".equals(function)) {
            return added;
        }
        if (!"concat".equals(function)) {
            throw new IllegalArgumentException("reduce names function " + function + ", which is not last-wins or "
                    + "concat");
        }
        return concatenate(accumulated, null, added);
    }

    private static byte[] initial(String function) {
        // count-bytes is a running total rendered as decimal ASCII, so its zero is the ASCII "0" rather than an
        // empty array - the aggregator parses what it wrote last time.
        return "count-bytes".equals(function) ? "0".getBytes(StandardCharsets.US_ASCII) : new byte[0];
    }

    private static byte[] aggregate(String function, @Nullable byte[] value, byte[] accumulated) {
        int added = value == null ? 0 : value.length;
        if ("count-bytes".equals(function)) {
            long total = Long.parseLong(new String(accumulated, StandardCharsets.US_ASCII)) + added;
            return Long.toString(total).getBytes(StandardCharsets.US_ASCII);
        }
        if (!"concat".equals(function)) {
            throw new IllegalArgumentException("aggregate names function " + function + ", which is not count-bytes "
                    + "or concat");
        }
        return value == null ? accumulated : concatenate(accumulated, null, value);
    }

    private static byte[] join(String function, byte[] streamValue, byte[] tableValue) {
        if (!"concat-sides".equals(function)) {
            throw new IllegalArgumentException("join names function " + function + ", which is not concat-sides");
        }
        // Stream side, separator, table side, in that order - so a transposed binding produces a different outcome.
        return concatenate(streamValue, JOIN_SEPARATOR, tableValue);
    }

    private static byte[] concatenate(byte[] left, @Nullable Byte separator, byte[] right) {
        byte[] joined = new byte[left.length + (separator == null ? 0 : 1) + right.length];
        System.arraycopy(left, 0, joined, 0, left.length);
        int offset = left.length;
        if (separator != null) {
            joined[offset] = separator;
            offset++;
        }
        System.arraycopy(right, 0, joined, offset, right.length);
        return joined;
    }

    // --------------------------------------------------------------------------------------- driving the driver

    private static Properties configuration(Path stateDirectory) {
        Properties configuration = new Properties();
        Object ignoredApplicationId =
                configuration.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        // No broker is ever contacted - TopologyTestDriver requires the key to be set, not to resolve.
        Object ignoredBootstrap = configuration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        Object ignoredStateDir = configuration.put(StreamsConfig.STATE_DIR_CONFIG, stateDirectory.toString());
        // See the class javadoc: with the cache on, what reaches a sink depends on when the driver commits.
        Object ignoredCache = configuration.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0");
        return configuration;
    }

    private static void pipe(ConformanceCase conformanceCase,
                             TopologyTestDriver driver,
                             Translation translation,
                             List<ConformanceCase.InputRecord> records) {
        Map<String, TestInputTopic<byte[], byte[]>> inputs = new LinkedHashMap<>();
        for (String topic : translation.sourceTopics) {
            TestInputTopic<byte[], byte[]> previous = inputs.put(topic, driver.createInputTopic(topic,
                    Serdes.ByteArray().serializer(), Serdes.ByteArray().serializer()));
            // Two sources on one topic would make "which source read this record" unanswerable; named rather than
            // dropped, because the overwrite would otherwise be silent.
            if (previous != null) {
                throw new OracleExecutionException(conformanceCase.name(),
                        "declares two sources reading topic " + topic, null);
            }
        }
        // Each record to the topic it names, in list order: the driver processes each pipe synchronously, so the
        // list IS the interleaving, and a join's two sides are ordered by the case rather than by the oracle.
        for (ConformanceCase.InputRecord record : records) {
            TestInputTopic<byte[], byte[]> input = inputs.get(record.topic());
            if (input == null) {
                // The loader has already refused this, so reaching it means a case was built some other way.
                throw new OracleExecutionException(conformanceCase.name(), "pipes " + record + " to topic "
                        + record.topic() + ", which no source reads; the sources are " + translation.sourceTopics,
                        null);
            }
            input.pipeInput(bytes(record.key()), bytes(record.value()), record.timestamp());
        }
    }

    @Nullable
    private static byte[] bytes(@Nullable String value) {
        return value == null ? null : value.getBytes(StandardCharsets.US_ASCII);
    }

    // ------------------------------------------------------------------------------------------- the snapshot

    private static FinalState snapshot(TopologyTestDriver driver, Translation translation) {
        Map<String, List<String>> stores = new LinkedHashMap<>();
        for (Map.Entry<String, Boolean> store : translation.stores.entrySet()) {
            List<String> previous = stores.put(store.getKey(),
                    store.getValue() ? windowStore(driver, store.getKey()) : keyValueStore(driver, store.getKey()));
            // One entry per store name - translate() has already refused a duplicate, so this only names a bug here.
            assert previous == null : "two snapshots for store " + store.getKey();
        }

        Map<String, List<String>> sinks = new LinkedHashMap<>();
        for (Map.Entry<String, Sink> sink : translation.sinks.entrySet()) {
            List<String> previous = sinks.put(sink.getKey(), sinkEntries(driver, sink.getKey(), sink.getValue()));
            assert previous == null : "two snapshots for sink " + sink.getKey();
        }

        return new FinalState(stores, sinks, translation.description);
    }

    private static List<String> keyValueStore(TopologyTestDriver driver, String name) {
        // An aggregation materialises into a TIMESTAMPED store, which getKeyValueStore refuses by returning null -
        // so the timestamped accessor is tried too, and the wrapper is unwrapped in render().
        KeyValueStore<Object, ?> store = driver.getKeyValueStore(name);
        if (store == null) {
            store = driver.<Object, Object>getTimestampedKeyValueStore(name);
        }
        if (store == null) {
            throw new IllegalStateException("no key-value store named " + name + " is registered with the driver; "
                    + "the topology named it as a store, so either the operation did not materialise it or it is a "
                    + "window store");
        }
        List<String> entries = new ArrayList<>();
        try (KeyValueIterator<Object, ?> iterator = store.all()) {
            while (iterator.hasNext()) {
                KeyValue<Object, ?> entry = iterator.next();
                entries.add(render(entry.key) + " -> " + render(entry.value));
            }
        }
        entries.sort(Comparator.naturalOrder());
        return entries;
    }

    private static List<String> windowStore(TopologyTestDriver driver, String name) {
        WindowStore<Object, ?> store = driver.getWindowStore(name);
        if (store == null) {
            store = driver.<Object, Object>getTimestampedWindowStore(name);
        }
        if (store == null) {
            throw new IllegalStateException("no window store named " + name + " is registered with the driver");
        }
        List<String> entries = new ArrayList<>();
        try (KeyValueIterator<Windowed<Object>, ?> iterator = store.all()) {
            while (iterator.hasNext()) {
                KeyValue<Windowed<Object>, ?> entry = iterator.next();
                entries.add(render(entry.key.key()) + "@[" + entry.key.window().start() + ","
                        + entry.key.window().end() + ") -> " + render(entry.value));
            }
        }
        entries.sort(Comparator.naturalOrder());
        return entries;
    }

    private static List<String> sinkEntries(TopologyTestDriver driver, String topic, Sink sink) {
        TestOutputTopic<byte[], Object> output = driver.createOutputTopic(topic,
                Serdes.ByteArray().deserializer(), Oracle.<Object>castSerde(sink.valueSerde).deserializer());
        List<TestRecord<byte[], Object>> records = output.readRecordsToList();

        List<String> ordered = new ArrayList<>();
        for (TestRecord<byte[], Object> record : records) {
            ordered.add(sinkEntry(render(record.key()), render(record.value()), record.timestamp()));
        }
        if (sink.windowedFed) {
            // R3: to-stream drops the window, so last-per-key would keep one record of many. The full ordered list
            // is the observable, and its order is the emission order.
            return ordered;
        }

        // Last record per key, keyed by the RENDERED key so two records under one byte-array key fold to one entry
        // rather than staying two (KTD3). Sorted, because a map's iteration order is not an observable.
        Map<String, String> lastPerKey = new TreeMap<>();
        for (TestRecord<byte[], Object> record : records) {
            String ignoredDisplaced = lastPerKey.put(render(record.key()),
                    sinkEntry(render(record.key()), render(record.value()), record.timestamp()));
            // Displacing an earlier record IS the fold, so the displaced entry is named and dropped on purpose.
        }
        return new ArrayList<>(lastPerKey.values());
    }

    private static String sinkEntry(String key, String value, @Nullable Long timestampMs) {
        return key + " -> " + value + " @" + timestampMs;
    }

    /**
     * The one place a key or a value becomes a value-equal, readable string (KTD3). Printable ASCII is quoted, so it
     * cannot collide with the {@code 0x}-prefixed hex a non-printable payload falls back to, or with the bare
     * {@code null} of an absent one; a {@code Long} - a {@code count}'s result - renders as its decimal.
     */
    static String render(@Nullable Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof ValueAndTimestamp) {
            // See FinalState's javadoc: the store wrapper's timestamp is not part of the rendering.
            return render(((ValueAndTimestamp<?>) value).value());
        }
        if (!(value instanceof byte[])) {
            return value.toString();
        }
        byte[] payload = (byte[]) value;
        boolean printable = true;
        for (byte current : payload) {
            if (current < 0x20 || current > 0x7e) {
                printable = false;
                break;
            }
        }
        if (printable) {
            return "\"" + new String(payload, StandardCharsets.US_ASCII) + "\"";
        }
        StringBuilder hex = new StringBuilder("0x");
        for (byte current : payload) {
            hex.append(String.format("%02x", current));
        }
        return hex.toString();
    }

    // ---------------------------------------------------------------------------------------------- plumbing

    private static Handle resolveOnly(ConformanceCase conformanceCase,
                                      Map<String, Handle> handles,
                                      ConformanceCase.Operation operation) {
        List<String> inputs = operation.inputs();
        if (inputs.size() != 1) {
            throw new OracleExecutionException(conformanceCase.name(), operation + " names " + inputs.size()
                    + " input handles; only a join names two", null);
        }
        return resolve(conformanceCase, handles, operation, Objects.requireNonNull(inputs.get(0)));
    }

    private static Handle resolve(ConformanceCase conformanceCase,
                                  Map<String, Handle> handles,
                                  ConformanceCase.Operation operation,
                                  String id) {
        Handle handle = handles.get(id);
        if (handle == null) {
            throw new OracleExecutionException(conformanceCase.name(), operation + " names input handle " + id
                    + ", which the oracle has not built", null);
        }
        return handle;
    }

    private static void put(ConformanceCase conformanceCase,
                            Map<String, Handle> handles,
                            ConformanceCase.Operation operation,
                            Handle handle) {
        String id = Objects.requireNonNull(operation.id(), "a handle-minting operation carries an id");
        Handle previous = handles.put(id, handle);
        if (previous != null) {
            throw new OracleExecutionException(conformanceCase.name(),
                    operation + " re-declares handle id " + id, null);
        }
    }

    private static String required(ConformanceCase conformanceCase,
                                   ConformanceCase.Operation operation,
                                   @Nullable String value,
                                   String field) {
        if (value == null) {
            throw new OracleExecutionException(conformanceCase.name(),
                    operation + " declares no " + field, null);
        }
        return value;
    }

    private static void requireBytes(ConformanceCase conformanceCase,
                                     ConformanceCase.Operation operation,
                                     Handle handle) {
        if (handle.valueType != ValueType.BYTES) {
            throw new OracleExecutionException(conformanceCase.name(), operation + " reads a handle carrying "
                    + handle.valueType + " values, but the vocabulary's functions are defined on byte arrays "
                    + "(KTD13); only count produces a non-byte-array value, and only to-stream and sink accept one",
                    null);
        }
    }

    private static OracleExecutionException wrongKind(ConformanceCase conformanceCase,
                                                      ConformanceCase.Operation operation,
                                                      Handle handle,
                                                      String expected) {
        return new OracleExecutionException(conformanceCase.name(), operation + " needs " + expected
                + ", but the handle it names is " + handle.kind, null);
    }

    private static void deleteRecursively(@Nullable Path directory) {
        if (directory == null || !Files.exists(directory)) {
            return;
        }
        try {
            Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path visited, @Nullable IOException failure)
                        throws IOException {
                    Files.delete(visited);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException("cannot clean up the oracle's temporary state directory " + directory, e);
        }
    }

    /**
     * A thrown build, pipe or snapshot, reported as its own category so a maintainer never has to infer which proof
     * failed (KTD10). It is distinct from a load-time refusal - {@link CaseLoader.CorpusRefusedException} - and from
     * a comparison red, and it always names the case.
     */
    public static final class OracleExecutionException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        private final String caseName;

        OracleExecutionException(String caseName, String what, @Nullable Throwable cause) {
            super("oracle execution failed: case " + caseName + " " + what, cause);
            this.caseName = caseName;
        }

        /** The case that failed - the thing a red has to name (R11). */
        public String caseName() {
            return caseName;
        }
    }
}
