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
import com.github.bsideup.jabel.Desugar;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Replays a host's builder calls against a real {@link StreamsBuilder}.
 *
 * <p>The engine holds the builder objects and hands back opaque handles; the host names handles back. Nothing about
 * the chain's shape is hardcoded here - the topology this produces is whatever the host described, which is the
 * claim the whole proof rests on.
 *
 * <p>Every mint records what it made - the handle's kind and its key and value types - in the same store as the
 * node, so the wire can tell the host what a handle IS and the sink can select its serde from the record instead of
 * special-casing what the node happens to be an instance of. One store, deliberately: a parallel type map would be
 * a second copy of the same fact, waiting to drift.
 *
 * <p>Not thread-safe: a session describes its topology from one stream, in order.
 */
public class TopologyAssembler {

    /** Resolves a host function token to the operator that calls it. */
    @FunctionalInterface
    public interface MapperFactory {
        ValueMapperWithKey<byte[], byte[], byte[]> forToken(long functionToken);
    }

    /** The joining third of the same seam: resolves a token to a joiner whose step runs in the host's language. */
    @FunctionalInterface
    public interface JoinerFactory {
        ValueJoiner<byte[], byte[], byte[]> forToken(long functionToken);
    }

    /**
     * The combining half of the same seam: resolves a token to a reducer whose step runs in the host's language.
     *
     * <p>A second interface rather than a second method on {@link MapperFactory}, so both stay lambda-shaped at
     * every call site. Two abstract methods would force an anonymous class on the service and on every test.
     */
    @FunctionalInterface
    public interface ReducerFactory {
        Reducer<byte[]> forToken(long functionToken);
    }

    /**
     * The windowed-aggregation quarter of the same seam: resolves a token to an aggregator whose step runs in the
     * host's language. A fourth interface for the same reason {@link ReducerFactory} is a second one - one
     * interface per Kafka functional shape keeps every factory lambda-shaped at every call site.
     */
    @FunctionalInterface
    public interface AggregatorFactory {
        Aggregator<byte[], byte[], byte[]> forToken(long functionToken);
    }

    /**
     * A minted handle: the builder node and the recorded type it carries on the wire.
     *
     * <p>The constructor validates that the recorded kind matches what the node actually is, so a future operator
     * that pairs a node with the wrong constant fails loudly at the mint that made the mistake - not one call later
     * as a bare {@code ClassCastException} leaking a Kafka Streams implementation class name to the host. That
     * validation is what makes the erased casts in {@link #resolve} and {@link #sink} safe. Package-visible so the
     * mismatch refusal is testable.
     */
    @Desugar // Jabel requires the annotation on every record
    record Minted(Object node, HandleType type) {
        Minted {
            Class<?> required = switch (type.getKind()) {
                case HANDLE_KIND_STREAM -> KStream.class;
                case HANDLE_KIND_GROUPED_STREAM -> KGroupedStream.class;
                case HANDLE_KIND_TABLE -> KTable.class;
                case HANDLE_KIND_TIME_WINDOWED_STREAM -> TimeWindowedKStream.class;
                default -> throw new IllegalArgumentException(
                        "engine bug: a mint must record a known kind, got " + type.getKind());
            };
            if (!required.isInstance(node)) {
                throw new IllegalArgumentException("engine bug: a mint paired a "
                        + node.getClass().getSimpleName() + " with recorded kind " + kindName(type.getKind()));
            }
        }
    }

    private static final HandleType STREAM_OF_BYTES = handleType(
            HandleKind.HANDLE_KIND_STREAM, DataType.DATA_TYPE_BYTES, DataType.DATA_TYPE_BYTES);
    private static final HandleType GROUPED_STREAM_OF_BYTES = handleType(
            HandleKind.HANDLE_KIND_GROUPED_STREAM, DataType.DATA_TYPE_BYTES, DataType.DATA_TYPE_BYTES);
    private static final HandleType TABLE_OF_BYTES = handleType(
            HandleKind.HANDLE_KIND_TABLE, DataType.DATA_TYPE_BYTES, DataType.DATA_TYPE_BYTES);
    private static final HandleType TABLE_OF_LONGS = handleType(
            HandleKind.HANDLE_KIND_TABLE, DataType.DATA_TYPE_BYTES, DataType.DATA_TYPE_LONG);

    private final StreamsBuilder builder = new StreamsBuilder();
    private final Map<Long, Minted> handles = new HashMap<>();
    private final Map<String, DataType> storeValueTypes = new HashMap<>();
    private final AtomicLong nextHandle = new AtomicLong(1);
    private final MapperFactory mappers;
    private final ReducerFactory reducers;
    private final JoinerFactory joiners;
    private final AggregatorFactory aggregators;
    private boolean built;
    private Topology topology;

    public TopologyAssembler(MapperFactory mappers, ReducerFactory reducers, JoinerFactory joiners,
                             AggregatorFactory aggregators) {
        this.mappers = mappers;
        this.reducers = java.util.Objects.requireNonNull(reducers, "reducers");
        this.joiners = java.util.Objects.requireNonNull(joiners, "joiners");
        this.aggregators = java.util.Objects.requireNonNull(aggregators, "aggregators");
    }

    public long source(String topic) {
        requireNotBuilt("source");
        require(topic != null && !topic.isEmpty(), "source names no topic");
        return mint(builder.stream(topic, Consumed.with(Serdes.ByteArray(), Serdes.ByteArray())), STREAM_OF_BYTES);
    }

    public long mapValues(long handle, long functionToken) {
        requireNotBuilt("mapValues");
        // The foreign function is bytes-in, bytes-out by contract, so the mapped stream stays a stream of bytes.
        KStream<byte[], byte[]> upstream = resolveStream(handle, "mapValues");
        return mint(upstream.mapValues(mappers.forToken(functionToken)), STREAM_OF_BYTES);
    }

    public long groupByKey(long handle) {
        requireNotBuilt("groupByKey");
        KStream<byte[], byte[]> upstream = resolveStream(handle, "groupByKey");
        return mint(upstream.groupByKey(Grouped.with(Serdes.ByteArray(), Serdes.ByteArray())),
                GROUPED_STREAM_OF_BYTES);
    }

    /**
     * The aggregation, over an IN-MEMORY store.
     *
     * <p>Not RocksDB, and not as a convenience: an in-memory store exercises state, the changelog and commits
     * while keeping the native-image question independent of this proof. Note the reason has narrowed - RocksDB
     * was measured working on the JVM sidecar first try, including macOS arm64, so the remaining question is
     * specifically RocksDB *under a native image*, not RocksDB. See
     * docs/inflight/core-rocksdb-works-on-the-jvm-sidecar.md.
     *
     * <p>This is the operator that makes handle types necessary at all: it mints a table of longs the host never
     * supplied, and the recorded {@link #TABLE_OF_LONGS} is how the sink and the host both learn that.
     */
    public long count(long handle, String storeName) {
        requireNotBuilt("count");
        require(storeName != null && !storeName.isEmpty(), "count names no store");
        KGroupedStream<byte[], byte[]> upstream = resolve(
                handle, HandleKind.HANDLE_KIND_GROUPED_STREAM, "count");
        // The store serdes derive from the SAME recorded type the sink will later select by, so the two cannot
        // drift apart when the next typed operator is copied from this one.
        HandleType resultType = TABLE_OF_LONGS;
        storeValueTypes.put(storeName, resultType.getValueType());
        return mint(upstream.count(Materialized.<byte[], Long>as(Stores.inMemoryKeyValueStore(storeName))
                .withKeySerde(operatorSerde(resultType.getKeyType()))
                .withValueSerde(operatorSerde(resultType.getValueType()))), resultType);
    }

    /**
     * Terminates a chain into a topic. The value serde is selected from the handle's RECORDED value type - there is
     * no per-operator special case, so a new typed operator needs a new serde branch here and nothing else.
     */
    /**
     * Combine each key's values with a host-supplied function, into a table of bytes.
     *
     * <p>The sibling of {@link #count(long, String)} and the more interesting one. Count computes its own result
     * and the host never sees the state; reduce sends the stored aggregate OUT to the host and stores what comes
     * back, so engine state is computed by foreign code. The result is a table of bytes rather than longs -
     * a reduction preserves the value type - which is why the recorded handle type earns its keep twice over.
     */
    public long reduce(long handle, long functionToken, String storeName) {
        requireNotBuilt("reduce");
        require(storeName != null && !storeName.isEmpty(), "reduce names no store");
        KGroupedStream<byte[], byte[]> upstream = resolve(
                handle, HandleKind.HANDLE_KIND_GROUPED_STREAM, "reduce");
        HandleType resultType = TABLE_OF_BYTES;
        storeValueTypes.put(storeName, resultType.getValueType());
        return mint(upstream.reduce(reducers.forToken(functionToken),
                Materialized.<byte[], byte[]>as(Stores.inMemoryKeyValueStore(storeName))
                        .withKeySerde(operatorSerde(resultType.getKeyType()))
                        .withValueSerde(operatorSerde(resultType.getValueType()))), resultType);
    }

    /**
     * Join a stream against a table, combining each record with the table's current value for its key.
     *
     * <p>The first operator taking TWO handles, so the topology stops being a chain. Both are resolved by their
     * recorded kind, which means naming them the wrong way round is refused at the call that made the mistake
     * rather than surfacing as a class-cast somewhere inside Kafka Streams.
     *
     * <p>Records whose key is absent from the table are dropped, which is Kafka's inner-join semantics and not
     * something this wire chooses.
     */
    /**
     * Joins a stream to a table, calling the host once per stream record that finds a match.
     *
     * <p>The table's value type is checked, not just its kind, because a count's table holds longs and the host's
     * joiner is handed bytes. Erasure lets that pair compile, so without this the mismatch would surface deep inside
     * Kafka Streams as a {@code ClassCastException} naming a class the host has never heard of - one record into a
     * run, not at the call that described the wrong thing.
     */
    public long join(long streamHandle, long tableHandle, long functionToken) {
        requireNotBuilt("join");
        KStream<byte[], byte[]> stream = resolve(streamHandle, HandleKind.HANDLE_KIND_STREAM, "join");
        KTable<byte[], byte[]> table = resolve(tableHandle, HandleKind.HANDLE_KIND_TABLE, "join");
        DataType tableValues = handles.get(tableHandle).type().getValueType();
        require(tableValues == DataType.DATA_TYPE_BYTES, "join cannot be applied to handle " + tableHandle
                + ": its table holds " + typeName(tableValues) + " values, and a host joiner is handed bytes");
        return mint(stream.join(table, joiners.forToken(functionToken)), STREAM_OF_BYTES);
    }

    /**
     * Windows a grouped stream by time, minting a time-windowed stream that records the specification on its
     * {@link HandleType} - the one place the store, the read path and the sink refusal can all read it back from.
     *
     * <p>Constructed with {@code TimeWindows.ofSizeAndGrace} only. The deprecated {@code TimeWindows.of} plus
     * {@code .grace} path silently carries {@code max(24h - size, 0)} grace where the new path carries zero, so
     * the two differ in behaviour and not only in style - it is banned here outright.
     *
     * <p>All four window fields must be present, and each default is a trap the refusals below name: a defaulted
     * retention is Kafka's own {@code size + grace}, under which a one-hour window retains roughly the currently
     * open window and nothing else.
     *
     * <p>Present-but-invalid values are refused here too, by name and in protocol vocabulary. Without these
     * refusals a value like {@code advance_ms} of zero reaches Kafka's own window constructors and surfaces to
     * the host as an unnamed engine failure quoting a class it has never heard of.
     */
    public long windowedBy(long handle, TimeWindowSpec window) {
        requireNotBuilt("windowed_by");
        requireWindowField(window.hasSizeMs(), "size_ms");
        requireWindowField(window.hasAdvanceMs(), "advance_ms");
        requireWindowField(window.hasGraceMs(), "grace_ms");
        requireWindowField(window.hasRetentionMs(), "retention_ms");
        require(window.getSizeMs() >= 1, "windowed_by names size_ms " + window.getSizeMs()
                + ", below the minimum 1: a window with no width can hold no record");
        require(window.getAdvanceMs() > 0, "windowed_by names advance_ms " + window.getAdvanceMs()
                + ", below the minimum 1: a hop that never advances would reopen the same window forever");
        require(window.getAdvanceMs() <= window.getSizeMs(), "windowed_by names advance_ms "
                + window.getAdvanceMs() + ", above size_ms " + window.getSizeMs()
                + ": a hop wider than the window would leave records falling between windows");
        require(window.getGraceMs() >= 0, "windowed_by names grace_ms " + window.getGraceMs()
                + ", below the minimum 0: a window cannot close before it ends");
        long minimumRetention = window.getSizeMs() + window.getGraceMs();
        require(window.getRetentionMs() >= minimumRetention,
                "windowed_by names retention_ms " + window.getRetentionMs() + ", below the minimum "
                        + minimumRetention + " (size_ms + grace_ms): a store retaining less than a window's whole"
                        + " life cannot serve it");
        KGroupedStream<byte[], byte[]> upstream = resolve(
                handle, HandleKind.HANDLE_KIND_GROUPED_STREAM, "windowed_by");
        TimeWindows windows = TimeWindows.ofSizeAndGrace(
                        Duration.ofMillis(window.getSizeMs()), Duration.ofMillis(window.getGraceMs()))
                .advanceBy(Duration.ofMillis(window.getAdvanceMs()));
        return mint(upstream.windowedBy(windows), windowedType(
                HandleKind.HANDLE_KIND_TIME_WINDOWED_STREAM, window));
    }

    /**
     * The windowed aggregation, whose accumulator passes through the host - the operator {@code reduce} cannot
     * stand in for, because Kafka never calls a reducer for a key's first value. Here the engine supplies the
     * initializer's bytes itself, so the host's function sees every record.
     *
     * <p>The initializer hands out a DEFENSIVE COPY of the captured bytes on every call. It runs once per new
     * window per key, and a shared array would alias: the first key's in-place mutations would become the second
     * key's starting accumulator.
     *
     * <p>The store serdes come from the recorded type exactly as {@link #count} and {@link #reduce} select
     * theirs, and {@code Materialized.withRetention} carries the specification's {@code retention_ms}.
     */
    public long aggregate(long handle, byte[] initial, long functionToken, String storeName) {
        byte[] captured = initial.clone();
        return windowedAggregate(handle, captured::clone, aggregators.forToken(functionToken), storeName);
    }

    /**
     * The whole {@code aggregate} wire call: dispatches between the two placements the message can name, and
     * refuses the shapes that name both or neither.
     *
     * <p>{@code function_token} and {@code combine} are alternatives - a host function at the aggregator, or a
     * fold the engine executes with nothing crossing per record. Precedence would silently discard half of what
     * the host said, so both set, and neither set, are refused by name. {@code initial} belongs to the token
     * placement only: a combine kind defines its own empty accumulator, and initial bytes arriving beside a
     * combine would be silently dropped state - refused instead.
     */
    public long aggregate(Aggregate call) {
        require(!(call.hasFunctionToken() && call.hasCombine()),
                "aggregate carries both function_token and combine; they are alternatives - a host function"
                        + " called at the aggregator, or an engine-executed combine");
        require(call.hasFunctionToken() || call.hasCombine(),
                "aggregate carries neither function_token nor combine; exactly one must be set");
        if (call.hasFunctionToken()) {
            return aggregate(call.getHandle(), call.getInitial().toByteArray(), call.getFunctionToken(),
                    call.getStoreName());
        }
        require(!call.hasInitial(), "aggregate carries initial alongside combine; a combine kind defines its own"
                + " empty accumulator, so the bytes would be silently dropped");
        return windowedAggregate(call.getHandle(), () -> new byte[0],
                combineAggregator(call.getCombine()), call.getStoreName());
    }

    /**
     * The engine-executed combine for a named kind - the whole point is that the returned {@link Aggregator}
     * never touches the host boundary. Package-visible so the counting tests can run the SAME implementation
     * under {@code suppress} and {@code emitStrategy}, which this wire does not expose.
     *
     * <p>An unknown or unspecified kind is refused by name: proto3's zero member selects nothing, and defaulting
     * it to either real kind would silently choose an accumulator shape the host never asked for.
     */
    static Aggregator<byte[], byte[], byte[]> combineAggregator(CombineKind kind) {
        return switch (kind) {
            case COMBINE_KIND_APPEND_BYTES -> (key, value, aggregate) -> appendLengthPrefixed(aggregate, value);
            case COMBINE_KIND_LAST_BYTES -> (key, value, aggregate) -> value;
            default -> throw new TopologyDescriptionException("aggregate names combine kind " + kind
                    + ", which is outside this engine's combine set; name COMBINE_KIND_APPEND_BYTES or"
                    + " COMBINE_KIND_LAST_BYTES");
        };
    }

    /**
     * One appended value: the accumulator, then the value's length as four big-endian bytes, then the value -
     * so a reader splits the collection on declared boundaries in arrival order rather than guessing.
     */
    private static byte[] appendLengthPrefixed(byte[] accumulator, byte[] value) {
        ByteBuffer grown = ByteBuffer.allocate(accumulator.length + Integer.BYTES + value.length);
        grown.put(accumulator).putInt(value.length).put(value);
        return grown.array();
    }

    /**
     * The materialisation both placements share, so the minted handle, the store, the read path and the sink
     * refusal cannot differ between them - which is what makes a placement comparison a comparison of the
     * placement and nothing else.
     */
    private long windowedAggregate(long handle, Initializer<byte[]> initializer,
                                   Aggregator<byte[], byte[], byte[]> aggregator, String storeName) {
        requireNotBuilt("aggregate");
        require(storeName != null && !storeName.isEmpty(), "aggregate names no store");
        TimeWindowedKStream<byte[], byte[]> upstream = resolve(
                handle, HandleKind.HANDLE_KIND_TIME_WINDOWED_STREAM, "aggregate");
        TimeWindowSpec window = handles.get(handle).type().getWindow();
        HandleType resultType = windowedType(HandleKind.HANDLE_KIND_TABLE, window);
        storeValueTypes.put(storeName, resultType.getValueType());
        Materialized<byte[], byte[], WindowStore<Bytes, byte[]>> materialized =
                Materialized.<byte[], byte[], WindowStore<Bytes, byte[]>>as(storeName)
                        .withStoreType(Materialized.StoreType.IN_MEMORY)
                        .withRetention(Duration.ofMillis(window.getRetentionMs()))
                        .withKeySerde(operatorSerde(resultType.getKeyType()))
                        .withValueSerde(operatorSerde(resultType.getValueType()));
        // MEASUREMENT-ONLY ESCAPE HATCH, off by default: the engine-floor spike needs one arm with the
        // changelog term removed and nothing else changed. It is deliberately NOT on the protocol - see
        // docs/inflight/perf-streams-engine-floor.md; a real capability would be an additive field on
        // Aggregate. A system property rather than an environment variable because the lab already owns
        // the engine's JVM arguments and does not own its environment.
        if (Boolean.getBoolean("pcStreams.measure.disableChangelog")) {
            materialized = materialized.withLoggingDisabled();
        }
        return mint(upstream.aggregate(initializer, aggregator, materialized), resultType);
    }

    /**
     * Re-keys a windowed table to its INNER key, minting a plain byte-keyed stream - the one call that makes a
     * windowed table consumable. The window is dropped, not encoded: no internal windowed-key layout reaches a
     * topic, and {@code map_values} and {@code sink} accept the result unchanged.
     *
     * <p>The cost lands on the reader rather than the wire: the sunk topic carries one record per emit per window
     * under colliding inner keys, so last-value-per-key stops meaning "final aggregate" over it.
     */
    public long toStream(long handle) {
        requireNotBuilt("to_stream");
        Minted minted = handles.get(handle);
        if (minted == null) {
            throw unknownHandle(handle, "to_stream");
        }
        HandleType type = minted.type();
        if (type.getKind() != HandleKind.HANDLE_KIND_TABLE || !type.hasWindow()) {
            throw new TopologyDescriptionException("to_stream cannot be applied to handle " + handle
                    + ": it names a " + describedKind(type) + ", and to_stream needs a windowed table");
        }
        @SuppressWarnings("unchecked")
        KTable<Windowed<byte[]>, byte[]> table = (KTable<Windowed<byte[]>, byte[]>) minted.node();
        return mint(table.toStream((windowedKey, value) -> windowedKey.key()), STREAM_OF_BYTES);
    }

    public void sink(long handle, String topic) {
        requireNotBuilt("sink");
        require(topic != null && !topic.isEmpty(), "sink names no topic");
        Minted minted = handles.get(handle);
        if (minted == null) {
            throw unknownHandle(handle, "sink");
        }
        // The refusal is on the windowed KEY, not on the table: writing one means either shipping Kafka's internal
        // window layout to a topic or inventing an encoding no other Kafka Streams consumer could read. The way
        // out is named, in protocol vocabulary.
        if (minted.type().hasWindow() && minted.type().getKind() == HandleKind.HANDLE_KIND_TABLE) {
            throw new TopologyDescriptionException("sink cannot be applied to handle " + handle
                    + ": it names a windowed table, whose keys carry a window this wire does not encode onto a"
                    + " topic; call to_stream first to re-key it to its inner key");
        }
        // The kind is checked before any serde is selected: a host that sinks the wrong kind of handle is told
        // about the kind, not about a serde it would never have reached - the actionable refusal comes first.
        switch (minted.type().getKind()) {
            case HANDLE_KIND_TABLE -> {
                // A count is a KTable, and a changelog: the sink carries every intermediate value per key, so a
                // reader of that topic must take the last value per key rather than summing what it sees.
                @SuppressWarnings("unchecked")
                KTable<byte[], Object> table = (KTable<byte[], Object>) minted.node();
                table.toStream().to(topic, produced(minted.type(), handle));
            }
            case HANDLE_KIND_STREAM -> {
                @SuppressWarnings("unchecked")
                KStream<byte[], Object> stream = (KStream<byte[], Object>) minted.node();
                stream.to(topic, produced(minted.type(), handle));
            }
            default -> throw new TopologyDescriptionException("sink cannot be applied to handle " + handle
                    + ": it names a " + kindName(minted.type().getKind())
                    + ", and sink needs a stream or a table");
        }
    }

    /** Both serdes the sink writes with, each selected from the handle's recorded type - key and value alike. */
    private static Produced<byte[], Object> produced(HandleType type, long handle) {
        return Produced.with(
                serdeFor(type.getKeyType(), "key type", handle),
                serdeFor(type.getValueType(), "value type", handle));
    }

    /**
     * What a handle is, as recorded at its mint. This is what the session puts on the wire in the
     * {@code HandleAssigned} that answers the minting call.
     */
    public HandleType typeOf(long handle) {
        Minted minted = handles.get(handle);
        if (minted == null) {
            throw unknownHandle(handle, "typeOf");
        }
        return minted.type();
    }

    /**
     * Materialises the topology, once. The host's description ends here; a session describes once.
     *
     * <p>Memoised rather than one-shot so that describing a topology does not consume the right to
     * start it. A host that asks what it just built, and is then refused permission to run it,
     * has been punished for looking.
     */
    public Topology build() {
        if (topology == null) {
            built = true;
            topology = builder.build();
        }
        return topology;
    }

    /**
     * The one mapping from a recorded type to the serde that writes it. Everything that serialises - the sink's
     * key and value, and an operator's store - selects through here, so a new mintable type is one new branch.
     * Null for a type with no serde; the two callers below refuse it in their own vocabulary.
     */
    private static Serde<?> serdeMapping(DataType type) {
        return switch (type) {
            case DATA_TYPE_BYTES -> Serdes.ByteArray();
            case DATA_TYPE_LONG -> Serdes.Long();
            default -> null;
        };
    }

    /**
     * The serde a sink writes one axis of a handle with. Exhaustive with a refusing default: a type this engine
     * has no serde for is refused naming the handle, the axis and the type - never silently written as bytes,
     * because a wrong value entering a topic is worse than a refused call. Package-visible so the refusal is
     * testable without forging a mint path for a type no current operator produces.
     */
    static <T> Serde<T> serdeFor(DataType type, String axis, long handle) {
        Serde<?> serde = serdeMapping(type);
        if (serde == null) {
            throw new TopologyDescriptionException("sink cannot write handle " + handle
                    + ": its " + axis + " " + typeName(type) + " has no serde in this engine");
        }
        @SuppressWarnings("unchecked")
        Serde<T> cast = (Serde<T>) serde;
        return cast;
    }

    /**
     * The serde an operator's own store uses for a type it is about to record. A miss here is an engine bug (an
     * operator recording a type the engine cannot write), not a host mistake, so it refuses as one.
     */
    private static <T> Serde<T> operatorSerde(DataType type) {
        Serde<?> serde = serdeMapping(type);
        if (serde == null) {
            throw new IllegalStateException("engine bug: an operator records type " + typeName(type)
                    + ", which has no serde");
        }
        @SuppressWarnings("unchecked")
        Serde<T> cast = (Serde<T>) serde;
        return cast;
    }

    /**
     * What a named store holds, recorded where the store was created.
     *
     * <p>An interactive query has only a store name to go on, so something must remember whether that store
     * holds longs or bytes. Recording it at the mint is the one place that cannot drift from the serde the
     * same call just chose.
     */
    public DataType storeValueType(String storeName) {
        DataType recorded = storeValueTypes.get(storeName);
        if (recorded == null) {
            throw new TopologyDescriptionException("no store named " + storeName + " in this topology");
        }
        return recorded;
    }

    private long mint(Object node, HandleType type) {
        long handle = nextHandle.getAndIncrement();
        handles.put(handle, new Minted(node, type));
        return handle;
    }

    /** The commonest resolution: an operator that consumes a stream of bytes. */
    private KStream<byte[], byte[]> resolveStream(long handle, String method) {
        return resolve(handle, HandleKind.HANDLE_KIND_STREAM, method);
    }

    /**
     * Resolves a handle to its node, refusing by the RECORDED kind in protocol vocabulary. The kind is the single
     * source of truth here; the cast that follows is safe because every mint records the kind of the node it stored.
     */
    private <T> T resolve(long handle, HandleKind expected, String method) {
        Minted minted = handles.get(handle);
        if (minted == null) {
            throw unknownHandle(handle, method);
        }
        if (minted.type().getKind() != expected) {
            throw new TopologyDescriptionException(method + " cannot be applied to handle " + handle
                    + ": it names a " + kindName(minted.type().getKind())
                    + ", and " + method + " needs a " + kindName(expected));
        }
        @SuppressWarnings("unchecked")
        T node = (T) minted.node();
        return node;
    }

    /** The protocol's name for a kind - "grouped stream", not {@code KGroupedStreamImpl}. */
    private static String kindName(HandleKind kind) {
        return switch (kind) {
            case HANDLE_KIND_STREAM -> "stream";
            case HANDLE_KIND_GROUPED_STREAM -> "grouped stream";
            case HANDLE_KIND_TABLE -> "table";
            case HANDLE_KIND_TIME_WINDOWED_STREAM -> "time-windowed stream";
            default -> "handle of unspecified kind";
        };
    }

    /** The kind plus its parameterisation: a table whose type carries a window is a "windowed table" to the host. */
    private static String describedKind(HandleType type) {
        if (type.hasWindow() && type.getKind() == HandleKind.HANDLE_KIND_TABLE) {
            return "windowed table";
        }
        return kindName(type.getKind());
    }

    /** A windowed mint's recorded type: bytes on both axes, with the specification riding on the type itself. */
    private static HandleType windowedType(HandleKind kind, TimeWindowSpec window) {
        return HandleType.newBuilder()
                .setKind(kind)
                .setKeyType(DataType.DATA_TYPE_BYTES)
                .setValueType(DataType.DATA_TYPE_BYTES)
                .setWindow(window)
                .build();
    }

    /** All four window fields are always present (tumbling is advance equal to size); a missing one is refused
     * by name rather than silently defaulted, because each proto3 default here means something wrong. */
    private static void requireWindowField(boolean present, String field) {
        require(present, "windowed_by carries a window specification missing " + field
                + "; size_ms, advance_ms, grace_ms and retention_ms must all be present");
    }

    private static String typeName(DataType type) {
        return switch (type) {
            case DATA_TYPE_BYTES -> "bytes";
            case DATA_TYPE_LONG -> "long";
            default -> "unspecified";
        };
    }

    private static HandleType handleType(HandleKind kind, DataType keyType, DataType valueType) {
        return HandleType.newBuilder().setKind(kind).setKeyType(keyType).setValueType(valueType).build();
    }

    private static TopologyDescriptionException unknownHandle(long handle, String method) {
        return new TopologyDescriptionException(method + " names handle " + handle + ", which does not exist");
    }

    private void requireNotBuilt(String method) {
        if (built) {
            throw new TopologyDescriptionException(
                    method + " arrived after the topology was built; a session describes its topology once");
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new TopologyDescriptionException(message);
        }
    }
}
