package bz.stub.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.streams.protocol.v1alpha1.DataType;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.HandleKind;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.HandleType;
import com.github.bsideup.jabel.Desugar;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapperWithKey;
import org.apache.kafka.streams.state.Stores;

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
    private boolean built;
    private Topology topology;

    public TopologyAssembler(MapperFactory mappers, ReducerFactory reducers) {
        this.mappers = mappers;
        this.reducers = java.util.Objects.requireNonNull(reducers, "reducers");
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

    public void sink(long handle, String topic) {
        requireNotBuilt("sink");
        require(topic != null && !topic.isEmpty(), "sink names no topic");
        Minted minted = handles.get(handle);
        if (minted == null) {
            throw unknownHandle(handle, "sink");
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
            default -> "handle of unspecified kind";
        };
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
