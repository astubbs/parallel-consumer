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

    /** A minted handle: the builder node and the recorded type it carries on the wire. */
    @Desugar // Jabel requires the annotation on every record
    private record Minted(Object node, HandleType type) {
    }

    private static final HandleType STREAM_OF_BYTES = handleType(
            HandleKind.HANDLE_KIND_STREAM, DataType.DATA_TYPE_BYTES, DataType.DATA_TYPE_BYTES);
    private static final HandleType GROUPED_STREAM_OF_BYTES = handleType(
            HandleKind.HANDLE_KIND_GROUPED_STREAM, DataType.DATA_TYPE_BYTES, DataType.DATA_TYPE_BYTES);
    private static final HandleType TABLE_OF_LONGS = handleType(
            HandleKind.HANDLE_KIND_TABLE, DataType.DATA_TYPE_BYTES, DataType.DATA_TYPE_LONG);

    private final StreamsBuilder builder = new StreamsBuilder();
    private final Map<Long, Minted> handles = new HashMap<>();
    private final AtomicLong nextHandle = new AtomicLong(1);
    private final MapperFactory mappers;
    private boolean built;
    private Topology topology;

    public TopologyAssembler(MapperFactory mappers) {
        this.mappers = mappers;
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
     * <p>Not RocksDB, and not as a convenience: RocksDB is a JNI-backed native library whose per-platform problems
     * under a native image are documented and land on this project's own platform. An in-memory store exercises
     * state, the changelog and commits while keeping the native-image question independent of this proof.
     *
     * <p>This is the operator that makes handle types necessary at all: it mints a table of longs the host never
     * supplied, and the recorded {@link #TABLE_OF_LONGS} is how the sink and the host both learn that.
     */
    public long count(long handle, String storeName) {
        requireNotBuilt("count");
        require(storeName != null && !storeName.isEmpty(), "count names no store");
        KGroupedStream<byte[], byte[]> upstream = resolve(
                handle, HandleKind.HANDLE_KIND_GROUPED_STREAM, "count");
        return mint(upstream.count(Materialized.<byte[], Long>as(Stores.inMemoryKeyValueStore(storeName))
                .withKeySerde(Serdes.ByteArray())
                .withValueSerde(Serdes.Long())), TABLE_OF_LONGS);
    }

    /**
     * Terminates a chain into a topic. The value serde is selected from the handle's RECORDED value type - there is
     * no per-operator special case, so a new typed operator needs a new serde branch here and nothing else.
     */
    public void sink(long handle, String topic) {
        requireNotBuilt("sink");
        require(topic != null && !topic.isEmpty(), "sink names no topic");
        Minted minted = handles.get(handle);
        if (minted == null) {
            throw unknownHandle(handle, "sink");
        }
        Serde<Object> valueSerde = serdeFor(minted.type().getValueType(), handle);
        switch (minted.type().getKind()) {
            case HANDLE_KIND_TABLE -> {
                // A count is a KTable, and a changelog: the sink carries every intermediate value per key, so a
                // reader of that topic must take the last value per key rather than summing what it sees.
                @SuppressWarnings("unchecked")
                KTable<byte[], Object> table = (KTable<byte[], Object>) minted.node();
                table.toStream().to(topic, Produced.with(Serdes.ByteArray(), valueSerde));
            }
            case HANDLE_KIND_STREAM -> {
                @SuppressWarnings("unchecked")
                KStream<byte[], Object> stream = (KStream<byte[], Object>) minted.node();
                stream.to(topic, Produced.with(Serdes.ByteArray(), valueSerde));
            }
            default -> throw new TopologyDescriptionException("sink cannot be applied to handle " + handle
                    + ": it names a " + kindName(minted.type().getKind())
                    + ", and sink needs a stream or a table");
        }
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
     * The serde a recorded value type writes with. Exhaustive with a refusing default: a type this engine has no
     * serde for is refused by name, never silently written as bytes - a wrong value entering a topic is worse than
     * a refused call. Package-visible so the refusal is testable without forging a mint path for a type no current
     * operator produces.
     */
    static Serde<Object> serdeFor(DataType valueType, long handle) {
        Serde<?> serde = switch (valueType) {
            case DATA_TYPE_BYTES -> Serdes.ByteArray();
            case DATA_TYPE_LONG -> Serdes.Long();
            default -> throw new TopologyDescriptionException("sink cannot write handle " + handle
                    + ": its value type " + typeName(valueType) + " has no serde in this engine");
        };
        @SuppressWarnings("unchecked")
        Serde<Object> cast = (Serde<Object>) serde;
        return cast;
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
