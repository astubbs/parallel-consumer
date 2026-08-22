package bz.stub.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

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
 * <p>Five methods, deliberately. A fixed set keeps an argument type system out of the proof; a sixth method taking a
 * typed argument is the increment that tests whether the wire generalises.
 *
 * <p>Not thread-safe: a session describes its topology from one stream, in order.
 */
public class TopologyAssembler {

    /** Resolves a host function token to the operator that calls it. */
    @FunctionalInterface
    public interface MapperFactory {
        ValueMapperWithKey<byte[], byte[], byte[]> forToken(long functionToken);
    }

    private final StreamsBuilder builder = new StreamsBuilder();
    private final Map<Long, Object> handles = new HashMap<>();
    private final AtomicLong nextHandle = new AtomicLong(1);
    private final MapperFactory mappers;
    private boolean built;

    public TopologyAssembler(MapperFactory mappers) {
        this.mappers = mappers;
    }

    public long source(String topic) {
        requireNotBuilt("source");
        require(topic != null && !topic.isEmpty(), "source names no topic");
        return mint(builder.stream(topic, Consumed.with(Serdes.ByteArray(), Serdes.ByteArray())));
    }

    public long mapValues(long handle, long functionToken) {
        requireNotBuilt("mapValues");
        KStream<byte[], byte[]> upstream = resolve(handle, KStream.class, "mapValues");
        return mint(upstream.mapValues(mappers.forToken(functionToken)));
    }

    public long groupByKey(long handle) {
        requireNotBuilt("groupByKey");
        KStream<byte[], byte[]> upstream = resolve(handle, KStream.class, "groupByKey");
        return mint(upstream.groupByKey(Grouped.with(Serdes.ByteArray(), Serdes.ByteArray())));
    }

    /**
     * The aggregation, over an IN-MEMORY store.
     *
     * <p>Not RocksDB, and not as a convenience: RocksDB is a JNI-backed native library whose per-platform problems
     * under a native image are documented and land on this project's own platform. An in-memory store exercises
     * state, the changelog and commits while keeping the native-image question independent of this proof.
     */
    public long count(long handle, String storeName) {
        requireNotBuilt("count");
        require(storeName != null && !storeName.isEmpty(), "count names no store");
        KGroupedStream<byte[], byte[]> upstream = resolve(handle, KGroupedStream.class, "count");
        return mint(upstream.count(Materialized.<byte[], Long>as(Stores.inMemoryKeyValueStore(storeName))
                .withKeySerde(Serdes.ByteArray())
                .withValueSerde(Serdes.Long())));
    }

    public void sink(long handle, String topic) {
        requireNotBuilt("sink");
        require(topic != null && !topic.isEmpty(), "sink names no topic");
        Object upstream = handles.get(handle);
        if (upstream instanceof KTable<?, ?> table) {
            // A count is a KTable, and a changelog: the sink carries every intermediate value per key, so a reader
            // of that topic must take the last value per key rather than summing what it sees.
            @SuppressWarnings("unchecked")
            KTable<byte[], Long> counts = (KTable<byte[], Long>) table;
            counts.toStream().to(topic, Produced.with(Serdes.ByteArray(), Serdes.Long()));
            return;
        }
        if (upstream instanceof KStream<?, ?> stream) {
            @SuppressWarnings("unchecked")
            KStream<byte[], byte[]> records = (KStream<byte[], byte[]>) stream;
            records.to(topic, Produced.with(Serdes.ByteArray(), Serdes.ByteArray()));
            return;
        }
        throw unknownHandle(handle, "sink");
    }

    /** Builds the topology. The host's description ends here; a session describes once. */
    public Topology build() {
        requireNotBuilt("build");
        built = true;
        return builder.build();
    }

    private long mint(Object node) {
        long handle = nextHandle.getAndIncrement();
        handles.put(handle, node);
        return handle;
    }

    private <T> T resolve(long handle, Class<T> expected, String method) {
        Object node = handles.get(handle);
        if (node == null) {
            throw unknownHandle(handle, method);
        }
        if (!expected.isInstance(node)) {
            throw new TopologyDescriptionException(method + " cannot be applied to handle " + handle
                    + ": it names a " + node.getClass().getSimpleName()
                    + ", and " + method + " needs a " + expected.getSimpleName());
        }
        return expected.cast(node);
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
