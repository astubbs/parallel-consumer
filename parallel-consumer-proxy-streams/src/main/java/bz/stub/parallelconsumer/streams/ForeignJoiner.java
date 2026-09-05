package bz.stub.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.streams.kstream.ValueJoiner;

import java.time.Duration;
import java.util.Objects;

/**
 * A {@link ValueJoiner} whose combining step runs in the host's language.
 *
 * <p>The third foreign shape, and the one that makes the topology a graph rather than a chain: two handles go in
 * and one comes out. A mapper transforms what passes through, a reducer folds a key's history together, and a
 * joiner brings a record together with the current table value for its key.
 *
 * <p>Takes two values exactly as a reducer does, which is precisely why the wire carries the kind explicitly.
 * Handing a joiner an aggregate, or a reducer a table value, would type-check on both sides and be wrong.
 */
public class ForeignJoiner implements ValueJoiner<byte[], byte[], byte[]> {

    private final InvocationRegistry registry;
    private final InvocationSink sink;
    private final long functionToken;
    private final Duration timeout;

    public ForeignJoiner(InvocationRegistry registry, InvocationSink sink, long functionToken, Duration timeout) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.sink = Objects.requireNonNull(sink, "sink");
        this.functionToken = functionToken;
        this.timeout = Objects.requireNonNull(timeout, "timeout");
    }

    @Override
    public byte[] apply(byte[] streamValue, byte[] tableValue) {
        return registry.awaitResult(functionToken, ForeignCall.join(streamValue, tableValue), sink, timeout);
    }
}
