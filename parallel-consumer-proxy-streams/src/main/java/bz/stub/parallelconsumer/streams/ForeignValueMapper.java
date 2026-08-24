package bz.stub.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.streams.kstream.ValueMapperWithKey;

import java.time.Duration;
import java.util.Objects;

/**
 * The operator that calls the host, and the only place a record leaves the engine.
 *
 * <p>A Kafka Streams operator is synchronous - it must return the mapped value inline - so this blocks the stream
 * thread for the whole round trip. That is a property of Streams rather than of this design, and it is why
 * concurrency here is bounded by the number of stream threads rather than by key count.
 *
 * <p>Nothing calls back into the host runtime: the host pulls the invocation and pushes the result on its own
 * threads, and this only waits.
 */
public class ForeignValueMapper implements ValueMapperWithKey<byte[], byte[], byte[]> {

    private final InvocationRegistry registry;
    private final InvocationSink sink;
    private final long functionToken;
    private final Duration timeout;

    public ForeignValueMapper(InvocationRegistry registry, InvocationSink sink, long functionToken, Duration timeout) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.sink = Objects.requireNonNull(sink, "sink");
        this.functionToken = functionToken;
        this.timeout = Objects.requireNonNull(timeout, "timeout");
    }

    @Override
    public byte[] apply(byte[] key, byte[] value) {
        return registry.awaitResult(functionToken, ForeignCall.map(key, value), sink, timeout);
    }
}
