package bz.stub.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.streams.kstream.Aggregator;

import java.time.Duration;
import java.util.Objects;

/**
 * An {@link Aggregator} whose combining step runs in the host's language - the fourth foreign bridge.
 *
 * <p>Like {@link ForeignReducer} this computes the contents of engine-side state, but with the two differences
 * that make {@code aggregate} the windowed operator of choice. The key travels, because Kafka's {@code Aggregator}
 * receives one where its {@code Reducer} does not. And the host is called for a key's FIRST value in each window:
 * the engine supplies the initializer's bytes itself, so every invocation carries a real accumulator and no record
 * ever bypasses the host - the skip that makes a reduction's call count differ from its record count.
 */
public class ForeignAggregator implements Aggregator<byte[], byte[], byte[]> {

    private final InvocationRegistry registry;
    private final InvocationSink sink;
    private final long functionToken;
    private final Duration timeout;

    public ForeignAggregator(InvocationRegistry registry, InvocationSink sink, long functionToken, Duration timeout) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.sink = Objects.requireNonNull(sink, "sink");
        this.functionToken = functionToken;
        this.timeout = Objects.requireNonNull(timeout, "timeout");
    }

    @Override
    public byte[] apply(byte[] key, byte[] value, byte[] aggregate) {
        return registry.awaitResult(functionToken, ForeignCall.aggregate(key, value, aggregate), sink, timeout);
    }
}
