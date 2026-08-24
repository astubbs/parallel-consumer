package bz.stub.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.streams.kstream.Reducer;

import java.time.Duration;
import java.util.Objects;

/**
 * A {@link Reducer} whose combining step runs in the host's language.
 *
 * <p>This is the first operator whose <em>state</em> crosses the boundary. A mapper sends a record out and takes a
 * value back; a reducer sends the STORED aggregate out alongside the new value, and what comes back is written to
 * the store as the new aggregate. So the host is not merely transforming records in passing - it is computing the
 * contents of engine-side state.
 *
 * <p>Kafka never calls a reducer for a key's first value: that value becomes the aggregate untouched. So every
 * invocation this class makes has a real prior aggregate, and the wire's aggregate field is never a stand-in for
 * "there wasn't one".
 */
public class ForeignReducer implements Reducer<byte[]> {

    private final InvocationRegistry registry;
    private final InvocationSink sink;
    private final long functionToken;
    private final Duration timeout;

    public ForeignReducer(InvocationRegistry registry, InvocationSink sink, long functionToken, Duration timeout) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.sink = Objects.requireNonNull(sink, "sink");
        this.functionToken = functionToken;
        this.timeout = Objects.requireNonNull(timeout, "timeout");
    }

    @Override
    public byte[] apply(byte[] aggregate, byte[] value) {
        // No key: Kafka's Reducer does not receive one, and sending a richer signature than the framework's would
        // put the host's function out of step with the operator it is standing in for.
        return registry.awaitResult(functionToken, null, value, aggregate, sink, timeout);
    }
}
