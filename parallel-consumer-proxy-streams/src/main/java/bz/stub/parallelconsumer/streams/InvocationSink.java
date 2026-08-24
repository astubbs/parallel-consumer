package bz.stub.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * Where an invocation leaves the engine for the host runtime.
 *
 * <p>Narrow on purpose. The registry and the mapper are the pieces whose concurrency has to be right, and neither
 * should need a gRPC stream to be tested - so the transport is one method behind this seam.
 */
public interface InvocationSink {

    /**
     * Hand one invocation to the host. Must not block on the host's answer; the answer arrives separately as a
     * result frame.
     *
     * <p>{@code aggregate} is null for a mapping and non-null for a reduction, which is the same distinction the
     * wire's optional field carries. Null rather than an empty array on a key's first value: Kafka does not call
     * the reducer at all for that record, so an empty aggregate would be a different and wrong thing to send.
     */
    void emit(long correlation, long functionToken, byte[] key, byte[] value, byte[] aggregate);
}
