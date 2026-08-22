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
     */
    void emit(long correlation, long functionToken, byte[] key, byte[] value);
}
