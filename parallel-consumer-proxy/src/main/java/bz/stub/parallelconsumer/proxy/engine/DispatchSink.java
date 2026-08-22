package bz.stub.parallelconsumer.proxy.engine;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.proxy.protocol.v1.Dispatch;
import bz.stub.parallelconsumer.proxy.protocol.v1.Report;

/**
 * The engine's half of the engine&harr;transport boundary: where assembled dispatch waves leave the engine.
 * <p>
 * The transport (the gRPC server of the language-proxy plan's U5) implements this to carry waves to the
 * connected client; the return half of the boundary is {@link ProxyProcessor#report(Report)}, which the
 * transport calls once per {@link Report} it receives. The engine is built against this pair only - it never
 * imports a transport class, and the wiring of the two sides is the plan's U7.
 * <p>
 * A wave is one multi-record {@link Dispatch} message - the frozen wire form (R50, KTD10): records the
 * assembler coalesced into one hand-off, ordered, and under restricted ordering drawn from distinct shards.
 * <p>
 * <b>Implementations must not throw.</b> This is called from the engine's control-loop thread; an exception
 * here fails the whole consumer fast, which is deliberate - records already registered in flight but never
 * carried would otherwise hang silently until the liveness machinery (the plan's U8) reclaims them.
 *
 * @author Antony Stubbs
 * @see ProxyProcessor
 */
@FunctionalInterface
public interface DispatchSink {

    /**
     * Carries one assembled wave to the far side. Never called with an empty wave.
     */
    void dispatch(Dispatch wave);
}
