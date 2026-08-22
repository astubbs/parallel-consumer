package bz.stub.parallelconsumer.ffi;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CIntPointer;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The C surface for embedding Parallel Consumer in a foreign process - the FFI ladder's Probe 0
 * shape, written to generate a real header rather than to work end to end yet.
 *
 * <h2>Why it is not a wrapper over ParallelStreamProcessor#poll</h2>
 *
 * Core's entry point is {@code void poll(Consumer<PollContext<K,V>>)} - it takes a Java lambda, and
 * a lambda cannot cross a C ABI. Wrapping it would also mean the caller's function runs while
 * holding a core thread, which is the shape an FFI must avoid; the ideation rejected exactly that.
 *
 * <h2>So it mirrors the protocol instead, which is already the right shape</h2>
 *
 * {@code rpc Session(stream ClientMessage) returns (stream ProxyMessage)} is a bidirectional stream.
 * As a C ABI that is two queues: the host PULLS work frames and PUSHES verdict frames, on its own
 * threads, at its own pace. The bytes are the SAME protobuf frames the gRPC transport carries, so
 * eleven clients' encoding logic is reused rather than reinvented - "frames as ABI".
 *
 * <p>It also dodges the one documented hole: nothing here calls back into Java from a foreign
 * thread, which GraalVM does not fully support (oracle/graal#730).
 */
public final class PcSession {

    private static final int OK = 0;
    private static final int ERR_NO_SESSION = -1;
    private static final int ERR_BUFFER_TOO_SMALL = -2;
    private static final int ERR_TIMEOUT = -3;
    private static final int ERR_NOT_IMPLEMENTED = -99;

    private static final ConcurrentHashMap<Long, BlockingQueue<byte[]>> OUTBOUND = new ConcurrentHashMap<>();
    private static final AtomicLong NEXT_HANDLE = new AtomicLong(1);

    /** Opens a session. `config` is a serialised Configure frame - the same one the gRPC path sends. */
    @CEntryPoint(name = "pc_session_open")
    static long sessionOpen(IsolateThread thread, CCharPointer config, int configLength) {
        long handle = NEXT_HANDLE.getAndIncrement();
        OUTBOUND.put(handle, new LinkedBlockingQueue<>());
        // Probe 0 stops here: the engine is not constructed yet. The signature is the deliverable.
        return handle;
    }

    /**
     * Pulls the next work frame, blocking up to {@code timeoutMillis}. The host calls this from its
     * own thread - a goroutine, an asyncio task - so concurrency stays the host's to decide.
     */
    @CEntryPoint(name = "pc_next")
    static int next(IsolateThread thread, long session, CCharPointer out, int capacity, CIntPointer written) {
        BlockingQueue<byte[]> queue = OUTBOUND.get(session);
        if (queue == null) {
            return ERR_NO_SESSION;
        }
        try {
            byte[] frame = queue.poll(1, TimeUnit.MILLISECONDS);
            if (frame == null) {
                return ERR_TIMEOUT;
            }
            if (frame.length > capacity) {
                written.write(frame.length);
                return ERR_BUFFER_TOO_SMALL;
            }
            for (int i = 0; i < frame.length; i++) {
                out.write(i, frame[i]);
            }
            written.write(frame.length);
            return OK;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ERR_TIMEOUT;
        }
    }

    /** Pushes a verdict frame back - a serialised ClientMessage, the same one the gRPC path sends. */
    @CEntryPoint(name = "pc_send")
    static int send(IsolateThread thread, long session, CCharPointer frame, int length) {
        if (!OUTBOUND.containsKey(session)) {
            return ERR_NO_SESSION;
        }
        return ERR_NOT_IMPLEMENTED;
    }

    /** Drains and closes, mirroring the sidecar's shutdown contract. */
    @CEntryPoint(name = "pc_session_close")
    static int sessionClose(IsolateThread thread, long session, int drainTimeoutMillis) {
        return OUTBOUND.remove(session) == null ? ERR_NO_SESSION : OK;
    }

    /** The last error for this session, as UTF-8. Returns the length written, or a negative code. */
    @CEntryPoint(name = "pc_last_error")
    static int lastError(IsolateThread thread, long session, CCharPointer out, int capacity) {
        byte[] message = "not implemented".getBytes(StandardCharsets.UTF_8);
        if (message.length > capacity) {
            return ERR_BUFFER_TOO_SMALL;
        }
        for (int i = 0; i < message.length; i++) {
            out.write(i, message[i]);
        }
        return message.length;
    }
}
