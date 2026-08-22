package bz.stub.parallelconsumer.ffi;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.proxy.config.ConfigureHandler;
import bz.stub.parallelconsumer.proxy.protocol.v1.ClientMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyMessage;
import io.grpc.stub.StreamObserver;
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
 * The C surface for embedding Parallel Consumer in a foreign process.
 *
 * <h2>Why it mirrors the protocol rather than wrapping poll</h2>
 *
 * Core's entry point is {@code void poll(Consumer<PollContext<K,V>>)} - it takes a Java lambda, and
 * a lambda cannot cross a C ABI. Wrapping it would also mean the caller's function runs while
 * holding a core thread, which is the shape an FFI must avoid.
 *
 * <p>{@code rpc Session(stream ClientMessage) returns (stream ProxyMessage)} is already the right
 * shape. As a C ABI it is two queues: the host PULLS work frames and PUSHES verdict frames, on its
 * own threads, at its own pace. The bytes are the SAME protobuf frames the gRPC transport carries,
 * so eleven clients' encoding logic is reused rather than reinvented.
 *
 * <h2>The seam this is possible through</h2>
 *
 * {@link ConfigureHandler} is a gRPC service, but its session state machine is expressed purely as
 * {@code StreamObserver<ClientMessage>} in and {@code StreamObserver<ProxyMessage>} out.
 * {@code StreamObserver} is a three-method interface, so this class implements the outbound side
 * itself and calls {@link ConfigureHandler#session} directly. There is no server, no port and no
 * Netty on this path.
 *
 * <p>Nothing here calls back into Java from a foreign thread, which GraalVM does not fully support
 * (<a href="https://github.com/oracle/graal/issues/730">oracle/graal#730</a>). The host's threads
 * only ever enter Java through these entry points and return.
 */
public final class PcSession {

    private static final int OK = 0;
    private static final int ERR_NO_SESSION = -1;
    private static final int ERR_BUFFER_TOO_SMALL = -2;
    private static final int ERR_TIMEOUT = -3;
    private static final int ERR_SESSION_ENDED = -4;
    private static final int ERR_BAD_FRAME = -5;
    private static final int ERR_INTERNAL = -6;

    private static final ConcurrentHashMap<Long, Session> SESSIONS = new ConcurrentHashMap<>();
    private static final AtomicLong NEXT_HANDLE = new AtomicLong(1);

    private PcSession() {
    }

    /**
     * One session: the handler's inbound observer, and the frames it has produced for the host.
     *
     * <p>{@code ConfigureHandler.SessionObserver} documents that its state needs no locking
     * <em>because gRPC serialises a stream's inbound callbacks</em>. Nothing serialises a foreign
     * host's threads - Go may call {@code pc_send} from any goroutine - so that guarantee has to be
     * re-established here, which is what {@link #inboundLock} is for. Getting this wrong would not
     * fail loudly; it would corrupt the handshake state machine under concurrency.
     */
    private static final class Session {
        final BlockingQueue<byte[]> outbound = new LinkedBlockingQueue<>();
        final Object inboundLock = new Object();
        volatile StreamObserver<ClientMessage> inbound;
        volatile boolean ended;
        volatile String lastError;
    }

    /**
     * Opens a session and returns its handle, or a negative error code.
     *
     * <p>Takes no configuration: the host sends a {@code Configure} frame through {@link #send} as
     * its first message, exactly as a gRPC client does. Keeping the handshake in the frame stream
     * is what lets the existing clients drive this transport without changing how they encode.
     */
    @CEntryPoint(name = "pc_session_open")
    static long sessionOpen(IsolateThread thread) {
        try {
            Session session = new Session();
            ConfigureHandler handler = ConfigureHandler.builder().build();
            session.inbound = handler.session(new OutboundObserver(session));
            long handle = NEXT_HANDLE.getAndIncrement();
            SESSIONS.put(handle, session);
            return handle;
        } catch (Throwable failure) {
            return ERR_INTERNAL;
        }
    }

    /**
     * Pushes a frame in - a serialised {@code ClientMessage}, the same bytes the gRPC path sends.
     */
    @CEntryPoint(name = "pc_send")
    static int send(IsolateThread thread, long handle, CCharPointer frame, int length) {
        Session session = SESSIONS.get(handle);
        if (session == null) {
            return ERR_NO_SESSION;
        }
        if (session.ended) {
            return ERR_SESSION_ENDED;
        }
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = frame.read(i);
        }
        ClientMessage message;
        try {
            message = ClientMessage.parseFrom(bytes);
        } catch (Exception malformed) {
            session.lastError = "could not parse ClientMessage: " + malformed.getMessage();
            return ERR_BAD_FRAME;
        }
        try {
            synchronized (session.inboundLock) {
                session.inbound.onNext(message);
            }
            return OK;
        } catch (Throwable failure) {
            session.lastError = String.valueOf(failure);
            return ERR_INTERNAL;
        }
    }

    /**
     * Pulls the next frame out, blocking up to {@code timeoutMillis}. The host calls this from its
     * own thread - a goroutine, an asyncio task - so concurrency stays the host's to decide.
     *
     * <p>On {@link #ERR_BUFFER_TOO_SMALL} the frame is NOT consumed and {@code written} carries the
     * size needed, so the host can grow its buffer and ask again without losing work.
     */
    @CEntryPoint(name = "pc_next")
    static int next(IsolateThread thread, long handle, CCharPointer out, int capacity,
                    CIntPointer written, int timeoutMillis) {
        Session session = SESSIONS.get(handle);
        if (session == null) {
            return ERR_NO_SESSION;
        }
        try {
            byte[] frame = session.outbound.poll(timeoutMillis, TimeUnit.MILLISECONDS);
            if (frame == null) {
                // An ended session with a drained queue is terminal; an empty one is just idle, and
                // the host should ask again. Collapsing the two would turn a quiet moment into a
                // shutdown.
                return session.ended ? ERR_SESSION_ENDED : ERR_TIMEOUT;
            }
            written.write(frame.length);
            if (frame.length > capacity) {
                // Put it back at the HEAD so ordering survives the retry. LinkedBlockingQueue has
                // no addFirst, which is why the queue is drained and rebuilt rather than offered to.
                requeueAtHead(session, frame);
                return ERR_BUFFER_TOO_SMALL;
            }
            for (int i = 0; i < frame.length; i++) {
                out.write(i, frame[i]);
            }
            return OK;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return ERR_TIMEOUT;
        } catch (Throwable failure) {
            session.lastError = String.valueOf(failure);
            return ERR_INTERNAL;
        }
    }

    /** Ends the stream from the host's side and releases the handle. */
    @CEntryPoint(name = "pc_session_close")
    static int sessionClose(IsolateThread thread, long handle) {
        Session session = SESSIONS.remove(handle);
        if (session == null) {
            return ERR_NO_SESSION;
        }
        try {
            synchronized (session.inboundLock) {
                session.inbound.onCompleted();
            }
            return OK;
        } catch (Throwable failure) {
            return ERR_INTERNAL;
        }
    }

    /** The last error for this session, as UTF-8. Returns the length written, or a negative code. */
    @CEntryPoint(name = "pc_last_error")
    static int lastError(IsolateThread thread, long handle, CCharPointer out, int capacity) {
        Session session = SESSIONS.get(handle);
        if (session == null) {
            return ERR_NO_SESSION;
        }
        String message = session.lastError;
        byte[] bytes = (message == null ? "" : message).getBytes(StandardCharsets.UTF_8);
        if (bytes.length > capacity) {
            return ERR_BUFFER_TOO_SMALL;
        }
        for (int i = 0; i < bytes.length; i++) {
            out.write(i, bytes[i]);
        }
        return bytes.length;
    }

    private static void requeueAtHead(Session session, byte[] frame) {
        synchronized (session.outbound) {
            java.util.List<byte[]> rest = new java.util.ArrayList<>();
            session.outbound.drainTo(rest);
            session.outbound.add(frame);
            session.outbound.addAll(rest);
        }
    }

    /**
     * The outbound half of the bidirectional stream. Where gRPC would serialise a {@code
     * ProxyMessage} onto the wire, this serialises it into the queue the host pulls from - the same
     * bytes, one hop shorter.
     */
    private record OutboundObserver(Session session) implements StreamObserver<ProxyMessage> {

        @Override
        public void onNext(ProxyMessage message) {
            session.outbound.add(message.toByteArray());
        }

        @Override
        public void onError(Throwable failure) {
            session.lastError = String.valueOf(failure);
            session.ended = true;
        }

        @Override
        public void onCompleted() {
            session.ended = true;
        }
    }
}
