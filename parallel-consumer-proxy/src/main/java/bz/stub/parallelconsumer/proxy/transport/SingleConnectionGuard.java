package bz.stub.parallelconsumer.proxy.transport;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import lombok.extern.slf4j.Slf4j;

import java.util.OptionalLong;

/**
 * Admits exactly one live stream at a time - the single-connection invariant of R41 (see the language-proxy
 * plan, astubbs#242).
 * <p>
 * The proxy serves one client connection: fan-out to workers happens inside the client library (KTD3), so a
 * second concurrent stream is never legitimate and is <b>rejected, not adopted in place of the first</b>.
 * Rejecting rather than replacing is what makes the invariant something the engine can rely on - the engine
 * never observes its peer silently swapped mid-session. Reconnection after connection loss is U8's concern
 * and re-uses this same admission slot: the slot is released when the admitted stream terminates, so the next
 * stream to arrive may claim it.
 * <p>
 * <b>The admission slot is a two-state machine with a generation counter.</b> The slot is either
 * {@code HELD} (a stream owns it) or {@code RELEASED}. Each successful acquisition increments the generation
 * and hands the new value back as the holder's ticket; release requires the ticket. The generation exists
 * because release is driven by stream-termination callbacks, which are asynchronous: a <em>late</em>
 * termination callback from the stream of generation N must not release the slot once generation N+1 holds
 * it, or a stale disconnect would open the door to a second concurrent stream. {@link #release(long)} is
 * therefore a no-op unless the ticket names the current generation.
 * <p>
 * A rejected stream is closed with {@link Status#RESOURCE_EXHAUSTED} - the slot is a quota of one, and the
 * condition is retryable once the holder terminates. {@code PERMISSION_DENIED} is deliberately left to
 * {@link AuthorityAllowlistInterceptor}, so a client can tell "you are not allowed" from "the slot is taken"
 * by status code alone.
 * <p>
 * As with the authority check, rejection happens in {@code interceptCall} by closing the call and returning a
 * no-op listener, so the service method never runs for a rejected stream - proven by the service-invocation
 * counter in this class's tests, not by inspection.
 */
@Slf4j
public class SingleConnectionGuard implements ServerInterceptor {

    /** Whether the slot is currently held. Guarded by {@code this}. */
    private boolean held = false;

    /**
     * Incremented on every successful acquisition; the current value is the live holder's ticket.
     * Guarded by {@code this}.
     */
    private long generation = 0;

    /**
     * Claims the admission slot if it is released.
     *
     * @return the new generation as the holder's ticket, or empty if the slot is already held
     */
    synchronized OptionalLong tryAcquire() {
        if (held) {
            return OptionalLong.empty();
        }
        held = true;
        generation++;
        return OptionalLong.of(generation);
    }

    /**
     * Releases the slot, but only if {@code ticket} names the current generation - a late termination
     * callback from a superseded stream must not release the slot out from under the live holder.
     *
     * @return true if this call released the slot; false if the ticket was stale and nothing changed
     */
    synchronized boolean release(long ticket) {
        if (held && generation == ticket) {
            held = false;
            return true;
        }
        return false;
    }

    /** Whether a stream currently holds the admission slot. */
    synchronized boolean isHeld() {
        return held;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                                                                 Metadata headers,
                                                                 ServerCallHandler<ReqT, RespT> next) {
        OptionalLong ticket = tryAcquire();
        if (ticket.isEmpty()) {
            log.warn("Rejecting a second concurrent stream: the admission slot is held (R41 - the proxy "
                    + "serves exactly one client connection; fan-out belongs inside the client library)");
            call.close(Status.RESOURCE_EXHAUSTED
                    .withDescription("the proxy serves exactly one client connection at a time and its "
                            + "admission slot is held; retry after the current connection terminates"),
                    new Metadata());
            return new ServerCall.Listener<>() {
            };
        }
        long heldTicket = ticket.getAsLong();
        ServerCall.Listener<ReqT> delegate;
        try {
            delegate = next.startCall(call, headers);
        } catch (RuntimeException | Error e) {
            release(heldTicket);
            throw e;
        }
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(delegate) {
            @Override
            public void onComplete() {
                try {
                    super.onComplete();
                } finally {
                    release(heldTicket);
                }
            }

            @Override
            public void onCancel() {
                try {
                    super.onCancel();
                } finally {
                    release(heldTicket);
                }
            }
        };
    }
}
