package bz.stub.parallelconsumer.proxy.engine;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;

import java.time.Instant;

/**
 * The session's liveness lease (R46): one clock for the whole connection, not one per record.
 * <p>
 * <b>It proves the client is alive; it is not a processing deadline.</b> A worker whose function runs for
 * hours keeps its record as long as its admin heartbeats - "a record stays in flight until your function
 * returns; there is no clock" is what distinguishes this from Share Groups' acquisition-lock timeout, so
 * there is deliberately no per-record timer anywhere in this class. What a record carries is the deadline it
 * was <em>dispatched</em> with ({@link #deadlineAtDispatch()}); every heartbeat then moves the whole session's
 * deadline forward at once, and a record expires only when <b>both</b> have passed - which is what "the
 * heartbeat extends the lease of EVERY record currently dispatched" means without touching a single entry.
 * <p>
 * <b>Suspension is R46's precedence rule.</b> During a connection loss no heartbeat can arrive, so leases are
 * suspended and {@link ReconnectWindow} alone governs the held records; they resume on reconnect for the
 * records the manifest keeps. The lease and the window are alternatives in time, never concurrent clocks over
 * one record - without suspension every lease would expire inside the window and the manifest would have
 * nothing left to reconcile.
 *
 * @author Antony Stubbs
 * @see ReconnectWindow
 */
@Slf4j
class LivenessLease {

    private final LivenessSettings settings;

    /** The deadline the last heartbeat (or reconnect) set for every record of the session. */
    private volatile Instant sessionDeadline;

    /** True while a connection loss holds the records - see the class javadoc's precedence rule. */
    private volatile boolean suspended;

    LivenessLease(LivenessSettings settings) {
        this.settings = settings;
        this.sessionDeadline = enabled()
                ? settings.clock().instant().plus(settings.leaseDuration())
                : Instant.MAX;
    }

    /** Whether the session negotiated {@code heartbeat}; false means no lease may ever expire. */
    boolean enabled() {
        return settings.leasesEnabled();
    }

    boolean isSuspended() {
        return suspended;
    }

    /**
     * The lease a delivery is dispatched with. {@link Instant#MAX} when leases are disabled - a session
     * without the capability sends no heartbeats, so any finite deadline would expire records on a client
     * behaving exactly as the negotiation allows.
     */
    Instant deadlineAtDispatch() {
        return enabled() ? settings.clock().instant().plus(settings.leaseDuration()) : Instant.MAX;
    }

    /** Extends the lease of every record of this session at once - the connection-level {@code Heartbeat}. */
    void heartbeat() {
        if (!enabled()) {
            return;
        }
        sessionDeadline = settings.clock().instant().plus(settings.leaseDuration());
        log.trace("Session lease extended to {}", sessionDeadline);
    }

    /** Connection lost: the window takes over until a reconnect resumes this. */
    void suspend() {
        suspended = true;
    }

    /** Reconnected: the handshake is itself the first heartbeat of the resumed session. */
    void resume() {
        suspended = false;
        heartbeat();
    }

    /**
     * Whether a delivery dispatched with {@code deadlineAtDispatch} has outlived the session's lease - false
     * whenever the lease is disabled or suspended, so a caller never has to remember either rule.
     */
    boolean hasExpired(Instant deadlineAtDispatch) {
        if (!enabled() || suspended) {
            return false;
        }
        var effective = deadlineAtDispatch.isAfter(sessionDeadline) ? deadlineAtDispatch : sessionDeadline;
        return settings.clock().instant().isAfter(effective);
    }
}
