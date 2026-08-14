package bz.stub.parallelconsumer.proxy.engine;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;

import java.time.Instant;

/**
 * The bounded protection window that governs a connection's records after it drops (R42, R44).
 * <p>
 * <b>Losing the connection does not return the records.</b> Returning them immediately is what produces the
 * two-workers-one-key gap: the proxy's books balance while the original worker is still alive and finishing.
 * So the window holds them, and only its expiry returns them - with their attempt counts unchanged, because
 * no verdict was ever reached.
 * <p>
 * <b>Three states, and the third is why this is not a boolean.</b> {@code CONNECTED} is the ordinary case;
 * {@code HOLDING} runs from connection loss to the deadline; {@code SPENT} is what the window becomes once it
 * has expired and its records have been returned. The distinction matters because the client may stay away
 * indefinitely: expiry must return the held set <b>once</b>, not re-arm a sweep that would spin the engine
 * through dispatch-and-abandon for as long as nobody is connected. After it is spent, records the engine
 * keeps dispatching to a stream that is not there simply fill the in-flight ceiling and stop - core's own
 * backpressure is the damper - and they are returned as unmanifested when a client finally reconnects.
 *
 * @author Antony Stubbs
 * @see LivenessLease
 */
@Slf4j
class ReconnectWindow {

    private enum State {
        CONNECTED, HOLDING, SPENT
    }

    private final LivenessSettings settings;

    private State state = State.CONNECTED;

    /** When the holding period ends; meaningful only in {@link State#HOLDING}. */
    private Instant closesAt;

    ReconnectWindow(LivenessSettings settings) {
        this.settings = settings;
    }

    /** The connection dropped: start holding this connection's records. Idempotent. */
    synchronized void open() {
        if (state == State.HOLDING) {
            return;
        }
        state = State.HOLDING;
        closesAt = settings.clock().instant().plus(settings.reconnectWindow());
        log.info("Connection lost: holding this session's in-flight records until {} (R42), leases suspended",
                closesAt);
    }

    /** A client reconnected: the window stops governing and the lease resumes. */
    synchronized void close() {
        state = State.CONNECTED;
        closesAt = null;
    }

    /** Whether the window is currently governing the held records - R46's precedence over the lease. */
    synchronized boolean isHolding() {
        return state == State.HOLDING;
    }

    /**
     * Whether the window has just expired, transitioning it to spent. True <b>once</b> per opening: the caller
     * returns the held records on that one true, and a client that never comes back does not keep the engine
     * sweeping.
     */
    synchronized boolean expireIfDue() {
        if (state != State.HOLDING || settings.clock().instant().isBefore(closesAt)) {
            return false;
        }
        state = State.SPENT;
        closesAt = null;
        return true;
    }
}
