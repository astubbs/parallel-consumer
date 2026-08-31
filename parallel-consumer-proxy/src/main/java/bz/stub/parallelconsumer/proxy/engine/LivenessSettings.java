package bz.stub.parallelconsumer.proxy.engine;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.TimeUtils;

import java.time.Clock;
import java.time.Duration;

/**
 * The three liveness numbers of a session, plus the clock they are measured against: the lease, the interval
 * the client's admin heartbeats at, and the reconnect window that governs held records while the connection is
 * down. U8 of the language-proxy plan (astubbs#242); requirements R42, R44, R46.
 * <p>
 * <b>{@code leasesEnabled} is the {@code heartbeat} capability, carried here.</b> A client that did not
 * negotiate it sends no heartbeats, so no lease may expire on it - the specification's "no lease machinery
 * runs", made structural rather than remembered at each expiry check.
 * <p>
 * <b>The defaults, and where they come from.</b> {@link #DEFAULT_RECONNECT_WINDOW} is ASM6's number. The other
 * two the plan does not state - "the lease duration and heartbeat interval have no defaults or stated
 * derivation" is an open question in
 * {@code docs/plans/2026-08-14-001-feat-language-proxy-plan.md} ("Numbers and names the freeze needs that
 * nothing states"), and this unit cannot implement two clocks without numbering them. So: a one minute lease,
 * heartbeated at a third of it, the ordinary 3-to-1 ratio that lets two heartbeats be lost to a GC pause or a
 * scheduling hiccup before a live client is declared gone. Both travel in {@code Configured}, so a client
 * reads what it got rather than assuming these; a session may name its own in {@code Configure}.
 *
 * @param leasesEnabled     whether the session negotiated {@code heartbeat} - false disables every lease clock
 * @param leaseDuration     how long a delivery's lease survives without a heartbeat. <b>Not a processing
 *                          deadline</b> (R46): it bounds the client's silence, never the worker's function
 * @param heartbeatInterval how often the client's admin must heartbeat; the proxy only echoes it
 * @param reconnectWindow   how long records are held after connection loss before returning to scheduling (R42)
 * @param clock             the clock every deadline here is measured against; injected so tests advance time
 *                          rather than sleeping through it
 * <b>A plain final class rather than a record, and that is a build fact rather than a style choice</b> - the
 * same one astubbs/parallel-consumer#387 hit one rung down, and the reason this module has no Java record
 * anywhere. Jabel REQUIRES {@code @Desugar} on every record it sees, even at release 17, and what the
 * annotation then does is rewrite the record into a class whose generated members carry no source positions.
 * Error Prone cannot read that: 2.42.0 crashes rather than reporting, first in {@code UnnecessaryStringBuilder}
 * ({@code invalid replacement: [0, -1)}) and, once that is suppressed, again in {@code DuplicateBranches}. A
 * crash is not a finding - it fails the whole compilation and is attributed to line 1 of whichever file javac
 * listed first, which is why it reads as unrelated to the file that caused it. Neither term can move: the root
 * pom pins Error Prone at 2.42.0 because 2.43.0 needs a JVM this build cannot use, and Jabel serves the
 * release 8 target. {@code grep -rn "record "} over the tree finds none.
 *
 * @author Antony Stubbs
 */
public final class LivenessSettings {

    private final boolean leasesEnabled;

    private final Duration leaseDuration;

    private final Duration heartbeatInterval;

    private final Duration reconnectWindow;

    private final Clock clock;

    public LivenessSettings(boolean leasesEnabled, Duration leaseDuration, Duration heartbeatInterval,
                            Duration reconnectWindow, Clock clock) {
        this.leasesEnabled = leasesEnabled;
        this.leaseDuration = leaseDuration;
        this.heartbeatInterval = heartbeatInterval;
        this.reconnectWindow = reconnectWindow;
        this.clock = clock;
    }

    public boolean leasesEnabled() {
        return leasesEnabled;
    }

    public Duration leaseDuration() {
        return leaseDuration;
    }

    public Duration heartbeatInterval() {
        return heartbeatInterval;
    }

    public Duration reconnectWindow() {
        return reconnectWindow;
    }

    public Clock clock() {
        return clock;
    }

    /** One minute of client silence before a delivery's lease lapses - see the class javadoc's derivation. */
    public static final Duration DEFAULT_LEASE_DURATION = Duration.ofSeconds(60);

    /** A third of the lease: two heartbeats may be lost before a live client is declared gone. */
    public static final Duration DEFAULT_HEARTBEAT_INTERVAL = Duration.ofSeconds(20);

    /** ASM6's number: the bounded window after connection loss during which records are held, not returned. */
    public static final Duration DEFAULT_RECONNECT_WINDOW = Duration.ofSeconds(30);

    /** The defaults on the production clock, with leases enabled - what a session negotiating everything gets. */
    public static LivenessSettings defaults() {
        return new LivenessSettings(true, DEFAULT_LEASE_DURATION, DEFAULT_HEARTBEAT_INTERVAL,
                DEFAULT_RECONNECT_WINDOW, TimeUtils.getClock());
    }

    /**
     * The defaults on the given clock, with leases enabled or not - the constructor tests and
     * {@code ConfigureHandler} reach for, the capability decided by the caller.
     */
    public static LivenessSettings defaults(boolean leasesEnabled, Clock clock) {
        return new LivenessSettings(leasesEnabled, DEFAULT_LEASE_DURATION, DEFAULT_HEARTBEAT_INTERVAL,
                DEFAULT_RECONNECT_WINDOW, clock);
    }
}
