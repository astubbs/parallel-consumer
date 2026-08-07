package io.confluent.parallelconsumer.dashboard;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Builder;
import lombok.Value;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Everything configurable about {@link DashboardServer}, with defaults chosen so the zero-configuration path is the
 * safe one.
 * <p>
 * <strong>Nothing here starts anything.</strong> Constructing this object, or having the module on the classpath, has
 * no effect whatsoever - the application has to call {@link DashboardServer#start()}. That is deliberate (plan R3): a
 * library that opens a socket because it was on the classpath is a library that opens a socket in somebody's
 * production process without being asked.
 *
 * <h2>The defaults, and why they are what they are</h2>
 * <ul>
 *   <li><strong>Bind address: {@link InetAddress#getLoopbackAddress()}.</strong> Correct for both IPv4-only and
 *       IPv6-only hosts and performs no DNS lookup. Deliberately <em>not</em> {@code InetAddress.getLocalHost()},
 *       which resolves the machine's hostname and is therefore sometimes loopback and sometimes a routable address
 *       depending on {@code /etc/hosts} - a default that is only usually safe is not a safe default.</li>
 *   <li><strong>Port: {@value #DEFAULT_PORT}, then walking upward.</strong> See
 *       {@link DashboardServer} for the search and why it is silent.</li>
 *   <li><strong>TLS: off.</strong> Turning it on with no supplied certificate means a self-signed one, which means a
 *       browser interstitial, which would spoil the zero-setup path this module exists for. On is one flag away.</li>
 * </ul>
 * <p>
 * Experimental: the dashboard module is opt-in and its API may change without notice.
 */
@InterfaceStability.Unstable
@Value
public class DashboardOptions {

    /**
     * The port the search starts from. 8080 because it is the port a human guesses first; if it is taken the server
     * walks upward rather than failing (plan R54).
     */
    public static final int DEFAULT_PORT = 8080;

    /**
     * How many consecutive ports the search will try before giving up. Generous enough that a developer machine
     * running a dozen instances never hits it, bounded so a pathological environment fails with a message instead of
     * spinning through 65535 sockets.
     */
    public static final int DEFAULT_MAX_PORT_ATTEMPTS = 100;

    /**
     * How often the server samples the published snapshot for the event stream, and what it advertises to clients as
     * the expected update cadence. This is a <em>read</em> cadence over an already-published value - it never causes
     * Parallel Consumer to be sampled more often (plan R19).
     */
    public static final Duration DEFAULT_UPDATE_INTERVAL = Duration.ofSeconds(1);

    /**
     * Concurrent event streams allowed. Not a resource constraint - Vert.x costs a registration, not a thread, per
     * stream - but a bound nobody reaches is cheap insurance against an accidental scrape loop opening streams
     * forever. Past the cap the server answers 503 with {@code Retry-After} so the client polls instead (KTD6).
     */
    public static final int DEFAULT_MAX_CONCURRENT_STREAMS = 64;

    /**
     * How long an event stream may go without making progress before the server closes it. Reaps a client that
     * vanished without closing its connection - the browser tab that was killed, the laptop lid that shut.
     */
    public static final Duration DEFAULT_STREAM_IDLE_TIMEOUT = Duration.ofSeconds(30);

    /**
     * The interface to bind. Never null. Defaults to the loopback address.
     */
    InetAddress bindAddress;

    /**
     * The first port to try. The server walks upward from here; see {@link #getMaxPortAttempts()}.
     */
    int port;

    /**
     * How many consecutive ports the port search may try, starting at {@link #getPort()}, before failing with a
     * message naming what it tried.
     */
    int maxPortAttempts;

    /**
     * Additional acceptable {@code Host} header values, beyond the loopback names and the bound address. This is the
     * escape hatch for port-forwarding workflows ({@code kubectl port-forward}, an SSH tunnel, a container name),
     * where the browser's idea of the host is legitimately not one the server can derive. Entries may carry a port
     * ({@code my-box:9000}) or not ({@code my-box}). Never null; unmodifiable; lower-cased.
     */
    Set<String> extraAllowedHosts;

    /**
     * How often the server re-reads the published snapshot, and the cadence it advertises to clients.
     */
    Duration updateInterval;

    /**
     * The concurrent event-stream cap. See {@link #DEFAULT_MAX_CONCURRENT_STREAMS}.
     */
    int maxConcurrentStreams;

    /**
     * How long a stream may fail to make progress before it is reaped.
     */
    Duration streamIdleTimeout;

    /**
     * Whether to serve HTTPS. Off by default (plan R50).
     */
    boolean tlsEnabled;

    /**
     * PEM certificate chain path. Null with {@link #isTlsEnabled()} true means "generate a self-signed certificate at
     * startup", which works with no preparation at the cost of a browser warning.
     */
    String tlsCertificatePath;

    /**
     * PEM private key path. Must be supplied if and only if {@link #getTlsCertificatePath()} is.
     */
    String tlsPrivateKeyPath;

    /**
     * Whether to query the broker for consumer-group context. Off by default: it is the one part of the dashboard
     * that talks to the broker rather than reading an in-process snapshot, so it is opt-in separately from the
     * dashboard itself.
     */
    boolean consumerGroupInfoEnabled;

    /**
     * Defaults are applied here rather than through {@code @Builder.Default} because the builder is on the
     * constructor - the same pattern the snapshot value types use. Validation is here too, so an impossible
     * configuration fails at build time with a message rather than at bind time with a stack trace.
     */
    @Builder(toBuilder = true)
    DashboardOptions(InetAddress bindAddress,
                     int port,
                     int maxPortAttempts,
                     Set<String> extraAllowedHosts,
                     Duration updateInterval,
                     int maxConcurrentStreams,
                     Duration streamIdleTimeout,
                     boolean tlsEnabled,
                     String tlsCertificatePath,
                     String tlsPrivateKeyPath,
                     boolean consumerGroupInfoEnabled) {
        this.bindAddress = bindAddress == null ? InetAddress.getLoopbackAddress() : bindAddress;
        this.port = port == 0 ? DEFAULT_PORT : port;
        this.maxPortAttempts = maxPortAttempts == 0 ? DEFAULT_MAX_PORT_ATTEMPTS : maxPortAttempts;
        this.extraAllowedHosts = normaliseHosts(extraAllowedHosts);
        this.updateInterval = updateInterval == null ? DEFAULT_UPDATE_INTERVAL : updateInterval;
        this.maxConcurrentStreams = maxConcurrentStreams == 0 ? DEFAULT_MAX_CONCURRENT_STREAMS : maxConcurrentStreams;
        this.streamIdleTimeout = streamIdleTimeout == null ? DEFAULT_STREAM_IDLE_TIMEOUT : streamIdleTimeout;
        this.tlsEnabled = tlsEnabled;
        this.tlsCertificatePath = tlsCertificatePath;
        this.tlsPrivateKeyPath = tlsPrivateKeyPath;
        this.consumerGroupInfoEnabled = consumerGroupInfoEnabled;

        if (this.port < 1 || this.port > 65535) {
            throw new IllegalArgumentException("Dashboard port must be between 1 and 65535, but was " + this.port);
        }
        if (this.maxPortAttempts < 1) {
            throw new IllegalArgumentException(
                    "Dashboard maxPortAttempts must be at least 1, but was " + this.maxPortAttempts);
        }
        if (this.updateInterval.toMillis() < 1) {
            throw new IllegalArgumentException(
                    "Dashboard updateInterval must be at least 1ms, but was " + this.updateInterval);
        }
        if (this.maxConcurrentStreams < 1) {
            throw new IllegalArgumentException(
                    "Dashboard maxConcurrentStreams must be at least 1, but was " + this.maxConcurrentStreams);
        }
        if (this.streamIdleTimeout.toMillis() < 1) {
            throw new IllegalArgumentException(
                    "Dashboard streamIdleTimeout must be at least 1ms, but was " + this.streamIdleTimeout);
        }
        // half a supplied pair is the failure that would otherwise surface as a self-signed certificate the user did
        // not ask for, which is worse than not starting: they would believe their own certificate was in use
        if ((this.tlsCertificatePath == null) != (this.tlsPrivateKeyPath == null)) {
            throw new IllegalArgumentException("Dashboard TLS needs both tlsCertificatePath and tlsPrivateKeyPath or "
                    + "neither (neither means a self-signed certificate is generated at startup), but got "
                    + "certificate=" + this.tlsCertificatePath + " key=" + this.tlsPrivateKeyPath);
        }
        if (!this.tlsEnabled && this.tlsCertificatePath != null) {
            throw new IllegalArgumentException("Dashboard TLS certificate paths were supplied but tlsEnabled is "
                    + "false, so they would be silently ignored - set tlsEnabled(true) or drop the paths");
        }
    }

    /**
     * The out-of-the-box configuration: loopback, port {@value #DEFAULT_PORT} walking upward, plain HTTP.
     */
    public static DashboardOptions defaults() {
        return builder().build();
    }

    /**
     * {@code https} when TLS is on, {@code http} otherwise. Used to build the startup URL and to decide which
     * {@code Origin} values can possibly be same-origin.
     */
    public String getScheme() {
        return tlsEnabled ? "https" : "http";
    }

    /**
     * Whether the bind address cannot be reached from another machine. False means
     * {@link DashboardServer#start()} logs the exposure warning (plan R23).
     */
    public boolean isLoopbackBind() {
        return bindAddress.isLoopbackAddress();
    }

    /**
     * The bind address as a literal suitable for a URL - bracketed if IPv6, with any scope id stripped, since a
     * scope id is meaningful to the local network stack and meaningless in a URL.
     */
    public String getBindHostLiteral() {
        return hostLiteral(bindAddress);
    }

    /**
     * The bind address in the form a socket wants: no brackets, no scope id. Netty rejects the bracketed URL form,
     * and a URL rejects the bare IPv6 form, so the two really are different strings and conflating them fails only
     * on IPv6 hosts - which is to say, on somebody else's machine.
     */
    public String getBindHostForSocket() {
        String literal = bindAddress.getHostAddress();
        int scope = literal.indexOf('%');
        return scope >= 0 ? literal.substring(0, scope) : literal;
    }

    /**
     * The host to put in the URL a human clicks. A wildcard bind ({@code 0.0.0.0} or {@code ::}) is not something a
     * browser can connect to, so the URL points at loopback while
     * {@link DashboardServer}'s warning names what was actually bound.
     */
    public String getReachableHostLiteral() {
        if (bindAddress.isAnyLocalAddress()) {
            return hostLiteral(InetAddress.getLoopbackAddress());
        }
        return getBindHostLiteral();
    }

    /**
     * The full URL for a bound port - what the startup line prints, and what a terminal linkifies.
     */
    public String url(int boundPort) {
        return getScheme() + "://" + getReachableHostLiteral() + ":" + boundPort + "/";
    }

    private static String hostLiteral(InetAddress address) {
        String literal = address.getHostAddress();
        int scope = literal.indexOf('%');
        if (scope >= 0) {
            literal = literal.substring(0, scope);
        }
        return address instanceof Inet6Address ? "[" + literal + "]" : literal;
    }

    private static Set<String> normaliseHosts(Set<String> hosts) {
        if (hosts == null || hosts.isEmpty()) {
            return Collections.emptySet();
        }
        // insertion-ordered so a diagnostic listing them reads the way the user wrote them
        Set<String> normalised = new LinkedHashSet<>();
        for (String host : hosts) {
            if (host == null) {
                continue;
            }
            String trimmed = host.trim().toLowerCase(Locale.ROOT);
            if (!trimmed.isEmpty()) {
                normalised.add(trimmed);
            }
        }
        return Collections.unmodifiableSet(normalised);
    }
}
