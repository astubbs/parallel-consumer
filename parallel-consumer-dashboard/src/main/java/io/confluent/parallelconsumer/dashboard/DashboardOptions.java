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
 * </ul>
 * <p>
 * The scheme is not among them: it is fixed at {@value #SCHEME}. See {@link #SCHEME}.
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
     * The one scheme this server speaks. Named rather than inlined because three places have to agree on it - the
     * startup URL, the {@code /status} page, and the {@code Origin} comparison in
     * {@code HostAllowlist} - and a same-origin check that disagreed with the scheme actually served would reject
     * the page's own requests.
     * <p>
     * There is deliberately no HTTPS option. It was built and then removed before release, because on the loopback
     * bind this module is for it buys a browser interstitial rather than confidentiality. It is deferred rather than
     * refused - plan requirement R50, under Scope Boundaries, records what reviving it actually costs on a modern
     * JDK, which is more than it looks.
     */
    public static final String SCHEME = "http";

    /**
     * How many consecutive ports the search will try before giving up. Generous enough that a developer machine
     * running a dozen instances never hits it, bounded so a pathological environment fails with a message instead of
     * spinning through 65535 sockets.
     */
    public static final int DEFAULT_MAX_PORT_ATTEMPTS = 100;

    /**
     * The fastest the event stream will send: at most one event per this interval per client, and what the server
     * advertises to clients as the expected update cadence.
     * <p>
     * <strong>This is a ceiling, not a period.</strong> Nothing ticks at it. Snapshots are pushed to
     * {@code StreamRoute} as they are published, and this only bounds how often they may go out; samples published
     * inside a window are coalesced, so the client sees the newest rather than every one. It never causes Parallel
     * Consumer to be sampled more often (plan R19) - sampling already happens on every control loop pass, and this
     * governs transmission alone.
     * <p>
     * <strong>Why 100ms.</strong> It was measured, not guessed. Driving a partition whose spans move on a known
     * smooth curve and sampling the rendered marker positions on every animation frame, the page's motion is
     * indistinguishable at 50ms, 100ms, 200ms and 300ms - zero frozen frames and no visible pause in any of them -
     * and degrades at 500ms (a pause every 3.5 seconds) and badly at the 1s this used to be (pauses up to 204ms,
     * with velocity jumping by up to 17x at each keyframe). So anything at or below 300ms buys the same picture, and
     * the number should be chosen on other grounds:
     * <ul>
     *   <li>100ms is the threshold at which a change reads as immediate rather than as a delayed reaction, and this
     *       page exists to watch a race being won in real time.</li>
     *   <li>It leaves 3x headroom to the 300ms knee, so a loaded machine - where both the event loop's timers and
     *       the browser's frame pacing are worse than on an idle one - still renders inside the smooth region.</li>
     *   <li>It gives the page's interpolator about six animation frames per keyframe, which is enough for tweening
     *       to be doing real work rather than rounding.</li>
     *   <li>Going faster costs bandwidth linearly - 50ms is twice the traffic for no measurable difference.</li>
     * </ul>
     */
    public static final Duration DEFAULT_UPDATE_INTERVAL = Duration.ofMillis(100);

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
     * The maximum rate the event stream sends at - at most one event per this interval - and the cadence the server
     * advertises to clients. See {@link #DEFAULT_UPDATE_INTERVAL} for why it is a ceiling rather than a period, and
     * why the default is what it is.
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
                     boolean consumerGroupInfoEnabled) {
        this.bindAddress = bindAddress == null ? InetAddress.getLoopbackAddress() : bindAddress;
        this.port = port == 0 ? DEFAULT_PORT : port;
        this.maxPortAttempts = maxPortAttempts == 0 ? DEFAULT_MAX_PORT_ATTEMPTS : maxPortAttempts;
        this.extraAllowedHosts = normaliseHosts(extraAllowedHosts);
        this.updateInterval = updateInterval == null ? DEFAULT_UPDATE_INTERVAL : updateInterval;
        this.maxConcurrentStreams = maxConcurrentStreams == 0 ? DEFAULT_MAX_CONCURRENT_STREAMS : maxConcurrentStreams;
        this.streamIdleTimeout = streamIdleTimeout == null ? DEFAULT_STREAM_IDLE_TIMEOUT : streamIdleTimeout;
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
    }

    /**
     * The out-of-the-box configuration: loopback, port {@value #DEFAULT_PORT} walking upward, plain HTTP.
     */
    public static DashboardOptions defaults() {
        return builder().build();
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
        return SCHEME + "://" + getReachableHostLiteral() + ":" + boundPort + "/";
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
