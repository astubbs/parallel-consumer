package io.confluent.parallelconsumer.dashboard.server;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.dashboard.DashboardOptions;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.net.InetAddress;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Rejects requests whose {@code Host} header is not one this server answers to, and requests carrying a cross-origin
 * {@code Origin}. Runs before every route, including when the server is bound to loopback.
 *
 * <h2>Why this exists even on a loopback-bound server - do not remove it</h2>
 * <p>
 * Binding loopback stops another <em>machine</em> connecting. It does not stop another <em>web page</em>. In a DNS
 * rebinding attack the victim loads {@code attacker.example}, whose DNS record has a one-second TTL and is then
 * re-pointed at {@code 127.0.0.1}. The browser dutifully connects to the loopback server, and the page - still
 * running with the attacker's origin - reads the response. The connection came from loopback; nothing about the bind
 * address helped.
 * <p>
 * What breaks the attack is that the browser sends {@code Host: attacker.example}, because that is the name it
 * resolved. A server that only answers to names it recognises therefore answers nothing. This is not theoretical and
 * it is not old: <strong>CVE-2024-28224</strong> (Ollama) and <strong>CVE-2025-66414</strong> (the MCP TypeScript
 * SDK) are both exactly this, against exactly this shape of local unauthenticated HTTP service, and in both cases the
 * cited mitigation was a {@code Host} allowlist.
 *
 * <h2>No CORS headers, ever</h2>
 * <p>
 * This class rejects a cross-origin {@code Origin} and emits <em>no</em> {@code Access-Control-*} headers at all -
 * not even a restrictive one. There is nothing a cross-origin page is meant to be able to read here, so the correct
 * CORS policy is the absence of one: no header means the browser's default same-origin rule applies unmodified. A
 * server that emits an allow-list header is a server that has an opinion about which other origins may read it, and
 * this one does not.
 * <p>
 * Experimental: the dashboard module is opt-in and its API may change without notice.
 */
@InterfaceStability.Unstable
public final class HostAllowlist implements Handler<RoutingContext> {

    /**
     * Names that always denote this machine. Present regardless of what was bound, because a loopback-bound server
     * is reachable by all of them and a user typing {@code localhost} must not be told no.
     */
    private static final Set<String> LOOPBACK_NAMES = loopbackNames();

    private final Set<String> allowedHosts;

    private final Set<String> extraAllowedHosts;

    public HostAllowlist(DashboardOptions options) {
        Set<String> hosts = new LinkedHashSet<>(LOOPBACK_NAMES);
        InetAddress bind = options.getBindAddress();
        // a wildcard bind has no single name to add - the user must name the reachable hosts through
        // extraAllowedHosts, which is the honest outcome: the server cannot know what name a caller will use
        if (!bind.isAnyLocalAddress()) {
            hosts.add(strip(bind.getHostAddress()));
        }
        for (String extra : options.getExtraAllowedHosts()) {
            hosts.add(extra);
            // an entry may pin a port ("my-box:9000"); the name is also allowed on its own, because the port in a
            // Host header is not a boundary this server can enforce anything with - it listens on one port and a
            // request reached it or it did not. The NAME is the part DNS rebinding turns on, so the name is what is
            // matched, and a user who wrote the port they use is not then rejected when the browser omits a
            // default one.
            String hostOnly = hostOf(extra);
            if (hostOnly != null) {
                hosts.add(hostOnly);
            }
        }
        this.allowedHosts = Collections.unmodifiableSet(hosts);
        this.extraAllowedHosts = options.getExtraAllowedHosts();
    }

    /**
     * Every acceptable {@code Host} value, as configured. Exposed so the rejection message and the diagnostics page
     * can say what <em>would</em> have been accepted - a 403 that does not say what it wanted is a 403 the user
     * cannot act on.
     */
    public Set<String> getAllowedHosts() {
        return allowedHosts;
    }

    @Override
    public void handle(RoutingContext ctx) {
        // RFC 7230 s5.4: more than one Host field is a 400, unconditionally. It matters here for a second reason -
        // getHeader() returns the FIRST value, so a request carrying an allowed Host followed by an attacker's would
        // be checked against the one it was not going to be routed by if anything downstream read the last. A header
        // this check rests on must have exactly one value or the request is not answerable at all.
        if (isDuplicated(ctx, "Host")) {
            reject(ctx, 400, "The request carries more than one Host header. RFC 7230 s5.4 requires exactly one, and "
                    + "this dashboard's allowlist is checked against it, so an ambiguous authority is refused rather "
                    + "than resolved by guessing which one counts.");
            return;
        }
        if (isDuplicated(ctx, "Origin")) {
            reject(ctx, 400, "The request carries more than one Origin header. Exactly one origin can have made a "
                    + "request, so this is either a broken client or an attempt to have this server check one value "
                    + "and the browser enforce another.");
            return;
        }
        String host = ctx.request().getHeader("Host");
        if (!isAllowedHost(host)) {
            reject(ctx, "Host header " + describe(host) + " is not in this dashboard's allowlist " + allowedHosts
                    + ". This check runs even on a loopback bind, because binding loopback does not stop a DNS "
                    + "rebinding attack (CVE-2024-28224, CVE-2025-66414). If you are reaching the dashboard through "
                    + "a port-forward or a tunnel, add the name your browser uses to "
                    + "DashboardOptions.extraAllowedHosts.");
            return;
        }
        String origin = ctx.request().getHeader("Origin");
        if (origin != null && !isSameOrigin(origin, host)) {
            reject(ctx, "Origin " + describe(origin) + " is not this dashboard's own origin. The dashboard is "
                    + "read-only and serves no cross-origin consumer, so it emits no CORS headers and accepts no "
                    + "cross-origin request.");
            return;
        }
        ctx.next();
    }

    /**
     * Whether {@code hostHeader} - with or without a port, bracketed or not - names this server.
     * <p>
     * A missing header is a rejection rather than a pass: HTTP/1.1 requires it and HTTP/2 synthesises it from
     * {@code :authority}, so its absence means a hand-rolled client, and defaulting to "allow" here would make the
     * whole check bypassable by omission.
     */
    public boolean isAllowedHost(String hostHeader) {
        if (hostHeader == null) {
            return false;
        }
        String normalised = hostHeader.trim().toLowerCase(Locale.ROOT);
        if (normalised.isEmpty()) {
            return false;
        }
        // a user-supplied entry may pin a port ("my-box:9000"); match the whole authority first so that pinning
        // actually pins, then fall back to the host alone
        if (extraAllowedHosts.contains(normalised)) {
            return true;
        }
        String hostOnly = hostOf(normalised);
        return hostOnly != null && allowedHosts.contains(hostOnly);
    }

    /**
     * Whether {@code origin} is this server's own origin, given the {@code Host} the request claimed.
     * <p>
     * {@code Origin: null} - what a sandboxed iframe, a {@code file://} page or a redirected cross-origin request
     * sends - is <em>not</em> same-origin. It is an opaque origin, deliberately unattributable, and treating it as
     * absent would let exactly the pages this check exists to stop through.
     */
    public boolean isSameOrigin(String origin, String hostHeader) {
        if (origin == null) {
            // no Origin at all is what a same-origin navigation sends; the Host check has already run
            return true;
        }
        String normalised = origin.trim().toLowerCase(Locale.ROOT);
        int schemeEnd = normalised.indexOf("://");
        if (schemeEnd < 0) {
            return false;
        }
        String originScheme = normalised.substring(0, schemeEnd);
        String originAuthority = normalised.substring(schemeEnd + 3);
        if (originAuthority.indexOf('/') >= 0) {
            // an Origin is scheme + authority and nothing else; a path means this is not an Origin
            return false;
        }
        // the server speaks exactly one scheme, so anything else - https included - is a different origin
        if (!DashboardOptions.SCHEME.equals(originScheme)) {
            return false;
        }
        String host = hostHeader == null ? null : hostHeader.trim().toLowerCase(Locale.ROOT);
        return originAuthority.equals(host);
    }

    /**
     * The host portion of an authority, lower-cased, with brackets and any port removed; null if it is not a
     * well-formed authority.
     */
    static String hostOf(String authority) {
        String value = authority.trim().toLowerCase(Locale.ROOT);
        if (value.isEmpty()) {
            return null;
        }
        if (value.charAt(0) == '[') {
            int close = value.indexOf(']');
            if (close < 0) {
                return null;
            }
            String rest = value.substring(close + 1);
            if (!rest.isEmpty() && !rest.startsWith(":")) {
                return null;
            }
            return strip(value.substring(1, close));
        }
        int firstColon = value.indexOf(':');
        if (firstColon >= 0) {
            if (firstColon == value.lastIndexOf(':')) {
                value = value.substring(0, firstColon);
            }
            // More than one colon and no brackets is a bare IPv6 literal. RFC 7230 requires brackets, so this is
            // malformed - but some clients send it, and where it is unbracketed there is no way to tell a port from
            // another group. Returning it whole means it can still match an allowlist entry written the same way and
            // can never match anything else, which is the safe reading of an ambiguous input.
        }
        return strip(value);
    }

    /**
     * Removes an IPv6 scope id and the trailing dot of a fully-qualified name, both of which denote the same host as
     * the bare form. Without this, {@code localhost.} - which a browser will happily send - reads as a different name.
     *
     * <h2>The scope-id strip is confined to IP literals, and that confinement is the security property</h2>
     * <p>
     * A scope id is an IPv6 concept: it only ever appears on a link-local literal ({@code fe80::1%eth0}, RFC 6874).
     * Stripping at {@code %} unconditionally truncates a <em>registered name</em> too, and this method feeds the
     * allowlist comparison - so {@code Host: localhost%evil.example} would be compared as {@code localhost} and
     * accepted. That is a bypass of the exact control this class exists to be, and the exact attack it cites: an
     * attacker-chosen name that resolves to loopback, matching by truncation.
     * <p>
     * The guard is a colon, because an authority that reached here without one cannot be an IPv6 literal, and a
     * {@code %} in a registered name is either percent-encoding or an attempt at this. Both must be preserved so the
     * comparison sees the whole name and fails it.
     */
    private static String strip(String host) {
        String value = host;
        if (value.indexOf(':') >= 0) {
            int scope = value.indexOf('%');
            if (scope >= 0) {
                value = value.substring(0, scope);
            }
        }
        while (value.endsWith(".")) {
            value = value.substring(0, value.length() - 1);
        }
        return value.toLowerCase(Locale.ROOT);
    }

    private static boolean isDuplicated(RoutingContext ctx, String header) {
        return ctx.request().headers().getAll(header).size() > 1;
    }

    private static void reject(RoutingContext ctx, String reason) {
        reject(ctx, 403, reason);
    }

    private static void reject(RoutingContext ctx, int statusCode, String reason) {
        ctx.response()
                .setStatusCode(statusCode)
                .putHeader("Content-Type", "text/plain; charset=utf-8")
                .putHeader("X-Content-Type-Options", "nosniff")
                .end(statusCode + (statusCode == 400 ? " Bad Request. " : " Forbidden. ") + reason);
    }

    private static String describe(String value) {
        return value == null ? "(absent)" : "'" + value + "'";
    }

    private static Set<String> loopbackNames() {
        Set<String> names = new LinkedHashSet<>();
        names.add("localhost");
        names.add("127.0.0.1");
        names.add("::1");
        // the uncompressed IPv6 loopback form, which some clients send verbatim
        names.add("0:0:0:0:0:0:0:1");
        return Collections.unmodifiableSet(names);
    }
}
