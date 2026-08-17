package bz.stub.parallelconsumer.proxy.transport;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import lombok.extern.slf4j.Slf4j;

import java.net.InetAddress;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Rejects any connection whose declared target authority is not in the allowlist - R29 of the language-proxy
 * plan (astubbs#242), enforced on every connection including loopback binds.
 * <p>
 * The threat is a browser page the operator happens to visit reaching the loopback listener cross-origin: a
 * browser always names the origin it thinks it is talking to in the {@code :authority} pseudo-header, so a
 * request that arrived via {@code evil.example.com} declares it. A well-behaved local client either declares
 * a loopback host form or declares nothing, which is why a connection declaring <b>no</b> authority is
 * accepted while one declaring an unlisted authority is rejected.
 * <p>
 * The mechanism is U1's cleared gate (KTD11): read {@link ServerCall#getAuthority()} in
 * {@code interceptCall}, close an unlisted authority with {@link Status#PERMISSION_DENIED} and return a
 * no-op listener - so the service method <b>never runs</b> for a rejected connection. The tests prove that
 * with counters (service invocations and application messages both unchanged across a rejection), not by
 * inspection, because a test asserting only the returned status can pass after the service method has
 * already run.
 * <p>
 * Matching is by host: the authority's port, IPv6 brackets and case are stripped before comparison, so an
 * allowlist entry {@code localhost} admits {@code localhost:41234}, and {@code ::1} admits {@code [::1]:80}.
 */
@Slf4j
public class AuthorityAllowlistInterceptor implements ServerInterceptor {

    /** Normalized (lowercase, portless, bracketless) host forms this listener will accept. */
    private final Set<String> allowedHosts;

    /**
     * @param allowedHostForms host forms to accept; each is normalized the same way declared authorities are
     */
    public AuthorityAllowlistInterceptor(Collection<String> allowedHostForms) {
        Set<String> normalized = new LinkedHashSet<>();
        for (var hostForm : allowedHostForms) {
            normalized.add(normalizeToHost(hostForm));
        }
        this.allowedHosts = Set.copyOf(normalized);
    }

    /**
     * The default allowlist R29 specifies: the loopback host forms plus the configured bind address, plus any
     * operator-configured additions.
     * <p>
     * Both textual forms of the IPv6 loopback are listed because {@link InetAddress#getHostAddress()} renders
     * it expanded ({@code 0:0:0:0:0:0:0:1}) while clients conventionally declare it compressed ({@code ::1}).
     */
    public static AuthorityAllowlistInterceptor defaultAllowlist(InetAddress bindAddress,
                                                                 Collection<String> operatorAdditions) {
        Set<String> hosts = new LinkedHashSet<>(Set.of("localhost", "127.0.0.1", "::1", "0:0:0:0:0:0:0:1"));
        hosts.add(bindAddress.getHostAddress());
        hosts.addAll(operatorAdditions);
        return new AuthorityAllowlistInterceptor(hosts);
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                                                                 Metadata headers,
                                                                 ServerCallHandler<ReqT, RespT> next) {
        String authority = call.getAuthority();
        if (authority == null) {
            // Accept-on-absent, per R29: the browser threat this interceptor exists for always declares an
            // authority, so absence is not evidence of it.
            return next.startCall(call, headers);
        }
        String host = normalizeToHost(authority);
        if (allowedHosts.contains(host)) {
            return next.startCall(call, headers);
        }
        log.warn("Rejecting connection declaring unlisted authority '{}' (normalized host '{}') - not in "
                + "the allowlist {} (R29)", authority, host, allowedHosts);
        call.close(Status.PERMISSION_DENIED
                        .withDescription("declared authority '" + authority + "' is not in the allowlist"),
                new Metadata());
        return new ServerCall.Listener<>() {
        };
    }

    /**
     * Reduces an authority to its host: lowercase, port stripped, IPv6 brackets stripped. Handles the four
     * shapes an authority takes - {@code host}, {@code host:port}, {@code [v6]}, {@code [v6]:port} - and
     * leaves a bare unbracketed IPv6 literal (more than one colon) intact rather than mistaking its last
     * segment for a port.
     */
    // DO NOT swap this for Guava's HostAndPort. It throws on a bare unbracketed IPv6 authority,
    // which this method deliberately admits - so the swap would narrow what the allowlist accepts,
    // which is a security-posture change wearing a library upgrade. Declined 2026-08-17; see
    // parallel-consumer-proxy/docs/simplifications-declined.md.
    static String normalizeToHost(String authority) {
        String a = authority.trim().toLowerCase(Locale.ROOT);
        if (a.startsWith("[")) {
            int closing = a.indexOf(']');
            if (closing > 0) {
                return a.substring(1, closing);
            }
            return a;
        }
        int firstColon = a.indexOf(':');
        if (firstColon >= 0 && a.indexOf(':', firstColon + 1) < 0) {
            return a.substring(0, firstColon);
        }
        return a;
    }
}
