package bz.stub.parallelconsumer.proxy.transport;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * The proxy's gRPC listener: binds loopback only by default, on an ephemeral port, admits exactly one live
 * stream, and rejects an unlisted declared authority before the service method runs. U5 of the language-proxy
 * plan (astubbs#242); requirements R17, R18, R29, R41; decisions KTD3 and KTD11.
 * <p>
 * <b>The engine-facing boundary is {@link BindableService}.</b> The transport hosts a session service; it
 * does not know what one says. The engine (the {@code engine} package's {@code ProxyProcessor} work) supplies
 * its {@code ProxyServiceGrpc.ProxyServiceImplBase} implementation to {@link Builder#sessionService}, and the
 * transport wraps it with the authority allowlist and the single-connection guard - so every admission rule
 * has already run by the time the engine's {@code session(..)} method is invoked, and the engine can rely on
 * R41's one-connection invariant without enforcing it. That wiring is the connect-time configuration unit's
 * job (U7); this class deliberately compiles against no generated protocol type so the seam stays one-way.
 * <p>
 * <b>Bind posture (R17/R18).</b> The bind address is {@link InetAddress#getLoopbackAddress()} unless
 * configured otherwise, and the bind is {@link NettyServerBuilder#forAddress(java.net.SocketAddress)} with an
 * explicit address - never {@code ServerBuilder.forPort}, which binds the wildcard address. The port defaults
 * to ephemeral so no well-known port is guessable; {@link #port()} reports the chosen one, and the lifecycle
 * channel (U10's unit) is what carries it to the parent process. A non-loopback bind address refuses to start
 * unless {@link Builder#exposeUnauthenticatedSurfaceBeyondLoopback()} was called - the opt-in's name states
 * what it does, per R18 - and when it is present the server starts but warns with the surface's full recorded
 * capability: no authentication, offset advancement, and receipt of the Kafka credentials and a
 * class-instantiating property map (KTD11).
 */
@Slf4j
public class ProxyServer implements AutoCloseable {

    /**
     * The name of the R18 opt-in, as refusal messages must state it: without this setting, a non-loopback
     * bind address refuses to start.
     */
    public static final String NON_LOOPBACK_OPT_IN = "exposeUnauthenticatedSurfaceBeyondLoopback";

    private final InetAddress bindAddress;
    private final int requestedPort;
    private final BindableService sessionService;
    private final List<String> operatorAllowedAuthorities;
    private final boolean exposeUnauthenticatedSurfaceBeyondLoopback;

    private Server server;

    private ProxyServer(Builder builder) {
        this.bindAddress = builder.bindAddress;
        this.requestedPort = builder.port;
        this.sessionService = Objects.requireNonNull(builder.sessionService, "sessionService is required");
        this.operatorAllowedAuthorities = List.copyOf(builder.operatorAllowedAuthorities);
        this.exposeUnauthenticatedSurfaceBeyondLoopback = builder.exposeUnauthenticatedSurfaceBeyondLoopback;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Binds and starts serving.
     *
     * @throws IllegalStateException if the bind address is non-loopback and the R18 opt-in is absent
     */
    public synchronized ProxyServer start() throws IOException {
        if (server != null) {
            throw new IllegalStateException("already started");
        }
        if (!bindAddress.isLoopbackAddress()) {
            if (!exposeUnauthenticatedSurfaceBeyondLoopback) {
                throw new IllegalStateException("refusing to bind non-loopback address " + bindAddress
                        + ": the proxy binds loopback only by default (R17). To bind beyond loopback, set "
                        + "the opt-in '" + NON_LOOPBACK_OPT_IN + "' - its name states what it does: this "
                        + "listener has no authentication");
            }
            // R18's full statement, per KTD11: a warning naming only offsets would understate the surface.
            log.warn("Binding non-loopback address {} under the '{}' opt-in: this listener has NO "
                            + "AUTHENTICATION, and any peer that can reach the socket can advance the "
                            + "application's consumer offsets. The listener also receives the Kafka client "
                            + "credentials and a property map whose class-valued entries Kafka instantiates "
                            + "reflectively, which escalates to arbitrary class instantiation inside this JVM "
                            + "- and beyond loopback both travel the network in cleartext.",
                    bindAddress, NON_LOOPBACK_OPT_IN);
        }
        var authorityAllowlist = AuthorityAllowlistInterceptor
                .defaultAllowlist(bindAddress, operatorAllowedAuthorities);
        var connectionGuard = new SingleConnectionGuard();
        // interceptForward runs interceptors in listed order: the authority check first, so a rejected
        // authority is turned away with PERMISSION_DENIED before it can consume the admission slot.
        server = NettyServerBuilder
                .forAddress(new InetSocketAddress(bindAddress, requestedPort))
                .addService(ServerInterceptors.interceptForward(sessionService, authorityAllowlist, connectionGuard))
                .build()
                .start();
        log.info("Proxy transport listening on {}:{}", bindAddress.getHostAddress(), server.getPort());
        return this;
    }

    /**
     * The port actually bound - the ephemeral choice when the default port 0 was requested. This is the value
     * the lifecycle channel (U10) reports to the parent process.
     */
    public synchronized int port() {
        if (server == null) {
            throw new IllegalStateException("not started");
        }
        return server.getPort();
    }

    @Override
    public synchronized void close() {
        if (server == null) {
            return;
        }
        server.shutdownNow();
        try {
            server.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        server = null;
    }

    public static class Builder {
        private InetAddress bindAddress = InetAddress.getLoopbackAddress();
        private int port = 0;
        private BindableService sessionService;
        private final List<String> operatorAllowedAuthorities = new ArrayList<>();
        private boolean exposeUnauthenticatedSurfaceBeyondLoopback = false;

        /**
         * The session service the transport hosts - the engine's {@code ProxyServiceImplBase}
         * implementation. Required.
         */
        public Builder sessionService(BindableService sessionService) {
            this.sessionService = sessionService;
            return this;
        }

        /** Defaults to {@link InetAddress#getLoopbackAddress()}; anything else needs the R18 opt-in. */
        public Builder bindAddress(InetAddress bindAddress) {
            this.bindAddress = Objects.requireNonNull(bindAddress);
            return this;
        }

        /** Defaults to 0 - an ephemeral port, so no well-known port is guessable. */
        public Builder port(int port) {
            this.port = port;
            return this;
        }

        /** Operator additions to R29's default authority allowlist (loopback forms + bind address). */
        public Builder allowAuthorities(Collection<String> hostForms) {
            this.operatorAllowedAuthorities.addAll(hostForms);
            return this;
        }

        /**
         * The R18 opt-in: permit a non-loopback bind address, accepting that it exposes an unauthenticated
         * surface. Absent this call, {@link ProxyServer#start()} refuses a non-loopback bind and names this
         * setting in the refusal.
         */
        public Builder exposeUnauthenticatedSurfaceBeyondLoopback() {
            this.exposeUnauthenticatedSurfaceBeyondLoopback = true;
            return this;
        }

        public ProxyServer build() {
            return new ProxyServer(this);
        }
    }
}
