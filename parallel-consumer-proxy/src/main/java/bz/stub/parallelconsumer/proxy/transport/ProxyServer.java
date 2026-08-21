package bz.stub.parallelconsumer.proxy.transport;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.epoll.Epoll;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.grpc.netty.shaded.io.netty.channel.unix.DomainSocketAddress;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
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

    /**
     * Set when this listener is a Unix domain socket rather than a loopback TCP port, in which case the
     * transport owns the socket file and deletes it on close.
     */
    private Path socketPath;

    private final boolean domainSocket;

    private EventLoopGroup bossGroup;

    private EventLoopGroup workerGroup;

    private ProxyServer(Builder builder) {
        this.domainSocket = builder.domainSocket;
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
        var builder = domainSocket
                ? domainSocketBuilder()
                : NettyServerBuilder.forAddress(new InetSocketAddress(bindAddress, requestedPort));
        server = builder
                .addService(ServerInterceptors.interceptForward(sessionService, authorityAllowlist, connectionGuard))
                .build()
                .start();
        if (domainSocket) {
            log.info("Proxy transport listening on Unix domain socket {}", socketPath);
        } else {
            log.info("Proxy transport listening on {}:{}", bindAddress.getHostAddress(), server.getPort());
        }
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
        if (domainSocket) {
            throw new IllegalStateException("this listener is a Unix domain socket, not a TCP port - "
                    + "ask for socketPath() instead");
        }
        return server.getPort();
    }

    /**
     * The Unix domain socket this listener bound, when it is one.
     * <p>
     * The path is chosen HERE and reported back, deliberately mirroring the ephemeral-port design above:
     * the parent process does not invent a path any more than it invents a port, so there is no shared
     * filesystem convention to collide on and no well-known location to guess.
     */
    public synchronized Path socketPath() {
        if (server == null) {
            throw new IllegalStateException("not started");
        }
        if (!domainSocket) {
            throw new IllegalStateException("this listener is a TCP port, not a Unix domain socket - "
                    + "ask for port() instead");
        }
        return socketPath;
    }

    /**
     * A domain-socket listener, which needs an epoll transport for both the channel and its event loops -
     * gRPC's default NIO groups cannot host an {@link EpollServerDomainSocketChannel}.
     *
     * <h2>What this does and does not change about the R17/R29 posture</h2>
     *
     * The loopback check above does not apply and is not being weakened: a filesystem socket is not a
     * network bind at all, so it is strictly narrower than the loopback default rather than wider - no
     * peer that lacks filesystem access to the path can reach it, and no remote peer can reach it by any
     * means. The authority allowlist is left in place unchanged rather than special-cased: the browser
     * threat R29 exists for cannot reach a Unix socket in the first place, so the interceptor is inert
     * here rather than bypassed, and a client that declares an authority is still held to the list.
     */
    private NettyServerBuilder domainSocketBuilder() throws IOException {
        if (!Epoll.isAvailable()) {
            throw new IOException("this platform has no epoll domain-socket transport, so the proxy "
                    + "cannot listen on a Unix domain socket here. grpc-netty-shaded bundles the Linux "
                    + "epoll natives and no kqueue transport, so this is expected on macOS - run under "
                    + "Linux, or in a container, or use the loopback listener", Epoll.unavailabilityCause());
        }
        // Created under a private directory rather than as a bare file, so the socket is unreachable to
        // anyone without a traversable path to it - the closest thing this listener has to the peer
        // identity KTD11 records loopback TCP as lacking entirely.
        Path directory = Files.createTempDirectory("pc-proxy-");
        socketPath = directory.resolve("proxy.sock");
        bossGroup = new EpollEventLoopGroup(1);
        workerGroup = new EpollEventLoopGroup();
        return NettyServerBuilder
                .forAddress(new DomainSocketAddress(socketPath.toFile()))
                .channelType(EpollServerDomainSocketChannel.class)
                .bossEventLoopGroup(bossGroup)
                .workerEventLoopGroup(workerGroup);
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
        // The event loop groups are ours only in domain-socket mode; gRPC owns its own defaults otherwise
        // and shutting those down would not be ours to do.
        if (bossGroup != null) {
            bossGroup.shutdownGracefully();
            bossGroup = null;
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
            workerGroup = null;
        }
        deleteSocketFile();
    }

    /** A socket file outlives the process that bound it, so this listener removes its own. */
    private void deleteSocketFile() {
        if (socketPath == null) {
            return;
        }
        try {
            Files.deleteIfExists(socketPath);
            Files.deleteIfExists(socketPath.getParent());
        } catch (IOException e) {
            log.warn("Could not remove the domain socket at {}", socketPath, e);
        }
        socketPath = null;
    }

    public static class Builder {
        private InetAddress bindAddress = InetAddress.getLoopbackAddress();
        private int port = 0;
        private BindableService sessionService;
        private final List<String> operatorAllowedAuthorities = new ArrayList<>();
        private boolean exposeUnauthenticatedSurfaceBeyondLoopback = false;

        private boolean domainSocket = false;

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

        /**
         * Listen on a Unix domain socket instead of a loopback TCP port, choosing the path here and
         * reporting it back through {@link ProxyServer#socketPath()} - the same shape as the ephemeral
         * port. Needs an epoll transport, so it fails with a named reason on a platform without one.
         */
        public Builder domainSocket(boolean domainSocket) {
            this.domainSocket = domainSocket;
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
