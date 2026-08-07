package io.confluent.parallelconsumer.dashboard;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.dashboard.server.HostAllowlist;
import io.confluent.parallelconsumer.dashboard.server.StateRoute;
import io.confluent.parallelconsumer.dashboard.server.StatusRoute;
import io.confluent.parallelconsumer.dashboard.server.StreamRoute;
import io.confluent.parallelconsumer.dashboard.snapshot.SnapshotPublisher;
import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.file.FileSystemOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.StaticHandler;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.BindException;
import java.net.SocketException;
import java.net.URLDecoder;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The embedded, opt-in, read-only web dashboard for a running Parallel Consumer instance.
 *
 * <h2>Nothing happens until you call {@link #start()}</h2>
 * <p>
 * Having this module on the classpath does nothing at all. There is no auto-start, no service loader, no
 * classpath-scanning activation (plan R3). A library that opens a listening socket because somebody added a
 * dependency is a library that opens a listening socket in production without being asked.
 *
 * <h2>The port search, and why it is silent</h2>
 * <p>
 * Start at {@link DashboardOptions#DEFAULT_PORT}; on a bind failure try the next port, and the next, up to
 * {@link DashboardOptions#getMaxPortAttempts()}. <strong>Nothing is logged during the search.</strong> A busy port is
 * not news, and the natural implementation - a warning per failed attempt - buries the only line that matters under a
 * wall of noise on exactly the machine where it matters most: the one running several instances.
 * <p>
 * Once bound, exactly one line is logged, carrying the full URL in a form terminals linkify. When several instances
 * share a machine, the port <em>is</em> the identifier for which consumer you are looking at, so that line is the
 * primary output of starting the dashboard, not an aside.
 *
 * <h2>Read-only, and structurally so</h2>
 * <p>
 * Every request other than {@code GET} and {@code HEAD} is answered {@code 405} by a handler that runs before the
 * route table, so the property holds for routes that do not exist yet as well as the ones that do (plan R24). The
 * consequence is that the threat model is information disclosure - group id, topic names, partition assignments,
 * offsets - and not integrity or availability. Adding a write path would be a deliberate reopening of that decision.
 *
 * <h2>It owns its {@link Vertx}</h2>
 * <p>
 * The module creates its own {@link Vertx} instance and closes it in {@link #close()}. It deliberately does not adopt
 * one the application may be using for its own work: closing somebody else's event loop as a side effect of stopping
 * a dashboard would be a spectacular way for a diagnostic tool to cause an outage.
 * <p>
 * Experimental: the dashboard module is opt-in and its API may change without notice.
 */
@InterfaceStability.Unstable
@Slf4j
public class DashboardServer implements AutoCloseable {

    public static final String STATE_PATH = "/api/state.json";

    public static final String STREAM_PATH = "/api/stream";

    public static final String STATUS_PATH = "/status";

    /**
     * URL prefix for the page's own assets, served from {@code web/} on the classpath.
     */
    public static final String ASSETS_PREFIX = "/assets";

    /**
     * URL prefix for third-party assets. Only the chart library lives here, and only its {@code dist} directory is
     * reachable - the WebJar also ships a README, a package.json and TypeScript definitions, none of which anybody
     * needs over HTTP.
     */
    public static final String VENDOR_PREFIX = "/vendor";

    /**
     * Must track the {@code uplot.version} property in this module's pom. The version is in the classpath layout
     * WebJars mandate, so it cannot be avoided; keeping it in one constant means a version bump is one edit.
     */
    public static final String UPLOT_VERSION = "1.6.30";

    public static final String UPLOT_CLASSPATH_ROOT = "META-INF/resources/webjars/uplot/" + UPLOT_VERSION + "/dist";

    /**
     * Classpath directory holding the page shell.
     */
    public static final String PAGE_CLASSPATH_ROOT = "web";

    /**
     * How long to wait for a bind or a shutdown before deciding the event loop is wedged. Generous; it only exists so
     * a pathological environment produces a message rather than a hung process.
     */
    private static final long LIFECYCLE_TIMEOUT_SECONDS = 30;

    private final SnapshotPublisher publisher;

    private final MeterRegistry registry;

    @Getter
    private final DashboardOptions options;

    /**
     * The event-stream route. Exposed because how many streams are open is genuine operational information - it is
     * what the concurrency cap is counted against.
     */
    @Getter
    private final StreamRoute streamRoute;

    private final AtomicBoolean closed = new AtomicBoolean();

    private final AtomicBoolean started = new AtomicBoolean();

    /**
     * The route table. Deliberately <em>not</em> exposed: Vert.x matches routes in registration order and evaluates
     * failure handlers by continuing forward through that order, so a route appended after this server's failure
     * handler would silently lose its 500-without-a-stack-trace behaviour. {@link #getProbePaths()} exposes what a
     * caller actually needs from it.
     */
    private Router router;

    private Vertx vertx;

    private HttpServer httpServer;

    /**
     * The port actually bound, which is not necessarily the one asked for - see the port search.
     */
    @Getter
    private int port;

    /**
     * The full URL the dashboard is reachable at, or null before {@link #start()}.
     */
    @Getter
    private String url;

    /**
     * @param publisher the published snapshot source; may be null, which the {@code /status} page reports rather than
     *                  failing on - a server with no publisher is one of the misconfigurations it exists to name
     * @param registry  used <em>only</em> to report whether a registry was supplied. No meter is ever read from it
     *                  here; see {@link StatusRoute}
     */
    public DashboardServer(SnapshotPublisher publisher, MeterRegistry registry, DashboardOptions options) {
        this.publisher = publisher;
        this.registry = registry;
        this.options = options == null ? DashboardOptions.defaults() : options;
        this.streamRoute = new StreamRoute(publisher, this.options);
    }

    /**
     * The usual wiring: register snapshot sampling on {@code pc}'s control loop, then serve it. This is the whole
     * integration - the caller's Parallel Consumer construction is otherwise unchanged (plan R4).
     */
    public static DashboardServer startFor(AbstractParallelEoSStreamProcessor<?, ?> pc,
                                           MeterRegistry registry,
                                           DashboardOptions options) {
        SnapshotPublisher publisher = SnapshotPublisher.createAndRegister(pc, registry);
        return new DashboardServer(publisher, registry, options).start();
    }

    /**
     * As {@link #startFor(AbstractParallelEoSStreamProcessor, MeterRegistry, DashboardOptions)} with the defaults.
     */
    public static DashboardServer startFor(AbstractParallelEoSStreamProcessor<?, ?> pc, MeterRegistry registry) {
        return startFor(pc, registry, DashboardOptions.defaults());
    }

    /**
     * Creates the event loop, binds a port, and starts serving. Returns {@code this} so the handle can be captured in
     * one expression.
     *
     * @throws IllegalStateException if no port in the configured range could be bound, or if the server is already
     *                               started
     */
    public DashboardServer start() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("This DashboardServer is already started, on " + url);
        }
        this.vertx = Vertx.vertx(new VertxOptions().setFileSystemOptions(fileSystemOptions()));
        this.router = buildRouter();

        HttpServerOptions serverOptions = new HttpServerOptions()
                .setHost(options.getBindHostForSocket())
                .setCompressionSupported(true);

        this.httpServer = bindWalkingUpward(serverOptions);
        this.url = options.url(port);

        // THE line. One, after the search, carrying a URL a terminal will linkify - see the class javadoc.
        log.info("Parallel Consumer dashboard (EXPERIMENTAL, read-only, unauthenticated) is at {}", url);

        if (!options.isLoopbackBind()) {
            log.warn("The Parallel Consumer dashboard is bound to the NON-LOOPBACK address {}, so anything that can "
                            + "reach this host can reach it. It is unauthenticated and has no access control. It "
                            + "discloses: the consumer group id, the topic names, this instance's partition "
                            + "assignments, and its committed, completed and seen offsets. Bind a loopback address "
                            + "(the default) or put it behind something that authenticates.",
                    options.getBindHostLiteral());
        }

        streamRoute.start(vertx);
        return this;
    }

    /**
     * Stops serving, releases the port, and closes the {@link Vertx} instance this server created. Idempotent.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        streamRoute.close();
        if (httpServer != null) {
            awaitQuietly(httpServer.close(), "closing the dashboard HTTP server");
        }
        if (vertx != null) {
            awaitQuietly(vertx.close(), "closing the dashboard Vertx instance");
        }
    }

    /**
     * Every path in the route table, with wildcard mounts turned into a concrete probe path. This is what makes
     * "every route rejects writes" assertable across the whole table rather than a hand-listed sample: a route added
     * later appears here automatically.
     */
    public List<String> getProbePaths() {
        List<String> paths = new ArrayList<>();
        paths.add("/");
        if (router != null) {
            router.getRoutes().forEach(route -> {
                String path = route.getPath();
                if (path == null) {
                    // a route with no path matches everything and is already covered by "/"
                    return;
                }
                String probe = path.endsWith("/*") ? path.substring(0, path.length() - 1) + "probe" : path;
                if (!paths.contains(probe)) {
                    paths.add(probe);
                }
            });
        }
        // a path that matches nothing must be covered too - a 404 route must still refuse a POST
        paths.add("/no-such-path");
        return Collections.unmodifiableList(paths);
    }

    private Router buildRouter() {
        Router built = Router.router(vertx);

        // ORDER MATTERS. These three run before the route table, so they hold for every route including ones added
        // after this method - which is the property that makes the read-only and Host guarantees structural rather
        // than a convention every future route has to remember.
        built.route().handler(new HostAllowlist(options));
        built.route().handler(DashboardServer::rejectWriteMethods);
        built.route().handler(DashboardServer::rejectTraversal);

        // route(path) rather than get(path): non-GET/HEAD has already been rejected above, and this way HEAD works
        // without every route being declared twice
        built.route(STATE_PATH).handler(new StateRoute(publisher, options));
        built.route(STREAM_PATH).handler(streamRoute);
        built.route(STATUS_PATH).handler(new StatusRoute(publisher, registry, options));

        built.route(VENDOR_PREFIX + "/uplot/*").handler(staticHandler(UPLOT_CLASSPATH_ROOT));
        built.route(ASSETS_PREFIX + "/*").handler(staticHandler(PAGE_CLASSPATH_ROOT));
        built.route("/").handler(ctx -> ctx.reroute(ASSETS_PREFIX + "/index.html"));

        built.route().failureHandler(DashboardServer::handleFailure);
        return built;
    }

    private static StaticHandler staticHandler(String classpathRoot) {
        return StaticHandler.create(classpathRoot)
                // a directory listing turns a static mount into a file browser, which is a disclosure surface with
                // no purpose on a dashboard whose assets are all referenced by the page
                .setDirectoryListing(false)
                .setIncludeHidden(false)
                .setDefaultContentEncoding("UTF-8");
    }

    /**
     * Answers anything that is not {@code GET} or {@code HEAD} with {@code 405}. Registered before the route table
     * so it cannot be forgotten by a route added later.
     */
    static void rejectWriteMethods(RoutingContext ctx) {
        HttpMethod method = ctx.request().method();
        if (HttpMethod.GET.equals(method) || HttpMethod.HEAD.equals(method)) {
            ctx.next();
            return;
        }
        ctx.response()
                .setStatusCode(405)
                .putHeader("Allow", "GET, HEAD")
                .putHeader("Content-Type", "text/plain; charset=utf-8")
                .putHeader("X-Content-Type-Options", "nosniff")
                .end("405 Method Not Allowed. The Parallel Consumer dashboard is read-only: it serves GET and HEAD "
                        + "and nothing else. It exposes no operation that changes the consumer, the group or the "
                        + "broker.");
    }

    /**
     * Rejects path traversal before anything gets a chance to resolve it.
     * <p>
     * Vert.x's router normalises the path before matching, so {@code /vendor/../../etc/passwd} already misses every
     * route. That was verified rather than assumed: removing this handler turns all six traversal cases in
     * {@code StaticAssetTest} from {@code 400} into {@code 404}, and none of them into a served file - so on Vert.x
     * 4.5.31 this guard is not what stops the disclosure.
     * <p>
     * It stays anyway, and runs on the <em>raw</em> path, for two reasons. A {@code 400} that names the reason is a
     * better answer than a {@code 404} that reads like a typo. And the defence should not rest entirely on the
     * static handler's normalisation continuing to be correct in every future version of it - Vert.x's static
     * handler has had traversal advisories before, and this repository will not notice the next one.
     */
    static void rejectTraversal(RoutingContext ctx) {
        String rawPath = ctx.request().path();
        if (isTraversal(rawPath)) {
            ctx.response()
                    .setStatusCode(400)
                    .putHeader("Content-Type", "text/plain; charset=utf-8")
                    .putHeader("X-Content-Type-Options", "nosniff")
                    .end("400 Bad Request. The request path contains a traversal sequence. The dashboard serves "
                            + "static files only from " + ASSETS_PREFIX + " and " + VENDOR_PREFIX + ".");
            return;
        }
        ctx.next();
    }

    /**
     * Whether a raw request path is trying to escape its mount. Decoded once and checked; a path that is
     * <em>double</em>-encoded is rejected outright, because there is no legitimate reason for one here and
     * decode-until-stable is how traversal filters get bypassed.
     */
    static boolean isTraversal(String rawPath) {
        if (rawPath == null || rawPath.isEmpty()) {
            return true;
        }
        String lower = rawPath.toLowerCase(Locale.ROOT);
        if (lower.contains("%25")) {
            return true;
        }
        String decoded;
        try {
            decoded = URLDecoder.decode(lower, "UTF-8");
        } catch (UnsupportedEncodingException | IllegalArgumentException e) {
            // a path that will not decode is not a path this server has any business guessing at
            return true;
        }
        return !decoded.startsWith("/")
                || decoded.contains("..")
                || decoded.contains("\\")
                || decoded.indexOf('\0') >= 0;
    }

    /**
     * Answers a handler that threw with {@code 500} and a fixed body. The exception goes to the log, where the
     * operator can see it; it does not go to the client, who is - on a non-loopback bind - not necessarily the
     * operator, and for whom a stack trace is a map of the internals.
     */
    static void handleFailure(RoutingContext ctx) {
        int statusCode = ctx.statusCode() > 0 ? ctx.statusCode() : 500;
        Throwable failure = ctx.failure();
        if (failure != null) {
            log.warn("A Parallel Consumer dashboard request handler threw; answering {} to the client. Path: {}",
                    statusCode, ctx.request().path(), failure);
        }
        if (ctx.response().ended()) {
            return;
        }
        ctx.response()
                .setStatusCode(statusCode)
                .putHeader("Content-Type", "text/plain; charset=utf-8")
                .putHeader("X-Content-Type-Options", "nosniff")
                .end(statusCode + " - the dashboard failed to handle this request. The reason is in the consumer's "
                        + "log; it is deliberately not in this response.");
    }

    /**
     * Binds the first free port at or above the configured one. Silent while searching - see the class javadoc.
     */
    private HttpServer bindWalkingUpward(HttpServerOptions serverOptions) {
        int startPort = options.getPort();
        int attempts = options.getMaxPortAttempts();
        Throwable lastBindFailure = null;
        int lastTried = startPort;
        for (int attempt = 0; attempt < attempts; attempt++) {
            int candidate = startPort + attempt;
            if (candidate > 65535) {
                break;
            }
            lastTried = candidate;
            HttpServer server = vertx.createHttpServer(new HttpServerOptions(serverOptions).setPort(candidate))
                    .requestHandler(router);
            try {
                HttpServer bound = await(server.listen());
                this.port = bound.actualPort();
                return bound;
            } catch (Exception e) {
                if (!isPortUnavailable(e)) {
                    // a permissions problem, or an address that is not on this host, will never be fixed by trying
                    // the next port, and walking through a hundred of them would turn one clear error into a
                    // hundred identical ones
                    awaitQuietly(server.close(), "closing a dashboard server that failed to start");
                    throw new IllegalStateException("The Parallel Consumer dashboard could not start on "
                            + options.getBindHostLiteral() + ":" + candidate
                            + ", and the failure is not a busy port so trying another would not help.", e);
                }
                lastBindFailure = e;
                awaitQuietly(server.close(), "closing a dashboard server that could not bind");
            }
        }
        throw new IllegalStateException(exhaustedMessage(startPort, lastTried, attempts), lastBindFailure);
    }

    private static String exhaustedMessage(int startPort, int lastTried, int attempts) {
        if (attempts == 1 || startPort == lastTried) {
            return "The Parallel Consumer dashboard could not bind port " + startPort + " - it is already in use, "
                    + "and DashboardOptions.maxPortAttempts is " + attempts + " so no other port was tried. Set "
                    + "DashboardOptions.port to a free port, or raise maxPortAttempts to let the server walk upward.";
        }
        return "The Parallel Consumer dashboard could not bind any port from " + startPort + " to " + lastTried
                + " - all " + attempts + " were in use. Set DashboardOptions.port to a free range, or raise "
                + "DashboardOptions.maxPortAttempts.";
    }

    /**
     * Whether a listen failure means "that port is taken" rather than something a different port would not fix.
     * Netty surfaces this as a {@link BindException} on the JDK transport and as a native {@link SocketException} on
     * the epoll/kqueue transports, so both are matched, and the message is checked as a backstop because the exact
     * type has moved between Netty versions before.
     */
    static boolean isPortUnavailable(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (t instanceof BindException) {
                return true;
            }
            String message = t.getMessage();
            if (t instanceof SocketException && message != null
                    && message.toLowerCase(Locale.ROOT).contains("address already in use")) {
                return true;
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return false;
    }

    /**
     * Keeps Vert.x's classpath-resolution cache out of the working directory. Vert.x extracts classpath resources to
     * disk to serve them, and its default is a {@code .vertx} directory in the process's CWD - which for a library
     * embedded in somebody else's application means litter in their working directory, and in this repository means
     * a dirty {@code git status} after running the tests.
     */
    private static FileSystemOptions fileSystemOptions() {
        File cacheDir = Paths.get(System.getProperty("java.io.tmpdir"), "pc-dashboard-vertx-cache").toFile();
        return new FileSystemOptions().setFileCacheDir(cacheDir.getAbsolutePath());
    }

    private static <T> T await(Future<T> future) throws Exception {
        try {
            return future.toCompletionStage().toCompletableFuture().get(LIFECYCLE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } catch (TimeoutException e) {
            throw e;
        }
    }

    private static void awaitQuietly(Future<Void> future, String what) {
        try {
            await(future);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.debug("Ignoring a failure while {}.", what, e);
        }
    }
}
