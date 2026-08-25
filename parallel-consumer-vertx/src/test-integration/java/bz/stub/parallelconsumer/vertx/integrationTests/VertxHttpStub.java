package bz.stub.parallelconsumer.vertx.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.RequestListener;
import com.github.tomakehurst.wiremock.http.Response;

import java.util.function.Consumer;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;

/**
 * The stub HTTP service both Vert.x integration entry points hand records to: {@link Demo}, which
 * measures throughput, and {@code VertxConcurrencyIT}, which asserts the concurrency level reached.
 * <p>
 * <b>The thread budget is the reason this is shared rather than copied.</b> A stub whose container
 * thread pool is smaller than the caller's max concurrency silently becomes the bottleneck, and the
 * result reads as Parallel Consumer failing to reach its ceiling rather than as the harness capping
 * it. Both callers got that right independently and expressed it differently - one as
 * {@code max(concurrencyTarget, 6)}, the other as {@code expectedConcurrentCount * 2} - which is the
 * shape of duplication that stays correct until someone changes one of them.
 * <p>
 * What each caller does per request stays its own: the listener is a parameter, because measuring a
 * rate and blocking to force a concurrency level are genuinely different jobs.
 */
public final class VertxHttpStub implements AutoCloseable {

    /**
     * Jetty will not start with fewer than this many container threads, so a small concurrency
     * target must still clear it.
     */
    private static final int MIN_THREADS_JETTY_NEEDS = 6;

    private final WireMockServer server;

    private VertxHttpStub(WireMockServer server) {
        this.server = server;
    }

    /**
     * Starts a stub answering {@code GET /} with an empty 200, sized to serve {@code maxConcurrency}
     * requests at once.
     *
     * @param maxConcurrency the caller's in-flight ceiling; the pool is sized to at least this, so the
     *                       stub is never what limits observed concurrency
     * @param onRequest      run on the serving thread for every request received - step a progress bar,
     *                       count concurrency, sleep to simulate work
     */
    public static VertxHttpStub start(int maxConcurrency, Consumer<Request> onRequest) {
        WireMockConfiguration options = WireMockConfiguration.wireMockConfig()
                .dynamicPort()
                .containerThreads(Math.max(maxConcurrency, MIN_THREADS_JETTY_NEEDS));

        WireMockServer server = new WireMockServer(options);
        server.stubFor(get(urlPathEqualTo("/")).willReturn(aResponse()));
        server.addMockServiceRequestListener(new RequestListener() {
            @Override
            public void requestReceived(Request request, Response response) {
                onRequest.accept(request);
            }
        });
        server.start();
        return new VertxHttpStub(server);
    }

    /** The ephemeral port the stub bound to. */
    public int port() {
        return server.port();
    }

    @Override
    public void close() {
        server.stop();
    }
}
