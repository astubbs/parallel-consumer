package bz.stub.parallelconsumer.vertx;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.web.client.WebClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.concurrent.ExecutionException;

import static com.google.common.truth.Truth.assertThat;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * The sharing half of the contract {@link VertxCloseTestBase} describes: an engine or client the caller supplied is
 * the caller's to close, so no close entry point touches it - and when the caller supplies only the engine, the
 * processor closes the client it built on it without closing the engine underneath.
 * {@link VertxCloseReleasesEngineTest} pins the owning half.
 */
class VertxCloseLeavesACallerSuppliedEngineRunningTest extends VertxCloseTestBase {

    private Vertx vertxSpy;
    private WebClient webClientSpy;

    /**
     * Spies on the instances {@link VertxBaseUnitTest} builds, so {@code verify} can see that close left them alone.
     */
    @Override
    protected Vertx createVertx(VertxOptions vertxOptions) {
        vertxSpy = spy(super.createVertx(vertxOptions));
        return vertxSpy;
    }

    @Override
    protected WebClient createWebClient(Vertx vertx) {
        webClientSpy = spy(super.createWebClient(vertx));
        return webClientSpy;
    }

    @ParameterizedTest
    @EnumSource(CloseEntryPoint.class)
    void noCloseEntryPointClosesAnEngineOrClientTheCallerSupplied(CloseEntryPoint entryPoint) {
        var probe = EngineProbeVerticle.deployOn(vertxSpy);

        entryPoint.closeVia(vertxAsync);

        verify(webClientSpy, never()).close();
        verify(vertxSpy, never()).close();
        verify(vertxSpy, never()).close(any());
        assertThat(probe.engineBeganClosing()).isFalse();
    }

    @Test
    void aClientTheProcessorBuiltOnTheCallersEngineIsClosedWithoutClosingTheEngine() throws Exception {
        var probe = EngineProbeVerticle.deployOn(vertxSpy);
        var sharingTheEngine = newProcessorOn(vertxSpy, null);

        sharingTheEngine.close();

        verify(vertxSpy, never()).close();
        assertThat(probe.engineBeganClosing()).isFalse();
        assertWebClientIsClosed(sharingTheEngine.getWebClient());
    }

    /**
     * A closed Vert.x client refuses a new request with {@code IllegalStateException: Client is closed}; an open one
     * would try the connection and report it refused instead, so the failure's type is what tells the two apart.
     */
    private static void assertWebClientIsClosed(WebClient webClient) throws Exception {
        Throwable failure = null;
        try {
            webClient.getAbs("http://127.0.0.1:1/").send().toCompletionStage().toCompletableFuture().get(10, SECONDS);
        } catch (ExecutionException requestFailedAsynchronously) {
            failure = requestFailedAsynchronously.getCause();
        } catch (IllegalStateException requestRefusedSynchronously) {
            failure = requestRefusedSynchronously;
        }
        assertThat(failure).isInstanceOf(IllegalStateException.class);
    }
}
