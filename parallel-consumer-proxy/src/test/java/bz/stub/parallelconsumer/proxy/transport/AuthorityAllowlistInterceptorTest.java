package bz.stub.parallelconsumer.proxy.transport;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.proxy.protocol.v1.ClientMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * R29's allowlist, proven at both altitudes: over the wire through a real loopback server (a connection
 * declaring {@code localhost} is admitted, one declaring {@code evil.example.com} is rejected with
 * {@code PERMISSION_DENIED} - and, per AE12, with the service-invocation and application-message counters
 * both unchanged), and directly against the interceptor for the shapes a channel cannot conveniently produce
 * (a null authority, the port and bracket forms).
 */
@Timeout(value = 30)
class AuthorityAllowlistInterceptorTest extends WireTestBase {

    @Test
    void connectionDeclaringLocalhostIsAdmitted() throws Exception {
        ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", server.port())
                .usePlaintext()
                .build();
        try {
            var responses = new RecordingProxyMessageObserver();
            StreamObserver<ClientMessage> requests = ProxyServiceGrpc.newStub(channel).session(responses);
            requests.onNext(configure("in"));

            await().atMost(10, SECONDS).until(() -> !responses.messages.isEmpty());
            assertThat(responses.messages.get(0).hasConfigured()).isTrue();
            assertThat(service.serviceInvocations.get()).isEqualTo(1);
            requests.onCompleted();
        } finally {
            channel.shutdownNow();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /**
     * Covers AE12. The proof is the counters, not the client-visible status: a rejection that arrived only
     * after the service method ran would return the same status to the client, so the assertion that matters
     * is that neither the service-invocation counter nor the application-message counter moved - even though
     * the client pushed a Configure at the stream.
     */
    @Test
    void connectionDeclaringUnlistedAuthorityIsRejectedBeforeTheServiceMethodRuns() throws Exception {
        ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", server.port())
                .usePlaintext()
                .overrideAuthority("evil.example.com")
                .build();
        try {
            var responses = new RecordingProxyMessageObserver();
            StreamObserver<ClientMessage> requests = ProxyServiceGrpc.newStub(channel).session(responses);
            try {
                requests.onNext(configure("never-delivered"));
            } catch (IllegalStateException alreadyClosed) {
                // The rejection can land before the client's send - equally fine: the message never
                // reached the application either way, which is what the counters below prove.
            }

            assertWithMessage("stream should terminate with the rejection")
                    .that(responses.terminated.await(10, SECONDS)).isTrue();
            Status status = Status.fromThrowable(responses.error.get());
            assertThat(status.getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
            assertWithMessage("service method must never have run for a rejected connection")
                    .that(service.serviceInvocations.get()).isEqualTo(0);
            assertWithMessage("no application message may reach the service across a rejected connection")
                    .that(service.applicationMessages.get()).isEqualTo(0);
        } finally {
            channel.shutdownNow();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void connectionDeclaringNoAuthorityIsAdmitted() {
        var interceptor = defaultInterceptor();
        var call = this.<String, String>callDeclaring(null);
        ServerCallHandler<String, String> handler = handlerReturningNoopListener();

        interceptor.interceptCall(call, new Metadata(), handler);

        verify(handler).startCall(eq(call), any());
        verify(call, never()).close(any(), any());
    }

    @Test
    void portAndBracketAuthorityFormsAreAdmitted() {
        var interceptor = defaultInterceptor();
        for (var authority : List.of("localhost:41234", "LOCALHOST", "127.0.0.1:1", "[::1]:8080", "[::1]")) {
            var call = this.<String, String>callDeclaring(authority);
            ServerCallHandler<String, String> handler = handlerReturningNoopListener();

            interceptor.interceptCall(call, new Metadata(), handler);

            verify(handler).startCall(eq(call), any());
            verify(call, never()).close(any(), any());
        }
    }

    @Test
    void unlistedAuthorityIsClosedWithPermissionDeniedAndTheHandlerNeverInvoked() {
        var interceptor = defaultInterceptor();
        var call = this.<String, String>callDeclaring("evil.example.com:443");
        @SuppressWarnings("unchecked")
        ServerCallHandler<String, String> handler = mock(ServerCallHandler.class);

        interceptor.interceptCall(call, new Metadata(), handler);

        verify(handler, never()).startCall(any(), any());
        var status = ArgumentCaptor.forClass(Status.class);
        verify(call).close(status.capture(), any());
        assertThat(status.getValue().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
    }

    @Test
    void normalizationStripsPortBracketsAndCaseButNotBareIpv6Segments() {
        assertThat(AuthorityAllowlistInterceptor.normalizeToHost("localhost:41234")).isEqualTo("localhost");
        assertThat(AuthorityAllowlistInterceptor.normalizeToHost("LocalHost")).isEqualTo("localhost");
        assertThat(AuthorityAllowlistInterceptor.normalizeToHost("[::1]:8080")).isEqualTo("::1");
        assertThat(AuthorityAllowlistInterceptor.normalizeToHost("[::1]")).isEqualTo("::1");
        // A bare unbracketed IPv6 literal: the last segment is not a port.
        assertThat(AuthorityAllowlistInterceptor.normalizeToHost("0:0:0:0:0:0:0:1")).isEqualTo("0:0:0:0:0:0:0:1");
    }

    private static AuthorityAllowlistInterceptor defaultInterceptor() {
        return AuthorityAllowlistInterceptor
                .defaultAllowlist(java.net.InetAddress.getLoopbackAddress(), List.of());
    }

    private <ReqT, RespT> ServerCall<ReqT, RespT> callDeclaring(String authority) {
        @SuppressWarnings("unchecked")
        ServerCall<ReqT, RespT> call = mock(ServerCall.class);
        when(call.getAuthority()).thenReturn(authority);
        return call;
    }

    private static ServerCallHandler<String, String> handlerReturningNoopListener() {
        @SuppressWarnings("unchecked")
        ServerCallHandler<String, String> handler = mock(ServerCallHandler.class);
        when(handler.startCall(any(), any())).thenReturn(new ServerCall.Listener<>() {
        });
        return handler;
    }
}
