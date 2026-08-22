package bz.stub.parallelconsumer.client.grpc;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.client.ClientOptions;
import bz.stub.parallelconsumer.client.Outcome;
import bz.stub.parallelconsumer.proxy.protocol.v1.ClientMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.Configured;
import bz.stub.parallelconsumer.proxy.protocol.v1.Dispatch;
import bz.stub.parallelconsumer.proxy.protocol.v1.DispatchRecord;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyServiceGrpc;
import bz.stub.parallelconsumer.proxy.protocol.v1.Record;
import bz.stub.parallelconsumer.proxy.protocol.v1.Report;
import bz.stub.parallelconsumer.proxy.protocol.v1.Token;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * How a session ends, from both sides of the stream: what the application is told, and what the engine is told.
 * <p>
 * <b>Both tests here were red before the change that added them</b>, and each fails in a different way, which
 * is why they are separate: the mid-session stream error <em>hung</em> (executors parked in an untimed hand-out
 * wait with nothing on the client surface able to report it - the parked P0), and the close path <em>fabricated
 * a verdict</em>, transmitting an interrupted user function's "processing was interrupted" as a real Failure
 * the engine applies against the record's retry budget.
 * <p>
 * It drives a <b>fake proxy</b> rather than the real engine on purpose. The two behaviours under test are the
 * transport's reaction to a stream that dies mid-session and to a close that lands mid-processing; a real
 * engine will not do either on demand, and the harness-backed conformance suite (which does run the real
 * engine) covers the clean paths.
 *
 * @author Antony Stubbs
 */
@Timeout(60)
class SessionEndTest {

    private static final Duration OBSERVE_BUDGET = Duration.ofSeconds(20);

    /**
     * <b>The parked P0.</b> The stream dies while a record is being processed and another executor is idle. The
     * application must learn that consumption stopped <em>and</em> why, and the executors must leave rather
     * than park for the life of the process.
     * <p>
     * Before the fix, {@code SessionObserver.onError} marked the stream closed and failed the handshake future
     * but never flipped {@code running}, so every executor stayed blocked in an untimed {@code take()} - this
     * test's executor-termination await ran to its budget and failed, with the threads still alive - and
     * nothing on the client surface could have carried the cause, because there was no such method.
     */
    @Test
    void aMidSessionStreamErrorEndsTheSessionAndReleasesTheExecutors() throws Exception {
        var processing = new CountDownLatch(1);
        var releaseProcessing = new CountDownLatch(1);
        // two executors, one record: one is busy in the user function, the other idle in hand-out
        try (var proxy = new FakeProxy(2, 2)) {
            var client = clientFor(proxy);
            try {
                client.poll(record -> {
                    processing.countDown();
                    // still executing when the stream dies - the case the finding names as the sharp one
                    releaseProcessing.await(OBSERVE_BUDGET.getSeconds(), TimeUnit.SECONDS);
                    return Outcome.success();
                });
                proxy.dispatch(0, "a-key", "a-value");
                assertWithMessage("the user function is executing when the stream dies")
                        .that(processing.await(OBSERVE_BUDGET.getSeconds(), TimeUnit.SECONDS)).isTrue();

                proxy.failStream(Status.UNAVAILABLE.withDescription("the sidecar vanished"));

                var thrown = Assertions.assertThrows(ExecutionException.class,
                        () -> client.sessionEnd().toCompletableFuture()
                                .get(OBSERVE_BUDGET.getSeconds(), TimeUnit.SECONDS),
                        "the session end completed normally, but the stream died under it");
                assertWithMessage("the caller learns WHY the session ended, from the same call that says THAT "
                        + "it ended: " + thrown)
                        .that(rootMessageOf(thrown)).contains("the sidecar vanished");

                releaseProcessing.countDown();
                Awaitility.await("the executors stop rather than parking in a hand-out that will never be fed")
                        .atMost(OBSERVE_BUDGET).untilAsserted(() ->
                                assertWithMessage("live executor threads after the session died")
                                        .that(liveExecutorThreads()).isEmpty());
            } finally {
                releaseProcessing.countDown();
                client.close();
            }
        }
    }

    /**
     * <b>Closing must not invent a verdict.</b> The specification's shutdown rule is stop hand-out, final
     * reports for records already executing, then half-close - so a user function still running when
     * {@code close()} is called finishes and its own outcome is what the engine hears.
     * <p>
     * Before the fix, {@code close()} called {@code shutdownNow()} <em>before</em> marking the stream closed:
     * the user function was interrupted, {@code Outcomes} turned the {@code InterruptedException} into a
     * "processing was interrupted" failure, and that failure was transmitted and applied engine-side as a real
     * one - an attempt consumed and a retry scheduled for a record whose processing nobody had decided. This
     * test saw a {@code FAILURE} report where the user function returned success.
     */
    @Test
    void closingWhileAUserFunctionExecutesReportsItsOwnOutcomeAndNoFabricatedFailure() throws Exception {
        var processing = new CountDownLatch(1);
        var releaseProcessing = new CountDownLatch(1);
        try (var proxy = new FakeProxy(1, 1)) {
            var client = clientFor(proxy);
            client.poll(record -> {
                processing.countDown();
                releaseProcessing.await(OBSERVE_BUDGET.getSeconds(), TimeUnit.SECONDS);
                return Outcome.success();
            });
            proxy.dispatch(0, "a-key", "a-value");
            assertWithMessage("the user function is executing when close is called")
                    .that(processing.await(OBSERVE_BUDGET.getSeconds(), TimeUnit.SECONDS)).isTrue();

            // the function finishes shortly after close begins: close must wait for it rather than interrupt it
            var release = new Thread(() -> {
                try {
                    TimeUnit.MILLISECONDS.sleep(300);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                releaseProcessing.countDown();
            }, "release-the-user-function");
            release.setDaemon(true);
            release.start();

            client.close();

            var reports = proxy.reports();
            assertWithMessage("the record's own verdict reached the engine: " + reports).that(reports).hasSize(1);
            assertWithMessage("no verdict is invented for work the user function did not decide")
                    .that(reports.get(0).getOutcomeCase()).isEqualTo(Report.OutcomeCase.SUCCESS);
            assertWithMessage("the final report is sent BEFORE the half-close, not dropped by it")
                    .that(proxy.reportsBeforeHalfClose()).isEqualTo(1);
        }
    }

    private static GrpcParallelConsumerClient clientFor(FakeProxy proxy) {
        return GrpcParallelConsumerClient.builder()
                .port(proxy.port())
                .options(ClientOptions.builder()
                        .topics(Collections.singletonList("session-end-topic"))
                        .maxConcurrency(2)
                        .build())
                .build();
    }

    /** Every live thread the transport's executor pool named as its own - empty once the session has ended. */
    private static List<String> liveExecutorThreads() {
        var alive = new ArrayList<String>();
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.isAlive() && thread.getName().startsWith("pc-grpc-client-executor-")) {
                alive.add(thread.getName());
            }
        }
        return alive;
    }

    /** The deepest message in the chain, since a completion stage wraps what it carries. */
    private static String rootMessageOf(Throwable thrown) {
        Throwable deepest = thrown;
        while (deepest.getCause() != null) {
            deepest = deepest.getCause();
        }
        return String.valueOf(deepest.getMessage());
    }

    /**
     * A proxy that does only what a test tells it to: it accepts the session, records what the client sends,
     * and sends exactly the messages asked for - including the two a real engine will not produce on cue, a
     * mid-session stream error and nothing at all.
     */
    private static final class FakeProxy implements AutoCloseable {

        private final List<ClientMessage> received = new CopyOnWriteArrayList<>();
        private final Server server;
        private final int executorCount;
        private final int maxConcurrency;

        private volatile StreamObserver<ProxyMessage> toClient;
        private volatile int reportsBeforeHalfClose = -1;

        private FakeProxy(int executorCount, int maxConcurrency) throws IOException {
            this.executorCount = executorCount;
            this.maxConcurrency = maxConcurrency;
            this.server = ServerBuilder.forPort(0)
                    .addService(new ProxyServiceGrpc.ProxyServiceImplBase() {
                        @Override
                        public StreamObserver<ClientMessage> session(StreamObserver<ProxyMessage> responses) {
                            toClient = responses;
                            return new StreamObserver<ClientMessage>() {
                                @Override
                                public void onNext(ClientMessage message) {
                                    received.add(message);
                                    if (message.hasConfigure()) {
                                        // answered here, on the stream's own thread, because the client's poll
                                        // does not return until the handshake does
                                        sendConfigured();
                                    }
                                }

                                @Override
                                public void onError(Throwable t) {
                                    // the client cancelled or failed the call; nothing to do but stop
                                }

                                @Override
                                public void onCompleted() {
                                    // the half-close: how many verdicts had arrived by the time it did
                                    reportsBeforeHalfClose = reports().size();
                                    responses.onCompleted();
                                }
                            };
                        }
                    })
                    .build()
                    .start();
        }

        private int port() {
            return server.getPort();
        }

        private void sendConfigured() {
            toClient.onNext(ProxyMessage.newBuilder()
                    .setConfigured(Configured.newBuilder()
                            .setExecutorCount(executorCount)
                            .setMaxConcurrency(maxConcurrency)
                            .addCapabilities("dispatch"))
                    .build());
        }

        private void dispatch(long offset, String key, String value) {
            toClient.onNext(ProxyMessage.newBuilder()
                    .setDispatch(Dispatch.newBuilder()
                            .addRecords(DispatchRecord.newBuilder()
                                    .setToken(Token.newBuilder().setRecordId("record-" + offset).setEpoch(1))
                                    .setAttempt(1)
                                    .setRecord(Record.newBuilder()
                                            .setTopic("session-end-topic")
                                            .setPartition(0)
                                            .setOffset(offset)
                                            .setKey(ByteString.copyFrom(key, StandardCharsets.UTF_8))
                                            .setValue(ByteString.copyFrom(value, StandardCharsets.UTF_8)))))
                    .build());
        }

        private void failStream(Status status) {
            toClient.onError(status.asRuntimeException());
        }

        /** Every verdict the client sent, in arrival order. */
        private List<Report> reports() {
            var reports = new ArrayList<Report>();
            for (ClientMessage message : received) {
                if (message.hasReport()) {
                    reports.add(message.getReport());
                }
            }
            return reports;
        }

        /** How many verdicts had arrived when the client half-closed, or -1 if it has not. */
        private int reportsBeforeHalfClose() {
            return reportsBeforeHalfClose;
        }

        @Override
        public void close() {
            server.shutdownNow();
            try {
                server.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
