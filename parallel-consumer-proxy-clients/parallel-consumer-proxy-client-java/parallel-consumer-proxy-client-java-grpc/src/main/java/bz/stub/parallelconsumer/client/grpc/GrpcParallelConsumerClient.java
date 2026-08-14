package bz.stub.parallelconsumer.client.grpc;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.client.ClientOptions;
import bz.stub.parallelconsumer.client.Outcomes;
import bz.stub.parallelconsumer.client.ParallelConsumerClient;
import bz.stub.parallelconsumer.client.RecordProcessor;
import bz.stub.parallelconsumer.proxy.protocol.v1.ClientMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.Configured;
import bz.stub.parallelconsumer.proxy.protocol.v1.Dispatch;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The gRPC transport: the client wrapper over the wire hop to the sidecar proxy. The admin connection opens
 * the one bidirectional session (KTD3), sends {@code Configure} as the first message (R39), and from
 * {@code Configured} onward relays dispatched records to executor threads and their per-record reports back.
 * <p>
 * <b>The dispatch queue between the stream and the executors is KTD39's, rule for rule:</b> the transport
 * thread always reads and never backpressures by not reading (rule 1 - it offers, it does not block); the
 * buffer's depth is the configured max concurrency, the engine's own in-flight ceiling, so overflow is a
 * protocol violation that fails the stream naming the count, never a load condition (rule 2); hand-out to
 * executors is FIFO by arrival and by position within a message (rule 3). The executor count is what
 * {@code Configured} said, once, never revised (KTD38).
 * <p>
 * <b>The client stores nothing per record (KTD8):</b> a dispatch's token rides from the queue to the report on
 * the executor's stack and is echoed verbatim - there is no request map, no dedupe cache, no completion state.
 * A stateless client cannot have a state bug, and every other language mirrors that statelessness.
 * <p>
 * v1 posture: the sidecar binds loopback, so the client connects to {@code 127.0.0.1} plaintext - matching the
 * proxy's KTD11 surface. Spawning the sidecar process is the lifecycle unit's job; this client connects to a
 * port it is given.
 *
 * @author Antony Stubbs
 */
@Slf4j
public class GrpcParallelConsumerClient implements ParallelConsumerClient {

    /** How long the connect-time handshake may take before {@link #poll} gives up. */
    public static final Duration CONNECT_BUDGET = Duration.ofSeconds(30);

    private static final String LOOPBACK_HOST = "127.0.0.1";

    private final int port;
    private final ClientOptions options;

    private final Object transmitLock = new Object();
    private final CompletableFuture<Configured> configured = new CompletableFuture<>();

    private ManagedChannel channel;
    private StreamObserver<ClientMessage> requests;

    /** Guarded by {@link #transmitLock}: once true, nothing more is written to the stream. */
    private boolean streamClosed = false;

    /** KTD39's queue; created by the transport thread when {@code Configured} arrives, before any dispatch. */
    private volatile BlockingQueue<Dispatch> dispatchQueue;

    /** The queue's depth - the configured max concurrency - held for the overflow violation to name (KTD39). */
    private volatile int dispatchQueueDepth;

    private volatile ExecutorService executors;
    private volatile boolean running = false;

    private GrpcParallelConsumerClient(Builder builder) {
        this.port = builder.port;
        this.options = builder.options;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void poll(RecordProcessor processor) {
        synchronized (this) {
            if (channel != null) {
                throw new IllegalStateException("poll may be called at most once per client");
            }
            channel = ManagedChannelBuilder.forAddress(LOOPBACK_HOST, port).usePlaintext().build();
        }
        requests = ProxyServiceGrpc.newStub(channel).session(new SessionObserver());

        // connect-time configuration is the first message on the stream, and the only configuration
        // channel there is (R39/KTD5)
        transmit(ClientMessage.newBuilder().setConfigure(WireMapping.toConfigure(options)).build());

        Configured effective = awaitConfigured();
        int executorCount = Math.max(1, effective.getExecutorCount());

        running = true;
        var threadNumber = new AtomicInteger(1);
        executors = Executors.newFixedThreadPool(executorCount, runnable -> {
            var thread = new Thread(runnable, "pc-grpc-client-executor-" + threadNumber.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        });
        for (int i = 0; i < executorCount; i++) {
            executors.execute(() -> executorLoop(processor));
        }
        log.info("Connected and configured: {} executor(s), dispatch queue depth {}",
                executorCount, effective.getMaxConcurrency());
    }

    private Configured awaitConfigured() {
        try {
            return configured.get(CONNECT_BUDGET.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted awaiting the Configured handshake", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("the proxy refused the session's Configure", e.getCause());
        } catch (TimeoutException e) {
            throw new IllegalStateException(
                    "no Configured arrived within " + CONNECT_BUDGET + " - is the sidecar listening on "
                            + LOOPBACK_HOST + ":" + port + "?", e);
        }
    }

    /**
     * One executor's life: take the next dispatch FIFO, run the processor, report the outcome with the token
     * echoed verbatim. A queued record is already leased and connection-level heartbeats cover it (KTD39 rule
     * 4), so nothing here is time-pressured by queue depth.
     */
    private void executorLoop(RecordProcessor processor) {
        while (running) {
            Dispatch dispatch;
            try {
                dispatch = dispatchQueue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            // KTD8: the token travels dispatch -> report on this stack frame; the client stores nothing
            var outcome = Outcomes.applyProcessor(processor, WireMapping.toInboundRecord(dispatch));
            transmit(ClientMessage.newBuilder()
                    .setReport(WireMapping.toReport(dispatch.getToken(), outcome))
                    .build());
        }
    }

    private void transmit(ClientMessage message) {
        synchronized (transmitLock) {
            if (streamClosed) {
                log.debug("Dropping a {} message: the stream is closed", message.getMessageCase());
                return;
            }
            try {
                requests.onNext(message);
            } catch (RuntimeException e) {
                streamClosed = true;
                log.warn("Stream no longer writable; dropping message and marking closed", e);
            }
        }
    }

    @Override
    public void close() {
        running = false;
        var currentExecutors = executors;
        if (currentExecutors != null) {
            currentExecutors.shutdownNow();
            try {
                currentExecutors.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        synchronized (transmitLock) {
            if (requests != null && !streamClosed) {
                streamClosed = true;
                try {
                    requests.onCompleted();
                } catch (RuntimeException e) {
                    log.debug("Stream refused completion; it was already terminated", e);
                }
            }
        }
        var currentChannel = channel;
        if (currentChannel != null && !currentChannel.isShutdown()) {
            currentChannel.shutdown();
            try {
                currentChannel.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** The admin's inbound half; gRPC serializes these callbacks, so no state here needs its own lock. */
    private class SessionObserver implements StreamObserver<ProxyMessage> {

        @Override
        public void onNext(ProxyMessage message) {
            switch (message.getMessageCase()) {
                case CONFIGURED:
                    onConfigured(message.getConfigured());
                    return;
                case DISPATCH:
                    onDispatch(message.getDispatch());
                    return;
                default:
                    log.warn("Ignoring proxy message with unrecognized case {}", message.getMessageCase());
            }
        }

        private void onConfigured(Configured effective) {
            if (dispatchQueue != null) {
                // a re-sent Configured is the proxy's refusal of a second Configure; this client never sends
                // one, so log and carry on - the original configuration stands
                log.warn("Ignoring an unexpected repeat Configured");
                return;
            }
            // created HERE, on the transport thread, before the future releases poll(): the first dispatch
            // can arrive on this same thread immediately after, and must find the queue existing.
            // KTD39 rule 2: the depth is max concurrency, the engine's own in-flight ceiling.
            dispatchQueueDepth = Math.max(1, effective.getMaxConcurrency());
            dispatchQueue = new ArrayBlockingQueue<>(dispatchQueueDepth);
            configured.complete(effective);
        }

        private void onDispatch(Dispatch dispatch) {
            // KTD39 rule 1: the admin always reads and never backpressures by not reading - so this offers
            // and moves on, and a full queue is a protocol violation (rule 2), not a reason to block a stream
            // that also carries the control plane
            if (dispatchQueue == null || !dispatchQueue.offer(dispatch)) {
                var violation = Status.FAILED_PRECONDITION.withDescription(
                        "protocol violation: dispatch queue overflow - the proxy exceeded the configured max "
                                + "concurrency of " + dispatchQueueDepth + " records in flight (KTD39)");
                log.error("Failing the stream: {}", violation.getDescription());
                synchronized (transmitLock) {
                    if (!streamClosed) {
                        streamClosed = true;
                        requests.onError(violation.asRuntimeException());
                    }
                }
            }
        }

        @Override
        public void onError(Throwable t) {
            synchronized (transmitLock) {
                streamClosed = true;
            }
            configured.completeExceptionally(t);
            if (running) {
                log.warn("Session stream errored", t);
            } else {
                log.debug("Session stream ended during close", t);
            }
        }

        @Override
        public void onCompleted() {
            synchronized (transmitLock) {
                streamClosed = true;
            }
            log.debug("Session stream completed by the proxy");
        }
    }

    public static class Builder {
        private int port;
        private ClientOptions options;

        /** The sidecar's loopback port - the value it reported when it bound. Required. */
        public Builder port(int port) {
            this.port = port;
            return this;
        }

        /** Required. */
        public Builder options(ClientOptions options) {
            this.options = options;
            return this;
        }

        public GrpcParallelConsumerClient build() {
            if (options == null) {
                throw new IllegalStateException("options are required");
            }
            if (port <= 0) {
                throw new IllegalStateException("a positive sidecar port is required");
            }
            return new GrpcParallelConsumerClient(this);
        }
    }
}
