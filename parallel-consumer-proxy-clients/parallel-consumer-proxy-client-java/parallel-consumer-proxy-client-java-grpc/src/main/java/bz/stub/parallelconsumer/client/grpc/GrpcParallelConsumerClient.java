package bz.stub.parallelconsumer.client.grpc;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.client.AsyncRecordProcessor;
import bz.stub.parallelconsumer.client.ClientOptions;
import bz.stub.parallelconsumer.client.Outcomes;
import bz.stub.parallelconsumer.client.ParallelConsumerClient;
import bz.stub.parallelconsumer.client.RecordProcessor;
import bz.stub.parallelconsumer.proxy.protocol.v1.ClientMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.Configured;
import bz.stub.parallelconsumer.proxy.protocol.v1.Dispatch;
import bz.stub.parallelconsumer.proxy.protocol.v1.DispatchRecord;
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
import java.util.concurrent.CompletionStage;
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
 * <b>Connecting and polling are separate steps</b> ({@link #connect()}, then {@link #poll} or
 * {@link #pollAsync}), because a wrapper in another language needs the negotiated session before it starts
 * handing records to anything: what the engine chose - the executor count, the in-flight ceiling, the
 * capability set - is only knowable after the handshake and is what the wrapper reports to its own user.
 * Calling poll without connecting first still works and connects on the way, so the shape a Java caller
 * already had is unchanged.
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

    /**
     * How long {@link #close} lets records already executing finish and report before it stops waiting.
     * <p>
     * The specification's shutdown rule is stop hand-out, final reports for executing records, <em>then</em>
     * half-close, so this is the budget for the middle step. Past it the stream is marked closed
     * <b>before</b> anything is interrupted, because an interrupted user function produces a "processing was
     * interrupted" failure and transmitting that would invent a verdict for work the user did not decide.
     */
    public static final Duration CLOSE_DRAIN_BUDGET = Duration.ofSeconds(10);

    /**
     * How often an idle executor wakes to re-check whether the session is still running.
     * <p>
     * <b>The wake-up is what makes a dead session releasable at all.</b> An untimed {@code take()} can only be
     * broken by interrupting the thread - and the same interrupt would tear through an executor that is running
     * the user's function, which is exactly the invented verdict {@link #close} exists to avoid. A poll
     * interval costs one wake per executor per interval while idle and nothing at all while records are
     * arriving (a waiting {@code poll} returns the instant one is offered), which buys a session end that needs
     * no interrupt.
     */
    private static final Duration HAND_OUT_POLL_INTERVAL = Duration.ofMillis(100);

    private static final String LOOPBACK_HOST = "127.0.0.1";

    private final int port;
    private final ClientOptions options;

    private final Object transmitLock = new Object();
    private final CompletableFuture<Configured> configured = new CompletableFuture<>();

    /**
     * The session's end, as {@link ParallelConsumerClient#sessionEnd} promises it: completed normally when the
     * session ended cleanly, exceptionally with the cause when it did not. Completed in exactly one place
     * ({@link #endSession}) so no path can end the session without the caller being told.
     */
    private final CompletableFuture<Void> sessionEnd = new CompletableFuture<>();

    /**
     * How many records are out with a processor and not yet reported - a <em>session</em> count, not per-record
     * state, so KTD8's statelessness is intact: no token, no identity, nothing to fence with.
     * <p>
     * It exists because the asynchronous form's executor thread does not wait for the verdict. Without it,
     * {@link #close} would see an idle thread pool and half-close the stream while stages were still on their
     * way to a report, dropping verdicts for work that did finish.
     */
    private final AtomicInteger recordsAwaitingVerdict = new AtomicInteger();

    /**
     * Volatile because {@code close()} reads it from a DIFFERENT thread than the one that assigned it in
     * {@code connect()}. The {@code synchronized (this)} block there gives at-most-once mutual exclusion,
     * which is a different guarantee from visibility: without volatile there is no happens-before edge to the
     * closing thread, so {@code close()} may read {@code null} after {@code connect()} has built the channel -
     * and then never shut it down, leaking the connection and the sidecar's group membership.
     * Found by SpotBugs (IS2_INCONSISTENT_SYNC, "locked 66% of time"); every other cross-thread mutable field
     * on this class was already volatile, so this was the one that was missed rather than a decision.
     */
    private volatile ManagedChannel channel;

    /**
     * Volatile for the same reason, found by looking for other instances of the same defect rather than by the
     * analyser: it is assigned on the connecting thread outside {@code transmitLock} and read by the executor
     * threads inside it.
     */
    private volatile StreamObserver<ClientMessage> requests;

    /** Guarded by {@link #transmitLock}: once true, nothing more is written to the stream. */
    private boolean streamClosed = false;

    /** KTD39's queue; created by the transport thread when {@code Configured} arrives, before any dispatch. */
    private volatile BlockingQueue<DispatchRecord> dispatchQueue;

    /** The queue's depth - the configured max concurrency - held for the overflow violation to name (KTD39). */
    private volatile int dispatchQueueDepth;

    private volatile ExecutorService executors;
    private volatile boolean running = false;

    /** Guarded by {@code synchronized (this)}: at-most-once for the pair of poll methods together. */
    private boolean polled = false;

    private GrpcParallelConsumerClient(Builder builder) {
        this.port = builder.port;
        this.options = builder.options;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Opens the session and completes the handshake: builds the channel, sends {@code Configure} as the
     * stream's first message (R39/KTD5), and completes with what the proxy replied it is running.
     * <p>
     * Idempotent - a second call returns a stage for the same handshake rather than opening a second session,
     * so {@link #poll} may call it without a caller having to know whether one already did. Nothing is
     * dispatched to a processor until a poll method is called; the handshake alone tells the proxy the session
     * exists and what it is configured as.
     *
     * @return the effective, negotiated session; the stage completes exceptionally if the proxy refuses the
     * {@code Configure} or breaks the contract in its reply
     */
    public CompletionStage<NegotiatedSession> connect() {
        synchronized (this) {
            if (channel == null) {
                channel = ManagedChannelBuilder.forAddress(LOOPBACK_HOST, port).usePlaintext().build();
                requests = ProxyServiceGrpc.newStub(channel).session(new SessionObserver());
                // connect-time configuration is the first message on the stream, and the only configuration
                // channel there is (R39/KTD5)
                transmit(ClientMessage.newBuilder().setConfigure(WireMapping.toConfigure(options)).build());
            }
        }
        return configured.thenApply(WireMapping::toNegotiatedSession);
    }

    @Override
    public void poll(RecordProcessor processor) {
        // the synchronous form IS the asynchronous one with a stage that is already complete: the user
        // function still runs on the executor thread that took the record, and that thread still moves on
        // only when the function returns. One loop, so a session bug has one place to live (Outcomes.asAsync)
        pollAsync(Outcomes.asAsync(processor));
    }

    @Override
    public void pollAsync(AsyncRecordProcessor processor) {
        synchronized (this) {
            if (polled) {
                throw new IllegalStateException("poll may be called at most once per client");
            }
            polled = true;
        }
        NegotiatedSession session = awaitConnected();
        int executorCount = Math.max(1, session.executorCount());

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
        if (sessionEnd.isDone()) {
            // the session died between the handshake and here - without this, hand-out would have just been
            // started for a session nothing will ever dispatch on, and `running` would never be read false again
            log.warn("The session ended before polling began; stopping the executors that were just started");
            endSession(null);
            return;
        }
        log.info("Connected and configured: {} executor(s), dispatch queue depth {}",
                executorCount, session.maxConcurrency());
    }

    private NegotiatedSession awaitConnected() {
        try {
            return connect().toCompletableFuture().get(CONNECT_BUDGET.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted awaiting the Configured handshake", e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ProxyProtocolViolation) {
                throw (ProxyProtocolViolation) e.getCause();
            }
            throw new IllegalStateException("the proxy refused the session's Configure", e.getCause());
        } catch (TimeoutException e) {
            throw new IllegalStateException(
                    "no Configured arrived within " + CONNECT_BUDGET + " - is the sidecar listening on "
                            + LOOPBACK_HOST + ":" + port + "?", e);
        }
    }

    @Override
    public CompletionStage<Void> sessionEnd() {
        // a view rather than the future itself: a caller that completed the session's own end would be able to
        // tell every other holder the session finished while it is still consuming
        return sessionEnd.thenApply(ended -> ended);
    }

    /**
     * One executor's life: take the next dispatch FIFO, start the processor, report the outcome with the token
     * echoed verbatim when it arrives. A queued record is already leased and connection-level heartbeats cover
     * it (KTD39 rule 4), so nothing here is time-pressured by queue depth.
     * <p>
     * <b>KTD8 survives the asynchronous form intact:</b> the token travels dispatch to report on this frame
     * when the processor answers inline, and captured in this one completion callback when it does not.
     * Either way it is echoed verbatim and the client holds no map, no dedupe cache and no completion state.
     * <p>
     * Nothing here waits on the stage, deliberately - waiting is what the asynchronous form exists to avoid.
     * A stage that never completes therefore never reports, which is {@link AsyncRecordProcessor}'s way of
     * saying this client has no verdict for that record.
     * <p>
     * <b>The hand-out wait is timed, and that is what ends this loop.</b> Hand-out stops when the session does
     * - a broken stream as much as a {@link #close} - and an executor waiting untimed for a record that will
     * never come is a thread parked for the life of the process, with the application still believing it is
     * consuming. See {@link #HAND_OUT_POLL_INTERVAL} for why the wait is timed rather than interrupted.
     */
    private void executorLoop(AsyncRecordProcessor processor) {
        while (running) {
            DispatchRecord dispatch;
            try {
                dispatch = dispatchQueue.poll(HAND_OUT_POLL_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (dispatch == null) {
                // nothing was handed out this turn: go round, re-read `running`, and leave if the session ended
                continue;
            }
            var token = dispatch.getToken();
            recordsAwaitingVerdict.incrementAndGet();
            Outcomes.applyProcessorAsync(processor, WireMapping.toInboundRecord(dispatch))
                    .whenComplete((outcome, thrown) -> {
                        try {
                            if (outcome != null) {
                                transmit(ClientMessage.newBuilder()
                                        .setReport(WireMapping.toReport(token, outcome))
                                        .build());
                            } else {
                                // Outcomes turns every exceptional completion into a failure Outcome, so this
                                // is unreachable for a conforming stage - and if it is ever reached, saying
                                // nothing is the honest answer rather than a verdict nobody gave
                                log.warn("A record's verdict stage completed with no outcome; reporting nothing "
                                        + "for it, so the engine will redeliver", thrown);
                            }
                        } finally {
                            recordsAwaitingVerdict.decrementAndGet();
                        }
                    });
        }
    }

    private void transmit(ClientMessage message) {
        synchronized (transmitLock) {
            if (streamClosed) {
                if (message.getMessageCase() == ClientMessage.MessageCase.REPORT) {
                    // a verdict the user's function DID reach, lost because the session ended under it - the
                    // engine redelivers the record, so this is a redelivery's cause and not a debug detail
                    log.warn("The session ended before a record's verdict could be sent; the engine will "
                            + "redeliver that record");
                } else {
                    log.debug("Dropping a {} message: the stream is closed", message.getMessageCase());
                }
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

    /**
     * The one place the session ends, whatever ended it: hand-out stops and the executors are told to finish.
     * <p>
     * <b>It never interrupts.</b> Idle executors leave of their own accord at their next hand-out turn, and
     * executing ones run their user function to its verdict - interrupting either is how a client comes to
     * report a failure the user never decided. {@link #close} is the only path that may resort to an interrupt,
     * and it marks the stream closed first so nothing fabricated can be transmitted.
     *
     * @param cause what ended the session, or {@code null} if it ended cleanly
     */
    private void endSession(Throwable cause) {
        running = false;
        var currentExecutors = executors;
        if (currentExecutors != null) {
            // shutdown, never shutdownNow: it refuses new work without touching what is already running
            currentExecutors.shutdown();
        }
        if (cause == null) {
            sessionEnd.complete(null);
        } else {
            sessionEnd.completeExceptionally(cause);
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * The frozen shutdown order, in three steps: stop hand-out, let records already executing reach their
     * verdict and report it, and only then half-close the stream. Queued records that were never handed out are
     * abandoned unreported - the engine redelivers them - because reporting anything for work that did not run
     * would be a verdict this client invented. ({@code Released}, the wire's word for "handed back unrun", is
     * gated behind the {@code shutdown} capability this transport does not negotiate.)
     */
    @Override
    public void close() {
        running = false;
        var currentExecutors = executors;
        if (currentExecutors != null) {
            currentExecutors.shutdown();
            if (!awaitFinalVerdicts(currentExecutors)) {
                // the budget is up with work still outstanding. Marking the stream closed BEFORE interrupting
                // is the whole point of the order: shutdownNow makes a blocked user function throw
                // InterruptedException, which Outcomes reports as a "processing was interrupted" failure - and
                // a transmitted failure is applied engine-side as a real one, consuming a retry attempt for a
                // record whose processing the user never got to decide
                synchronized (transmitLock) {
                    streamClosed = true;
                }
                log.warn("Records were still executing after {}; ending the session without their verdicts, so "
                        + "the engine will redeliver them", CLOSE_DRAIN_BUDGET);
                currentExecutors.shutdownNow();
            }
        }
        var queue = dispatchQueue;
        if (queue != null && !queue.isEmpty()) {
            log.info("{} record(s) were queued and never handed out; no verdict is reported for them and the "
                    + "engine will redeliver them", queue.size());
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
        endSession(null);
    }

    /**
     * Waits out {@link #CLOSE_DRAIN_BUDGET} for both halves of "executing records finish and report": the
     * executor threads leaving their loop, and the verdicts still travelling from an asynchronous processor's
     * stage to the wire.
     *
     * @return whether everything reported within the budget
     */
    private boolean awaitFinalVerdicts(ExecutorService currentExecutors) {
        long deadline = System.nanoTime() + CLOSE_DRAIN_BUDGET.toNanos();
        try {
            if (!currentExecutors.awaitTermination(CLOSE_DRAIN_BUDGET.toMillis(), TimeUnit.MILLISECONDS)) {
                return false;
            }
            while (recordsAwaitingVerdict.get() > 0) {
                if (System.nanoTime() - deadline >= 0) {
                    return false;
                }
                TimeUnit.MILLISECONDS.sleep(HAND_OUT_POLL_INTERVAL.toMillis());
            }
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
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
                    // Drop and Shutdown are frozen-schema messages this spike-stage transport does not
                    // implement yet (the plan's U25 grows it to the full protocol); SetExecutorCount is
                    // declared-unused and never sent by a v1 proxy (KTD38)
                    log.warn("Ignoring proxy message this client does not implement yet: {}",
                            message.getMessageCase());
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

        private void onDispatch(Dispatch wave) {
            // KTD39 rule 1: the admin always reads and never backpressures by not reading - so this offers
            // and moves on, and a full queue is a protocol violation (rule 2), not a reason to block a stream
            // that also carries the control plane. Queueing is per record, in wave order (rule 3): one
            // multi-record Dispatch is the frozen wave form (R50).
            for (DispatchRecord dispatch : wave.getRecordsList()) {
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
                    // the caller learns it as the violation rather than as the CANCELLED the cancelled call
                    // would otherwise deliver, so the reason names the proxy's fault and the count
                    endSession(new ProxyProtocolViolation(violation.getDescription()));
                    return;
                }
            }
        }

        /**
         * The session died under us. Both halves matter and the second is the one that was missing: the stream
         * is unwritable, <b>and consumption has stopped</b> - so hand-out ends, the executors leave, and the
         * caller's {@link #sessionEnd} stage carries the cause. Without that, {@link #poll} had already
         * returned, every executor sat in an untimed hand-out wait, and nothing on this surface could tell the
         * application it had stopped consuming.
         */
        @Override
        public void onError(Throwable t) {
            synchronized (transmitLock) {
                streamClosed = true;
            }
            configured.completeExceptionally(t);
            if (running) {
                log.warn("Session stream errored; ending the session and stopping the executors", t);
            } else {
                log.debug("Session stream ended during close", t);
            }
            endSession(t);
        }

        @Override
        public void onCompleted() {
            synchronized (transmitLock) {
                streamClosed = true;
            }
            log.debug("Session stream completed by the proxy");
            if (!configured.isDone()) {
                // otherwise a connect that will never be answered waits out the whole CONNECT_BUDGET
                configured.completeExceptionally(new ProxyProtocolViolation(
                        "the proxy completed the session stream without ever sending Configured"));
            }
            // a stream the proxy completed is a session that ended, not merely a stream that stopped: the
            // executors have nothing more coming, and a clean end is a clean completion of the caller's stage
            endSession(null);
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
