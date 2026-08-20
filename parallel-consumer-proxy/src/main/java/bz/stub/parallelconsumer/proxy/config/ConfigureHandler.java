package bz.stub.parallelconsumer.proxy.config;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.proxy.engine.DispatchSink;
import bz.stub.parallelconsumer.proxy.engine.LivenessSettings;
import bz.stub.parallelconsumer.proxy.engine.ProxyProcessor;
import bz.stub.parallelconsumer.proxy.protocol.v1.ClientMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.Configure;
import bz.stub.parallelconsumer.proxy.protocol.v1.Configured;
import bz.stub.parallelconsumer.proxy.protocol.v1.Dispatch;
import bz.stub.parallelconsumer.proxy.protocol.v1.Drop;
import bz.stub.parallelconsumer.proxy.protocol.v1.Manifest;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.Shutdown;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyServiceGrpc;
import bz.stub.parallelconsumer.proxy.protocol.v1.Report;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * The proxy's session service: connect-time configuration, reconnect reconciliation, and the
 * engine&harr;transport bridge. U7 and U8 of the language-proxy plan (astubbs#242); requirements R10, R36, R39,
 * R40, R42, R43, R45, R46, R48; decisions KTD5, KTD8, KTD11, KTD16, KTD38.
 * <p>
 * <b>The first message on a fresh session configures the proxy, and nothing before it does (R39).</b> The proxy
 * starts with a listener and no consumer; it builds {@code ParallelConsumerOptions} and constructs the Kafka
 * clients only on receiving {@code Configure}, reading no file, no environment variable and no shell. A stream
 * whose first message is anything else is closed with {@code FAILED_PRECONDITION} - and because the transport's
 * {@code SingleConnectionGuard} releases its admission slot on stream termination, a refused stream frees the
 * slot rather than wedging the proxy, so a corrected client may simply connect again.
 * <p>
 * <b>The first message on a RECONNECT stream is {@code Manifest}, and it carries no {@code Configure} (R43).</b>
 * The configured session outlives its connection: the engine, the negotiated capability set and the effective
 * configuration all survive, and the reconnecting stream inherits them rather than negotiating anything of its
 * own. Reconciliation happens before the reply, so the {@code Configured} echo the client reads back is a
 * session whose books already balance; the {@code Drop} orders follow it, and dispatching resumes after those.
 * A {@code Configure} on such a stream is still refused - the subscription is fixed for the process lifetime.
 * <p>
 * <b>A second {@code Configure} on a configured stream is refused without killing the session:</b> the proxy
 * re-sends the original effective {@code Configured} unchanged, which is a truthful refusal under the
 * assert-what-you-got contract - the client reads back a configuration that is not what it just asked for.
 * Closing the stream instead would drop a live session's in-flight records over a client bug.
 * <p>
 * <b>Credential hygiene (R48/KTD11):</b> {@code kafka_properties} is handed to the {@link KafkaClientFactory}
 * and appears in no log line at any level. Concretely: no log statement in this class receives the
 * {@code Configure} message (whose protobuf {@code toString} prints the map), the property map, or any value
 * from it - session logging names whitelisted fields (subscription, counts) rebuilt by hand. The
 * {@code Configured} echo cannot leak the map because the wire message has no field for it.
 * <p>
 * <b>The executor count (KTD38/R47)</b> travels once in {@code Configured}, computed by
 * {@link OptionsMapper#EXECUTOR_COUNT_FUNCTION} from connect-time configuration only, and is never revised.
 * <p>
 * Not this unit's scope, deliberately: terminal-failure replies and protocol-error messages for discarded
 * reports (U9), and shutdown/drain lifecycle (U10 - closing the engine is its owner's job, reachable through
 * {@link #engine()}).
 *
 * @author Antony Stubbs
 * @see OptionsMapper
 * @see ProxyProcessor
 */
@Slf4j
public class ConfigureHandler extends ProxyServiceGrpc.ProxyServiceImplBase {

    /**
     * The capability token gating {@code Dispatch} waves - one of the frozen specification's v1 baseline
     * tokens ({@code parallel-consumer-proxy/docs/protocol-specification.md}, "Capabilities and
     * versioning"). A client whose declared capability set does not include it receives no dispatches (the
     * negotiated-intersection rule, R38).
     */
    public static final String CAPABILITY_DISPATCH = "dispatch";

    /** Gates {@code Heartbeat} and the whole lease semantics: without it, no lease clock runs at all (R46). */
    public static final String CAPABILITY_HEARTBEAT = "heartbeat";

    /** Gates {@code Manifest} reconnects and the {@code Drop} replies reconciliation produces (R43). */
    public static final String CAPABILITY_MANIFEST = "manifest";

    /** Gates {@code WorkerDied} - the primary reclaim path, ahead of both backstop clocks (R45). */
    public static final String CAPABILITY_WORKER_DEATH = "worker-death";

    /**
     * Everything this proxy can send or answer beyond the handshake; the intersection with the client's set is
     * what travels. Grows towards the specification's full v1 baseline as the engine units land the behaviours
     * behind the remaining tokens (terminal in U9, shutdown in U10) - a token is declared here only once the
     * proxy actually answers it, so the negotiation never promises what this build cannot do.
     */
    public static final List<String> PROXY_CAPABILITIES =
            List.of(CAPABILITY_DISPATCH, CAPABILITY_HEARTBEAT, CAPABILITY_MANIFEST, CAPABILITY_WORKER_DEATH);

    /** Observation seam: fires once, after the engine is subscribed and started, before dispatches can flow. */
    @FunctionalInterface
    public interface EngineStartedListener {
        void engineStarted(ProxyProcessor engine, OptionsMapper.Subscription subscription);
    }

    private final KafkaClientFactory clientFactory;
    private final EngineStartedListener engineStartedListener;
    private final Clock clock;

    /**
     * Routes dispatch waves at whichever stream currently holds the session - the indirection reconnect needs,
     * because the engine is constructed once with one sink and outlives every connection that follows.
     */
    private final SessionRouter router = new SessionRouter();

    /** The one engine this process runs; set by the stream that configures it, fixed until process death. */
    private volatile ProxyProcessor engine;

    /** Negotiated once, on the configuring stream, and surviving every connection loss (the spec's rule). */
    private volatile List<String> negotiatedCapabilities = List.of();

    /** The effective configuration sent on configure - re-sent unchanged on reconnect and on a second one. */
    private volatile Configured effectiveConfiguration;

    private ConfigureHandler(Builder builder) {
        this.clientFactory = Objects.requireNonNull(builder.clientFactory, "clientFactory is required");
        this.engineStartedListener = builder.engineStartedListener;
        this.clock = Objects.requireNonNull(builder.clock, "clock is required");
    }

    public static Builder builder() {
        return new Builder();
    }

    /** The engine, once a stream has configured one - the handle its lifecycle owner closes. */
    /** Stop dispatching new work - the drain's first step (KTD17). */
    public void stopAcceptingNewWork() {
        router.stopDispatching();
    }

    /**
     * Ask the client to wind down. Sent BEFORE the drain waits, so the client has a reason to return what it
     * is holding rather than being waited out.
     *
     * @return false when no client stream is connected
     */
    public boolean tellClientToShutDown() {
        return router.sendShutdown();
    }

    public Optional<ProxyProcessor> engine() {
        return Optional.ofNullable(engine);
    }

    @Override
    public StreamObserver<ClientMessage> session(StreamObserver<ProxyMessage> responseObserver) {
        return new SessionObserver(responseObserver);
    }

    /**
     * One stream's state machine: awaiting its opening message, then established. gRPC serializes a stream's
     * inbound callbacks, so the state fields need no locking; outbound sends do, because dispatch waves arrive
     * from the engine's control-loop thread while the transport thread may be answering a report or a manifest.
     */
    private class SessionObserver implements StreamObserver<ClientMessage> {

        private final StreamObserver<ProxyMessage> responseObserver;
        private final Object transmitLock = new Object();

        /** Guarded by {@link #transmitLock}: once true, nothing more is written to the stream. */
        private boolean streamClosed = false;

        /** Whether this stream got past its opening message - by configuring, or by reconnecting. */
        private volatile boolean established;

        private SessionObserver(StreamObserver<ProxyMessage> responseObserver) {
            this.responseObserver = responseObserver;
        }

        @Override
        public void onNext(ClientMessage message) {
            if (!established) {
                handleOpeningMessage(message);
                return;
            }
            switch (message.getMessageCase()) {
                case CONFIGURE:
                    // deliberately content-free: a Configure can carry credentials, so not even the refusal
                    // log may embed it
                    log.warn("Refusing a second Configure on a configured stream: configuration is connect-time "
                            + "and the subscription is fixed for the process lifetime (R36, R39). Re-sending "
                            + "the unchanged effective configuration");
                    transmit(ProxyMessage.newBuilder().setConfigured(effectiveConfiguration).build());
                    return;
                case REPORT:
                    onReport(message.getReport());
                    return;
                case HEARTBEAT:
                    if (negotiated(CAPABILITY_HEARTBEAT)) {
                        engine.heartbeat();
                    } else {
                        logUnnegotiated(message);
                    }
                    return;
                case WORKER_DIED:
                    if (negotiated(CAPABILITY_WORKER_DEATH)) {
                        engine.onWorkerDied(message.getWorkerDied().getTokensList());
                    } else {
                        logUnnegotiated(message);
                    }
                    return;
                case MANIFEST:
                    // a manifest opens a reconnect stream and nothing else: mid-session it names a set of held
                    // tokens that the live stream has never stopped reporting on, so acting on it could return
                    // records a worker is running right now
                    log.warn("Ignoring a Manifest on an established stream: it is a reconnect stream's opening "
                            + "message only (R43)");
                    return;
                default:
                    // a truly unknown case is a newer client than this proxy; ignored rather than fatal, per
                    // the specification's forward-compatibility rule
                    log.warn("Ignoring client message this proxy does not implement: {}",
                            message.getMessageCase());
            }
        }

        private void onReport(Report report) {
            var result = engine.report(report);
            switch (result) {
                case APPLIED_SUCCESS:
                case APPLIED_FAILURE:
                case ACCEPTED_PRODUCING:
                    // accepted, not discarded: the record is claimed, and its produce payload's acks are
                    // awaited on the engine's own lane precisely so this callback - the session's single
                    // serialized inbound lane, which also carries Heartbeat - is not held by a broker
                    break;
                default:
                    // reply-with-protocol-error is U9's; until then the discard reason is at least visible
                    log.debug("Report discarded by the engine: {}", result);
            }
        }

        /**
         * The opening message of a stream: {@code Configure} on a fresh session, {@code Manifest} on a
         * reconnect. Which one is legal is decided by whether this process already has an engine, so a client
         * cannot reconfigure a running session by reconnecting, and cannot reconcile one that never existed.
         */
        private void handleOpeningMessage(ClientMessage message) {
            if (engine == null) {
                if (message.hasConfigure()) {
                    handleConfigure(message.getConfigure());
                    return;
                }
                // R39: nothing before Configure configures - or does anything else. Closing releases the
                // admission slot (the guard releases on stream termination), so the refusal is recoverable.
                closeStream(Status.FAILED_PRECONDITION.withDescription(
                        "the first client message on a session must be Configure (R39); got "
                                + message.getMessageCase() + ". The admission slot is released; connect "
                                + "again and configure first"));
                return;
            }
            if (message.hasManifest()) {
                handleReconnect(message.getManifest());
                return;
            }
            if (message.hasConfigure()) {
                closeStream(Status.FAILED_PRECONDITION.withDescription(
                        "this proxy is already configured by an earlier connection, and its subscription is "
                                + "fixed for the process lifetime (R36); a reconnect stream opens with a "
                                + "Manifest of the tokens your live workers still hold, never a Configure (R43)"));
                return;
            }
            closeStream(Status.FAILED_PRECONDITION.withDescription(
                    "the first message on a reconnect stream must be Manifest (R43); got "
                            + message.getMessageCase() + ". The admission slot is released; connect again"));
        }

        private void handleConfigure(Configure configure) {
            var capabilities = negotiate(configure.getCapabilitiesList());

            OptionsMapper.Subscription subscription;
            ParallelConsumerOptions.ParallelConsumerOptionsBuilder<byte[], byte[]> optionsBuilder;
            LivenessSettings liveness;
            try {
                // all three run BEFORE any Kafka client is constructed: a refused Configure costs nothing
                subscription = OptionsMapper.subscriptionOf(configure);
                optionsBuilder = OptionsMapper.toOptionsBuilder(configure);
                liveness = OptionsMapper.livenessSettingsOf(configure, capabilities, clock);
            } catch (OptionsMapper.ConfigureRejectedException rejected) {
                closeStream(Status.INVALID_ARGUMENT.withDescription(rejected.getMessage()));
                return;
            }

            // R48: the credential map becomes the real clients here, and is never referenced again. Anything
            // in this region can throw on client-suppliable input (a Kafka client constructor rejecting the
            // property map is the live case), and by then real resources exist - a KafkaProducer's Sender
            // thread, the engine's wave-window timer - so a throw must release whatever was built, or every
            // reconnect leaks another set (F1 of the U7 review).
            ParallelConsumerOptions<byte[], byte[]> options;
            Consumer<byte[], byte[]> consumer = null;
            Producer<byte[], byte[]> producer = null;
            ProxyProcessor builtEngine = null;
            try {
                consumer = clientFactory.consumer(configure.getKafkaPropertiesMap());
                producer = clientFactory.producer(configure.getKafkaPropertiesMap());
                options = optionsBuilder.consumer(consumer).producer(producer).build();
                builtEngine = new ProxyProcessor(options, router, ProxyProcessor.DEFAULT_COALESCING_WINDOW,
                        liveness);
                if (subscription.isPattern()) {
                    builtEngine.subscribe(subscription.compiledPattern());
                } else {
                    builtEngine.subscribe(subscription.topics());
                }
                builtEngine.start();
            } catch (RuntimeException constructionFailure) {
                releaseHalfBuilt(builtEngine, consumer, producer);
                // R48 hygiene: name ONLY the exception class - a ConfigException's message embeds property
                // VALUES from the credential map, so neither the message nor the exception itself may reach
                // the stream or a log line
                var status = constructionFailure instanceof KafkaException
                        ? Status.INVALID_ARGUMENT : Status.INTERNAL;
                closeStream(status.withDescription(
                        "constructing the session's Kafka clients and engine failed with "
                                + constructionFailure.getClass().getName() + "; the proxy remains "
                                + "unconfigured and the admission slot is released - correct the "
                                + "configuration and connect again. (The reason is withheld from this "
                                + "message deliberately: Kafka's configuration exceptions embed property "
                                + "values, and kafka_properties may carry credentials - R48)"));
                return;
            }
            negotiatedCapabilities = capabilities;
            effectiveConfiguration =
                    OptionsMapper.effectiveConfiguration(options, subscription, capabilities, liveness);
            engine = builtEngine;

            // bind before the reply: the client may report the moment Configured arrives, and a wave
            // dispatched in that instant must have somewhere to go
            established = true;
            router.bind(this);
            transmit(ProxyMessage.newBuilder().setConfigured(effectiveConfiguration).build());

            // whitelist logging, rebuilt by hand - never the Configure message, whose toString prints the
            // credential map
            log.info("Session configured: subscription {}, maxConcurrency {}, executorCount {}, capabilities {}",
                    subscription.isPattern() ? "pattern " + subscription.pattern() : subscription.topics(),
                    options.getMaxConcurrency(), OptionsMapper.executorCountFor(options), capabilities);

            if (engineStartedListener != null) {
                engineStartedListener.engineStarted(builtEngine, subscription);
            }
        }

        /**
         * A reconnect stream's opening {@code Manifest} (R43): reconcile first, then reply with the unchanged
         * effective {@code Configured}, then the {@code Drop} orders reconciliation produced. Dispatching
         * resumes only once this stream holds the session, so no wave is written to a stream that has not yet
         * been told what it is connected to.
         */
        private void handleReconnect(Manifest manifest) {
            if (!negotiated(CAPABILITY_MANIFEST)) {
                closeStream(Status.FAILED_PRECONDITION.withDescription(
                        "this session did not negotiate the '" + CAPABILITY_MANIFEST + "' capability, so it "
                                + "has no reconnect path; the original session's negotiated set governs every "
                                + "stream that follows (R38, R43)"));
                return;
            }
            var outcome = engine.reconcileManifest(manifest.getTokensList());

            established = true;
            router.bind(this);
            transmit(ProxyMessage.newBuilder().setConfigured(effectiveConfiguration).build());
            outcome.drops().forEach(token -> transmit(ProxyMessage.newBuilder()
                    .setDrop(Drop.newBuilder().setToken(token))
                    .build()));
            log.info("Reconnected within the protection window: {} record(s) kept in flight, {} dropped, {} "
                            + "returned to scheduling, {} manifest token(s) rejected",
                    outcome.kept(), outcome.drops().size(), outcome.returned(), outcome.unissued().size());
        }

        /**
         * Releases whatever a failed configure managed to build, most-derived first. Each release is
         * independent - one refusing does not strand the others - and each failure is logged by class name
         * only, because a Kafka client's close-time exceptions can embed its configuration the same way its
         * constructor's do (R48).
         */
        private void releaseHalfBuilt(ProxyProcessor builtEngine,
                                      Consumer<byte[], byte[]> consumer,
                                      Producer<byte[], byte[]> producer) {
            // the engine was never started, so core's close transitions UNUSED -> CLOSED immediately; the
            // close funnel's finally tears down the wave-window timer either way
            releaseQuietly("Half-built engine", builtEngine);
            releaseQuietly("Consumer built by a failed configure", consumer);
            releaseQuietly("Producer built by a failed configure", producer);
        }

        /**
         * One release: absent is nothing to do, and a refusal is logged by class name only rather than
         * propagated, so the releases after it still run.
         *
         * @param resource {@code AutoCloseable} rather than a narrower type because core's close sneaky-throws
         *                 checked exceptions, which is also why the catch is {@code Exception}
         */
        private void releaseQuietly(String what, AutoCloseable resource) {
            if (resource == null) {
                return;
            }
            try {
                resource.close();
            } catch (Exception e) {
                log.warn("{} refused to close: {}", what, e.getClass().getName());
            }
        }

        private List<String> negotiate(List<String> declared) {
            if (declared.isEmpty()) {
                // a pre-capability client declares nothing and gets the v1 baseline, not silence
                return PROXY_CAPABILITIES;
            }
            var intersection = new ArrayList<>(PROXY_CAPABILITIES);
            intersection.retainAll(declared);
            return List.copyOf(intersection);
        }

        private void logUnnegotiated(ClientMessage message) {
            log.warn("Ignoring a {} the session did not negotiate: neither side sends outside the negotiated "
                    + "capability set (R38)", message.getMessageCase());
        }

        @Override
        public void onError(Throwable t) {
            log.debug("Session stream errored from the client side", t);
            synchronized (transmitLock) {
                streamClosed = true;
            }
            onStreamGone();
        }

        @Override
        public void onCompleted() {
            synchronized (transmitLock) {
                if (!streamClosed) {
                    streamClosed = true;
                    responseObserver.onCompleted();
                }
            }
            onStreamGone();
        }

        /**
         * The peer went away. The engine keeps running and keeps its records: only the stream that actually
         * holds the session may declare the connection lost, because a reconnect can be admitted before the
         * old stream's termination callback arrives, and a late one must not suspend the leases of the live
         * session that replaced it.
         */
        private void onStreamGone() {
            if (router.unbind(this) && engine != null) {
                engine.onConnectionLost();
            }
        }

        private void transmit(ProxyMessage message) {
            synchronized (transmitLock) {
                if (streamClosed) {
                    log.debug("Dropping a {} message: the stream is closed", message.getMessageCase());
                    return;
                }
                try {
                    responseObserver.onNext(message);
                } catch (RuntimeException e) {
                    streamClosed = true;
                    log.debug("Stream no longer writable; dropping message and marking closed", e);
                }
            }
        }

        private void closeStream(Status status) {
            synchronized (transmitLock) {
                if (streamClosed) {
                    return;
                }
                streamClosed = true;
                try {
                    responseObserver.onError(status.asRuntimeException());
                } catch (RuntimeException e) {
                    log.debug("Stream refused the close status; it was already terminated", e);
                }
            }
        }
    }

    private boolean negotiated(String capability) {
        return negotiatedCapabilities.contains(capability);
    }

    /**
     * The engine's outbound boundary, held for the life of the engine rather than the life of a connection:
     * one {@code ProxyMessage} per wave, the frozen multi-record {@link Dispatch} form (R50). Never throws,
     * per the {@link DispatchSink} contract - a wave with no stream to write to is swallowed and the records
     * stay registered in flight, where the reconnect window and the manifest are their reclaim path, and a
     * session whose client did not negotiate the dispatch capability never receives one (R38's intersection
     * rule).
     */
    private class SessionRouter implements DispatchSink {

        /** The stream currently holding the session; null between a connection loss and its replacement. */
        private volatile SessionObserver active;

        /** Set by the drain. Once true the in-flight set can only shrink, which is what makes a drain finite. */
        private volatile boolean windingDown;

        private synchronized void bind(SessionObserver observer) {
            active = observer;
        }

        /** @return true only if this observer was the one holding the session - see {@code onStreamGone} */
        private synchronized boolean unbind(SessionObserver observer) {
            if (active != observer) {
                return false;
            }
            active = null;
            return true;
        }

        @Override
        public void dispatch(Dispatch wave) {
            if (!negotiated(CAPABILITY_DISPATCH)) {
                log.debug("Dropping a wave of {}: the client did not negotiate the '{}' capability",
                        wave.getRecordsCount(), CAPABILITY_DISPATCH);
                return;
            }
            if (windingDown) {
                log.debug("Dropping a wave of {}: the sidecar is winding down", wave.getRecordsCount());
                return;
            }
            var target = active;
            if (target == null) {
                log.debug("Dropping a wave of {}: no client stream holds the session. The records stay in "
                        + "flight for the reconnect machinery", wave.getRecordsCount());
                return;
            }
            target.transmit(ProxyMessage.newBuilder().setDispatch(wave).build());
        }

        /**
         * Drops new waves from here on. Undispatched records are NOT stranded: never having reached the
         * client, they are not in the in-flight registry, so they neither hold the drain open nor get
         * committed - they stay uncommitted and are redelivered, which is the same treatment this router
         * already gives a wave that arrives while no client stream holds the session.
         */
        private void stopDispatching() {
            windingDown = true;
        }

        /**
         * Tells the client to stop handing records to its workers and report what it already holds. Not
         * capability-gated: shutdown is not an optional dialect feature, and a client that could not be told
         * to wind down would be one the drain could only ever wait out.
         *
         * @return false when no stream holds the session - there is nobody to tell, which is not an error
         */
        private boolean sendShutdown() {
            var target = active;
            if (target == null) {
                log.debug("No client stream holds the session; nobody to send Shutdown to");
                return false;
            }
            target.transmit(ProxyMessage.newBuilder().setShutdown(Shutdown.newBuilder().build()).build());
            return true;
        }
    }

    public static class Builder {
        private KafkaClientFactory clientFactory = KafkaClientFactory.production();
        private EngineStartedListener engineStartedListener;
        private Clock clock = Clock.systemUTC();

        /** Defaults to {@link KafkaClientFactory#production()}; test fixtures substitute mock clients here. */
        public Builder clientFactory(KafkaClientFactory clientFactory) {
            this.clientFactory = clientFactory;
            return this;
        }

        /** Optional observation seam - the test harness uses it for partition assignment and seeding. */
        public Builder engineStartedListener(EngineStartedListener engineStartedListener) {
            this.engineStartedListener = engineStartedListener;
            return this;
        }

        /** The clock the session's lease and reconnect window are measured against; the system clock unless a
         * test needs to advance time rather than sleep through it. */
        public Builder clock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public ConfigureHandler build() {
            return new ConfigureHandler(this);
        }
    }
}
