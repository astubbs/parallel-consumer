package bz.stub.parallelconsumer.proxy.config;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.proxy.engine.DispatchSink;
import bz.stub.parallelconsumer.proxy.engine.ProxyProcessor;
import bz.stub.parallelconsumer.proxy.protocol.v1.ClientMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.Configure;
import bz.stub.parallelconsumer.proxy.protocol.v1.Configured;
import bz.stub.parallelconsumer.proxy.protocol.v1.Dispatch;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * The proxy's session service: connect-time configuration and the engine&harr;transport bridge. U7 of the
 * language-proxy plan (astubbs#242); requirements R10, R36, R39, R40, R48; decisions KTD5, KTD11, KTD16, KTD38.
 * <p>
 * <b>The first message on the stream configures the proxy, and nothing before it does (R39).</b> The proxy
 * starts with a listener and no consumer; it builds {@code ParallelConsumerOptions} and constructs the Kafka
 * clients only on receiving {@code Configure}, reading no file, no environment variable and no shell. A stream
 * whose first message is anything else is closed with {@code FAILED_PRECONDITION} - and because the transport's
 * {@code SingleConnectionGuard} releases its admission slot on stream termination, a refused stream frees the
 * slot rather than wedging the proxy, so a corrected client may simply connect again.
 * <p>
 * <b>A second {@code Configure} on a configured stream is refused without killing the session:</b> the proxy
 * re-sends the original effective {@code Configured} unchanged, which is a truthful refusal under the
 * assert-what-you-got contract - the client reads back a configuration that is not what it just asked for.
 * Closing the stream instead would drop a live session's in-flight records over a client bug, with no reconnect
 * reconciliation until U8. The subscription is fixed for the process lifetime either way (R36).
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
 * Not this unit's scope, deliberately: reconnect reconciliation (U8 - a new stream after an earlier one
 * configured the engine is refused naming that unit), terminal-failure replies and protocol-error messages for
 * discarded reports (U9), and shutdown/drain lifecycle (U10 - closing the engine is its owner's job, reachable
 * through {@link #engine()}).
 *
 * @author Antony Stubbs
 * @see OptionsMapper
 * @see ProxyProcessor
 */
@Slf4j
public class ConfigureHandler extends ProxyServiceGrpc.ProxyServiceImplBase {

    /**
     * The capability naming the {@code Dispatch} message type - the one message the proxy sends beyond the
     * handshake in the provisional schema. A client whose declared capability set does not include it receives
     * no dispatches (the negotiated-intersection rule, R38); the schema freeze (U18) completes the set.
     */
    public static final String CAPABILITY_DISPATCH = "dispatch";

    /** Everything this proxy can send beyond the handshake; the intersection with the client's set is what travels. */
    public static final List<String> PROXY_CAPABILITIES = List.of(CAPABILITY_DISPATCH);

    /** Observation seam: fires once, after the engine is subscribed and started, before dispatches can flow. */
    @FunctionalInterface
    public interface EngineStartedListener {
        void engineStarted(ProxyProcessor engine, OptionsMapper.Subscription subscription);
    }

    private final KafkaClientFactory clientFactory;
    private final EngineStartedListener engineStartedListener;

    /** The one engine this process runs; set by the stream that configures it, fixed until process death. */
    private volatile ProxyProcessor engine;

    private ConfigureHandler(Builder builder) {
        this.clientFactory = Objects.requireNonNull(builder.clientFactory, "clientFactory is required");
        this.engineStartedListener = builder.engineStartedListener;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** The engine, once a stream has configured one - the handle its lifecycle owner closes. */
    public Optional<ProxyProcessor> engine() {
        return Optional.ofNullable(engine);
    }

    @Override
    public StreamObserver<ClientMessage> session(StreamObserver<ProxyMessage> responseObserver) {
        return new SessionObserver(responseObserver);
    }

    /**
     * One stream's state machine: awaiting-configure, then configured. gRPC serializes a stream's inbound
     * callbacks, so the state fields need no locking; outbound sends do, because dispatch waves arrive from the
     * engine's control-loop thread while the transport thread may be answering a report or a second Configure.
     */
    private class SessionObserver implements StreamObserver<ClientMessage> {

        private final StreamObserver<ProxyMessage> responseObserver;
        private final Object transmitLock = new Object();

        /** Guarded by {@link #transmitLock}: once true, nothing more is written to the stream. */
        private boolean streamClosed = false;

        /** The effective configuration sent on configure - re-sent, unchanged, as the second-Configure refusal. */
        private Configured effectiveConfiguration;

        private SessionObserver(StreamObserver<ProxyMessage> responseObserver) {
            this.responseObserver = responseObserver;
        }

        @Override
        public void onNext(ClientMessage message) {
            if (effectiveConfiguration == null) {
                if (!message.hasConfigure()) {
                    // R39: nothing before Configure configures - or does anything else. Closing releases the
                    // admission slot (the guard releases on stream termination), so the refusal is recoverable.
                    closeStream(Status.FAILED_PRECONDITION.withDescription(
                            "the first client message on a session must be Configure (R39); got "
                                    + message.getMessageCase() + ". The admission slot is released; connect "
                                    + "again and configure first"));
                    return;
                }
                handleConfigure(message.getConfigure());
                return;
            }
            if (message.hasConfigure()) {
                // deliberately content-free: a Configure can carry credentials, so not even the refusal
                // log may embed it
                log.warn("Refusing a second Configure on a configured stream: configuration is connect-time "
                        + "and the subscription is fixed for the process lifetime (R36, R39). Re-sending the "
                        + "unchanged effective configuration");
                transmit(ProxyMessage.newBuilder().setConfigured(effectiveConfiguration).build());
                return;
            }
            if (message.hasReport()) {
                var result = engine.report(message.getReport());
                switch (result) {
                    case APPLIED_SUCCESS:
                    case APPLIED_FAILURE:
                        break;
                    default:
                        // reply-with-protocol-error is U9's; until then the discard reason is at least visible
                        log.debug("Report discarded by the engine: {}", result);
                }
                return;
            }
            log.warn("Ignoring client message with unrecognized case {}", message.getMessageCase());
        }

        private void handleConfigure(Configure configure) {
            if (engine != null) {
                closeStream(Status.FAILED_PRECONDITION.withDescription(
                        "this proxy is already configured by an earlier connection, and its subscription is "
                                + "fixed for the process lifetime (R36); reconnect reconciliation is not built "
                                + "yet (the language-proxy plan's U8)"));
                return;
            }

            OptionsMapper.Subscription subscription;
            ParallelConsumerOptions.ParallelConsumerOptionsBuilder<byte[], byte[]> optionsBuilder;
            try {
                // both run BEFORE any Kafka client is constructed: a refused Configure costs nothing
                subscription = OptionsMapper.subscriptionOf(configure);
                optionsBuilder = OptionsMapper.toOptionsBuilder(configure);
            } catch (OptionsMapper.ConfigureRejectedException rejected) {
                closeStream(Status.INVALID_ARGUMENT.withDescription(rejected.getMessage()));
                return;
            }

            var negotiatedCapabilities = negotiate(configure.getCapabilitiesList());
            var sink = new StreamDispatchSink(negotiatedCapabilities.contains(CAPABILITY_DISPATCH));

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
                builtEngine = new ProxyProcessor(options, sink);
                if (subscription.isPattern()) {
                    builtEngine.subscribe(Pattern.compile(subscription.pattern()));
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
            var startedEngine = builtEngine;
            engine = startedEngine;

            effectiveConfiguration =
                    OptionsMapper.effectiveConfiguration(options, subscription, negotiatedCapabilities);
            transmit(ProxyMessage.newBuilder().setConfigured(effectiveConfiguration).build());

            // whitelist logging, rebuilt by hand - never the Configure message, whose toString prints the
            // credential map
            log.info("Session configured: subscription {}, maxConcurrency {}, executorCount {}, capabilities {}",
                    subscription.isPattern() ? "pattern " + subscription.pattern() : subscription.topics(),
                    options.getMaxConcurrency(), OptionsMapper.executorCountFor(options),
                    negotiatedCapabilities);

            if (engineStartedListener != null) {
                engineStartedListener.engineStarted(startedEngine, subscription);
            }
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
            if (builtEngine != null) {
                try {
                    // never started, so core's close transitions UNUSED -> CLOSED immediately; the close
                    // funnel's finally tears down the wave-window timer either way
                    builtEngine.close();
                } catch (Exception e) { // Exception, not RuntimeException: core's close sneaky-throws checked ones
                    log.warn("Half-built engine refused to close: {}", e.getClass().getName());
                }
            }
            if (consumer != null) {
                try {
                    consumer.close();
                } catch (Exception e) {
                    log.warn("Consumer built by a failed configure refused to close: {}", e.getClass().getName());
                }
            }
            if (producer != null) {
                try {
                    producer.close();
                } catch (Exception e) {
                    log.warn("Producer built by a failed configure refused to close: {}", e.getClass().getName());
                }
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

        @Override
        public void onError(Throwable t) {
            // the peer went away; the engine keeps running - reconnect reconciliation is U8's
            log.debug("Session stream errored from the client side", t);
            synchronized (transmitLock) {
                streamClosed = true;
            }
        }

        @Override
        public void onCompleted() {
            synchronized (transmitLock) {
                if (!streamClosed) {
                    streamClosed = true;
                    responseObserver.onCompleted();
                }
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

        /**
         * The transport's implementation of the engine's outbound boundary: one {@code ProxyMessage} per
         * {@link Dispatch} until the schema freeze (U18) defines the wave form. Never throws, per the
         * {@link DispatchSink} contract - a closed stream swallows the wave (the records stay registered in
         * flight; U8's liveness machinery is their reclaim path), and a stream whose client did not negotiate
         * the dispatch capability never receives one (R38's intersection rule).
         */
        private class StreamDispatchSink implements DispatchSink {

            private final boolean dispatchNegotiated;

            private StreamDispatchSink(boolean dispatchNegotiated) {
                this.dispatchNegotiated = dispatchNegotiated;
            }

            @Override
            public void dispatch(List<Dispatch> wave) {
                if (!dispatchNegotiated) {
                    log.debug("Dropping a wave of {}: the client did not negotiate the '{}' capability",
                            wave.size(), CAPABILITY_DISPATCH);
                    return;
                }
                for (Dispatch dispatch : wave) {
                    transmit(ProxyMessage.newBuilder().setDispatch(dispatch).build());
                }
            }
        }
    }

    public static class Builder {
        private KafkaClientFactory clientFactory = KafkaClientFactory.production();
        private EngineStartedListener engineStartedListener;

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

        public ConfigureHandler build() {
            return new ConfigureHandler(this);
        }
    }
}
