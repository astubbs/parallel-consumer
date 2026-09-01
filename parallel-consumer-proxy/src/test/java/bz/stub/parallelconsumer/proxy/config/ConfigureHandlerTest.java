package bz.stub.parallelconsumer.proxy.config;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import bz.stub.parallelconsumer.proxy.protocol.v1.ClientMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.Configure;
import bz.stub.parallelconsumer.proxy.protocol.v1.Configured;
import bz.stub.parallelconsumer.proxy.protocol.v1.Manifest;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProxyMessage;
import bz.stub.parallelconsumer.proxy.protocol.v1.Report;
import bz.stub.parallelconsumer.proxy.protocol.v1.Token;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The stream state machine of connect-time configuration (the language-proxy plan's U7), exercised in-memory -
 * {@code session(..)} invoked directly, no netty - so the assertions are about THIS class's behaviour: the
 * Configure-first rule, the second-Configure refusal, capability negotiation, and above all credential hygiene,
 * proven by capturing every log line at TRACE and grepping for the test credential.
 * <p>
 * The same service behind the real wire is {@code TestModeMainTest}'s end-to-end scenario.
 *
 * @author Antony Stubbs
 */
@Timeout(120)
class ConfigureHandlerTest {

    static final String TOPIC = "configure-handler-test";

    static final String SECRET = "super-secret-sasl-password-7a1";

    static final Map<String, String> CREDENTIALS = Map.of(
            "bootstrap.servers", "localhost:9092",
            "group.id", "proxy-under-test",
            "sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required "
                    + "username=\"proxy\" password=\"" + SECRET + "\";");

    /** How long a negative control watches for a message that must not arrive. */
    static final Duration NEGATIVE_CONTROL_BUDGET = Duration.ofSeconds(2);

    private final LongPollingMockConsumer<byte[], byte[]> mockConsumer =
            new LongPollingMockConsumer<>(OffsetResetStrategy.EARLIEST);

    private final MockProducer<byte[], byte[]> mockProducer =
            new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());

    /** Records what the handler hands the factory - the "credentials reach the constructed consumer" probe. */
    private final AtomicReference<Map<String, String>> propertiesTheFactorySaw = new AtomicReference<>();

    private final AtomicInteger clientsConstructed = new AtomicInteger();

    private ConfigureHandler handler;

    private ConfigureHandler newHandler() {
        handler = ConfigureHandler.builder()
                .clientFactory(recordingMockFactory())
                .build();
        return handler;
    }

    private ConfigureHandler newHandlerWithSeededRecord() {
        handler = ConfigureHandler.builder()
                .clientFactory(recordingMockFactory())
                .engineStartedListener((engine, subscription) -> {
                    // the manual rebalance dance the harness documents, then one record to dispatch
                    mockConsumer.subscribeWithRebalanceAndAssignment(List.of(TOPIC), 1);
                    engine.onPartitionsAssigned(List.of(new TopicPartition(TOPIC, 0)));
                    mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0,
                            "key".getBytes(StandardCharsets.UTF_8), "hello".getBytes(StandardCharsets.UTF_8)));
                })
                .build();
        return handler;
    }

    private KafkaClientFactory recordingMockFactory() {
        return new KafkaClientFactory() {
            @Override
            public Consumer<byte[], byte[]> consumer(Map<String, String> kafkaProperties) {
                propertiesTheFactorySaw.set(kafkaProperties);
                clientsConstructed.incrementAndGet();
                return mockConsumer;
            }

            @Override
            public Producer<byte[], byte[]> producer(Map<String, String> kafkaProperties) {
                clientsConstructed.incrementAndGet();
                return mockProducer;
            }
        };
    }

    @AfterEach
    void closeEngine() {
        if (handler != null) {
            handler.engine().ifPresent(engine -> {
                if (!engine.isClosedOrFailed()) {
                    engine.close();
                }
            });
        }
    }

    /** R39: any first message other than Configure closes the stream, and nothing real gets constructed. */
    @Test
    void aFirstMessageOtherThanConfigureClosesTheStream() {
        var session = new RecordingSession(newHandler());

        session.send(ClientMessage.newBuilder()
                .setReport(Report.newBuilder()
                        .setToken(Token.newBuilder().setRecordId("t/0/0").setEpoch(1))
                        .setSuccess(Report.Success.newBuilder()))
                .build());

        var status = Status.fromThrowable(session.awaitError());
        assertThat(status.getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
        assertThat(status.getDescription()).contains("Configure");
        assertWithMessage("a refused stream must have constructed no Kafka client")
                .that(clientsConstructed.get()).isEqualTo(0);
        assertThat(handler.engine().isPresent()).isFalse();
    }

    /** R48: the credential map reaches the factory that constructs the consumer, verbatim. */
    @Test
    void credentialsFromConfigureReachTheClientFactory() {
        var session = new RecordingSession(newHandler());

        session.send(configureMessage(Configure.newBuilder()
                .addTopics(TOPIC)
                .setMaxConcurrency(3)
                .putAllKafkaProperties(CREDENTIALS)));

        var configured = session.awaitConfigured();
        assertThat(configured.getMaxConcurrency()).isEqualTo(3);
        assertThat(configured.getExecutorCount()).isEqualTo(3);
        assertThat(propertiesTheFactorySaw.get()).containsEntry("sasl.jaas.config",
                CREDENTIALS.get("sasl.jaas.config"));
        assertWithMessage("the effective echo must not carry the credential map")
                .that(configured.toString()).doesNotContain(SECRET);
    }

    /**
     * The hygiene gate: with EVERY logger capturing at TRACE, a full session - configure with credentials,
     * a refused second Configure, a discarded report - writes the credential to no log line at any level.
     */
    @Test
    void credentialsAppearInNoLogLineAtAnyLevel() {
        var root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        var previousLevel = root.getLevel();
        var capture = new ListAppender<ILoggingEvent>();
        // Configure starts a real engine, and its threads keep logging for as long as this test runs - so the
        // capture is WRITTEN by them while it is READ here. ListAppender's own list is a bare ArrayList, which
        // makes that a data race: in CI an append landed inside the scan below and threw
        // ConcurrentModificationException. Detaching first does not close the window - an append already inside
        // doAppend completes afterwards. A synchronized list, scanned under its own monitor, does close it, and
        // asserts on exactly the same events.
        capture.list = Collections.synchronizedList(new ArrayList<>());
        capture.start();
        root.addAppender(capture);
        root.setLevel(Level.TRACE);
        try {
            var session = new RecordingSession(newHandler());
            session.send(configureMessage(Configure.newBuilder()
                    .addTopics(TOPIC)
                    .putAllKafkaProperties(CREDENTIALS)));
            session.awaitConfigured();

            // the refusal path must be as silent about the map as the happy path
            session.send(configureMessage(Configure.newBuilder()
                    .addTopics("some-other-topic")
                    .putAllKafkaProperties(CREDENTIALS)));
            session.awaitConfigured();

            // and the report path, which logs discards
            session.send(ClientMessage.newBuilder()
                    .setReport(Report.newBuilder()
                            .setToken(Token.newBuilder().setRecordId("unknown/0/0").setEpoch(1))
                            .setSuccess(Report.Success.newBuilder()))
                    .build());
        } finally {
            root.setLevel(previousLevel);
            root.detachAppender(capture);
            capture.stop();
        }

        synchronized (capture.list) {
            for (ILoggingEvent event : capture.list) {
                assertWithMessage("log line leaks the credential (logger %s): %s",
                        event.getLoggerName(), event.getFormattedMessage())
                        .that(event.getFormattedMessage()).doesNotContain(SECRET);
            }
            assertWithMessage("the capture saw the session at all - an empty capture proves nothing")
                    .that(capture.list).isNotEmpty();
        }
    }

    /**
     * A second Configure on a configured stream is refused without killing the session: the proxy re-sends the
     * ORIGINAL effective configuration unchanged, so the client reads back a configuration that is not what it
     * asked for - a truthful refusal under the assert-what-you-got contract - and the subscription stays fixed
     * (R36). Closing the stream instead would drop in-flight records with no reconnect reconciliation until U8.
     */
    @Test
    void aSecondConfigureIsRefusedAndTheOriginalConfigurationStands() {
        var session = new RecordingSession(newHandler());

        session.send(configureMessage(Configure.newBuilder().addTopics(TOPIC).setMaxConcurrency(4)));
        var original = session.awaitConfigured();

        session.send(configureMessage(Configure.newBuilder().addTopics("hijack-topic").setMaxConcurrency(99)));
        var refusal = session.awaitConfigured();

        assertWithMessage("the refusal echoes the ORIGINAL configuration, unchanged")
                .that(refusal).isEqualTo(original);
        assertThat(refusal.getTopicsList()).containsExactly(TOPIC);
        assertThat(refusal.getMaxConcurrency()).isEqualTo(4);
        assertWithMessage("the session survived the refusal").that(session.error.get()).isNull();
    }

    /** R36 for patterns: a pattern subscription is fixed; a later change attempt echoes the original pattern. */
    @Test
    void aTopicPatternSubscriptionIsFixedForTheProcessLifetime() {
        var session = new RecordingSession(newHandler());

        session.send(configureMessage(Configure.newBuilder().setTopicPattern("input-.*")));
        var original = session.awaitConfigured();
        assertThat(original.getTopicPattern()).isEqualTo("input-.*");

        session.send(configureMessage(Configure.newBuilder().setTopicPattern("other-.*")));
        var refusal = session.awaitConfigured();

        assertThat(refusal.getTopicPattern()).isEqualTo("input-.*");
    }

    /** A rejected Configure (KTD7's transactional mode) closes the stream before any client is constructed. */
    @Test
    void aRejectedConfigureClosesTheStreamBeforeConstructingClients() {
        var session = new RecordingSession(newHandler());

        session.send(configureMessage(Configure.newBuilder()
                .addTopics(TOPIC)
                .setCommitMode(bz.stub.parallelconsumer.proxy.protocol.v1.CommitMode
                        .COMMIT_MODE_PERIODIC_TRANSACTIONAL_PRODUCER)));

        var status = Status.fromThrowable(session.awaitError());
        assertThat(status.getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(status.getDescription()).contains("PERIODIC_TRANSACTIONAL_PRODUCER");
        assertWithMessage("refusal must precede client construction")
                .that(clientsConstructed.get()).isEqualTo(0);
    }

    /** An invalid topic pattern is refused before any client is constructed, not after (F1 of the U7 review). */
    @Test
    void anInvalidTopicPatternClosesTheStreamBeforeConstructingClients() {
        var session = new RecordingSession(newHandler());

        session.send(configureMessage(Configure.newBuilder().setTopicPattern("input-[")));

        var status = Status.fromThrowable(session.awaitError());
        assertThat(status.getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(status.getDescription()).contains("topic_pattern");
        assertWithMessage("refusal must precede client construction")
                .that(clientsConstructed.get()).isEqualTo(0);
        assertThat(handler.engine().isPresent()).isFalse();
    }

    /** A non-positive max_concurrency is refused by name before any client is constructed. */
    @Test
    void aNonPositiveMaxConcurrencyClosesTheStreamBeforeConstructingClients() {
        var session = new RecordingSession(newHandler());

        session.send(configureMessage(Configure.newBuilder().addTopics(TOPIC).setMaxConcurrency(0)));

        var status = Status.fromThrowable(session.awaitError());
        assertThat(status.getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(status.getDescription()).contains("max_concurrency");
        assertWithMessage("refusal must precede client construction")
                .that(clientsConstructed.get()).isEqualTo(0);
    }

    /**
     * Forward compatibility: an enum wire number this proxy's schema does not know is a clean
     * INVALID_ARGUMENT naming the number - not an escaped {@code getNumber()} throw from the rejection
     * message itself.
     */
    @Test
    void anUnknownEnumWireNumberIsRefusedCleanly() {
        var session = new RecordingSession(newHandler());

        session.send(configureMessage(Configure.newBuilder().addTopics(TOPIC).setOrderingValue(99)));

        var status = Status.fromThrowable(session.awaitError());
        assertThat(status.getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(status.getDescription()).contains("99");
        assertWithMessage("refusal must precede client construction")
                .that(clientsConstructed.get()).isEqualTo(0);
    }

    /**
     * The construction-time half of F1: when a Kafka client constructor itself rejects the supplied
     * kafka_properties AFTER an earlier client was built, everything half-built is released - the consumer is
     * closed, no engine is published - the stream closes naming only the exception class (never Kafka's
     * message, which embeds property values - R48), and the process still accepts a subsequent connection.
     */
    @Test
    void aClientConstructorFailureReleasesTheHalfBuiltAndLeavesTheProcessConfigurable() {
        var firstConsumer = new LongPollingMockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST);
        var rejectedValue = "rejected-property-value-3f9";
        var producerConstructionsAttempted = new AtomicInteger();
        handler = ConfigureHandler.builder()
                .clientFactory(new KafkaClientFactory() {
                    @Override
                    public Consumer<byte[], byte[]> consumer(Map<String, String> kafkaProperties) {
                        return producerConstructionsAttempted.get() == 0 ? firstConsumer : mockConsumer;
                    }

                    @Override
                    public Producer<byte[], byte[]> producer(Map<String, String> kafkaProperties) {
                        if (producerConstructionsAttempted.getAndIncrement() == 0) {
                            // the shape KafkaProducer's ctor produces for a bad config - message embeds the value
                            throw new KafkaException("Invalid value " + rejectedValue + " for configuration x");
                        }
                        return mockProducer;
                    }
                })
                .build();

        var failed = new RecordingSession(handler);
        failed.send(configureMessage(Configure.newBuilder().addTopics(TOPIC)));

        var status = Status.fromThrowable(failed.awaitError());
        assertThat(status.getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertWithMessage("the close status names the exception class and NOTHING of its message (R48)")
                .that(status.getDescription()).contains("KafkaException");
        assertThat(status.getDescription()).doesNotContain(rejectedValue);
        assertWithMessage("the consumer built before the failure must be closed, not leaked")
                .that(firstConsumer.closed()).isTrue();
        assertWithMessage("no engine may be published from a failed configure")
                .that(handler.engine().isPresent()).isFalse();

        // the process is not wedged: a corrected client configures on a fresh connection
        var second = new RecordingSession(handler);
        second.send(configureMessage(Configure.newBuilder().addTopics(TOPIC)));
        second.awaitConfigured();
        assertThat(handler.engine().isPresent()).isTrue();
    }

    /**
     * R38's intersection rule: a client declaring an older capability set - one without {@code dispatch} -
     * receives a {@code Configured} naming only the intersection, and the proxy then sends NO message type
     * outside it: a seeded record produces no {@code Dispatch} on the stream.
     */
    @Test
    void anOlderCapabilitySetReceivesTheIntersectionAndNothingOutsideIt() throws InterruptedException {
        var session = new RecordingSession(newHandlerWithSeededRecord());

        session.send(configureMessage(Configure.newBuilder()
                .addTopics(TOPIC)
                .addCapabilities("some-older-capability")));

        var configured = session.awaitConfigured();
        assertWithMessage("the intersection of {some-older-capability} and {dispatch} is empty")
                .that(configured.getCapabilitiesList()).isEmpty();

        var unexpected = session.messages.poll(NEGATIVE_CONTROL_BUDGET.toMillis(), TimeUnit.MILLISECONDS);
        assertWithMessage("no message type outside the negotiated intersection may be sent")
                .that(unexpected).isNull();
    }

    /** The positive control for the negative above: with {@code dispatch} declared, the record IS dispatched. */
    @Test
    void aClientDeclaringDispatchReceivesTheSeededRecord() {
        var session = new RecordingSession(newHandlerWithSeededRecord());

        session.send(configureMessage(Configure.newBuilder()
                .addTopics(TOPIC)
                .addCapabilities(ConfigureHandler.CAPABILITY_DISPATCH)
                .addCapabilities("some-future-capability")));

        var configured = session.awaitConfigured();
        assertThat(configured.getCapabilitiesList()).containsExactly(ConfigureHandler.CAPABILITY_DISPATCH);

        var dispatch = session.awaitMessage().getDispatch();
        assertThat(dispatch.getRecords(0).getRecord().getValue().toStringUtf8()).isEqualTo("hello");
    }

    /**
     * After one stream configured the engine, a new stream cannot reconfigure the process: the subscription is
     * fixed for the process lifetime, and a reconnect stream opens with a {@code Manifest} instead (R36, R43).
     */
    @Test
    void aReconnectStreamCarryingConfigureIsRefusedAndToldToSendAManifest() {
        var handler = newHandler();
        var first = new RecordingSession(handler);
        first.send(configureMessage(Configure.newBuilder().addTopics(TOPIC)));
        first.awaitConfigured();

        var second = new RecordingSession(handler);
        second.send(configureMessage(Configure.newBuilder().addTopics(TOPIC)));

        var status = Status.fromThrowable(second.awaitError());
        assertThat(status.getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
        assertThat(status.getDescription()).contains("Manifest");
    }

    /** A Manifest with no configured session behind it has nothing to reconcile against (R39). */
    @Test
    void aManifestBeforeAnythingConfiguredTheProxyIsRefused() {
        var session = new RecordingSession(newHandler());

        session.send(ClientMessage.newBuilder().setManifest(Manifest.getDefaultInstance()).build());

        var status = Status.fromThrowable(session.awaitError());
        assertThat(status.getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
        assertThat(status.getDescription()).contains("Configure");
        assertWithMessage("a refused stream must have constructed no Kafka client")
                .that(clientsConstructed.get()).isEqualTo(0);
    }

    /**
     * The reconnect handshake end to end (R42, R43): a session drops with a record in flight, the record is
     * NOT returned, and a new stream opening with a manifest that names it gets the unchanged effective
     * configuration back and may report the record on the new stream. The report applying is the proof the
     * delivery survived the connection loss - a returned-and-redelivered record would answer with a superseded
     * or unknown token instead.
     */
    @Test
    void aReconnectStreamOpensWithAManifestAndKeepsTheRecordItNames() {
        var handler = newHandlerWithSeededRecord();
        var first = new RecordingSession(handler);
        first.send(configureMessage(Configure.newBuilder().addTopics(TOPIC)));
        first.awaitConfigured();
        var token = first.awaitMessage().getDispatch().getRecords(0).getToken();

        first.drop();
        assertWithMessage("connection loss must hold the record, not return it (R42)")
                .that(handler.engine().orElseThrow().getNumberRecordsOutForProcessing()).isEqualTo(1);

        var second = new RecordingSession(handler);
        second.send(ClientMessage.newBuilder()
                .setManifest(Manifest.newBuilder().addTokens(token))
                .build());

        var configured = second.awaitConfigured();
        assertWithMessage("the reconnect echo is the ORIGINAL effective configuration, unchanged")
                .that(configured.getTopicsList()).containsExactly(TOPIC);
        assertThat(configured.getCapabilitiesList()).contains(ConfigureHandler.CAPABILITY_MANIFEST);

        second.send(ClientMessage.newBuilder()
                .setReport(Report.newBuilder().setToken(token).setSuccess(Report.Success.newBuilder()))
                .build());

        awaitNoRecordsOutForProcessing(handler);
    }

    /**
     * R43's second reconciliation arm at the transport: a manifest token naming a delivery that has been
     * superseded is answered with a {@code Drop} carrying that exact token, and the record's current delivery
     * is left alone.
     */
    @Test
    void aManifestTokenNamingASupersededDeliveryIsAnsweredWithADrop() {
        var handler = newHandlerWithSeededRecord();
        var first = new RecordingSession(handler);
        first.send(configureMessage(Configure.newBuilder().addTopics(TOPIC)));
        first.awaitConfigured();
        var token = first.awaitMessage().getDispatch().getRecords(0).getToken();

        first.drop();

        var supersededToken = Token.newBuilder()
                .setRecordId(token.getRecordId())
                .setEpoch(token.getEpoch() - 1)
                .build();
        var second = new RecordingSession(handler);
        second.send(ClientMessage.newBuilder()
                .setManifest(Manifest.newBuilder().addTokens(supersededToken))
                .build());

        second.awaitConfigured();
        var drop = second.awaitMessage();
        assertWithMessage("expected a Drop, got %s", drop.getMessageCase()).that(drop.hasDrop()).isTrue();
        assertThat(drop.getDrop().getToken()).isEqualTo(supersededToken);

        // the record the manifest accounted for is still out; the live delivery may still be reported
        second.send(ClientMessage.newBuilder()
                .setReport(Report.newBuilder().setToken(token).setSuccess(Report.Success.newBuilder()))
                .build());
        awaitNoRecordsOutForProcessing(handler);
    }

    private static void awaitNoRecordsOutForProcessing(ConfigureHandler handler) {
        var engine = handler.engine().orElseThrow();
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertWithMessage("records out for processing must return to baseline")
                        .that(engine.getNumberRecordsOutForProcessing()).isEqualTo(0));
    }

    /**
     * KTD38's regression test, and it is written as a CONTROL rather than as an absence.
     * <p>
     * The plan reached this seam carrying the credit ledger under another name: an
     * {@code ExecutorCountPolicy} deriving the count from OBSERVED report concurrency and re-sending
     * {@code SetExecutorCount} whenever the observation moved. Rename "advertised capacity" to "observed
     * report concurrency" and the structure is identical - a closed feedback loop between proxy and client -
     * and so are the four unanswered questions: over what window, damped how, what happens to records already
     * dispatched when the number falls, and how is the value proven not to drift. Four review rounds died on
     * it.
     * <p>
     * <b>The structural half is the load-bearing one.</b> Asserting no {@code SetExecutorCount} arrives
     * during one test's workload only proves it about that workload; the plan asks for the property under a
     * workload "whose report concurrency varies widely", and no single scenario can stand for all of them.
     * Asserting that <b>no production source constructs the message at all</b> covers every workload,
     * including the ones no test runs - which is what a regression guard has to do.
     */
    @Test
    void noProductionCodePathConstructsSetExecutorCount() throws IOException {
        var mainSources = Paths.get("src/main/java");
        assertWithMessage("the module's main sources must be where this test thinks they are")
                .that(Files.isDirectory(mainSources)).isTrue();

        var builders = new ArrayList<String>();
        try (var paths = Files.walk(mainSources)) {
            for (var file : paths.filter(path -> path.toString().endsWith(".java")).toArray(java.nio.file.Path[]::new)) {
                var text = Files.readString(file);
                if (text.contains("SetExecutorCount.newBuilder") || text.contains("setSetExecutorCount")) {
                    builders.add(file.toString());
                }
            }
        }

        assertWithMessage("SetExecutorCount is declared in the schema and deliberately never sent (KTD38). "
                + "Sending it reintroduces the credit ledger as a capacity feedback loop, which needs its own "
                + "KTD answering the window, damping, in-flight and drift questions first.")
                .that(builders).isEmpty();
    }

    /**
     * The other half of KTD38, and the reason the field is not simply deleted: it stays <b>declared</b> so a
     * dynamic count remains an additive change under R38 rather than a breaking one. A test that only proved
     * nobody sends it would pass just as well against a schema that had dropped the field.
     */
    @Test
    void setExecutorCountRemainsInTheSchemaSoADynamicCountStaysAdditive() {
        assertThat(ProxyMessage.MessageCase.SET_EXECUTOR_COUNT).isNotNull();
        assertWithMessage("a message the proxy never sends must still be one the schema declares")
                .that(ProxyMessage.newBuilder().build().getMessageCase())
                .isNotEqualTo(ProxyMessage.MessageCase.SET_EXECUTOR_COUNT);
    }

    /**
     * The runtime half: across a connection's whole life - configure, a dispatch, a report, the connection
     * dropping, and a reconnect that resumes with a manifest - the count travels exactly once, in
     * {@code Configured}, and nothing revises it afterwards.
     */
    @Test
    void theExecutorCountTravelsOnceAndIsNeverRevisedAcrossAReconnect() {
        var handler = newHandlerWithSeededRecord();
        var observed = new ArrayList<ProxyMessage>();

        var first = new RecordingSession(handler);
        first.send(configureMessage(Configure.newBuilder().addTopics(TOPIC).setMaxConcurrency(7)));
        var configured = first.awaitConfigured();
        assertWithMessage("the count travels in Configured, which is the only place it may")
                .that(configured.getExecutorCount()).isEqualTo(7);
        var dispatch = first.awaitMessage();
        observed.add(dispatch);
        first.drop();

        var second = new RecordingSession(handler);
        second.send(ClientMessage.newBuilder()
                .setManifest(Manifest.newBuilder()
                        .addTokens(dispatch.getDispatch().getRecords(0).getToken()))
                .build());

        observed.addAll(first.messages);
        observed.addAll(second.messages);
        assertWithMessage("nothing in a session's life may revise the executor count")
                .that(observed.stream()
                        .map(ProxyMessage::getMessageCase)
                        .filter(ProxyMessage.MessageCase.SET_EXECUTOR_COUNT::equals)
                        .count())
                .isEqualTo(0);
    }

    private static ClientMessage configureMessage(Configure.Builder configure) {
        return ClientMessage.newBuilder().setConfigure(configure).build();
    }

    /** One in-memory session: the handler's inbound observer plus a recording outbound one. */
    private static final class RecordingSession {

        final BlockingQueue<ProxyMessage> messages = new LinkedBlockingQueue<>();
        final AtomicReference<Throwable> error = new AtomicReference<>();
        private final StreamObserver<ClientMessage> inbound;

        RecordingSession(ConfigureHandler handler) {
            this.inbound = handler.session(new StreamObserver<>() {
                @Override
                public void onNext(ProxyMessage message) {
                    messages.add(message);
                }

                @Override
                public void onError(Throwable t) {
                    error.set(t);
                }

                @Override
                public void onCompleted() {
                    // nothing to record: the tests assert on messages and errors
                }
            });
        }

        void send(ClientMessage message) {
            inbound.onNext(message);
        }

        /** The connection going away underneath the session - the transport's own onError, as gRPC calls it. */
        void drop() {
            inbound.onError(new java.io.IOException("connection reset by peer (test)"));
        }

        Configured awaitConfigured() {
            var message = awaitMessage();
            assertWithMessage("expected a Configured, got %s", message.getMessageCase())
                    .that(message.hasConfigured()).isTrue();
            return message.getConfigured();
        }

        ProxyMessage awaitMessage() {
            try {
                var message = messages.poll(30, TimeUnit.SECONDS);
                assertWithMessage("no message arrived within the budget (stream error: %s)", error.get())
                        .that(message).isNotNull();
                return message;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }

        Throwable awaitError() {
            assertWithMessage("expected the stream to be closed with an error").that(error.get()).isNotNull();
            return error.get();
        }
    }
}
