package bz.stub.parallelconsumer.proxy.harness;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.RecordContext;
import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import bz.stub.parallelconsumer.proxy.config.ConfigureHandler;
import bz.stub.parallelconsumer.proxy.config.KafkaClientFactory;
import bz.stub.parallelconsumer.proxy.config.OptionsMapper;
import bz.stub.parallelconsumer.proxy.engine.ProxyProcessor;
import bz.stub.parallelconsumer.proxy.transport.ProxyServer;
import com.github.bsideup.jabel.Desugar;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.awaitility.Awaitility;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The engine-side fixture that drives Parallel Consumer from a {@link LongPollingMockConsumer} and a
 * {@link MockProducer} - no broker, no Docker, no Testcontainers, so every scenario stays in the surefire lane
 * at unit-test speed.
 * <p>
 * <b>This harness lives entirely on the JVM engine side and knows nothing about the client's language.</b> One
 * harness therefore drives every client: a Java test hands it an in-JVM {@link Client}, and a foreign test
 * reaches the same fixture by spawning the test-mode sidecar
 * ({@link bz.stub.parallelconsumer.proxy.testmode.TestModeMain}) and speaking the production gRPC protocol at
 * it. Each language's first test reduces to "connect, process a record, report" against a fixture that already
 * exists - the {@link HarnessScenario} names are that shared conformance vocabulary.
 * <p>
 * <b>Mock machinery provenance:</b> this follows the pattern core's {@code MockConsumerTestBase} family settled
 * (subscribe PC, rebalance the partition in by hand, tell PC about the assignment separately, then set beginning
 * offsets), using the {@link LongPollingMockConsumer} wrapper that core's {@code MockConsumerTest} documents as
 * the one to prefer - plain {@code MockConsumer} is not a correct implementation of the {@code Consumer}
 * contract, and the wrapper also wakes its simulated long poll on {@code addRecord} and captures async commit
 * history for the committed-offset assertions here.
 * <p>
 * <b>Convergence discipline:</b> awaits are bounded by the named budgets below, never ad-hoc wall-clock
 * deadlines - and every await here is on a <em>non-zero</em> target state (a delivery count, a committed
 * offset), because a condition like "no in-flight work" is vacuously true before anything connects.
 *
 * @author Antony Stubbs
 * @see HarnessScenario
 */
@Slf4j
public class ProxyHarness implements AutoCloseable {

    /**
     * How long a scenario may take to reach its convergent state before the harness declares it failed. Sized
     * for slow shared CI hardware, like the budgets in core's {@code MockConsumerTestBase} family; a healthy run
     * converges in a fraction of it.
     */
    public static final Duration CONVERGENCE_BUDGET = Duration.ofSeconds(30);

    /** Commit interval for the fixture - far below core's 5s default, so committed-offset awaits converge fast. */
    public static final Duration COMMIT_INTERVAL = Duration.ofMillis(100);

    /** Retry delay for the fixture - far below core's 1s default, so redelivery scenarios converge fast. */
    public static final Duration RETRY_DELAY = Duration.ofMillis(50);

    /**
     * The in-JVM client seam: the same contract a foreign worker has over the wire, with the transport layer
     * removed. Return normally to report success; throw to report a failure (the record will be redelivered
     * with its failure history); never return to report nothing (the negative control - the harness must then
     * fail its convergence condition, not pass).
     */
    @FunctionalInterface
    public interface Client {
        void process(RecordContext<String, String> record) throws Exception;
    }

    /**
     * One hand-off of a record to the client, observed at the moment of delivery - the per-record outcome
     * ledger the scenarios assert on. {@code failedAttempts} and {@code lastFailureReason} are captured
     * <em>before</em> the client runs, so a redelivery shows the history it arrived with.
     */
    @Desugar // Jabel requires the annotation on every record, even in this module where release=17 makes it a no-op
    public record Delivery(String key, String value, long offset, int failedAttempts,
                           Optional<Throwable> lastFailureReason) {
    }

    private final HarnessScenario scenario;

    /** Also the harness's topic: one scenario, one topic, one partition. */
    private final String topic;

    private final TopicPartition topicPartition;

    @Getter
    private final LongPollingMockConsumer<String, String> mockConsumer =
            new LongPollingMockConsumer<>(OffsetResetStrategy.EARLIEST);

    /**
     * Paired with the consumer so R6's produce payload can be asserted without a broker, once the engine
     * carries it - see {@link #producedRecords()}.
     */
    @Getter
    private final MockProducer<String, String> mockProducer =
            new MockProducer<>(true, Serdes.String().serializer(), Serdes.String().serializer());

    private final ConcurrentLinkedQueue<Delivery> deliveries = new ConcurrentLinkedQueue<>();

    private ParallelEoSStreamProcessor<String, String> parallelConsumer;

    // --- the engine lane: real gRPC transport + ProxyProcessor over byte[] mock clients. Separate mock pair
    // because the engine never deserializes (its record types are byte[]), while the in-JVM lane above speaks
    // Strings; the two lanes are mutually exclusive per harness instance. ---

    private ProxyServer engineServer;

    /** The engine the configuring client booted; set by the engine-started listener. */
    private volatile ProxyProcessor engine;

    private LongPollingMockConsumer<byte[], byte[]> engineMockConsumer;

    private MockProducer<byte[], byte[]> engineMockProducer;

    public ProxyHarness(HarnessScenario scenario) {
        this.scenario = scenario;
        this.topic = scenario.name();
        this.topicPartition = new TopicPartition(topic, 0);
    }

    /**
     * Boots PC over the mock clients and hands every delivered record to the given in-JVM {@link Client}.
     * <p>
     * {@link #startEngine()} is the higher-fidelity route for anything that can speak gRPC; this direct route
     * remains the harness's own stub-client lane - the one that proves the harness can fail.
     */
    public void start(Client client) {
        if (parallelConsumer != null) {
            throw new IllegalStateException("harness already started");
        }

        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(mockConsumer)
                .producer(mockProducer)
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
                .commitInterval(COMMIT_INTERVAL)
                .defaultMessageRetryDelay(RETRY_DELAY)
                .build();

        parallelConsumer = new ParallelEoSStreamProcessor<>(options);
        parallelConsumer.subscribe(List.of(topic));

        // The manual rebalance dance from core's MockConsumerTestBase: MockConsumer#rebalance assigns the
        // partition but fires no listener, so PC must be told about the assignment separately.
        mockConsumer.subscribeWithRebalanceAndAssignment(List.of(topic), 1);
        parallelConsumer.onPartitionsAssigned(List.of(topicPartition));

        parallelConsumer.poll(pollContext -> pollContext.forEach(recordContext -> {
            // Normalised because core's RecordContext#getLastFailureReason returns a NULL Optional before the
            // first failure: WorkContainer#lastFailureReason has no initializer and is only assigned in
            // onUserFunctionFailure. The engine's serializer (U6) will meet the same quirk.
            Optional<Throwable> lastFailureReason =
                    Optional.ofNullable(recordContext.getLastFailureReason()).flatMap(reason -> reason);

            // capture the history the record ARRIVES with, before the client gets a chance to change it
            deliveries.add(new Delivery(recordContext.key(), recordContext.value(), recordContext.offset(),
                    recordContext.getNumberOfFailedAttempts(), lastFailureReason));
            log.info("Delivering to client: {}", recordContext);
            try {
                client.process(recordContext);
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                // PC's user-function contract is unchecked; a checked throw is still a failure report
                throw new RuntimeException(e);
            }
        }));
    }

    /**
     * The engine lane: boots the real proxy transport (gRPC server on an ephemeral loopback port,
     * {@code ConfigureHandler} as the session service, {@code ProxyProcessor} behind it once a client
     * configures) over this harness's mock clients and returns the bound port - so any client that can speak
     * the production protocol, in any language, runs against the same fixture.
     * {@link bz.stub.parallelconsumer.proxy.testmode.TestModeMain} is the process entry point that calls this
     * on behalf of a spawned, non-JVM test.
     * <p>
     * The mock clients replace the Kafka clients the credential map would build - the R39-exception the
     * test-mode sidecar's usage text records. The engine itself only exists once the client's {@code Configure}
     * arrives (connect-time configuration, the plan's U7); at that moment the harness performs the manual
     * rebalance dance on the engine's mock consumer and seeds the scenario, so the connecting client must
     * configure the scenario's name as its topic.
     *
     * @return the ephemeral loopback port the engine's gRPC server bound
     */
    public int startEngine() {
        if (parallelConsumer != null || engineServer != null) {
            throw new IllegalStateException("harness already started");
        }
        engineMockConsumer = new LongPollingMockConsumer<>(OffsetResetStrategy.EARLIEST);
        engineMockProducer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());

        var handler = ConfigureHandler.builder()
                .clientFactory(new KafkaClientFactory() {
                    @Override
                    public Consumer<byte[], byte[]> consumer(Map<String, String> kafkaProperties) {
                        return engineMockConsumer;
                    }

                    @Override
                    public Producer<byte[], byte[]> producer(Map<String, String> kafkaProperties) {
                        return engineMockProducer;
                    }
                })
                .engineStartedListener(this::onEngineStarted)
                .build();
        try {
            engineServer = ProxyServer.builder()
                    .sessionService(handler)
                    .build()
                    .start();
        } catch (IOException e) {
            throw new UncheckedIOException("engine transport failed to bind", e);
        }
        return engineServer.port();
    }

    /**
     * Fires on the configuring stream's transport thread, after the engine is subscribed and started: the
     * manual rebalance dance from core's {@code MockConsumerTestBase} (see the class javadoc), then the
     * scenario's records - seeding must follow assignment, because {@code MockConsumer#addRecord} refuses a
     * partition the consumer is not assigned.
     */
    private void onEngineStarted(ProxyProcessor startedEngine, OptionsMapper.Subscription subscription) {
        this.engine = startedEngine;
        engineMockConsumer.subscribeWithRebalanceAndAssignment(List.of(topic), 1);
        startedEngine.onPartitionsAssigned(List.of(topicPartition));
        long offset = 0;
        for (HarnessScenario.SeedRecord seed : scenario.seeds()) {
            engineMockConsumer.addRecord(new ConsumerRecord<>(topic, topicPartition.partition(), offset++,
                    seed.key().getBytes(StandardCharsets.UTF_8), seed.value().getBytes(StandardCharsets.UTF_8)));
        }
    }

    /** Seeds the scenario's records into the mock consumer, offsets assigned in seed order from zero. */
    public void seed() {
        long offset = 0;
        for (HarnessScenario.SeedRecord seed : scenario.seeds()) {
            mockConsumer.addRecord(new ConsumerRecord<>(topic, topicPartition.partition(), offset++,
                    seed.key(), seed.value()));
        }
    }

    /** Every hand-off to the client so far, in delivery order. */
    public List<Delivery> deliveries() {
        return List.copyOf(deliveries);
    }

    /** Everything the engine produced, for asserting the produce payload without a broker. */
    public List<ProducerRecord<String, String>> producedRecords() {
        return List.copyOf(mockProducer.history());
    }

    /** The engine lane's counterpart of {@link #producedRecords()}: R6's produce payload, as the engine sent it. */
    public List<ProducerRecord<byte[], byte[]>> engineProducedRecords() {
        if (engineMockProducer == null) {
            throw new IllegalStateException("the engine lane is not started - call startEngine first");
        }
        return List.copyOf(engineMockProducer.history());
    }

    /**
     * The engine lane's standing leak check: how many records the engine currently counts as out for
     * processing. Throws rather than answering zero before a client has configured the engine, because a
     * vacuous zero would pass the very assertion this exists to make meaningful.
     */
    public int engineRecordsOutForProcessing() {
        var currentEngine = engine;
        if (currentEngine == null) {
            throw new IllegalStateException("no engine yet - it exists once a client's Configure arrives");
        }
        return currentEngine.getNumberRecordsOutForProcessing();
    }

    /** The most recently committed offset for the scenario's partition, whichever lane is active. */
    public OptionalLong lastCommittedOffset() {
        var history = (engineMockConsumer != null ? engineMockConsumer : mockConsumer).getCommitHistoryInt();
        for (int i = history.size() - 1; i >= 0; i--) {
            var offsetAndMetadata = history.get(i).get(topicPartition);
            if (offsetAndMetadata != null) {
                return OptionalLong.of(offsetAndMetadata.offset());
            }
        }
        return OptionalLong.empty();
    }

    /** Arrival-sync: waits until at least {@code atLeast} records have been handed to the client. */
    public void awaitDeliveries(int atLeast) {
        // Counts the queue directly: deliveries() takes a full snapshot copy, wasted when only the
        // size is asserted on every poll tick.
        Awaitility.await().atMost(CONVERGENCE_BUDGET).untilAsserted(() ->
                assertWithMessage("records delivered to the client")
                        .that(deliveries.size()).isAtLeast(atLeast));
    }

    /** Waits for the scenario's convergent state within the default {@link #CONVERGENCE_BUDGET}. */
    public void awaitCommittedOffset(long expectedOffset) {
        awaitCommittedOffset(expectedOffset, CONVERGENCE_BUDGET);
    }

    /**
     * Waits until the committed offset reaches {@code expectedOffset} - the next offset to be read, i.e. one
     * past the last record the scenario completed. Throws Awaitility's {@code ConditionTimeoutException} if the
     * state is never reached, which is exactly what the negative control asserts on: with a budget passed
     * explicitly, a client that reports nothing must make this FAIL.
     */
    public void awaitCommittedOffset(long expectedOffset, Duration budget) {
        Awaitility.await().atMost(budget).untilAsserted(() ->
                assertWithMessage("committed offset for %s", topicPartition)
                        .that(lastCommittedOffset()).isEqualTo(OptionalLong.of(expectedOffset)));
    }

    /**
     * Teardown, following core's {@code MockConsumerTestBase}: reset Awaitility's JVM-wide defaults first,
     * close PC without draining (teardown runs on the failure path too), then assert the control thread did not
     * die - a scenario that expects a PC failure cause should assert and clear it in its own body.
     */
    @Override
    public void close() {
        Awaitility.reset();

        // transport first, so the engine is not dispatching into a dying server while it drains down
        if (engineServer != null) {
            engineServer.close();
        }
        if (engine != null && !engine.isClosedOrFailed()) {
            engine.close();
        }
        if (engine != null) {
            assertWithMessage("the engine ended with a failure cause; the scenario did not expect one")
                    .that(engine.getFailureCause()).isNull();
        }

        if (parallelConsumer != null && !parallelConsumer.isClosedOrFailed()) {
            parallelConsumer.close();
        }
        if (parallelConsumer != null) {
            assertWithMessage("PC ended with a failure cause; the scenario did not expect one")
                    .that(parallelConsumer.getFailureCause()).isNull();
        }
    }
}
