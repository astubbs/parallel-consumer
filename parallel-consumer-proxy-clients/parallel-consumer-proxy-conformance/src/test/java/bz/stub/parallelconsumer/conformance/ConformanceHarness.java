package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.RecordContext;
import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import bz.stub.parallelconsumer.model.CommitHistory;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.awaitility.Awaitility;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
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
 * harness therefore drives every binding: {@link CoreBinding} hands it an in-JVM {@link Client}, and a client
 * library that constructs Parallel Consumer for itself is handed the mock Kafka clients through
 * {@link #startEmbeddedClient}. Each binding's run reduces to "connect, process a record, report" against a
 * fixture that already exists - the {@link HarnessScenario} names are that shared conformance vocabulary.
 * <p>
 * <b>THE ENGINE LANE IS NOT HERE, AND THAT IS WHAT THIS CLASS IS MISSING RATHER THAN WHAT IT DECLINES.</b>
 * On {@code feats/proxy-requirements} this class is {@code ProxyHarness}, lives in the sidecar module's test
 * tree, and carries a third lane - {@code startEngine()} - that boots the real gRPC transport with
 * {@code ConfigureHandler} and {@code ProxyProcessor} behind it, so anything speaking the production protocol
 * runs against the same fixture. None of those types exist on this stack: the sidecar here hosts no engine and
 * refuses every session {@code UNIMPLEMENTED} (astubbs/parallel-consumer#384). The two lanes that need no
 * engine are the two that are here, so this class lives with the suite that is its only consumer.
 * <b>When the engine rung lands, this file and the branch's {@code ProxyHarness} are one class</b> - reconcile
 * them rather than keeping both, and the engine lane decides which module it ends up in.
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
public class ConformanceHarness implements AutoCloseable {

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
    /* A plain class rather than a record - HarnessScenario's header says why, for the whole module. */
    public static final class Delivery {

        private final String key;

        private final String value;

        private final long offset;

        private final int failedAttempts;

        private final Optional<Throwable> lastFailureReason;

        public Delivery(String key, String value, long offset, int failedAttempts,
                        Optional<Throwable> lastFailureReason) {
            this.key = key;
            this.value = value;
            this.offset = offset;
            this.failedAttempts = failedAttempts;
            this.lastFailureReason = lastFailureReason;
        }

        public String key() {
            return key;
        }

        public String value() {
            return value;
        }

        public long offset() {
            return offset;
        }

        public int failedAttempts() {
            return failedAttempts;
        }

        public Optional<Throwable> lastFailureReason() {
            return lastFailureReason;
        }
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

    // --- the embedded-client lane's byte[] mock pair. Separate from the pair above because a client library
    // that constructs its own engine speaks byte[] the way the wire does, while the in-JVM lane speaks
    // Strings; the two lanes are mutually exclusive per harness instance. ---

    private LongPollingMockConsumer<byte[], byte[]> engineMockConsumer;

    private MockProducer<byte[], byte[]> engineMockProducer;

    public ConformanceHarness(HarnessScenario scenario) {
        this.scenario = scenario;
        this.topic = scenario.name();
        this.topicPartition = new TopicPartition(topic, 0);
    }

    /**
     * Boots PC over the mock clients and hands every delivered record to the given in-JVM {@link Client}.
     * <p>
     * This is the harness's own stub-client lane - the one {@link ConformanceHarnessTest} uses to prove the
     * harness can fail - and it is also the lane {@link CoreBinding} drives, because the control arm's whole
     * claim is that nothing stands between the scenario and the engine.
     */
    public void start(Client client) {
        start(client, ParallelConsumerOptions.DEFAULT_MAX_CONCURRENCY);
    }

    /**
     * The same lane with the in-flight ceiling set, for a scenario that is a claim about how many records may
     * be outstanding at once.
     * <p>
     * <b>A control arm that could not be given the ceiling would be a control arm for a different
     * configuration.</b> Max concurrency is what an application sets and the proxy passes straight through -
     * {@code maxConcurrency * batchSize}, with the batch size pinned at 1 - so the engine driven by a plain
     * Java function has to be handed the number a foreign client would have sent, or a scenario about the
     * ceiling would go red here for want of a setting rather than for anything about the product.
     */
    public void start(Client client, int maxConcurrency) {
        if (parallelConsumer != null) {
            throw new IllegalStateException("harness already started");
        }

        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(mockConsumer)
                .producer(mockProducer)
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
                .maxConcurrency(maxConcurrency)
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
     * The embedded-client lane: this harness's byte[] mock Kafka clients, handed to an in-JVM client that
     * brings its <em>own</em> engine, followed by the same rebalance-and-seed dance the stub lane does.
     * <p>
     * <b>Why a second lane rather than a special case in a test.</b> {@link #start} owns the engine and the
     * client is a function. A client library that constructs Parallel Consumer for itself - the in-process
     * transport of the Java client wrapper - does not fit that, and the piece it needs is exactly the piece
     * this class owns and keeps private: the mock consumer whose commit history {@link #lastCommittedOffset()}
     * reads. A test that built its own mocks instead would be asserting about a fixture the harness cannot
     * see, so every scenario written against the harness would have to be written a second time for it.
     * <p>
     * The client is handed the clients and returns its {@link ConsumerRebalanceListener}, because
     * {@code MockConsumer#rebalance} assigns the partition but fires no listener - the assignment has to be
     * delivered separately, and seeding has to follow it, exactly as the class javadoc's provenance note
     * describes.
     */
    public void startEmbeddedClient(EmbeddedClient client) {
        if (parallelConsumer != null || engineMockConsumer != null) {
            throw new IllegalStateException("harness already started");
        }
        engineMockConsumer = new LongPollingMockConsumer<>(OffsetResetStrategy.EARLIEST);
        engineMockProducer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());

        var rebalanceListener = client.start(engineMockConsumer, engineMockProducer);
        engineMockConsumer.subscribeWithRebalanceAndAssignment(List.of(topic), 1);
        rebalanceListener.onPartitionsAssigned(List.of(topicPartition));
        seedByteLane();
    }

    /**
     * An in-JVM client that owns its own engine: it is given this harness's mock Kafka clients and returns
     * the rebalance listener that must be told about the assignment.
     */
    @FunctionalInterface
    public interface EmbeddedClient {
        ConsumerRebalanceListener start(Consumer<byte[], byte[]> consumer, Producer<byte[], byte[]> producer);
    }

    /** Seeds the scenario's records into the byte[] lane, offsets in seed order from zero. */
    private void seedByteLane() {
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

    /**
     * The embedded-client lane's counterpart of {@link #producedRecords()}: R6's produce payload, as the
     * client's own engine sent it. Throws rather than answering empty before that lane is started, because a
     * vacuous empty would pass the very assertion this exists to make meaningful.
     */
    public List<ProducerRecord<byte[], byte[]>> embeddedProducedRecords() {
        if (engineMockProducer == null) {
            throw new IllegalStateException("the embedded-client lane is not started - "
                    + "call startEmbeddedClient first");
        }
        return List.copyOf(engineMockProducer.history());
    }

    /** The most recently committed offset for the scenario's partition, whichever lane is active. */
    public OptionalLong lastCommittedOffset() {
        var history = (engineMockConsumer != null ? engineMockConsumer : mockConsumer).getCommitHistoryInt();
        return CommitHistory.forPartition(history, topicPartition).highestCommit()
                .map(OptionalLong::of)
                .orElseGet(OptionalLong::empty);
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

        // The embedded-client lane's engine belongs to the client, so the binding closes it - this harness
        // owns only the mock Kafka clients it lent, and closing an engine it did not construct would race the
        // client's own shutdown.
        if (parallelConsumer != null && !parallelConsumer.isClosedOrFailed()) {
            parallelConsumer.close();
        }
        if (parallelConsumer != null) {
            assertWithMessage("PC ended with a failure cause; the scenario did not expect one")
                    .that(parallelConsumer.getFailureCause()).isNull();
        }
    }
}
