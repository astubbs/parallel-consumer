package bz.stub.parallelconsumer.proxy.harness;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.RecordContext;
import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import com.github.bsideup.jabel.Desugar;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.awaitility.Awaitility;

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
     * ENGINE SEAM - STUBBED. The message {@link #startEngine()} throws until the engine units land.
     */
    public static final String ENGINE_SEAM_PENDING_MESSAGE =
            "the proxy engine is not built yet: ProxyHarness.startEngine() is the seam the engine units fill in "
                    + "docs/plans/2026-08-14-001-feat-language-proxy-plan.md - U5 (transport/ProxyServer), "
                    + "U6 (engine/ProxyProcessor), U7 (config/ConfigureHandler). Until they land, drive the "
                    + "harness with an in-JVM client via ProxyHarness.start(Client)";

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

    public ProxyHarness(HarnessScenario scenario) {
        this.scenario = scenario;
        this.topic = scenario.name();
        this.topicPartition = new TopicPartition(topic, 0);
    }

    /**
     * Boots PC over the mock clients and hands every delivered record to the given in-JVM {@link Client}.
     * <p>
     * This is the path that works today. When the engine units land, {@link #startEngine()} becomes the
     * higher-fidelity route for anything that can speak gRPC; this direct route remains the harness's own
     * stub-client lane - the one that proves the harness can fail.
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
     * ENGINE SEAM - STUBBED, awaiting the engine units. Once U5-U7 land, this boots the real proxy engine
     * (gRPC server on an ephemeral loopback port, {@code ProxyProcessor} behind it) over this harness's mock
     * clients and returns the bound port, so any client that can speak the production protocol - in any
     * language - runs against the same fixture. {@link bz.stub.parallelconsumer.proxy.testmode.TestModeMain}
     * is the process entry point that calls this on behalf of a spawned, non-JVM test.
     *
     * @return the ephemeral loopback port the engine's gRPC server bound (once implemented)
     */
    public int startEngine() {
        throw new UnsupportedOperationException(ENGINE_SEAM_PENDING_MESSAGE);
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

    /** The most recently committed offset for the scenario's partition, if anything has committed yet. */
    public OptionalLong lastCommittedOffset() {
        var history = mockConsumer.getCommitHistoryInt();
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
        Awaitility.await().atMost(CONVERGENCE_BUDGET).untilAsserted(() ->
                assertWithMessage("records delivered to the client")
                        .that(deliveries().size()).isAtLeast(atLeast));
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

        if (parallelConsumer != null && !parallelConsumer.isClosedOrFailed()) {
            parallelConsumer.close();
        }
        if (parallelConsumer != null) {
            assertWithMessage("PC ended with a failure cause; the scenario did not expect one")
                    .that(parallelConsumer.getFailureCause()).isNull();
        }
    }
}
