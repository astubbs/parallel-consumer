package bz.stub.parallelconsumer.proxy.engine;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import bz.stub.parallelconsumer.proxy.engine.ProxyProcessor.ReportResult;
import bz.stub.parallelconsumer.proxy.protocol.v1.Dispatch;
import bz.stub.parallelconsumer.proxy.protocol.v1.DispatchRecord;
import bz.stub.parallelconsumer.proxy.protocol.v1.ProduceRecord;
import bz.stub.parallelconsumer.proxy.protocol.v1.Report;
import bz.stub.parallelconsumer.proxy.protocol.v1.Token;
import com.google.protobuf.ByteString;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.awaitility.Awaitility;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The engine-side fixture: mock Kafka clients, the manual rebalance dance core's {@code MockConsumerTestBase}
 * family settled, and a {@link CollectingSink} standing in for the transport - so the test is on both halves
 * of the engine&harr;transport boundary and every hand-off is a deterministic rendezvous rather than a sleep.
 * <p>
 * Mirrors {@code ProxyHarness}'s conventions (budgets, arrival-sync before zero-state awaits) without reusing
 * it: the harness boots core's own processor behind a real gRPC server, and this fixture's whole point is
 * booting {@link ProxyProcessor} directly, so a scenario can drive the engine's own liveness API.
 * <p>
 * Time is {@link TestClock}'s, not the wall's: the lease and reconnect-window scenarios advance it in one
 * step. Nothing here sleeps to reach a deadline.
 *
 * @author Antony Stubbs
 */
class EngineFixture implements AutoCloseable {

    /** Sized for slow shared CI hardware; a healthy run converges in a fraction of it. */
    static final Duration CONVERGENCE_BUDGET = Duration.ofSeconds(30);

    /** Far below core's defaults, so commit and redelivery awaits converge fast. */
    static final Duration COMMIT_INTERVAL = Duration.ofMillis(100);
    static final Duration RETRY_DELAY = Duration.ofMillis(50);

    final String topic;
    final TopicPartition topicPartition;
    final CollectingSink sink = new CollectingSink();
    final TestClock clock = new TestClock();

    final LongPollingMockConsumer<byte[], byte[]> mockConsumer =
            new LongPollingMockConsumer<>(OffsetResetStrategy.EARLIEST);
    final MockProducer<byte[], byte[]> mockProducer;

    ProxyProcessor processor;
    private long nextOffset = 0;

    EngineFixture(String topic) {
        this(topic, true);
    }

    /**
     * @param autoCompleteProduceAcks false hands the acks to the scenario: {@code mockProducer.completeNext()}
     *                                and {@code errorNext(..)} then decide when - and whether - a produce
     *                                payload is acknowledged, which is the only way to test the ordering the
     *                                at-least-once claim rests on, and the only way to hold an ack open long
     *                                enough to see whether it holds the report lane with it
     */
    EngineFixture(String topic, boolean autoCompleteProduceAcks) {
        this.topic = topic;
        this.topicPartition = new TopicPartition(topic, 0);
        this.mockProducer = autoCompleteProduceAcks
                ? new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer())
                : new ScenarioAckedMockProducer();
    }

    void start(ProcessingOrder ordering) {
        startWith(options -> options.ordering(ordering), ProxyProcessor.DEFAULT_COALESCING_WINDOW);
    }

    void startWith(UnaryOperator<ParallelConsumerOptions.ParallelConsumerOptionsBuilder<byte[], byte[]>> customizer,
                   Duration coalescingWindow) {
        startWith(customizer, coalescingWindow, LivenessSettings.defaults(true, clock),
                InFlightRegistry.Hook.NO_OP);
    }

    void startWith(UnaryOperator<ParallelConsumerOptions.ParallelConsumerOptionsBuilder<byte[], byte[]>> customizer,
                   Duration coalescingWindow, LivenessSettings liveness, InFlightRegistry.Hook hook) {
        var options = customizer.apply(ParallelConsumerOptions.<byte[], byte[]>builder()
                        .consumer(mockConsumer)
                        .producer(mockProducer)
                        .commitInterval(COMMIT_INTERVAL)
                        .defaultMessageRetryDelay(RETRY_DELAY))
                .build();

        processor = new ProxyProcessor(options, sink, coalescingWindow, liveness, hook);
        processor.subscribe(List.of(topic));

        // MockConsumer#rebalance assigns the partition but fires no listener, so PC is told separately
        mockConsumer.subscribeWithRebalanceAndAssignment(List.of(topic), 1);
        processor.onPartitionsAssigned(List.of(topicPartition));

        processor.start();
    }

    void seed(String key, String value) {
        seedAt(nextOffset++, key, value);
    }

    /** Seeds at an explicit offset - what a rebalance scenario needs, to re-poll a record already delivered. */
    void seedAt(long offset, String key, String value) {
        mockConsumer.addRecord(new ConsumerRecord<>(topic, topicPartition.partition(), offset,
                key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8)));
    }

    /** The next dispatched record - a hard rendezvous, failing loudly if none arrives within the budget. */
    DispatchRecord takeDispatch() {
        var dispatch = pollDispatch(CONVERGENCE_BUDGET);
        assertWithMessage("no record was dispatched within the convergence budget").that(dispatch).isNotNull();
        return dispatch;
    }

    /** Bounded poll for the negative assertions - null when nothing arrived in the given time. */
    DispatchRecord pollDispatch(Duration budget) {
        try {
            return sink.dispatches.poll(budget.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted waiting for a dispatch", e);
        }
    }

    ReportResult reportSuccess(Token token) {
        var result = processor.report(Report.newBuilder()
                .setToken(token)
                .setSuccess(Report.Success.newBuilder())
                .build());
        if (result == ReportResult.APPLIED_SUCCESS || result == ReportResult.APPLIED_FAILURE) {
            sink.outstanding.decrementAndGet();
        }
        return result;
    }

    /**
     * A success report whose payload asks the proxy to produce {@code values.length} records - R6's worker
     * output. With manual acks the engine's completion of this record is now in the scenario's hands.
     */
    ReportResult reportSuccessProducing(Token token, String... values) {
        var success = Report.Success.newBuilder();
        for (String value : values) {
            success.addProduce(ProduceRecord.newBuilder()
                    .setTopic(topic + "-output")
                    .setValue(ByteString.copyFrom(value, StandardCharsets.UTF_8)));
        }
        var result = processor.report(Report.newBuilder().setToken(token).setSuccess(success).build());
        if (result != ReportResult.UNKNOWN_TOKEN && result != ReportResult.SUPERSEDED_EPOCH
                && result != ReportResult.MALFORMED && result != ReportResult.UNSUPPORTED_OUTCOME) {
            sink.outstanding.decrementAndGet();
        }
        return result;
    }

    ReportResult reportFailure(Token token, String reason) {
        var result = processor.report(Report.newBuilder()
                .setToken(token)
                .setFailure(Report.Failure.newBuilder().setReason(reason))
                .build());
        if (result == ReportResult.APPLIED_FAILURE) {
            sink.outstanding.decrementAndGet();
        }
        return result;
    }

    Optional<OffsetAndMetadata> lastCommitted() {
        var history = mockConsumer.getCommitHistoryInt();
        for (int i = history.size() - 1; i >= 0; i--) {
            var offsetAndMetadata = history.get(i).get(topicPartition);
            if (offsetAndMetadata != null) {
                return Optional.of(offsetAndMetadata);
            }
        }
        return Optional.empty();
    }

    void awaitCommittedOffset(long expectedOffset) {
        Awaitility.await().atMost(CONVERGENCE_BUDGET).untilAsserted(() ->
                assertWithMessage("committed offset for %s", topicPartition)
                        .that(lastCommitted().map(OffsetAndMetadata::offset))
                        .isEqualTo(Optional.of(expectedOffset)));
    }

    /** Arrival-sync on a NON-zero state: how many records core currently counts as out for processing. */
    void awaitRecordsOutForProcessing(int expected) {
        Awaitility.await().atMost(CONVERGENCE_BUDGET).untilAsserted(() ->
                assertWithMessage("records out for processing")
                        .that(processor.getNumberRecordsOutForProcessing()).isEqualTo(expected));
    }

    /**
     * The standing leak check. Only meaningful after an arrival-synced assertion such as
     * {@link #awaitCommittedOffset} - "no in-flight work" is vacuously true before anything was dispatched.
     */
    void awaitNoRecordsOutForProcessing() {
        awaitRecordsOutForProcessing(0);
    }

    @Override
    public void close() {
        Awaitility.reset();
        if (processor != null && !processor.isClosedOrFailed()) {
            processor.close();
        }
        if (processor != null) {
            assertWithMessage("PC ended with a failure cause; the scenario did not expect one")
                    .that(processor.getFailureCause()).isNull();
        }
    }

    /**
     * A {@link MockProducer} whose acks the scenario supplies, with core's send callback deliberately
     * withheld.
     * <p>
     * The mock runs that callback on whichever thread completes the send - the scenario's own thread - where
     * a real producer runs it on its sender thread and guards it. Core's callback rethrows a failed send, and
     * the mock releases the future's latch <b>after</b> calling the callback, so a throw there leaves the
     * engine waiting on a future that can never complete: with the callback attached, "the broker rejected
     * this record" is indistinguishable from "the broker never answered". Withholding it restores
     * production's shape, where the engine learns the outcome from the future - the only channel it uses.
     */
    private static class ScenarioAckedMockProducer extends MockProducer<byte[], byte[]> {

        ScenarioAckedMockProducer() {
            super(false, new ByteArraySerializer(), new ByteArraySerializer());
        }

        @Override
        public synchronized Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record,
                                                        Callback callback) {
            return super.send(record, null);
        }
    }

    /**
     * A clock a scenario moves by hand. The liveness deadlines are the only wall-clock dependency the engine
     * has, and jumping this forward is what lets a one-minute lease be tested in microseconds - and, more
     * importantly, deterministically: nothing here can expire early on a loaded CI box.
     */
    static class TestClock extends Clock {

        private volatile Instant now = Instant.parse("2026-08-14T00:00:00Z");

        void advance(Duration by) {
            now = now.plus(by);
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }

    /**
     * The test's transport stand-in: waves land here. {@code outstanding} counts dispatched-minus-applied
     * (the fixture decrements it as reports are applied) and {@code maxOutstanding} records its high-water
     * mark - the ceiling assertion's evidence.
     */
    static class CollectingSink implements DispatchSink {

        final BlockingQueue<DispatchRecord> dispatches = new LinkedBlockingQueue<>();
        final List<Dispatch> waves = new CopyOnWriteArrayList<>();
        final AtomicInteger outstanding = new AtomicInteger();
        final AtomicInteger maxOutstanding = new AtomicInteger();

        @Override
        public void dispatch(Dispatch wave) {
            waves.add(wave);
            for (DispatchRecord dispatch : wave.getRecordsList()) {
                int now = outstanding.incrementAndGet();
                maxOutstanding.accumulateAndGet(now, Math::max);
                dispatches.add(dispatch);
            }
        }

        int dispatchCount() {
            return waves.stream().mapToInt(Dispatch::getRecordsCount).sum();
        }
    }
}
