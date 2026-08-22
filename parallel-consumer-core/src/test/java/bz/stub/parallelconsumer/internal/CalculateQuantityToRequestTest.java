package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.PollContextInternal;
import bz.stub.parallelconsumer.state.WorkManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Pins the arithmetic of {@link AbstractParallelEoSStreamProcessor#calculateQuantityToRequest()} - the method's first
 * test coverage (astubbs#311).
 * <p>
 * The defect these tests were written against: under batching, the round-up-to-fill-a-batch step added
 * {@code target - modulo} where filling the last batch needs {@code batchSize - modulo}. Because the target is always
 * a whole multiple of the batch size, that requested almost a whole extra target of work on nearly every control-loop
 * pass, settling in-flight work at roughly 2x the configured target. Second-order effect: the inflated request could
 * not be fulfilled, so {@code lastWorkRequestWasFulfilled} stayed false and
 * {@link AbstractParallelEoSStreamProcessor#checkPipelinePressure()} stopped stepping up the
 * {@link DynamicLoadFactor} exactly when the pool queue ran low.
 * <p>
 * The first group of tests isolates the arithmetic: the target is pinned by overriding
 * {@code getTargetOutForProcessing()} and the in-flight count by mocking the {@link WorkManager}, so each expectation
 * is a hand-checkable sum. The second group drives {@code retrieveAndDistributeNewWork} through a real
 * {@link WorkManager} - the same harness as {@link SubmitWorkToPoolShutdownRaceTest} - so the request quantity is what
 * actually leaves the shards, and the fulfillment flag is read as production sets it.
 */
class CalculateQuantityToRequestTest {

    static final String TOPIC = "quantity-topic";

    final TopicPartition tp = new TopicPartition(TOPIC, 0);

    final Function<PollContextInternal<String, String>, List<String>> userFunction = context -> new ArrayList<>();
    final Consumer<String> callback = result -> {
    };

    /**
     * Only assigned by the harness-backed tests; the arithmetic tests manage their own instances.
     */
    TestParallelEoSStreamProcessor<String, String> pc;
    WorkManager<String, String> wm;

    /**
     * Offsets are never reused across a test, so unique per-record keys stay unique (default ordering is KEY - a
     * shared key would serialise the records and cap every take at one).
     */
    long nextOffset = 0;

    @AfterEach
    void tearDown() {
        if (pc != null) {
            // these tests never start the control loop, so the normal close handshake does not apply
            pc.setState(State.CLOSED);
            pc.close();
            pc.workerThreadPool.get().shutdownNow();
        }
    }

    // --- the arithmetic in isolation ---

    /**
     * Today's only exercised shape: no batching, so the request is exactly the shortfall to target.
     */
    @Test
    void batchSizeOneRequestsExactlyTheShortfall() {
        try (var processor = arithmeticProcessor(1, 24, 7)) {
            assertThat(processor.calculateQuantityToRequest()).isEqualTo(17);
        }
    }

    /**
     * The defect's home case. Shortfall to target is 17; filling the last batch needs 3 more, so the request is 20 -
     * the next whole-batch multiple. The wrong operand added {@code target - modulo} = 22 instead, requesting 39:
     * nearly a whole extra target of work.
     */
    @Test
    void batchingRoundsTheShortfallUpToTheNextWholeBatchMultiple() {
        try (var processor = arithmeticProcessor(5, 24, 7)) {
            assertThat(processor.calculateQuantityToRequest()).isEqualTo(20);
        }
    }

    /**
     * Modulo-zero boundary: a shortfall already on a batch boundary needs no top-up, and must not get one.
     */
    @Test
    void aShortfallOnABatchBoundaryIsNotRoundedUp() {
        try (var processor = arithmeticProcessor(5, 25, 10)) {
            assertThat(processor.calculateQuantityToRequest()).isEqualTo(15);
        }
    }

    // --- the request as the WorkManager sees it, and the fulfillment flag ---

    /**
     * The rounded-up quantity is a ceiling for {@link WorkManager#getWorkIfAvailable(int)}, so over-requesting is not
     * cosmetic: given ample supply, everything requested is taken. With 7 in flight against a target of 20, the
     * corrected request is 15; the defective one was 30 and, with supply available, took all 30.
     */
    @Test
    void onlyTheRoundedUpShortfallIsTakenFromTheWorkManager() {
        buildBatchingHarness();
        takeWork(7);
        registerWork(40); // ample - more than even the defective request could take
        pc.setState(State.RUNNING);

        int got = pc.retrieveAndDistributeNewWork(userFunction, callback);

        assertWithMessage("the take must stop at the next whole-batch multiple of the shortfall, never absorb "
                + "a whole extra target")
                .that(got).isEqualTo(15);
    }

    /**
     * A request filled to exactly the corrected quantity is a fulfilled request. Under the defect the same supply
     * looked starved - 15 received against 30 requested - which kept
     * {@link AbstractParallelEoSStreamProcessor#checkPipelinePressure()} from ever stepping the load factor up.
     */
    @Test
    void aRequestFilledExactlyReportsFulfilled() {
        buildBatchingHarness();
        takeWork(7);
        registerWork(15);
        pc.setState(State.RUNNING);

        int got = pc.retrieveAndDistributeNewWork(userFunction, callback);

        assertThat(got).isEqualTo(15);
        assertWithMessage("receiving every record requested must report the request fulfilled - this flag gates "
                + "DynamicLoadFactor step-up")
                .that(pc.lastWorkRequestWasFulfilled()).isTrue();
    }

    /**
     * The flag's other half must survive the fix: a genuinely starved request still reports unfulfilled.
     */
    @Test
    void aStarvedRequestReportsUnfulfilled() {
        buildBatchingHarness();
        takeWork(7);
        registerWork(4); // well short of the 15 the corrected request asks for
        pc.setState(State.RUNNING);

        int got = pc.retrieveAndDistributeNewWork(userFunction, callback);

        assertThat(got).isEqualTo(4);
        assertWithMessage("a starved request must not report fulfilled - stepping up the load factor cannot help "
                + "when the shards have nothing more to give")
                .that(pc.lastWorkRequestWasFulfilled()).isFalse();
    }

    // --- helpers ---

    /**
     * A processor whose target and in-flight count are pinned, so {@code calculateQuantityToRequest} is a pure
     * function of the three numbers each test names. The target is overridden rather than derived from options
     * because the interesting cases need targets the options arithmetic cannot produce (a target that is not a
     * multiple of the batch size), and the mock keeps the in-flight count exact.
     */
    private TestParallelEoSStreamProcessor<String, String> arithmeticProcessor(int batchSize, int target, int inFlight) {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(new MockConsumer<String, String>(OffsetResetStrategy.LATEST))
                .batchSize(batchSize)
                .build();
        var processor = new TestParallelEoSStreamProcessor<String, String>(options) {
            @Override
            protected int getTargetOutForProcessing() {
                return target;
            }
        };
        WorkManager<String, String> mockWm = Mockito.mock(WorkManager.class);
        Mockito.when(mockWm.getNumberRecordsOutForProcessing()).thenReturn(inFlight);
        processor.setWm(mockWm);
        return processor;
    }

    /**
     * A real processor over a real {@link WorkManager}, batching enabled. The scenarios above are computed for a
     * target of 20 (maxConcurrency 2 * batchSize 5 * initial load factor 2), which the fixture assertion pins so a
     * changed default fails loudly here rather than skewing every expectation.
     */
    private void buildBatchingHarness() {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(new MockConsumer<String, String>(OffsetResetStrategy.LATEST))
                .maxConcurrency(2)
                .batchSize(5)
                // pinned: a virtual-thread pool deliberately does not multiply by the load factor, so the target -
                // and every expectation derived from it - would change under CI's execution-mode axis
                .useVirtualThreads(false)
                .build();
        pc = new TestParallelEoSStreamProcessor<>(options);
        wm = new PCModuleTestEnv(options).workManager();
        wm.onPartitionsAssigned(UniLists.of(tp));
        pc.setWm(wm);

        assertWithMessage("fixture: these scenarios are hand-computed for a target of 20")
                .that(pc.getTargetLoad()).isEqualTo(20);
    }

    /**
     * Registers {@code count} records and takes them as work the way the control loop does, so every container really
     * is in flight and counted against {@code numberRecordsOutForProcessing}.
     */
    private void takeWork(int count) {
        registerWork(count);
        var taken = wm.getWorkIfAvailable(count);
        assertWithMessage("fixture: all %s records must be taken as work", count)
                .that(taken).hasSize(count);
        assertWithMessage("fixture: the take must be counted as out for processing")
                .that(wm.getNumberRecordsOutForProcessing()).isEqualTo(count);
    }

    /**
     * Registers {@code count} records, left sitting in the shards - selectable but not taken - which is the starting
     * state {@code retrieveAndDistributeNewWork} is given: it does its own take.
     */
    private void registerWork(int count) {
        var records = new ArrayList<ConsumerRecord<String, String>>();
        for (int i = 0; i < count; i++) {
            long offset = nextOffset++;
            records.add(new ConsumerRecord<>(TOPIC, tp.partition(), offset, "key-" + offset, "value"));
        }
        wm.registerWork(new EpochAndRecordsMap<>(new ConsumerRecords<>(UniMaps.of(tp, records)), wm.getPm()));
    }
}
