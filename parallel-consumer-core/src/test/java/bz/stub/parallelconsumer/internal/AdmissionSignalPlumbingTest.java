package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.AdaptiveConcurrencyMode;
import bz.stub.parallelconsumer.PollContextInternal;
import bz.stub.parallelconsumer.internal.admission.AdmissionController;
import bz.stub.parallelconsumer.internal.admission.AdmissionController.Outcome;
import bz.stub.parallelconsumer.state.WorkContainer;
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Pins the signal plumbing (the plan's U6, {@code docs/plans/2026-08-18-001-feat-self-scaling-concurrency-plan.md}):
 * the admission controller's three inputs flow from the real engine, independent of Micrometer.
 * <p>
 * <b>No MeterRegistry is configured anywhere in this class</b> - deliberately, that IS one of the contracts: every
 * sample asserted here accumulated without one.
 * <ul>
 * <li><b>Service time</b> - tapped in {@code runUserFunctionInternal}, ONE fill-normalized sample per invocation,
 * with the whole-batch retry exclusion ({@link AbstractParallelEoSStreamProcessor#admissionServiceTimeSampleNanos}).</li>
 * <li><b>Outcomes</b> - tapped in {@link WorkManager#handleFutureResult}, behind the staleness filters; retries
 * count as outcome signal even while their latency is excluded.</li>
 * <li><b>In-flight</b> - one snapshot per control-loop pass of
 * {@link WorkManager#getNumberRecordsOutForProcessing()}
 * ({@link AbstractParallelEoSStreamProcessor#sampleAdmissionInFlight()}), driven directly here the way
 * {@link PipelinePressureLoggingTest} drives {@code checkPipelinePressure()}.</li>
 * </ul>
 * Assertions read the controller's recorded-signal counters, which count what reached the window since
 * construction. Harness patterns follow {@link AdmissionSeamTest}.
 */
class AdmissionSignalPlumbingTest {

    static final String TOPIC = "admission-signal-topic";

    static final long MS = 1_000_000L; // nanos per millisecond

    final TopicPartition tp = new TopicPartition(TOPIC, 0);

    final Function<PollContextInternal<String, String>, List<String>> successFunction = context -> new ArrayList<>();
    final Consumer<String> callback = result -> {
    };

    PCModule<String, String> module;
    TestParallelEoSStreamProcessor<String, String> pc;
    WorkManager<String, String> wm;

    /** Offsets never reused across a test, so unique per-record keys stay unique under default KEY ordering. */
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

    // --- service time: one fill-normalized sample per invocation, no registry needed ---

    /**
     * The registry-independence contract: with no MeterRegistry configured (none is, anywhere in this class), a
     * user-function invocation driven through the processor still lands a service-time sample in the controller.
     */
    @Test
    void samplesAccumulateWithNoMeterRegistryConfigured() {
        buildHarness(observeOptions(1).build());
        runOneSuccessfulBatch(1);

        assertThat(controller().serviceTimeSamplesRecorded()).isEqualTo(1);
    }

    /**
     * A batch of 5 is ONE invocation, so ONE sample - never one duplicated sample per record.
     */
    @Test
    void aBatchOfFiveYieldsExactlyOneServiceTimeSample() {
        buildHarness(observeOptions(5).build());
        runOneSuccessfulBatch(5);

        assertWithMessage("one invocation must yield one sample, whatever the batch fill")
                .that(controller().serviceTimeSamplesRecorded()).isEqualTo(1);
    }

    // --- the sampler arithmetic, pure (red-proofed by sabotaging the division and the retry guard) ---

    /**
     * Fill normalization: a batch of 5 taking 50ms is one sample of 10ms.
     */
    @Test
    void fillNormalizationDividesTheBatchDurationByItsFill() {
        OptionalLong sample = AbstractParallelEoSStreamProcessor
                .admissionServiceTimeSampleNanos(50 * MS, freshBatch(5));

        assertThat(sample).isEqualTo(OptionalLong.of(10 * MS));
    }

    /**
     * Varying fill at constant per-record time leaves the sample value unchanged - the property that stops a
     * half-full batch reading as the system speeding up.
     */
    @Test
    void varyingFillAtConstantPerRecordTimeLeavesTheSampleUnchanged() {
        long perRecordNanos = 10 * MS;

        OptionalLong single = AbstractParallelEoSStreamProcessor
                .admissionServiceTimeSampleNanos(perRecordNanos, freshBatch(1));
        OptionalLong pair = AbstractParallelEoSStreamProcessor
                .admissionServiceTimeSampleNanos(2 * perRecordNanos, freshBatch(2));
        OptionalLong five = AbstractParallelEoSStreamProcessor
                .admissionServiceTimeSampleNanos(5 * perRecordNanos, freshBatch(5));

        assertThat(single).isEqualTo(OptionalLong.of(perRecordNanos));
        assertThat(pair).isEqualTo(single);
        assertThat(five).isEqualTo(single);
    }

    /**
     * The whole-batch retry rule: ONE retry anywhere in the batch and the whole invocation yields no sample -
     * a mixed batch's duration cannot be attributed record-by-record, so the contaminated whole is excluded.
     */
    @Test
    void aRetryAnywhereInTheBatchExcludesTheWholeBatchSample() {
        List<WorkContainer<String, String>> batch = freshBatch(5);
        Mockito.when(batch.get(3).hasPreviouslyFailed()).thenReturn(true);

        OptionalLong sample = AbstractParallelEoSStreamProcessor.admissionServiceTimeSampleNanos(50 * MS, batch);

        assertThat(sample).isEqualTo(OptionalLong.empty());
    }

    @Test
    void anEmptyBatchYieldsNoSample() {
        assertThat(AbstractParallelEoSStreamProcessor.admissionServiceTimeSampleNanos(50 * MS, UniLists.of()))
                .isEqualTo(OptionalLong.empty());
    }

    // --- retry storm: outcome signal without latency samples ---

    /**
     * A record with a prior failure generates OUTCOME signal but ZERO latency samples: the failing attempt
     * classifies IGNORE (no sample - the tap sits after a normal return), and the retry attempt's successful
     * execution is excluded from the latency window while its success still counts.
     */
    @Test
    void aRetryGeneratesOutcomeSignalButNoLatencySample() throws InterruptedException {
        buildHarness(observeOptions(1)
                .defaultMessageRetryDelay(Duration.ofMillis(1))
                .build());
        registerWork(1);
        var first = takeWork(1);

        Function<PollContextInternal<String, String>, List<String>> throwingFunction = context -> {
            throw new RuntimeException("business failure");
        };
        assertThrows(RuntimeException.class, () -> pc.runUserFunc(throwingFunction, callback, first));
        wm.handleFutureResult(first.get(0));

        assertWithMessage("a throwing invocation must land no latency sample")
                .that(controller().serviceTimeSamplesRecorded()).isEqualTo(0);
        assertWithMessage("the failure must still count as outcome signal")
                .that(controller().outcomesRecorded(Outcome.IGNORE)).isEqualTo(1);

        var retry = takeRetriedWork();
        assertWithMessage("fixture: the retry attempt must carry its failure history")
                .that(retry.get(0).hasPreviouslyFailed()).isTrue();

        pc.runUserFunc(successFunction, callback, retry);
        wm.handleFutureResult(retry.get(0));

        assertWithMessage("a retry attempt's execution must NEVER enter the latency window")
                .that(controller().serviceTimeSamplesRecorded()).isEqualTo(0);
        assertWithMessage("the retry's success still counts as outcome signal")
                .that(controller().outcomesRecorded(Outcome.SUCCESS)).isEqualTo(1);
    }

    // --- outcome classification at the completion path ---

    /**
     * Success classifies SUCCESS; a throwing user function classifies IGNORE (business failure); nothing
     * classifies OVERLOAD_DROP in v1 - that socket's default is pinned in {@code AdmissionOutcomeClassifierTest}.
     */
    @Test
    void completionsClassifySuccessAndBusinessFailureAsIgnore() {
        buildHarness(observeOptions(1).build());
        registerWork(2);

        var succeeding = takeWork(1);
        pc.runUserFunc(successFunction, callback, succeeding);
        wm.handleFutureResult(succeeding.get(0));

        var failing = takeWork(1);
        Function<PollContextInternal<String, String>, List<String>> throwingFunction = context -> {
            throw new RuntimeException("business failure");
        };
        assertThrows(RuntimeException.class, () -> pc.runUserFunc(throwingFunction, callback, failing));
        wm.handleFutureResult(failing.get(0));

        assertThat(controller().outcomesRecorded(Outcome.SUCCESS)).isEqualTo(1);
        assertThat(controller().outcomesRecorded(Outcome.IGNORE)).isEqualTo(1);
        assertThat(controller().outcomesRecorded(Outcome.OVERLOAD_DROP)).isEqualTo(0);
    }

    // --- the in-flight sampler: one snapshot per control-loop pass ---

    /**
     * Each control-loop pass records exactly one in-flight sample, read from the EXISTING conservation-derived
     * accounting - and a window with zero completions still carries them, which is what lets the law see
     * starvation rather than a blank.
     */
    @Test
    void eachControlLoopPassRecordsExactlyOneInFlightSample() {
        var options = observeOptions(1).build();
        module = new PCModule<>(options);
        pc = new TestParallelEoSStreamProcessor<>(options, module);
        WorkManager<String, String> mockedWm = Mockito.mock(WorkManager.class);
        Mockito.when(mockedWm.getNumberRecordsOutForProcessing()).thenReturn(7);
        pc.setWm(mockedWm);

        int passes = 3;
        for (int pass = 0; pass < passes; pass++) {
            pc.sampleAdmissionInFlight();
        }

        assertWithMessage("N passes must record exactly N samples")
                .that(controller().inFlightSamplesRecorded()).isEqualTo(passes);
        Mockito.verify(mockedWm, Mockito.times(passes)).getNumberRecordsOutForProcessing();
        assertWithMessage("a zero-completion window must still carry its in-flight samples")
                .that(controller().outcomesRecorded(Outcome.SUCCESS)
                        + controller().outcomesRecorded(Outcome.IGNORE)
                        + controller().outcomesRecorded(Outcome.OVERLOAD_DROP)).isEqualTo(0);
    }

    // --- mode gating: OBSERVE records, DISABLED records nothing ---

    /**
     * DISABLED constructs the controller inert: driving every tap - user function, completion, in-flight pass -
     * accumulates nothing.
     */
    @Test
    void disabledRecordsNoSignalsAtAll() {
        buildHarness(optionsBuilder(1).build()); // adaptiveConcurrencyMode defaults to DISABLED
        registerWork(1);
        var work = takeWork(1);
        pc.runUserFunc(successFunction, callback, work);
        wm.handleFutureResult(work.get(0));
        pc.sampleAdmissionInFlight();

        assertThat(controller().serviceTimeSamplesRecorded()).isEqualTo(0);
        assertThat(controller().inFlightSamplesRecorded()).isEqualTo(0);
        assertThat(controller().outcomesRecorded(Outcome.SUCCESS)).isEqualTo(0);
        assertThat(controller().outcomesRecorded(Outcome.IGNORE)).isEqualTo(0);
        assertThat(controller().outcomesRecorded(Outcome.OVERLOAD_DROP)).isEqualTo(0);
    }

    /**
     * The OBSERVE control arm of the gate, end to end: all three signals flow - measuring without acting is that
     * mode's whole point.
     */
    @Test
    void observeRecordsAllThreeSignals() {
        buildHarness(observeOptions(1).build());
        registerWork(1);
        var work = takeWork(1);
        pc.runUserFunc(successFunction, callback, work);
        wm.handleFutureResult(work.get(0));
        pc.sampleAdmissionInFlight();

        assertThat(controller().serviceTimeSamplesRecorded()).isEqualTo(1);
        assertThat(controller().inFlightSamplesRecorded()).isEqualTo(1);
        assertThat(controller().outcomesRecorded(Outcome.SUCCESS)).isEqualTo(1);
    }

    // --- helpers ---

    private static ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> optionsBuilder(int batchSize) {
        return ParallelConsumerOptions.<String, String>builder()
                .consumer(new MockConsumer<>(OffsetResetStrategy.LATEST))
                .maxConcurrency(8)
                .batchSize(batchSize)
                // pinned: the platform-vs-virtual axis is orthogonal to signal plumbing
                .useVirtualThreads(false);
    }

    private static ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> observeOptions(int batchSize) {
        return optionsBuilder(batchSize).adaptiveConcurrencyMode(AdaptiveConcurrencyMode.OBSERVE);
    }

    /**
     * A real processor and the real {@link WorkManager} from the SAME module, so the taps see the attached
     * processor's activity decision - the {@link AdmissionSeamTest} harness.
     */
    private void buildHarness(ParallelConsumerOptions<String, String> options) {
        module = new PCModule<>(options);
        pc = new TestParallelEoSStreamProcessor<>(options, module);
        wm = module.workManager();
        wm.onPartitionsAssigned(UniLists.of(tp));
        pc.setWm(wm);
    }

    private AdmissionController controller() {
        return module.admissionController();
    }

    /** A batch of {@code fill} first-attempt records - mocks, because only {@code hasPreviouslyFailed} is read. */
    private static List<WorkContainer<String, String>> freshBatch(int fill) {
        var batch = new ArrayList<WorkContainer<String, String>>();
        for (int i = 0; i < fill; i++) {
            batch.add(Mockito.mock(WorkContainer.class));
        }
        return batch;
    }

    private void runOneSuccessfulBatch(int fill) {
        registerWork(fill);
        pc.runUserFunc(successFunction, callback, takeWork(fill));
    }

    private void registerWork(int count) {
        var records = new ArrayList<ConsumerRecord<String, String>>();
        for (int i = 0; i < count; i++) {
            long offset = nextOffset++;
            records.add(new ConsumerRecord<>(TOPIC, tp.partition(), offset, "key-" + offset, "value"));
        }
        wm.registerWork(new EpochAndRecordsMap<>(new ConsumerRecords<>(UniMaps.of(tp, records)), wm.getPm()));
    }

    private List<WorkContainer<String, String>> takeWork(int count) {
        var taken = wm.getWorkIfAvailable(count);
        assertWithMessage("fixture: all %s requested records must be taken as work", count)
                .that(taken).hasSize(count);
        return taken;
    }

    /**
     * Takes the failed record back once its (1ms) retry delay has passed on the real clock - bounded poll, no
     * fixed sleep, so slow CI cannot flake it.
     */
    private List<WorkContainer<String, String>> takeRetriedWork() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            var taken = wm.getWorkIfAvailable(1);
            if (!taken.isEmpty()) {
                return taken;
            }
            Thread.sleep(5);
        }
        throw new AssertionError("retried record never became selectable within the deadline");
    }
}
