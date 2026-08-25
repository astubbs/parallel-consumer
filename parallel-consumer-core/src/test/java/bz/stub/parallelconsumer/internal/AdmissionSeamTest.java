package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.AdaptiveConcurrencyMode;
import bz.stub.parallelconsumer.PollContextInternal;
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
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Pins the admission seam (the plan's U5, {@code docs/plans/2026-08-18-001-feat-self-scaling-concurrency-plan.md}):
 * where {@link PCModule#admissionTargetRecords()} does and does not reach the dispatch and intake arithmetic.
 * <p>
 * Three contracts, each with its control arm:
 * <ul>
 * <li><b>R1 parity</b> - in {@code DISABLED} and {@code OBSERVE}, every seam read (loaded target, request quantity,
 * poller-gate threshold, retry-branch block time) is byte-for-byte today's static arithmetic.</li>
 * <li><b>KTD10</b> - under active {@code ENFORCE} the dispatch chain consumes the live target (slots x batch size)
 * <em>un-multiplied</em> by the load factor: with the pool at the ceiling and a live target below it, target x
 * factor records would run, not buffer. The factor keeps its buffer job in
 * {@link WorkManager#isSufficientlyLoaded()}'s threshold only.</li>
 * <li><b>KTD7</b> - the block-time arithmetic keeps reading the pinned target
 * ({@link WorkManager#isWorkInFlightMeetingTarget()}), bounded so the loop never blocks past the next commit
 * check - a case {@code DISABLED}-parity cannot catch, tested explicitly.</li>
 * </ul>
 * Above-ceiling target <em>estimates</em> are clamped inside the controller ({@code AdmissionControllerTest}); the
 * seam-level pin here is that a target AT the ceiling reproduces the ceiling arithmetic. The Zipf/KEY contraction
 * probe scenario is engine-level and deliberately deferred to the plan's U9/U10.
 * <p>
 * Harness patterns follow {@link CalculateQuantityToRequestTest}: arithmetic pinned through a mocked
 * {@link WorkManager}, plus a real-{@link WorkManager} path for what actually leaves the shards.
 */
class AdmissionSeamTest {

    static final String TOPIC = "admission-seam-topic";

    /** The user's explicit {@code maxConcurrency} - the static ceiling ENFORCE adapts under. */
    static final int CEILING_SLOTS = 24;

    /** A live target well below the ceiling, seeded so no control-law windows need driving. */
    static final int CONTRACTED_TARGET_SLOTS = 8;

    static final int BATCH_SIZE = 5;

    /**
     * Every scenario is hand-computed against the default initial load factor; the fixture assertions pin it so a
     * changed default fails loudly rather than skewing expectations.
     */
    static final int EXPECTED_LOAD_FACTOR = DynamicLoadFactor.DEFAULT_INITIAL_LOADING_FACTOR;

    final TopicPartition tp = new TopicPartition(TOPIC, 0);

    final Function<PollContextInternal<String, String>, List<String>> userFunction = context -> new ArrayList<>();
    final Consumer<String> callback = result -> {
    };

    /** Only assigned by the harness-backed tests; the arithmetic tests manage their own instances. */
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

    // --- R1 parity: DISABLED and OBSERVE are byte-for-byte today's static arithmetic ---

    /**
     * DISABLED keeps the load-factor multiplication on the loaded target: maxConcurrency 4 x batch 5 = 20 in-flight
     * target, x factor 2 = 40 loaded.
     */
    @Test
    void disabledLoadedTargetKeepsTheLoadFactorMultiplication() {
        try (var processor = arithmeticProcessor(disabledOptions(4, BATCH_SIZE), 7)) {
            assertLoadFactorFixture(processor);
            assertThat(processor.getTargetLoad()).isEqualTo(40);
        }
    }

    /**
     * DISABLED request quantity: target 40, in-flight 7, shortfall 33, rounded up to the next whole-batch multiple,
     * 35 - the {@link CalculateQuantityToRequestTest} arithmetic, reproduced through the seam accessor.
     */
    @Test
    void disabledRequestQuantityIsTodaysStaticArithmetic() {
        try (var processor = arithmeticProcessor(disabledOptions(4, BATCH_SIZE), 7)) {
            assertThat(processor.calculateQuantityToRequest()).isEqualTo(35);
        }
    }

    /**
     * OBSERVE measures without acting: with a seed set (which OBSERVE uses only for its hypothetical target), every
     * dispatch-side read matches DISABLED exactly.
     */
    @Test
    void observeSeamReadsMatchDisabled() {
        var options = optionsBuilder(4, BATCH_SIZE)
                .adaptiveConcurrencyMode(AdaptiveConcurrencyMode.OBSERVE)
                .adaptiveConcurrencyInitialTarget(2)
                .build();
        try (var processor = arithmeticProcessor(options, 7)) {
            assertWithMessage("a non-acting mode must keep the load-factor-multiplied loaded target")
                    .that(processor.getTargetLoad()).isEqualTo(40);
            assertWithMessage("a non-acting mode must keep today's request arithmetic - the seed feeds only the "
                    + "hypothetical target")
                    .that(processor.calculateQuantityToRequest()).isEqualTo(35);
        }
    }

    // --- KTD10: active ENFORCE consumes the live target un-multiplied ---

    /**
     * The live target reaches dispatch denominated in records: 8 slots x batch 5 = 40 loaded target (NOT x factor
     * 80); in-flight 7 leaves a shortfall of 33, rounded up to 35. State {@code RUNNING}, because the live read is
     * state-derived (U7's KTD9 drain release) - the non-RUNNING arm has its own tests in
     * {@link AdmissionLifecycleTest}.
     */
    @Test
    void enforceDispatchTargetIsTheLiveTargetTimesBatchSize() {
        try (var processor = arithmeticProcessor(enforceOptions(BATCH_SIZE, CONTRACTED_TARGET_SLOTS), 7)) {
            processor.setState(State.RUNNING);
            assertLoadFactorFixture(processor);
            assertThat(processor.getTargetLoad()).isEqualTo(CONTRACTED_TARGET_SLOTS * BATCH_SIZE);
            assertThat(processor.calculateQuantityToRequest()).isEqualTo(35);
            processor.setState(State.CLOSED);
        }
    }

    /**
     * THE KTD10 pin. Pool at the ceiling (24 threads), live target 8, factor 2: the request tops in-flight up to
     * the published target - 8 - and never to target x factor. With the whole surplus running rather than queueing,
     * target x factor here would be 16 records RUNNING: a breach of the published admission, not a deeper buffer.
     */
    @Test
    void enforceRequestTopsUpToThePublishedTargetNeverTargetTimesFactor() {
        try (var processor = arithmeticProcessor(enforceOptions(1, CONTRACTED_TARGET_SLOTS), CONTRACTED_TARGET_SLOTS)) {
            processor.setState(State.RUNNING);
            assertLoadFactorFixture(processor);
            assertWithMessage("in-flight already at the published target - anything positive here is the factor "
                    + "leaking into dispatch")
                    .that(processor.calculateQuantityToRequest()).isAtMost(0);
            processor.setState(State.CLOSED);
        }
        try (var processor = arithmeticProcessor(enforceOptions(1, CONTRACTED_TARGET_SLOTS), 5)) {
            processor.setState(State.RUNNING);
            assertWithMessage("the top-up must stop at the published target of %s", CONTRACTED_TARGET_SLOTS)
                    .that(processor.calculateQuantityToRequest()).isEqualTo(CONTRACTED_TARGET_SLOTS - 5);
            processor.setState(State.CLOSED);
        }
    }

    /**
     * AE3 at seam level: a live target at the ceiling reproduces the ceiling arithmetic exactly - dispatch can
     * never be asked past it, because the controller clamps estimates to the ceiling before they get here.
     */
    @Test
    void enforceTargetAtTheCeilingMatchesTheCeilingArithmetic() {
        try (var processor = arithmeticProcessor(enforceOptions(1, CEILING_SLOTS), 0)) {
            processor.setState(State.RUNNING);
            assertThat(processor.getTargetLoad()).isEqualTo(CEILING_SLOTS);
            assertThat(processor.calculateQuantityToRequest()).isEqualTo(CEILING_SLOTS);
            processor.setState(State.CLOSED);
        }
    }

    /**
     * The virtual-thread branch already consumed the pool-load target un-multiplied; adaptive modes must leave it
     * exactly as it was (DISABLED), and active ENFORCE must flow the live target through it unchanged.
     */
    @Test
    void virtualThreadBranchIsUnchangedByAdaptiveModes() {
        try (var processor = fakeVirtualPoolProcessor(disabledOptions(4, BATCH_SIZE), 0)) {
            assertWithMessage("today's virtual-thread behaviour: static target, no factor multiplication")
                    .that(processor.getTargetLoad()).isEqualTo(20);
        }
        try (var processor = fakeVirtualPoolProcessor(enforceOptions(BATCH_SIZE, CONTRACTED_TARGET_SLOTS), 0)) {
            processor.setState(State.RUNNING);
            assertWithMessage("active ENFORCE on a queueless pool: the live target, still un-multiplied")
                    .that(processor.getTargetLoad()).isEqualTo(CONTRACTED_TARGET_SLOTS * BATCH_SIZE);
            processor.setState(State.CLOSED);
        }
    }

    // --- a shrinking target throttles intake, never stalls ---

    /**
     * Live target below current in-flight: the request quantity goes non-positive, which
     * {@link WorkManager#getWorkIfAvailable(int)} answers with nothing - no new dispatch - and as in-flight work
     * completes below the target, dispatch resumes on its own. Arithmetic of the sequence (the vacuous-await
     * lesson: computed before asserting): 12 in flight against a target of 8 is a delta of -4, so the first pass
     * takes 0; completing 5 leaves 7 in flight, delta 1, so the next pass takes exactly 1.
     */
    @Test
    void aTargetBelowInFlightRequestsNothingThenResumesAsInFlightCompletes() {
        buildEnforceHarness();
        registerWork(20);
        var inFlight = takeWork(12);
        pc.setState(State.RUNNING);

        int firstPass = pc.retrieveAndDistributeNewWork(userFunction, callback);
        assertWithMessage("in-flight (12) above the live target (8): intake must throttle to zero")
                .that(firstPass).isEqualTo(0);

        completeWork(inFlight.subList(0, 5));
        assertWithMessage("fixture: completions must be accounted before the next pass")
                .that(wm.getNumberRecordsOutForProcessing()).isEqualTo(7);

        int secondPass = pc.retrieveAndDistributeNewWork(userFunction, callback);
        assertWithMessage("in-flight (7) back under the live target (8): dispatch must resume with the shortfall - "
                + "throttling must not become a stall")
                .that(secondPass).isEqualTo(1);
    }

    // --- the poller gate: live target, factor KEPT (buffer arithmetic) ---

    /**
     * Under active ENFORCE the gate's threshold is live-target x factor: 8 x 2 = 16 workable records. At the
     * threshold the poller keeps fetching; one past it, the gate reports loaded - far sooner than the static 24 x 2
     * = 48 would.
     */
    @Test
    void enforcePollerGateThresholdIsTheLiveTargetTimesLoadFactor() {
        buildEnforceHarness();
        int threshold = CONTRACTED_TARGET_SLOTS * EXPECTED_LOAD_FACTOR;

        registerWork(threshold);
        assertWithMessage("at the threshold (%s) the gate must still want more", threshold)
                .that(wm.isSufficientlyLoaded()).isFalse();

        registerWork(1);
        assertWithMessage("one past the live-target threshold must read as loaded - the static threshold (48) "
                + "would keep fetching")
                .that(wm.isSufficientlyLoaded()).isTrue();
    }

    /**
     * The DISABLED control arm at its exact boundary: threshold is the static target x factor, 24 x 2 = 48.
     */
    @Test
    void disabledPollerGateThresholdIsTheStaticTargetTimesLoadFactor() {
        buildDisabledHarness();
        int threshold = CEILING_SLOTS * EXPECTED_LOAD_FACTOR;

        registerWork(threshold);
        assertThat(wm.isSufficientlyLoaded()).isFalse();

        registerWork(1);
        assertThat(wm.isSufficientlyLoaded()).isTrue();
    }

    // --- KTD7: the retry branch's block time is bounded by the remaining commit-check time ---

    /**
     * The case DISABLED-parity cannot catch. The retry branch measures against the FULL commit interval, not the
     * remaining one; under active ENFORCE with a contracted target it becomes the loop's common branch, so it is
     * bounded by the remaining time to the next commit check. Here the interval (50ms) has fully elapsed before
     * the call, so the bounded result must be non-positive - the unbounded arithmetic would return the whole 50ms
     * again, blocking past the overdue commit check.
     */
    @Test
    void enforceRetryBlockTimeIsBoundedByTheRemainingCommitCheckTime() throws InterruptedException {
        var options = optionsBuilder(CEILING_SLOTS, 1)
                .adaptiveConcurrencyMode(AdaptiveConcurrencyMode.ENFORCE)
                .adaptiveConcurrencyInitialTarget(CONTRACTED_TARGET_SLOTS)
                .commitInterval(COMMIT_INTERVAL)
                .build();
        try (var processor = retryScheduledProcessor(options)) {
            processor.setState(State.RUNNING);
            Thread.sleep(COMMIT_INTERVAL.multipliedBy(3).toMillis());

            Duration blockFor = processor.timeToBlockFor();

            assertWithMessage("the commit interval has fully elapsed, so a bounded block time must not be positive")
                    .that(blockFor).isAtMost(Duration.ZERO);
            processor.setState(State.CLOSED);
        }
    }

    /**
     * The control arm: DISABLED keeps today's arithmetic byte-for-byte - the same overdue-commit setup still
     * returns the full interval, because the retry branch has always measured against the interval, not the
     * remainder.
     */
    @Test
    void disabledRetryBlockTimeKeepsTodaysFullIntervalMeasure() throws InterruptedException {
        var options = optionsBuilder(CEILING_SLOTS, 1)
                .commitInterval(COMMIT_INTERVAL)
                .build();
        try (var processor = retryScheduledProcessor(options)) {
            processor.setState(State.RUNNING);
            Thread.sleep(COMMIT_INTERVAL.multipliedBy(3).toMillis());

            Duration blockFor = processor.timeToBlockFor();

            assertWithMessage("today's arithmetic: min(full commit interval, retry delay), whatever remains of "
                    + "the interval")
                    .that(blockFor).isEqualTo(COMMIT_INTERVAL);
            processor.setState(State.CLOSED);
        }
    }

    static final Duration COMMIT_INTERVAL = Duration.ofMillis(50);

    /** Scheduled well past both the commit interval and the default retry delay, so it never becomes the minimum. */
    static final Duration RETRY_SCHEDULED_IN = Duration.ofSeconds(30);

    // --- helpers ---

    private static ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> optionsBuilder(
            int maxConcurrency, int batchSize) {
        return ParallelConsumerOptions.<String, String>builder()
                .consumer(new MockConsumer<>(OffsetResetStrategy.LATEST))
                .maxConcurrency(maxConcurrency)
                .batchSize(batchSize)
                // pinned: the platform-vs-virtual axis is exercised explicitly by the fake-virtual-pool tests
                .useVirtualThreads(false);
    }

    private static ParallelConsumerOptions<String, String> disabledOptions(int maxConcurrency, int batchSize) {
        return optionsBuilder(maxConcurrency, batchSize).build();
    }

    private static ParallelConsumerOptions<String, String> enforceOptions(int batchSize, int seedSlots) {
        return optionsBuilder(CEILING_SLOTS, batchSize)
                .adaptiveConcurrencyMode(AdaptiveConcurrencyMode.ENFORCE)
                .adaptiveConcurrencyInitialTarget(seedSlots)
                .build();
    }

    /**
     * A processor whose in-flight count is pinned by a mocked {@link WorkManager}, and whose target flows through
     * the REAL seam chain (options → module → controller) - unlike {@link CalculateQuantityToRequestTest}'s
     * arithmetic processor, which overrides the target away, this one exists to test where the target comes from.
     */
    private TestParallelEoSStreamProcessor<String, String> arithmeticProcessor(
            ParallelConsumerOptions<String, String> options, int inFlight) {
        var processor = new TestParallelEoSStreamProcessor<>(options);
        processor.setWm(mockedWorkManager(inFlight));
        return processor;
    }

    /**
     * Same, but over a pool that is not a {@link java.util.concurrent.ThreadPoolExecutor}, which is exactly how
     * {@code isVirtualThreadPool()} recognises a queueless pool - no JDK 21 needed to pin the branch.
     */
    private TestParallelEoSStreamProcessor<String, String> fakeVirtualPoolProcessor(
            ParallelConsumerOptions<String, String> options, int inFlight) {
        var processor = new TestParallelEoSStreamProcessor<>(options) {
            @Override
            protected ExecutorService setupWorkerPool(int poolSize) {
                return Executors.newSingleThreadExecutor();
            }
        };
        processor.setWm(mockedWorkManager(inFlight));
        return processor;
    }

    /**
     * The KTD7 fixture: in-flight below the PINNED target (the branch condition,
     * {@link WorkManager#isWorkInFlightMeetingTarget()}), with work scheduled for retry far in the future.
     */
    private TestParallelEoSStreamProcessor<String, String> retryScheduledProcessor(
            ParallelConsumerOptions<String, String> options) {
        var processor = new TestParallelEoSStreamProcessor<>(options);
        WorkManager<String, String> mockWm = Mockito.mock(WorkManager.class);
        Mockito.when(mockWm.isWorkInFlightMeetingTarget()).thenReturn(false);
        Mockito.when(mockWm.getLowestRetryTime()).thenReturn(Optional.of(RETRY_SCHEDULED_IN));
        processor.setWm(mockWm);
        return processor;
    }

    private WorkManager<String, String> mockedWorkManager(int inFlight) {
        WorkManager<String, String> mockWm = Mockito.mock(WorkManager.class);
        Mockito.when(mockWm.getNumberRecordsOutForProcessing()).thenReturn(inFlight);
        return mockWm;
    }

    private void assertLoadFactorFixture(TestParallelEoSStreamProcessor<String, String> processor) {
        assertWithMessage("fixture: expectations are hand-computed for the default initial load factor")
                .that(processor.module.dynamicExtraLoadFactor().getCurrentFactor()).isEqualTo(EXPECTED_LOAD_FACTOR);
    }

    /**
     * A real processor and the real {@link WorkManager} from the SAME module, so
     * {@link PCModule#admissionTargetRecords()} sees the attached processor's enforcement decision.
     */
    private void buildHarness(ParallelConsumerOptions<String, String> options) {
        var module = new PCModule<>(options);
        pc = new TestParallelEoSStreamProcessor<>(options, module);
        wm = module.workManager();
        wm.onPartitionsAssigned(UniLists.of(tp));
        pc.setWm(wm);
    }

    private void buildEnforceHarness() {
        buildHarness(enforceOptions(1, CONTRACTED_TARGET_SLOTS));
        // the live read is state-derived (U7's KTD9): only a RUNNING processor consumes the contracted target
        pc.setState(State.RUNNING);
        assertWithMessage("fixture: these scenarios are hand-computed for a live target of %s under a ceiling of %s",
                CONTRACTED_TARGET_SLOTS, CEILING_SLOTS)
                .that(pc.getTargetLoad()).isEqualTo(CONTRACTED_TARGET_SLOTS);
    }

    private void buildDisabledHarness() {
        buildHarness(disabledOptions(CEILING_SLOTS, 1));
        assertWithMessage("fixture: the static loaded target these scenarios are computed for")
                .that(pc.getTargetLoad()).isEqualTo(CEILING_SLOTS * EXPECTED_LOAD_FACTOR);
    }

    /**
     * Registers {@code count} records and takes them as work the way the control loop does, so every container
     * really is in flight and counted against {@code numberRecordsOutForProcessing}.
     */
    private List<WorkContainer<String, String>> takeWork(int count) {
        var taken = wm.getWorkIfAvailable(count);
        assertWithMessage("fixture: all %s requested records must be taken as work", count)
                .that(taken).hasSize(count);
        return taken;
    }

    /** Completes work the way the control loop does on a success result. */
    private void completeWork(List<WorkContainer<String, String>> containers) {
        for (var wc : containers) {
            wc.onUserFunctionSuccess();
            wm.onSuccessResult(wc);
        }
    }

    /**
     * Registers {@code count} records, left sitting in the shards - selectable but not taken.
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
