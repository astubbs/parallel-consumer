package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.AdaptiveConcurrencyMode;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.PollContextInternal;
import bz.stub.parallelconsumer.internal.admission.AdmissionController;
import bz.stub.parallelconsumer.internal.admission.AdmissionController.Outcome;
import bz.stub.parallelconsumer.state.WorkManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Pins the pool actuator (the plan's U2, {@code docs/plans/2026-08-24-003-feat-admission-control-law-design.md},
 * R9-R12 and KTD5): under active ENFORCE with a steerable pool, the admission target IS the worker pool's
 * concurrency - {@code maximumPoolSize} reserved at the resolved ceiling (inert under the unbounded queue),
 * {@code corePoolSize} tracking the published target as the single live knob.
 * <p>
 * The open-loop defect this closes: today the pool is built {@code core == max == maxConcurrency}, so a target
 * above the pool size (reachable exactly in the default configuration, where the KTD4 ceiling substitution makes
 * the ceiling 64 over a pool of 16) changed only the FEEDING rate - the surplus queued, and the excluded queue
 * wait poisoned the service-time signal.
 */
class AdmissionPoolActuatorTest {

    static final long MS = 1_000_000L; // nanos per millisecond

    /** The user's explicit {@code maxConcurrency} in the explicit-ceiling fixtures. */
    static final int CEILING_SLOTS = 24;

    /** A seeded target well below that ceiling. */
    static final int CONTRACTED_TARGET_SLOTS = 8;

    /** The window step and per-window sample count, restated as {@link AdmissionLifecycleTest} restates them. */
    static final Duration WINDOW_STEP = Duration.ofSeconds(2);
    static final int SAMPLES = 12;

    AdmissionLifecycleTest.ClockedModule module;
    TestParallelEoSStreamProcessor<String, String> pc;

    /** Released in teardown so latched worker tasks never outlive a test. */
    final CountDownLatch releaseWorkers = new CountDownLatch(1);

    @AfterEach
    void tearDown() {
        releaseWorkers.countDown();
        if (pc != null) {
            pc.setState(State.CLOSED);
            pc.close();
            pc.workerThreadPool.get().shutdownNow();
        }
    }

    // --- R9/KTD5: construction - ceiling reserved as max, seed as core ---

    /**
     * With {@code maxConcurrency} left at the library default, the KTD4 substitution resolves the ceiling to
     * {@link AdmissionController#ADAPTIVE_DEFAULT_CEILING} - and the pool's {@code maximumPoolSize} must reserve
     * exactly that, or every target above the default pool size stays open-loop. Under the unbounded work queue,
     * max is inert for thread creation, so the reservation costs nothing until the controller steers core up.
     */
    @Test
    void enforceReservesTheResolvedCeilingAsMaximumPoolSize() {
        pc = new TestParallelEoSStreamProcessor<>(optionsBuilder()
                .adaptiveConcurrencyMode(AdaptiveConcurrencyMode.ENFORCE)
                .build());

        assertWithMessage("maximumPoolSize must reserve the resolved adaptive ceiling")
                .that(steerablePool().getMaximumPoolSize()).isEqualTo(AdmissionController.ADAPTIVE_DEFAULT_CEILING);
        assertWithMessage("unseeded: the core starts at the static-configuration-derived target (R4)")
                .that(steerablePool().getCorePoolSize())
                .isEqualTo(ParallelConsumerOptions.DEFAULT_MAX_CONCURRENCY);
    }

    /**
     * With an explicit ceiling and a contracted seed, {@code corePoolSize} starts at the SEED, not at the
     * ceiling: the seed is the published target, and core is its actuator.
     */
    @Test
    void enforceSeedsTheCorePoolSizeAtTheInitialTarget() {
        pc = new TestParallelEoSStreamProcessor<>(enforceOptions(CONTRACTED_TARGET_SLOTS));

        assertThat(steerablePool().getMaximumPoolSize()).isEqualTo(CEILING_SLOTS);
        assertWithMessage("corePoolSize must start at the seeded initial target - it is the single live knob")
                .that(steerablePool().getCorePoolSize()).isEqualTo(CONTRACTED_TARGET_SLOTS);
    }

    /**
     * The open-loop case, closed - BEHAVIOURALLY. The law grows the target above the default-built pool size
     * (16), driven through the engine's own tick so the actuator (not the test) moves the pool; that many latched
     * tasks must then really run at once. On the old {@code core == max == 16} construction the seventeenth
     * worker can never start, whatever the target says.
     */
    @Test
    void aTargetGrownAboveTheOldPoolSizeGrowsActiveWorkersToTheTarget() {
        buildClockedHarness(optionsBuilder().adaptiveConcurrencyMode(AdaptiveConcurrencyMode.ENFORCE).build());
        pc.setState(State.RUNNING);
        int oldPoolSize = ParallelConsumerOptions.DEFAULT_MAX_CONCURRENCY;
        growTargetAbove(oldPoolSize);

        int target = controller().currentTarget();
        ExecutorService pool = pc.workerThreadPool.get();
        for (int i = 0; i < target; i++) {
            pool.submit(this::parkUntilReleased);
        }

        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .alias(target + " workers active at once - the old core==max==" + oldPoolSize + " pool caps at "
                        + oldPoolSize)
                .until(() -> steerablePool().getActiveCount() >= target);
    }

    // --- R9: DISABLED and OBSERVE construct exactly today's pool, and never steer it ---

    /**
     * The non-acting modes must be byte-for-byte today's construction: {@code core == max == maxConcurrency}.
     * OBSERVE carries a seed to prove the seed feeds only the hypothetical target, never the pool; DISABLED may
     * not carry one at all (options validation forbids it).
     */
    @Test
    void disabledAndObserveConstructExactlyTodaysPool() {
        var disabled = optionsBuilder().maxConcurrency(CEILING_SLOTS).build();
        var observe = optionsBuilder()
                .maxConcurrency(CEILING_SLOTS)
                .adaptiveConcurrencyMode(AdaptiveConcurrencyMode.OBSERVE)
                .adaptiveConcurrencyInitialTarget(CONTRACTED_TARGET_SLOTS)
                .build();
        for (var options : java.util.Arrays.asList(disabled, observe)) {
            try (var processor = new TestParallelEoSStreamProcessor<>(options)) {
                ThreadPoolExecutor pool = (ThreadPoolExecutor) processor.workerThreadPool.get();
                assertWithMessage("mode %s must keep today's core pool size",
                        options.getAdaptiveConcurrencyMode())
                        .that(pool.getCorePoolSize()).isEqualTo(CEILING_SLOTS);
                assertWithMessage("mode %s must keep today's maximum pool size",
                        options.getAdaptiveConcurrencyMode())
                        .that(pool.getMaximumPoolSize()).isEqualTo(CEILING_SLOTS);
                processor.setState(State.CLOSED);
                processor.workerThreadPool.get().shutdownNow();
            }
        }
    }

    // --- R9: the tick steers corePoolSize on published-target CHANGE only ---

    /**
     * The wiring pin, through a recording pool: construction sets the knob once (the seed); ticks below the
     * window boundary and a window that HOLDS the target set nothing; the first window that MOVES it sets it
     * exactly once, to the new target. {@code setCorePoolSize} takes the pool's main lock, so calling it every
     * tick would be a per-pass tax for a value that holds on most windows.
     * <p>
     * The two-window growth shape (hold at 8, then 9) is the {@link AdmissionLifecycleTest} growth fixture.
     */
    @Test
    void theTickSetsCorePoolSizeOnTargetChangeOnly() {
        var recordingPool = new RecordingThreadPoolExecutor(CEILING_SLOTS);
        buildRecordingHarness(enforceOptions(CONTRACTED_TARGET_SLOTS), recordingPool);
        pc.setState(State.RUNNING);
        assertWithMessage("construction must have seeded the core exactly once")
                .that(recordingPool.coreSizeCalls).containsExactly(CONTRACTED_TARGET_SLOTS);

        for (int pass = 0; pass < 3; pass++) {
            pc.tickAdmissionController();
        }
        assertWithMessage("no window boundary elapsed - no steering may happen")
                .that(recordingPool.coreSizeCalls).hasSize(1);

        // Window 1: sample-rich but UNBOUND (no active tasks at the boundary) - the U5 binding gate preserves
        // the target, so the pool must not be touched.
        feedHealthySamples();
        module.clock.add(WINDOW_STEP);
        pc.tickAdmissionController();
        assertWithMessage("fixture: an unbound window must hold the published target")
                .that(controller().currentTarget()).isEqualTo(CONTRACTED_TARGET_SLOTS);
        assertWithMessage("a hold must not touch the pool").that(recordingPool.coreSizeCalls).hasSize(1);

        // Window 2: LIMIT-BOUND - the warmup band grows the target; exactly one call, carrying the new target.
        feedHealthySamples();
        markPoolSaturatedAt(module.admissionTargetSlots());
        module.clock.add(WINDOW_STEP);
        pc.tickAdmissionController();
        int grownTarget = controller().currentTarget();
        assertWithMessage("fixture: the bound window must grow the published target")
                .that(grownTarget).isGreaterThan(CONTRACTED_TARGET_SLOTS);
        assertWithMessage("one movement, one setCorePoolSize call, carrying the published target")
                .that(recordingPool.coreSizeCalls)
                .containsExactly(CONTRACTED_TARGET_SLOTS, grownTarget).inOrder();
    }

    /**
     * A target LOWERED mid-load, driven through the engine (overload drops fire the law's AIMD cut): the pool's
     * core follows down, no running worker is cut - the latched workers stay active; surplus workers exit only
     * as they idle, which is the pool's own contract and deliberately not timed here.
     */
    @Test
    void aTargetLoweredMidLoadNarrowsTheCoreWithoutCuttingRunningWorkers() {
        buildClockedHarness(enforceOptions(CONTRACTED_TARGET_SLOTS));
        wm.onPartitionsAssigned(UniLists.of(tp));
        pc.setState(State.RUNNING);
        registerWork(CONTRACTED_TARGET_SLOTS);
        int dispatched = pc.retrieveAndDistributeNewWork(this::latchingUserFunction, callback);
        assertWithMessage("fixture: the full contracted width must dispatch")
                .that(dispatched).isEqualTo(CONTRACTED_TARGET_SLOTS);
        awaitActiveTasks(CONTRACTED_TARGET_SLOTS);

        for (int window = 0; window < 10 && controller().currentTarget() >= CONTRACTED_TARGET_SLOTS; window++) {
            feedOverloadedSamples();
            module.clock.add(WINDOW_STEP);
            pc.tickAdmissionController();
        }
        int loweredTarget = controller().currentTarget();
        assertWithMessage("fixture: overload drops must contract the target")
                .that(loweredTarget).isLessThan(CONTRACTED_TARGET_SLOTS);

        assertWithMessage("the core must have followed the lowered target - verify the knob moved")
                .that(steerablePool().getCorePoolSize()).isEqualTo(loweredTarget);
        assertWithMessage("no running worker is cut - lowering the core only interrupts IDLE workers")
                .that(pc.userFunctionTaskAccounting().getActive()).isEqualTo(CONTRACTED_TARGET_SLOTS);

        registerWork(CONTRACTED_TARGET_SLOTS);
        assertWithMessage("active (8) above the lowered target (%s): no new task may be admitted", loweredTarget)
                .that(pc.retrieveAndDistributeNewWork(this::latchingUserFunction, callback)).isEqualTo(0);
    }

    /**
     * The clamp that makes the JDK 9+ {@code setCorePoolSize > maximumPoolSize} throw unreachable: max was
     * reserved at the ceiling at construction, and the live knob clamps to it (and to a floor of one worker)
     * whatever it is handed.
     */
    @Test
    void theActuatorClampsSoSetCorePoolSizeAboveMaximumIsUnreachable() {
        pc = new TestParallelEoSStreamProcessor<>(enforceOptions(CONTRACTED_TARGET_SLOTS));

        pc.applyAdmissionTargetToWorkerPool(10_000);
        assertWithMessage("an above-ceiling value must clamp to maximumPoolSize, not throw")
                .that(steerablePool().getCorePoolSize()).isEqualTo(CEILING_SLOTS);

        pc.applyAdmissionTargetToWorkerPool(0);
        assertWithMessage("a below-floor value must clamp to one worker")
                .that(steerablePool().getCorePoolSize()).isEqualTo(1);
    }

    /** OBSERVE ticks measure without acting: windows close, the would-be target moves, the pool is never touched. */
    @Test
    void observeNeverSteersTheCorePoolSize() {
        var recordingPool = new RecordingThreadPoolExecutor(CEILING_SLOTS);
        buildRecordingHarness(optionsBuilder()
                .maxConcurrency(CEILING_SLOTS)
                .adaptiveConcurrencyMode(AdaptiveConcurrencyMode.OBSERVE)
                .adaptiveConcurrencyInitialTarget(CONTRACTED_TARGET_SLOTS)
                .build(), recordingPool);
        pc.setState(State.RUNNING);

        for (int window = 0; window < 3; window++) {
            feedHealthySamples();
            // OBSERVE's boundary target is the STATIC maxConcurrency (a non-acting mode resizes nothing), so
            // that is the level the boundary sample must saturate for the window to read LIMIT-BOUND.
            markPoolSaturatedAt(module.admissionTargetSlots());
            module.clock.add(WINDOW_STEP);
            pc.tickAdmissionController();
        }

        assertWithMessage("fixture: OBSERVE must have been measuring - its hypothetical target moved")
                .that(controller().wouldBeTarget()).isGreaterThan(CONTRACTED_TARGET_SLOTS);
        assertWithMessage("a non-acting mode must never call setCorePoolSize")
                .that(recordingPool.coreSizeCalls).isEmpty();
    }

    // --- R10: non-steerable pools - actuator inert, dispatch gating remains the bound ---

    /**
     * A pool that is not a {@link ThreadPoolExecutor} - a virtual-thread executor, or a subclass-overridden pool
     * (the {@code Executors.newSingleThreadExecutor()} wrapper here is both shapes at once, exactly how
     * {@code isVirtualThreadPool()} recognises queueless pools) - leaves the actuator INERT on every path, while
     * the R12 dispatch gate keeps bounding admission in tasks.
     */
    @Test
    void nonSteerablePoolsLeaveTheActuatorInertButKeepTheDispatchGate() {
        pc = new TestParallelEoSStreamProcessor<>(enforceOptions(CONTRACTED_TARGET_SLOTS)) {
            @Override
            protected ExecutorService setupWorkerPool(int poolSize) {
                return Executors.newSingleThreadExecutor();
            }
        };
        pc.setWm(mockedWorkManager(CONTRACTED_TARGET_SLOTS));
        pc.setState(State.RUNNING);

        // every actuator path must no-op rather than cast or throw
        pc.applyAdmissionTargetToWorkerPool(20);
        pc.widenWorkerPoolForShutdown();

        assertWithMessage("in-flight at the published target: the dispatch gate is still the bound")
                .that(pc.calculateQuantityToRequest()).isAtMost(0);
        pc.setWm(mockedWorkManager(CONTRACTED_TARGET_SLOTS - 3));
        assertWithMessage("below the target the gate must top up to it, exactly as on a steerable pool")
                .that(pc.calculateQuantityToRequest()).isEqualTo(3);
        pc.setState(State.CLOSED);
    }

    // --- R11: DRAINING and CLOSING widen the pool to the ceiling ---

    /**
     * The edge action: entering DRAINING with the target contracted widens {@code corePoolSize} to the ceiling
     * BEFORE in-flight work is awaited - without it, a drain that starts below the ceiling races
     * {@code drainTimeout} at contracted width, and a breach discards in-flight work.
     */
    @Test
    void enteringDrainingWidensTheCorePoolToTheCeiling() {
        pc = new TestParallelEoSStreamProcessor<>(enforceOptions(CONTRACTED_TARGET_SLOTS));
        assertWithMessage("fixture: the pool starts at the contracted seed")
                .that(steerablePool().getCorePoolSize()).isEqualTo(CONTRACTED_TARGET_SLOTS);

        pc.transitionToDraining();

        assertThat(pc.getState()).isEqualTo(State.DRAINING);
        assertWithMessage("the DRAINING edge must widen the core to the ceiling")
                .that(steerablePool().getCorePoolSize()).isEqualTo(CEILING_SLOTS);
    }

    /**
     * The mandatory backstop, isolated from the edge action: a {@code DONT_DRAIN} close never runs
     * {@code transitionToDraining}, so only the {@code innerDoClose} backstop can widen on this path - a real
     * engine, closed for real, must end with the core at the ceiling. (In a DRAIN close the same backstop is
     * what repairs a concurrent tick's transient re-narrowing after the edge action.)
     */
    @Test
    void doCloseWidensThePoolBeforeShutdownTheBackstop() {
        var mockConsumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(mockConsumer)
                .maxConcurrency(CEILING_SLOTS)
                .adaptiveConcurrencyMode(AdaptiveConcurrencyMode.ENFORCE)
                .adaptiveConcurrencyInitialTarget(2)
                // pinned like optionsBuilder(): the widen is a ThreadPoolExecutor core-size move, which the
                // virtual-thread pool does not have - under -Dpc.virtualThreads=true the cast below would throw
                .useVirtualThreads(false)
                .build();
        var processor = new ParallelEoSStreamProcessor<String, String>(options);
        var pool = (ThreadPoolExecutor) processor.workerThreadPool.get();
        try {
            processor.subscribe(UniLists.of(TOPIC));
            // the MockConsumer rebalance dance - see MockConsumerTestBase: beginning offsets BEFORE the assignment
            mockConsumer.updateBeginningOffsets(Collections.singletonMap(tp, 0L));
            mockConsumer.rebalance(Collections.singletonList(tp));
            processor.onPartitionsAssigned(UniLists.of(tp));
            processor.poll(context -> {
            });
            assertWithMessage("fixture: the pool must start contracted, or the widen has nothing to prove")
                    .that(pool.getCorePoolSize()).isEqualTo(2);
        } finally {
            processor.close(); // DONT_DRAIN: transitionToClosing, never transitionToDraining - no edge action ran
        }

        assertWithMessage("only the innerDoClose backstop can have widened on the DONT_DRAIN path")
                .that(pool.getCorePoolSize()).isEqualTo(CEILING_SLOTS);
    }

    // --- R12: the dispatch gate is task-denominated, with the record seam as cap ---

    /**
     * Under-filled batches must not queue tasks beyond slots. Thin availability (3 records against a batch size
     * of 4) means each pass dispatches one UNDER-FILLED task, so record arithmetic alone would keep admitting
     * until the record seam (32) filled - queueing tasks far past the 8 slots the target commands, which is the
     * excluded-queue-wait loop. The task-denominated gate stops admission at 8 tasks, record headroom or not.
     */
    @Test
    void underFilledBatchesNeverPutMoreTasksInFlightThanTheTargetHasSlots() {
        buildClockedHarness(optionsBuilder()
                .maxConcurrency(CEILING_SLOTS)
                .batchSize(UNDER_FILL_BATCH_SIZE)
                .adaptiveConcurrencyMode(AdaptiveConcurrencyMode.ENFORCE)
                .adaptiveConcurrencyInitialTarget(CONTRACTED_TARGET_SLOTS)
                .build());
        wm.onPartitionsAssigned(UniLists.of(tp));
        pc.setState(State.RUNNING);

        for (int pass = 1; pass <= CONTRACTED_TARGET_SLOTS; pass++) {
            registerWork(THIN_AVAILABILITY);
            assertWithMessage("pass %s: a free slot remains, so the thin availability must dispatch", pass)
                    .that(pc.retrieveAndDistributeNewWork(this::latchingUserFunction, callback))
                    .isEqualTo(THIN_AVAILABILITY);
            awaitActiveTasks(pass);
        }
        assertWithMessage("fixture: the record seam must still have headroom - only the SLOT gate stops pass 9")
                .that(wm.getNumberRecordsOutForProcessing())
                .isLessThan(CONTRACTED_TARGET_SLOTS * UNDER_FILL_BATCH_SIZE);

        registerWork(THIN_AVAILABILITY);
        assertWithMessage("all %s slots hold an under-filled task: admission must stop despite record headroom",
                CONTRACTED_TARGET_SLOTS)
                .that(pc.retrieveAndDistributeNewWork(this::latchingUserFunction, callback)).isEqualTo(0);
        assertWithMessage("tasks in flight never exceed the target's slots")
                .that(pc.userFunctionTaskAccounting().getActive()).isEqualTo(CONTRACTED_TARGET_SLOTS);
    }

    /**
     * The cap side of the min: with FULL batches and idle slots, admitted records must never exceed the record
     * seam. 28 of the seam's 32 records are in flight, all 8 slots idle - a slots-only gate would ask for
     * {@code freeSlots x batchSize = 32}; the seam remainder (4) must win.
     */
    @Test
    void theRecordSeamCapsAdmissionEvenWithIdleSlots() {
        pc = new TestParallelEoSStreamProcessor<>(optionsBuilder()
                .maxConcurrency(CEILING_SLOTS)
                .batchSize(UNDER_FILL_BATCH_SIZE)
                .adaptiveConcurrencyMode(AdaptiveConcurrencyMode.ENFORCE)
                .adaptiveConcurrencyInitialTarget(CONTRACTED_TARGET_SLOTS)
                .build());
        int seamRecords = CONTRACTED_TARGET_SLOTS * UNDER_FILL_BATCH_SIZE;
        pc.setWm(mockedWorkManager(seamRecords - UNDER_FILL_BATCH_SIZE));
        pc.setState(State.RUNNING);

        assertWithMessage("the seam remainder must cap the request, however many slots sit idle")
                .that(pc.calculateQuantityToRequest()).isEqualTo(UNDER_FILL_BATCH_SIZE);
        pc.setState(State.CLOSED);
    }

    // --- helpers ---

    static final String TOPIC = "admission-pool-actuator-topic";
    static final int UNDER_FILL_BATCH_SIZE = 4;
    static final int THIN_AVAILABILITY = 3;

    final TopicPartition tp = new TopicPartition(TOPIC, 0);
    final Consumer<String> callback = result -> {
    };

    WorkManager<String, String> wm;

    /** Offsets never reused across a test, so unique per-record keys stay unique under default KEY ordering. */
    long nextOffset = 0;

    /** A {@link ThreadPoolExecutor} that records every {@code setCorePoolSize} call - the steering oracle. */
    static class RecordingThreadPoolExecutor extends ThreadPoolExecutor {
        final List<Integer> coreSizeCalls = Collections.synchronizedList(new ArrayList<>());

        RecordingThreadPoolExecutor(int size) {
            super(size, size, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), new AbortPolicy());
        }

        @Override
        public void setCorePoolSize(int corePoolSize) {
            coreSizeCalls.add(corePoolSize);
            super.setCorePoolSize(corePoolSize);
        }
    }

    private static ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> optionsBuilder() {
        return ParallelConsumerOptions.<String, String>builder()
                .consumer(new MockConsumer<>(OffsetResetStrategy.LATEST))
                // pinned: the platform-vs-virtual axis is exercised explicitly by the non-steerable-pool tests
                .useVirtualThreads(false);
    }

    private static ParallelConsumerOptions<String, String> enforceOptions(int seedSlots) {
        return optionsBuilder()
                .maxConcurrency(CEILING_SLOTS)
                .adaptiveConcurrencyMode(AdaptiveConcurrencyMode.ENFORCE)
                .adaptiveConcurrencyInitialTarget(seedSlots)
                .build();
    }

    /** A processor over a {@link AdmissionLifecycleTest.ClockedModule}, so the law's windows are test-driven. */
    private void buildClockedHarness(ParallelConsumerOptions<String, String> options) {
        module = new AdmissionLifecycleTest.ClockedModule(options);
        pc = new TestParallelEoSStreamProcessor<>(options, module);
        wm = module.workManager();
        pc.setWm(wm);
    }

    /** As above, but over a {@link RecordingThreadPoolExecutor} and a mocked (empty) work manager. */
    private void buildRecordingHarness(ParallelConsumerOptions<String, String> options,
                                       RecordingThreadPoolExecutor recordingPool) {
        module = new AdmissionLifecycleTest.ClockedModule(options);
        pc = new TestParallelEoSStreamProcessor<>(options, module) {
            @Override
            protected ExecutorService setupWorkerPool(int poolSize) {
                return recordingPool;
            }
        };
        pc.setWm(mockedWorkManager(0));
    }

    private WorkManager<String, String> mockedWorkManager(int inFlight) {
        WorkManager<String, String> mockWm = Mockito.mock(WorkManager.class);
        Mockito.when(mockWm.getNumberRecordsOutForProcessing()).thenReturn(inFlight);
        return mockWm;
    }

    private AdmissionController controller() {
        return module.admissionController();
    }

    private ThreadPoolExecutor steerablePool() {
        return (ThreadPoolExecutor) pc.workerThreadPool.get();
    }

    /**
     * Grows the published target past {@code threshold} by feeding healthy, saturated windows through the
     * ENGINE's tick (so the actuator sees each movement). Bounded, and asserted to have got there - the law's
     * growth cadence is its own business, but it must grow under this signal or the fixture is broken.
     * (Growth here is the U5 band machine's warmup band, which the binding gate licenses only on a LIMIT-BOUND
     * boundary sample - hence the saturation marking, the {@link AdaptiveConcurrencyModeTest} fixture assist.)
     */
    private void growTargetAbove(int threshold) {
        for (int window = 0; window < 40 && controller().currentTarget() <= threshold; window++) {
            feedHealthySamples();
            markPoolSaturatedAt(module.admissionTargetSlots());
            module.clock.add(WINDOW_STEP);
            pc.tickAdmissionController();
        }
        assertWithMessage("fixture: healthy saturated bound windows must grow the target past %s", threshold)
                .that(controller().currentTarget()).isGreaterThan(threshold);
    }

    /**
     * Tops the task ACCOUNTING up to {@code slots} active, so the window's boundary sample classifies
     * LIMIT-BOUND (the U5 binding gate) - the pool itself stays real and untouched.
     */
    private void markPoolSaturatedAt(int slots) {
        int deficit = slots - pc.userFunctionTaskAccounting().getActive();
        for (int task = 0; task < deficit; task++) {
            pc.userFunctionTaskAccounting().onSubmitting();
            pc.userFunctionTaskAccounting().onTaskStarted();
        }
    }

    /**
     * One healthy, saturated window's worth of raw signal - flat 10ms latency, in-flight pinned at the live
     * target, all successes (the {@link AdmissionLifecycleTest} fixture).
     */
    private void feedHealthySamples() {
        var controller = controller();
        int inFlight = controller.currentTarget();
        for (int i = 0; i < SAMPLES; i++) {
            controller.recordServiceTime(10 * MS);
            controller.recordInFlight(inFlight);
            controller.recordOutcome(Outcome.SUCCESS);
        }
    }

    /**
     * One saturated window that reports the downstream OVERLOADED - drops fire the law's multiplicative cut,
     * which is how a mid-load contraction is driven through the engine rather than injected past it.
     */
    private void feedOverloadedSamples() {
        var controller = controller();
        int inFlight = controller.currentTarget();
        for (int i = 0; i < SAMPLES; i++) {
            controller.recordServiceTime(10 * MS);
            controller.recordInFlight(inFlight);
            controller.recordOutcome(Outcome.OVERLOAD_DROP);
        }
    }

    /** A user function that parks its worker until the test (or teardown) releases it. */
    private List<String> latchingUserFunction(PollContextInternal<String, String> context) {
        parkUntilReleased();
        return new ArrayList<>();
    }

    /** Awaits the CONVERGED task count - never a moving comparison (the write-time testing rule). */
    private void awaitActiveTasks(int expected) {
        Awaitility.await()
                .atMost(Duration.ofSeconds(5))
                .alias(expected + " tasks running the user function")
                .until(() -> pc.userFunctionTaskAccounting().getActive() == expected);
    }

    /**
     * Registers {@code count} records, left sitting in the shards - selectable but not taken (the
     * {@link AdmissionSeamTest} fixture).
     */
    private void registerWork(int count) {
        var records = new ArrayList<ConsumerRecord<String, String>>();
        for (int i = 0; i < count; i++) {
            long offset = nextOffset++;
            records.add(new ConsumerRecord<>(TOPIC, tp.partition(), offset, "key-" + offset, "value"));
        }
        wm.registerWork(new EpochAndRecordsMap<>(new ConsumerRecords<>(UniMaps.of(tp, records)), wm.getPm()));
    }

    /** A worker task that parks until the test (or teardown) releases it - interrupt-clean. */
    private void parkUntilReleased() {
        try {
            releaseWorkers.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
