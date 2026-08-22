package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.AdaptiveConcurrencyMode;
import bz.stub.parallelconsumer.PollContextInternal;
import bz.stub.parallelconsumer.internal.admission.AdmissionController;
import bz.stub.parallelconsumer.internal.admission.AdmissionController.Outcome;
import bz.stub.parallelconsumer.internal.admission.AdmissionDecisionReason;
import bz.stub.parallelconsumer.state.WorkManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.threeten.extra.MutableClock;
import pl.tlinkowski.unij.api.UniMaps;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Pins the lifecycle integration (the plan's U7, {@code docs/plans/2026-08-18-001-feat-self-scaling-concurrency-plan.md}):
 * how the admission controller's tick rides the engine, and how the engine's lifecycle feeds back into admission.
 * <ul>
 * <li><b>Tick gating (KTD7)</b> - {@link AbstractParallelEoSStreamProcessor#tickAdmissionController()} runs once
 * per control-loop pass, driven directly here the way {@link AdmissionSignalPlumbingTest} drives the in-flight
 * sampler; it acts only across the controller's own injected-clock window boundary, only while {@code RUNNING},
 * and never in {@code DISABLED}.</li>
 * <li><b>Pause poison (R13)</b> - a RUNNING-&gt;PAUSED-&gt;RUNNING cycle discards the in-progress window at the
 * first post-resume tick, so pre-pause samples never appear in the first post-resume window.</li>
 * <li><b>Drain release (KTD9)</b> - {@link PCModule#admissionTargetRecords()} is STATE-DERIVED: only a
 * {@code RUNNING} processor reads the live target; every other state reads the effective-maximum derivation, with
 * no tick and no edge action required - including when {@code close()} arrives before the control loop ever
 * ran.</li>
 * <li><b>Load-factor pinning (KTD10)</b> - adaptive ENFORCE constructs {@link DynamicLoadFactor} static
 * (initial == max) through {@link PCModule}, with the {@code messageBufferSize} branch dividing by the
 * CEILING-derived in-flight figure; DISABLED and OBSERVE construction stays byte-for-byte today's.</li>
 * </ul>
 * <b>Deferred INTEGRATION scenarios</b> (broker-backed, deliberately not built in this unit):
 * <ul>
 * <li>AE7's transactional variant: contracted target + buffered records + {@code close(DRAIN)} under
 * transactional commit mode - the released dispatch burst must produce no {@code produceLockAcquisitionTimeout}
 * failures while the commit lock cycles. The unit-level AE7 arm (full-width drain dispatch) is
 * {@link #drainDispatchesAtFullWidthUnderAContractedTarget()}.</li>
 * <li>The tick observed through a REAL control-loop pass (engine-level, the plan's U9/U10) - here the per-pass
 * method is driven directly, the same trust boundary U6 drew for the in-flight sampler.</li>
 * </ul>
 * Time is driven via {@link MutableClock} injected through the module; no wall clock anywhere.
 */
class AdmissionLifecycleTest {

    static final String TOPIC = "admission-lifecycle-topic";

    static final long MS = 1_000_000L; // nanos per millisecond

    /** The user's explicit {@code maxConcurrency} - the ceiling ENFORCE adapts under. */
    static final int CEILING_SLOTS = 24;

    /** A live target well below the ceiling, seeded so no warmup windows need driving. */
    static final int CONTRACTED_TARGET_SLOTS = 8;

    /**
     * Comfortably past {@code AdmissionController#SAMPLE_WINDOW_DURATION} (package-private to the admission
     * package, so restated here): a step of this size always crosses the window boundary. If the cadence is ever
     * raised past it, these tests fail loudly on "reason never updated".
     */
    static final Duration WINDOW_STEP = Duration.ofSeconds(2);

    /**
     * Comfortably above the law's per-window minimum sample count ({@code DEFAULT_MIN_SAMPLES_PER_WINDOW}, also
     * package-private to the admission package), so a fed window is always acted on rather than held.
     */
    static final int SAMPLES = 12;

    final TopicPartition tp = new TopicPartition(TOPIC, 0);

    final Function<PollContextInternal<String, String>, List<String>> userFunction = context -> new ArrayList<>();
    final Consumer<String> callback = result -> {
    };

    ClockedModule module;
    TestParallelEoSStreamProcessor<String, String> pc;
    WorkManager<String, String> wm;

    /** Offsets never reused across a test, so unique per-record keys stay unique under default KEY ordering. */
    long nextOffset = 0;

    /** A {@link PCModule} whose injected clock the test can move - everything else real. */
    static class ClockedModule extends PCModule<String, String> {
        final MutableClock clock = MutableClock.epochUTC();

        ClockedModule(ParallelConsumerOptions<String, String> options) {
            super(options);
        }

        @Override
        public Clock clock() {
            return clock;
        }
    }

    @AfterEach
    void tearDown() {
        if (pc != null) {
            // these tests never start the control loop, so the normal close handshake does not apply
            pc.setState(State.CLOSED);
            pc.close();
            pc.workerThreadPool.get().shutdownNow();
        }
    }

    // --- tick gating: window boundary, state, mode (KTD7) ---

    /**
     * Per-pass contract: passes below the window's injected-clock boundary decide nothing; the first pass across
     * it does. The boundary is the CONTROLLER's clock deadline - nothing here touches the loop's block time.
     */
    @Test
    void tickActsOnlyOnceTheWindowBoundaryElapses() {
        buildEnforceHarness();
        pc.setState(State.RUNNING);
        feedHealthySamples();

        for (int pass = 0; pass < 3; pass++) {
            pc.tickAdmissionController();
        }
        assertWithMessage("no window boundary has elapsed on the injected clock - no pass may decide")
                .that(controller().lastDecisionReason()).isEmpty();

        module.clock.add(WINDOW_STEP);
        pc.tickAdmissionController();
        assertWithMessage("the first pass across the boundary must close the window and decide")
                .that(controller().lastDecisionReason()).hasValue(AdmissionDecisionReason.ADAPTING);
    }

    /**
     * The state gate: no tick outside {@code RUNNING} - PAUSED and DRAINING passes decide nothing even across a
     * boundary (DRAINING's release is the seam read, not a tick) - and the RUNNING control arm proves the same
     * setup does tick.
     */
    @Test
    void noTickOutsideRunningState() {
        buildEnforceHarness();
        feedHealthySamples();
        module.clock.add(WINDOW_STEP);

        pc.setState(State.PAUSED);
        pc.tickAdmissionController();
        assertWithMessage("a PAUSED pass must not tick").that(controller().lastDecisionReason()).isEmpty();

        pc.setState(State.DRAINING);
        pc.tickAdmissionController();
        assertWithMessage("a DRAINING pass must not tick").that(controller().lastDecisionReason()).isEmpty();

        pc.setState(State.RUNNING);
        pc.tickAdmissionController();
        assertWithMessage("the RUNNING control arm: the same pass must tick")
                .that(controller().lastDecisionReason()).isPresent();
    }

    /**
     * DISABLED never ticks: the gate short-circuits on the resolved active flag before touching the (inert)
     * controller.
     */
    @Test
    void disabledNeverTicks() {
        buildHarness(optionsBuilder().build()); // adaptiveConcurrencyMode defaults to DISABLED
        pc.setState(State.RUNNING);
        module.clock.add(WINDOW_STEP);

        pc.tickAdmissionController();

        assertThat(controller().lastDecisionReason()).isEmpty();
    }

    // --- pause poison (R13): the first post-resume window carries no pre-pause samples ---

    /**
     * Samples recorded before a pause must not appear in the first post-resume window: the first post-resume tick
     * consumes the poison and discards the window, so the next boundary closes an EMPTY window (a hold,
     * APP_LIMITED) rather than deciding on stale samples (which would be ADAPTING - the sabotage signature: with
     * the poison consumption removed from tickAdmissionController, this fails exactly there).
     */
    @Test
    void pauseResumeCycleDiscardsPrePauseSamples() {
        buildEnforceHarness();
        pc.setState(State.RUNNING);
        feedHealthySamples();
        int targetBefore = controller().currentTarget();

        pc.pauseIfRunning();
        pc.resumeIfPaused();

        pc.tickAdmissionController(); // consumes the poison: discards the window, decides nothing
        assertThat(controller().lastDecisionReason()).isEmpty();

        module.clock.add(WINDOW_STEP);
        pc.tickAdmissionController();
        assertWithMessage("the first post-resume window must close empty - pre-pause samples discarded")
                .that(controller().lastDecisionReason()).hasValue(AdmissionDecisionReason.APP_LIMITED);
        assertWithMessage("an empty hold must not move the target")
                .that(controller().currentTarget()).isEqualTo(targetBefore);
    }

    // --- drain release (KTD9): the seam read is state-derived ---

    /**
     * The KTD9 table: only {@code RUNNING} reads the live (contracted) target; every other state reads the
     * effective-maximum derivation - and the read flips both ways with no tick in between.
     */
    @Test
    void drainReleaseIsAStateDerivedSeamRead() {
        buildEnforceHarness();

        pc.setState(State.RUNNING);
        assertThat(module.admissionTargetRecords()).isEqualTo(CONTRACTED_TARGET_SLOTS);

        pc.setState(State.DRAINING);
        assertWithMessage("DRAINING must release to the effective maximum - a contracted target must never "
                + "stretch a drain")
                .that(module.admissionTargetRecords()).isEqualTo(CEILING_SLOTS);

        pc.setState(State.RUNNING);
        assertWithMessage("back to RUNNING, back to the live target - it is a read, not an edge")
                .that(module.admissionTargetRecords()).isEqualTo(CONTRACTED_TARGET_SLOTS);

        pc.setState(State.PAUSED);
        assertThat(module.admissionTargetRecords()).isEqualTo(CEILING_SLOTS);
    }

    /**
     * {@code close()} before the control loop ever ran: the state-derived read needs no tick, so the release
     * holds from {@code UNUSED} straight through {@code CLOSED}.
     */
    @Test
    void drainReleaseHoldsWhenCloseComesBeforeTheLoopEverRan() {
        buildEnforceHarness();

        assertWithMessage("UNUSED (the loop never ran): already the effective-maximum derivation")
                .that(module.admissionTargetRecords()).isEqualTo(CEILING_SLOTS);

        pc.close(); // UNUSED -> CLOSED directly; no control thread exists to hand the close to

        assertWithMessage("CLOSED: still the effective-maximum derivation, no tick ever having run")
                .that(module.admissionTargetRecords()).isEqualTo(CEILING_SLOTS);
    }

    /**
     * AE7, unit level: a contracted target with more-than-a-ceiling of buffered records dispatches at the LIVE
     * width while RUNNING, then at FULL width on the first DRAINING pass - the release is what keeps
     * {@code close(DRAIN)} inside {@code drainTimeout}. (The transactional variant is a deferred integration
     * scenario - see the class javadoc.)
     */
    @Test
    void drainDispatchesAtFullWidthUnderAContractedTarget() {
        buildEnforceHarness();
        registerWork(CEILING_SLOTS + 6);
        pc.setState(State.RUNNING);

        int runningPass = pc.retrieveAndDistributeNewWork(userFunction, callback);
        assertWithMessage("while RUNNING, dispatch is bounded by the contracted live target")
                .that(runningPass).isEqualTo(CONTRACTED_TARGET_SLOTS);

        pc.setState(State.DRAINING);
        int drainingPass = pc.retrieveAndDistributeNewWork(userFunction, callback);
        assertWithMessage("the first DRAINING pass must top dispatch up to the effective maximum")
                .that(drainingPass).isEqualTo(CEILING_SLOTS - CONTRACTED_TARGET_SLOTS);
        assertWithMessage("everything the ceiling allows is now out for processing")
                .that(wm.getNumberRecordsOutForProcessing()).isEqualTo(CEILING_SLOTS);
    }

    // --- a grown target re-checks the poller wakeup path ---

    /**
     * A tick that GROWS the target re-runs the poller wakeup check the same pass (its first probe is
     * {@code WorkManager#isSufficientlyLoaded()}), so a poller paused for throttling notices the new headroom
     * without waiting a pass; a tick that holds the target must not. The wake channel is
     * {@code ConsumerManager#wakeup()} - no thread interrupt is involved.
     */
    @Test
    void aGrowthTickRechecksThePollerWakeupPathAHoldDoesNot() {
        var options = enforceOptions(CONTRACTED_TARGET_SLOTS);
        module = new ClockedModule(options);
        pc = new TestParallelEoSStreamProcessor<>(options, module);
        WorkManager<String, String> mockedWm = Mockito.mock(WorkManager.class);
        pc.setWm(mockedWm);
        pc.setState(State.RUNNING);

        // Window 1: healthy and saturated, but smoothing keeps the published target at 8 (8 -> est. 8.8) - a hold.
        feedHealthySamples();
        module.clock.add(WINDOW_STEP);
        pc.tickAdmissionController();
        assertWithMessage("fixture: the first window must not move the published target")
                .that(controller().currentTarget()).isEqualTo(CONTRACTED_TARGET_SLOTS);
        Mockito.verify(mockedWm, Mockito.never()).isSufficientlyLoaded();

        // Window 2: the estimate crosses a whole slot (8.8 -> 9.6, published 9) - growth, so the check runs.
        feedHealthySamples();
        module.clock.add(WINDOW_STEP);
        pc.tickAdmissionController();
        assertWithMessage("fixture: the second window must grow the published target")
                .that(controller().currentTarget()).isGreaterThan(CONTRACTED_TARGET_SLOTS);
        Mockito.verify(mockedWm, Mockito.times(1)).isSufficientlyLoaded();
    }

    // --- KTD10: load-factor pinning at construction ---

    /**
     * ENFORCE pins the factor static at the initial value (initial == max): {@code isStatic()} true from
     * construction, nowhere to step. Dispatch consuming the live target un-multiplied is {@link AdmissionSeamTest}'s
     * pin; this is the factor half.
     */
    @Test
    void enforcePinsTheLoadFactorStatic() {
        var factorModule = new PCModule<>(enforceOptions(CONTRACTED_TARGET_SLOTS));

        DynamicLoadFactor factor = factorModule.dynamicExtraLoadFactor();

        assertThat(factor.isStatic()).isTrue();
        assertThat(factor.getCurrentFactor()).isEqualTo(DynamicLoadFactor.DEFAULT_INITIAL_LOADING_FACTOR);
        assertWithMessage("initial == max: the ceiling is reached at construction, so the factor can never climb")
                .that(factor.isMaxReached()).isTrue();
    }

    /**
     * The non-acting modes are byte-for-byte today's construction - the gate must not leak the pin into OBSERVE
     * (a mode advertised as pure measurement) or DISABLED.
     */
    @Test
    void observeAndDisabledKeepTodaysFactorConstruction() {
        for (var mode : new AdaptiveConcurrencyMode[]{AdaptiveConcurrencyMode.DISABLED, AdaptiveConcurrencyMode.OBSERVE}) {
            var factorModule = new PCModule<>(optionsBuilder().adaptiveConcurrencyMode(mode).build());

            DynamicLoadFactor factor = factorModule.dynamicExtraLoadFactor();

            assertWithMessage("mode %s must construct today's dynamic factor", mode)
                    .that(factor.getCurrentFactor()).isEqualTo(DynamicLoadFactor.DEFAULT_INITIAL_LOADING_FACTOR);
            assertWithMessage("mode %s must keep today's ceiling", mode)
                    .that(factor.getMaxFactor()).isEqualTo(DynamicLoadFactor.DEFAULT_MAX_LOADING_FACTOR);
            assertWithMessage("mode %s must stay genuinely dynamic", mode)
                    .that(factor.isStatic()).isFalse();
        }
    }

    /**
     * The {@code messageBufferSize} branch under ENFORCE divides by the CEILING-derived in-flight figure
     * (effective maximum x batch size - here the KTD4-substituted default ceiling of 64), never the seed and never
     * the static {@code maxConcurrency} derivation: 640 records / (64 x 1) = 10. The seed-derived figure would be
     * 160, the static-derived 40 - both wrong, both loudly distinguishable.
     */
    @Test
    void messageBufferSizeUnderEnforceDividesByTheCeilingDerivedFigure() {
        var factorModule = new PCModule<>(ParallelConsumerOptions.<String, String>builder()
                // maxConcurrency deliberately left at the library default: ENFORCE substitutes the adaptive
                // default ceiling (64), which is exactly what makes this derivation distinguishable from today's
                .adaptiveConcurrencyMode(AdaptiveConcurrencyMode.ENFORCE)
                .adaptiveConcurrencyInitialTarget(4)
                .messageBufferSize(640)
                .build());

        DynamicLoadFactor factor = factorModule.dynamicExtraLoadFactor();

        assertThat(factor.getCurrentFactor()).isEqualTo(10);
        assertThat(factor.getMaxFactor()).isEqualTo(10);
        assertThat(factor.isStatic()).isTrue();
    }

    /** The remainder still rounds up under the ceiling derivation: ceil(650 / 64) = 11. */
    @Test
    void messageBufferSizeUnderEnforceRoundsTheCeilingDivisionUp() {
        var factorModule = new PCModule<>(ParallelConsumerOptions.<String, String>builder()
                .adaptiveConcurrencyMode(AdaptiveConcurrencyMode.ENFORCE)
                .messageBufferSize(650)
                .build());

        assertThat(factorModule.dynamicExtraLoadFactor().getCurrentFactor()).isEqualTo(11);
    }

    /**
     * The control arm: outside ENFORCE the buffer derivation is exactly today's -
     * ceil(640 / (default maxConcurrency 16 x batch 1)) = 40 - in DISABLED and OBSERVE alike.
     */
    @Test
    void messageBufferSizeOutsideEnforceKeepsTodaysDerivation() {
        for (var mode : new AdaptiveConcurrencyMode[]{AdaptiveConcurrencyMode.DISABLED, AdaptiveConcurrencyMode.OBSERVE}) {
            var factorModule = new PCModule<>(ParallelConsumerOptions.<String, String>builder()
                    .adaptiveConcurrencyMode(mode)
                    .messageBufferSize(640)
                    .build());

            assertWithMessage("mode %s must keep today's messageBufferSize derivation", mode)
                    .that(factorModule.dynamicExtraLoadFactor().getCurrentFactor()).isEqualTo(40);
        }
    }

    // --- helpers ---

    private static ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> optionsBuilder() {
        return ParallelConsumerOptions.<String, String>builder()
                .consumer(new MockConsumer<>(OffsetResetStrategy.LATEST))
                .maxConcurrency(CEILING_SLOTS)
                .batchSize(1)
                // pinned: the platform-vs-virtual axis is AdmissionSeamTest's business
                .useVirtualThreads(false);
    }

    private static ParallelConsumerOptions<String, String> enforceOptions(int seedSlots) {
        return optionsBuilder()
                .adaptiveConcurrencyMode(AdaptiveConcurrencyMode.ENFORCE)
                .adaptiveConcurrencyInitialTarget(seedSlots)
                .build();
    }

    /**
     * A real processor, the real {@link WorkManager} from the SAME module, and the module's clock mutable - the
     * {@link AdmissionSignalPlumbingTest} harness plus injected time.
     */
    private void buildHarness(ParallelConsumerOptions<String, String> options) {
        module = new ClockedModule(options);
        pc = new TestParallelEoSStreamProcessor<>(options, module);
        wm = module.workManager();
        wm.onPartitionsAssigned(pl.tlinkowski.unij.api.UniLists.of(tp));
        pc.setWm(wm);
    }

    private void buildEnforceHarness() {
        buildHarness(enforceOptions(CONTRACTED_TARGET_SLOTS));
    }

    private AdmissionController controller() {
        return module.admissionController();
    }

    /**
     * One healthy, saturated window's worth of raw signal, fed straight to the controller (the tap plumbing is
     * {@link AdmissionSignalPlumbingTest}'s pin; here only the tick's treatment of a fed window matters): flat
     * 10ms latency, in-flight pinned at the live target, all successes.
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
}
