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
import org.threeten.extra.MutableClock;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Proves the adaptive-concurrency path <b>ran</b>, rather than proving it exists - the marker class for the
 * {@code adaptive-concurrency} execution-mode lane ({@code bin/check-execution-mode.sh}).
 *
 * <h2>Why an assumption would have been the wrong tool</h2>
 * <p>
 * The natural way to write these is {@code Assumptions.assumeTrue(adaptiveConcurrencyActive())}, and it is the
 * failure mode this repository has already shipped once: a test that skips and reports green having verified
 * nothing ({@code docs/solutions/workflow-issues/a-check-that-reports-success-without-having-run.md}). So the
 * gate is INVERTED and keyed on <b>intent</b>: when {@value ParallelConsumerOptions#ADAPTIVE_CONCURRENCY_MODE_PROPERTY}
 * asks for a mode - which is how CI's execution-mode matrix runs this lane - a build that does not deliver it is a
 * <b>failure</b>, never a skip.
 * <p>
 * Unlike {@link VirtualThreadExecutionModeTest}, there is no capability that can be absent here: adaptive
 * concurrency is configuration, available on every runtime. So this class carries <b>no assumption at all</b> -
 * nothing in it can skip, on any JDK, with or without the property. Zero executed tests therefore means the lane
 * is broken, which is exactly what {@code bin/check-execution-mode.sh} concludes from the same count.
 *
 * <h2>What is in here, and what is next door</h2>
 * <ul>
 * <li><b>The selection proof</b> - {@link #theSelectedModeReachesTheEngineRatherThanBeingIgnored()}: the lane's
 * {@code -D} really did reach surefire's fork and really did reach the engine.</li>
 * <li><b>The observe-only "could have said yes" proof</b> -
 * {@link #observeComputesADifferentTargetFromTheOneItPublishes()}: OBSERVE's whole product is a number it declines
 * to act on, so a test that only checks OBSERVE changed nothing is unfalsifiable. This one makes the shadow
 * computation MOVE, against an ENFORCE control arm fed the identical signals.</li>
 * <li><b>The truth probe</b> - {@link #theReportedInternalViewMatchesAnIndependentlyComputedExpectation()}: the
 * controller's own account of itself, against an expectation computed from the arranged inputs rather than read
 * back off the controller.</li>
 * <li><b>The rip-out triad</b> - {@link #theTargetGrowsFromALowSeedWithinABoundedNumberOfWindows()},
 * {@link #theTargetNeverExceedsTheEffectiveMaximum()},
 * {@link #noWindowContractsTheTargetByMoreThanTheBoundedStep()}: the plan's stop conditions, asserted here against
 * the REAL engine rather than (as {@code AdmissionLawFalsifierTest} does) against the deterministic plant.
 * Migrated with the U5 band-machine rewrite: growth is now the KTD2 warmup allowance (bounded blind growth),
 * not the old law's climb-to-the-ceiling - which was precisely the ratchet the rewrite deletes.</li>
 * </ul>
 * <p>
 * <b>HOW THE ENGINE IS DRIVEN, and what that does not cover.</b> The ENFORCE tests drive a real
 * {@link AbstractParallelEoSStreamProcessor} with a real {@link WorkManager} and real dispatch
 * ({@code AbstractParallelEoSStreamProcessor#retrieveAndDistributeNewWork}), sampling in-flight through the
 * engine's own {@link AbstractParallelEoSStreamProcessor#sampleAdmissionInFlight()} so that signal comes off real
 * engine state, and stepping windows through
 * {@link AbstractParallelEoSStreamProcessor#tickAdmissionController()} on an injected {@link MutableClock}. It is
 * NOT a true end-to-end control-loop drive, and deliberately so: the window boundary is on the module's injected
 * clock, so a running control loop would race the test thread's clock advances, and the only alternative - real
 * one-second windows - buys nothing but wall time and flakiness. The two things this drive therefore does not
 * exercise are the control loop's own scheduling of the tick (pinned by {@link AdmissionLifecycleTest}) and the
 * service-time tap (pinned by {@link AdmissionSignalPlumbingTest}); service times are fed to the controller
 * directly here.
 *
 * @see ParallelConsumerOptions#getAdaptiveConcurrencyMode()
 */
class AdaptiveConcurrencyModeTest {

    /**
     * The system property {@link ParallelConsumerOptions} defaults {@code adaptiveConcurrencyMode} from, and the
     * one CI's execution-mode axis sets. Its presence is what turns a would-be skip into a failure below.
     */
    private static final String MODE_PROPERTY = ParallelConsumerOptions.ADAPTIVE_CONCURRENCY_MODE_PROPERTY;

    static final String TOPIC = "adaptive-concurrency-mode-topic";

    static final long MS = 1_000_000L; // nanos per millisecond

    /** The user's explicit {@code maxConcurrency} - so no KTD4 ceiling substitution is in play anywhere here. */
    static final int CEILING_SLOTS = 32;

    /** A deliberately low ENFORCE seed, so growth has somewhere to come from. */
    static final int LOW_SEED_SLOTS = 4;

    /**
     * Comfortably past {@code AdmissionController#SAMPLE_WINDOW_DURATION} (package-private to the admission
     * package, so restated here, as {@link AdmissionLifecycleTest} restates it): a step of this size always
     * crosses the window boundary.
     */
    static final Duration WINDOW_STEP = Duration.ofSeconds(2);

    /**
     * Comfortably above the law's per-window minimum sample count ({@code DEFAULT_MIN_SAMPLES_PER_WINDOW}, also
     * package-private to the admission package), so a fed window is always acted on rather than held.
     */
    static final int SAMPLES = 12;

    /**
     * The steepest single-window contraction the law's per-window brakes may produce, restated from
     * {@code AdmissionControlLaw.AIMD_BACKOFF_RATIO} (package-private to the admission package): the BACKOFF
     * brake and the FALL band's contraction are both exactly this. The band machine's RETRACTION (taking back a
     * provisional growth step the next verdict showed bought nothing) may land deeper in one window, but only
     * ever on a previously-held recorded baseline - pinned in {@code AdmissionControlLawTest}, not here. A
     * published target is the truncated estimate, and truncation is monotonic, so {@code floor(previous x
     * ratio)} is a sound bound on the published series too.
     */
    static final double BOUNDED_CONTRACTION_RATIO = 0.9;

    /**
     * The band machine's per-episode blind-growth allowance in slots, restated from
     * {@code AdmissionControlLaw.DEFAULT_WARMUP_ALLOWANCE_SLOTS} (package-private to the admission package,
     * as the other law constants here are restated). With no elasticity verdict computable in this drive (see
     * {@link #driveSaturatedEnforceWindows}), the warmup band's allowance IS the whole growth budget.
     */
    static final int WARMUP_ALLOWANCE_SLOTS = 4;

    /**
     * How many windows the target is allowed to take to move off a low seed - the bound the plan's stop
     * condition actually claims: bounded, not immediate. (The warmup band moves on the very first bound window,
     * so this is generous by design.)
     */
    static final int GROWTH_DEADLINE_WINDOWS = 10;

    /** Long enough for the warmup allowance to be spent and the trajectory to settle. */
    static final int LONG_RUN_WINDOWS = 20;

    /** A seed one warmup grant below the ceiling, so the ceiling clamp genuinely binds in the cap test. */
    static final int NEAR_CEILING_SEED_SLOTS = 30;

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

    /**
     * Whether a mode was actually ASKED FOR, which is not the same as the property being present: the root pom
     * forwards {@value ParallelConsumerOptions#ADAPTIVE_CONCURRENCY_MODE_PROPERTY} to every surefire fork with a
     * default of {@code DISABLED} (it must - an EMPTY forwarded value is not "absent" and would fail
     * {@code AdaptiveConcurrencyMode.valueOf}), so the default suite sees the property set and switched off. A
     * present-but-DISABLED property is the default lane, and owes nothing.
     */
    private static boolean modeSelected() {
        String requested = System.getProperty(MODE_PROPERTY);
        return requested != null
                && !requested.trim().isEmpty()
                && !requested.trim().equalsIgnoreCase(AdaptiveConcurrencyMode.DISABLED.name());
    }

    /**
     * Fails - never skips - when the lane selected a mode the build did not deliver.
     * <p>
     * Two distinct ways that happens, and both are silent: the {@code -D} never reached surefire's forked JVM (the
     * root pom's {@code systemPropertyVariables} forwarding is what carries it, and without the forwarding the
     * whole lane runs the DEFAULT path and reports green), or it reached it and the engine refused the mode - the
     * direct-pull and external-engine refusals in
     * {@code AbstractParallelEoSStreamProcessor#resolveAdaptiveConcurrencyActive()}.
     */
    private static void requireAdaptiveConcurrencyWhenModeSelected() {
        if (!modeSelected()) {
            return; // nothing was asked for, so nothing is owed - and nothing below needs the property either
        }
        String requested = System.getProperty(MODE_PROPERTY);
        var options = options().build();
        if (options.getAdaptiveConcurrencyMode() == AdaptiveConcurrencyMode.DISABLED) {
            throw new AssertionError(String.format(
                    "-D%s=%s selected the adaptive-concurrency execution mode, but options built in this JVM "
                            + "resolved to DISABLED. The property did not reach the test JVM - surefire forks, and a "
                            + "-D on the Maven command line only reaches the fork through the root pom's "
                            + "systemPropertyVariables forwarding. This run has verified nothing about that mode.",
                    MODE_PROPERTY, requested));
        }
        try (var probe = new TestParallelEoSStreamProcessor<>(options)) {
            if (!probe.isAdaptiveConcurrencyActive()) {
                throw new AssertionError(String.format(
                        "-D%s=%s selected mode %s, and the options carry it, but the engine resolved adaptive "
                                + "concurrency INACTIVE - it refused the mode (see the WARN it logged at "
                                + "construction). This run has verified nothing about that mode.",
                        MODE_PROPERTY, requested, options.getAdaptiveConcurrencyMode()));
            }
        }
    }

    // --- the selection proof ---

    /**
     * The lane's own premise: what the selector asked for is what the engine got.
     * <p>
     * Asserted in BOTH directions, so the test says something whichever way it is run. With the property set, a
     * default-options processor must be adaptive-active in the property's mode - and the property may select at
     * most {@code OBSERVE}, so {@code ENFORCE} must never appear here however the lane was invoked. With no
     * property, the same options must come out {@code DISABLED} and inactive: the default is not merely quiet, it
     * is off.
     */
    @Test
    void theSelectedModeReachesTheEngineRatherThanBeingIgnored() {
        requireAdaptiveConcurrencyWhenModeSelected();

        String requested = System.getProperty(MODE_PROPERTY);

        var options = options().build();
        try (var probe = new TestParallelEoSStreamProcessor<>(options)) {
            if (modeSelected()) {
                assertWithMessage("the selector asked for %s, so the engine must be running it", requested)
                        .that(probe.isAdaptiveConcurrencyActive()).isTrue();
                assertWithMessage("%s may select at most OBSERVE - enforcement is builder-only", MODE_PROPERTY)
                        .that(options.getAdaptiveConcurrencyMode()).isEqualTo(AdaptiveConcurrencyMode.OBSERVE);
            } else {
                assertWithMessage("nothing was selected, so the default must be genuinely off")
                        .that(options.getAdaptiveConcurrencyMode()).isEqualTo(AdaptiveConcurrencyMode.DISABLED);
                assertThat(probe.isAdaptiveConcurrencyActive()).isFalse();
            }
        }
    }

    // --- the observe-only "could have said yes" proof ---

    /**
     * OBSERVE's entire product is a number it declines to act on. A test that only checks it acted on nothing is
     * unfalsifiable - it passes just as well against a controller that computes nothing at all, which is the
     * shape this test exists to close.
     * <p>
     * So the workload is built to MOVE the shadow computation, from a starting point where it demonstrably
     * agrees with what is published: no seed, so the would-be target starts AT the static target, and then a
     * window carrying overload drops takes the AIMD arm down by a bounded step. The would-be target must end
     * somewhere the published target is not.
     * <p>
     * The control arm is an ENFORCE controller fed the identical signals through the identical path: it publishes
     * the number OBSERVE only reported. Same signals, same arithmetic, different authority - which is the claim
     * the mode makes, stated as a difference rather than as an absence.
     * <p>
     * SABOTAGE (manual mutation, {@code docs/testing-at-write-time.md}): returning from
     * {@code AdmissionController#tick()} whenever the mode is not ENFORCE - i.e. making the shadow computation a
     * no-op - leaves the would-be target sitting at the static value and fails this test on the divergence
     * assertion, while every "OBSERVE published nothing" assertion still passes.
     */
    @Test
    void observeComputesADifferentTargetFromTheOneItPublishes() {
        requireAdaptiveConcurrencyWhenModeSelected();

        buildHarness(optionsBuilder().adaptiveConcurrencyMode(AdaptiveConcurrencyMode.OBSERVE).build());
        pc.setState(State.RUNNING);
        var observing = controller();

        assertWithMessage("fixture: unseeded OBSERVE starts with nothing to report - the shadow target IS the "
                + "published one, so any later difference was computed rather than configured")
                .that(observing.wouldBeTarget()).isEqualTo(observing.currentTarget());

        feedOverloadedWindow(observing);
        module.clock.add(WINDOW_STEP);
        pc.tickAdmissionController();

        // The independently computed expectation: one AIMD cut off the static target, ROUNDED to whole slots -
        // the law's published figure rounds to nearest (truncation withheld committed estimate; see
        // AdmissionControlLaw#publishedLimit(), the comparison/closed-loop IT fix).
        int expectedShadowTarget = (int) Math.round(CEILING_SLOTS * BOUNDED_CONTRACTION_RATIO);

        assertWithMessage("OBSERVE saw a shedding downstream and would have cut - this is the finding the mode "
                + "exists to produce")
                .that(observing.wouldBeTarget()).isEqualTo(expectedShadowTarget);
        assertWithMessage("...and it is a DIFFERENT number from the one admission is actually run at")
                .that(observing.wouldBeTarget()).isNotEqualTo(observing.currentTarget());
        assertWithMessage("...while what is published stayed exactly the static configuration - OBSERVE resizes "
                + "nothing")
                .that(observing.currentTarget()).isEqualTo(CEILING_SLOTS);
        assertThat(observing.effectiveMaximum()).isEqualTo(CEILING_SLOTS);
        assertThat(observing.lastDecisionReason()).hasValue(AdmissionDecisionReason.BACKOFF);

        // The control arm: identical signals, ENFORCE authority - the same number, published.
        tearDown();
        pc = null;
        buildHarness(optionsBuilder().adaptiveConcurrencyMode(AdaptiveConcurrencyMode.ENFORCE).build());
        pc.setState(State.RUNNING);
        var enforcing = controller();

        feedOverloadedWindow(enforcing);
        module.clock.add(WINDOW_STEP);
        pc.tickAdmissionController();

        assertWithMessage("the control arm: on the same signals ENFORCE publishes exactly what OBSERVE only "
                + "reported, so OBSERVE's number was a real decision withheld, not an artefact")
                .that(enforcing.currentTarget()).isEqualTo(expectedShadowTarget);
    }

    // --- the truth probe ---

    /**
     * The controller's account of itself, against an expectation computed from the arranged inputs - never read
     * back off the controller, which would only prove it is self-consistent.
     * <p>
     * The outcomes are arranged as a LIST and the expectation tallied from that list, so the counters are checked
     * against an independent count of the same events rather than against the constants that produced them. The
     * decision is arranged too: enough samples to clear the minimum, plus one overload drop, which is the AIMD
     * arm and nothing else - so the reason, the resulting target and the counters are all predictable before the
     * controller is asked.
     */
    @Test
    void theReportedInternalViewMatchesAnIndependentlyComputedExpectation() {
        requireAdaptiveConcurrencyWhenModeSelected();

        int seed = 20;
        buildHarness(optionsBuilder()
                .adaptiveConcurrencyMode(AdaptiveConcurrencyMode.ENFORCE)
                .adaptiveConcurrencyInitialTarget(seed)
                .build());
        pc.setState(State.RUNNING);
        var controller = controller();

        // Deliberately arranged state: nine clean completions, two business-logic failures, one overload drop.
        List<Outcome> arranged = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            arranged.add(Outcome.SUCCESS);
        }
        arranged.add(Outcome.IGNORE);
        arranged.add(Outcome.IGNORE);
        arranged.add(Outcome.OVERLOAD_DROP);

        for (Outcome outcome : arranged) {
            controller.recordServiceTime(10 * MS);
            controller.recordInFlight(seed);
            controller.recordOutcome(outcome);
        }

        module.clock.add(WINDOW_STEP);
        pc.tickAdmissionController();

        assertWithMessage("every service time fed was counted, and nothing else was")
                .that(controller.serviceTimeSamplesRecorded()).isEqualTo(arranged.size());
        assertWithMessage("...and every in-flight snapshot")
                .that(controller.inFlightSamplesRecorded()).isEqualTo(arranged.size());
        for (Outcome outcome : Outcome.values()) {
            assertWithMessage("outcome %s, counted independently of the controller", outcome)
                    .that(controller.outcomesRecorded(outcome))
                    .isEqualTo(Collections.frequency(arranged, outcome));
        }

        assertWithMessage("an overload drop in a window with enough samples is the AIMD arm, whatever else the "
                + "window carried")
                .that(controller.lastDecisionReason()).hasValue(AdmissionDecisionReason.BACKOFF);
        assertWithMessage("...and the AIMD arm's cut off the seed, rounded to whole slots")
                .that(controller.currentTarget()).isEqualTo((int) Math.round(seed * BOUNDED_CONTRACTION_RATIO));
        assertWithMessage("the target moved, so the controller must be able to say when")
                .that(controller.lastMovementAt()).isPresent();
        assertWithMessage("an explicit maxConcurrency is never substituted for, in any mode")
                .that(controller.effectiveMaximum()).isEqualTo(CEILING_SLOTS);
        assertWithMessage("and under ENFORCE the enforce ceiling IS the effective maximum")
                .that(controller.wouldBeEnforceCeiling()).isEqualTo(controller.effectiveMaximum());
    }

    // --- the rip-out triad, against the real engine ---

    /**
     * Stop condition one: a target seeded far below the ceiling must climb, and climb within a bounded number of
     * windows rather than eventually.
     * <p>
     * EXPECTATION FLIPPED by the U5 rewrite, deliberately: the old law climbed all the way to the ceiling on
     * this workload - additive headroom growth with no absolute objective, which is exactly the ratchet the
     * band machine deletes. Blind growth is the KTD2 warmup allowance and NOT ONE SLOT MORE, because this
     * drive's window cadence (2s steps against the law's short elasticity horizon) never accumulates the
     * estimator's minimum evidence, so no verdict ever licenses more.
     * <p>
     * REFINED with the stagnation probe (the 2026-08-25 comparison-IT freeze fix): the verdict-less
     * {@code WARMUP_EXHAUSTED} park is no longer an absorbing hold - after its arming streak the controller
     * takes ONE bounded accelerator-step re-measurement up, and this drive's flat throughput makes it restore.
     * The pins encode both halves: the only windows above seed-plus-allowance are that single probe excursion
     * (exactly one accelerator step, throughput-evaluated), NET growth still stops at the allowance (the
     * restore must not re-fund blind growth - that regression walked 8 -&gt; 11 -&gt; 12 here before the
     * no-refill conclusion), and the backed-off cadence keeps the tail parked.
     */
    @Test
    void theTargetGrowsFromALowSeedWithinABoundedNumberOfWindows() {
        requireAdaptiveConcurrencyWhenModeSelected();

        List<Integer> trace = driveSaturatedEnforceWindows(LONG_RUN_WINDOWS, LOW_SEED_SLOTS);

        int firstGrowth = -1;
        for (int window = 0; window < trace.size(); window++) {
            if (trace.get(window) > LOW_SEED_SLOTS) {
                firstGrowth = window + 1;
                break;
            }
        }
        int warmupParkSlots = LOW_SEED_SLOTS + WARMUP_ALLOWANCE_SLOTS;
        int probeExcursionSlots = warmupParkSlots + (int) Math.round(Math.sqrt(warmupParkSlots));
        assertWithMessage("the target never left the seed at all: trace was %s", trace)
                .that(firstGrowth).isGreaterThan(0);
        assertWithMessage("the target must leave the seed within %s windows, not eventually: trace was %s",
                GROWTH_DEADLINE_WINDOWS, trace)
                .that(firstGrowth).isAtMost(GROWTH_DEADLINE_WINDOWS);
        assertWithMessage("...and the run may exceed the warmup allowance ONLY by the stagnation probe's "
                + "single bounded accelerator step - anything higher is blind growth past the KTD2 cap: "
                + "trace was %s", trace)
                .that(Collections.max(trace)).isAtMost(probeExcursionSlots);
        assertWithMessage("...and the probe excursion must actually fire within the run - the verdict-less "
                + "park is no longer absorbing (the comparison-IT freeze): trace was %s", trace)
                .that(Collections.max(trace)).isEqualTo(probeExcursionSlots);
        assertWithMessage("...and flat throughput restores it: NET blind growth stays at exactly the warmup "
                + "allowance, with no post-restore refill: trace was %s", trace)
                .that(trace.get(trace.size() - 1)).isEqualTo(warmupParkSlots);
    }

    /**
     * Stop condition two: nothing the law can do puts the target above the ceiling the user configured. Asserted
     * over the whole trace, not just its end - a single overshoot window would dispatch more work than the pool
     * has threads for, and be gone by the time anything looked. Seeded one warmup grant below the ceiling so
     * the clamp genuinely binds (the low-seed drive can no longer reach it - see stop condition one).
     */
    @Test
    void theTargetNeverExceedsTheEffectiveMaximum() {
        requireAdaptiveConcurrencyWhenModeSelected();

        List<Integer> trace = driveSaturatedEnforceWindows(LONG_RUN_WINDOWS, NEAR_CEILING_SEED_SLOTS);

        assertWithMessage("no window may put the target above the ceiling: trace was %s", trace)
                .that(Collections.max(trace)).isAtMost(CEILING_SLOTS);
        assertThat(controller().effectiveMaximum()).isEqualTo(CEILING_SLOTS);
        assertWithMessage("the run must actually have REACHED the ceiling, or this asserts nothing: trace was %s",
                trace)
                .that(trace).contains(CEILING_SLOTS);
    }

    /**
     * Stop condition three: a brake window may back off, but never collapse. The overload window's cut is
     * checked against {@link #BOUNDED_CONTRACTION_RATIO}, so a future brake that cuts harder - or one that cuts
     * to the floor on an outlier - fails here rather than in production. (The band machine's deeper RETRACTION
     * movement lands only on a recorded previously-held baseline, and is pinned at law level.)
     */
    @Test
    void noWindowContractsTheTargetByMoreThanTheBoundedStep() {
        requireAdaptiveConcurrencyWhenModeSelected();

        List<Integer> trace = driveSaturatedEnforceWindows(LONG_RUN_WINDOWS, LOW_SEED_SLOTS);
        int settled = trace.get(trace.size() - 1);

        // A real contraction, chosen by the law itself: one overloaded window fires the BACKOFF brake.
        feedOverloadedWindow(controller());
        module.clock.add(WINDOW_STEP);
        pc.tickAdmissionController();
        int afterCut = controller().currentTarget();

        assertWithMessage("the overloaded window must actually have CONTRACTED, or this asserts nothing")
                .that(afterCut).isLessThan(settled);
        assertWithMessage("the cut %s -> %s must be the bounded AIMD step, nothing steeper", settled, afterCut)
                .that(afterCut).isAtLeast((int) Math.floor(settled * BOUNDED_CONTRACTION_RATIO));
    }

    // --- helpers ---

    private static ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> optionsBuilder() {
        return ParallelConsumerOptions.<String, String>builder()
                .consumer(new MockConsumer<>(OffsetResetStrategy.LATEST))
                .maxConcurrency(CEILING_SLOTS)
                .batchSize(1)
                // pinned: the platform-vs-virtual axis is its own execution mode, and its own lane
                .useVirtualThreads(false);
    }

    /**
     * Library-default options - deliberately NOT {@link #optionsBuilder()}, because the selection proof is about
     * what the property alone does to a configuration that says nothing about adaptive concurrency.
     */
    private static ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> options() {
        return ParallelConsumerOptions.<String, String>builder()
                .consumer(new MockConsumer<>(OffsetResetStrategy.LATEST));
    }

    /**
     * A real processor, the real {@link WorkManager} from the SAME module, and the module's clock mutable - the
     * {@link AdmissionLifecycleTest} harness.
     */
    private void buildHarness(ParallelConsumerOptions<String, String> options) {
        module = new ClockedModule(options);
        pc = new TestParallelEoSStreamProcessor<>(options, module);
        wm = module.workManager();
        wm.onPartitionsAssigned(UniLists.of(tp));
        pc.setWm(wm);
    }

    private AdmissionController controller() {
        return module.admissionController();
    }

    /**
     * Drives {@code windows} closed windows of a SATURATED, healthy, admission-bound workload against a real
     * engine under ENFORCE, and returns the published target after each one.
     * <p>
     * Admission-bound means there is always more work registered than the target allows: every pass tops dispatch
     * up to the live target and nothing is ever completed back (the mailbox is never drained, because no control
     * loop is running), so the engine's own in-flight reading sits exactly at the target. Service times are flat
     * and fed directly; see the class javadoc for why that half is not driven through the tap.
     * <p>
     * One fixture assist the U5 binding gate needs: the instant-return user function means the POOL's tasks
     * finish long before the window boundary, so the boundary sample would read zero OCCUPIED slots and classify
     * every window app-limited (which the band machine, correctly, never grows on). The task accounting is
     * therefore topped up to the live target before each boundary - including one task held in the
     * submit-to-start handoff, the real post-dispatch shape (see {@link #markPoolSaturatedAtTheLiveTarget()}) -
     * so the boundary reads what a genuinely saturated pool would report, while dispatch, the pool and the
     * in-flight sampling stay fully real.
     */
    private List<Integer> driveSaturatedEnforceWindows(int windows, int seedSlots) {
        buildHarness(optionsBuilder()
                .adaptiveConcurrencyMode(AdaptiveConcurrencyMode.ENFORCE)
                .adaptiveConcurrencyInitialTarget(seedSlots)
                .build());
        // more than the ceiling can ever hold out for processing, so the workload is never the constraint
        registerWork(CEILING_SLOTS * 2);
        pc.setState(State.RUNNING);

        assertWithMessage("fixture: ENFORCE must start at the seed, or growth has nowhere to come from")
                .that(controller().currentTarget()).isEqualTo(seedSlots);

        List<Integer> trace = new ArrayList<>(windows);
        for (int window = 0; window < windows; window++) {
            pc.retrieveAndDistributeNewWork(userFunction, callback);
            for (int sample = 0; sample < SAMPLES; sample++) {
                controller().recordServiceTime(10 * MS);
                pc.sampleAdmissionInFlight(); // the in-flight signal, read off real engine state
                controller().recordOutcome(Outcome.SUCCESS);
            }
            markPoolSaturatedAtTheLiveTarget();
            module.clock.add(WINDOW_STEP);
            pc.tickAdmissionController();
            trace.add(controller().currentTarget());
        }
        return trace;
    }

    /**
     * How many phantom tasks {@link #markPoolSaturatedAtTheLiveTarget()} has arranged so far. Tracked test-side
     * rather than read back off the accounting: the live {@code getOccupied()} also counts REAL dispatched
     * tasks mid-flight, whose instant-return completions race the drive's loop - a deficit computed from it
     * under-tops-up nondeterministically and window binding starts depending on scheduler timing (observed as
     * a same-suite run ending mid-stagnation-probe while a solo run concluded it). Phantoms never finish, so
     * this count is exact and the drive stays deterministic.
     */
    private int phantomOccupiedTasks = 0;

    /**
     * The binding-gate fixture assist described on {@link #driveSaturatedEnforceWindows(int, int)} - topped up
     * to the live target by OCCUPANCY (dispatched-and-unfinished), with exactly one task deliberately left in
     * the submit-to-start handoff (submitted, never started). That is what the real post-dispatch instant looks
     * like on a saturated engine, and it is the arrangement that catches the tap defect the 2026-08-25
     * comparison IT hit: a sampler reading {@code getActive()} instead of {@code getOccupied()} sees
     * target-minus-one here, every window reads unbound, and the growth these tests assert never happens.
     */
    private void markPoolSaturatedAtTheLiveTarget() {
        var accounting = pc.userFunctionTaskAccounting();
        int deficit = controller().currentTarget() - phantomOccupiedTasks;
        for (int task = 0; task < deficit; task++) {
            accounting.onSubmitting();
            if (phantomOccupiedTasks > 0) {
                accounting.onTaskStarted();
            }
            phantomOccupiedTasks++;
        }
    }

    /**
     * One window's worth of a shedding downstream: enough samples to clear the law's minimum, flat latency, and a
     * single overload drop - the AIMD arm's trigger, and the cheapest signal that moves a target which is already
     * at its ceiling and so cannot grow.
     */
    private void feedOverloadedWindow(AdmissionController controller) {
        int inFlight = controller.currentTarget();
        for (int i = 0; i < SAMPLES; i++) {
            controller.recordServiceTime(10 * MS);
            controller.recordInFlight(inFlight);
            controller.recordOutcome(i == 0 ? Outcome.OVERLOAD_DROP : Outcome.SUCCESS);
        }
    }

    /**
     * Registers {@code count} records, left sitting in the shards - selectable but not taken (the
     * {@link AdmissionLifecycleTest} fixture).
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
