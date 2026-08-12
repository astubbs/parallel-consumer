package bz.stub.parallelconsumer.dashboard.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.integrationTests.chaostests.scenario.MembershipAction;
import bz.stub.parallelconsumer.integrationTests.chaostests.scenario.Scenario;
import bz.stub.parallelconsumer.integrationTests.chaostests.scenario.ScenarioAction;
import bz.stub.parallelconsumer.integrationTests.chaostests.scenario.ScenarioContext;
import bz.stub.parallelconsumer.integrationTests.chaostests.scenario.ScenarioPhase;
import bz.stub.parallelconsumer.integrationTests.chaostests.scenario.WorkloadActions;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * The scenario the dashboard exists to be watched during: an ordered walk through every condition a panel
 * draws, arranged so the interesting one arrives while someone is still looking.
 * <p>
 * Declared entirely OUTSIDE the scenario framework's package, and outside its module - it is a consumer of the
 * framework exactly as the Chaos Pain Suite is (R30). Nothing here needed a framework change.
 *
 * <h2>Scripted structure, seeded detail</h2>
 * The phase list below is the script: it always runs in this order, so the demo always reaches head-of-line
 * blocking and always reaches a rebalance. What varies with the seed is the detail inside a phase - which
 * weighted action a tick draws and how long the ticks are. Same scenario plus same seed reproduces a run
 * exactly; a different seed gives the same seven phases with different specifics (R31).
 * <p>
 * The condition each phase is <em>named</em> for is applied in its {@code onEnter}, not left to a draw. A
 * postcondition that only holds when the RNG cooperates is a flaky test dressed up as a demo; the draws vary
 * the texture on top of a guaranteed floor.
 * <p>
 * <b>Every phase must also work on the SECOND loop.</b> In {@code LOOP} mode the list runs again against a
 * fleet the previous pass already changed, so a phase whose action is a silent no-op the second time round
 * turns a long-running demo into a still picture. See the membership phases below for the case that bit.
 *
 * <h2>Postconditions are the point</h2>
 * Every phase declares what it intends to produce and is checked against the very state document the page
 * renders (via {@link DashboardWatcher}). A phase that quietly stopped demonstrating its thing is a run
 * failure, not a non-event (R33) - which is what makes {@code --once} a test.
 */
public final class ShowcaseScenario {

    /**
     * The key pinned to fail. One key, failing on every delivery, is the head-of-line-blocking lever: its
     * shard stalls behind retry backoff while the rest of the partition completes past it, leaving a band of
     * finished-but-uncommittable work - the offset ribbon's entire reason to exist.
     */
    public static final String FAILING_KEY = "key-7";

    /** Where the ramp starts, and the ceiling it ramps to. Rates are per second, fleet-wide. */
    public static final int STARTING_RATE = 60;
    public static final int PEAK_RATE = 600;

    /**
     * How much finished work must strand above the commit point before the phase counts as having produced
     * head-of-line blocking. Deliberately low relative to what the phase actually produces (hundreds of
     * offsets at {@link #PEAK_RATE}): the postcondition exists to catch the phase doing NOTHING, not to
     * re-measure the throughput.
     */
    public static final int MIN_STRANDED_BAND = 10;

    /** Distinct key shards the spread phase must populate - low for the same reason as the band floor. */
    public static final int MIN_SHARDS = 5;

    /** In-flight records the slow-function phase must reach. Well under the demo's max concurrency. */
    public static final int MIN_INFLIGHT_UNDER_SLOW_FUNCTION = 5;

    /** Dwell injected into the user function to back the worker pool up. */
    public static final Duration SLOW_FUNCTION_DELAY = Duration.ofMillis(150);

    /**
     * What the user function dwells per record when a phase is not deliberately slowing it - and it is NOT
     * zero on purpose.
     * <p>
     * A user function that returns instantly makes every occupancy panel read empty: PC drains each poll batch
     * in microseconds, so a shard exists for less time than the sampling interval and the shard gauge reads
     * zero however much work is flowing. Measured: with no dwell, the spread phase saw 0 shards for its whole
     * 10 seconds while 48,000 records went through, and only passed on runs that happened to sample mid-batch.
     * A small dwell makes steady-state occupancy {@code rate x dwell} - at {@link #PEAK_RATE} that is ~15
     * shards - which is both what a real workload looks like and what makes the panels legible.
     */
    public static final Duration BASELINE_FUNCTION_DELAY = Duration.ofMillis(25);

    private ShowcaseScenario() {
    }

    /**
     * @param watcher reads the same state document the page draws from; the postconditions below are asked of
     *                it, so an assertion here is an assertion about what a viewer would have seen
     */
    public static Scenario declare(DashboardWatcher watcher) {
        if (watcher == null) throw new IllegalArgumentException("the showcase scenario asserts against a watcher");
        return Scenario.of("dashboard showcase",

                phase("ramp the throughput up from a trickle, so the rate charts climb",
                        Duration.ofSeconds(12), watcher,
                        context -> context.workload().setPublishRatePerSecond(STARTING_RATE),
                        weights(WorkloadActions.scalePublishRate(1.8, STARTING_RATE, PEAK_RATE), 3,
                                WorkloadActions.publishAt(PEAK_RATE), 1),
                        // phase-scoped, deliberately: getPeakProcessedRecords() is a monotonic RUN total, so in LOOP
                        // mode "> 0" would be true from pass two onward even with the producer dead
                        () -> watcher.getPhaseProcessedRecords() > 0
                                ? null
                                : "no records were processed during this phase, so nothing could have moved on the "
                                + "page while the rate was ramping"),

                phase("hold at full rate across many keys, so the shard view fills up",
                        Duration.ofSeconds(12), watcher,
                        context -> context.workload().setPublishRatePerSecond(PEAK_RATE),
                        weights(WorkloadActions.publishAt(PEAK_RATE), 1),
                        () -> watcher.getPhasePeakShards() >= MIN_SHARDS
                                ? null
                                : "only " + watcher.getPhasePeakShards() + " shard(s) held work at once during this "
                                + "phase, fewer than the " + MIN_SHARDS + " it spreads work across - at "
                                + PEAK_RATE + "/s with a " + BASELINE_FUNCTION_DELAY + " dwell there should be "
                                + "roughly " + (PEAK_RATE * BASELINE_FUNCTION_DELAY.toMillis() / 1000) + " of them"),

                phase("strand completed work behind one repeatedly failing key - head-of-line blocking",
                        Duration.ofSeconds(25), watcher,
                        context -> {
                            // forget the previous LOOP pass's band, so this one has to strand work of its own
                            // rather than passing on the last pass's evidence
                            watcher.resetStrandedBandCapture();
                            context.workload().setFailingKey(FAILING_KEY);
                            context.workload().setFailureProportion(0.02);
                        },
                        weights(WorkloadActions.failKeyRepeatedly(FAILING_KEY), 3,
                                WorkloadActions.failProportion(0.02), 1),
                        () -> {
                            DashboardWatcher.StrandedBand band = watcher.getWidestStrandedBand();
                            if (band == null) {
                                return "no partition ever held completed work above an incomplete offset, so the "
                                        + "offset ribbon had nothing to draw - the failing key '" + FAILING_KEY
                                        + "' never blocked anything";
                            }
                            return band.getWidth() >= MIN_STRANDED_BAND
                                    ? null
                                    : "the widest stranded band was only " + band.getWidth() + " offsets ("
                                    + band + "), under the " + MIN_STRANDED_BAND + " this phase must produce";
                        }),

                phase("clear the failures and let the retries drain, so the committed offset lurches forward",
                        Duration.ofSeconds(25), watcher,
                        context -> {
                            // pin the band this phase has to clear BEFORE unblocking anything: after the
                            // failures are cleared a busy partition still grows transient bands from records
                            // in flight, and letting one of those replace the target would move the goalposts
                            watcher.freezeStrandedBandCapture();
                            context.workload().clearFailingKey();
                            context.workload().setFailureProportion(0);
                        },
                        weights(WorkloadActions.clearInducedFailures(), 1),
                        () -> {
                            DashboardWatcher.StrandedBand band = watcher.getWidestStrandedBand();
                            if (band == null) {
                                return "there was no stranded band to recover from - the previous phase did not "
                                        + "produce one, so this phase proves nothing";
                            }
                            return watcher.hasCommittedPastStrandedBand()
                                    ? null
                                    : "the commit never cleared the top of the band at offset " + band.getTopOffset()
                                    + " on " + band.getPartitionKey() + " (" + band + ", highest committed there: "
                                    + watcher.getHighestCommittedOnStrandedPartition() + ") - the retries did not "
                                    + "drain within the phase";
                        }),

                phase("slow the user function until the worker pool backs up, so in-flight climbs",
                        Duration.ofSeconds(18), watcher,
                        context -> context.workload().setFunctionDelay(SLOW_FUNCTION_DELAY),
                        weights(WorkloadActions.functionDelay(SLOW_FUNCTION_DELAY), 3,
                                WorkloadActions.functionDelay(SLOW_FUNCTION_DELAY.multipliedBy(2)), 1),
                        () -> watcher.getPhasePeakInflight() >= MIN_INFLIGHT_UNDER_SLOW_FUNCTION
                                ? null
                                : "in-flight records peaked at only " + watcher.getPhasePeakInflight()
                                + " while the user function was dwelling " + SLOW_FUNCTION_DELAY
                                + " per record - the worker pool never backed up"),

                phase("bring another instance into the group, forcing a rebalance and a new assignment epoch",
                        Duration.ofSeconds(25), watcher,
                        context -> {
                            // BOTH, and in this order, because the driver caps the fleet and a stopped member
                            // stays in it: on the first pass nothing is stopped so joinNew is what fires, and
                            // on every LOOP pass after it the fleet is already at its ceiling and joinNew is a
                            // silent no-op - restart is what brings the member the previous pass killed back.
                            // Measured: with joinNew alone, this phase and the next failed on every loop after
                            // the first, and a loop-mode demo silently stopped rebalancing at all.
                            context.fleet().restart(0);
                            context.fleet().joinNew();
                        },
                        weights(MembershipAction.JOIN_NEW, 1, MembershipAction.RESTART, 1),
                        () -> watcher.getPhaseAssignmentEpochChanges() > 0
                                ? null
                                : "no partition changed assignment epoch while an instance was joining - the "
                                + "group did not rebalance, so the epoch panel had nothing to show"),

                phase("stop an instance WITHOUT draining, abandoning its in-flight work",
                        Duration.ofSeconds(25), watcher,
                        context -> {
                            context.workload().setPublishRatePerSecond(PEAK_RATE);
                            context.fleet().stopNoDrain(0);
                        },
                        weights(MembershipAction.STOP_NO_DRAIN, 1),
                        () -> watcher.getPhaseAssignmentEpochChanges() > 0 || watcher.phasePartitionCountGrew()
                                ? null
                                : "the surviving instance neither saw an epoch change nor picked up partitions "
                                + "after a member was killed - the group never noticed it had gone"));
    }

    /**
     * One phase, wired so its description reaches the runner's log, the watcher's phase scope is rebased on
     * entry, and its postcondition reads as a single sentence explaining what did not happen.
     *
     * @param problem returns null when the phase produced what it declared, or the reason it did not
     */
    private static ScenarioPhase phase(String description,
                                       Duration duration,
                                       DashboardWatcher watcher,
                                       Consumer<ScenarioContext> setUp,
                                       Map<ScenarioAction, Integer> weights,
                                       Problem problem) {
        return ScenarioPhase.builder()
                .description(description)
                .duration(duration)
                .minTick(Duration.ofSeconds(1))
                .maxTick(Duration.ofSeconds(3))
                .weights(weights)
                .onEnter(context -> {
                    // rebase FIRST: a phase-scoped observation must not include anything from the phase before
                    watcher.beginPhase(description);
                    // then restore the baseline dwell, so a phase that slowed the function cannot leak that
                    // into the next one - or into the next LOOP pass. A phase wanting something else sets it
                    // in its own set-up, which runs after this and therefore wins.
                    context.workload().setFunctionDelay(BASELINE_FUNCTION_DELAY);
                    setUp.accept(context);
                })
                .postcondition(context -> {
                    String reason = problem.describe();
                    return reason == null ? Collections.<String>emptyList() : Collections.singletonList(reason);
                })
                .build();
    }

    /** Null means "the phase produced what it declared"; anything else is the reason it did not. */
    @FunctionalInterface
    private interface Problem {
        String describe();
    }

    /**
     * A {@link LinkedHashMap}, never a {@link java.util.HashMap}: the weighted pick walks the map in iteration
     * order to turn one draw into an action, so an identity-hash order would make the seed unreplayable.
     */
    private static Map<ScenarioAction, Integer> weights(ScenarioAction a, int weightA) {
        Map<ScenarioAction, Integer> weights = new LinkedHashMap<>();
        weights.put(a, weightA);
        return weights;
    }

    private static Map<ScenarioAction, Integer> weights(ScenarioAction a, int weightA, ScenarioAction b, int weightB) {
        Map<ScenarioAction, Integer> weights = weights(a, weightA);
        weights.put(b, weightB);
        return weights;
    }

    /** The phase descriptions, in order - what a watcher is told they are being shown. */
    public static List<String> phaseDescriptions(Scenario scenario) {
        List<String> descriptions = new ArrayList<>();
        for (ScenarioPhase phase : scenario.getPhases()) {
            descriptions.add(phase.getDescription());
        }
        return descriptions;
    }
}
