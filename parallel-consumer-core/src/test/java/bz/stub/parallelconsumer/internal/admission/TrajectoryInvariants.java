package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.admission.ScenarioRunner.Trajectory;
import bz.stub.parallelconsumer.internal.admission.ScenarioRunner.WindowRecord;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The trajectory invariant kit (soak/torture plan U2): assertions over a {@link Trajectory} whose bounds are
 * DERIVED - from the plant's construction, the law's own step arithmetic, or Little's-Law arithmetic the
 * caller performs - never fitted to a run. The forms follow {@code AdaptiveConcurrencyClosedLoopIT}'s settled
 * precedent: run-length-independent (a final-third range, a rolling maximum, a derived fraction), so
 * lengthening a scenario cannot rot an assertion.
 * <p>
 * <b>Scope per the plan's open question:</b> the settle/liveness invariants are defined for STATIC schedules
 * (capacity held). For dynamic schedules only the safety invariants apply
 * ({@link #assertCeilingRespected}, {@link #assertFloorRespected}) plus whatever bound the scenario derives
 * itself - what a construction-derived invariant IS for a moving schedule is recorded as open in the plan's
 * Deferred section, and this kit deliberately does not invent one.
 * <p>
 * <b>Probe excursions are designed behaviour, not ratchet</b> (the review's residual risk, answered here):
 * the stagnation and recovery probes step UP by one accelerator step by design, so the no-ratchet bound
 * allows exactly one {@link AdmissionControlLaw#acceleratorStep} above the settle-time maximum, and the
 * amplitude bound is the closed-loop IT's two accelerator steps. A law change that widens probe excursions
 * beyond one step SHOULD go red here - that is the invariant doing its job, not flakiness.
 */
final class TrajectoryInvariants {

    private TrajectoryInvariants() {
    }

    // ------------------------------------------------------------------
    // Safety - applies to EVERY schedule, static or dynamic.
    // ------------------------------------------------------------------

    /** The commanded target never exceeds the resolved ceiling, on any window of the run. */
    static void assertCeilingRespected(Trajectory trajectory, int ceilingSlots) {
        for (WindowRecord record : trajectory.getRecords()) {
            assertWithMessage("window %s commanded %s slots above the ceiling of %s",
                    record.getWindowIndex(), record.getCommandedTarget(), ceilingSlots)
                    .that(record.getCommandedTarget()).isAtMost(ceilingSlots);
        }
    }

    /** The commanded target never leaves the law's floor, on any window of the run. */
    static void assertFloorRespected(Trajectory trajectory) {
        for (WindowRecord record : trajectory.getRecords()) {
            assertWithMessage("window %s commanded %s slots below the floor",
                    record.getWindowIndex(), record.getCommandedTarget())
                    .that(record.getCommandedTarget()).isAtLeast(AdmissionControlLaw.LIMIT_FLOOR_SLOTS);
        }
    }

    /**
     * The no-ratchet invariant, extended over arbitrary horizon (STATIC schedules): after
     * {@code settleWindow}, the commanded target never exceeds the maximum it had already reached by then
     * plus ONE accelerator step (the designed up-probe excursion). This is the falsifier suite's core claim
     * with wall-clock-scale reach: a target that keeps finding new highs on an unchanging plant is the
     * ratchet, however slowly it climbs.
     */
    static void assertNoRatchetAfterSettle(Trajectory trajectory, int settleWindow) {
        int settleMax = 0;
        for (WindowRecord record : trajectory.getRecords()) {
            if (record.getWindowIndex() < settleWindow) {
                settleMax = Math.max(settleMax, record.getCommandedTarget());
            }
        }
        int allowed = settleMax + (int) Math.ceil(AdmissionControlLaw.acceleratorStep(settleMax));
        for (WindowRecord record : trajectory.getRecords()) {
            if (record.getWindowIndex() >= settleWindow) {
                assertWithMessage("window %s commanded %s slots - above the settle-time maximum of %s plus "
                                + "one probe excursion (%s): the target is still finding new highs on an "
                                + "unchanging plant, which is the ratchet",
                        record.getWindowIndex(), record.getCommandedTarget(), settleMax, allowed)
                        .that(record.getCommandedTarget()).isAtMost(allowed);
            }
        }
    }

    /**
     * The settled-band invariant in the closed-loop IT's derived form (STATIC schedules): over the final third
     * of the run, the commanded target's range is at most two accelerator steps at the knee, and its median
     * sits at or below {@code kneeSlots} plus one step. Run-length-independent by construction.
     */
    static void assertSettledBand(Trajectory trajectory, double kneeSlots) {
        int from = trajectory.getRecords().size() * 2 / 3;
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        java.util.List<Integer> tail = new java.util.ArrayList<>();
        for (WindowRecord record : trajectory.getRecords()) {
            if (record.getWindowIndex() >= from) {
                min = Math.min(min, record.getCommandedTarget());
                max = Math.max(max, record.getCommandedTarget());
                tail.add(record.getCommandedTarget());
            }
        }
        java.util.Collections.sort(tail);
        int median = tail.get(tail.size() / 2);
        double step = AdmissionControlLaw.acceleratorStep(kneeSlots);
        // The settled walk's two descent steps are taken at the BAND TOP (knee plus one step), not at the
        // knee, and acceleratorStep grows with the level - a second-order term that is sub-slot at the
        // closed-loop IT's knee of 12 and first showed at the matrix's WIDE scenario (knee 200: observed
        // steady-state cycle [196..225] = sqrt(225) + sqrt(210) = 29.5, against 2*sqrt(200) = 28.3). The
        // bound therefore evaluates the step where the walk actually takes it: still derived from the law's
        // own arithmetic, never from an observed run.
        double bandTopStep = AdmissionControlLaw.acceleratorStep(kneeSlots + step);
        assertWithMessage("final-third target range [%s..%s] wider than two accelerator steps (%s) taken at "
                + "the band top for the knee of %s - the trajectory has not settled", min, max,
                2 * bandTopStep, kneeSlots)
                .that((double) (max - min)).isAtMost(2 * bandTopStep);
        assertWithMessage("final-third median target %s sits above the knee %s plus one step %s - parked on "
                + "the wrong side of the knee", median, kneeSlots, step)
                .that((double) median).isAtMost(kneeSlots + step);
    }

    // ------------------------------------------------------------------
    // Liveness - bounds the CALLER derives from plant construction.
    // ------------------------------------------------------------------

    /**
     * Settled mean throughput over the final third is at least {@code derivedFloorPerSecond} - a value the
     * scenario derives from plant construction (a fraction of {@code min(arrival, mu_max)}), never from a
     * previous run.
     */
    static void assertSettledThroughputAtLeast(Trajectory trajectory, double derivedFloorPerSecond) {
        int tailWindows = Math.max(1, trajectory.getRecords().size() / 3);
        double settled = trajectory.settledMeanThroughput(tailWindows);
        assertWithMessage("settled mean throughput %s/s below the construction-derived floor %s/s",
                String.format(Locale.ROOT, "%.1f", settled),
                String.format(Locale.ROOT, "%.1f", derivedFloorPerSecond))
                .that(settled).isAtLeast(derivedFloorPerSecond);
    }

    // ------------------------------------------------------------------
    // Reporting - the artifact the soak plan's ledger convention carries as evidence.
    // ------------------------------------------------------------------

    /**
     * Writes the trajectory as CSV under {@code target/trajectories/} and returns the path. The artifact a
     * red scenario attaches to its inflight note, and the raw material for the drift report.
     */
    static Path writeCsv(String name, Trajectory trajectory) {
        Path dir = Paths.get("target", "trajectories");
        try {
            Files.createDirectories(dir);
            Path file = dir.resolve(name + ".csv");
            StringBuilder csv = new StringBuilder("window,commandedTarget,throughputPerSecond,"
                    + "meanServiceTimeNanos,limitBound\n");
            for (WindowRecord record : trajectory.getRecords()) {
                csv.append(record.getWindowIndex()).append(',')
                        .append(record.getCommandedTarget()).append(',')
                        .append(String.format(Locale.ROOT, "%.3f", record.getThroughputPerSecond())).append(',')
                        .append(String.format(Locale.ROOT, "%.0f", record.getMeanServiceTimeNanos())).append(',')
                        .append(record.isLimitBound()).append('\n');
            }
            Files.write(file, csv.toString().getBytes(StandardCharsets.UTF_8));
            return file;
        } catch (IOException e) {
            throw new UncheckedIOException("could not write trajectory CSV " + name, e);
        }
    }

    /** One verdict line per scenario - what the matrix runner emits. */
    static String summarize(String scenarioName, Trajectory trajectory) {
        int windows = trajectory.getRecords().size();
        int tail = Math.max(1, windows / 3);
        return String.format(Locale.ROOT,
                "%s: %d windows, final target %d, max target %d, settled throughput %.1f/s",
                scenarioName, windows, trajectory.getFinalTarget(), trajectory.maxCommandedTarget(),
                trajectory.settledMeanThroughput(tail));
    }
}
