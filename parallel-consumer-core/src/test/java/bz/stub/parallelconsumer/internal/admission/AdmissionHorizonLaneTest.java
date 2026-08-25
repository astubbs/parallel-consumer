package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.admission.ScenarioRunner.Trajectory;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import static bz.stub.parallelconsumer.internal.admission.ScenarioMatrix.CEILING;
import static bz.stub.parallelconsumer.internal.admission.ScenarioMatrix.KNEE;
import static bz.stub.parallelconsumer.internal.admission.ScenarioMatrix.MU;
import static bz.stub.parallelconsumer.internal.admission.ScenarioMatrix.SATURATING_ARRIVAL;
import static bz.stub.parallelconsumer.internal.admission.ScenarioMatrix.W0;

/**
 * The simulated-horizon lane (soak/torture plan U7): hour-scale WINDOW counts against the deterministic
 * plant, per-PR, in seconds - because long-horizon and broker-attached are different axes, and every
 * trajectory invariant that needs HORIZON (a ratchet that only shows over hours, drift tracking, resonance
 * endurance) runs here deterministically. Twelve simulated hours is {@value #TWELVE_HOURS_OF_WINDOWS}
 * one-second windows; the real-broker soak lane (the plan's U5) carries only what genuinely needs wall
 * clock - heap and thread trends, real engine events, real window density.
 * <p>
 * The no-ratchet claims here are the falsifier suite's core claims extended three orders of magnitude in
 * horizon: a target that finds a new high in hour eleven of an unchanging plant is a ratchet the 200-window
 * falsifiers structurally cannot see.
 */
@Slf4j
class AdmissionHorizonLaneTest {

    static final int TWELVE_HOURS_OF_WINDOWS = 12 * 3600;

    /** Settle horizon granted before the long-haul invariants bind: generous, derived-order (many settle
     * cadences), and three orders of magnitude short of the run length, so it cannot mask a slow ratchet. */
    static final int SETTLE_HORIZON_WINDOWS = 600;

    @Test
    void twelveSimulatedHoursOnAStaticPlantDoNotRatchetTheLaw() {
        Trajectory trajectory = ScenarioRunner.run(
                new LawAdmissionPolicy(2, CEILING),
                new DeterministicPlant(MU, W0, 1), 2,
                CapacitySchedules.constant(TWELVE_HOURS_OF_WINDOWS, SATURATING_ARRIVAL));
        TrajectoryInvariants.assertCeilingRespected(trajectory, CEILING);
        TrajectoryInvariants.assertFloorRespected(trajectory);
        TrajectoryInvariants.assertNoRatchetAfterSettle(trajectory, SETTLE_HORIZON_WINDOWS);
        TrajectoryInvariants.assertSettledBand(trajectory, KNEE);
        TrajectoryInvariants.writeCsv("horizon-12h-static-law", trajectory);
        log.info("{}", TrajectoryInvariants.summarize("horizon-12h-static-law", trajectory));
    }

    /**
     * The same twelve hours through the REAL controller - probe machinery, cadence backoff and all. Probes
     * keep firing forever at their capped cadences, so this is the arm that would catch a probe ladder whose
     * excursions compound over hours rather than restoring.
     */
    @Test
    void twelveSimulatedHoursOnAStaticPlantDoNotRatchetTheController() {
        Trajectory trajectory = ScenarioRunner.run(
                new ControllerAdmissionPolicy(2, 2),
                new DeterministicPlant(MU, W0, 1), 2,
                CapacitySchedules.constant(TWELVE_HOURS_OF_WINDOWS, SATURATING_ARRIVAL));
        TrajectoryInvariants.assertCeilingRespected(trajectory, CEILING);
        TrajectoryInvariants.assertFloorRespected(trajectory);
        TrajectoryInvariants.assertNoRatchetAfterSettle(trajectory, SETTLE_HORIZON_WINDOWS);
        TrajectoryInvariants.assertSettledBand(trajectory, KNEE);
        TrajectoryInvariants.writeCsv("horizon-12h-static-controller", trajectory);
        log.info("{}", TrajectoryInvariants.summarize("horizon-12h-static-controller", trajectory));
    }

    /**
     * Hours-long slow drift - the baseline-contamination shape that convicted the old law, at soak scale:
     * capacity creeps from half to full over four simulated hours (a knee moving ~0.007 slots per window,
     * far below any single step), then holds for two. The settled band is asserted against the FINAL knee;
     * the drift phase itself gets safety plus the ceiling of sanity: the target must never exceed what the
     * FINAL knee's band allows, because capacity only ever grew toward it.
     */
    @Test
    void fourSimulatedHoursOfCapacityDriftAreTrackedNotRatcheted() {
        int driftWindows = 4 * 3600;
        Trajectory trajectory = ScenarioRunner.run(
                new LawAdmissionPolicy(2, CEILING),
                new DeterministicPlant(MU / 2, W0, 1), 2,
                CapacitySchedules.concat(
                        CapacitySchedules.drift(driftWindows, SATURATING_ARRIVAL, MU / 2, MU),
                        CapacitySchedules.constant(2 * 3600, SATURATING_ARRIVAL)));
        TrajectoryInvariants.assertCeilingRespected(trajectory, CEILING);
        TrajectoryInvariants.assertFloorRespected(trajectory);
        TrajectoryInvariants.assertSettledBand(trajectory, KNEE);
        TrajectoryInvariants.writeCsv("horizon-4h-drift-law", trajectory);
        log.info("{}", TrajectoryInvariants.summarize("horizon-4h-drift-law", trajectory));
    }

    /**
     * Resonance endurance: a square-wave capacity oscillation near the law's own cadence, sustained for two
     * simulated hours (hundreds of cycles). Dynamic schedule, so safety-only invariants per the plan's open
     * question - plus the one bound that IS derivable: the commanded target must never exceed the HIGH
     * capacity's knee band, because no phase of the plant ever supports more.
     */
    @Test
    void twoSimulatedHoursOfCadenceResonantOscillationStaySafe() {
        int halfPeriod = 8; // near the settle cadence - the adversarial phase relationship
        int cycles = 2 * 3600 / (2 * halfPeriod);
        Trajectory trajectory = ScenarioRunner.run(
                new ControllerAdmissionPolicy(KNEE, 2),
                new DeterministicPlant(MU, W0, 1), KNEE,
                CapacitySchedules.oscillation(cycles, halfPeriod, SATURATING_ARRIVAL, MU, MU / 2));
        TrajectoryInvariants.assertCeilingRespected(trajectory, CEILING);
        TrajectoryInvariants.assertFloorRespected(trajectory);
        double highKnee = KNEE;
        double bandTopAllowance = highKnee
                + Math.ceil(AdmissionControlLaw.acceleratorStep(highKnee))
                + Math.ceil(AdmissionControlLaw.acceleratorStep(
                highKnee + AdmissionControlLaw.acceleratorStep(highKnee)));
        for (ScenarioRunner.WindowRecord record : trajectory.getRecords()) {
            if (record.getWindowIndex() >= SETTLE_HORIZON_WINDOWS
                    && record.getCommandedTarget() > bandTopAllowance) {
                throw new AssertionError(String.format(
                        "window %d commanded %d slots - above the high-capacity knee band (%f) on a plant no "
                                + "phase of which supports more: resonance is pumping the target",
                        record.getWindowIndex(), record.getCommandedTarget(), bandTopAllowance));
            }
        }
        TrajectoryInvariants.writeCsv("horizon-2h-resonance-controller", trajectory);
        log.info("{}", TrajectoryInvariants.summarize("horizon-2h-resonance-controller", trajectory));
    }
}
