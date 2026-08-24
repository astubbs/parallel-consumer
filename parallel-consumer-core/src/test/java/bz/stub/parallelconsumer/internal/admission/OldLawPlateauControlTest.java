package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.admission.ScenarioRunner.Phase;
import bz.stub.parallelconsumer.internal.admission.ScenarioRunner.Trajectory;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static bz.stub.parallelconsumer.internal.admission.FalsifierScenarios.CEILING_SLOTS;
import static bz.stub.parallelconsumer.internal.admission.FalsifierScenarios.MU_MAX_RECORDS_PER_SECOND;
import static bz.stub.parallelconsumer.internal.admission.FalsifierScenarios.SWEEP_WINDOWS;
import static bz.stub.parallelconsumer.internal.admission.FalsifierScenarios.standardPlant;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * THE CONTROL RUN of the falsifier suite (the plan's U8 execution note): the CURRENT
 * {@link AdmissionControlLaw} driven through the graceful-saturation plateau, green today by asserting the
 * DEFECT - the ratchet HAPPENS. The target walks upward while throughput stays flat at {@code mu_max},
 * because the relative objective's long-latency baseline slowly absorbs the degraded service time and hands
 * the gradient its headroom back, window after window (the design's Finding 1: a ratio cannot detect a
 * steadily-bad absolute level).
 * <p>
 * This test is the ablation control the U5 rewrite flips: U5 DELETES this class and the plateau scenario
 * joins the real (must-not-walk) suite. If this test ever fails before U5, the harness or the law changed
 * shape - investigate, do not loosen.
 * <p>
 * <b>Observed walk</b> (recorded per the U8 packet; the plant is deterministic, so exact): starting ON the
 * knee at target 20, the target walked <b>20 -&gt; 60 over 250 windows</b> while settled throughput stayed at
 * exactly 400.0 records/s ({@code mu_max}) - a 3x over-commitment that bought zero throughput. The sweep
 * observation (recorded, not required): the old law FAILED all six arms {1, 2, 5, 20, 50, 100}, every one on
 * the HIGH side (targets 43/43/44/51/100/100 at the window-160 deadline against a band ceiling of 27) - the
 * plan allowed that the additive headroom might legitimately carry the sweep; observed, the same ratchet
 * overruns it instead.
 */
@Slf4j
class OldLawPlateauControlTest {

    private static final int KNEE_SLOTS = 20; // = standardPlant(1).optimalTargetSlots()
    private static final int PLATEAU_WINDOWS = 250;

    private static OldLawAdmissionPolicy oldLaw(int initialTarget) {
        return new OldLawAdmissionPolicy(initialTarget, CEILING_SLOTS);
    }

    /**
     * The defect, asserted: starting ON the knee under saturating arrival, the old law's target walks
     * materially above the knee while settled throughput never moves off {@code mu_max}.
     */
    @Test
    void oldLawRatchetsUpThePlateau() {
        DeterministicPlant plant = standardPlant(1);
        Trajectory trajectory = ScenarioRunner.run(oldLaw(KNEE_SLOTS), plant, KNEE_SLOTS,
                Arrays.asList(Phase.of(PLATEAU_WINDOWS, 1.5 * MU_MAX_RECORDS_PER_SECOND)));

        int startTarget = trajectory.commandedTargetAt(0);
        int finalTarget = trajectory.getFinalTarget();
        log.info("old-law plateau control: target walked {} -> {} over {} windows (knee = {}), settled"
                        + " throughput {} records/s",
                startTarget, finalTarget, PLATEAU_WINDOWS, KNEE_SLOTS, trajectory.settledMeanThroughput(50));

        assertThat(startTarget).isEqualTo(KNEE_SLOTS);
        assertWithMessage("throughput must be flat at mu_max the whole plateau - the walk buys NOTHING")
                .that(trajectory.settledMeanThroughput(50))
                .isWithin(0.02 * MU_MAX_RECORDS_PER_SECOND).of(MU_MAX_RECORDS_PER_SECOND);
        assertWithMessage("the ratchet: the final target walks materially above the knee")
                .that(finalTarget)
                .isAtLeast((int) Math.ceil(FalsifierScenarios.bandCeiling(KNEE_SLOTS)) + 1);
        assertWithMessage("the walk is a ratchet, not an excursion: the maximum is where it ends")
                .that(trajectory.maxCommandedTarget()).isEqualTo(finalTarget);
    }

    /**
     * The same defect through the scenario's own front door: the old law FAILS the graceful-saturation
     * plateau scenario that the U5 law must pass - the ablation pair that proves the scenario can tell the
     * two laws apart.
     */
    @Test
    void oldLawFailsTheGracefulSaturationPlateauScenario() {
        assertThrows(AssertionError.class,
                () -> FalsifierScenarios.gracefulSaturationPlateau(oldLaw(KNEE_SLOTS), KNEE_SLOTS));
    }

    /**
     * The sweep result for the old law is RECORDED, never asserted (the U8 execution note): the additive
     * queue-headroom growth can legitimately carry a fixed-L* sweep, and a passing sweep must not read as a
     * broken harness. This test only proves each arm ran to full length; the pass/fail observation per start
     * goes to the log (and the commit body).
     */
    @Test
    void oldLawSweepResultIsRecordedNotAsserted() {
        for (int start : Arrays.asList(1, 2, 5, 20, 50, CEILING_SLOTS)) {
            String observation;
            try {
                FalsifierScenarios.initialConditionSweep(oldLaw(start), start, 1);
                observation = "PASSED";
            } catch (AssertionError walkedOutOfBand) {
                observation = "FAILED (" + firstLine(walkedOutOfBand.getMessage()) + ")";
            }
            log.info("old-law sweep observation from initial target {}: {}", start, observation);

            // The only assertion: the arm genuinely ran - the observation above is about a full-length run.
            DeterministicPlant plant = standardPlant(1);
            Trajectory trajectory = ScenarioRunner.run(oldLaw(start), plant, start,
                    Arrays.asList(Phase.of(SWEEP_WINDOWS, 1.5 * MU_MAX_RECORDS_PER_SECOND)));
            assertThat(trajectory.getRecords()).hasSize(SWEEP_WINDOWS);
        }
    }

    private static String firstLine(String message) {
        if (message == null) {
            return "no message";
        }
        int newline = message.indexOf('\n');
        return newline < 0 ? message : message.substring(0, newline);
    }
}
