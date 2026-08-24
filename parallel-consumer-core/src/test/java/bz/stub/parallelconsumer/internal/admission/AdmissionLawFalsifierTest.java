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
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * THE FLIP of the falsifier suite's control run (the design's R14, the U5 rewrite): the {@link FalsifierScenarios
 * scenarios} that the deleted {@code OldLawPlateauControlTest} proved the Gradient2 port FAILED now run against
 * the {@link AdmissionControlLaw band-machine law} as a real green suite. The mutant matrix in
 * {@link AdmissionFalsifierHarnessTest} stays green alongside, so the suite can still fail a broken controller.
 * <p>
 * <b>The headline pair, for the record</b> (the deterministic plant makes both exact): on the
 * graceful-saturation plateau from the knee (20 slots), the old law walked <b>20 -&gt; 60 over 250 windows</b>
 * (its final target the run's maximum - a ratchet, not an excursion) while throughput stayed flat at 400
 * records/s. The new law's trajectory on the identical plant: one blind warmup step 20 -&gt; 24, retracted to
 * <b>exactly 20</b> when the first elasticity verdict showed it bought nothing, and 20 for every window of the
 * settled tail - asserted below, pinned to those numbers.
 * <p>
 * <b>What is deliberately NOT asserted here:</b>
 * <ul>
 * <li>sweep arms starting ABOVE the knee (50, the ceiling) - descent on a flat plateau needs a signal this law
 * is forbidden to read (queueing latency, R8) or U6's escape probe; a throughput-steered law cannot distinguish
 * 50 slots from 20 when both complete the same 400 records/s.
 * <!-- TODO(refactor): assert the above-knee sweep arms once U6's escape probe lands (plan 2026-08-24-003, R6) -
 *  until then the law parks where the plateau band catches it, which the plateau scenario already pins. --></li>
 * <li>{@code pauseCycling}, {@code rebalanceShrink}, {@code floorPin} - they exercise controller machinery
 * (pause invalidation boundaries, rebalance restore, the ungated escape) that U6 builds; their TODOs live on the
 * scenarios themselves in {@link FalsifierScenarios}.</li>
 * </ul>
 */
@Slf4j
class AdmissionLawFalsifierTest {

    private static final int KNEE_SLOTS = 20; // = standardPlant(1).optimalTargetSlots()

    private static LawAdmissionPolicy law(int initialTarget) {
        return new LawAdmissionPolicy(initialTarget, CEILING_SLOTS);
    }

    /**
     * The initial-condition sweep, from-below arms at batchSize 1: every start must be inside the oracle's band
     * from the convergence deadline on. Includes the oracle start itself - the law must not walk OFF the answer
     * either (which is exactly what the old law did).
     */
    @Test
    void sweepConvergesFromBelowAndHoldsTheKnee() {
        for (int start : Arrays.asList(1, 2, 5, KNEE_SLOTS)) {
            FalsifierScenarios.initialConditionSweep(law(start), start, 1);
        }
    }

    /**
     * The units-seam arm (R14: at least one arm with batchSize &gt; 1): at batchSize 4 the oracle is 5 SLOTS,
     * and a slots-denominated law must converge to it - a records-denominated one would head for 20.
     */
    @Test
    void sweepConvergesAtBatchSizeFour() {
        for (int start : Arrays.asList(1, 2)) {
            FalsifierScenarios.initialConditionSweep(law(start), start, 4);
        }
    }

    /**
     * The arrival-burst dual: L* constant while arrival settles, bursts, gaps and returns - the law must not
     * chase load (the estimator's central confound on a Kafka topic).
     */
    @Test
    void arrivalBurstIsNotChased() {
        FalsifierScenarios.arrivalBurstDual(law(KNEE_SLOTS), KNEE_SLOTS);
    }

    /**
     * The app-limited lull - HOLD's falsifier, the RFC 7661 decay-on-idle regression test: preserved through the
     * lull, throughput recovers after it.
     */
    @Test
    void appLimitedLullPreservesTheTarget() {
        FalsifierScenarios.appLimitedLull(law(KNEE_SLOTS), KNEE_SLOTS);
    }

    /**
     * The graceful-saturation plateau - the scenario the OLD law failed (its control test asserted the ratchet
     * happening; U5 deleted it and flipped the assertion here): flat throughput with climbing in-flight must not
     * license growth.
     */
    @Test
    void gracefulSaturationPlateauDoesNotRatchet() {
        FalsifierScenarios.gracefulSaturationPlateau(law(KNEE_SLOTS), KNEE_SLOTS);
    }

    /**
     * The headline trajectory, pinned exactly (the deterministic plant makes this reproducible): from the knee
     * under saturating arrival, the new law takes ONE blind warmup step (20 -&gt; 24, the KTD2 allowance), the
     * first elasticity verdict reads the plateau (flat throughput), the step is RETRACTED to exactly the knee,
     * and the target never moves again - where the old law's identical run walked 20 -&gt; 60.
     */
    @Test
    void plateauTrajectoryWarmupStepIsRetractedToExactlyTheKnee() {
        DeterministicPlant plant = FalsifierScenarios.standardPlant(1);
        Trajectory trajectory = ScenarioRunner.run(law(KNEE_SLOTS), plant, KNEE_SLOTS,
                Arrays.asList(Phase.of(250, 1.5 * MU_MAX_RECORDS_PER_SECOND)));

        log.info("new-law plateau trajectory: start {}, max {}, final {} over 250 windows, settled throughput "
                        + "{} records/s",
                trajectory.commandedTargetAt(0), trajectory.maxCommandedTarget(), trajectory.getFinalTarget(),
                trajectory.settledMeanThroughput(50));

        assertWithMessage("the one blind excursion is the warmup allowance, nothing more")
                .that(trajectory.maxCommandedTarget()).isEqualTo(24);
        assertWithMessage("the warmup step is retracted to exactly the knee - the law converges to the last "
                + "level that PAID, not one overshoot step above it")
                .that(trajectory.getFinalTarget()).isEqualTo(KNEE_SLOTS);
        for (int window = 200; window < 250; window++) {
            assertWithMessage("settled tail window %s", window)
                    .that(trajectory.commandedTargetAt(window)).isEqualTo(KNEE_SLOTS);
        }
        assertWithMessage("and the hold costs no throughput - flat at capacity")
                .that(trajectory.settledMeanThroughput(50))
                .isWithin(0.02 * MU_MAX_RECORDS_PER_SECOND).of(MU_MAX_RECORDS_PER_SECOND);
    }

    /**
     * Sparse adjudication - KTD2's cap falsifier: a plant whose windows mostly cannot adjudicate must not fund
     * unbounded blind growth. Under this schedule the new law never sees a bound adjudicated window at all, so
     * growth is exactly zero - well inside the allowance the scenario brackets.
     */
    @Test
    void sparseAdjudicationStaysWithinTheWarmupAllowance() {
        FalsifierScenarios.sparseAdjudication(law(10), 10);
    }

    /**
     * The batch-4 oracle sanity companion: the trajectory lands ON the slots oracle (5), pinned exactly - the
     * band assertion above would also pass at 4 or 6, and the units seam deserves the sharper pin.
     */
    @Test
    void batchFourTrajectoryLandsOnTheSlotsOracle() {
        DeterministicPlant plant = FalsifierScenarios.standardPlant(4);
        Trajectory trajectory = ScenarioRunner.run(law(1), plant, 1,
                Arrays.asList(Phase.of(200, 1.5 * MU_MAX_RECORDS_PER_SECOND)));

        assertThat(trajectory.getFinalTarget()).isEqualTo(5);
    }
}
