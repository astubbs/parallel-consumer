package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.admission.MutantPolicies.AlwaysMaxLimit;
import bz.stub.parallelconsumer.internal.admission.MutantPolicies.AlwaysMinLimit;
import bz.stub.parallelconsumer.internal.admission.MutantPolicies.FrozenLimit;
import bz.stub.parallelconsumer.internal.admission.ScenarioRunner.Phase;
import bz.stub.parallelconsumer.internal.admission.ScenarioRunner.Trajectory;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static bz.stub.parallelconsumer.internal.admission.FalsifierScenarios.CEILING_SLOTS;
import static bz.stub.parallelconsumer.internal.admission.FalsifierScenarios.MU_MAX_RECORDS_PER_SECOND;
import static bz.stub.parallelconsumer.internal.admission.FalsifierScenarios.W0_SECONDS;
import static bz.stub.parallelconsumer.internal.admission.FalsifierScenarios.standardPlant;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Meta-tests of the falsifier harness itself (the design's R14), green BEFORE any law rewrite exists:
 * <ul>
 * <li><b>plant honesty</b> - determinism, the load model's latency and saturation shape, and honest
 * work-limited windows (the old law reads latency, so a dishonest plant would void the control run);</li>
 * <li><b>oracle arithmetic</b> - {@code L*_slots = mu_max * W0 / batchSize}, including the batchSize &gt; 1
 * units seam;</li>
 * <li><b>the negative-control layer</b> - each mutant controller FAILS each applicable scenario, asserted via
 * {@code assertThrows} on the scenario's own assertion. Green here means "the suite can fail a broken
 * controller", the property a safety-only suite lacks.</li>
 * </ul>
 * Mutant applicability matrix (a mutant is asserted only against scenarios whose falsified decision it
 * embodies; a dash is a scenario the mutant legitimately passes, each dash explained on its test):
 * <pre>
 *                  sweep(wrong start)  burst  lull  plateau  sparse
 * FrozenLimit             FAILS        FAILS  FAILS    -       -
 * AlwaysMaxLimit          FAILS        FAILS  FAILS  FAILS   FAILS
 * AlwaysMinLimit          FAILS        FAILS  FAILS  FAILS     -
 * </pre>
 */
class AdmissionFalsifierHarnessTest {

    private static final double ORACLE_SLOTS_BATCH_1 = 20.0; // 400 records/s * 0.05 s / 1
    private static final double ORACLE_SLOTS_BATCH_4 = 5.0;  // 400 records/s * 0.05 s / 4
    private static final int ORACLE_START = 20;

    // ------------------------------------------------------------------
    // Plant honesty
    // ------------------------------------------------------------------

    @Test
    void plantIsDeterministic() {
        Trajectory first = burstTrajectory();
        Trajectory second = burstTrajectory();

        assertWithMessage("same config must produce an identical trajectory")
                .that(second).isEqualTo(first);
    }

    private static Trajectory burstTrajectory() {
        return ScenarioRunner.run(new FrozenLimit(), standardPlant(1), ORACLE_START, Arrays.asList(
                Phase.of(30, 380), Phase.of(10, 700), Phase.of(30, 100)));
    }

    @Test
    void oracleIsDerivedFromPlantParameters() {
        assertThat(standardPlant(1).optimalTargetSlots()).isEqualTo(ORACLE_SLOTS_BATCH_1);
        assertThat(standardPlant(4).optimalTargetSlots()).isEqualTo(ORACLE_SLOTS_BATCH_4);

        DeterministicPlant doubledCapacity =
                new DeterministicPlant(2 * MU_MAX_RECORDS_PER_SECOND, W0_SECONDS, 1);
        assertWithMessage("doubling capacity must double the slots oracle")
                .that(doubledCapacity.optimalTargetSlots()).isEqualTo(2 * ORACLE_SLOTS_BATCH_1);
    }

    @Test
    void serviceTimeFollowsTheLoadModel() {
        DeterministicPlant plant = standardPlant(1);
        plant.setArrivalRatePerSecond(10 * MU_MAX_RECORDS_PER_SECOND); // saturate: every window limit-bound

        ClosedAdmissionWindow atKnee = plant.produceWindow(ORACLE_START);
        assertWithMessage("at the knee the plant reports the uncongested service time")
                .that(atKnee.getMeanServiceTimeNanos()).isEqualTo(plant.uncongestedServiceTimeNanos());

        ClosedAdmissionWindow doubled = plant.produceWindow(2 * ORACLE_START);
        assertWithMessage("at twice the knee, queueing doubles the reported service time - the honesty the"
                + " old-law control run depends on, since the old law reads latency")
                .that(doubled.getMeanServiceTimeNanos())
                .isEqualTo(2 * plant.uncongestedServiceTimeNanos());
    }

    @Test
    void throughputSaturatesAtCapacityAboveTheKnee() {
        DeterministicPlant plant = standardPlant(1);
        plant.setArrivalRatePerSecond(1.5 * MU_MAX_RECORDS_PER_SECOND);

        ClosedAdmissionWindow window = plant.produceWindow(2 * ORACLE_START);
        double backlogAfterFirst = plant.getBacklogRecords();
        ClosedAdmissionWindow next = plant.produceWindow(2 * ORACLE_START);

        assertWithMessage("above the knee throughput is flat at mu_max - a higher target buys queueing only")
                .that(window.successThroughputPerSecond()).isWithin(1.0).of(MU_MAX_RECORDS_PER_SECOND);
        assertThat(next.successThroughputPerSecond()).isWithin(1.0).of(MU_MAX_RECORDS_PER_SECOND);
        assertWithMessage("the un-served arrivals accumulate as backlog")
                .that(plant.getBacklogRecords()).isGreaterThan(backlogAfterFirst);
        assertThat(window.isLimitBound()).isTrue();
    }

    @Test
    void workLimitedWindowsReadHonestly() {
        DeterministicPlant plant = standardPlant(1);
        plant.setArrivalRatePerSecond(100); // a quarter of capacity

        ClosedAdmissionWindow window = plant.produceWindow(ORACLE_START);

        assertWithMessage("a window the limit did not bind must not classify as limit-bound")
                .that(window.isLimitBound()).isFalse();
        assertThat(window.bindingClassification())
                .isEqualTo(ClosedAdmissionWindow.BindingClassification.NO_WORK);
        assertWithMessage("uncongested service time when work-limited")
                .that(window.getMeanServiceTimeNanos()).isEqualTo(plant.uncongestedServiceTimeNanos());
        assertWithMessage("occupancy follows Little's Law: 100 records/s * 0.05 s = 5 slots")
                .that(window.getInFlightMedian()).isEqualTo(5);
    }

    // ------------------------------------------------------------------
    // The negative-control layer: each mutant fails each applicable scenario
    // ------------------------------------------------------------------

    /** Every wrong sweep start at batchSize 1: all of the packet's {1, 2, 5, 20, 50, ceiling} except 20. */
    private static final List<Integer> WRONG_STARTS = Arrays.asList(1, 2, 5, 50, CEILING_SLOTS);

    @Nested
    class FrozenLimitMutant {

        @Test
        void failsTheSweepFromEveryWrongStart() {
            for (int wrongStart : WRONG_STARTS) {
                assertThrows(AssertionError.class,
                        () -> FalsifierScenarios.initialConditionSweep(new FrozenLimit(), wrongStart, 1),
                        "FrozenLimit must fail the sweep from initial target " + wrongStart);
            }
        }

        /**
         * The units-seam arm: at batchSize 4 the oracle is 5 SLOTS. A frozen controller sitting on 20 - the
         * RECORDS oracle {@code mu_max * W0} - must fail, which is exactly the failure a records-denominated
         * band would miss.
         */
        @Test
        void failsTheBatchSizeSweepWhenFrozenOnTheRecordsOracle() {
            assertThrows(AssertionError.class,
                    () -> FalsifierScenarios.initialConditionSweep(new FrozenLimit(), 20, 4));
        }

        /**
         * The one arm a frozen controller passes: sitting on the oracle. Converging from ELSEWHERE is the
         * liveness the sweep tests; sitting on the answer satisfies every safety property - which is why one
         * green arm here is expected, and why the sweep parameterises over starts at all.
         */
        @Test
        void passesTheSweepOnlyFromTheOracleStart() {
            FalsifierScenarios.initialConditionSweep(new FrozenLimit(), ORACLE_START, 1);
        }

        @Test
        void failsTheArrivalBurstDualFromAWrongStart() {
            assertThrows(AssertionError.class,
                    () -> FalsifierScenarios.arrivalBurstDual(new FrozenLimit(), 1));
        }

        @Test
        void failsTheAppLimitedLullFromAWrongStart() {
            assertThrows(AssertionError.class,
                    () -> FalsifierScenarios.appLimitedLull(new FrozenLimit(), 1));
        }

        // No plateau/sparse arms: a frozen controller started on the knee holds it (plateau) and never grows
        // (sparse) - both are the inaction those scenarios do not exist to falsify; the sweep owns FrozenLimit.
    }

    @Nested
    class AlwaysMaxLimitMutant {

        private AdmissionPolicy mutant() {
            return new AlwaysMaxLimit(CEILING_SLOTS);
        }

        @Test
        void failsTheSweep() {
            assertThrows(AssertionError.class,
                    () -> FalsifierScenarios.initialConditionSweep(mutant(), ORACLE_START, 1));
        }

        /** The plateau's band and latency brackets: a ceiling-pinned target queues, W &gt; W0. */
        @Test
        void failsTheGracefulSaturationPlateau() {
            assertThrows(AssertionError.class,
                    () -> FalsifierScenarios.gracefulSaturationPlateau(mutant(), ORACLE_START));
        }

        @Test
        void failsTheArrivalBurstDual() {
            assertThrows(AssertionError.class,
                    () -> FalsifierScenarios.arrivalBurstDual(mutant(), ORACLE_START));
        }

        @Test
        void failsTheAppLimitedLull() {
            assertThrows(AssertionError.class,
                    () -> FalsifierScenarios.appLimitedLull(mutant(), ORACLE_START));
        }

        @Test
        void failsSparseAdjudicationByUnboundedGrowth() {
            assertThrows(AssertionError.class,
                    () -> FalsifierScenarios.sparseAdjudication(mutant(), 10));
        }
    }

    @Nested
    class AlwaysMinLimitMutant {

        @Test
        void failsTheSweep() {
            assertThrows(AssertionError.class,
                    () -> FalsifierScenarios.initialConditionSweep(new AlwaysMinLimit(), ORACLE_START, 1));
        }

        /** The plateau's throughput bracket: a floor-pinned target starves the downstream. */
        @Test
        void failsTheGracefulSaturationPlateau() {
            assertThrows(AssertionError.class,
                    () -> FalsifierScenarios.gracefulSaturationPlateau(new AlwaysMinLimit(), ORACLE_START));
        }

        @Test
        void failsTheArrivalBurstDual() {
            assertThrows(AssertionError.class,
                    () -> FalsifierScenarios.arrivalBurstDual(new AlwaysMinLimit(), ORACLE_START));
        }

        @Test
        void failsTheAppLimitedLull() {
            assertThrows(AssertionError.class,
                    () -> FalsifierScenarios.appLimitedLull(new AlwaysMinLimit(), ORACLE_START));
        }

        // No sparse arm: pinning the floor grows by nothing, and bounded growth is exactly what sparse
        // adjudication asserts - the floor pin's own falsifier is U6's escape scenario, not this one.
    }
}
