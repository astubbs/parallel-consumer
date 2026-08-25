package bz.stub.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Gating-contract regression for {@link ProgressProbe}'s Class 2 lag-stagnation detector: crossing
 * {@link ProgressProbe#LAG_STAGNATION_BOUND} must be recorded as a non-gating
 * {@link ProgressProbe#getObservations() observation} and must NOT fail a chaos run.
 * <p>
 * <b>Why this test exists as a permanent guard rather than a note.</b> The demotion (2026-08-25) is
 * a deliberate weakening of what the chaos suite asserts, and the ordinary reading of a bound in a
 * test suite is that crossing it is a failure - so the natural "repair" for someone who finds this
 * detector silent is to route it back through {@code violate()}. That would restore a false-positive
 * class measured three times over: seeds 4734674029169027864, 6825864417772979246 and
 * 4044221734199516240 all cross this bound and then drain completely, the latter two being the
 * seeds the sightings ledger itself nominated as its strongest evidence. Nothing else in the tree
 * would go red if the routing changed back, which is exactly the silent-regression shape this repo
 * writes guards for.
 * <p>
 * Drives the {@link ProgressProbe#recordLagStagnation} seam directly - no broker, no sampler thread,
 * no admin round-trip - in the {@link InstanceStallProbeIT} / {@link ProgressProbeLedgerIT} style,
 * and is deliberately NOT tagged {@code chaos} so it gates every default integration build. A guard
 * that only runs in the on-demand lane is not a guard.
 */
class Class2ObservationIT {

    private static final TopicPartition TP = new TopicPartition("class2-observation-it", 7);
    private static final long OVER_BOUND_MS = ProgressProbe.LAG_STAGNATION_BOUND.toMillis() + 1;
    private static final long REAL_LAG = ProgressProbe.LAG_STAGNATION_MIN_LAG + 1;

    /**
     * Chaos mode is the one that gates, so it is the mode this contract has to hold in - asserting it
     * in ambient observer mode would prove nothing, since nothing gates there anyway. The null kcu is
     * legal for the same reason it is in {@link InstanceStallProbeIT}: the sampler thread is never
     * started, and {@link ProgressProbe#recordLagStagnation} reaches no cluster.
     */
    private static ProgressProbe gatingProbe() {
        return new ProgressProbe(null, "class2-observation-group", "class2-observation-it", () -> 0L, 0);
    }

    @Test
    void crossingTheBoundIsObservedAndDoesNotGate() {
        ProgressProbe probe = gatingProbe();

        boolean crossed = probe.recordLagStagnation(TP, 91L, REAL_LAG, OVER_BOUND_MS);

        assertWithMessage("the bound was crossed, so the caller must re-arm this partition")
                .that(crossed).isTrue();
        assertWithMessage("CLASS2_STALL is a timing measurement and must never fail a correctness run - "
                + "see the class javadoc for the three replays that established this")
                .that(probe.getViolations()).isEmpty();
        assertWithMessage("a demoted detector must still be heard, or demoting it is deleting it")
                .that(probe.hasViolations()).isFalse();
        assertThat(probe.getObservations()).hasSize(1);
        assertThat(probe.getObservations().get(0)).contains("CLASS2_STALL/LAG_STAGNATION");
        assertThat(probe.getObservations().get(0)).contains("partition " + TP);
    }

    /**
     * The measurement must survive the demotion. Peak tracking is what makes a non-gating detector
     * worth keeping - it is the only surface a timing regression has once it cannot turn anything red.
     */
    @Test
    void thePeakIsMeasuredEvenWhenTheBoundIsNotCrossed() {
        ProgressProbe probe = gatingProbe();
        long underBound = ProgressProbe.LAG_STAGNATION_BOUND.toMillis() - 1_000;

        boolean crossed = probe.recordLagStagnation(TP, 91L, REAL_LAG, underBound);

        assertWithMessage("under the bound, so nothing to report and nothing to re-arm")
                .that(crossed).isFalse();
        assertThat(probe.getObservations()).isEmpty();
        assertWithMessage("the peak is measured unconditionally - suppressing a finding must not lose "
                + "the number behind it")
                .that(probe.getPeakLagStagnationMs()).isEqualTo(underBound);
    }

    /**
     * The trivial-tail guard: the Class 2 signature is real backlog going nowhere, so a stagnant
     * partition with negligible lag is neither observed nor measured.
     */
    @Test
    void aTrivialTailIsNeitherObservedNorMeasured() {
        ProgressProbe probe = gatingProbe();

        boolean crossed = probe.recordLagStagnation(TP, 91L, ProgressProbe.LAG_STAGNATION_MIN_LAG - 1,
                OVER_BOUND_MS);

        assertThat(crossed).isFalse();
        assertThat(probe.getObservations()).isEmpty();
        assertWithMessage("lag below LAG_STAGNATION_MIN_LAG is not a Class 2 sample at all, so it must "
                + "not inflate the peak either")
                .that(probe.getPeakLagStagnationMs()).isEqualTo(0L);
    }
}
