package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.AdaptiveConcurrencyMode;
import org.apache.kafka.common.TopicPartition;
import org.threeten.extra.MutableClock;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Adapter putting a REAL {@link AdmissionController} - probe machinery, pause boundaries, rebalance restore and
 * all - behind the {@link AdmissionPolicy} seam, the U6 counterpart of {@link LawAdmissionPolicy}: the scenarios
 * that exercise controller lifecycle machinery ({@code floorPin}, {@code pauseCycling}, {@code rebalanceShrink},
 * {@code descentFromAbove}) cannot be driven through the law alone, because the escape and descent probes are
 * deliberately controller-owned (the design's KTD4).
 * <p>
 * Windows reach the controller through its {@link AdmissionController#injectClosedWindow} seam - the same
 * pipeline {@code tick} runs, minus the accumulator - with the injected clock advanced one nominal second per
 * window, so the 30s rebalance cooldown spans exactly 30 plant windows.
 * <p>
 * <b>Phase-to-lifecycle mapping</b> (via {@link AdmissionPolicy#onPhaseStart}), mirroring what the engine does:
 * <ul>
 * <li>a ZERO-arrival phase is a PAUSE interval: the engine's state gate stops ticking while {@code PAUSED}, so
 * no windows are fed and the target holds; the first non-zero phase after it delivers the pause-poison
 * consumption ({@link AdmissionController#notifyPauseResumed()}) exactly as the first post-resume engine tick
 * does;</li>
 * <li>a CAPACITY-OVERRIDE phase is a rebalance: the per-instance capacity change models partitions moving away,
 * so the matching revoke/assign cycle is delivered and the controller's own delta gate takes it from there.</li>
 * </ul>
 * The escape jitter seed is FIXED so trajectories are deterministic (the production constructor draws it
 * randomly - a fleet must not probe in lockstep; a test suite must).
 */
final class ControllerAdmissionPolicy implements AdmissionPolicy {

    static final long DETERMINISTIC_JITTER_SEED = 42L;

    private final MutableClock clock = MutableClock.epochUTC();
    private final AdmissionController controller;
    private final List<TopicPartition> assignedPartitions = new ArrayList<>();
    private boolean pausedPhase = false;
    private boolean capacityShrinkDelivered = false;

    ControllerAdmissionPolicy(int initialTarget, int partitionCount) {
        ParallelConsumerOptions<?, ?> options = ParallelConsumerOptions.builder()
                .adaptiveConcurrencyMode(AdaptiveConcurrencyMode.ENFORCE)
                .maxConcurrency(FalsifierScenarios.CEILING_SLOTS)
                .adaptiveConcurrencyInitialTarget(initialTarget)
                .build();
        this.controller = new AdmissionController(options, clock, AdmissionControlLaw.newBuilder(), null,
                DETERMINISTIC_JITTER_SEED);
        for (int partition = 0; partition < partitionCount; partition++) {
            assignedPartitions.add(new TopicPartition("falsifier-topic", partition));
        }
        controller.onPartitionsAssigned(assignedPartitions); // establishes the delta gate's baseline
    }

    @Override
    public void onPhaseStart(ScenarioRunner.Phase phase) {
        if (phase.getArrivalRatePerSecond() == 0) {
            pausedPhase = true;
            return;
        }
        if (pausedPhase) {
            pausedPhase = false;
            controller.notifyPauseResumed(); // the first post-resume tick's poison consumption
        }
        if (phase.getMuMaxOverrideRecordsPerSecond() > 0 && !capacityShrinkDelivered
                && assignedPartitions.size() > 1) {
            // Capacity override = the per-instance share changed = a rebalance. Revoke all but partition 0 -
            // the rebalanceShrink scenario halves capacity with two partitions assigned, so the ratio matches.
            capacityShrinkDelivered = true;
            List<TopicPartition> revoked = assignedPartitions.subList(1, assignedPartitions.size());
            controller.onPartitionsRevoked(revoked);
            controller.onPartitionsAssigned(UniLists.of()); // completes the cycle so the delta gate compares
        }
    }

    @Override
    public int nextTarget(int previousTarget, ClosedAdmissionWindow window) {
        if (pausedPhase) {
            return controller.currentTarget(); // the engine's state gate: no tick while PAUSED
        }
        clock.add(Duration.ofSeconds(1));
        controller.injectClosedWindow(window);
        return controller.currentTarget();
    }
}
