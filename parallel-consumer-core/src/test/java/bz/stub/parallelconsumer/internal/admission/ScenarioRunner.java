package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Drives an {@link AdmissionPolicy} against a {@link DeterministicPlant} for a schedule of phases and records
 * the trajectory - one {@link WindowRecord} per window - for the scenario assertions to read. The runner owns
 * the loop and only a floor clamp (a zero target would stall the plant forever); ceilings belong to the policy.
 */
final class ScenarioRunner {

    private ScenarioRunner() {
    }

    /** One phase of a scenario schedule: a window count at an arrival rate, optionally moving the capacity. */
    @Value
    static class Phase {
        int windowCount;
        double arrivalRatePerSecond;
        /** Zero means "keep the plant's current capacity". */
        double muMaxOverrideRecordsPerSecond;
        /** NaN means "keep the plant's current outcome mix" (torture plan U1). */
        double nonSuccessFractionOverride;
        /** Negative means "keep the plant's current per-window overload drops" (torture plan U1). */
        long overloadDropsPerWindowOverride;

        static Phase of(int windowCount, double arrivalRatePerSecond) {
            return new Phase(windowCount, arrivalRatePerSecond, 0.0, Double.NaN, -1);
        }

        static Phase withCapacity(int windowCount, double arrivalRatePerSecond, double muMaxRecordsPerSecond) {
            return new Phase(windowCount, arrivalRatePerSecond, muMaxRecordsPerSecond, Double.NaN, -1);
        }

        /** A phase that also moves the outcome mix - the failure-threshold-riding scenarios' knob. */
        static Phase withOutcomes(int windowCount, double arrivalRatePerSecond, double nonSuccessFraction,
                                  long overloadDropsPerWindow) {
            return new Phase(windowCount, arrivalRatePerSecond, 0.0, nonSuccessFraction, overloadDropsPerWindow);
        }
    }

    /** One window of a trajectory: what was commanded, and what the plant did under it. */
    @Value
    static class WindowRecord {
        int windowIndex;
        /** The target the policy had commanded when this window ran. */
        int commandedTarget;
        double throughputPerSecond;
        double meanServiceTimeNanos;
        boolean limitBound;
    }

    /** A full run: the per-window records plus the policy's last output (the target it would command next). */
    @Value
    static class Trajectory {
        List<WindowRecord> records;
        int finalTarget;

        int commandedTargetAt(int windowIndex) {
            return records.get(windowIndex).getCommandedTarget();
        }

        int maxCommandedTarget() {
            int max = Integer.MIN_VALUE;
            for (WindowRecord record : records) {
                max = Math.max(max, record.getCommandedTarget());
            }
            return max;
        }

        /** Mean throughput over the last {@code tailWindows} windows. */
        double settledMeanThroughput(int tailWindows) {
            double sum = 0;
            for (int i = records.size() - tailWindows; i < records.size(); i++) {
                sum += records.get(i).getThroughputPerSecond();
            }
            return sum / tailWindows;
        }

        /** Mean service time over the last {@code tailWindows} windows, in nanoseconds. */
        double settledMeanServiceTimeNanos(int tailWindows) {
            double sum = 0;
            for (int i = records.size() - tailWindows; i < records.size(); i++) {
                sum += records.get(i).getMeanServiceTimeNanos();
            }
            return sum / tailWindows;
        }
    }

    static Trajectory run(AdmissionPolicy policy, DeterministicPlant plant, int initialTarget,
                          List<Phase> phases) {
        int target = Math.max(1, initialTarget);
        List<WindowRecord> records = new ArrayList<>();
        int windowIndex = 0;
        for (Phase phase : phases) {
            policy.onPhaseStart(phase);
            plant.setArrivalRatePerSecond(phase.getArrivalRatePerSecond());
            if (phase.getMuMaxOverrideRecordsPerSecond() > 0) {
                plant.setMuMaxRecordsPerSecond(phase.getMuMaxOverrideRecordsPerSecond());
            }
            if (!Double.isNaN(phase.getNonSuccessFractionOverride())) {
                plant.setNonSuccessFraction(phase.getNonSuccessFractionOverride());
            }
            if (phase.getOverloadDropsPerWindowOverride() >= 0) {
                plant.setOverloadDropsPerWindow(phase.getOverloadDropsPerWindowOverride());
            }
            for (int w = 0; w < phase.getWindowCount(); w++) {
                ClosedAdmissionWindow window = plant.produceWindow(target);
                records.add(new WindowRecord(windowIndex++, target, window.successThroughputPerSecond(),
                        window.getMeanServiceTimeNanos(), window.isLimitBound()));
                target = Math.max(1, policy.nextTarget(target, window));
            }
        }
        return new Trajectory(records, target);
    }
}
