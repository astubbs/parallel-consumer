package bz.stub.parallelconsumer.internal.admission;

/*-
 * Portions copyright 2018 Netflix, Inc. - from Netflix/concurrency-limits (Apache-2.0), class ExpAvgMeasurement
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 *
 * This file contains modified code derived from the Apache-2.0 licensed
 * Netflix/concurrency-limits project (https://github.com/Netflix/concurrency-limits).
 */

import java.util.function.DoubleUnaryOperator;

/**
 * Exponentially weighted moving average of service-time samples, with an arithmetic-mean warmup phase.
 * <p>
 * During the first {@code warmupWindow} samples the value is the plain arithmetic mean of everything seen so far -
 * an exponential average seeded from a single sample would give that first sample far too much weight. Once warm,
 * each new sample is folded in with factor {@code 2 / (window + 1)} (the standard EWMA span-to-alpha conversion).
 * <p>
 * Ported from {@code com.netflix.concurrency.limits.limit.measurement.ExpAvgMeasurement}. Modifications: primitive
 * {@code double} arithmetic instead of boxed {@code Number}, domain naming, and no {@code Measurement} interface -
 * this class is used directly by {@link AdmissionControlLaw} as its long-term service-time baseline.
 * <p>
 * Deterministic and single-threaded by design: every update is an explicit method call, there is no clock.
 */
class ServiceTimeExpAvg {

    private final int window;
    private final int warmupWindow;

    private double value = 0.0;
    private double sum = 0.0;
    private int count = 0;

    ServiceTimeExpAvg(int window, int warmupWindow) {
        this.window = window;
        this.warmupWindow = warmupWindow;
    }

    /**
     * Folds one sample into the average.
     *
     * @return the updated average
     */
    double add(double sample) {
        if (count < warmupWindow) {
            count++;
            sum += sample;
            value = sum / count;
        } else {
            double factor = factor(window);
            value = value * (1 - factor) + sample * factor;
        }
        return value;
    }

    private static double factor(int n) {
        return 2.0 / (n + 1);
    }

    double get() {
        return value;
    }

    /**
     * Applies an arbitrary correction to the current value (e.g. the anti-drift decay in
     * {@link AdmissionControlLaw}) without counting as a sample.
     */
    void update(DoubleUnaryOperator operation) {
        this.value = operation.applyAsDouble(value);
    }
}
