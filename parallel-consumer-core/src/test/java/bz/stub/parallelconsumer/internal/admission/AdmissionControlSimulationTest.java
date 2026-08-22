package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Value;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Seeded closed-loop simulation of the whole law against an M/M/1-flavored latency model: service time is flat at
 * a base value while concurrency is below a modeled capacity, and past it queueing makes latency grow linearly
 * with concurrency (so, by Little's law, throughput saturates at the capacity instead of growing). Multiplicative
 * Gaussian noise (fixed seed) keeps the gradient honest; a simulated clock (fixed one-second windows) converts
 * completions to throughput.
 * <p>
 * NOTE: this latency curve is a modeling placeholder - it should be RE-FITTED to measured arrival-harness data
 * (the real per-partition service-time curves) when that harness exists, keeping the same assertions.
 */
class AdmissionControlSimulationTest {

    private static final long FIXED_SEED = 42L;

    private static final double MS = 1_000_000.0;
    private static final double BASE_SERVICE_TIME_NANOS = 10 * MS;
    private static final int MODELED_CAPACITY_SLOTS = 50;
    private static final double NOISE_STDDEV = 0.05;
    private static final long WINDOW_DURATION_NANOS = 1_000_000_000L;

    private static final int CEILING = 150;
    private static final int INITIAL_LIMIT = 10;
    private static final int TOTAL_WINDOWS = 300;
    private static final int SETTLED_WINDOWS = 50; // the tail we assert over

    /**
     * Throughput ceiling of the modeled downstream, in completions per window.
     */
    private static final double MODELED_CAPACITY_THROUGHPUT =
            MODELED_CAPACITY_SLOTS * (WINDOW_DURATION_NANOS / BASE_SERVICE_TIME_NANOS);

    @Value
    private static class SimResult {
        double settledMeanThroughput;
        int settledMinLimit;
        int settledMaxLimit;
        int finalLimit;
    }

    /**
     * M/M/1-flavored: flat at base below capacity; past capacity the queue makes latency proportional to
     * concurrency, so completions per window saturate at the modeled capacity.
     */
    private static double modeledLatency(int limit) {
        return BASE_SERVICE_TIME_NANOS * Math.max(1.0, (double) limit / MODELED_CAPACITY_SLOTS);
    }

    private static SimResult runSimulation(long seed) {
        Random random = new Random(seed);
        AdmissionControlLaw law = AdmissionControlLaw.newBuilder()
                .initialLimit(INITIAL_LIMIT).ceiling(CEILING)
                .build();

        double settledThroughputSum = 0;
        int settledMin = Integer.MAX_VALUE;
        int settledMax = Integer.MIN_VALUE;

        for (int window = 0; window < TOTAL_WINDOWS; window++) {
            int limit = law.getLimit();
            double noise = Math.max(0.2, 1 + NOISE_STDDEV * random.nextGaussian());
            double latency = modeledLatency(limit) * noise;
            // Little's law over the simulated one-second window
            int completions = (int) (limit * (WINDOW_DURATION_NANOS / latency));

            law.onWindowClosed(new ClosedAdmissionWindow(
                    completions, latency, completions, limit, 2, completions, 0, 0));

            if (window >= TOTAL_WINDOWS - SETTLED_WINDOWS) {
                settledThroughputSum += completions;
                settledMin = Math.min(settledMin, law.getLimit());
                settledMax = Math.max(settledMax, law.getLimit());
            }
        }
        return new SimResult(settledThroughputSum / SETTLED_WINDOWS, settledMin, settledMax, law.getLimit());
    }

    @Test
    void throughputSettlesNearModeledCapacityWithoutCollapse() {
        SimResult result = runSimulation(FIXED_SEED);

        assertWithMessage("settled throughput should be near the modeled capacity")
                .that(result.getSettledMeanThroughput())
                .isAtLeast(0.9 * MODELED_CAPACITY_THROUGHPUT);
        assertWithMessage("the limit must not collapse")
                .that(result.getSettledMinLimit())
                .isAtLeast(MODELED_CAPACITY_SLOTS / 2);
        assertWithMessage("the limit must not run away to the ceiling")
                .that(result.getSettledMaxLimit())
                .isLessThan(CEILING);
    }

    @Test
    void simulationIsDeterministicUnderTheFixedSeed() {
        SimResult first = runSimulation(FIXED_SEED);
        SimResult second = runSimulation(FIXED_SEED);

        assertThat(second).isEqualTo(first);
    }
}
