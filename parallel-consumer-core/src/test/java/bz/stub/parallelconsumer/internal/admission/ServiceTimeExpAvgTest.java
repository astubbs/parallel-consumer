package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;

/**
 * Tests the EWMA warmup (arithmetic-mean phase) and the exponential phase separately, in the style of upstream's
 * {@code ExpAvgMeasurementTest}.
 */
class ServiceTimeExpAvgTest {

    private static final double TOLERANCE = 1e-9;

    @Test
    void warmupPhaseIsArithmeticMean() {
        ServiceTimeExpAvg avg = new ServiceTimeExpAvg(100, 3);

        assertThat(avg.add(10)).isWithin(TOLERANCE).of(10.0);
        assertThat(avg.add(20)).isWithin(TOLERANCE).of(15.0);
        assertThat(avg.add(30)).isWithin(TOLERANCE).of(20.0);
    }

    @Test
    void exponentialPhaseUsesSpanFactor() {
        ServiceTimeExpAvg avg = new ServiceTimeExpAvg(100, 3);
        avg.add(10);
        avg.add(20);
        avg.add(30); // warmup complete, value = 20

        double factor = 2.0 / (100 + 1);
        double expected = 20.0 * (1 - factor) + 100.0 * factor;
        assertThat(avg.add(100)).isWithin(TOLERANCE).of(expected);
        assertThat(avg.get()).isWithin(TOLERANCE).of(expected);
    }

    @Test
    void shortSpanWeightsNewSamplesHeavily() {
        // window = 3 gives factor 0.5, so one sample moves the value half way
        ServiceTimeExpAvg avg = new ServiceTimeExpAvg(3, 1);
        avg.add(4); // warmup of one: value = 4

        assertThat(avg.add(8)).isWithin(TOLERANCE).of(6.0);
    }

    @Test
    void updateAppliesOperatorWithoutCountingAsSample() {
        ServiceTimeExpAvg avg = new ServiceTimeExpAvg(100, 2);
        avg.add(10);

        avg.update(current -> current * 2);
        assertThat(avg.get()).isWithin(TOLERANCE).of(20.0);

        // Still in warmup: the next add recomputes the arithmetic mean from the raw sum, so the correction does
        // not stick during warmup - faithful to upstream ExpAvgMeasurement's behavior.
        assertThat(avg.add(30)).isWithin(TOLERANCE).of(20.0);
    }

    @Test
    void updateSticksOncePastWarmup() {
        ServiceTimeExpAvg avg = new ServiceTimeExpAvg(100, 1);
        avg.add(40); // warmup complete

        avg.update(current -> current * 0.5);
        assertThat(avg.get()).isWithin(TOLERANCE).of(20.0);

        // Exponential phase folds new samples into the CORRECTED value
        double factor = 2.0 / (100 + 1);
        double expected = 20.0 * (1 - factor) + 20.0 * factor;
        assertThat(avg.add(20)).isWithin(TOLERANCE).of(expected);
    }
}
