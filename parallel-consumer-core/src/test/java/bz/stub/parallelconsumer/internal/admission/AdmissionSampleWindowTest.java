package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;

/**
 * Contract of the single-owner accumulation window: aggregates, percentile conventions, reset-on-close, and the
 * fill-normalization contract (per-invocation values only - the window has no fill dimension).
 */
class AdmissionSampleWindowTest {

    private static final double TOLERANCE = 1e-9;

    @Test
    void meanAndSampleCount() {
        AdmissionSampleWindow window = new AdmissionSampleWindow();
        window.addServiceTimeSample(10);
        window.addServiceTimeSample(20);
        window.addServiceTimeSample(30);

        assertThat(window.getSampleCount()).isEqualTo(3);

        ClosedAdmissionWindow closed = window.close();
        assertThat(closed.getSampleCount()).isEqualTo(3);
        assertThat(closed.getMeanServiceTimeNanos()).isWithin(TOLERANCE).of(20.0);
    }

    @Test
    void inFlightMedianAndSpreadUseNearestRankPercentiles() {
        AdmissionSampleWindow window = new AdmissionSampleWindow();
        // 0..10 sorted: p50 index round(0.5 * 10) = 5 -> 5; p10 index 1 -> 1; p90 index 9 -> 9
        for (int i = 10; i >= 0; i--) { // insertion order must not matter
            window.addInFlightSample(i);
        }

        ClosedAdmissionWindow closed = window.close();
        assertThat(closed.getInFlightSampleCount()).isEqualTo(11);
        assertThat(closed.getInFlightMedian()).isEqualTo(5);
        assertThat(closed.getInFlightSpread()).isEqualTo(8);
    }

    @Test
    void inFlightPercentilesOnSmallEvenCount() {
        AdmissionSampleWindow window = new AdmissionSampleWindow();
        window.addInFlightSample(1);
        window.addInFlightSample(2);
        window.addInFlightSample(3);
        window.addInFlightSample(4);

        ClosedAdmissionWindow closed = window.close();
        // n = 4: p50 index round(1.5) = 2 -> 3; p10 index round(0.3) = 0 -> 1; p90 index round(2.7) = 3 -> 4
        assertThat(closed.getInFlightMedian()).isEqualTo(3);
        assertThat(closed.getInFlightSpread()).isEqualTo(3);
    }

    @Test
    void outcomeCountsAndNonSuccessFraction() {
        AdmissionSampleWindow window = new AdmissionSampleWindow();
        window.recordSuccess();
        window.recordSuccess();
        window.recordIgnore();
        window.recordOverloadDrop();

        ClosedAdmissionWindow closed = window.close();
        assertThat(closed.getSuccessCount()).isEqualTo(2);
        assertThat(closed.getIgnoreCount()).isEqualTo(1);
        assertThat(closed.getOverloadDropCount()).isEqualTo(1);
        assertThat(closed.totalOutcomeCount()).isEqualTo(4);
        assertThat(closed.nonSuccessFraction()).isWithin(TOLERANCE).of(0.5);
    }

    @Test
    void closeResetsForTheNextWindow() {
        AdmissionSampleWindow window = new AdmissionSampleWindow();
        window.addServiceTimeSample(10);
        window.addInFlightSample(7);
        window.recordSuccess();
        window.recordOverloadDrop();
        window.close();

        ClosedAdmissionWindow second = window.close();
        assertThat(second.getSampleCount()).isEqualTo(0);
        assertThat(second.getMeanServiceTimeNanos()).isWithin(TOLERANCE).of(0.0);
        assertThat(second.getInFlightSampleCount()).isEqualTo(0);
        assertThat(second.getInFlightMedian()).isEqualTo(0);
        assertThat(second.getInFlightSpread()).isEqualTo(0);
        assertThat(second.totalOutcomeCount()).isEqualTo(0);
    }

    @Test
    void closesEmptyOnTimeBoundWithoutError() {
        // The caller closes on the TIME BOUND with whatever the window has - possibly nothing
        ClosedAdmissionWindow closed = new AdmissionSampleWindow().close();

        assertThat(closed.getSampleCount()).isEqualTo(0);
        assertThat(closed.getMeanServiceTimeNanos()).isWithin(TOLERANCE).of(0.0);
        assertThat(closed.nonSuccessFraction()).isWithin(TOLERANCE).of(0.0);
    }

    /**
     * Batch normalization is the CALLER's job: the window API takes per-invocation values and nothing else - it
     * has no fill dimension. Feeding the same normalized values, whatever batch fills they came from and in
     * whatever order, must produce identical aggregates.
     */
    @Test
    void sameNormalizedValuesGiveIdenticalAggregatesRegardlessOfImpliedFills() {
        AdmissionSampleWindow first = new AdmissionSampleWindow();
        // e.g. two batches of two records, each normalized to 5ms and 7ms per record
        first.addServiceTimeSample(5_000_000);
        first.addServiceTimeSample(5_000_000);
        first.addServiceTimeSample(7_000_000);
        first.addServiceTimeSample(7_000_000);
        first.addInFlightSample(3);
        first.addInFlightSample(4);
        first.recordSuccess();
        first.recordSuccess();

        AdmissionSampleWindow second = new AdmissionSampleWindow();
        // same normalized per-invocation values from a different implied grouping and arrival order
        second.addServiceTimeSample(7_000_000);
        second.addServiceTimeSample(5_000_000);
        second.addServiceTimeSample(7_000_000);
        second.addServiceTimeSample(5_000_000);
        second.addInFlightSample(4);
        second.addInFlightSample(3);
        second.recordSuccess();
        second.recordSuccess();

        assertThat(first.close()).isEqualTo(second.close());
    }

    @Test
    void extremeServiceTimesDoNotOverflowTheSum() {
        AdmissionSampleWindow window = new AdmissionSampleWindow();
        window.addServiceTimeSample(Long.MAX_VALUE);
        window.addServiceTimeSample(Long.MAX_VALUE);
        window.addServiceTimeSample(Long.MAX_VALUE);

        ClosedAdmissionWindow closed = window.close();
        // a long-based sum would have wrapped negative; the double sum keeps the mean at the sample value
        assertThat(closed.getMeanServiceTimeNanos()).isGreaterThan(0.0);
        assertThat(closed.getMeanServiceTimeNanos()).isWithin(1e12).of((double) Long.MAX_VALUE);
    }
}
