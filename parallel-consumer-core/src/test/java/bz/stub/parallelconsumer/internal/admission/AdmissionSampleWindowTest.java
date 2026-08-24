package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.admission.ClosedAdmissionWindow.BindingClassification;
import org.junit.jupiter.api.Test;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Contract of the single-owner accumulation window: aggregates, percentile conventions, reset-on-close, the
 * fill-normalization contract (per-invocation values only - the window has no fill dimension), useful throughput
 * over MEASURED elapsed time (the design's R4), and the binding classification from the boundary signals (R2,
 * KTD1).
 */
class AdmissionSampleWindowTest {

    private static final double TOLERANCE = 1e-9;

    /** The nominal window length - what the caller aims for; the MEASURED elapsed time is what throughput uses. */
    private static final long NOMINAL_ELAPSED_NANOS = 1_000_000_000L;

    @Test
    void meanAndSampleCount() {
        AdmissionSampleWindow window = new AdmissionSampleWindow();
        window.addServiceTimeSample(10);
        window.addServiceTimeSample(20);
        window.addServiceTimeSample(30);

        assertThat(window.getSampleCount()).isEqualTo(3);

        ClosedAdmissionWindow closed = close(window);
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

        ClosedAdmissionWindow closed = close(window);
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

        ClosedAdmissionWindow closed = close(window);
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

        ClosedAdmissionWindow closed = close(window);
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
        close(window);

        ClosedAdmissionWindow second = close(window);
        assertThat(second.getSampleCount()).isEqualTo(0);
        assertThat(second.getMeanServiceTimeNanos()).isWithin(TOLERANCE).of(0.0);
        assertThat(second.getInFlightSampleCount()).isEqualTo(0);
        assertThat(second.getInFlightMedian()).isEqualTo(0);
        assertThat(second.getInFlightSpread()).isEqualTo(0);
        assertThat(second.totalOutcomeCount()).isEqualTo(0);
        assertThat(second.successThroughputPerSecond()).isWithin(TOLERANCE).of(0.0);
    }

    @Test
    void discardResetsWithoutProducingAWindow() {
        AdmissionSampleWindow window = new AdmissionSampleWindow();
        window.addServiceTimeSample(10);
        window.recordSuccess();
        window.discard();

        ClosedAdmissionWindow closed = close(window);
        assertThat(closed.getSampleCount()).isEqualTo(0);
        assertThat(closed.totalOutcomeCount()).isEqualTo(0);
    }

    @Test
    void closesEmptyOnTimeBoundWithoutError() {
        // The caller closes on the TIME BOUND with whatever the window has - possibly nothing
        ClosedAdmissionWindow closed = close(new AdmissionSampleWindow());

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

        assertThat(close(first)).isEqualTo(close(second));
    }

    @Test
    void extremeServiceTimesDoNotOverflowTheSum() {
        AdmissionSampleWindow window = new AdmissionSampleWindow();
        window.addServiceTimeSample(Long.MAX_VALUE);
        window.addServiceTimeSample(Long.MAX_VALUE);
        window.addServiceTimeSample(Long.MAX_VALUE);

        ClosedAdmissionWindow closed = close(window);
        // a long-based sum would have wrapped negative; the double sum keeps the mean at the sample value
        assertThat(closed.getMeanServiceTimeNanos()).isGreaterThan(0.0);
        assertThat(closed.getMeanServiceTimeNanos()).isWithin(1e12).of((double) Long.MAX_VALUE);
    }

    // --- useful throughput: successCount over MEASURED elapsed time (R4, the design's Finding 2) ---

    /**
     * Windows drift (an idle consumer produces one 4-second window), so the nominal 1s is a lie: a window that ran
     * TWICE the nominal length with equal successes must report HALF the throughput - elapsed measured, never
     * assumed.
     */
    @Test
    void aWindowRunTwiceTheNominalLengthReportsHalfTheThroughput() {
        AdmissionSampleWindow nominal = new AdmissionSampleWindow();
        AdmissionSampleWindow drifted = new AdmissionSampleWindow();
        for (int i = 0; i < 10; i++) {
            nominal.recordSuccess();
            drifted.recordSuccess();
        }

        ClosedAdmissionWindow nominalClosed = nominal.close(NOMINAL_ELAPSED_NANOS, AdmissionBoundarySignals.UNSAMPLED);
        ClosedAdmissionWindow driftedClosed = drifted.close(2 * NOMINAL_ELAPSED_NANOS, AdmissionBoundarySignals.UNSAMPLED);

        assertThat(nominalClosed.successThroughputPerSecond()).isWithin(TOLERANCE).of(10.0);
        assertWithMessage("equal successes over twice the measured time must read as half the throughput")
                .that(driftedClosed.successThroughputPerSecond()).isWithin(TOLERANCE).of(5.0);
    }

    /**
     * IGNORE and OVERLOAD_DROP outcomes are completions but not USEFUL ones: rate-limit rejections land in these
     * counters, so a total-outcome rate stays high exactly when useful throughput collapses (Finding 2). They must
     * never move the numerator.
     */
    @Test
    void ignoresAndOverloadDropsDoNotMoveTheThroughputNumerator() {
        AdmissionSampleWindow window = new AdmissionSampleWindow();
        for (int i = 0; i < 4; i++) {
            window.recordSuccess();
        }
        for (int i = 0; i < 100; i++) {
            window.recordIgnore();
        }
        for (int i = 0; i < 50; i++) {
            window.recordOverloadDrop();
        }

        ClosedAdmissionWindow closed = close(window);
        assertWithMessage("the collapse must be visible: total outcomes stay high while useful throughput falls")
                .that(closed.totalOutcomeCount()).isEqualTo(154);
        assertThat(closed.successThroughputPerSecond()).isWithin(TOLERANCE).of(4.0);
    }

    /** Zero or negative measured elapsed time yields zero throughput, never a division blow-up. */
    @Test
    void zeroElapsedTimeYieldsZeroThroughput() {
        AdmissionSampleWindow window = new AdmissionSampleWindow();
        window.recordSuccess();

        ClosedAdmissionWindow closed = window.close(0, AdmissionBoundarySignals.UNSAMPLED);
        assertThat(closed.successThroughputPerSecond()).isWithin(TOLERANCE).of(0.0);
    }

    // --- binding classification (KTD1): limit-bound iff active tasks reached the target at the boundary ---

    /**
     * The binding verdict is SLOT saturation: active tasks at the target, whatever the batch fill or sample
     * count - a thin-batch workload that fills every slot reads BOUND, never app-limited.
     */
    @Test
    void activeTasksAtTheTargetClassifiesLimitBoundRegardlessOfBatchFill() {
        AdmissionSampleWindow window = new AdmissionSampleWindow();
        window.recordSuccess(); // deliberately thin: one success, one sample
        window.addServiceTimeSample(5);

        ClosedAdmissionWindow closed = window.close(NOMINAL_ELAPSED_NANOS,
                signals(8, 8, false, 0, 0, false, false));

        assertThat(closed.bindingClassification()).isEqualTo(BindingClassification.LIMIT_BOUND);
        assertThat(closed.isLimitBound()).isTrue();
    }

    @Test
    void notBoundWithNoSelectableWorkAndEmptyBufferClassifiesNoWork() {
        ClosedAdmissionWindow closed = close(new AdmissionSampleWindow(),
                signals(2, 8, false, 0, 0, false, false));

        assertThat(closed.bindingClassification()).isEqualTo(BindingClassification.NO_WORK);
        assertThat(closed.isLimitBound()).isFalse();
    }

    /**
     * Buffered-but-unyieldable: work is present but the shards could not yield it (the ordering-aware upper bound
     * below what dispatch asked) - the slots sat empty because of ORDERING, not absence of work.
     */
    @Test
    void notBoundWithBufferedButUnyieldableWorkClassifiesOrderingStarved() {
        ClosedAdmissionWindow closed = close(new AdmissionSampleWindow(),
                signals(2, 8, true, 0, 40, false, false));

        assertThat(closed.bindingClassification()).isEqualTo(BindingClassification.ORDERING_STARVED);
        assertThat(closed.isLimitBound()).isFalse();
    }

    /**
     * A poller paused for throttling is SELF-inflicted starvation - "must never be read as evidence of anything" -
     * so it wins over the other unbound causes.
     */
    @Test
    void notBoundWithThePollerPausedForThrottlingClassifiesSelfThrottled() {
        ClosedAdmissionWindow closed = close(new AdmissionSampleWindow(),
                signals(2, 8, false, 0, 0, true, false));

        assertThat(closed.bindingClassification()).isEqualTo(BindingClassification.SELF_THROTTLED);
        assertThat(closed.isLimitBound()).isFalse();
    }

    /** Slot saturation wins over every unbound cause: a bound window is bound even while the poller throttles. */
    @Test
    void limitBoundWinsOverSelfThrottled() {
        ClosedAdmissionWindow closed = close(new AdmissionSampleWindow(),
                signals(8, 8, false, 0, 0, true, false));

        assertThat(closed.bindingClassification()).isEqualTo(BindingClassification.LIMIT_BOUND);
    }

    /** The R8 plumbing: offset-encoding back-pressure sampled at the boundary rides the closed window. */
    @Test
    void offsetBackPressureSampledAtTheBoundaryRidesTheClosedWindow() {
        ClosedAdmissionWindow pressured = close(new AdmissionSampleWindow(),
                signals(8, 8, false, 0, 0, false, true));
        ClosedAdmissionWindow clear = close(new AdmissionSampleWindow(),
                signals(8, 8, false, 0, 0, false, false));

        assertThat(pressured.isOffsetBackPressure()).isTrue();
        assertThat(clear.isOffsetBackPressure()).isFalse();
    }

    /** close() stamps the measured elapsed time and the boundary signals onto the closed window unmodified. */
    @Test
    void closeStampsElapsedTimeAndBoundarySignalsOntoTheClosedWindow() {
        AdmissionBoundarySignals boundary = signals(3, 8, true, 5, 9, false, true);

        ClosedAdmissionWindow closed = new AdmissionSampleWindow().close(7_000_000_000L, boundary);

        assertThat(closed.getElapsedNanos()).isEqualTo(7_000_000_000L);
        assertThat(closed.getBoundarySignals()).isEqualTo(boundary);
    }

    // --- helpers ---

    private static ClosedAdmissionWindow close(AdmissionSampleWindow window) {
        return close(window, AdmissionBoundarySignals.UNSAMPLED);
    }

    private static ClosedAdmissionWindow close(AdmissionSampleWindow window, AdmissionBoundarySignals boundary) {
        return window.close(NOMINAL_ELAPSED_NANOS, boundary);
    }

    private static AdmissionBoundarySignals signals(int activeTasks, int targetSlots, boolean dispatchUnderServed,
                                                    long selectableWorkUpperBound, long bufferedShardWork,
                                                    boolean pollerSelfThrottled, boolean offsetBackPressure) {
        return new AdmissionBoundarySignals(activeTasks, targetSlots, dispatchUnderServed,
                selectableWorkUpperBound, bufferedShardWork, pollerSelfThrottled, offsetBackPressure);
    }
}
