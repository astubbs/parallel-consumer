package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.admission.AdmissionElasticityEstimator.Band;
import bz.stub.parallelconsumer.internal.admission.AdmissionElasticityEstimator.InvalidationReason;
import bz.stub.parallelconsumer.internal.admission.AdmissionElasticityEstimator.Verdict;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The estimator is pure and clock-free, so every scenario drives it with explicit instants and fabricated
 * {@link ClosedAdmissionWindow}s and asserts exact verdict state - no time, no threads, no tolerance beyond
 * the fabrication's own quantisation.
 */
class AdmissionElasticityEstimatorTest {

    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    /** Throughput quantisation of {@link #limitBound}: successes per unit of throughput. */
    private static final long THROUGHPUT_SCALE = 1_000_000L;

    private final AdmissionElasticityEstimator estimator = new AdmissionElasticityEstimator();

    // ------------------------------------------------------------------ the three bands

    @Test
    void risingThroughputWithRisingInFlightReadsRise() {
        // y proportional to x -> elasticity ~1, well above the 0.25 threshold
        offerSweep(estimator, T0, x -> 100.0 * x);

        Verdict verdict = estimator.verdict();
        assertThat(verdict.getBand()).isEqualTo(Band.RISE);
        assertThat(verdict.isLive()).isTrue();
        assertThat(verdict.getElasticity()).isWithin(0.001).of(1.0);
        assertThat(verdict.getComputedAt()).isEqualTo(T0.plusSeconds(7));
    }

    @Test
    void flatThroughputWithRisingInFlightReadsHold() {
        // y constant while x rises -> elasticity 0: growth stopped paying, not yet destructive. Exactly zero,
        // because the estimator snaps sub-noise-floor slopes - summation noise must not band a plateau as FALL
        offerSweep(estimator, T0, x -> 500.0);

        Verdict verdict = estimator.verdict();
        assertThat(verdict.getBand()).isEqualTo(Band.HOLD);
        assertThat(verdict.getElasticity()).isEqualTo(0.0);
    }

    @Test
    void subThresholdGrowthReadsHold() {
        // y = x^0.1 -> elasticity ~0.1, inside (0, 0.25] - the graceful-saturation plateau the HOLD band exists for
        offerSweep(estimator, T0, x -> Math.pow(x, 0.1));

        Verdict verdict = estimator.verdict();
        assertThat(verdict.getBand()).isEqualTo(Band.HOLD);
        assertThat(verdict.getElasticity()).isWithin(0.001).of(0.1);
    }

    @Test
    void fallingThroughputWithRisingInFlightReadsFall() {
        // y inversely proportional to x -> elasticity ~-1: more concurrency bought less work
        offerSweep(estimator, T0, x -> 1000.0 / x);

        Verdict verdict = estimator.verdict();
        assertThat(verdict.getBand()).isEqualTo(Band.FALL);
        assertThat(verdict.getElasticity()).isWithin(0.001).of(-1.0);
    }

    // ------------------------------------------------------------------ refusals (R2, KTD3, log guards)

    @Test
    void unboundWindowsAreRefused() {
        boolean accepted = estimator.offer(T0, unbound(5, 100), true);

        assertThat(accepted).isFalse();
        assertThat(estimator.historySize()).isEqualTo(0);
        assertThat(estimator.verdict()).isEqualTo(Verdict.INSUFFICIENT);
    }

    @Test
    void unadjudicatedWindowsAreRefused() {
        boolean accepted = estimator.offer(T0, limitBound(5, 100), false);

        assertThat(accepted).isFalse();
        assertThat(estimator.historySize()).isEqualTo(0);
    }

    @Test
    void zeroThroughputWindowsAreRefused() {
        // log(0) is undefined; a zero-success window carries no elasticity signal
        boolean accepted = estimator.offer(T0, limitBound(5, 0), true);

        assertThat(accepted).isFalse();
        assertThat(estimator.historySize()).isEqualTo(0);
    }

    // A limit-bound window with active slots <= 0 cannot be constructed: bindingClassification() requires
    // activeTasks >= targetSlots > 0 to read LIMIT_BOUND, so the x=0 guard in offer() is defensive-only and
    // untestable through the window API - documented on the estimator instead of asserted here.

    @Test
    void refusedWindowsLeaveAnEstablishedVerdictUntouched() {
        offerSweep(estimator, T0, x -> 100.0 * x);
        Verdict before = estimator.verdict();

        estimator.offer(T0.plusSeconds(8), unbound(9, 900), true);
        estimator.offer(T0.plusSeconds(9), limitBound(9, 900), false);

        assertThat(estimator.historySize()).isEqualTo(8);
        assertThat(estimator.verdict()).isEqualTo(before);
    }

    // ------------------------------------------------------------------ minimum signal

    @Test
    void zeroVarianceInFlightHistoryStaysInsufficient() {
        // plenty of entries, but every window at the same slot count: no spread, no slope, never a band
        for (int i = 0; i < 12; i++) {
            assertThat(estimator.offer(T0.plusSeconds(i), limitBound(5, 100 + i * 10), true)).isTrue();
        }

        assertThat(estimator.verdict().getBand()).isEqualTo(Band.INSUFFICIENT_SIGNAL);
        assertThat(estimator.verdict().isLive()).isFalse();
    }

    @Test
    void fewerThanMinEntriesStaysInsufficient() {
        for (int i = 0; i < 7; i++) {
            estimator.offer(T0.plusSeconds(i), limitBound(1 + i, 100 * (1 + i)), true);
        }

        assertThat(estimator.verdict().getBand()).isEqualTo(Band.INSUFFICIENT_SIGNAL);
    }

    // ------------------------------------------------------------------ horizon eviction and persistence (KTD1)

    @Test
    void entriesBeyondHorizonAgeOutRelativeToNewest() {
        offerSweep(estimator, T0, x -> 100.0 * x);
        assertThat(estimator.historySize()).isEqualTo(8);

        // one entry 200s later: everything older than 60s relative to IT is evicted
        estimator.offer(T0.plusSeconds(200), limitBound(4, 400), true);

        assertThat(estimator.historySize()).isEqualTo(1);
    }

    @Test
    void liveVerdictSurvivesEvictionDrivenInsufficiency() {
        offerSweep(estimator, T0, x -> 100.0 * x);
        Verdict established = estimator.verdict();
        assertThat(established.getBand()).isEqualTo(Band.RISE);

        // eviction empties the qualifying signal - but a controller holding correctly at the knee must not
        // self-evict into growth: the verdict stays in force (KTD1)
        estimator.offer(T0.plusSeconds(200), limitBound(4, 400), true);

        assertThat(estimator.historySize()).isEqualTo(1);
        assertThat(estimator.verdict()).isEqualTo(established);
        assertThat(estimator.verdict().isLive()).isTrue();
    }

    @Test
    void verdictReplacedOnlyByNewQualifyingComputation() {
        offerSweep(estimator, T0, x -> 100.0 * x);
        Verdict rise = estimator.verdict();
        assertThat(rise.getBand()).isEqualTo(Band.RISE);

        // a falling sweep far past the horizon: the old entries evict on its first offer, and until the new
        // signal qualifies (8 entries) the RISE verdict persists
        Instant later = T0.plusSeconds(300);
        for (int i = 0; i < 7; i++) {
            estimator.offer(later.plusSeconds(i), limitBound(1 + i, Math.round(1000.0 / (1 + i))), true);
            assertThat(estimator.verdict()).isEqualTo(rise);
        }
        estimator.offer(later.plusSeconds(7), limitBound(8, 125), true);

        Verdict replaced = estimator.verdict();
        assertThat(replaced.getBand()).isEqualTo(Band.FALL);
        assertThat(replaced.getComputedAt()).isEqualTo(later.plusSeconds(7));
    }

    // ------------------------------------------------------------------ invalidation

    @Test
    void invalidationKillsVerdictAndEntries() {
        offerSweep(estimator, T0, x -> 100.0 * x);
        assertThat(estimator.verdict().getBand()).isEqualTo(Band.RISE);

        estimator.invalidate(InvalidationReason.PAUSE);

        assertThat(estimator.historySize()).isEqualTo(0);
        assertThat(estimator.verdict()).isEqualTo(Verdict.INSUFFICIENT);
        assertThat(estimator.verdict().isLive()).isFalse();
    }

    @Test
    void freshQualifyingSignalRequiredAfterInvalidation() {
        offerSweep(estimator, T0, x -> 100.0 * x);
        estimator.invalidate(InvalidationReason.REBALANCE);

        // seven fresh entries: still insufficient - the dead verdict must not resurrect
        Instant resumed = T0.plusSeconds(100);
        for (int i = 0; i < 7; i++) {
            estimator.offer(resumed.plusSeconds(i), limitBound(1 + i, 100 * (1 + i)), true);
            assertThat(estimator.verdict().getBand()).isEqualTo(Band.INSUFFICIENT_SIGNAL);
        }
        estimator.offer(resumed.plusSeconds(7), limitBound(8, 800), true);

        assertThat(estimator.verdict().getBand()).isEqualTo(Band.RISE);
        assertThat(estimator.verdict().getComputedAt()).isEqualTo(resumed.plusSeconds(7));
    }

    // ------------------------------------------------------------------ arithmetic guards

    @Test
    void activeSlotsOfOneAreAccepted() {
        // log(1) = 0 is a legitimate regression point, not a degenerate one
        offerSweep(estimator, T0, x -> 100.0 * x); // sweep starts at x=1

        assertThat(estimator.historySize()).isEqualTo(8);
        assertThat(Double.isFinite(estimator.verdict().getElasticity())).isTrue();
    }

    @Test
    void verdictNeverNaNOrInfinite() {
        // extreme throughput magnitudes across the sweep: the logs stay finite and so does the slope
        double[] extremes = { 1e-6, 1e12, 1e-6, 1e12, 1e-6, 1e12, 1e-6, 1e12 };
        for (int i = 0; i < extremes.length; i++) {
            assertThat(estimator.offer(T0.plusSeconds(i), limitBound(1 + i, extremes[i]), true)).isTrue();
        }

        Verdict verdict = estimator.verdict();
        assertThat(verdict.getBand()).isNotEqualTo(Band.INSUFFICIENT_SIGNAL);
        assertThat(Double.isFinite(verdict.getElasticity())).isTrue();
    }

    // ------------------------------------------------------------------ determinism

    @Test
    void identicalOfferSequencesYieldIdenticalVerdicts() {
        AdmissionElasticityEstimator other = new AdmissionElasticityEstimator();
        offerSweep(estimator, T0, x -> Math.pow(x, 0.4));
        offerSweep(other, T0, x -> Math.pow(x, 0.4));

        // exact double equality: same offers in, same verdict out - bit for bit
        assertThat(estimator.verdict()).isEqualTo(other.verdict());
        assertThat(estimator.verdict().getElasticity()).isEqualTo(other.verdict().getElasticity());
    }

    // ------------------------------------------------------------------ configuration guards

    @Test
    void configurableThresholdsAreHonoured() {
        AdmissionElasticityEstimator small = new AdmissionElasticityEstimator(Duration.ofSeconds(60), 2, 1);
        small.offer(T0, limitBound(1, 100), true);
        small.offer(T0.plusSeconds(1), limitBound(2, 200), true);

        assertThat(small.verdict().getBand()).isEqualTo(Band.RISE);
    }

    @Test
    void degenerateConstructionIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new AdmissionElasticityEstimator(Duration.ZERO, 8, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new AdmissionElasticityEstimator(Duration.ofSeconds(60), 1, 1));
        // minSpreadSlots >= 1 is what makes the slope denominator strictly positive - 0 must be rejected
        assertThrows(IllegalArgumentException.class,
                () -> new AdmissionElasticityEstimator(Duration.ofSeconds(60), 8, 0));
    }

    // ------------------------------------------------------------------ fabrication helpers

    /** Offers the default eight-window sweep: x = 1..8, one second apart, y = f(x); asserts each is accepted. */
    private static void offerSweep(AdmissionElasticityEstimator estimator, Instant start, ThroughputCurve curve) {
        for (int i = 0; i < 8; i++) {
            int x = 1 + i;
            boolean accepted = estimator.offer(start.plusSeconds(i), limitBound(x, curve.at(x)), true);
            assertThat(accepted).isTrue();
        }
    }

    private interface ThroughputCurve {
        double at(int activeSlots);
    }

    /**
     * A limit-bound window whose {@code successThroughputPerSecond()} reproduces {@code throughput} to
     * {@link #THROUGHPUT_SCALE} precision: successes and elapsed scaled together so fractional curves survive
     * the integer success count.
     */
    private static ClosedAdmissionWindow limitBound(int activeSlots, double throughput) {
        long successes = Math.round(throughput * THROUGHPUT_SCALE);
        long elapsedNanos = THROUGHPUT_SCALE * 1_000_000_000L;
        return new ClosedAdmissionWindow(1, 1.0, 1, activeSlots, 0, 0, 0, successes, 0, 0,
                elapsedNanos, TestWindows.boundAt(activeSlots));
    }

    /**
     * A window whose boundary signals read unbound (ORDERING_STARVED: active below target with work buffered)
     * while active tasks and throughput stay POSITIVE - so the limit-bound refusal is the ONLY guard that can
     * reject it. UNSAMPLED signals would carry activeTasks 0 and let the defensive log(0) guard mask a missing
     * limit-bound check, which sabotage-testing caught.
     */
    private static ClosedAdmissionWindow unbound(int activeSlots, long successes) {
        AdmissionBoundarySignals belowTarget =
                new AdmissionBoundarySignals(activeSlots, activeSlots + 1, true, 100, 100, false, false);
        return new ClosedAdmissionWindow(1, 1.0, 1, activeSlots, 0, 0, 0, successes, 0, 0,
                1_000_000_000L, belowTarget);
    }
}
