package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * The one statistic the admission law reads three ways (the design's R1/KTD1): the elasticity of useful
 * throughput against concurrency, estimated as the least-squares slope of {@code log(success throughput)} on
 * {@code log(active slots)} over the limit-bound windows within a wall-clock horizon.
 * <p>
 * <b>Pure, clock-free, deterministic.</b> The estimator owns no clock: each entry arrives stamped with its
 * window-close instant, and the eviction horizon is measured relative to the NEWEST entry's instant, never to
 * "now". Identical offer sequences produce identical verdicts.
 * <p>
 * <b>What enters the history (R2, KTD3).</b> Only windows that are BOTH limit-bound (slot saturation - the
 * window's own {@link ClosedAdmissionWindow#isLimitBound()} verdict) AND adjudicated by the caller (the law
 * discards cooldown, pause-poisoned and sample-starved windows - that adjudication happens outside this class,
 * so the caller asserts it per offer). Everything else is refused, leaving history and verdict untouched:
 * <ul>
 * <li><b>not adjudicated</b> - KTD3: discarded windows must never teach the estimator;</li>
 * <li><b>not limit-bound</b> - R2: an unbound window's throughput says nothing about the limit;</li>
 * <li><b>zero or negative throughput</b> - {@code log(y)} is undefined at or below zero, and a zero-success
 * window carries no elasticity information anyway;</li>
 * <li><b>zero or negative active slots</b> - {@code log(0)} is undefined. Unreachable by construction today
 * ({@link ClosedAdmissionWindow#bindingClassification()} requires {@code activeTasks >= targetSlots > 0} to
 * read limit-bound), kept as a defensive guard so the arithmetic can never see it.</li>
 * </ul>
 * <p>
 * <b>The x input is the boundary-sampled active-task count</b>
 * ({@link AdmissionBoundarySignals#getActiveTasks()}). KTD1 words the input as the active-task window median,
 * but the closed window carries only the boundary snapshot of active tasks (its {@code inFlightMedian} is the
 * in-flight snapshot median, a different accounting) - so the boundary snapshot is the x, per the unit contract.
 * Denomination is slots throughout: no batch-size term exists anywhere in this class (KTD1).
 * <p>
 * <b>Verdict persistence (KTD1, load-bearing).</b> A computed verdict REMAINS IN FORCE until replaced by a new
 * qualifying computation or killed by {@link #invalidate}. Horizon eviction alone never revokes it - a
 * controller holding correctly still at the knee must not self-evict into growth. After invalidation the
 * verdict is dead and {@link Band#INSUFFICIENT_SIGNAL} reigns until fresh qualifying signal accumulates.
 * <p>
 * <b>Why the output can never be NaN or infinite:</b> every entry has {@code x >= 1} and {@code y > 0} (the
 * refusals above), so both logs are finite; and a computation requires in-flight spread of at least
 * {@link #minSpreadSlots} ({@code >= 1}, constructor-enforced), so at least two distinct x values exist and
 * the regression denominator is strictly positive.
 */
@Slf4j
public class AdmissionElasticityEstimator {

    /** Diminishing-returns ratio the RISE threshold derives from (the design's r=3, settled). */
    static final int DIMINISHING_RETURNS_RATIO = 3;

    /**
     * RISE requires elasticity above {@code 1/(r+1)} with r={@value #DIMINISHING_RETURNS_RATIO}: growth must
     * still buy proportionally more throughput. Between this and zero is HOLD; below zero is FALL.
     */
    static final double RISE_THRESHOLD = 1.0 / (DIMINISHING_RETURNS_RATIO + 1);

    /**
     * Slopes with magnitude below this are snapped to exactly {@code 0.0} before banding. The HOLD/FALL
     * boundary sits at zero, and perfectly flat throughput computes to a slope of about {@code ±1e-16} - pure
     * floating-point summation noise - which would otherwise band a textbook plateau as FALL. This is a
     * numeric guard, not a tolerance on the law: FALL means MEASURABLY negative.
     */
    static final double ZERO_SLOPE_NOISE_FLOOR = 1e-9;

    static final Duration DEFAULT_HORIZON = Duration.ofSeconds(60);
    static final int DEFAULT_MIN_ENTRIES = 8;
    static final int DEFAULT_MIN_SPREAD_SLOTS = 1;

    /** How the law reads the statistic - three bands plus the no-verdict state. */
    public enum Band {
        /** Elasticity above {@link #RISE_THRESHOLD}: more concurrency is still buying proportionally more. */
        RISE,
        /** Elasticity in {@code [0, threshold]}: growth stopped paying but is not yet destructive. */
        HOLD,
        /** Elasticity below zero: more concurrency bought less work. */
        FALL,
        /** No verdict in force - cold start, post-invalidation, or signal below minimum. Never a band value. */
        INSUFFICIENT_SIGNAL,
    }

    /** Why a verdict and its history were killed - carried for the log only. */
    public enum InvalidationReason {
        PAUSE,
        ESCAPE_PROBE_CLEAR,
        REBALANCE,
    }

    /**
     * The estimator's answer. A computed verdict ({@code live == true}) carries the raw elasticity and the
     * instant of the newest entry in the regression that produced it. The {@link #INSUFFICIENT} sentinel is the
     * only non-live verdict ever returned: its elasticity is a meaningless {@code 0.0} and its
     * {@code computedAt} is null - read the band first.
     */
    @Value
    public static class Verdict {
        /** The no-verdict sentinel: what {@link #verdict()} returns until a computation qualifies. */
        public static final Verdict INSUFFICIENT = new Verdict(Band.INSUFFICIENT_SIGNAL, 0.0, null, false);

        Band band;
        double elasticity;
        Instant computedAt;
        boolean live;
    }

    /** One accepted window, distilled to the regression's inputs. */
    @Value
    private static class Entry {
        Instant closeInstant;
        int activeSlots;
        double successThroughputPerSecond;
    }

    private final Duration horizon;
    private final int minEntries;
    private final int minSpreadSlots;

    private final List<Entry> history = new ArrayList<>();
    /** Running max of accepted close instants since the last invalidation - the horizon's reference point. */
    private Instant newestInstant;
    /** The verdict in force; null means {@link Verdict#INSUFFICIENT} reigns. */
    private Verdict liveVerdict;

    public AdmissionElasticityEstimator() {
        this(DEFAULT_HORIZON, DEFAULT_MIN_ENTRIES, DEFAULT_MIN_SPREAD_SLOTS);
    }

    public AdmissionElasticityEstimator(Duration horizon, int minEntries, int minSpreadSlots) {
        if (horizon == null || horizon.isZero() || horizon.isNegative()) {
            throw new IllegalArgumentException("horizon must be positive: " + horizon);
        }
        if (minEntries < 2) {
            throw new IllegalArgumentException("minEntries must be at least 2 for a slope: " + minEntries);
        }
        if (minSpreadSlots < 1) {
            // spread >= 1 guarantees two distinct x values, which is what makes the slope always finite
            throw new IllegalArgumentException("minSpreadSlots must be at least 1: " + minSpreadSlots);
        }
        this.horizon = horizon;
        this.minEntries = minEntries;
        this.minSpreadSlots = minSpreadSlots;
    }

    /**
     * Offers one closed window to the history. Refusals (documented on the class) leave both history and
     * verdict untouched. An accepted entry evicts anything older than {@link #horizon} relative to the newest
     * entry's instant, then attempts a fresh verdict computation - which replaces the live verdict only when
     * the minimum signal holds.
     *
     * @param closeInstant the instant the caller's clock closed the window (the estimator has no clock)
     * @param window       the closed aggregates; the entry is distilled from its boundary active tasks and
     *                     {@link ClosedAdmissionWindow#successThroughputPerSecond()}
     * @param adjudicated  the caller's assertion that the law accepted this window (not cooldown-discarded,
     *                     pause-poisoned or sample-starved - KTD3)
     * @return whether the entry entered the history
     */
    public boolean offer(Instant closeInstant, ClosedAdmissionWindow window, boolean adjudicated) {
        Objects.requireNonNull(closeInstant, "closeInstant");
        Objects.requireNonNull(window, "window");
        if (!adjudicated) {
            log.trace("Refusing unadjudicated window (KTD3)");
            return false;
        }
        if (!window.isLimitBound()) {
            log.trace("Refusing window: not limit-bound ({}) (R2)", window.bindingClassification());
            return false;
        }
        int activeSlots = window.getBoundarySignals().getActiveTasks();
        if (activeSlots <= 0) {
            // unreachable while limit-bound requires activeTasks >= targetSlots > 0; guarded so log(x) is safe
            log.warn("Refusing limit-bound window with non-positive active slots {} - log undefined", activeSlots);
            return false;
        }
        double throughput = window.successThroughputPerSecond();
        if (throughput <= 0) {
            log.trace("Refusing window with non-positive success throughput {} - log undefined", throughput);
            return false;
        }

        history.add(new Entry(closeInstant, activeSlots, throughput));
        if (newestInstant == null || closeInstant.isAfter(newestInstant)) {
            newestInstant = closeInstant;
        }
        evictBeyondHorizon();
        recompute();
        return true;
    }

    /**
     * The verdict in force: the last qualifying computation, or {@link Verdict#INSUFFICIENT} when none is
     * (cold start, post-{@link #invalidate}). Horizon eviction never demotes a live verdict (KTD1).
     */
    public Verdict verdict() {
        return liveVerdict != null ? liveVerdict : Verdict.INSUFFICIENT;
    }

    /**
     * Kills the live verdict AND every current entry (KTD3): pause, escape-probe clear and rebalance
     * reconstruction all describe a plant discontinuity that makes both the history and its conclusion stale.
     * {@link Band#INSUFFICIENT_SIGNAL} reigns until fresh qualifying signal accumulates.
     */
    public void invalidate(InvalidationReason reason) {
        Objects.requireNonNull(reason, "reason");
        log.debug("Invalidating elasticity history ({} entries) and verdict ({}) - reason: {}",
                history.size(), verdict().getBand(), reason);
        history.clear();
        newestInstant = null;
        liveVerdict = null;
    }

    /** Number of entries currently in the history - eviction and refusal are observable through this. */
    public int historySize() {
        return history.size();
    }

    /** Drops entries strictly older than {@link #horizon} relative to the newest entry's instant - never "now". */
    private void evictBeyondHorizon() {
        Instant cutoff = newestInstant.minus(horizon);
        for (Iterator<Entry> it = history.iterator(); it.hasNext(); ) {
            if (it.next().getCloseInstant().isBefore(cutoff)) {
                it.remove();
            }
        }
    }

    /**
     * Recomputes the slope of {@code log(y)} on {@code log(x)} and installs it as the live verdict - but ONLY
     * when the minimum signal holds: at least {@link #minEntries} entries AND in-flight spread
     * ({@code max(x) - min(x)}) of at least {@link #minSpreadSlots}. Otherwise the previous verdict stays in
     * force untouched (KTD1 persistence).
     */
    private void recompute() {
        if (history.size() < minEntries) {
            return;
        }
        int minX = Integer.MAX_VALUE;
        int maxX = Integer.MIN_VALUE;
        for (Entry entry : history) {
            minX = Math.min(minX, entry.getActiveSlots());
            maxX = Math.max(maxX, entry.getActiveSlots());
        }
        if (maxX - minX < minSpreadSlots) {
            return;
        }

        int n = history.size();
        double meanLogX = 0;
        double meanLogY = 0;
        for (Entry entry : history) {
            meanLogX += Math.log(entry.getActiveSlots());
            meanLogY += Math.log(entry.getSuccessThroughputPerSecond());
        }
        meanLogX /= n;
        meanLogY /= n;

        double covariance = 0;
        double varianceX = 0;
        for (Entry entry : history) {
            double dx = Math.log(entry.getActiveSlots()) - meanLogX;
            double dy = Math.log(entry.getSuccessThroughputPerSecond()) - meanLogY;
            covariance += dx * dy;
            varianceX += dx * dx;
        }
        double elasticity = covariance / varianceX;
        if (Math.abs(elasticity) < ZERO_SLOPE_NOISE_FLOOR) {
            elasticity = 0.0; // summation noise, not a measured slope - see ZERO_SLOPE_NOISE_FLOOR
        }
        if (!Double.isFinite(elasticity)) {
            // the refusals and the spread gate make this unreachable; fail loudly rather than emit poison
            throw new IllegalStateException("Elasticity computation produced " + elasticity
                    + " from " + n + " entries with x spread [" + minX + ", " + maxX + "]");
        }
        liveVerdict = new Verdict(bandOf(elasticity), elasticity, newestInstant, true);
    }

    private static Band bandOf(double elasticity) {
        if (elasticity > RISE_THRESHOLD) {
            return Band.RISE;
        }
        if (elasticity >= 0) {
            return Band.HOLD;
        }
        return Band.FALL;
    }
}
