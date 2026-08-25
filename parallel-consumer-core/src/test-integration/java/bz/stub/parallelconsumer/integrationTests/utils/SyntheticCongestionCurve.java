package bz.stub.parallelconsumer.integrationTests.utils;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * A synthetic downstream's service-time curve: flat below a knee, rising as a power of the overshoot above it.
 * <pre>
 *     serviceTime = base * max(1, inFlight / knee)^exponent
 * </pre>
 * The input is always <b>the concurrency the downstream itself observes</b> - callers count invocations in and
 * out of the user function - never the admission controller's target. A real downstream does not know what the
 * controller decided; it only feels how many callers arrived, and every adaptive-concurrency integration test in
 * this package depends on that distinction (a curve driven by the target would close the loop through the very
 * variable under test).
 * <p>
 * The exponent selects which downstream pathology the curve models, and the difference is load-bearing for what
 * a test can assert:
 * <ul>
 * <li><b>{@link #quadratic}</b> ({@code exponent = 2}) - congestion collapse: total throughput
 * {@code inFlight / serviceTime} <em>falls</em> as {@code knee^2 / (base * inFlight)} above the knee, so
 * over-driving costs throughput, not just latency. This is the shape that makes a hand-tuned-too-high arm
 * measurably worse end-to-end, and the shape {@link
 * bz.stub.parallelconsumer.integrationTests.AdaptiveConcurrencyClosedLoopIT} has always used.</li>
 * <li><b>{@link #linear}</b> ({@code exponent = 1}) - graceful saturation: throughput plateaus exactly flat at
 * {@code knee / base} above the knee while latency climbs linearly. No fall, no rejection - the plateau the
 * HOLD band and the descent probe exist for, and the one shape where over-driving costs <em>nothing</em> in
 * throughput (which is why it cannot carry a phase whose assertion needs the static arm to lose ground).</li>
 * </ul>
 * Immutable; a test that switches phases swaps which instance a volatile field holds.
 */
public final class SyntheticCongestionCurve {

    private final long baseServiceTimeMillis;
    private final int kneeInFlight;
    private final double exponent;

    private SyntheticCongestionCurve(long baseServiceTimeMillis, int kneeInFlight, double exponent) {
        if (baseServiceTimeMillis <= 0 || kneeInFlight <= 0) {
            throw new IllegalArgumentException("base service time and knee must both be positive");
        }
        this.baseServiceTimeMillis = baseServiceTimeMillis;
        this.kneeInFlight = kneeInFlight;
        this.exponent = exponent;
    }

    /** Graceful saturation: flat throughput plateau of {@code knee / base} above the knee. */
    public static SyntheticCongestionCurve linear(long baseServiceTimeMillis, int kneeInFlight) {
        return new SyntheticCongestionCurve(baseServiceTimeMillis, kneeInFlight, 1.0);
    }

    /** Congestion collapse: throughput falls as {@code knee^2 / (base * inFlight)} above the knee. */
    public static SyntheticCongestionCurve quadratic(long baseServiceTimeMillis, int kneeInFlight) {
        return new SyntheticCongestionCurve(baseServiceTimeMillis, kneeInFlight, 2.0);
    }

    /** The service time this downstream serves at the given observed concurrency. */
    public long serviceTimeMillis(int inFlight) {
        double overshoot = Math.max(1.0, inFlight / (double) kneeInFlight);
        return Math.round(baseServiceTimeMillis * Math.pow(overshoot, exponent));
    }

    /**
     * The downstream's maximum useful throughput in records/second - the Little's-Law oracle each phase of the
     * comparison IT asserts against. At or above the knee, {@code knee / base} for every exponent; the linear
     * curve holds that value flat however far above the knee the caller drives it, the quadratic falls away.
     */
    public double capacityPerSecond() {
        return kneeInFlight * 1000.0 / baseServiceTimeMillis;
    }

    public int kneeInFlight() {
        return kneeInFlight;
    }

    public long baseServiceTimeMillis() {
        return baseServiceTimeMillis;
    }

    @Override
    public String toString() {
        return String.format("SyntheticCongestionCurve(base=%dms, knee=%d, exponent=%.1f, capacity=%.0f/s)",
                baseServiceTimeMillis, kneeInFlight, exponent, capacityPerSecond());
    }
}
