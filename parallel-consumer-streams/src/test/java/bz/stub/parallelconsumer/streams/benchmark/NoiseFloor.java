package bz.stub.parallelconsumer.streams.benchmark;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Getter;

import java.util.Locale;

/**
 * How far apart two arms that did <b>byte-identical work</b> came out on this machine, on this run.
 * <p>
 * <b>Every ratio in a benchmark report is meaningless until this number is beside it.</b> A pair of stock arms
 * differ by scheduling, page cache, JIT state and whatever else the machine was doing, and that difference is
 * an upper bound on how much a ratio can move without any change in the thing being measured. Measured on the
 * example rung (astubbs/parallel-consumer#391), two stock arms once differed by more than the control arm's
 * own headline number - so the run that looked like a finding had measured nothing, and nothing on screen said
 * so.
 * <p>
 * It has to be measured <b>inside the run that quotes it</b>. A floor from yesterday, or from another machine,
 * describes a different machine in a different state, which is the same mistake as copying a figure.
 * <p>
 * {@link #beatenBy(double)} is what an assertion should ask, rather than "did the ratio come out above one".
 * A direction check is a sound test of an arm that loses and a coin toss for an arm at parity - it fires on
 * roughly half of all healthy runs - and a bare "greater than one" is satisfied by a sabotaged run that has
 * lost the entire mechanism, which is how a broken run once printed the headline claim about itself.
 *
 * @author Antony Stubbs
 * @see NoiseFloorTest
 */
@Getter
public final class NoiseFloor {

    /**
     * How far outside the floor a ratio has to sit before it is worth believing.
     * <p>
     * Chosen so that the empty space between a sabotaged run (which lands at parity, inside any floor) and a
     * real result (which is several-fold) is where the threshold lives, rather than at either edge. A margin
     * of one would accept anything a bad run could reach; a margin large enough to reject a real result would
     * make the check unusable. This is not tuned to a measurement - if a real result ever needs it lowered,
     * the result is not outside the noise and the honest move is to say so.
     */
    public static final double MARGIN = 1.5d;

    private final double firstArm;

    private final double secondArm;

    private NoiseFloor(final double firstArm, final double secondArm) {
        this.firstArm = firstArm;
        this.secondArm = secondArm;
    }

    /**
     * @param firstArm  a statistic from one arm
     * @param secondArm the same statistic from a second arm that did identical work
     * @throws IllegalArgumentException if either is not positive - a zero or negative statistic means the arm
     *                                  did not run, and dividing by it would manufacture a floor of infinity or
     *                                  zero, either of which silently disables every check built on it
     */
    public static NoiseFloor between(final double firstArm, final double secondArm) {
        if (firstArm <= 0d || secondArm <= 0d || Double.isNaN(firstArm) || Double.isNaN(secondArm)) {
            throw new IllegalArgumentException("A noise floor needs two positive statistics from two arms that "
                    + "did identical work; got " + firstArm + " and " + secondArm + ". A non-positive arm did "
                    + "not run, and a floor derived from one disables every check that reads it.");
        }
        return new NoiseFloor(firstArm, secondArm);
    }

    /**
     * The floor itself: the larger arm over the smaller, so it is always at least 1.0 and is read in the same
     * units as the ratios it bounds.
     */
    public double getRatio() {
        return Math.max(firstArm, secondArm) / Math.min(firstArm, secondArm);
    }

    /** The smallest ratio this run is entitled to call a result. */
    public double getThreshold() {
        return getRatio() * MARGIN;
    }

    /**
     * @return whether an observed ratio is far enough outside the floor to be a result rather than variance
     */
    public boolean beatenBy(final double observedRatio) {
        return observedRatio > getThreshold();
    }

    /**
     * The row this belongs on in a report, phrased so a reader who reads nothing else knows what to divide by.
     */
    public String describe() {
        return String.format(Locale.ROOT,
                "%.2fx between two arms doing identical work - read every ratio against this, and treat "
                        + "anything below %.2fx as variance",
                getRatio(), getThreshold());
    }
}
