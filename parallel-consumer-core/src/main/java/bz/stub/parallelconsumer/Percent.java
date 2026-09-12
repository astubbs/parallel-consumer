package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.annotation.InterfaceStability;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * A percentage, so that a setting taking one says what its number means instead of leaving a bare number for the
 * reader to guess at.
 *
 * <h2>One of these, for the whole library</h2>
 * It lives in core's own package rather than beside the first settings to take one, so that the engine's own
 * percentage-shaped values can take the same type as the fluent API's do (owner-directed, 2026-09-12: "Of course DRY
 * if the engine already has a percentage class then use it of course" - it had none, so this is it). <b>Do not add a
 * second percentage type anywhere in this library</b>; a setting that wants one takes this, and a setting that wants
 * the quantity as a fraction of one converts at the point it needs it.
 *
 * <h2>The unit</h2>
 * <b>It is a percentage out of a hundred, not a fraction of one.</b> {@link #percentOf(double)} takes the number a
 * person says out loud: {@code percentOf(70)} is seventy percent, and the fraction-of-one spelling of the same
 * quantity, {@code percentOf(0.7)}, is seven tenths of one percent instead. Both compile, which is exactly why this
 * type exists and why the unit is stated again on the factory rather than only here - a reader who lands on the call
 * site never opens the class.
 *
 * <h2>What it refuses</h2>
 * A percentage that is not a percentage is refused at construction, so a bad value cannot reach a setting at all,
 * let alone travel on to a later check that would explain it as something else: not a number, infinite, zero or
 * negative, or above a hundred. That is the whole of it. It deliberately knows nothing about what any one setting
 * does with the number - a ceiling that comes from an engine threshold belongs to the setting that has the
 * threshold, because a type that refused values above one engine's threshold could not express that threshold
 * itself.
 *
 * <h2>Under a static import</h2>
 * The factory is named for the call site it is written at, which is where the unit has to be legible:
 * <pre>{@code
 * import static bz.stub.parallelconsumer.Percent.percentOf;
 *
 * definition.withDlqWhenOffsetPayloadReaches(percentOf(70));
 * }</pre>
 * A caller who does not want the ceremony passes the bare {@code double} that every setting taking one of these also
 * accepts, and the setting builds this on their behalf - so the refusals above hold whichever door was used.
 */
/**
 * {@code Evolving} rather than the fluent package's {@code Unstable}, matching {@link ParallelConsumerOptions} - the
 * type on the classic surface a user holds settings in. Being reachable from both surfaces makes it part of the
 * shipped API, so it says what it means to be changed rather than borrowing the incubating package's licence.
 */
@InterfaceStability.Evolving
public final class Percent implements Comparable<Percent> {

    /**
     * The largest percentage there is, named rather than left as a bare literal in the refusal it produces, so the
     * bound and the sentence explaining it cannot drift apart.
     */
    private static final double A_HUNDRED = 100d;

    /**
     * The percentage out of a hundred, held as a {@code double} because the settings that take one are no longer
     * whole-numbered and a percentage of a byte budget has no reason to be. Final and validated by the only
     * constructor's caller, so an instance is a percentage for the whole of its life.
     */
    private final double percentage;

    /**
     * Private, so {@link #percentOf(double)} is the only way in and therefore the only place the refusals have to
     * live. Nothing may construct one that skipped them.
     */
    private Percent(double percentage) {
        this.percentage = percentage;
    }

    /**
     * A percentage <b>out of a hundred</b>: {@code percentOf(70)} is seventy percent. It is not a fraction of one -
     * {@code percentOf(0.7)} is seven tenths of one percent, which is a hundredth of what a reader who expected the
     * fraction spelling meant.
     * <p>
     * Written to read as a sentence at the call site under a static import, which is the form it is meant to be used
     * in: {@code withDlqWhenOffsetPayloadReaches(percentOf(70))}.
     *
     * @param percentage the percentage out of a hundred - above zero, at most a hundred, and a real number
     * @throws IllegalArgumentException if that is not a percentage: not a finite number, zero or negative, or above
     *                                  a hundred
     */
    public static Percent percentOf(double percentage) {
        if (!Double.isFinite(percentage)) {
            throw new IllegalArgumentException(msg("percentOf({}) is not a percentage - supply a finite number out "
                    + "of a hundred, so percentOf(70) for seventy percent", render(percentage)));
        }
        if (percentage <= 0) {
            throw new IllegalArgumentException(msg("percentOf({}) must be above zero - a setting that fires at none "
                    + "of something fires on nothing, and leaving the setting out is how it is turned off",
                    render(percentage)));
        }
        if (percentage > A_HUNDRED) {
            throw new IllegalArgumentException(msg("percentOf({}) is above a hundred percent - the value is a "
                    + "percentage out of a hundred rather than a fraction of one, so seventy percent is "
                    + "percentOf(70)", render(percentage)));
        }
        return new Percent(percentage);
    }

    /**
     * The percentage out of a hundred, as it was declared: {@code percentOf(70).percentage()} is seventy.
     *
     * @return the percentage out of a hundred, above zero and at most a hundred
     */
    public double percentage() {
        return percentage;
    }

    /**
     * Ordered by the quantity, so a setting can hold a ceiling as one of these and compare against it rather than
     * unwrapping both sides to {@code double} at the comparison and losing the unit there.
     */
    @Override
    public int compareTo(Percent other) {
        return Double.compare(percentage, other.percentage);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Percent)) {
            return false;
        }
        return Double.compare(percentage, ((Percent) other).percentage) == 0;
    }

    @Override
    public int hashCode() {
        return Double.hashCode(percentage);
    }

    /**
     * Carries the unit, because this is what a refusal message quotes back at the user: a bare number there would
     * leave the reader with the same ambiguity the type exists to remove.
     */
    @Override
    public String toString() {
        return render(percentage) + "%";
    }

    /**
     * A whole percentage renders whole. Every percentage a user is likely to type is whole, and {@code 70.0} in a
     * refusal reads as though the setting had a precision it was fussy about.
     */
    private static String render(double percentage) {
        return percentage == Math.rint(percentage) && !Double.isInfinite(percentage)
                ? Long.toString((long) percentage)
                : Double.toString(percentage);
    }
}
