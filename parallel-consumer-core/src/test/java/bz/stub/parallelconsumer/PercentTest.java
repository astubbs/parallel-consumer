package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import static bz.stub.parallelconsumer.Percent.percentOf;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The unit, and the four things that are not a percentage.
 * <p>
 * The unit test is the one that matters most: the whole reason a setting takes this type rather than a bare number is
 * that {@code 70} and {@code 0.7} are both plausible spellings of seventy percent, and a caller who picks the wrong
 * one gets a setting that is out by a hundred with nothing to tell them. The refusals matter because a value this
 * type accepts travels on to a setting, and a setting's own check would then have to explain a number that was never
 * a percentage in the first place.
 */
class PercentTest {

    @Test
    void aPercentageIsOutOfAHundredRatherThanAFractionOfOne() {
        assertThat(percentOf(70).percentage()).isEqualTo(70d);
    }

    /**
     * The fraction-of-one spelling of the same quantity is accepted, because it is a legal percentage - it just is
     * not the one the caller meant. Nothing can catch that, which is why the type is named and its javadoc says
     * which unit it is; this pins the behaviour so nobody "fixes" it into a silent multiplication by a hundred.
     */
    @Test
    void theFractionSpellingIsAHundredthOfWhatTheCallerProbablyMeant() {
        assertThat(percentOf(0.7).percentage()).isEqualTo(0.7d);
    }

    @Test
    void aPercentageOfNoneIsRefused() {
        var thrown = assertThrows(IllegalArgumentException.class, () -> percentOf(0));

        assertThat(thrown).hasMessageThat().contains("above zero");
    }

    @Test
    void aNegativePercentageIsRefused() {
        var thrown = assertThrows(IllegalArgumentException.class, () -> percentOf(-5));

        assertThat(thrown).hasMessageThat().contains("above zero");
        assertThat(thrown).hasMessageThat().contains("-5");
    }

    /**
     * The refusal for a value above a hundred names the unit rather than only the bound, because the likeliest way
     * to arrive here is having meant the other spelling and multiplied by a hundred twice.
     */
    @Test
    void aPercentageAboveAHundredIsRefusedAndSaysWhichUnitItIs() {
        var thrown = assertThrows(IllegalArgumentException.class, () -> percentOf(700));

        assertThat(thrown).hasMessageThat().contains("above a hundred");
        assertThat(thrown).hasMessageThat().contains("fraction of one");
        assertThat(thrown).hasMessageThat().contains("700");
    }

    @Test
    void exactlyAHundredPercentIsAPercentage() {
        assertThat(percentOf(100).percentage()).isEqualTo(100d);
    }

    @Test
    void whatIsNotAFiniteNumberIsNotAPercentage() {
        assertThat(assertThrows(IllegalArgumentException.class, () -> percentOf(Double.NaN)))
                .hasMessageThat().contains("NaN");
        assertThat(assertThrows(IllegalArgumentException.class, () -> percentOf(Double.POSITIVE_INFINITY)))
                .hasMessageThat().contains("finite");
        assertThat(assertThrows(IllegalArgumentException.class, () -> percentOf(Double.NEGATIVE_INFINITY)))
                .hasMessageThat().contains("finite");
    }

    /**
     * A refusal quotes the percentage back, so its rendering is part of the message a user reads: the unit is on it,
     * and a whole percentage does not acquire a decimal point it was never typed with.
     */
    @Test
    void itRendersWithItsUnitAndWithoutInventedPrecision() {
        assertThat(percentOf(70).toString()).isEqualTo("70%");
        assertThat(percentOf(12.5).toString()).isEqualTo("12.5%");
    }

    @Test
    void twoPercentagesOfTheSameQuantityAreEqualAndOrderByIt() {
        assertThat(percentOf(70)).isEqualTo(percentOf(70d));
        assertThat(percentOf(70).hashCode()).isEqualTo(percentOf(70d).hashCode());
        assertThat(percentOf(70)).isNotEqualTo(percentOf(71));
        assertThat(percentOf(71).compareTo(percentOf(70))).isGreaterThan(0);
        assertThat(percentOf(70).compareTo(percentOf(71))).isLessThan(0);
        assertThat(percentOf(70).compareTo(percentOf(70))).isEqualTo(0);
    }
}
