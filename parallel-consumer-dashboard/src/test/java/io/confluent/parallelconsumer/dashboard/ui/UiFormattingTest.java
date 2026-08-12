package io.confluent.parallelconsumer.dashboard.ui;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The page's shared formatters - {@code ui.js} - executed as the browser executes them.
 *
 * <h2>Why formatting is worth a gating test</h2>
 * <p>
 * A formatting bug is the quietest failure this page can have. The layout is intact, the panel is populated, the
 * number is plausible - it is simply not the number the instance reported. The two that matter most here are the
 * ones that guard precision: {@code formatOffset} must print the string that came off the wire character for
 * character, and {@code formatBigCount} must refuse anything that is not already a {@code BigInt}, because a count
 * that reached it as a {@code Number} has already lost whatever it was going to lose.
 *
 * <h2>One thread, deliberately</h2>
 * <p>
 * A GraalVM context may not be entered from a second thread, and this repository runs JUnit methods
 * concurrently by default - so without {@code SAME_THREAD} the suite fails with a thread-access error that
 * says nothing about the page. One context per class, used by that class's methods in turn.
 */
@Execution(ExecutionMode.SAME_THREAD)
class UiFormattingTest {

    private static PageModules page;

    private static Value ui;

    @BeforeAll
    static void loadTheModules() {
        page = PageModules.open();
        ui = page.module(PageModules.UI);
    }

    @AfterAll
    static void closeTheEngine() {
        if (page != null) {
            page.close();
        }
    }

    /**
     * An offset is printed as the string the server sent, never re-derived from the number the geometry uses. The
     * value below has nineteen digits - near the top of the {@code long} range and far past what a {@code double}
     * can hold - so any round trip through a {@code Number} would show.
     */
    @Test
    void anOffsetIsPrintedCharacterForCharacter() {
        String enormous = "9223372036854775806";

        assertThat(call("formatOffset", enormous)).isEqualTo(enormous);
    }

    @Test
    void anAbsentOffsetRendersAsADashRatherThanZero() {
        assertThat(call("formatOffset", (Object) null)).isEqualTo("-");
    }

    /**
     * A count held as a {@code BigInt} prints exactly, however wide. Grouped for reading - but the separator belongs
     * to the reader's locale, so the assertion pins the digits and the fact that grouping happened, not the
     * character in between.
     */
    @Test
    void aBigCountPrintsEveryDigitAndIsGroupedForReading() {
        Value huge = page.bigInt("9007199254740993");

        String formatted = ui.getMember("formatBigCount").execute(huge).asString();

        assertThat(formatted.replaceAll("[^0-9]", "")).isEqualTo("9007199254740993");
        assertThat(formatted).as("a sixteen-digit count is unreadable ungrouped").isNotEqualTo("9007199254740993");
    }

    /**
     * Anything that is not a {@code BigInt} is refused outright. That is the point: a count that arrived as a
     * {@code Number} has already been rounded, and printing it would present a rounded value as an exact one.
     */
    @Test
    void aBigCountRefusesAValueThatIsNotABigInt() {
        assertThat(call("formatBigCount", 1006)).isEqualTo("-");
        assertThat(call("formatBigCount", "1006")).isEqualTo("-");
    }

    @Test
    void aMissingCountRendersAsADashAndAMeasuredZeroAsZero() {
        assertThat(call("formatCount", (Object) null)).isEqualTo("-");
        assertThat(call("formatCount", 0)).isEqualTo("0");
    }

    @Test
    void durationsAreShortEnoughToSitInACell() {
        assertThat(call("formatDuration", 4000)).isEqualTo("4s");
        assertThat(call("formatDuration", 59_400)).isEqualTo("59s");
        assertThat(call("formatDuration", 125_000)).isEqualTo("2m 05s");
        assertThat(call("formatDuration", 4_320_000)).isEqualTo("1h 12m");
        assertThat(call("formatDuration", (Object) null)).isEqualTo("-");
    }

    /**
     * Interpolation treats absence as absence. A value that is null in either sample is not tweened towards zero,
     * because a missing reading and a measured zero are different facts and a chart that merged them would draw a
     * confident line through a hole in the data.
     */
    @Test
    void interpolationDoesNotTweenAnAbsentReadingTowardsZero() {
        Value interpolate = ui.getMember("interpolateNumber");

        assertThat(interpolate.execute(10, 20, 0.5).asDouble()).isEqualTo(15d);
        assertThat(interpolate.execute(null, 20, 0.5).asDouble())
                .as("no previous sample means jump to the current one, not rise from zero")
                .isEqualTo(20d);
        assertThat(interpolate.execute(10, null, 0.5).isNull())
                .as("a reading that has gone missing stays missing")
                .isTrue();
    }

    @Test
    void anUnparseableOffsetBecomesAbsenceRatherThanThrowing() {
        Value toBigInt = ui.getMember("toBigInt");

        assertThat(page.stringify(toBigInt.execute("42"))).isEqualTo("42");
        assertThat(toBigInt.execute("").isNull()).isTrue();
        assertThat(toBigInt.execute("not-an-offset").isNull()).isTrue();
    }

    /**
     * The number used for pixels is explicitly approximate, and is never allowed to be the value a reader is shown.
     * This pins that it is a {@code Number}, so the distinction between the two forms stays visible in the tests as
     * well as in the source.
     */
    @Test
    void theGeometryFormOfAnOffsetIsANumber() {
        Value geometry = ui.getMember("toGeometryNumber").execute("9007199254740993");

        assertThat(geometry.fitsInDouble()).isTrue();
        assertThat(ui.getMember("toGeometryNumber").execute((Object) null).isNull()).isTrue();
    }

    private static String call(String function, Object argument) {
        return ui.getMember(function).execute(argument).asString();
    }
}
