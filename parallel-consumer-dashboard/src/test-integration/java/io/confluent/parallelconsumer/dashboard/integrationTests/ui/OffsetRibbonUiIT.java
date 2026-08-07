package io.confluent.parallelconsumer.dashboard.integrationTests.ui;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The offset ribbon in a real browser: the panel's structure, and the values it puts on the screen measured against
 * the document they came from.
 *
 * <h2>The assertion that matters most</h2>
 * <p>
 * {@link #theOffsetsOnThePageAreTheOnesInTheDocumentDigitForDigit()} compares the text of every offset cell with the
 * string in the state document. The fixture's offsets sit above 2<sup>53</sup>, so any point in the pipeline that
 * put one through a JavaScript {@code Number} - the fetch, the interpolation, the model, the renderer, the
 * formatter - shows up here as a wrong digit. That is the single trap the whole string-on-the-wire and
 * {@code BigInt}-in-the-page design exists to close, and this is where it is closed end to end.
 */
@Execution(ExecutionMode.SAME_THREAD)
class OffsetRibbonUiIT extends DashboardUiTestBase {

    /**
     * The labels {@code rows.js} prints beside each exact offset. Matched by label rather than by position, so
     * inserting a cell cannot make an assertion silently start reading a different number.
     */
    private static final String COMMITTED = "Last committed";

    private static final String SEQUENTIAL = "Highest sequential success";

    private static final String COMPLETED = "Highest success";

    private static final String SEEN = "Highest seen";

    private static final String INCOMPLETE = "Incomplete offsets";

    @Test
    void everyPartitionInTheDocumentGetsItsOwnRow() {
        String url = serve(busyInstance());
        openPage(url);

        List<Map<String, Object>> rows = partitionRows();

        assertThat(names(rows))
                .containsExactlyInAnyOrder(HUGE_PARTITION, ORDINARY_PARTITION, CAUGHT_UP_PARTITION);
        assertThat(rows).hasSize(stateDocument(url).getJsonArray("partitions").size());
    }

    @Test
    void theOffsetsOnThePageAreTheOnesInTheDocumentDigitForDigit() {
        String url = serve(busyInstance());
        openPage(url);

        JsonArray partitions = stateDocument(url).getJsonArray("partitions");
        List<Map<String, Object>> rows = partitionRows();

        for (int i = 0; i < partitions.size(); i++) {
            JsonObject partition = partitions.getJsonObject(i);
            Map<String, Object> values = valuesOf(rowNamed(rows, partition.getString("key")));

            assertThat(values.get(COMMITTED)).isEqualTo(partition.getString("lastCommittedOffset"));
            assertThat(values.get(SEQUENTIAL))
                    .isEqualTo(partition.getString("highestSequentialSucceededOffset"));
            assertThat(values.get(COMPLETED)).isEqualTo(partition.getString("highestCompletedOffset"));
            assertThat(values.get(SEEN)).isEqualTo(partition.getString("highestSeenOffset"));
        }

        // stated explicitly as well as in the loop, so the failure message names the boundary when it goes
        Map<String, Object> huge = valuesOf(rowNamed(rows, HUGE_PARTITION));
        assertThat(huge.get(COMMITTED))
                .as("an offset one above Number.MAX_SAFE_INTEGER must reach the screen with every digit intact")
                .isEqualTo(Long.toString(ABOVE_SAFE_INTEGER));
        assertThat(huge.get(COMPLETED)).isEqualTo(Long.toString(HUGE_COMPLETED));
        assertThat(huge.get(SEEN)).isEqualTo(Long.toString(HUGE_SEEN));
        assertThat(huge.get(INCOMPLETE)).isEqualTo("7");
    }

    /**
     * The hero number: records finished beyond the point a single-threaded consumer would have stopped at, summed
     * across the assigned partitions. 1,006 from the huge partition and 5 from the ordinary one, which is a sum a
     * {@code double} would get wrong by two.
     */
    @Test
    void theHeroNumberCountsEveryRecordWonPastTheBlock() {
        String url = serve(busyInstance());
        openPage(url);

        String hero = textOf("#panel-offsets .hero-value");

        assertThat(hero).startsWith("+");
        assertThat(digitsOf(hero)).isEqualTo("1011");
    }

    /**
     * The partition doing the most impressive work sits at the top. Rows are re-ordered with the flex {@code order}
     * property rather than by moving nodes, so the order is asserted the same way the page sets it.
     */
    @Test
    void theWidestWonSpanIsDrawnFirst() {
        String url = serve(busyInstance());
        openPage(url);

        List<Map<String, Object>> rows = partitionRows();

        assertThat(rowNamed(rows, HUGE_PARTITION).get("order")).isEqualTo("0");
        assertThat(rowNamed(rows, ORDINARY_PARTITION).get("order")).isEqualTo("1");
        assertThat(rowNamed(rows, CAUGHT_UP_PARTITION).get("order")).isEqualTo("2");
    }

    /**
     * A partition with every marker on one offset is completely caught up, and the page must present that as health.
     * The row is marked collapsed, it draws a single marker rather than dividing a zero-width axis into spans, and
     * its sentence says so in words.
     */
    @Test
    void aCaughtUpPartitionReadsAsHealthyRatherThanAsBroken() {
        String url = serve(busyInstance());
        openPage(url);

        Map<String, Object> caughtUp = rowNamed(partitionRows(), CAUGHT_UP_PARTITION);

        assertThat(caughtUp.get("collapsed")).isEqualTo("true");
        assertThat(String.valueOf(caughtUp.get("note"))).startsWith("Caught up");
        assertThat(listOf(caughtUp, "spans")).as("a zero-width axis has no spans to draw").isEmpty();
        assertThat(listOf(caughtUp, "markers")).containsExactly("committed");
        assertThat(String.valueOf(caughtUp.get("won"))).isEqualTo("nothing won past the block");
    }

    /**
     * The counterfactual marker is the point of the graphic: it is where somebody else's consumer would be stopped.
     * It therefore appears only where something is genuinely incomplete, and the row says in words what it costs.
     */
    @Test
    void theStopMarkerAppearsOnlyWhereWorkIsIncomplete() {
        String url = serve(busyInstance());
        openPage(url);

        List<Map<String, Object>> rows = partitionRows();

        Map<String, Object> blocked = rowNamed(rows, HUGE_PARTITION);
        assertThat(listOf(blocked, "markers")).contains("stop");
        assertThat(String.valueOf(blocked.get("note")))
                .as("the sentence names the offset a single-threaded consumer would stop at, exactly")
                .contains(Long.toString(ABOVE_SAFE_INTEGER + 1));
        assertThat(listOf(blocked, "spans"))
                .as("the won span is the loudest thing on a blocked row, and it has to be drawn")
                .contains("won");

        assertThat(listOf(rowNamed(rows, CAUGHT_UP_PARTITION), "markers")).doesNotContain("stop");
    }

    /**
     * Every row carries the ordering caveat, hidden while the document says the markers were read consistently.
     * Asserting it is hidden here is what makes its appearance meaningful: a badge that is always present says
     * nothing.
     */
    @Test
    void theOrderingCaveatStaysHiddenWhileTheDocumentSaysTheMarkersAgree() {
        String url = serve(busyInstance());
        openPage(url);

        for (Map<String, Object> row : partitionRows()) {
            assertThat(row.get("caveatHidden")).as("row %s", row.get("name")).isEqualTo(Boolean.TRUE);
        }
    }

    /**
     * R16. The counterfactual sentence has four wordings that differ by more than a hundred characters, so while its
     * height was free it re-wrapped between one line and two as partitions changed state - seventeen pixels, moving
     * every row below it. Writing the longest wording into every row must not change one row's height.
     */
    @Test
    void theLongestSentenceARowCanSayDoesNotChangeItsHeight() {
        String url = serve(busyInstance());
        openPage(url);

        Object measured = script(""
                + "const rows = Array.from(document.querySelectorAll('#panel-offsets .pt'));"
                + "const before = rows.map(row => row.offsetHeight);"
                + "rows.forEach(row => { row.querySelector('.pt-note').textContent = 'A single-threaded consumer "
                + "would stop at offset 9223372036854775807 and reprocess everything above it. This instance has "
                + "already finished 9,223,372,036,854,775,807 of those records, safely recorded in the commit "
                + "metadata.'; });"
                // reading offsetHeight after the writes is what forces the browser to lay the page out again
                + "return rows.map(row => row.offsetHeight).join() === before.join();");

        assertThat(measured).isEqualTo(Boolean.TRUE);
    }

    @Test
    void theRibbonIsCapturedInBothThemes() {
        String url = serve(busyInstance());

        openPage(url, "light");
        Path light = screenshot("offset-ribbon-light");

        openPage(url, "dark");
        Path dark = screenshot("offset-ribbon-dark");

        assertThat(light).exists();
        assertThat(dark).exists();
    }

    /**
     * The digits of a grouped count. The separator belongs to the browser's locale and the assertion is about the
     * number, not about the punctuation.
     */
    private static String digitsOf(String formatted) {
        return formatted.replaceAll("[^0-9]", "");
    }
}
