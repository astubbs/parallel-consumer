package io.confluent.parallelconsumer.dashboard.integrationTests.ui;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.nio.file.Path;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The page shell in a real browser: the states it can be in, and that each one is distinguishable by a machine and
 * not only by a person looking at it.
 *
 * <h2>Why the states are the thing worth asserting</h2>
 * <p>
 * The page's whole claim is that it never lets a stalled instance look like a healthy one. Idle - connected, caught
 * up, nothing to do - and stale - still connected, but the instance has stopped publishing - draw almost the same
 * picture, and a chart flatlining at zero is what both of them look like. So the distinction has to be carried in
 * the DOM, where a test can see it, rather than only in a sentence a reader might notice.
 */
@Execution(ExecutionMode.SAME_THREAD)
class PageShellUiIT extends DashboardUiTestBase {

    /**
     * The page decides a snapshot is stale after three expected intervals, with an eight-second floor. Nothing here
     * can make that happen faster, so the wait is real - and it is the only slow test in this suite.
     */
    private static final Duration LONGER_THAN_THE_STALENESS_FLOOR = Duration.ofSeconds(25);

    @Test
    void theShellReportsTheLivePhaseAndNamesTheInstanceItIsWatching() {
        String url = serve(busyInstance());

        openPage(url);

        assertThat(statusPhase()).isEqualTo("live");
        assertThat(textOf("#status-label")).isEqualTo("Live");
        assertThat(textOf("#instance-id"))
                .as("the instance id comes from the document's lifecycle block")
                .isEqualTo(INSTANCE_ID);
        assertThat(textOf("#footer-meta"))
                .as("the footer states the schema and sample the page is drawing, so a stuck page is provable")
                .contains("schema 1")
                .contains("sample 1");
    }

    /**
     * The two states that must never be confused, asserted against one another in one run so the difference is the
     * subject rather than a coincidence of two separate fixtures.
     */
    @Test
    void anIdleInstanceIsDistinguishableFromAStalledOneWithoutLookingAtIt() {
        String url = serve(idleInstance());

        openPage(url);

        assertThat(statusPhase())
                .as("connected, caught up and with nothing in flight is idle - a healthy state, not a fault")
                .isEqualTo("idle");
        assertThat(textOf("#status-label")).isEqualTo("Idle");

        // the fixture publishes exactly once, so from here on the instance has genuinely stopped publishing
        awaitPhase("stale", LONGER_THAN_THE_STALENESS_FLOOR);

        assertThat(textOf("#status-label")).isEqualTo("Stale");
        assertThat(textOf("#status-detail"))
                .as("staleness is stated in words with an age beside it, never left to a flat chart")
                .contains("No new sample for");
    }

    /**
     * A transport failure is its own state, and it is not staleness: staleness means the instance stopped
     * publishing, this means the page cannot reach it at all. The page has to say which.
     */
    @Test
    void anInstanceThatGoesAwayIsReportedAsAnErrorRatherThanAsSilence() {
        String url = serve(busyInstance());
        openPage(url);
        assertThat(statusPhase()).isEqualTo("live");

        stopServing();

        awaitPhase("error");
        assertThat(textOf("#status-label")).isEqualTo("Error");
        assertThat(textOf("#status-detail"))
                .as("the error names what could not be read, rather than saying something went wrong")
                .contains("/api/state.json");
    }

    /**
     * Screenshots in both themes, for the breakage no assertion anticipated.
     * <p>
     * Nothing compares pixels - an image diff would fail on font hinting long before it caught anything real. What
     * <em>is</em> asserted is that the emulated colour scheme actually reached the page, because a pair of
     * identical-looking screenshots labelled light and dark would be worse than none.
     */
    @Test
    void theShellIsCapturedInBothThemesAndTheThemesDiffer() {
        String url = serve(busyInstance());

        openPage(url, "light");
        String lightBackground = backgroundColour();
        Path light = screenshot("page-shell-light");

        openPage(url, "dark");
        String darkBackground = backgroundColour();
        Path dark = screenshot("page-shell-dark");

        assertThat(light).exists();
        assertThat(dark).exists();
        assertThat(light.toFile().length())
                .as("a screenshot of a rendered page is not a handful of bytes")
                .isGreaterThan(5000L);
        assertThat(darkBackground)
                .as("the page's own prefers-color-scheme rules must answer the emulated preference")
                .isNotEqualTo(lightBackground);
    }

    /**
     * The state document behind the page, asserted to be the same one the shell is reporting. This is the join
     * between the two halves of the harness: everything else here reads the DOM, and this proves the DOM is a view
     * of the document rather than of something the page invented.
     */
    @Test
    void theShellReportsTheSampleTheDocumentCarries() {
        String url = serve(busyInstance());
        openPage(url);

        JsonObject document = stateDocument(url);

        assertThat(document.getInteger("schemaVersion")).isEqualTo(1);
        assertThat(document.getJsonObject("lifecycle").getString("instanceId")).isEqualTo(textOf("#instance-id"));
        assertThat(textOf("#footer-meta")).contains("sample " + document.getLong("sampleSequence"));
    }

    private String backgroundColour() {
        return String.valueOf(script("return getComputedStyle(document.body).backgroundColor;"));
    }
}
