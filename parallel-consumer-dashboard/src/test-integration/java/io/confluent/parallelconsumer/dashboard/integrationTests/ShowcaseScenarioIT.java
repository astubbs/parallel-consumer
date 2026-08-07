package io.confluent.parallelconsumer.dashboard.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The showcase scenario in {@code ONCE} mode against a real broker - the same run
 * {@code bin/dashboard-demo.sh --once} performs, asserted rather than watched.
 * <p>
 * This is what stops the demo rotting. A demo that silently stopped producing head-of-line blocking would
 * still start, still serve a page, still draw flat lines, and nobody would notice for months. Here, the
 * conditions the panels exist to render are assertions: if the run stops producing them, the build goes red
 * and the failure names the phase.
 * <p>
 * The assertions below deliberately duplicate what the phase postconditions already check. The postconditions
 * fail the RUN; these fail the TEST with the observation in the message, so a CI log says which shape was
 * missing without anyone having to read the scenario source.
 * <p>
 * Replay a specific run with {@code -Ddashboard.demo.seed=<long>}; the seed of every run is logged either way.
 */
@Timeout(900)
@Slf4j
class ShowcaseScenarioIT {

    /**
     * Fixed by default so a failure is reproducible without hunting through a CI log. The scenario's phase
     * structure - and therefore every assertion here - is scripted rather than drawn, so a different seed
     * varies the texture of a run and not its verdict.
     */
    private static final long DEFAULT_SEED = 20260807L;

    @Test
    void theShowcaseRunDrivesTheStateDocumentThroughEveryShapeThePanelsDraw() {
        long seed = Long.getLong("dashboard.demo.seed", DEFAULT_SEED);

        DemoMain.Result result = DemoMain.run(DemoMain.Options.once(seed));
        DashboardWatcher watcher = result.getWatcher();
        String replay = "bin/dashboard-demo.sh --once --seed=" + seed;

        assertWithMessage("every phase must produce what it declared (replay: %s); observations: %s",
                replay, watcher.summarise())
                .that(result.getFailures()).isEmpty();

        assertWithMessage("the dashboard must have served a URL (replay: %s)", replay)
                .that(result.getUrl()).startsWith("http");

        assertWithMessage("the run must have read the state document the page draws from (replay: %s)", replay)
                .that(watcher.getSamplesTaken()).isGreaterThan(0L);

        // 1. a partition acquires incomplete offsets below its highest succeeded offset
        DashboardWatcher.StrandedBand band = watcher.getWidestStrandedBand();
        assertWithMessage("a partition must strand completed work above an incomplete offset - that band IS the "
                + "offset ribbon's subject (replay: %s)", replay)
                .that(band).isNotNull();
        assertWithMessage("the stranded band must be wide enough to see, not a one-offset blip: %s (replay: %s)",
                band, replay)
                .that(band.getWidth()).isAtLeast((long) ShowcaseScenario.MIN_STRANDED_BAND);

        // 2. the committed offset later advances past them - past the TOP of the band, not its floor: PC
        //    reports the committed offset as the next offset to consume, so it sits one above the floor from
        //    the moment the band forms and a floor comparison would pass while the key was still failing
        assertWithMessage("the commit must be seen to clear the top of the stranded band once the failures are "
                        + "cleared - band was %s, highest committed there %s (replay: %s)",
                band, watcher.getHighestCommittedOnStrandedPartition(), replay)
                .that(watcher.getRecoveredStrandedBand()).isNotNull();

        // 3. in-flight rises under the slow-function phase
        assertWithMessage("in-flight records must climb while the user function is slowed (replay: %s)", replay)
                .that(watcher.getPeakInflight())
                .isAtLeast((long) ShowcaseScenario.MIN_INFLIGHT_UNDER_SLOW_FUNCTION);

        // 4. the assignment epoch changes when a second instance joins
        assertWithMessage("the assignment epoch must change when another instance joins the group (replay: %s)",
                replay)
                .that(watcher.getAssignmentEpochChanges()).isGreaterThan(0L);

        // the shard view has to have had something in it, or the key spread never reached the panels
        assertWithMessage("work must spread across key shards (replay: %s)", replay)
                .that(watcher.getPeakShards()).isAtLeast((long) ShowcaseScenario.MIN_SHARDS);
    }
}
