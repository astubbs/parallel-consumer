package bz.stub.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Map;

/**
 * CONTROL ARM for {@link ChaosRevokeUnderWorkIT}: the same scenario, but every stop DRAINS.
 * <p>
 * It exists to test a mechanism rather than to hunt a defect. The eager scenario's stops are
 * {@code close()}, which is {@code closeDontDrainFirst()} - in-flight work is abandoned without being
 * committed, so at-least-once redelivers it to the next assignee. A record that takes
 * {@link AbstractRevokeUnderWorkScenario#HEAVY_SLEEP} to process therefore costs that dwell AGAIN
 * each time a storm tick catches it in flight, and a partition's commit watermark cannot advance past
 * it. Chain enough of those and the watermark is legitimately pinned past
 * {@link ProgressProbe#LAG_STAGNATION_BOUND} with nothing wrong - which is the leading explanation for
 * the {@code CLASS2_STALL/LAG_STAGNATION} sightings this family has collected.
 * <p>
 * A draining stop lets the in-flight heavy FINISH and commit before the member leaves, so the chain
 * cannot form. The prediction is therefore sharp, and either outcome is informative:
 * <ul>
 *   <li><b>Drain arm clean, no-drain arm red</b> - the stagnation is redelivery of abandoned heavy
 *   work, an artifact of the workload against the bound, not a PC defect.</li>
 *   <li><b>Drain arm ALSO red</b> - the abandoned-work explanation is wrong and something else pins
 *   the watermark. That would be the first real lead on a Class 2 mechanism in this family.</li>
 * </ul>
 * <b>Duplicates are the same measurement from the other side.</b> A draining close should produce
 * few or none, where the no-drain arm measured ~1% (2,421 of 250,000). If this arm still duplicates
 * heavily, the drain is not doing what its name says, which is worth knowing on its own.
 * <p>
 * <b>This deliberately inverts {@link ChaosRevokeUnderWorkIT}'s central design choice, and is not a
 * gentler version of it.</b> That scenario excludes drains precisely because a drain opens the
 * Class 1 drain-zombie window, which can mask the Class 2 mechanism it isolates. So this arm can see
 * a failure class that one cannot, and is blind to nothing it needs - but its GREEN must never be
 * read as evidence about the no-drain arm's Class 2 hunt. It answers "does abandoning in-flight work
 * cause the watermark pinning", and only that.
 * <p>
 * Seed protocol and lane are inherited: {@code -Dchaos.seed=<long>} replays a schedule, and
 * {@code @Tag("chaos")} keeps it out of the default suites.
 */
@Tag("chaos")
@Timeout(600)
@Testcontainers
@Slf4j
class ChaosRevokeUnderWorkDrainIT extends AbstractRevokeUnderWorkScenario {

    @Override
    protected boolean useCooperativeAssignor() {
        return false;
    }

    @Override
    protected String scenarioLabel() {
        return "w4drain";
    }

    /**
     * Drain-only stops - the single variable against {@link ChaosRevokeUnderWorkIT}. The mix itself is
     * {@link AbstractRevokeUnderWorkScenario#drainOnlyChaosWeights()}, shared with the cooperative
     * control arm ({@link ChaosRevokeUnderWorkCooperativeDrainIT}) so the two cannot drift apart.
     */
    @Override
    protected Map<ChaosConductor.ChaosAction, Integer> chaosWeights() {
        return drainOnlyChaosWeights();
    }

    @Test
    void revokeUnderDrainingStopsStaysProtocolHonest() throws Exception {
        runRevokeUnderWorkScenario();
    }
}
