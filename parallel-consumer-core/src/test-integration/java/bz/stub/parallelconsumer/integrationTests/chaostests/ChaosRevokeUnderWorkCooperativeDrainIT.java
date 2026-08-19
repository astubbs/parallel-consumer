package bz.stub.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.EnumMap;
import java.util.Map;

/**
 * The fourth cell of the assignor x stop-mode matrix: COOPERATIVE assignor, DRAINING stops.
 * <p>
 * The other three were measured on seed 4734674029169027864 on 2026-08-19, and they show the
 * assignor doing nearly all the work: eager/no-drain gave 53 violations and 2421 duplicates,
 * eager/drain 45 and 2007, cooperative/no-drain 0 and 405. Draining moved almost nothing while
 * changing the assignor moved everything, because eager revokes ALL partitions from ALL members on
 * any membership change, so the in-flight work that gets abandoned was never the departing member's
 * to drain.
 * <p>
 * This cell exists to complete the matrix rather than to test a hypothesis, and that is the point:
 * with three cells measured, the fourth is where an unexpected interaction would show up. The
 * prediction is boring - cooperative/drain should look like cooperative/no-drain, at or near zero
 * violations, with duplicates at or below 405, because the only work left to abandon belongs to the
 * member that is leaving and draining is exactly what saves it. A result far from that would mean
 * the two variables interact, which nothing so far suggests.
 * <p>
 * ORIGINAL CONTROL-ARM REASONING, which still applies to the eager sibling.
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
class ChaosRevokeUnderWorkCooperativeDrainIT extends AbstractRevokeUnderWorkScenario {

    @Override
    protected boolean useCooperativeAssignor() {
        return true;
    }

    @Override
    protected String scenarioLabel() {
        return "w4coopdrain";
    }

    /**
     * Drain-only stops - the single variable against {@link ChaosRevokeUnderWorkIT}. RESTART and
     * JOIN_NEW keep their W4 weights so the membership churn RATE is comparable; only the manner of
     * leaving changes, which is what keeps this a control arm rather than a different workload.
     */
    @Override
    protected Map<ChaosConductor.ChaosAction, Integer> chaosWeights() {
        Map<ChaosConductor.ChaosAction, Integer> w = new EnumMap<>(ChaosConductor.ChaosAction.class);
        w.put(ChaosConductor.ChaosAction.STOP_DRAIN, 3); // was STOP_NO_DRAIN at 3
        w.put(ChaosConductor.ChaosAction.RESTART, 3);
        w.put(ChaosConductor.ChaosAction.JOIN_NEW, 2);
        return w;
    }

    @Test
    void revokeUnderDrainingStopsWithCooperativeAssignorStaysProtocolHonest() throws Exception {
        runRevokeUnderWorkScenario();
    }
}
