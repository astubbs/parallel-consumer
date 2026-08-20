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
 * <b>The control-arm reasoning is not repeated here.</b> Why a draining stop should break the
 * abandoned-heavy-work chain, what each outcome would mean, why the duplicate count is the same
 * measurement taken from the other side, and why this deliberately INVERTS
 * {@link ChaosRevokeUnderWorkIT}'s central design choice rather than softening it - all of that is in
 * {@link ChaosRevokeUnderWorkDrainIT}'s javadoc and applies unchanged to this cell. Read that one
 * first; this file records only what swapping the assignor adds, and the same warning carries over:
 * a GREEN here is never evidence about the no-drain arm's Class 2 hunt.
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
     * Drain-only stops - the single variable against {@link ChaosRevokeUnderWorkIT}. The mix itself is
     * {@link AbstractRevokeUnderWorkScenario#drainOnlyChaosWeights()}, shared with the eager
     * control arm ({@link ChaosRevokeUnderWorkDrainIT}) so the two cannot drift apart.
     */
    @Override
    protected Map<ChaosConductor.ChaosAction, Integer> chaosWeights() {
        return drainOnlyChaosWeights();
    }

    @Test
    void revokeUnderDrainingStopsWithCooperativeAssignorStaysProtocolHonest() throws Exception {
        runRevokeUnderWorkScenario();
    }
}
