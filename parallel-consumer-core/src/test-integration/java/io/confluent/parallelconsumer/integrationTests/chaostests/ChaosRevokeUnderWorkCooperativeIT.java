package io.confluent.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Chaos Pain Suite - W4 "revoke under work", <b>COOPERATIVE-STICKY assignor variant</b> (Phase 2,
 * Class 2 hunt lever 2). Identical to {@link ChaosRevokeUnderWorkIT} (see its javadoc for the full
 * design + eager calibration record) except the single experimental variable: the fleet uses Kafka's
 * {@code CooperativeStickyAssignor} (incremental rebalancing) instead of the default eager assignor.
 * <p>
 * Why this variant changes the physics twice over:
 * <ol>
 *   <li><b>The eager-restart artifact class disappears.</b> Under eager, every membership change
 *   revokes ALL partitions, restarting every in-flight heavy - which pinned commit low-watermarks and
 *   produced the false-positive class root-caused during eager calibration. Under cooperative-sticky,
 *   unaffected partitions keep processing straight through rebalances, so the legitimate
 *   lag-stagnation window shrinks to a single heavy dwell + slack - far more probe headroom.</li>
 *   <li><b>Exposure to the actual quarry was hypothesized to rise.</b> The open #857
 *   commit-during-revoke deadlock ({@code synchronized(commitCommand)} in
 *   {@code onPartitionsRevoked} vs {@code commitOffsetsThatAreReady}) fires per-revoke, and the
 *   going-in hypothesis was that cooperative mode produces MORE FREQUENT, smaller revokes = more
 *   draws at the probabilistic stall the eager variant's 9-seed sweep never hit. The calibration
 *   record below REVISED this: sticky assignment avoids unnecessary movement, so revoke events
 *   dropped ~6x - what rises is per-event sharpness, not frequency.</li>
 * </ol>
 * <p>
 * <b>Novel-coverage note</b>: this is the codebase's first-ever end-to-end exercise of PC under the
 * cooperative assignor - the harness flag existed with zero users, no docs claim cooperative support,
 * and while the state layer is delta-correct by design (per-partition epochs, subset-scoped
 * truncation, delta counters - session exploration 2026-07-31), it has never been validated. Any
 * novel cooperative-mode failure this scenario surfaces is a finding to document and roster, not to
 * mask (see the pre-declared outcome matrix in
 * {@code docs/plans/2026-07-31-001-feat-chaos-w4-cooperative-variant-plan.md}).
 * <p>
 * <b>Calibration record (2026-07-31, same-seed A/B: defect arm = plain master, fixed arm =
 * all-fixes composition; 4 defect seeds incl. smoke + 3 A/B pairs)</b> - outcome-matrix row:
 * <b>both arms GREEN</b>.
 * <ul>
 *   <li><b>PC survived its first-ever cooperative-sticky exercise end-to-end on both arms</b> -
 *   backlog drained, ledger balanced, no violations. The state layer's delta-correctness holds up
 *   empirically, not just by inspection.</li>
 *   <li><b>No Class 2 trigger under cooperative either</b> (4 defect-arm seeds, 0 hits). And the
 *   measured mechanism honestly REVISES the hypothesis: sticky assignment avoids unnecessary
 *   movement, so revoke events DROPPED ~6x vs eager (5-15 per run vs ~57) - fewer #857 draws, not
 *   more; what rises is per-event sharpness (partitions keep processing through rebalances, so the
 *   commit path is active whenever a revoke arrives).</li>
 *   <li><b>Dwell does NOT discriminate arms under cooperative</b>: defect 74-78s vs fixed 74-101s
 *   (eager discriminated 30s vs 8s). The long dwell band is cooperative incremental-rebalance
 *   chaining inherent to this churn shape + 30s eviction horizon - NOT a defect signature. Corollary:
 *   eager-calibrated Class 1 dwell bounds do not transfer to cooperative; a future W1-coop variant
 *   needs its own calibration.</li>
 *   <li>Lag-stagnation peaks 90-118s on both arms - the wedge arithmetic (30s eviction +
 *   reassignment + 20s heavy redelivery + slack), ~1.3x under the 150s bound. Storm-phase throughput
 *   roughly halves vs eager (~121k vs ~210k consumed) under near-continuous incremental
 *   rebalancing with sync commits.</li>
 * </ul>
 * <p>
 * Seed protocol: {@code -Dchaos.seed=<long>} replays a schedule; unset = random seed, always logged.
 * Excluded from default suites via {@code @Tag("chaos")}; run with {@code -Dincluded.groups=chaos}.
 */
@Tag("chaos")
@Timeout(600)
@Testcontainers
@Slf4j
class ChaosRevokeUnderWorkCooperativeIT extends AbstractRevokeUnderWorkScenario {

    @Override
    protected boolean useCooperativeAssignor() {
        return true;
    }

    @Override
    protected String scenarioLabel() {
        return "w4coop";
    }

    @Test
    void revokeUnderWorkStaysProtocolHonestWithCooperativeAssignor() throws Exception {
        runRevokeUnderWorkScenario();
    }
}
