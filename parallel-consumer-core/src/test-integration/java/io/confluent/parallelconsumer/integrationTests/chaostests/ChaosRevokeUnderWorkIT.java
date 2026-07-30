package io.confluent.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2020-2026 Confluent, Inc. and contributors
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Chaos Pain Suite - W4 "revoke under work" (Phase 2, EAGER assignor): the trigger scenario for
 * <b>Class 2, protocol-INVISIBLE stalls</b> - the "#857 locks forever until manual restart" family,
 * where the group stays STABLE, heartbeats and polls keep flowing, no rebalance is pending (so the
 * broker's 5-minute eviction clock never starts), yet a partition's committed offset stops moving
 * while its lag is real. {@link ProgressProbe}'s {@code CLASS2_STALL/LAG_STAGNATION} probe is the
 * detector this scenario exists to exercise. The cooperative-sticky sibling is
 * {@link ChaosRevokeUnderWorkCooperativeIT}; the shared two-phase driver lives in
 * {@link AbstractRevokeUnderWorkScenario}.
 * <p>
 * Shape - <b>two phases</b>, discovered by fail-first calibration (run 1 on the defect arm went RED
 * via the CLASS 1 dwell probe instead: under CONTINUOUS churn, a revoke-path deadlock blocks the
 * rebalance itself, i.e. the stall is protocol-VISIBLE - the Class 2 signature can only emerge after
 * rebalances settle):
 * <ol>
 *   <li><b>Storm</b>: <b>no drain stops at all</b> ({@link ChaosConductor#defaultW4Weights()}) -
 *   drains open the Class 1 zombie window and mask the Class 2 mechanism. Hard stops, restarts and
 *   joins force frequent partition REVOCATIONS while heavy non-interruptible work is in flight, with
 *   {@link CommitMode#PERIODIC_CONSUMER_SYNC} to maximise revoke-path vs commit-path lock contention
 *   (the upstream #857 deadlock recipe: {@code synchronized(commitCommand)} between
 *   {@code onPartitionsRevoked} and {@code commitOffsetsThatAreReady}).</li>
 *   <li><b>Quiet observation</b>: chaos stops; a low {@code max.poll.interval.ms} (30s) means any
 *   member wedged during the storm gets evicted quickly, pending rebalances resolve, and the group
 *   re-STABILIZES - the exact production shape of the "locks forever" reports. From here the only
 *   possible defect signal is protocol-invisible.</li>
 * </ol>
 * <p>
 * <b>Calibration status (2026-07-30, seed 7612284256787897904, same schedule both arms)</b>: the
 * scenario is calibrated ARTIFACT-FREE; the Class 2 probe's RED trigger remains an open hunt.
 * <ul>
 *   <li>First shape (90s storm, 45s dwell) went RED on BOTH arms - root-caused as a workload artifact,
 *   not a stall: eager reassignment restarts in-flight heavies every storm tick, legitimately pinning
 *   commit low-watermarks for storm+dwell+slack ~155s, past the 150s bound. Diagnostic run proved the
 *   freeze RESOLVES (~155s) and the backlog completes. Recalibrated to 60s+20s so the legit window
 *   (~100s arithmetic; 117-123s measured peaks) sits under the bound.</li>
 *   <li>Recalibrated verdicts: defect arm (plain master) GREEN with dwell peak 30.1s - the wedged
 *   member's protocol-visible block, capped exactly by the 30s eviction horizon; fixed arm GREEN with
 *   dwell peak 7.9s. The dwell MEASUREMENT discriminates the defect even though its violation is
 *   delegated to W1.</li>
 *   <li>No true (unbounded) Class 2 stall reproduced on master under this seed/shape or the 8-seed
 *   sweep (9 seeds total, 0 hits) - the open #857 root-cause stall did not bite under EAGER. Hence
 *   the cooperative-sticky sibling: more frequent, smaller revokes = more draws at the probabilistic
 *   deadlock, and the eager-restart artifact class disappears entirely.</li>
 * </ul>
 * <p>
 * Seed protocol: {@code -Dchaos.seed=<long>} replays a schedule; unset = random seed, always logged.
 * Excluded from default suites via {@code @Tag("chaos")}; run with {@code -Dincluded.groups=chaos}.
 */
@Tag("chaos")
@Timeout(600)
@Testcontainers
@Slf4j
class ChaosRevokeUnderWorkIT extends AbstractRevokeUnderWorkScenario {

    @Override
    protected boolean useCooperativeAssignor() {
        return false;
    }

    @Override
    protected String scenarioLabel() {
        return "w4";
    }

    @Test
    void revokeUnderWorkStaysProtocolHonest() throws Exception {
        runRevokeUnderWorkScenario();
    }
}
