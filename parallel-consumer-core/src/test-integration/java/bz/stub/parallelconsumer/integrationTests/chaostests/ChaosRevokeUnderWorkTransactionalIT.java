/*
 * Copyright (C) 2026 Antony Stubbs and contributors
 */
package bz.stub.parallelconsumer.integrationTests.chaostests;

import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Chaos Pain Suite - revoke under work, in {@code PERIODIC_TRANSACTIONAL_PRODUCER}.
 *
 * <p><b>The first chaos scenario in this suite to run the transactional mode at all</b>, and it
 * exists because the absence was an accident rather than a decision. The fleet builder was always
 * parameterised by commit mode, but {@code ManagedPCInstance} never wired a producer and this
 * family's base class hardcoded the mode - so transactional was unreachable by construction, and the
 * gap read as a considered exclusion.
 *
 * <p><b>What that cost.</b> astubbs/parallel-consumer#44 (confluentinc/parallel-consumer#803) is the
 * one issue upstream labelled a <i>verified bug</i> - one of a couple of dozen so labelled, not
 * the only one - and it is transactional. So is the
 * unbounded revoke wait in {@code docs/inflight/bug-857-transactional-revoke-wait.md}, whose fix is
 * blocked on an unsettled design decision. The suite built to hunt this family could not reach
 * either of them.
 *
 * <p><b>What this scenario can see that the sync arms cannot.</b> The revoke path in transactional
 * mode waits with no deadline for an in-flight transaction, on the poll thread, inside
 * {@code poll()} - so it is bounded only by {@code max.poll.interval.ms}, and overrunning it evicts
 * the member. This family already drives revokes while work is outstanding, which is precisely the
 * condition that wait needs; running it in this mode points an existing instrument at an existing
 * defect rather than inventing a new one.
 *
 * <p><b>Calibration status: UNCALIBRATED.</b> First run 2026-09-01: GREEN in 144s with
 * {@code probe violations=[]}, on the confluentinc#857 branch. That establishes the scenario
 * RUNS in this mode - the fleet starts, the conductor churns it, the probe reports - and
 * nothing more. It is
 * not yet known whether it goes red on master, red only under particular timing, or green because
 * the revoke wait needs a sharper shape than this family produces. <b>Do not read a green run as
 * evidence the transactional revoke path is healthy</b> until that is established: a scenario whose
 * failing case has never been demonstrated is an unarmed detector, and this repo's own rule is that
 * a detector which cannot be shown to fire proves nothing when it stays quiet. The first job for
 * whoever picks this up is to make it go red on a tree that should fail - the pre-fix composition -
 * before trusting it anywhere.
 *
 * <p>Excluded from default suites via {@code @Tag("chaos")}; run with
 * {@code -Dincluded.groups=chaos}. Seed protocol and the usage recipe are as for the rest of the
 * family - see {@link ChaosRevokeUnderWorkIT}.
 */
@Tag("chaos")
@Timeout(600)
@Testcontainers
@Slf4j
class ChaosRevokeUnderWorkTransactionalIT extends AbstractRevokeUnderWorkScenario {

    @Override
    protected CommitMode commitMode() {
        return CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
    }

    /** Eager, so this varies one term from {@link ChaosRevokeUnderWorkIT} - the mode, not the assignor. */
    @Override
    protected boolean useCooperativeAssignor() {
        return false;
    }

    @Override
    protected String scenarioLabel() {
        return "w4tx";
    }

    @Test
    void revokeUnderWorkStaysProtocolHonestInTransactionalMode() throws Exception {
        runRevokeUnderWorkScenario();
    }
}
