#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Run integration tests only (failsafe, requires Docker for TestContainers).
# Skips unit tests to avoid duplicate work.
#
# Usage: bin/ci-integration-test.sh [extra-maven-args...]
#        INTEGRATION_SHARD=heavy bin/ci-integration-test.sh [extra-maven-args...]
#        INTEGRATION_SHARD=rest  bin/ci-integration-test.sh [extra-maven-args...]
#
# SHARDING: ONE NAMED SET, ONE CATCH-ALL - not N balanced shards.
#
# `heavy` runs exactly the classes in HEAVY_CLASSES below; `rest` runs everything EXCEPT them.
# Unset runs the whole suite, which is what a local invocation and every non-CI caller gets.
#
# Why this shape rather than the chaos suite's four sized bins. A balanced N-way split has to be
# re-sized whenever the suite changes, and its failure mode is silent: add a class and it belongs
# to no shard, so it stops running and nothing goes red. Here the catch-all is defined by
# SUBTRACTION, so a new test runs by default and the only way to lose one is to name it in
# HEAVY_CLASSES and then delete it - which the report check below turns into a hard failure.
# The list is also small enough to reason about, which is the point: it is the handful of classes
# that dominate the wall, not a bin-packing whose contents nobody can predict.
#
# Sizing, measured (docs/plans/2026-09-03-001-investigate-integration-gate-wall-time.md): core's
# failsafe is ~1450-1530s of test time over 42 classes, and each JOB re-pays ~136s of serial build
# plus ~20s of job overhead - so a split only pays while the largest remaining class is smaller
# than the work it takes off the critical path. THE 857 PROBE IS THE BINDING CONSTRAINT: at ~342s
# in a single unsplittable class it sets the floor for whichever shard holds it, which is why the
# heavy set is that class alone and why the projected saving is ~20%. Splitting that class four
# ways (measured green, not yet adopted) would move the floor to PartitionStateCommittedOffsetIT's
# ~160s and roughly double the benefit - at which point the right heavy set is larger than one
# class and this list should be re-derived, not extended by guess.
#
# Extra args are forwarded to Maven. Forked per-broker mode - each JVM fork gets its
# own broker, reliable AND parallel - is requested with -DforkCount=N -DreuseForks=true,
# and .github/workflows/maven.yml's gating Integration Tests lane passes forkCount=4.
# This script itself passes none, so a bare invocation runs failsafe at its default of 1.
#
# forkCount=4 is a measured ceiling, not a floor: raising it to 6 was measured on the hosted
# runner at 469s of failsafe against a 420s baseline, because the forks contend (the same
# tests cost 11% more CPU time), and it produced a first-ever timeout failure in
# ManagedPCInstanceLifecycleTest. See docs/plans/2026-09-03-001-investigate-integration-gate-wall-time.md
# before raising it. See also docs/self-hosted-runner.md.

set -euo pipefail

# Group exclusions are HARDCODED here, not inherited from the pom default - enforced by
# QuarantinedAnnotationContractTest (pom inheritance once made the quarantine exclusion a
# silent no-op for unit tests). Keep in sync with the pom excluded.groups default - the two lists
# drifting is silent in the direction that matters: a lane the pom excludes but this does not runs
# here, in the GATING suite, which is how the Lincheck lane would have arrived if nobody looked.
# The publishing artefacts are dead weight in a TEST lane. delombok, javadoc:jar and source:jar
# are bound in the root pom's always-active <build> section rather than behind the release
# profile, so `clean verify` builds a javadoc jar and a sources jar for all eleven modules on
# every run. Nothing here consumes either.
#
# TAKEN ON FIRST PRINCIPLES, NOT ON A MEASUREMENT, and the distinction is the point. The skips
# provably do less work - a run with them builds 0 javadoc jars against 9 without, and delombok
# reports skipping 11 times against 2 - so this cannot be slower. But the saving is around 10s of
# a ~600s job, and this lane's wall time cannot resolve it: three CONCURRENT samples of IDENTICAL
# code spread 119 seconds, roughly 11x the effect. Do not go looking for this in a before/after
# timing; it is not visible there and never will be.
#
# Property names matter, and two of the three are non-obvious: maven-source-plugin reads
# maven.source.skip (NOT source.skip, which silently does nothing and shipped in the first cut of
# this change), and lombok-maven-plugin reads lombok.delombok.skip. Verify a change to these by
# counting artefacts in the log, never by timing the job.
#
# Deliberately NOT skipped: compiler:testCompile, 60s. It looks like the same waste, since
# -DskipUTs=true means the unit tests never run - but the integration sources import 19 classes
# out of src/test/java, including ParallelEoSStreamProcessorTestBase and its transitive tail, so
# that compilation is load-bearing. Checked before spending a CI run on it.
#
# docs/plans/2026-09-03-001-investigate-integration-gate-wall-time.md holds the measurements.
# ---------------------------------------------------------------------------------------------
# SHARD MEMBERSHIP. Three NAMED shards plus a catch-all, derived by longest-processing-time
# packing over MEASURED per-class wall times (run 33818955853). Simple class names, comma-separated.
#
# The catch-all is `rest`, defined by SUBTRACTION of the three lists - so a newly added test runs
# there by default and can never belong to no shard. That is the property that makes this
# maintainable: the failure mode of N explicit bins is a new class silently running nowhere, and
# nothing goes red for it.
#
# Balance is not asserted here and must not be: it decays as the suite changes, and a number
# written into a comment cannot notice. bin/check-integration-shard-balance.mjs recomputes the
# optimal partition from recorded per-class times and reports how far this one has drifted - so
# every CI run feeds the signal that says when to re-derive these lists.
# ---------------------------------------------------------------------------------------------
readonly SHARD_1_CLASSES="PartitionStateCommittedOffsetIT,Rebalance857CommitSyncDeadlockProbe2IT,RebalanceTest,TransactionalPartialResultSetIT,TransactionTimeoutsTest,CoreAppMetricsIntegrationTest,PartitionOrderProcessingTest,MultiInstanceMetricsTest,DrainingMemberRebalanceIT,KafkaSanityTests"
readonly SHARD_2_CLASSES="TransactionAndCommitModeTest,Rebalance857CommitSyncDeadlockProbe3IT,DbTest,CustomConsumersTest,CloseAndOpenOffsetTest,VertxConcurrencyIT,TransactionalEagerProcessingIT,BrokerPollerBackpressureTest,ProgressBarTest"
readonly SHARD_3_CLASSES="MultiInstanceRebalanceTest,Rebalance857CommitSyncDeadlockProbe4IT,TransactionalCrashReplayIT,TransactionMarkersTest,TransactionalVisibilityIT,LatestResetTailNudgeIT,LoadTest,DrainCloseTest,RetriesTest"

# Every named class, for the catch-all's exclusion and for the disjointness check below.
ALL_NAMED="${SHARD_1_CLASSES},${SHARD_2_CLASSES},${SHARD_3_CLASSES}"

# A class in two lists would run twice and be paid for twice, and nothing downstream would notice -
# both shards would pass. Cheap to check, so check it on every invocation rather than trusting
# review to catch a copy-paste.
dupes=$(echo "$ALL_NAMED" | tr ',' '\n' | sort | uniq -d)
if [ -n "$dupes" ]; then
    echo "ci-integration-test: FAILED - class(es) named in more than one shard list:" >&2
    printf '    %s\n' $dupes >&2
    exit 2
fi

shard_include_arg() {  # shard_include_arg <comma-list>
    echo "-Dit.test=$1"
}
shard_exclude_arg() {  # shard_exclude_arg <comma-list> -> failsafe <excludes> patterns
    echo "-Dit.excluded.classes=$(echo "$1" | sed 's#[^,][^,]*#**/&.java#g')"
}

SHARD_ARGS=()
SHARD_EXPECT=""
case "${INTEGRATION_SHARD:-}" in
    1) SHARD_EXPECT="$SHARD_1_CLASSES"
       SHARD_ARGS=("$(shard_include_arg "$SHARD_1_CLASSES")" -Dfailsafe.failIfNoSpecifiedTests=false) ;;
    2) SHARD_EXPECT="$SHARD_2_CLASSES"
       SHARD_ARGS=("$(shard_include_arg "$SHARD_2_CLASSES")" -Dfailsafe.failIfNoSpecifiedTests=false) ;;
    3) SHARD_EXPECT="$SHARD_3_CLASSES"
       SHARD_ARGS=("$(shard_include_arg "$SHARD_3_CLASSES")" -Dfailsafe.failIfNoSpecifiedTests=false) ;;
    rest)
       # NOT a negated -Dit.test. Setting it.test REPLACES failsafe's <includes>, and both test
       # source roots compile into the same target/test-classes - so the negated form silently
       # drops the `**/integrationTest*/**` restriction and runs the entire UNIT suite under
       # failsafe too. Measured before it was understood: 168 classes and 981 tests instead of 42
       # and 204, and it PASSED, because running more tests than you meant to fails nothing.
       SHARD_ARGS=("$(shard_exclude_arg "$ALL_NAMED")") ;;
    "") ;;  # whole suite - local runs and any caller that does not opt in
    *)
       echo "ci-integration-test: INTEGRATION_SHARD must be 1, 2, 3, 'rest' or unset (got '${INTEGRATION_SHARD}')" >&2
       exit 2 ;;
esac

./mvnw --batch-mode \
  -Pci \
  clean verify \
  -DskipUTs=true \
  -Dmaven.javadoc.skip=true \
  -Dmaven.source.skip=true \
  -Dlombok.delombok.skip=true \
  -Dexcluded.groups=performance,chaos,quarantined,lincheck \
  ${SHARD_ARGS[@]+"${SHARD_ARGS[@]}"} \
  "$@"

# A SHARD THAT RAN NOTHING MUST NOT READ AS A PASS. failIfNoSpecifiedTests is disabled above -
# it has to be, because the reactor builds ten modules containing none of these classes and the
# first would otherwise fail the build - so Maven exits 0 whether the selection matched every class
# or none of them. These checks are the only thing between a renamed class and a permanently green
# job that tests nothing. The mutation lane shipped exactly that bug once ("nothing to mutate,
# skipping", green forever); bin/chaos-test.sh guards the same shape.
REPORTS_DIR=parallel-consumer-core/target/failsafe-reports
report_exists() {  # report_exists <simple class name>
    ls "${REPORTS_DIR}"/TEST-*."$1".xml >/dev/null 2>&1
}

if [ -n "${SHARD_EXPECT}" ]; then
    # A NAMED shard must contain every class it was assigned. A rename fails HERE, loudly, while
    # the catch-all quietly keeps running the test - so the suite stays complete and the list is
    # what gets reported as wrong. That asymmetry is deliberate and is why the catch-all exists.
    missing=""
    for c in ${SHARD_EXPECT//,/ }; do
        report_exists "$c" || missing="${missing} ${c}"
    done
    if [ -n "$missing" ]; then
        echo "ci-integration-test: FAILED - shard ${INTEGRATION_SHARD} produced no failsafe report for:${missing}" >&2
        echo "  The shard ran, exited 0, and tested less than it was assigned. Usually a renamed or" >&2
        echo "  deleted class still named in its shard list - the catch-all is still running it, so" >&2
        echo "  nothing is untested, but the list is now wrong." >&2
        exit 1
    fi
elif [ "${INTEGRATION_SHARD:-}" = "rest" ]; then
    # The catch-all must contain NONE of the named classes and still not be empty.
    found=$(ls "${REPORTS_DIR}"/TEST-*.xml 2>/dev/null | wc -l | tr -d ' ')
    if [ "$found" -eq 0 ]; then
        echo "ci-integration-test: FAILED - the catch-all shard produced no failsafe reports at all." >&2
        exit 1
    fi
    for c in ${ALL_NAMED//,/ }; do
        if report_exists "$c"; then
            echo "ci-integration-test: FAILED - '${c}' is in a named shard but ran in the catch-all too." >&2
            echo "  Both shards are paying for it, and both will pass." >&2
            exit 1
        fi
    done
fi

# EVERY class that ran must live in an integrationTest package, in EVERY shard. This is the guard
# that caught the -Dit.test bug described above, and none of the checks before it would have: they
# ask whether the RIGHT tests ran, and that failure was 126 EXTRA ones. A "ran at least N" gate
# cannot see it either - more is not fewer - which is why this asks about PROVENANCE rather than
# count. It is the only check here that is not about the shard lists at all.
if [ -n "${INTEGRATION_SHARD:-}" ]; then
    strays=$(ls "${REPORTS_DIR}"/TEST-*.xml 2>/dev/null | grep -v '\.integrationTest' || true)
    if [ -n "$strays" ]; then
        echo "ci-integration-test: FAILED - shard '${INTEGRATION_SHARD}' ran classes from outside an" >&2
        echo "  integrationTest package. Failsafe's <includes> restriction has been lost - check whether" >&2
        echo "  something set -Dit.test, which REPLACES it. Offenders:" >&2
        printf '    %s\n' $strays >&2
        exit 1
    fi
fi
