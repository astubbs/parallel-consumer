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
# The classes the heavy shard owns. SIMPLE class names, comma-separated, and the single source of
# truth for both shards - `rest` is built from this by negation, so the two can never disagree
# about what is where. Keep it a short, deliberate list.
readonly HEAVY_CLASSES="Rebalance857CommitSyncDeadlockProbeIT"

SHARD_ARGS=()
case "${INTEGRATION_SHARD:-}" in
    heavy)
        # -Dfailsafe.failIfNoSpecifiedTests=false is REQUIRED, not defensive: the reactor builds
        # ten other modules that contain none of these classes, and without it the first such
        # module fails the build before the requested tests run. bin/chaos-test.sh's header records
        # the same trap. The report check below is what stops that flag turning a shard that
        # selected NOTHING into a silent pass.
        SHARD_ARGS=(-Dit.test="${HEAVY_CLASSES}" -Dfailsafe.failIfNoSpecifiedTests=false)
        ;;
    rest)
        # Negated selection: failsafe takes `!Class` patterns, so the catch-all is the complement
        # of the list above rather than a second list that could drift out of step with it.
        SHARD_ARGS=(-Dit.test="$(echo "$HEAVY_CLASSES" | sed 's/[^,][^,]*/!&/g')" \
                    -Dfailsafe.failIfNoSpecifiedTests=false)
        ;;
    "") ;;  # whole suite - local runs and any caller that does not opt in
    *)
        echo "ci-integration-test: INTEGRATION_SHARD must be 'heavy', 'rest' or unset (got '${INTEGRATION_SHARD}')" >&2
        exit 2
        ;;
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

# A SHARD THAT RAN NOTHING MUST NOT READ AS A PASS. With failIfNoSpecifiedTests disabled above,
# Maven exits 0 whether the selection matched every class or none of them, so the only thing
# standing between a renamed class and a permanently green job that tests nothing is this check.
# The mutation lane shipped exactly that bug once ("nothing to mutate, skipping", green forever)
# and bin/chaos-test.sh guards the same shape for the same reason.
#
# The two shards need OPPOSITE assertions, which is what makes the pair airtight: heavy must
# contain every named class, rest must contain none of them and still not be empty. A rename
# therefore fails the heavy shard loudly while the catch-all quietly keeps running the test - the
# safe direction, and the reason this shape was chosen.
REPORTS_DIR=parallel-consumer-core/target/failsafe-reports
case "${INTEGRATION_SHARD:-}" in
    heavy)
        missing=""
        for c in ${HEAVY_CLASSES//,/ }; do
            ls "${REPORTS_DIR}"/TEST-*."${c}".xml >/dev/null 2>&1 || missing="${missing} ${c}"
        done
        if [ -n "$missing" ]; then
            echo "ci-integration-test: FAILED - the 'heavy' shard produced no failsafe report for:${missing}" >&2
            echo "  The shard ran, exited 0, and tested less than it was assigned. Usually a renamed or" >&2
            echo "  deleted class still named in HEAVY_CLASSES - the catch-all shard is still running it," >&2
            echo "  so nothing is untested, but this list is now wrong." >&2
            exit 1
        fi
        ;;
    rest)
        found=$(ls "${REPORTS_DIR}"/TEST-*.xml 2>/dev/null | wc -l | tr -d ' ')
        if [ "$found" -eq 0 ]; then
            echo "ci-integration-test: FAILED - the 'rest' shard produced no failsafe reports at all." >&2
            exit 1
        fi
        for c in ${HEAVY_CLASSES//,/ }; do
            if ls "${REPORTS_DIR}"/TEST-*."${c}".xml >/dev/null 2>&1; then
                echo "ci-integration-test: FAILED - '${c}' is in HEAVY_CLASSES but ran in the 'rest' shard too." >&2
                echo "  The negated selection is not excluding it, so both shards pay for it." >&2
                exit 1
            fi
        done
        ;;
esac
