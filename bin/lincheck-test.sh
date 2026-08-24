#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Run the Lincheck lane - scheduler-controlled concurrency testing over parallel-consumer-core's state
# classes. Non-gating and opt-in, the same shape as bin/chaos-test.sh.
#
# FIVE flags have to line up, which is why this script exists rather than an mvnw line in a doc - and
# every one of them fails SILENTLY on its own:
#
#   1. -Dincluded.groups=lincheck AND -Dexcluded.groups= . The `lincheck` tag is in the pom's default
#      excluded.groups, and in JUnit Platform exclusion beats inclusion - so the include ALONE selects
#      nothing and the run exits BUILD SUCCESS having tested zero classes. Same trap the performance
#      lane documents in the root pom.
#   2. -Plincheck, which supplies the --add-opens/--add-exports the MODEL CHECKING strategy needs. Miss
#      it and model checking dies with InaccessibleObjectException while the stress tests still pass,
#      so a partial red looks like a flake rather than a missing flag.
#   3. -Dpc.log.level=info - the interleaving traces are logged at INFO and logback-test.xml
#      defaults the harness to warn, so without -Dpc.log.level=info a FOUND violation prints no trace.
#   4. -Dparallel-tests=false . Lincheck installs a JVM-WIDE instrumentation agent and drives the
#      scheduler of the fork it runs in; two Lincheck classes running concurrently in one JVM (which is
#      what this module's JUnit thread parallelism does by default) share that agent. Serial execution
#      is the only configuration whose results mean anything.
#   5. -Djacoco.skip=true . JaCoCo's probes are ordinary field writes to the model checker, so an
#      instrumented run interleaves and PRINTS them: every trace line is padded with $jacocoInit and
#      RuntimeData.getProbes frames, burying the three lines that matter. Coverage of a lane that
#      deliberately re-runs four operations a few thousand times is worthless anyway.
#
# Env (data, not code - workflow inputs must pass through env, never ${{ }} into scripts):
#   LINCHECK_TEST - optional -Dtest= filter (e.g. ShardManagerLincheckTest); empty = the whole lane
#
# Runtime at the committed bounds is well under a minute for the whole lane (26-29s measured, build
# included), but it grows superlinearly with threads/actorsPerThread - the bounds are stated in each
# test method rather than here, and docs/plans/2026-08-25-001-test-lincheck-poc-plan.md records what
# each one cost.

set -euo pipefail

cd "$(dirname "$0")/.."

# Note the `+` expansion: under `set -u`, bash 3.2 (which is what macOS ships) treats "${arr[@]}" on an
# EMPTY array as an unbound variable and aborts. The lane then exits 1 having run nothing.
TEST_ARG=()
if [ -n "${LINCHECK_TEST:-}" ]; then
    TEST_ARG=(-Dtest="${LINCHECK_TEST}" -Dsurefire.failIfNoSpecifiedTests=false)
fi

start=$(date +%s)

set +e
./mvnw -Plincheck -pl parallel-consumer-core -am test \
    -Dincluded.groups=lincheck \
    -Dexcluded.groups= \
    -Dpc.log.level=info \
    -Dparallel-tests=false \
    -Djacoco.skip=true \
    -DskipITs \
    ${TEST_ARG[@]+"${TEST_ARG[@]}"}
status=$?
set -e

total=$(( $(date +%s) - start ))

# A lane that selected nothing must say so rather than impersonating a green run - the same failure
# mode the chaos lane guards against, and the one the mutation lane shipped with.
selected=$(find parallel-consumer-core -path '*/surefire-reports/TEST-*Lincheck*.xml' | wc -l | tr -d ' ')

printf '\n## Lincheck lane\n\n'
printf 'Wall-clock: **%dm %02ds** (build included)\n\n' $((total / 60)) $((total % 60))
if [ "$selected" -eq 0 ]; then
    printf 'ZERO Lincheck classes ran - this measured NOTHING. Check the group filters above.\n'
    exit 1
fi
printf 'Lincheck report files: %s\n' "$selected"

exit $status
