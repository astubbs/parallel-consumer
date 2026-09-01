#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# THE SEAM-ON EVIDENCE LANE for parallel-consumer-streams (astubbs#255).
#
# Runs Apache Kafka's own test suite TWICE against this module's patched classes - once with the PC
# dispatch seam off, once with it on - and then classifies every case that passes in the first and
# fails in the second by the mechanism its own recorded failure names. A divergence matching a named
# mechanism passes; one matching none fails this lane.
#
# Usage: bin/ci-streams-seam-on-evidence.sh [extra-maven-args...]
#
# Exit codes:
#   0  the two arms were both produced, and every divergence between them is explained
#   1  unexplained divergences, or an arm that could not be read (missing, empty or stale reports)
#   2  the environment cannot run the lane (no JDK 17 on JAVA_HOME, no maven wrapper)
#
# WHY THE SEAM-ON RUN DOES NOT FAIL THE BUILD ITSELF. Its failures ARE the measurement: the module
# refuses constructs it cannot run safely, and commits Parallel Consumer's frontier rather than the
# offset stock Streams would have committed, so Kafka's own assertions about those two things are
# statements about a different design. A suite that goes red on every build stops being read. The
# classification is therefore a SEPARATE surefire execution, which is what lets "these failures are
# expected" and "a new divergence fails the build" both be true at once.
#
# WHY THIS SCRIPT DELETES THE REPORT DIRECTORIES FIRST. A surefire report directory left behind by an
# earlier run parses perfectly and reads as this run's result - and a run that was skipped leaves an
# empty glob, which summed over "how many failed" is zero, indistinguishable from a clean pass. Both
# cost real time on this module before. Deleting them here makes the classifier's own existence and
# freshness checks reachable rather than theoretical.
#
# DO NOT NARROW THIS WITH -Dtest=. That silently overrides each Kafka execution's own <includes>, the
# suite never runs, and the build goes green having computed nothing. Narrowing the module's OWN tests
# out of the way is what -Dincluded.groups is for, and this script does it.
#
# WHY THE SEAM-OFF ORACLE IS RELAXED HERE AND NOWHERE ELSE. In an ordinary build that run is the
# behaviour-preservation gate and its failures are fatal. Here it is the CONTROL ARM, and stopping on
# its first failure means the measured arm never runs - so one already-diagnosed flake in Kafka's own
# suite costs a full re-run of both arms, which is the habit the flake ledger exists to replace. The
# lane therefore relaxes that ONE execution and makes the verdict itself: every control-arm failure
# must carry a flaky-case marker in docs/inflight/, and the classifier fails if one does not. Nothing
# is waved through; the failure is classified instead of being allowed to abort the measurement. The
# classifier's own execution is NOT relaxed, which is why this script's exit code still means
# something.

set -euo pipefail

readonly MODULE="parallel-consumer-streams"
readonly SEAM_OFF_REPORTS="${MODULE}/target/surefire-reports-kafka-upstream"
readonly SEAM_ON_REPORTS="${MODULE}/target/surefire-reports-kafka-upstream-seam-on"
readonly LANE_REPORT="${MODULE}/target/seam-on-evidence-report.txt"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"

if [[ ! -x ./mvnw ]]; then
  echo "ci-streams-seam-on-evidence: no ./mvnw in ${repo_root} - cannot run" >&2
  exit 2
fi

# The lane's own report directories, gone before maven starts. Nothing else in target/ is touched:
# a full clean would re-unpack and re-patch Kafka's sources, which is minutes of work this lane does
# not need and which would make an incremental re-run of a sabotage experiment far more expensive.
rm -rf "${SEAM_OFF_REPORTS}" "${SEAM_ON_REPORTS}" "${LANE_REPORT}"

# -Dincluded.groups is the documented way to keep this module's OWN tests (including the broker-backed
# arms, which want Docker) out of a run that is only interested in Kafka's suite. It filters the
# default-test execution alone: both Kafka executions override the group filters, so they are
# unaffected - which is exactly why -Dtest= must not be used for the same job. It also does the same
# job for parallel-consumer-core, which -am pulls into the reactor.
#
# -am, NOT `-pl .,parallel-consumer-streams`. This module depends on parallel-consumer-core's jar AND
# its tests jar, and a developer box has them installed while a fresh CI runner does not - so the
# narrower form works locally and fails on the first CI run with an unresolved snapshot. Same shape as
# bin/chaos-test.sh. -am brings the parent in as well, so the enforcer's reactor-convergence rule is
# satisfied without naming the root explicitly.
#
# `set -e` would take the exit here and never print the report, which is the one artefact a reader
# wants when the lane is red - so the status is captured rather than propagated immediately.
status=0
./mvnw --batch-mode \
  -pl "${MODULE}" -am \
  test \
  -Dseam.on.upstream.skip=false \
  -Dseam.off.upstream.failure.ignore=true \
  -Dincluded.groups=seam-on-evidence-runs-no-module-tests \
  "$@" || status=$?

if [[ -f "${LANE_REPORT}" ]]; then
  echo
  cat "${LANE_REPORT}"
fi

exit "${status}"
