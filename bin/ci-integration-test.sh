#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Run integration tests only (failsafe, requires Docker for TestContainers).
# Skips unit tests to avoid duplicate work.
# Usage: bin/ci-integration-test.sh [extra-maven-args...]
#
# Extra args are forwarded to Maven. The self-hosted runner workflow passes
# -DforkCount=4 -DreuseForks=true to run integration in forked per-broker mode
# (each JVM fork gets its own broker - reliable AND parallel; the ci profile
# runs sequentially on GitHub-hosted 2-core runners). See
# docs/self-hosted-runner.md.

set -euo pipefail

# Group exclusions are HARDCODED here, not inherited from the pom default - enforced by
# QuarantinedAnnotationContractTest (pom inheritance once made the quarantine exclusion a
# silent no-op for unit tests). Keep in sync with the pom excluded.groups default - the two lists
# drifting is silent in the direction that matters: a lane the pom excludes but this does not runs
# here, in the GATING suite, which is how the Lincheck lane would have arrived if nobody looked.
# The publishing artefacts are dead weight in a TEST lane. delombok, javadoc:jar and
# source:jar are bound in the root pom's always-active <build> section rather than behind the
# release profile, so `clean verify` builds a javadoc jar and a sources jar for all eleven
# modules on every run - measured at 29s of a 604s Maven step, and every second of it is serial,
# unlike test time which four forks already overlap. Nothing in this lane consumes them.
#
# Deliberately NOT skipped here: compiler:testCompile (60s). It looks like the same kind of
# waste, since -DskipUTs=true means the unit tests never run - but the integration sources import
# 19 classes out of src/test/java, including ParallelEoSStreamProcessorTestBase and its
# transitive tail, so that compilation is load-bearing.
# docs/plans/2026-09-03-001-investigate-integration-gate-wall-time.md has the measurements.
./mvnw --batch-mode \
  -Pci \
  clean verify \
  -DskipUTs=true \
  -Dmaven.javadoc.skip=true \
  -Dsource.skip=true \
  -Dlombok.delombok.skip=true \
  -Dexcluded.groups=performance,chaos,quarantined,lincheck \
  "$@"
