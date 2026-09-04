#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Run integration tests only (failsafe, requires Docker for TestContainers).
# Skips unit tests to avoid duplicate work.
# Usage: bin/ci-integration-test.sh [extra-maven-args...]
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
# provably do less work - a run with them builds 0 javadoc jars against 9 without (two of the
# eleven reactor poms are aggregators and produce none either way), and delombok reports skipping
# 11 times against 2 - so this cannot be slower. The SIZE is an estimate and not a measurement:
# per-goal attribution puts javadoc:jar at 14s and delombok at 8s, so roughly 22s. Nothing measured
# it, because this lane's wall time cannot resolve 22s - three CONCURRENT samples of IDENTICAL code
# spread 119 seconds. Do not go looking for this in a before/after timing; it is not visible there
# and never will be.
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
./mvnw --batch-mode \
  -Pci \
  clean verify \
  -DskipUTs=true \
  -Dmaven.javadoc.skip=true \
  -Dmaven.source.skip=true \
  -Dlombok.delombok.skip=true \
  -Dexcluded.groups=performance,chaos,quarantined,lincheck \
  "$@"
