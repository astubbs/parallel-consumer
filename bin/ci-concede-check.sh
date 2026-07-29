#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# ci-concede-check.sh <highcpu-suite-name>   (e.g. "Unit", "Integration", "Performance")
#
# "Concede" optimizer for the GitHub-hosted gate. The self-hosted `highcpu` runner runs the same suites,
# usually faster. This lets the GitHub-hosted job, BEFORE running its own (slow, 2-core) suite, check whether
# the highcpu equivalent for THIS commit has already PASSED - and if so, skip its own run and declare green.
# The GitHub-hosted job therefore stays the required gate (it always reports), it just finishes faster when
# grumpy wins the race. It NEVER depends on the self-hosted runner: if grumpy is offline/queued/slow/failed,
# the hosted job runs its tests normally.
#
# Outputs `skip=true|false` to $GITHUB_OUTPUT.
#
# CAVEAT (deliberate): conceding trusts the self-hosted environment. An env-specific failure that only
# manifests on GitHub's runner (DNS, timing, resource limits) could be missed on a conceded run. Acceptable
# here because these suites are deterministic and the self-hosted box is a faithful Linux/JDK17 env; the
# throughput-noise-sensitive Performance suite is unaffected (pass/fail doesn't depend on contention).
#
# Env: GH_TOKEN (needs actions:read), CONCEDE_HEAD_SHA (the PR head SHA - both workflows share it),
#      GITHUB_REPOSITORY. Optional: CONCEDE_MAX_WAIT (default 600s), CONCEDE_WORKFLOW (default the highcpu one).
set -uo pipefail

SUITE="${1:?usage: ci-concede-check.sh <suite-name>}"
REPO="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY unset}"
SHA="${CONCEDE_HEAD_SHA:?CONCEDE_HEAD_SHA unset}"
WORKFLOW="${CONCEDE_WORKFLOW:-PR High-CPU Fast Feedback}"
JOB_PREFIX="Build & Test - ${SUITE} (high-CPU"     # matches the highcpu matrix job for this suite
MAX_WAIT="${CONCEDE_MAX_WAIT:-600}"
INTERVAL=20

emit() { echo "skip=$1" >>"${GITHUB_OUTPUT:-/dev/stdout}"; echo "concede[$SUITE]: skip=$1 - $2"; exit 0; }

command -v gh >/dev/null 2>&1 || emit false "gh not available"

deadline=$(( $(date +%s) + MAX_WAIT ))
queued_ticks=0
while :; do
  RUN=$(gh api "/repos/${REPO}/actions/runs?head_sha=${SHA}&per_page=40" \
        --jq ".workflow_runs[] | select(.name==\"${WORKFLOW}\") | .id" 2>/dev/null | head -1)
  if [ -z "${RUN:-}" ]; then emit false "no '${WORKFLOW}' run for ${SHA:0:8} (self-hosted not triggered)"; fi

  read -r STATUS CONCL < <(gh api "/repos/${REPO}/actions/runs/${RUN}/jobs?per_page=100" \
        --jq ".jobs[] | select(.name | startswith(\"${JOB_PREFIX}\")) | \"\(.status) \(.conclusion // \"none\")\"" \
        2>/dev/null | head -1)
  STATUS="${STATUS:-missing}"; CONCL="${CONCL:-none}"

  case "$CONCL" in
    success)                             emit true  "highcpu ${SUITE} already passed - skipping hosted run" ;;
    failure|cancelled|timed_out|skipped) emit false "highcpu ${SUITE} did not pass (${CONCL}) - run on hosted" ;;
  esac
  case "$STATUS" in
    in_progress)  queued_ticks=0 ;;                          # actively running -> wait for it
    queued|missing)
      queued_ticks=$((queued_ticks + 1))
      [ "$queued_ticks" -ge 3 ] && emit false "highcpu ${SUITE} ${STATUS} (runner offline?) - run on hosted" ;;
  esac

  [ "$(date +%s)" -ge "$deadline" ] && emit false "highcpu ${SUITE} not done within ${MAX_WAIT}s - run on hosted"
  echo "concede[$SUITE]: highcpu status=${STATUS} concl=${CONCL} - waiting ${INTERVAL}s..."
  sleep "$INTERVAL"
done
