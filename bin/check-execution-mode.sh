#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Decides whether a test run actually EXERCISED the execution mode it was asked for, and renders what
# it found.
#
# WHY THIS EXISTS
#
# Parallel Consumer has execution paths that are selected by a flag or a runtime and are meant to be
# BEHAVIOURALLY EQUIVALENT to the default engine - virtual threads today, direct pull next
# (docs/inflight/test-opt-in-engine-paths-are-unexercised.md). CI runs the existing suite again with
# a different selector, which makes one failure mode overwhelmingly likely:
#
#     a suite that SKIPPED the mode's own tests, and reported green.
#
# That is not hypothetical here. The virtual-thread tests on the upstream PR used JUnit Assumptions,
# and CI runs a JDK the mode is unavailable on, so they skipped and the job passed - its author said
# so himself. This repository has shipped that shape before; the roster is in
# docs/solutions/workflow-issues/a-check-that-reports-success-without-having-run.md.
#
# The Java side already inverts the assumption, so a selected-but-unavailable mode FAILS a test. This
# script is the second half: it reads the surefire XML - structural evidence from the artifact, not a
# string in a log - and refuses to call a run green when the mode's own tests did not execute. It also
# states the run and skip counts in the job summary, so a skip is visible rather than absent.
#
# TWO REDS, AND THEY MUST NOT BE CONFUSED
#
#   mode not proven to have run -> exit 1  (red: the LANE is broken - a green result means nothing)
#   mode ran, tests all passed  -> exit 0
#   mode ran, tests failed      -> exit 2  (red: the TREE has a real disagreement to triage)
#
# They demand opposite responses: exit 1 means go fix the lane and learn nothing about the tree;
# exit 2 means the lane worked and there is a genuine behavioural difference between this mode and the
# default engine - which is the most valuable thing a mode axis produces, because it identifies tests
# that assert implementation rather than behaviour. When both are true, exit 1 wins: failures from a
# run that cannot be shown to have exercised the mode are not evidence in either direction.
#
# ADDING THE NEXT MODE is a row in MODE_TESTS below plus a matrix entry - not another script.
#
# Usage:  bin/check-execution-mode.sh <mode> [surefire-reports-dir ...]
#
#   mode                 the execution mode this run selected: `virtual-threads`, or `default`
#   surefire-reports-dir defaults to every */target/surefire-reports in the tree
#
# Markdown report goes to stdout, so CI can append it to $GITHUB_STEP_SUMMARY. Progress and the
# verdict go to stderr.

set -euo pipefail

# The test class that can only pass by actually exercising the mode. `default` has no marker: the
# whole suite is its evidence, and there is nothing that could silently skip.
mode_marker() {
  case "$1" in
    virtual-threads) echo "VirtualThreadExecutionModeTest" ;;
    default) echo "" ;;
    *) return 1 ;;
  esac
}

# The system property a mode is selected by, quoted in the failure message so the reader can
# reproduce the run.
mode_selector() {
  case "$1" in
    virtual-threads) echo "-Dpc.virtualThreads=true" ;;
    default) echo "(none)" ;;
    *) return 1 ;;
  esac
}

MODE="${1:-}"
if [ -z "$MODE" ]; then
  echo "usage: $0 <mode> [surefire-reports-dir ...]" >&2
  exit 1
fi
shift || true

if ! MARKER="$(mode_marker "$MODE")"; then
  echo "check-execution-mode: unknown mode '$MODE'. Known modes: virtual-threads, default." >&2
  echo "Add a row to mode_marker()/mode_selector() when you add a mode." >&2
  exit 1
fi
SELECTOR="$(mode_selector "$MODE")"

REPORT_DIRS=("$@")
if [ ${#REPORT_DIRS[@]} -eq 0 ]; then
  while IFS= read -r dir; do
    REPORT_DIRS+=("$dir")
  done < <(find . -type d -path '*/target/surefire-reports' -not -path './.git/*' | sort)
fi

if [ ${#REPORT_DIRS[@]} -eq 0 ]; then
  echo "## Execution mode: \`$MODE\`"
  echo
  echo "**No surefire reports found.** The suite did not run, so nothing is known about this mode."
  echo "check-execution-mode: no surefire reports - the lane did not run" >&2
  exit 1
fi

total_tests=0
total_failures=0
total_errors=0
total_skipped=0
marker_tests=0
marker_skipped=0
marker_seen=0

for dir in "${REPORT_DIRS[@]}"; do
  [ -d "$dir" ] || continue
  while IFS= read -r xml; do
    # The <testsuite ...> attributes, read off the artifact rather than parsed out of console text -
    # console output is truncated, reordered and localised; the XML is none of those.
    header="$(head -c 4000 "$xml" | tr '\n' ' ')"
    case "$header" in
      *"<testsuite "*) : ;;
      *) continue ;;
    esac

    # Bash's own regex, deliberately, rather than sed or grep. GNU sed accepts `s/.../.../;t;d` and
    # BSD sed (macOS, which contributors run) rejects it with "undefined label" - on stderr, while
    # still exiting 0, so every attribute silently read as zero and the guard called a healthy run a
    # broken lane. Its self-test caught that; nothing else would have. No pipe either, so there is no
    # SIGPIPE hazard for bin/check-shell-sigpipe.sh to find.
    attr() {
      local re="[[:space:]]$1=\"([0-9]+)\""
      if [[ "$header" =~ $re ]]; then
        echo "${BASH_REMATCH[1]}"
      else
        echo 0
      fi
    }

    t="$(attr tests)"; f="$(attr failures)"; e="$(attr errors)"; s="$(attr skipped)"
    total_tests=$((total_tests + t))
    total_failures=$((total_failures + f))
    total_errors=$((total_errors + e))
    total_skipped=$((total_skipped + s))

    if [ -n "$MARKER" ] && case "$xml" in *"$MARKER"*) true ;; *) false ;; esac; then
      marker_seen=1
      marker_tests=$((marker_tests + t))
      marker_skipped=$((marker_skipped + s))
    fi
  done < <(find "$dir" -name 'TEST-*.xml' -type f | sort)
done

marker_executed=$((marker_tests - marker_skipped))

echo "## Execution mode: \`$MODE\`"
echo
echo "| | |"
echo "|---|---:|"
echo "| Selector | \`$SELECTOR\` |"
echo "| Tests run | $total_tests |"
echo "| Failures | $total_failures |"
echo "| Errors | $total_errors |"
echo "| **Skipped** | **$total_skipped** |"
if [ -n "$MARKER" ]; then
  echo "| Mode-proving tests in \`$MARKER\` | $marker_tests |"
  echo "| ...of which executed | $marker_executed |"
  echo "| ...of which skipped | $marker_skipped |"
fi
echo

status=0

if [ "$total_tests" -eq 0 ]; then
  echo "### Lane broken"
  echo
  echo "Surefire reports exist but contain no tests. Nothing was learned about \`$MODE\`."
  echo "check-execution-mode: no tests in the reports - the lane did not run" >&2
  exit 1
fi

if [ -n "$MARKER" ]; then
  if [ "$marker_seen" -eq 0 ]; then
    echo "### Lane broken"
    echo
    echo "The suite ran, but \`$MARKER\` produced no report at all - so nothing here proves the"
    echo "\`$MODE\` path was taken. A green result from this run means nothing about that mode."
    echo "check-execution-mode: $MARKER did not run - the lane cannot prove mode '$MODE' was exercised" >&2
    exit 1
  fi
  if [ "$marker_executed" -le 0 ]; then
    echo "### Lane broken"
    echo
    echo "Every test in \`$MARKER\` was skipped, so this run verified nothing about \`$MODE\` while"
    echo "reporting a result. Run it on a runtime that provides the mode - for virtual threads, a"
    echo "JDK 21+ test JVM via \`-Djvm.location=<jdk21-home>\`. The build JDK does not have to move."
    echo "check-execution-mode: all $marker_tests tests in $MARKER skipped - mode '$MODE' was never exercised" >&2
    exit 1
  fi
fi

if [ "$total_failures" -gt 0 ] || [ "$total_errors" -gt 0 ]; then
  echo "### Real findings to triage"
  echo
  echo "The \`$MODE\` path was exercised and $((total_failures + total_errors)) test(s) disagreed with"
  echo "the default engine's expectations. **Do not silence these.** Each is either a behaviour worth"
  echo "keeping in both modes, or an assertion on the current engine's implementation that should not"
  echo "have been written - and that distinction is invisible while only one mode exists."
  echo "check-execution-mode: mode '$MODE' ran ($marker_executed mode tests executed) and the tree has $((total_failures + total_errors)) failure(s)" >&2
  status=2
else
  echo "### Exercised, and in agreement"
  echo
  if [ -n "$MARKER" ]; then
    echo "\`$MARKER\` executed $marker_executed test(s) that can only pass on the \`$MODE\` path."
  fi
  echo "The suite agrees with the default engine."
  echo "check-execution-mode: mode '$MODE' exercised ($marker_executed mode tests), $total_tests tests green, $total_skipped skipped" >&2
fi

exit "$status"
