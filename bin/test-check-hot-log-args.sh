#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for bin/check-hot-log-args.sh.
#
# THE FIRST CASE IS THE REASON THIS FILE EXISTS. The check's first draft used gawk's ENDFILE, and
# the default awk on this platform is mawk, which has no such block: the program parsed, matched
# nothing, and printed its success line over a file containing the exact defect. A gate that passes
# everything is indistinguishable from a clean tree, so every case here is a CONTROL - two that must
# fail and two that must pass. A check verified only against the fixed tree proves nothing.

set -uo pipefail

CHECK="$(cd "$(dirname "$0")" && pwd)/check-hot-log-args.sh"
failures=0

run_case() { # description expected-exit java-body
  local desc="$1" expected="$2" body="$3"
  local dir out rc
  dir="$(mktemp -d)"
  mkdir -p "$dir/bin" "$dir/parallel-consumer-core/src/main/java/bz/stub/x"
  cp "$CHECK" "$dir/bin/"
  printf '%s\n' "$body" > "$dir/parallel-consumer-core/src/main/java/bz/stub/x/Subject.java"
  out=$( cd "$dir" && bash bin/check-hot-log-args.sh 2>&1 ); rc=$?
  rm -rf "$dir"
  if [ "$rc" -eq "$expected" ]; then
    printf 'ok    %s (exit %d)\n' "$desc" "$rc"
  else
    printf 'FAIL  %s - expected exit %d, got %d\n%s\n' "$desc" "$expected" "$rc" "$out"
    failures=$((failures + 1))
  fi
}

# MUST FAIL: the defect as it actually reached the control loop - a multi-line log.trace whose
# argument scans every shard, with no guard anywhere above it.
run_case "unguarded multi-line log.trace" 1 'package bz.stub.x;
class Subject {
    void loop() {
        log.trace("Control loop: blocking for {}, queued={}",
                timeToBlockFor,
                wm.getNumberOfWorkQueuedInShardsAwaitingSelection());
    }
}'

# MUST FAIL: the fluent form without a lambda. This is the trap the fluent API introduces - it reads
# as deferred and evaluates eagerly, so it is worse than the plain form it appears to improve on.
run_case "addArgument without a lambda" 1 'package bz.stub.x;
class Subject {
    void loop() {
        log.atTrace()
                .addArgument(wm.getNumberOfIncompleteOffsets())
                .log("queued={}");
    }
}'

# MUST PASS: the older house idiom, still used in this codebase. If this ever fails, the check has
# started flagging correct code and will be switched off rather than fixed.
run_case "isTraceEnabled guard" 0 'package bz.stub.x;
class Subject {
    void loop() {
        if (log.isTraceEnabled()) {
            log.trace("queued={}, incomplete={}",
                    wm.getNumberOfWorkQueuedInShardsAwaitingSelection(),
                    wm.getNumberOfIncompleteOffsets());
        }
    }
}'

# MUST PASS: the preferred fix. atTrace() returns the NOP builder when trace is off and NOP never
# calls the supplier - asserted at runtime by HotPathLogArgumentsAreDeferredTest, which this script
# cannot do and which cannot do this.
run_case "fluent form with suppliers" 0 'package bz.stub.x;
class Subject {
    void loop() {
        log.atTrace()
                .addArgument(() -> wm.getNumberOfWorkQueuedInShardsAwaitingSelection())
                .log("queued={}");
    }
}'

if [ "$failures" -eq 0 ]; then
  echo "All check-hot-log-args self-tests passed"
  exit 0
fi
printf '%d check-hot-log-args self-test(s) failed\n' "$failures"
exit 1
