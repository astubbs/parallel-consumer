#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Self-test for bin/check-pr-ready.sh. Every case must go RED against the broken version; a
# regression test that has never failed proves nothing (bin/AGENTS.md).
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
SCRIPT_UNDER_TEST="$PWD/bin/check-pr-ready.sh"
pass=0; fail=0
assert() { # <desc> <expected> <actual>
    if [ "$2" = "$3" ]; then echo "ok:   $1"; pass=$((pass+1));
    else echo "FAIL: $1 (expected '$2', got '$3')" >&2; fail=$((fail+1)); fi
}

# THE SCRIPT MUST NEVER CONCLUDE READY. Its whole reason for existing is that an agent turned a git
# fact into a verdict; a version that prints "ready" would reproduce the defect it guards.
body="$(cat "$SCRIPT_UNDER_TEST")"
case "$body" in *'is ready'*|*'READY TO MERGE'*) got=claims_ready ;; *) got=never_claims ;; esac
assert "the script never prints a readiness verdict" never_claims "$got"
case "$body" in *"THAT IS NOT READINESS"*) got=says_so ;; *) got=silent ;; esac
assert "a clean run says explicitly that clean is not ready" says_so "$got"
case "$body" in *"NOT a readiness verdict"*) got=labelled ;; *) got=unlabelled ;; esac
assert "git mergeability is labelled as a git fact" labelled "$got"

# EVERY matching note, not the first. `pr-322-*` matches more than one file, and reading only the
# first skipped the note that actually recorded what was open.
# CODE ONLY, not comments. The first version of this case matched the word anywhere in the file and
# so flagged the comment that EXPLAINS the fix - a test asserting on prose rather than behaviour.
code="$(grep -v '^[[:space:]]*#' "$SCRIPT_UNDER_TEST")"
case "$code" in *'-name "pr-${pr}-*.md"'*'head -1'*) got=takes_first ;; *) got=reads_all ;; esac
assert "it does not read only the first matching note" reads_all "$got"
case "$body" in *'while IFS= read -r note'*) got=loops ;; *) got=no_loop ;; esac
assert "it loops over the notes it finds" loops "$got"

# A human LGTM is required; automated review is not approval and neither is green CI.
case "$body" in *'no human approval'*) got=checks_human ;; *) got=missing ;; esac
assert "absence of human approval is a blocker" checks_human "$got"
case "$body" in *'background task'*) got=checks_inflight ;; *) got=missing ;; esac
assert "live background work is a blocker" checks_inflight "$got"

# Usage, with no PR resolvable, must not exit 0 - a silent success would read as "nothing outstanding".
out=$( cd "$(mktemp -d)" && bash "$SCRIPT_UNDER_TEST" 2>&1 ); rc=$?
[ "$rc" -ne 0 ] && got=nonzero || got=zero
assert "an unresolvable PR exits non-zero" nonzero "$got"

printf '\n%d passed, %d failed\n' "$pass" "$fail"
(( fail == 0 ))
