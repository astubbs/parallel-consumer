#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for bin/check-human-lgtm.sh. Runs in CI ahead of the gate it protects.
#
# Deliberately small. The rule is "a review by the owner whose body contains lgtm, any case,
# anywhere", so the cases are: it is there, it is not there, it is someone else's, and the two
# real-world spellings the repo's own history contains. The 855-line predecessor tested code
# fences, blockquotes, negation forms, glued repeats and CRLF - defences against an attacker who
# is also the only person the check protects.

set -uo pipefail

SCRIPT="$(cd "${BASH_SOURCE[0]%/*}/.." && pwd)/bin/check-human-lgtm.sh"
fails=0

check() { # <name> <expected-exit> <reviews-json>
    local name=$1 want=$2 json=$3 got
    "$SCRIPT" >/dev/null 2>&1 <<<"$json"
    got=$?
    if [ "$got" = "$want" ]; then
        echo "ok:   $name"
    else
        echo "FAIL: $name (expected exit $want, got $got)"
        fails=$((fails + 1))
    fi
}

check "owner said lgtm"                 0 '[{"user":{"login":"astubbs"},"body":"lgtm"}]'
check "owner said LGTM in caps"         0 '[{"user":{"login":"astubbs"},"body":"LGTM"}]'
check "Lgtm, as on astubbs#84"          0 '[{"user":{"login":"astubbs"},"body":"Lgtm"}]'
check "mid-sentence, as on astubbs#73"  0 '[{"user":{"login":"astubbs"},"body":"lgtm, @claude how about you?"}]'
check "owner reviewed but did not say"  1 '[{"user":{"login":"astubbs"},"body":"a few comments"}]'
check "somebody else said lgtm"         1 '[{"user":{"login":"someone"},"body":"lgtm"}]'
check "no reviews at all"               1 '[]'
check "owner lgtm among other reviews"  0 '[{"user":{"login":"someone"},"body":"nope"},{"user":{"login":"astubbs"},"body":"lgtm"}]'
check "empty stdin cannot be scanned"   2 ''
check "two pages, lgtm on the second" 0 '[{"user":{"login":"x"},"body":"no"}] [{"user":{"login":"astubbs"},"body":"lgtm"}]'

# The negative control that matters: prove the check can actually fail. A gate that has never
# failed proves nothing (bin/AGENTS.md).
if "$SCRIPT" <<<'[{"user":{"login":"astubbs"},"body":"lgtm"}]' >/dev/null 2>&1 &&
   ! "$SCRIPT" <<<'[]' >/dev/null 2>&1; then
    echo "ok:   the check distinguishes present from absent"
else
    echo "FAIL: the check does not distinguish present from absent"
    fails=$((fails + 1))
fi

[ "$fails" -eq 0 ] || { echo "$fails failure(s)."; exit 1; }
echo "All checks passed."
