#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for bin/check-inflight-tags.sh. Every case is a NEGATIVE control: build a note wrong in
# one specific way and assert the checker rejects it, plus the valid shapes to prove it is not simply
# rejecting everything.
#
# WHY. docs/agent-harness.md rule 3 - make it go red on purpose before you trust it. A checker only
# ever run against a corpus it already passes has proven nothing, and this repo has shipped exactly
# that: a self-test suite that printed FAIL and exited 0.

set -uo pipefail
CHECK="$(cd "$(dirname "$0")" && pwd)/check-inflight-tags.sh"
failures=0

assert() { # <name> <pass|fail> <note-body>
    local tmp rc got
    tmp=$(mktemp -d)
    (
      cd "$tmp" || exit 1
      git init -q .
      mkdir -p docs/inflight
      printf '%b' "$3" > docs/inflight/bug-case.md
      bash "$CHECK" >/dev/null 2>&1
    )
    rc=$?
    [ "$rc" -eq 0 ] && got=pass || got=fail
    if [ "$got" = "$2" ]; then
        printf 'ok:   %s\n' "$1"
    else
        printf 'FAIL: %s (expected %s, got %s)\n' "$1" "$2" "$got"
        failures=$((failures + 1))
    fi
    rm -rf "$tmp"
}

assert "a well-formed bug passes"             pass '# T\n\n<!-- inflight-type: bug -->\n<!-- inflight-impact: stall -->\n'
assert "a feature with no impact passes"      pass '# T\n\n<!-- inflight-type: feature -->\n'
assert "a state carrying a reason passes"     pass '# T\n\n<!-- inflight-type: task -->\n<!-- inflight-impact: coordination -->\n<!-- inflight-state: closed - will not do -->\n'
assert "a missing type is rejected"           fail '# T\n\n<!-- inflight-impact: stall -->\n'
assert "an unknown type is rejected"          fail '# T\n\n<!-- inflight-type: chore -->\n<!-- inflight-impact: stall -->\n'
assert "a misspelt impact is rejected"        fail '# T\n\n<!-- inflight-type: bug -->\n<!-- inflight-impact: misdirekshun -->\n'
assert "a bug with no impact is rejected"     fail '# T\n\n<!-- inflight-type: bug -->\n'
assert "a task with no impact is rejected"    fail '# T\n\n<!-- inflight-type: task -->\n'
assert "a feature WITH an impact is rejected" fail '# T\n\n<!-- inflight-type: feature -->\n<!-- inflight-impact: stall -->\n'
assert "a state with no reason is rejected"   fail '# T\n\n<!-- inflight-type: task -->\n<!-- inflight-impact: coordination -->\n<!-- inflight-state: closed -->\n'

echo
if [ "$failures" -eq 0 ]; then echo "All check-inflight-tags self-tests passed"; exit 0; fi
echo "$failures self-test(s) FAILED"
exit 1
