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
# A FEATURE MAY NOW CARRY AN IMPACT. It used to be rejected on the reasoning that proposed work has an
# opportunity rather than a consequence - which buried a feature that exists to prevent a crash among
# the cosmetic ones. The tag exists to make work fall out in priority order.
assert "a feature WITH an impact passes"       pass '# T\n\n<!-- inflight-type: feature -->\n<!-- inflight-impact: crash -->\n'
assert "a feature with a task impact passes"   pass '# T\n\n<!-- inflight-type: feature -->\n<!-- inflight-impact: reliability -->\n'
assert "a feature with a BOGUS impact fails"   fail '# T\n\n<!-- inflight-type: feature -->\n<!-- inflight-impact: nonsense -->\n'
# the new impacts, one of each partition, so a typo in the lib is caught here rather than in a note
assert "bug/crash passes"                      pass '# T\n\n<!-- inflight-type: bug -->\n<!-- inflight-impact: crash -->\n'
assert "bug/reliability passes"                pass '# T\n\n<!-- inflight-type: bug -->\n<!-- inflight-impact: reliability -->\n'
assert "task/ci passes"                        pass '# T\n\n<!-- inflight-type: task -->\n<!-- inflight-impact: ci -->\n'
assert "task/process passes"                   pass '# T\n\n<!-- inflight-type: task -->\n<!-- inflight-impact: process -->\n'
assert "task/refactor passes"                  pass '# T\n\n<!-- inflight-type: task -->\n<!-- inflight-impact: refactor -->\n'
assert "task/test-debt passes"                 pass '# T\n\n<!-- inflight-type: task -->\n<!-- inflight-impact: test-debt -->\n'
assert "task/security passes"                  pass '# T\n\n<!-- inflight-type: task -->\n<!-- inflight-impact: security -->\n'
# the partition still holds where it should: these belong to one side only
assert "bug with release-gate still fails"     fail '# T\n\n<!-- inflight-type: bug -->\n<!-- inflight-impact: release-gate -->\n'
assert "task with data-loss still fails"       fail '# T\n\n<!-- inflight-type: task -->\n<!-- inflight-impact: data-loss -->\n'

# A REGISTER is consulted, never completed - a ranked backlog, a collision list. Impact optional,
# like a feature's: a collision list whose cost of going unread IS a collision may say so.
assert "a register with no impact passes"      pass '# T\n\n<!-- inflight-type: register -->\n'
assert "a register WITH an impact passes"      pass '# T\n\n<!-- inflight-type: register -->\n<!-- inflight-impact: coordination -->\n'
assert "a register with a BOGUS impact fails"  fail '# T\n\n<!-- inflight-type: register -->\n<!-- inflight-impact: nonsense -->\n'

# The assert() above BUILDS a fixture note and runs the gate on it - the wrong shape for a case that
# needs to break the doc or the lib instead. These two get their own comparison rather than misusing
# a helper whose signature is <name> <pass|fail> <note-body>.
dry_assert() { # <name> <caught|missed>
    if [ "$2" = "caught" ]; then
        echo "ok:   $1"
    else
        echo "FAIL: $1 (the gate did not name the offending value)" >&2
        failures=$((failures + 1))
    fi
}

# THE DOC AND THE LIB MUST AGREE, checked in both directions. Run against fixture copies so the case
# tests the CHECK rather than the repo's current state - and so it keeps working when the vocabulary
# legitimately grows.
dry_dir=$(mktemp -d); mkdir -p "$dry_dir/bin/lib" "$dry_dir/docs/inflight"
cp bin/check-inflight-tags.sh "$dry_dir/bin/"
cp bin/lib/inflight-tags.sh "$dry_dir/bin/lib/"
cp docs/inflight/AGENTS.md "$dry_dir/docs/inflight/"

# Not `sed -i`: GNU takes the suffix attached (-i.bak), BSD takes it as the NEXT argument, so the one
# spelling cannot mean in-place on both. On BSD this read the script as the suffix and the file as the
# script - "sed: 1: invalid command code f" - leaving the fixture unedited, so both directions of this
# doc-and-lib agreement check silently tested nothing and reported the gate as broken.
sed 's/^INFLIGHT_TASK_IMPACTS="\(.*\)"$/INFLIGHT_TASK_IMPACTS="\1 undocumented-value"/' \
    "$dry_dir/bin/lib/inflight-tags.sh" > "$dry_dir/inflight-tags.tmp"
mv "$dry_dir/inflight-tags.tmp" "$dry_dir/bin/lib/inflight-tags.sh"
out=$( cd "$dry_dir" && bash bin/check-inflight-tags.sh 2>&1 )
case "$out" in *undocumented-value*) dry_got=caught ;; *) dry_got=missed ;; esac
dry_assert "a lib value the doc never explains is caught" "$dry_got"

cp bin/lib/inflight-tags.sh "$dry_dir/bin/lib/"
printf '| `bogus-impact` | task | nonsense |\n' >> "$dry_dir/docs/inflight/AGENTS.md"
out=$( cd "$dry_dir" && bash bin/check-inflight-tags.sh 2>&1 )
case "$out" in *bogus-impact*) dry_got=caught ;; *) dry_got=missed ;; esac
dry_assert "a doc value the lib rejects is caught" "$dry_got"
rm -rf "$dry_dir"
assert "a state with no reason is rejected"   fail '# T\n\n<!-- inflight-type: task -->\n<!-- inflight-impact: coordination -->\n<!-- inflight-state: closed -->\n'

# The shapes where this gate and the session index used to DISAGREE. Each one passed here and then
# vanished or was mislabelled in the index - a gate that green-lights what the index cannot place is
# worse than no gate, because the mistake surfaces to whoever starts the next session instead.
assert "a bare marker with no comment wrapper is rejected" fail '# T\n\ninflight-type: bug\ninflight-impact: stall\n'
assert "a task impact on a bug is rejected"                fail '# T\n\n<!-- inflight-type: bug -->\n<!-- inflight-impact: release-gate -->\n'
assert "a bug impact on a task is rejected"                fail '# T\n\n<!-- inflight-type: task -->\n<!-- inflight-impact: stall -->\n'
assert "a task with a task impact passes"                  pass '# T\n\n<!-- inflight-type: task -->\n<!-- inflight-impact: stranded-work -->\n'
# A '>' inside a state reason splits the gate from the index: the gate's old greedy extraction
# parsed it happily, while the index's is_open ('inflight-state:[^>]*-->') cannot cross the '>',
# so the same note was gate-green yet listed as OPEN at every session start (astubbs#324 review,
# proven by execution). The gate must reject what the index cannot parse.
assert "a state reason containing '>' is rejected"         fail '# T\n\n<!-- inflight-type: task -->\n<!-- inflight-impact: coordination -->\n<!-- inflight-state: closed - superseded, fix -> astubbs#331 -->\n'

# A DUPLICATED tag is invisible to every other check: they all read the first match, so a note can
# carry two contradictory states and be reported valid. A merge produced exactly that - the stale
# block appended under the corrected one - and the gate passed it.
assert "a second inflight-state is rejected"               fail '# T\n\n<!-- inflight-type: task -->\n<!-- inflight-impact: ci -->\n<!-- inflight-state: deferred - parked -->\n<!-- inflight-state: parked - deferred -->\n'
assert "a second inflight-type is rejected"                fail '# T\n\n<!-- inflight-type: task -->\n<!-- inflight-type: task -->\n<!-- inflight-impact: ci -->\n'
assert "a second inflight-impact is rejected"              fail '# T\n\n<!-- inflight-type: task -->\n<!-- inflight-impact: ci -->\n<!-- inflight-impact: ci -->\n'
assert "one of each still passes"                          pass '# T\n\n<!-- inflight-type: task -->\n<!-- inflight-impact: ci -->\n<!-- inflight-state: deferred - after v6 -->\n'

echo
if [ "$failures" -eq 0 ]; then echo "All check-inflight-tags self-tests passed"; exit 0; fi
echo "$failures self-test(s) FAILED"
exit 1
