#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for bin/check-docs-data.sh.
#
# Mutates the real corpus one way at a time, asserts the guard's verdict, and restores it. Each case
# is a gap the guard actually had and shipped green through:
#    1. baseline, corpus untouched                                      -> pass (0)
#    2. a cross-reference written as PROSE, pointing at a missing file  -> FAIL (1)
#    3. a required field removed from a NESTED item                     -> FAIL (1)
#    4. availability replaced by a scalar instead of a mapping          -> FAIL (1)
#    5. a README anchor FRAGMENT that does not exist                    -> FAIL (1)
#    6. an undeclared field inside availability.evidence                -> FAIL (1)
#    7. a schema contract that governs no collection                    -> FAIL (1)
#    8. availability.milestones as a SCALAR instead of a list           -> FAIL (1)
#    9. a required field EMPTIED rather than removed                    -> FAIL (1)
#   10. a field the kind never declared, where it declares an optional  -> FAIL (1)
#   11. an optional list with no required partner to extend             -> FAIL (1)
#
# Case 2 is the one worth keeping. The guard's first cross-reference check only resolved a string
# that was ENTIRELY a path, so it caught `path: foo.yaml` and missed `... see foo.yaml.` - which is
# how most of the corpus actually cites a sibling. It was proven with a negative control that broke
# the case which already worked, and shipped reporting green on the failure it was written for.
#
# Case 7 guards the guard's own completeness: a schema that declares a per-item contract the checker
# cannot locate must fail loudly, because the alternative is a contract everybody believes is
# enforced and nothing enforces. Six were in that state when this was written.
#
# Cases 10 and 11 are the same class caught a second time, in the optional lists. Those were pure
# documentation: nothing checked that a record's fields came from required plus optional, so the
# lists could say anything. Declaring an optional list is now what closes a field set, and case 11
# is the self-consistency half - an optional list with nothing to extend closes nothing.
#
# Run: bin/test-check-docs-data.sh   (CI runs it before the guard it protects)

set -uo pipefail

cd "$(dirname "$0")/.."

GUARD=bin/check-docs-data.sh
failures=0
restore_path=""
restore_copy=""

# Restore by copying bytes back, not by replaying a shell variable: command substitution strips
# trailing newlines, so a variable round-trip silently rewrites every fixture file it touches. The
# first version of this self-test did exactly that and left four corpus files modified while
# reporting every case green.
restore() {
  if [ -n "$restore_path" ] && [ -f "$restore_copy" ]; then
    cp "$restore_copy" "$restore_path"
    rm -f "$restore_copy"
    restore_path=""
    restore_copy=""
  fi
}
trap restore EXIT

# expect <expected-exit> <label> -- runs the guard and reports
#
# A failing case prints what the guard actually said. The first version sent the guard's output to
# /dev/null, so a red case in CI reported an exit code and nothing else - undiagnosable from the log,
# and it had to be reproduced by hand before anyone could see which record was at fault.
expect() {
  local want=$1 label=$2 got output
  output=$("$GUARD" 2>&1)
  got=$?
  if [ "$got" -eq "$want" ]; then
    printf 'ok:   %s (exit %s)\n' "$label" "$got"
  else
    printf 'FAIL: %s - expected exit %s, got %s. The guard said:\n' "$label" "$want" "$got"
    printf '%s\n' "$output" | sed 's/^/      | /'
    failures=$((failures + 1))
  fi
}

# expect_problems <expected-total> <pattern> <label> -- asserts HOW MANY problems, and about what
#
# `expect` compares the exit code, which is 1 for any number of problems from one upward. That makes
# it blind to a whole class of regression: a guard that floods the log with one problem per character
# exits 1, and so does the fixed guard that reports once. Case 8 exists to pin that difference, so it
# cannot be written with `expect`.
#
# It asserts the guard's OWN total, taken from its summary line, plus a pattern. Both are needed, and
# both were got wrong on the way here:
#   - Counting only lines matching the pattern passes against the bug. The per-character problems say
#     "should be a mapping, found str" and never name the field, so the pattern matched exactly one
#     line before the fix and one after.
#   - Asserting a total without a pattern passes for the wrong reason. The first version of case 8
#     wrote a scalar over a key whose list items followed it, which is not valid YAML; the guard
#     stopped at the parse error and reported exactly one problem, having never reached the code
#     under test.
expect_problems() {
  local want=$1 pattern=$2 label=$3 output total
  output=$("$GUARD" 2>&1)
  total=$(printf '%s\n' "$output" | sed -n 's/^check-docs-data: \([0-9]*\) structural problem(s).*/\1/p')
  total=${total:-0}
  if [ "$total" -eq "$want" ] && printf '%s\n' "$output" | grep -q "$pattern"; then
    printf 'ok:   %s (%s problem(s) total, mentioning %s)\n' "$label" "$total" "$pattern"
  else
    printf 'FAIL: %s - expected exactly %s problem(s) mentioning %s, got %s. The guard said:\n' \
      "$label" "$want" "$pattern" "$total"
    printf '%s\n' "$output" | sed 's/^/      | /'
    failures=$((failures + 1))
  fi
}

# mutate <file> <python-expression-on-t> - edits in place, remembering how to put it back
mutate() {
  restore_path=$1
  restore_copy=$(mktemp)
  cp "$1" "$restore_copy"
  python3 - "$1" "$2" <<'PYTHON'
import pathlib
import re
import sys

path, expression = sys.argv[1], sys.argv[2]
p = pathlib.Path(path)
t = p.read_text()
new = eval(expression, {"t": t, "re": re})  # noqa: S307 - fixture mutation, inputs are literals here
if new == t:
    print(f"self-test fixture did not change {path}", file=sys.stderr)
    sys.exit(2)
p.write_text(new)
PYTHON
  if [ $? -ne 0 ]; then
    printf 'FAIL: could not apply fixture to %s\n' "$1"
    failures=$((failures + 1))
  fi
}

expect 0 "baseline: the corpus is valid"

mutate docs/features/commit-modes.yaml \
  't.replace("boundaries:\n", "boundaries:\n  - Prose reference; see no-such-record.yaml for detail.\n", 1)'
expect 1 "a prose cross-reference to a missing file is caught"
restore

mutate docs/data/roadmap.yaml 't.replace("    blocks_1_0: false\n", "", 1)'
expect 1 "a required field missing from a nested entry is caught"
restore

mutate docs/features/vertx-integration.yaml \
  're.sub(r"availability:\n(?:  [^\n]*\n)+", "availability: published\n", t, count=1)'
expect 1 "availability as a scalar rather than a mapping is caught"
restore

mutate docs/features/commit-modes.yaml \
  't.replace("boundaries:\n", "boundaries:\n  - See README.adoc#no-such-anchor for detail.\n", 1)'
expect 1 "a README anchor fragment that does not exist is caught"
restore

mutate docs/features/commit-modes.yaml 't.replace("    basis:", "    commit: deadbeef\n    basis:", 1)'
expect 1 "an undeclared field inside availability.evidence is caught"
restore

mutate docs/data/schema.yaml \
  't.replace("    item_contracts:\n      entry_required: entries\n", "    item_contracts: {}\n", 1)'
expect 1 "a schema contract governing no collection is caught"
restore

# Replaces the key AND its list items, so the result is valid YAML and the guard actually reaches
# the milestones check. `not-a-list` is 10 characters: the pre-fix per-character loop reported 11
# problems here (one per character, plus the type error), and exited 1 - same verdict as the fix.
mutate docs/features/ordering-modes.yaml \
  're.sub(r"  milestones:\n(?:    [^\n]*\n)+", "  milestones: not-a-list\n", t, count=1)'
expect_problems 1 "availability.milestones" \
  "milestones as a scalar reports once, not once per character"
restore

mutate docs/features/commit-modes.yaml 're.sub(r"^summary: .*$", "summary: \"\"", t, count=1, flags=re.M)'
expect 1 "a required field emptied rather than removed is caught"
restore

mutate docs/features/commit-modes.yaml 't.replace("kind: feature\n", "kind: feature\nundeclared_field: x\n", 1)'
expect 1 "a field the kind never declared is caught"
restore

mutate docs/data/schema.yaml \
  't.replace("  roadmap:\n    required:\n", "  roadmap:\n    stray_optional:\n      - x\n    required:\n", 1)'
expect 1 "an optional list with no required partner is caught"
restore

expect 0 "restored: the corpus is valid again"

if [ "$failures" -ne 0 ]; then
  printf '\ntest-check-docs-data: %s case(s) failed\n' "$failures"
  exit 1
fi
printf '\ntest-check-docs-data: all cases pass\n'
