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
#   12. two independently added fragments both read; deferrals NAMED    -> pass (0)
#   13. a reactor module with no row and no deferral                    -> FAIL (1)
#   14. a module added only to a NESTED aggregator's <modules>          -> FAIL (1)
#   15. a feature record naming a module in no <modules> list           -> FAIL (1)
#   16. a maturity row naming a module in no <modules> list             -> FAIL (1)
#   17. the same module keyed in both a fragment and the root file      -> FAIL (1)
#   18. an evidence_id that resolves to no module_evidence entry        -> FAIL (1)
#   19. a maturity row's feature path that does not resolve             -> FAIL (1)
#   20. a deferral missing its reason                                   -> FAIL (1)
#   21. a deferral written into the shared ROOT file                    -> FAIL (1)
#   22. a fragment whose artifact does not match its filename           -> FAIL (1)
#   23. a fragment carrying the wrong kind for its directory            -> FAIL (1)
#   24. a fragment_collection naming a collection no contract governs   -> FAIL (1)
#
# Cases 12-24 pin the reactor cross-checks and the per-module fragment split. Case 13 is the gap
# the cross-check exists to close: a module in a <modules> list with no maturity row used to pass
# clean, and the repo carries the scar of the reverse direction (cases 15/16) - feature records
# whose published coordinates could not resolve. Case 17 is the failure mode the fragment merge
# itself introduces, and 22 pins the property the split exists for: the filename is the ownership
# claim, so no two waves can write the same module's data.
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
  # Herestring, not `printf | grep -q`, for the reason set out at the `grep -qE` call in
  # check-review-posted.sh: grep -q exits on its first match, printf takes EPIPE, and `pipefail`
  # promotes 141 to the pipeline's status - so finding the pattern would FAIL the case. Written
  # here as a pipe on the first pass, in a file that documents the trap, and caught by
  # bin/check-shell-sigpipe.sh rather than by review.
  if [ "$total" -eq "$want" ] && grep -q "$pattern" <<<"$output"; then
    printf 'ok:   %s (%s problem(s) total, mentioning %s)\n' "$label" "$total" "$pattern"
  else
    printf 'FAIL: %s - expected exactly %s problem(s) mentioning %s, got %s. The guard said:\n' \
      "$label" "$want" "$pattern" "$total"
    printf '%s\n' "$output" | sed 's/^/      | /'
    failures=$((failures + 1))
  fi
}

# expect_mentioning <expected-exit> <pattern> <label> -- asserts the verdict AND that the guard said why
#
# `expect` checks only the exit code, and `expect_problems` additionally pins an exact problem
# count - too strict for the reactor cross-checks, where one fixture legitimately trips several
# related findings at once (a renamed fragment artifact is simultaneously a filename mismatch, a
# coverage gap and an unresolvable module). This asserts the verdict plus the one line that
# matters, and also serves the passing direction: exit 0 plus a DEFERRED line proves a fragment
# was read, which a bare exit code cannot.
expect_mentioning() {
  local want=$1 pattern=$2 label=$3 got output
  output=$("$GUARD" 2>&1)
  got=$?
  # Herestring, not a pipe into grep -q - same SIGPIPE trap as expect_problems above.
  if [ "$got" -eq "$want" ] && grep -q "$pattern" <<<"$output"; then
    printf 'ok:   %s (exit %s, mentioning %s)\n' "$label" "$got" "$pattern"
  else
    printf 'FAIL: %s - expected exit %s mentioning "%s", got exit %s. The guard said:\n' \
      "$label" "$want" "$pattern" "$got"
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

# Case 12: no mutation. Fragments written by independent waves in independent files are all read
# into one corpus, and every deferral is named on a GREEN run - a deferral that only surfaced on
# failure would be forgotten precisely while everything passes.
expect_mentioning 0 "DEFERRED: module-maturity for parallel-consumer-proxy" \
  "a deferred fragment passes, and the deferral is named in the output"
expect_mentioning 0 "DEFERRED: module-maturity for parallel-consumer-example-core" \
  "a second, independently added fragment in the same corpus is also read"
expect_mentioning 0 "DEFERRED: testing-evidence for parallel-consumer-proxy" \
  "the testing-evidence corpus reads its fragments too"

# Case 13: the gap the cross-check closes. Emptying the module's fragment leaves a reactor module
# with no row and no deferral, which used to pass clean.
mutate docs/data/module-maturity.d/parallel-consumer-proxy.yaml \
  '"# fragment emptied: this module now has no row and no deferral anywhere\n"'
expect_problems 1 "module 'parallel-consumer-proxy' has no module-maturity row" \
  "a reactor module with no row and no deferral is caught, naming the module"
restore

mutate parallel-consumer-examples/pom.xml \
  't.replace("<module>parallel-consumer-example-core</module>", "<module>parallel-consumer-example-core</module>\n        <module>parallel-consumer-example-phantom</module>", 1)'
expect_problems 1 "parallel-consumer-example-phantom" \
  "a module added only to a nested aggregator's <modules> is caught"
restore

mutate docs/features/commit-modes.yaml \
  't.replace("module: parallel-consumer-core\n", "module: parallel-consumer-ghost\n", 1)'
expect_problems 1 "parallel-consumer-ghost" \
  "a feature record naming a module in no <modules> list is caught"
restore

# Renaming the row's artifact trips the reverse check AND leaves the real module uncovered, so
# this asserts the line rather than a count.
mutate docs/data/module-maturity.yaml \
  't.replace("artifact: parallel-consumer-vertx\n", "artifact: parallel-consumer-vertxx\n", 1)'
expect_mentioning 1 "names module 'parallel-consumer-vertxx', which is in no pom.xml" \
  "a maturity row naming a module in no <modules> list is caught"
restore

mutate docs/data/module-maturity.yaml \
  't.replace("modules:\n", "modules:\n  - artifact: parallel-consumer-proxy\n    maturity: alpha\n    reliability_confidence: Fixture row.\n    api_expectation: Fixture row.\n    support_posture: Fixture row.\n    evidence_id: core\n    use_this_if: Fixture row.\n", 1)'
expect_problems 1 "duplicate module-maturity record for module 'parallel-consumer-proxy'" \
  "the same module keyed in a fragment and the root file is caught, not silently merged"
restore

mutate docs/data/module-maturity.yaml \
  't.replace("evidence_id: vertx\n", "evidence_id: no-such-evidence\n", 1)'
expect_problems 1 "no-such-evidence" \
  "an evidence_id resolving to no module_evidence entry is caught"
restore

mutate docs/data/module-maturity.yaml \
  't.replace("feature: ../features/mutiny-integration.yaml\n", "feature: ../features/no-such-feature.yaml\n", 1)'
expect_problems 1 "no-such-feature.yaml" \
  "a maturity row's feature path that does not resolve is caught"
restore

mutate docs/data/module-maturity.d/parallel-consumer-proxy.yaml \
  're.sub(r"      reason: >-\n(?:        [^\n]*\n)+", "", t, count=1)'
expect_problems 1 "requires 'reason'" \
  "a deferral missing its reason is caught"
restore

# A deferral in the ROOT file also duplicates the fragment's key, so assert the line, not a count.
mutate docs/data/module-maturity.yaml \
  't.replace("modules:\n", "modules:\n  - artifact: parallel-consumer-proxy\n    deferred:\n      reason: Fixture.\n      lifted_by: Fixture.\n", 1)'
expect_mentioning 1 "deferral lives in the module's own fragment" \
  "a deferral written into the shared root file is caught"
restore

# The rename is at once a filename mismatch, a coverage gap and an unresolvable module - assert
# the ownership line specifically.
mutate docs/data/module-maturity.d/parallel-consumer-proxy.yaml \
  't.replace("artifact: parallel-consumer-proxy\n", "artifact: parallel-consumer-prox\n", 1)'
expect_mentioning 1 "the filename is the ownership claim" \
  "a fragment whose artifact does not match its filename is caught"
restore

mutate docs/data/module-maturity.d/parallel-consumer-proxy.yaml \
  't.replace("kind: module-maturity\n", "kind: testing-evidence\n", 1)'
expect_mentioning 1 "must carry kind 'module-maturity'" \
  "a fragment carrying the wrong kind for its directory is caught"
restore

mutate docs/data/schema.yaml \
  't.replace("fragment_collection: modules\n", "fragment_collection: modulez\n", 1)'
expect_mentioning 1 "no item contract governs" \
  "a fragment_collection no item contract governs is caught in the schema itself"
restore

expect 0 "restored: the corpus is valid again"

if [ "$failures" -ne 0 ]; then
  printf '\ntest-check-docs-data: %s case(s) failed\n' "$failures"
  exit 1
fi
printf '\ntest-check-docs-data: all cases pass\n'
