#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for bin/check-upstream-map.sh.
#
# The arms that matter are the CONTROLS: each mutation below passed clean against the validator as
# it existed before astubbs#327, because `validate` read only `entries` and never looked at
# `branch_accounting` at all. A regression test that has never failed proves nothing, so every arm
# here was run against the old code first and observed to pass wrongly.
#
# Fixtures are built in a throwaway directory holding a COPY of the real manifest, so the real
# repository cannot make an arm pass by accident.

set -euo pipefail

cd "$(dirname "$0")/.."
REPO="$PWD"
MANIFEST="src/docs/development/upstream-map.yaml"

PY=""
for c in python3 python; do
  if command -v "$c" >/dev/null 2>&1 && "$c" -c '' >/dev/null 2>&1; then PY="$c"; break; fi
done
if [ -z "$PY" ] || ! "$PY" -c 'import yaml' >/dev/null 2>&1; then
  echo "test-check-upstream-map: no Python 3 with PyYAML, cannot self-test" >&2
  exit 2
fi

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

pass=0
fail=0

# Run the gate against a mutated copy of the tree and assert its exit code.
#
# TWO MUTATION FORMS, and the first is strongly preferred:
#
#   row:<yaml>   INSERT a synthetic entry at the top of `branch_accounting`
#   <old>=><new> anchored replace of existing text
#
# PREFER `row:`. The replace form anchors on REAL manifest content, which makes the test fail
# whenever that content legitimately changes - and it fails as "fixture anchor not found", which
# reads like a broken test rather than a moved row. That is not hypothetical: three arms anchored on
# `- {ref: presentation, state: ours,` and broke the day that branch was archived and its state
# became `deleted`. Worse, `bin/check-all.sh` without `--with-tests` does not run this file, so the
# breakage reached CI having passed a local sweep. A synthetic row depends on nothing but the schema
# it is testing.
#
# The replace form is kept only for arms that must corrupt the FILE rather than an entry - the
# unparseable-YAML case - and for asserting on the shape of a real row where that is the point.
#
# `sed` is deliberately avoided for either: these are YAML flow mappings on one long line and an
# anchored Python replace says exactly what changed.
arm() {
  local name="$1" want="$2" mutation="$3"
  local work="$TMP/$RANDOM$RANDOM"
  mkdir -p "$work/bin" "$work/scripts" "$work/src/docs/development"
  cp "$REPO/bin/check-upstream-map.sh" "$work/bin/"
  cp "$REPO/scripts/upstream-map.py" "$work/scripts/"
  cp "$REPO/$MANIFEST" "$work/$MANIFEST"

  if [ -n "$mutation" ]; then
    MUT="$mutation" MANIFEST="$MANIFEST" "$PY" - "$work" <<'PYTHON'
import os, pathlib, sys
work = pathlib.Path(sys.argv[1])
p = work / os.environ["MANIFEST"]
mut = os.environ["MUT"]
s = p.read_text()
if mut.startswith("row:"):
    key = "branch_accounting:\n"
    if key not in s:
        sys.exit("fixture: no branch_accounting section to insert into")
    i = s.index(key) + len(key)
    p.write_text(s[:i] + "  - " + mut[len("row:"):] + "\n" + s[i:])
else:
    old, new = mut.split("=>", 1)
    if old not in s:
        sys.exit(f"fixture anchor not found: {old!r}")
    p.write_text(s.replace(old, new, 1))
PYTHON
  fi

  set +e
  ( cd "$work" && bash bin/check-upstream-map.sh >/dev/null 2>&1 )
  local got=$?
  set -e

  if [ "$got" -eq "$want" ]; then
    echo "  ok    $name (exit $got)"
    pass=$((pass + 1))
  else
    echo "  FAIL  $name: wanted exit $want, got $got"
    fail=$((fail + 1))
  fi
}

echo "test-check-upstream-map:"

# The real manifest must be valid, or every negative arm below is meaningless.
arm "unmutated manifest is valid" 0 ""

# --- controls: each of these validated CLEAN before branch_accounting was covered -------------
#
# Every arm below inserts its OWN row. Nothing here reads a real entry, so an ordinary manifest edit
# - archiving a branch, retiring one, adding one - cannot turn these red.

arm "bogus branch state is rejected" 1 \
  "row:{ref: fixture/bad-state, tip: aaaaaaaa1, state: TYPO_NOT_A_STATE}"
arm "missing ref is rejected" 1 \
  "row:{tip: aaaaaaaa1, state: mirrored}"
arm "duplicate ref is rejected" 1 \
  "row:{ref: upstream/master, tip: aaaaaaaa1, state: mirrored}"
arm "'see' as a bare string is rejected" 1 \
  "row:{ref: fixture/bad-see, tip: aaaaaaaa1, state: mirrored, see: \"astubbs#1\"}"

# A tip that YAML parses as an integer never string-compares equal to `git rev-parse` output, so it
# is silently useless rather than absent. This one bit for real, in review on astubbs#327.
arm "integer tip is rejected" 1 \
  "row:{ref: fixture/int-tip, tip: 255916684, state: mirrored}"

arm "deleted branch with no date is rejected" 1 \
  "row:{ref: fixture/no-date, tip: aaaaaaaa1, state: deleted}"
arm "non-ISO deleted date is rejected" 1 \
  "row:{ref: fixture/bad-date, tip: aaaaaaaa1, state: deleted, deleted: last tuesday}"

# A tip is required only where nothing else can recover it. A live branch is one `git rev-parse`
# away, and this section's rule is "nothing a command answers" - so the pairs below are the point:
# the same omission is a finding once the branch is gone and legitimate while it is there. Both
# terminal states are covered, because a regression to just ("deleted",) would otherwise pass.
arm "deleted branch with no tip is rejected" 1 \
  "row:{ref: fixture/deleted-no-tip, state: deleted, deleted: 2026-08-26}"
arm "archived branch with no tip is rejected" 1 \
  "row:{ref: fixture/archived-no-tip, state: archived}"
arm "live branch with no tip is accepted" 0 \
  "row:{ref: fixture/live, state: ours}"
arm "archived branch WITH a tip is accepted" 0 \
  "row:{ref: fixture/archived, tip: aaaaaaaa1, state: archived}"

# --- the gate must not launder "could not run" into a pass ------------------------------------
# This one corrupts the FILE, not an entry, so it keeps the replace form.
arm "unparseable manifest is not a pass" 2 \
  "branch_accounting:=>branch_accounting: [unclosed"

echo "test-check-upstream-map: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
