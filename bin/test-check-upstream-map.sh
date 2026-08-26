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
# `sed` is deliberately avoided for the mutation: these are YAML flow mappings on one long line and
# an anchored Python replace says exactly what changed.
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
old, new = os.environ["MUT"].split("=>", 1)
s = p.read_text()
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
arm "bogus branch state is rejected" 1 \
  "state: mirrored=>state: TYPO_NOT_A_STATE"
arm "deleted branch with no date is rejected" 1 \
  "state: deleted, deleted: 2026-08-20=>state: deleted"
arm "non-ISO deleted date is rejected" 1 \
  "deleted: 2026-08-20=>deleted: last tuesday"
arm "duplicate ref is rejected" 1 \
  "- {ref: upstream/master,=>- {ref: upstream/pyallel-consumer,"
arm "'see' as a bare string is rejected" 1 \
  'see: [confluentinc#539],=>see: "confluentinc#539",'

# A tip that YAML parses as an integer never string-compares equal to `git rev-parse` output, so it
# is silently useless rather than absent. This one bit for real, in review on astubbs#327.
arm "integer tip is rejected" 1 \
  "tip: 4533f6d8d,=>tip: 255916684,"

# A tip is required only where nothing else can recover it. A live branch is one `git rev-parse`
# away, and this section's rule is "nothing a command answers" - so the pair below is the point:
# the same omission is a finding for a deleted branch and legitimate for a live one.
arm "deleted branch with no tip is rejected" 1 \
  "tip: 9a25939d7, state: deleted=>state: deleted"
arm "live branch with no tip is accepted" 0 \
  "- {ref: presentation, state: ours,=>- {ref: presentation, state: ours, extra_note: fine,"

# `archived` is the other half of the same rule and has no entry in the real manifest, so without
# this arm the tuple could regress to just ("deleted",) and nothing - data or test - would notice.
arm "archived branch with no tip is rejected" 1 \
  "- {ref: presentation, state: ours,=>- {ref: presentation, state: archived,"
arm "archived branch WITH a tip is accepted" 0 \
  "- {ref: presentation, state: ours,=>- {ref: presentation, tip: deadbeef1, state: archived,"

# --- the gate must not launder "could not run" into a pass ------------------------------------
arm "unparseable manifest is not a pass" 2 \
  "branch_accounting:=>branch_accounting: [unclosed"

echo "test-check-upstream-map: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
