#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Validate src/docs/development/upstream-map.yaml against its declared schema.
#
# WHY THIS EXISTS
#
# `scripts/upstream-map.py validate` has existed for a long time and was run by nobody: no workflow,
# no gate, no hook. The manifest is the source of truth for the fork<->upstream mapping and is
# hand-edited at every lifecycle transition, so a typo in a status or a state is exactly the kind of
# error that passes review and then quietly misroutes the next sweep.
#
# The second half of the same failure is what prompted this: astubbs#327 added a whole new
# `branch_accounting` section and `validate` read only `entries`, so a bogus state validated clean
# while the run printed a reassuring count of the section it HAD looked at. A gate reporting success
# about something it never inspected is worse than no gate.
#
# This wrapper is deliberately thin - the schema knowledge stays in the Python, which is also the
# renderer, so the two cannot disagree about what a field means. It reads files only and touches no
# network, which is what keeps the `check-` prefix honest under bin/AGENTS.md.
#
# Exit 0 valid, 1 schema errors, 2 cannot run (no Python or no PyYAML - never a pass).

set -euo pipefail

cd "$(dirname "$0")/.."

MANIFEST="src/docs/development/upstream-map.yaml"

if [ ! -f "$MANIFEST" ]; then
  echo "check-upstream-map: $MANIFEST does not exist" >&2
  exit 2
fi

PY=""
for c in python3 python; do
  if command -v "$c" >/dev/null 2>&1 && "$c" -c '' >/dev/null 2>&1; then PY="$c"; break; fi
done
if [ -z "$PY" ]; then
  echo "check-upstream-map: no working Python 3 on PATH" >&2
  exit 2
fi

if ! "$PY" -c 'import yaml' >/dev/null 2>&1; then
  echo "check-upstream-map: PyYAML is not installed. Install it with: $PY -m pip install pyyaml" >&2
  exit 2
fi

# Distinguish "the manifest is invalid" (exit 1) from "the validator itself blew up" (exit 2).
# Without this an unparseable YAML file and a broken script are indistinguishable, and the repo's
# standing rule is that a skip must never read as a pass.
set +e
out="$("$PY" scripts/upstream-map.py validate 2>&1)"
rc=$?
set -e

printf '%s\n' "$out"

if [ "$rc" -eq 0 ]; then
  exit 0
fi

# Herestring, not a pipe: `printf | grep -q` takes EPIPE under pipefail and turns a MATCH into a
# failure. bin/check-shell-sigpipe.sh enforces this across the directory.
if grep -q '^INVALID:' <<<"$out"; then
  echo "check-upstream-map: $MANIFEST does not satisfy its schema" >&2
  exit 1
fi

echo "check-upstream-map: the validator failed to run (exit $rc), so nothing was checked" >&2
exit 2
