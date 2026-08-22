#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Fails when a language row in .github/workflows/clients.yml would run no static analysis, or
# names one it cannot run.
#
# WHY THIS EXISTS
#
# The clients matrix runs each row's analyser as its own step, from `scanner-cmd`. One row - Swift -
# now leaves that field EMPTY on purpose: its module builds in a container whose Dockerfile already
# runs the same formatter on the way to the artifact stage, so a host step would be the same command
# written twice. The workflow therefore skips the static-analysis step when `scanner-cmd` is empty,
# and that skip is invisible: the row goes green having linted nothing, which is the exact shape of
# the failures this repository keeps recording.
#
# So an empty `scanner-cmd` has to mean something, and this check is what makes it mean it. A row
# may only leave it empty if it appears in DELEGATED below, and the entry names the FILE that runs
# the analysis instead and the command that must still be in it. Delete the lint from that
# Dockerfile and this check goes red, rather than the row quietly becoming a build with no lint.
#
# The two weaker rules beside it exist because they are nearly free once the matrix is parsed:
#
#   - Every row names its `scanner`. It is what the step title and the job summary say, so an empty
#     one leaves a reader unable to tell "no analyser" from "analyser not named".
#   - A `scanner-cmd` that starts with a path (scripts/analyse.sh, and the workflow's header asks
#     rows to prefer exactly that shape) must point at a file that exists in the module. A wave
#     renaming its own script is otherwise found by a red row.
#
# WHAT THIS DOES NOT CHECK, so a green run is not read as more than it is: whether the analyser is
# any good, whether its findings fail the row, and whether a scanner-cmd spelled out in full still
# matches the module's own configuration. docs/client-static-analysis.md owns the per-language
# policy; this check only ensures every row runs something, and that a row which delegates has
# somewhere real to delegate to.
#
# Exit: 0 clean, 1 findings, 2 the environment cannot run the check.
#
# Run: bin/check-client-scanners.sh   (CI runs it in the Repo Hygiene workflow)
#      bin/check-client-scanners.sh <root>   points it at a fixture tree; the self-test does this

set -euo pipefail

ROOT="${1:-}"
if [ -z "$ROOT" ]; then
    cd "$(dirname "$0")/.."
    ROOT="$PWD"
fi

PY=""
for c in python3 python; do
  if command -v "$c" >/dev/null 2>&1 && "$c" -c '' >/dev/null 2>&1; then PY="$c"; break; fi
done
if [ -z "$PY" ]; then
  echo "check-client-scanners: no working Python 3 on PATH" >&2
  exit 2
fi

if ! "$PY" -c 'import yaml' >/dev/null 2>&1; then
  echo "check-client-scanners: PyYAML is not installed. Install it with: $PY -m pip install pyyaml" >&2
  exit 2
fi

"$PY" - "$ROOT" <<'PYTHON'
import os
import sys

import yaml

root = sys.argv[1]

WORKFLOW = ".github/workflows/clients.yml"
CLIENTS_DIR = "parallel-consumer-proxy-clients"

# language -> (file inside the module, the command that must still be in it, why the row delegates)
#
# The only legal reason to leave `scanner-cmd` empty. Adding an entry is a claim that this file
# checks, so add it in the same commit that empties the field, and point it at the line that
# actually runs the analysis.
DELEGATED = {
    "swift": (
        "Dockerfile",
        "swift format lint",
        "the module builds in a container and lints on the way to the artifact stage, so a host "
        "step would be the same command written twice",
    ),
}

problems = []
checked = 0


def fail(message):
    problems.append(message)


def matrix_rows():
    path = os.path.join(root, WORKFLOW)
    if not os.path.exists(path):
        fail(f"{WORKFLOW}: not found - this check derives its scope from that workflow's matrix")
        return []
    try:
        with open(path, encoding="utf-8") as handle:
            doc = yaml.safe_load(handle)
    except Exception as exc:  # noqa: BLE001 - a parse failure is a finding, not a crash
        fail(f"{WORKFLOW}: does not parse as YAML - {exc}")
        return []
    try:
        rows = doc["jobs"]["client"]["strategy"]["matrix"]["include"]
    except (KeyError, TypeError):
        fail(f"{WORKFLOW}: no jobs.client.strategy.matrix.include - the clients matrix moved, and "
             "this check reads its scope from there; point it at the new location rather than "
             "letting it check nothing")
        return []
    if not isinstance(rows, list) or not rows:
        fail(f"{WORKFLOW}: the clients matrix is empty, so this check would verify nothing")
        return []
    return rows


def delegation_holds(language, module_dir):
    """The delegated file exists and still carries the command the entry claims for it."""
    relative, command, _reason = DELEGATED[language]
    path = os.path.join(root, module_dir, relative)
    if not os.path.isfile(path):
        fail(f"{language}: the row runs no static-analysis step of its own and delegates to\n"
             f"    {module_dir}/{relative}, which does not exist.\n"
             "    Restore it, give the row a `scanner-cmd` again, or update the DELEGATED entry in\n"
             "    bin/check-client-scanners.sh to name where the analysis moved to.")
        return False
    with open(path, encoding="utf-8") as handle:
        if command not in handle.read():
            fail(f"{language}: the row runs no static-analysis step of its own because\n"
                 f"    {module_dir}/{relative} is supposed to run `{command}` - and it no longer\n"
                 "    contains it. Nothing would lint this module, and the row would stay green.\n"
                 "    Put the command back, or give the row its own `scanner-cmd`.")
            return False
    return True


for row in matrix_rows():
    if not isinstance(row, dict) or "module" not in row or "language" not in row:
        fail(f"{WORKFLOW}: a matrix row carries no `language` and `module` pair: {row!r}")
        continue
    language = row["language"]
    module = row["module"]
    module_dir = f"{CLIENTS_DIR}/{module}"
    scanner = str(row.get("scanner") or "").strip()
    command = str(row.get("scanner-cmd") or "").strip()

    if not os.path.isdir(os.path.join(root, module_dir)):
        fail(f"{module}: the {language} row names a module with no directory at {module_dir}")
        continue

    checked += 1

    if not scanner:
        fail(f"{language}: the row names no `scanner`. That string titles the step and the job\n"
             "    summary, so an empty one leaves a reader unable to tell an unanalysed module\n"
             "    from an unnamed analyser.")
        continue

    if not command:
        if language not in DELEGATED:
            fail(f"{language}: the row has an empty `scanner-cmd`, so the workflow SKIPS its\n"
                 "    static-analysis step - the row would report green having linted nothing.\n"
                 "    Give it a command, or add the language to DELEGATED in\n"
                 "    bin/check-client-scanners.sh naming the file that analyses it instead.")
            continue
        if delegation_holds(language, module_dir):
            relative, delegated_command, reason = DELEGATED[language]
            print(f"ok:   {language} delegates its analysis to {relative} (`{delegated_command}`) - {reason}")
        continue

    first = command.split()[0]
    if "/" in first:
        script = os.path.join(root, module_dir, first)
        if not os.path.isfile(script):
            fail(f"{language}: `scanner-cmd` runs {first}, which does not exist under\n"
                 f"    {module_dir}. The step runs from the module directory, so this row would\n"
                 "    fail on a missing file rather than on a finding.")
            continue

    print(f"ok:   {language} runs {scanner} as its own step")

if problems:
    print()
    for problem in problems:
        print(f"FAIL: {problem}")
    print()
    print(f"{len(problems)} problem(s) across {checked} client row(s).")
    sys.exit(1)

print()
print(f"OK: {checked} client row(s); every one analyses its module, in a step or in its own build.")
PYTHON
