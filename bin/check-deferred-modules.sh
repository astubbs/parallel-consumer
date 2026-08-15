#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Fails when a client module has real source while its maturity fragment still says `deferred:`.
#
# WHY THIS EXISTS
#
# .github/workflows/clients.yml derives each language row's fate from that module's own maturity
# fragment: `deferred:` present means the wave has not started, so the row writes the reason into
# the job summary and skips everything else - green-as-skipped, never red. That is exactly right
# for a seeded skeleton, and it has no counterpart in the other direction. A wave that writes a
# real client and forgets to lift the deferral gets a row that keeps reporting green while
# building, testing and linting nothing at all, indefinitely. `lifted_by:` is a note addressed to
# a human, not a check.
#
# This is the missing half. It is deliberately the smallest thing that closes it: for every module
# the clients matrix has a row for, source present beyond the skeleton AND `deferred:` still set is
# a failure that names the module and the fix.
#
# WHAT "BEYOND THE SKELETON" MEANS, AND WHY IT IS AN ALLOWLIST
#
# A deferred module may hold ONLY the files the plan's seed wrote:
#
#   - pom.xml               the Maven wrapper that puts the skeleton in the reactor
#   - README.md / .adoc     prose
#   - Dockerfile, .gitignore, .dockerignore, .gitattributes, LICENSE
#   - CMakeLists.txt, Package.swift, at the MODULE ROOT ONLY - the seeded, target-less package
#     manifest. These two names are here because the seed really did write them (C++ and Swift
#     each get one so the skeleton stays configurable on a machine whose wave has not started);
#     a manifest that declares dependencies - go.mod, Cargo.toml, package.json, pyproject.toml,
#     Gemfile, a .csproj - is NOT skeleton, and neither is a lockfile.
#   - toolchain-smoke/**    the seeded toolchain probe, which compiles a hello-world to prove the
#                           row's pinned toolchain still resolves before the wave starts
#
# Anything else - a source file, a test, a dependency manifest, a lockfile, a build script - is
# real source, and the deferral must go.
#
# An allowlist rather than a list of source extensions, on purpose. An extension list goes stale
# the moment a language arrives that it does not know, and it goes stale SILENTLY, which is the
# same failure this check exists to prevent. An allowlist errs the other way: a genuinely new
# skeleton file fails loudly and gets added here in review, with a reason.
#
# SCOPE: the modules `.github/workflows/clients.yml` has a matrix row for, read from the workflow
# rather than listed here, so a new row is covered the day it is added. Nothing else in the
# maturity corpus is in scope, and that is not an oversight - `deferred:` there also carries the
# unrelated meaning "a test, build-graph or aggregator module with no maturity claim to make"
# (parallel-consumer-proxy-conformance, ...-java-harness, the example modules). Those deferrals
# skip nothing and hide nothing. The hazard is specific to a fragment that a CI row reads as a
# reason not to run.
#
# Files are taken from git (`ls-files --cached --others --exclude-standard`), so build output,
# vendored dependencies and anything else ignored is out of the picture, and source a wave has
# written but not yet committed still counts.
#
# Exit: 0 clean, 1 findings, 2 the environment cannot run the check.
#
# Run: bin/check-deferred-modules.sh   (CI runs it in the Repo Hygiene workflow)
#      bin/check-deferred-modules.sh <root>   points it at a fixture tree; the self-test does this

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
  echo "check-deferred-modules: no working Python 3 on PATH" >&2
  exit 2
fi

if ! "$PY" -c 'import yaml' >/dev/null 2>&1; then
  echo "check-deferred-modules: PyYAML is not installed. Install it with: $PY -m pip install pyyaml" >&2
  exit 2
fi

"$PY" - "$ROOT" <<'PYTHON'
import os
import subprocess
import sys

import yaml

root = sys.argv[1]

WORKFLOW = ".github/workflows/clients.yml"
FRAGMENT_DIR = "docs/data/module-maturity.d"
CLIENTS_DIR = "parallel-consumer-proxy-clients"

# The seeded skeleton, and nothing else. See this script's header before adding to either.
SKELETON_FILES = {
    "pom.xml",
    "README.md",
    "README.adoc",
    "Dockerfile",
    ".dockerignore",
    ".gitignore",
    ".gitattributes",
    "LICENSE",
    # Module-root only - enforced below, not by this set.
    "CMakeLists.txt",
    "Package.swift",
}
SKELETON_ROOT_ONLY = {"CMakeLists.txt", "Package.swift"}
SKELETON_DIRS = ("toolchain-smoke/",)

problems = []
checked = 0


def fail(message):
    problems.append(message)


def is_skeleton(relative_path):
    """relative_path is relative to the module directory, with forward slashes."""
    if any(relative_path.startswith(prefix) for prefix in SKELETON_DIRS):
        return True
    if "/" in relative_path:
        return False  # a nested file is only skeleton via SKELETON_DIRS above
    if relative_path in SKELETON_ROOT_ONLY:
        return True
    return relative_path in SKELETON_FILES


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


def deferred_entry(module):
    """The module's fragment entry, and whether it carries a `deferred:` block.

    Every shape this cannot classify is a finding, never a fall-through to 'started' - the same
    stance the workflow's own gate step takes, for the same reason: an unreadable fragment means
    the corpus is broken, and guessing turns a bookkeeping defect into a misleading verdict.
    """
    fragment = os.path.join(FRAGMENT_DIR, f"{module}.yaml")
    path = os.path.join(root, fragment)
    if not os.path.exists(path):
        fail(f"{module}: maturity fragment {fragment} is missing - every client module carries "
             "one (astubbs#242), and clients.yml reads it to decide whether to run this row")
        return None
    try:
        with open(path, encoding="utf-8") as handle:
            doc = yaml.safe_load(handle)
    except Exception as exc:  # noqa: BLE001
        fail(f"{fragment}: does not parse as YAML - {exc}")
        return None
    entries = doc.get("modules") if isinstance(doc, dict) else None
    entry = next((item for item in entries or []
                  if isinstance(item, dict) and item.get("artifact") == module), None)
    if entry is None:
        fail(f"{fragment}: carries no modules entry for artifact '{module}' - the corpus is broken "
             "(bin/check-docs-data.sh pins its shape); restore the entry rather than letting this "
             "check, or clients.yml's gate, guess")
        return None
    return entry


def tracked_files(module_dir):
    """Everything git knows about under the module - committed or not, ignored files excluded."""
    result = subprocess.run(
        ["git", "-C", root, "ls-files", "--cached", "--others", "--exclude-standard", "--", module_dir],
        capture_output=True, text=True, check=False)
    if result.returncode != 0:
        fail(f"{module_dir}: `git ls-files` failed ({result.stderr.strip()}) - this check reads the "
             "module's contents from git")
        return []
    return [line for line in result.stdout.splitlines() if line]


for row in matrix_rows():
    if not isinstance(row, dict) or "module" not in row:
        fail(f"{WORKFLOW}: a matrix row carries no `module` field: {row!r}")
        continue
    module = row["module"]
    language = row.get("language", "?")
    module_dir = f"{CLIENTS_DIR}/{module}"
    if not os.path.isdir(os.path.join(root, module_dir)):
        fail(f"{module}: the {language} row names a module with no directory at {module_dir}")
        continue
    checked += 1
    entry = deferred_entry(module)
    if entry is None:
        continue
    if "deferred" not in entry:
        print(f"ok:   {module} is started - its {language} row builds, tests and lints it")
        continue

    real = []
    for path in tracked_files(module_dir):
        relative = path[len(module_dir) + 1:]
        if not is_skeleton(relative):
            real.append(relative)
    if not real:
        print(f"ok:   {module} is deferred and is still a seeded skeleton")
        continue

    shown = "\n".join(f"      {name}" for name in sorted(real)[:12])
    if len(real) > 12:
        shown += f"\n      ... and {len(real) - 12} more"
    fail(f"{module} has real source but its maturity fragment still says `deferred:`.\n"
         f"    clients.yml skips the {language} row on that basis, so nothing builds, tests or "
         "lints these files:\n"
         f"{shown}\n"
         f"    FIX: remove the `deferred:` block from {FRAGMENT_DIR}/{module}.yaml and write the\n"
         "    module's first real maturity row in its place (a started module's fragment is the\n"
         "    shape). That one edit is what starts the row.\n"
         "    If a file listed above really is part of the seeded skeleton, add it to\n"
         "    SKELETON_FILES or SKELETON_DIRS in bin/check-deferred-modules.sh in the same commit,\n"
         "    and say in the commit why it is skeleton rather than source.")

if problems:
    print()
    for problem in problems:
        print(f"FAIL: {problem}")
    print()
    print(f"{len(problems)} problem(s) across {checked} client module(s) with a clients.yml row.")
    sys.exit(1)

print()
print(f"OK: {checked} client module(s) with a clients.yml row; every deferred one is still a skeleton.")
PYTHON
