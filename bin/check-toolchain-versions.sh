#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Asserts that `mise.toml` and the `.github/workflows/clients.yml` matrix declare the SAME toolchain
# versions.
#
# WHY IT EXISTS. The two install differently on purpose - mise locally, `setup-*` actions in CI,
# because those actions carry the client matrix's caching and `ruby/setup-ruby` is SHA-pinned. What
# is not on purpose is that they were two sources of truth for the same numbers with nothing
# comparing them, and they had already drifted by WHOLE MAJOR VERSIONS before anyone looked: dotnet
# 8.0.404 in CI against 9.0.101 on a developer machine, node 22.17.0 against 25.9.0. That is the
# classic shape of "passes locally, fails in CI" - and the nastier reverse, where CI runs the older
# runtime and the developer never sees the failure their change causes.
#
# WHAT IT DOES NOT DO. It does not check what is INSTALLED - that is
# `assert_toolchain_version` in bin/build-client.sh, which runs at build time against the machine.
# This one compares two declarations, so it is exact: a patch difference between two files that are
# both meant to say the same thing is a mistake, not tolerable drift.
#
# THE FOUR ABSENT LANGUAGES ARE CHECKED TOO, because "nobody declared it" and "it deliberately has no
# host toolchain" look identical otherwise: swift and cpp build in a container, kotlin and scala on
# the Maven reactor, so all four must be absent from mise.toml AND carry an empty matrix toolchain.
#
# Read-only: no network, no writes, nothing outside this repository. Safe under the `check-*` grant
# described in bin/AGENTS.md.
#
# Run: bin/check-toolchain-versions.sh

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# The two overrides exist only for bin/test-check-toolchain-versions.sh, which has to drive this
# gate against crafted fixtures to prove it can go red. bin/AGENTS.md's rule is that a regression
# test which has never failed proves nothing, and the only other way to make this one fail is to
# break the real repository.
MISE="${PC_MISE_TOML:-$REPO_ROOT/mise.toml}"
WORKFLOW="${PC_CLIENTS_WORKFLOW:-$REPO_ROOT/.github/workflows/clients.yml}"

for f in "$MISE" "$WORKFLOW"; do
    [ -f "$f" ] || {
        printf 'check-toolchain-versions: MISSING %s - cannot compare, so this is a FAILURE rather than a pass\n' "$f" >&2
        exit 1
    }
done

# python3 rather than a yaml/toml parser: it is already a hard dependency of the agent hooks and the
# conformance harness, and adding a parser dependency to a gate that guards the build is backwards.
python3 - "$MISE" "$WORKFLOW" <<'PY'
import re
import sys

mise_path, workflow_path = sys.argv[1], sys.argv[2]

# The matrix names the LANGUAGE; mise names the RUNTIME. They differ in exactly one place, and
# hard-coding that one mapping is clearer than inferring it.
LANGUAGE_TO_TOOL = {"typescript": "node"}

# Languages with no host toolchain by design. Named here so that a language quietly losing its
# version is a failure rather than joining this list by accident.
NO_HOST_TOOLCHAIN = {"swift", "cpp", "kotlin", "scala"}

mise_text = open(mise_path).read()
tools_block = re.search(r"^\[tools\]\s*$(.*?)(?=^\[|\Z)", mise_text, re.M | re.S)
if not tools_block:
    print("check-toolchain-versions: mise.toml has no [tools] section", file=sys.stderr)
    raise SystemExit(1)
declared = dict(re.findall(r'^\s*([A-Za-z0-9_-]+)\s*=\s*"([^"]+)"', tools_block.group(1), re.M))

workflow_text = open(workflow_path).read()
entries = re.findall(r"- language:\s*(\S+)(.*?)(?=\n\s*- language:|\Z)", workflow_text, re.S)
matrix = {}
for language, body in entries:
    found = re.search(r"toolchain:\s*'([^']*)'", body)
    if found is not None:
        matrix[language] = found.group(1)

if not matrix:
    print("check-toolchain-versions: parsed NO languages out of the workflow matrix - the format "
          "changed, and a gate that silently checks nothing is worse than no gate", file=sys.stderr)
    raise SystemExit(1)

problems = []

for language, ci_version in sorted(matrix.items()):
    tool = LANGUAGE_TO_TOOL.get(language, language)
    if language in NO_HOST_TOOLCHAIN:
        if ci_version:
            problems.append(f"{language}: builds without a host toolchain, but the CI matrix pins "
                            f"'{ci_version}'. Either it now needs one (add it to mise.toml) or the "
                            f"pin is stale.")
        if tool in declared:
            problems.append(f"{language}: builds without a host toolchain, but mise.toml declares "
                            f"{tool} = \"{declared[tool]}\".")
        continue
    if not ci_version:
        problems.append(f"{language}: the CI matrix declares no toolchain version. If that is "
                        f"deliberate, add it to NO_HOST_TOOLCHAIN in this script and say why.")
        continue
    if tool not in declared:
        problems.append(f"{language}: CI pins '{ci_version}' but mise.toml declares no {tool}.")
        continue
    if declared[tool] != ci_version:
        problems.append(f"{language}: mise.toml says {tool} = \"{declared[tool]}\", the CI matrix "
                        f"says '{ci_version}'. These must be identical.")

matrix_tools = {LANGUAGE_TO_TOOL.get(lang, lang) for lang in matrix}
for tool in sorted(set(declared) - matrix_tools):
    problems.append(f"mise.toml declares {tool} = \"{declared[tool]}\", which no language in the CI "
                    f"matrix uses. Remove it, or add the language that needs it.")

if problems:
    print("check-toolchain-versions: the two declarations disagree.\n", file=sys.stderr)
    for problem in problems:
        print(f"  - {problem}", file=sys.stderr)
    print("\nmise.toml is the single declaration; the workflow matrix must mirror it. They install\n"
          "differently on purpose - see the header of mise.toml.", file=sys.stderr)
    raise SystemExit(1)

checked = len(matrix)
print(f"check-toolchain-versions: {checked} language(s) agree between mise.toml and the CI matrix "
      f"({len(declared)} pinned, {len(NO_HOST_TOOLCHAIN & set(matrix))} deliberately without a host toolchain).")
PY
