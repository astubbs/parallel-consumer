#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Fails when .github/dependabot.yml and the manifests actually in the tree disagree - in either
# direction.
#
# WHY THIS EXISTS
#
# For most of this repo's life `dependabot.yml` declared two ecosystems, `maven` and
# `github-actions`, while the language-proxy work (astubbs#242) had brought seven more into the
# tree: Go modules, Cargo, npm, Bundler, pip, NuGet and Swift Package Manager. Nothing updated any
# of them, and - this is the part that matters - NOTHING SAID SO. A missing ecosystem produces no
# error, no warning and no PR; it is indistinguishable from an ecosystem with nothing to update.
# The gap surfaced only when a gRPC CVE was found by a CI scan rather than by a bot.
#
# That is the same class as `bin/check-ossindex-audit.sh`'s: a check that reports success without
# having run - see docs/solutions/workflow-issues/a-check-that-reports-success-without-having-run.md.
# Here the "check" is Dependabot itself, and the silent-green state is an ecosystem it was never
# told about. So the invariant is enforced rather than written down: adding a manifest for a new
# ecosystem now fails this check until an entry exists for it.
#
# TWO DIRECTIONS, BECAUSE EACH CATCHES SOMETHING THE OTHER CANNOT
#
#   real -> declared   a manifest in the tree with no `updates:` entry covering it. The original
#                      defect, and the one that recurs every time a language is added.
#   declared -> real   an entry whose `directory` does not exist, or holds no manifest of the
#                      ecosystem it claims. Dependabot reports this only in a repository settings
#                      page nobody opens, so a typo'd path is coverage that reads as present and
#                      is not. This leg is why the script resolves the manifest rather than
#                      trusting the string.
#
# WHAT IT DELIBERATELY DOES NOT CHECK: grouping, schedule, ignores. Those are policy calls that the
# config file argues for itself, entry by entry, and a checker asserting one shape would freeze a
# judgement that is supposed to move. This asserts only COVERAGE - that every ecosystem in the tree
# has an update path, and that every declared path is real.
#
# UNCOVERABLE, AND NAMED RATHER THAN IGNORED: parallel-consumer-proxy-client-cpp has no package
# manager at all (CMake resolves through find_package/PkgConfig against system packages), so there
# is no manifest, no lockfile and nothing for any updater to bump. It is listed in UNCOVERABLE
# below and printed on every run, including green ones, so "C++ is not covered" stays a stated fact
# rather than an absence somebody has to notice.
#
# EXIT CODES - the same split bin/check-ossindex-audit.sh uses, for the same reason:
#
#   0  config and tree agree
#   1  the CHECK could not run   (no config, unparseable YAML, no PyYAML) - nothing was learned
#   2  they DISAGREE             (an uncovered manifest, or an entry pointing at nothing)
#
# Do not collapse 1 and 2: a broken checker and a real coverage gap demand opposite responses, and
# an undifferentiated red cannot tell you which you have.
#
# Usage:
#   bin/check-dependabot-coverage.sh            # check this repository
#   PC_REPO_ROOT=<dir> bin/check-dependabot-coverage.sh   # check a fixture tree (self-test only)

set -euo pipefail

repo_root="${PC_REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"

python3 - "$repo_root" <<'PYTHON'
import os
import sys

try:
    import yaml
except ImportError:  # pragma: no cover - exercised by hand, not by the suite
    print("::error::PyYAML is not installed - this check cannot run "
          "(it ships on the ubuntu-latest runner image; locally: pip install pyyaml)")
    sys.exit(1)

root = sys.argv[1]
config_path = os.path.join(root, ".github", "dependabot.yml")

# ── The manifest table ───────────────────────────────────────────────────────────────────
# ecosystem -> the filenames that mean "this ecosystem lives in this directory". A directory
# satisfies an entry if it holds ANY of them; the tree is scanned for the same names.
MANIFESTS = {
    "gomod": ("go.mod",),
    "cargo": ("Cargo.toml",),
    "npm": ("package.json",),
    "bundler": ("Gemfile", "gems.rb"),
    "pip": ("pyproject.toml", "requirements.txt", "Pipfile", "setup.py"),
    "nuget": (".sln", ".csproj", ".fsproj", ".vbproj"),  # suffix match, see below
    "swift": ("Package.swift",),
    "maven": ("pom.xml",),
}

# Ecosystems whose single `/` entry covers the whole tree rather than one directory: Maven walks
# the reactor from the root pom's <modules>, and the actions updater reads every workflow file.
# For these, a manifest anywhere is covered by an entry at "/".
TREE_WIDE = {"maven", "github-actions"}

# Suffix-matched rather than name-matched: .NET project files are named after their project.
SUFFIX_MATCHED = {"nuget"}

# Never scanned: build output, vendored dependencies, and virtualenvs. Every one of these holds
# copies of manifests belonging to somebody else's package, which no updater here should chase.
SKIP_DIRS = {".git", "target", "node_modules", ".venv", "venv", "vendor", "build", "obj", "bin/obj",
             ".gradle", "dist", ".mypy_cache", ".pytest_cache", "__pycache__", ".build"}

# Genuinely uncoverable, printed on every run so it never reads as an oversight.
UNCOVERABLE = [
    ("parallel-consumer-proxy-clients/parallel-consumer-proxy-client-cpp",
     "C++ - no package manager: CMakeLists.txt resolves through find_package/PkgConfig against "
     "system packages, so there is no manifest and no version for any updater to bump. Its "
     "dependencies move when the runner image does."),
]


def broken(message):
    print(f"::error file=.github/dependabot.yml::{message}")
    sys.exit(1)


if not os.path.exists(config_path):
    broken("no .github/dependabot.yml - this check cannot conclude anything about coverage; "
           "restore the file rather than deleting the check")
try:
    with open(config_path, encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
except Exception as exc:
    broken(f"dependabot.yml does not parse as YAML ({str(exc).splitlines()[0]}) - "
           "Dependabot would reject it too, and open nothing at all")
if not isinstance(config, dict) or not isinstance(config.get("updates"), list) or not config["updates"]:
    broken("dependabot.yml has no `updates:` list - either the schema changed or the file was "
           "emptied; this check will not read that as 'nothing to cover'")


def directory_holds(directory, ecosystem):
    """True when `directory` (repo-relative, leading slash) holds a manifest for `ecosystem`."""
    names = MANIFESTS.get(ecosystem)
    if names is None:
        return True  # ecosystem this check has no table for - the other leg still applies
    absolute = os.path.join(root, directory.strip("/"))
    if not os.path.isdir(absolute):
        return None  # the directory itself is missing - a different message from "no manifest"
    entries = os.listdir(absolute)
    if ecosystem in SUFFIX_MATCHED:
        return any(name.endswith(suffix) for name in entries for suffix in names)
    return any(name in entries for name in names)


# ── Leg 1: declared -> real ──────────────────────────────────────────────────────────────
problems = []
declared = {}  # ecosystem -> set of repo-relative directories, normalised without a leading slash
for entry in config["updates"]:
    ecosystem = entry.get("package-ecosystem")
    directories = entry.get("directories") or [entry.get("directory", "/")]
    for directory in directories:
        declared.setdefault(ecosystem, set()).add(directory.strip("/"))
        if ecosystem in TREE_WIDE or "*" in directory:
            continue
        held = directory_holds(directory, ecosystem)
        if held is None:
            problems.append(f"`{ecosystem}` declares directory `{directory}`, which does not exist "
                            f"- Dependabot reports that only in the repository's Dependabot page, "
                            f"so this entry is coverage that reads as present and is not")
        elif held is False:
            expected = ", ".join(MANIFESTS.get(ecosystem, ()))
            problems.append(f"`{ecosystem}` declares directory `{directory}`, which holds none of "
                            f"({expected}) - the manifest moved, or the ecosystem is wrong")

# ── Leg 2: real -> declared ──────────────────────────────────────────────────────────────
name_to_ecosystem = {}
for ecosystem, names in MANIFESTS.items():
    if ecosystem in SUFFIX_MATCHED:
        continue
    for name in names:
        name_to_ecosystem.setdefault(name, ecosystem)

found = []  # (ecosystem, directory) pairs seen in the tree
for dirpath, dirnames, filenames in os.walk(root):
    dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS and not d.startswith(".claude")]
    relative = os.path.relpath(dirpath, root)
    relative = "" if relative == "." else relative
    for filename in filenames:
        ecosystem = name_to_ecosystem.get(filename)
        if ecosystem is None:
            for suffix_ecosystem in SUFFIX_MATCHED:
                if any(filename.endswith(suffix) for suffix in MANIFESTS[suffix_ecosystem]):
                    ecosystem = suffix_ecosystem
                    break
        if ecosystem is None:
            continue
        found.append((ecosystem, relative))

for ecosystem, directory in sorted(set(found)):
    covered = declared.get(ecosystem, set())
    if ecosystem in TREE_WIDE:
        if "" in covered:
            continue
    # A .NET project sits below the directory the solution (and the entry) names, so a nuget
    # manifest is covered by any declared ancestor. Same for a nested npm workspace.
    elif any(directory == d or directory.startswith(d + "/") for d in covered):
        continue
    where = directory or "/"
    problems.append(f"`{where}` holds a `{ecosystem}` manifest that NO `updates:` entry covers - "
                    f"nothing will ever propose a bump for it, and nothing else would report that. "
                    f"Add an entry, or record why it cannot have one")

print("## Dependabot coverage")
print()
for path, reason in UNCOVERABLE:
    print(f"- **uncoverable, by design:** `{path}` - {reason}")
print()
if problems:
    print(f"### :x: {len(problems)} coverage problem(s)")
    print()
    for problem in problems:
        print(f"- {problem}")
        print(f"::error file=.github/dependabot.yml::{problem}", file=sys.stderr)
    sys.exit(2)

ecosystems = sorted(declared)
print(f"- :white_check_mark: {len(ecosystems)} ecosystem(s) declared and resolved: "
      f"{', '.join(ecosystems)}")
print(f"- :white_check_mark: {len(set(found))} manifest director(ies) in the tree, all covered")
PYTHON
