#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for bin/check-dependabot-coverage.sh - the "every ecosystem in the tree has an update
# path" guard.
#
# The guard exists because the failure it catches is invisible: an ecosystem Dependabot was never
# told about produces no error and no PR, which is indistinguishable from an ecosystem with nothing
# to update. A guard for an invisible failure is worth exactly as much as the evidence that it can
# fail, so every case below is a negative control with an asserted EXIT CODE, not merely a non-zero.
#
# THREE EXIT CODES, HELD APART ON PURPOSE:
#
#   0  config and tree agree
#   1  the CHECK could not run   (no config, unparseable YAML)
#   2  they DISAGREE             (an uncovered manifest, or an entry pointing at nothing)
#
# Case 1 is the integration arm - the REAL repository, which must be green - and cases 2-9 are
# hermetic fixture trees built in a temp directory, so the suite is deterministic on any machine
# and needs no toolchain beyond python3 + PyYAML.
#
# Cases:
#    1. the real repository                                     -> pass (0)   <- integration arm
#    2. fixture: gomod declared, go.mod present                 -> pass (0)
#    3. fixture: gomod declared at a directory that is missing  -> fail (2)
#    4. fixture: gomod declared at a directory with no go.mod   -> fail (2)
#    5. fixture: Cargo.toml in the tree, no cargo entry         -> fail (2)   <- the original defect
#    6. fixture: dependabot.yml is not valid YAML               -> fail (1)
#    7. fixture: no dependabot.yml at all                       -> fail (1)
#    8. fixture: manifests only under node_modules/ and target/ -> pass (0)
#    9. fixture: .csproj below the declared solution directory  -> pass (0)
#
# Cases 3 and 4 differ only in whether the directory EXISTS, and both are separately worth having:
# a path typo and a moved manifest are different mistakes with different fixes, and the guard says
# which.

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
guard="${script_dir}/check-dependabot-coverage.sh"
repo_root="$(cd "${script_dir}/.." && pwd)"

failures=0
workspace="$(mktemp -d)"
trap 'rm -rf "${workspace}"' EXIT

# Runs the guard against a fixture root and asserts its exit code.
expect() {
    local description="$1" expected="$2" root="$3"
    local actual=0
    PC_REPO_ROOT="${root}" bash "${guard}" >"${workspace}/out.txt" 2>&1 || actual=$?
    if [ "${actual}" -eq "${expected}" ]; then
        echo "  ok   ${description} (exit ${actual})"
    else
        echo "  FAIL ${description}: expected exit ${expected}, got ${actual}"
        sed 's/^/       | /' "${workspace}/out.txt"
        failures=$((failures + 1))
    fi
}

# Builds a fixture root: $1 = name, $2 = dependabot.yml body (empty string = no file), then any
# number of `path/to/file` arguments that are created empty.
fixture() {
    local name="$1" config="$2"
    shift 2
    local root="${workspace}/${name}"
    mkdir -p "${root}/.github"
    if [ -n "${config}" ]; then
        printf '%s\n' "${config}" >"${root}/.github/dependabot.yml"
    fi
    local path
    for path in "$@"; do
        mkdir -p "${root}/$(dirname "${path}")"
        : >"${root}/${path}"
    done
    printf '%s' "${root}"
}

echo "test-check-dependabot-coverage.sh"

# 1. The real repository. If this ever fails, the tree and the config have genuinely diverged -
#    fix the config, never this suite.
expect "the real repository is covered" 0 "${repo_root}"

# 2. Happy path: one ecosystem, declared, present.
root="$(fixture happy '
version: 2
updates:
  - package-ecosystem: "gomod"
    directory: "/client-go"
    schedule:
      interval: "daily"
' client-go/go.mod)"
expect "gomod declared and present" 0 "${root}"

# 3. The typo case: the entry names a directory that does not exist. Dependabot surfaces this only
#    in a settings page, so nothing else in the repo would ever say so.
root="$(fixture missing-dir '
version: 2
updates:
  - package-ecosystem: "gomod"
    directory: "/client-golang"
    schedule:
      interval: "daily"
' client-go/go.mod)"
expect "gomod declared at a directory that does not exist" 2 "${root}"

# 4. The moved-manifest case: the directory is real, the manifest is not in it.
root="$(fixture wrong-manifest '
version: 2
updates:
  - package-ecosystem: "gomod"
    directory: "/client-go"
    schedule:
      interval: "daily"
' client-go/README.md)"
expect "gomod declared at a directory holding no go.mod" 2 "${root}"

# 5. THE ORIGINAL DEFECT, reduced: a manifest in the tree that no entry covers. This is the shape
#    the repository was in for the whole of the language-proxy work.
root="$(fixture uncovered '
version: 2
updates:
  - package-ecosystem: "maven"
    directory: "/"
    schedule:
      interval: "daily"
' pom.xml client-rust/Cargo.toml)"
expect "a Cargo.toml no entry covers" 2 "${root}"

# 6. Unparseable config: Dependabot would open nothing at all, so this must not read as coverage.
root="$(fixture unparseable '
version: 2
updates:
  - package-ecosystem: "gomod"
   directory: "/client-go"
      schedule:
' client-go/go.mod)"
expect "dependabot.yml that is not valid YAML" 1 "${root}"

# 7. No config at all.
root="$(fixture absent '' client-go/go.mod)"
expect "no dependabot.yml at all" 1 "${root}"

# 8. Vendored and build-output manifests belong to somebody else's package. Scanning them would
#    make the guard permanently red on a tree that is perfectly covered.
root="$(fixture vendored '
version: 2
updates:
  - package-ecosystem: "maven"
    directory: "/"
    schedule:
      interval: "daily"
' pom.xml client-ts/node_modules/left-pad/package.json target/generated/go.mod)"
expect "manifests under node_modules/ and target/ are not counted" 0 "${root}"

# 9. .NET projects sit below the directory the solution - and the entry - names. A declared
#    ancestor covers them; requiring an entry per .csproj would be three entries for one module.
root="$(fixture nested-nuget '
version: 2
updates:
  - package-ecosystem: "nuget"
    directory: "/client-dotnet"
    schedule:
      interval: "daily"
' client-dotnet/Client.sln client-dotnet/src/Client/Client.csproj client-dotnet/tests/Tests/Tests.csproj)"
expect ".csproj files below the declared solution directory" 0 "${root}"

echo
if [ "${failures}" -ne 0 ]; then
    echo "${failures} case(s) failed"
    exit 1
fi
echo "all cases passed"
