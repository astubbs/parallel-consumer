#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for bin/check-deferred-modules.sh.
#
# Builds throwaway fixture git repositories in a temp dir - it never touches this checkout, which
# matters here because the modules the real check reads are being written by other sessions while
# this runs, and a self-test whose baseline moves under it is a self-test nobody can trust.
#
#    1. a deferred skeleton beside a started module with source           -> pass (0)
#    2. SOURCE added to the deferred module                               -> FAIL (1)
#    3. a dependency lockfile added to the deferred module                -> FAIL (1)
#    4. a manifest that declares dependencies (go.mod) in the deferred one -> FAIL (1)
#    5. a README and a Dockerfile added to the deferred module            -> pass (0)
#    6. a nested toolchain-smoke/ probe in the deferred module            -> pass (0)
#    7. the case-2 source, with the DEFERRAL LIFTED                       -> pass (0)
#    8. the case-2 source, added to the STARTED module instead            -> pass (0)
#    9. a root Package.swift is skeleton; a NESTED one is not             -> FAIL (1)
#   10. build output the repo's .gitignore ignores                        -> pass (0)
#   11. the deferred module's fragment is missing                         -> FAIL (1)
#   12. the fragment carries no entry for its artifact                    -> FAIL (1)
#   13. the fragment does not parse as YAML                               -> FAIL (1)
#   14. a matrix row naming a module with no directory                    -> FAIL (1)
#   15. the matrix moved - no jobs.client.strategy.matrix.include         -> FAIL (1)
#   16. the matrix is present but empty                                   -> FAIL (1)
#
# Cases 7 and 8 are the control arms, and they are the point. Case 2 going red proves the check can
# fail; only 7 proves it fails for the reason claimed (lifting the deferral fixes it, without
# deleting a line of source) and only 8 proves the deferral half is load-bearing rather than the
# check simply disliking source. Without them, a check that failed on every module with source at
# all would pass this file.
#
# Cases 15 and 16 guard the guard. This check reads its scope from clients.yml's matrix, so a matrix
# that moved or emptied would leave it verifying nothing while still exiting 0 - the exact shape of
# the seven recorded checks in this repo that reported success without having run.
#
# Case 5 is the other half of case 2: a new FILE is not the trigger, a new kind of file is.
#
# Run: bin/test-check-deferred-modules.sh   (CI runs it before the guard it protects)

set -uo pipefail

cd "$(dirname "$0")/.."

GUARD="$PWD/bin/check-deferred-modules.sh"
WORK="$(mktemp -d)"
failures=0
cases=0

cleanup() { rm -rf "$WORK"; }
trap cleanup EXIT

DEFERRED_MODULE=parallel-consumer-proxy-client-swift
STARTED_MODULE=parallel-consumer-proxy-client-go

# fixture <name> -- prints the path to a fresh baseline repo: one deferred skeleton, one started
# module with real source, and a two-row matrix over them. Everything is committed, so the check
# reads it the way CI does; case mutations then arrive as uncommitted files, the way a wave's own
# working tree presents them.
fixture() {
    local dir="$WORK/$1"
    mkdir -p "$dir/.github/workflows" "$dir/docs/data/module-maturity.d" \
             "$dir/parallel-consumer-proxy-clients/$DEFERRED_MODULE" \
             "$dir/parallel-consumer-proxy-clients/$STARTED_MODULE/parallelconsumer"

    cat >"$dir/.github/workflows/clients.yml" <<YAML
name: Clients
on:
  pull_request:
jobs:
  client:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        include:
          - language: swift
            module: $DEFERRED_MODULE
          - language: go
            module: $STARTED_MODULE
    steps:
      - uses: actions/checkout@v6
YAML

    cat >"$dir/docs/data/module-maturity.d/$DEFERRED_MODULE.yaml" <<YAML
schema_version: 1
kind: module-maturity
modules:
  - artifact: $DEFERRED_MODULE
    deferred:
      reason: >-
        Empty skeleton seeded ahead of the fan-out.
      lifted_by: >-
        astubbs#242 - the Swift client wave.
YAML

    cat >"$dir/docs/data/module-maturity.d/$STARTED_MODULE.yaml" <<YAML
schema_version: 1
kind: module-maturity
modules:
  - artifact: $STARTED_MODULE
    maturity: alpha
YAML

    # The deferred module holds only what the seed wrote.
    echo '<project/>' >"$dir/parallel-consumer-proxy-clients/$DEFERRED_MODULE/pom.xml"
    echo 'FROM scratch' >"$dir/parallel-consumer-proxy-clients/$DEFERRED_MODULE/Dockerfile"
    printf '// swift-tools-version:5.9\nlet package = 0\n' \
        >"$dir/parallel-consumer-proxy-clients/$DEFERRED_MODULE/Package.swift"

    # The started module holds a real client.
    echo '<project/>' >"$dir/parallel-consumer-proxy-clients/$STARTED_MODULE/pom.xml"
    echo 'module example' >"$dir/parallel-consumer-proxy-clients/$STARTED_MODULE/go.mod"
    echo 'package parallelconsumer' \
        >"$dir/parallel-consumer-proxy-clients/$STARTED_MODULE/parallelconsumer/session.go"

    echo 'target/' >"$dir/.gitignore"

    git -C "$dir" init --quiet
    git -C "$dir" add -A >/dev/null
    git -C "$dir" -c user.email=t@example.invalid -c user.name=test commit --quiet -m fixture
    printf '%s\n' "$dir"
}

# expect <expected-exit> <label> <fixture-dir> [message-must-name...]
#
# Asserts the exit code AND, for a failing case, that the report names what the reader has to act
# on. A guard that fails with an unattributable message costs the next reader a bisect, and the
# exit code alone cannot tell the difference.
expect() {
    local want=$1 label=$2 dir=$3; shift 3
    local output got needle
    cases=$((cases + 1))
    output="$(bash "$GUARD" "$dir" 2>&1)"
    got=$?
    if [ "$got" -ne "$want" ]; then
        printf 'FAIL: %s - expected exit %s, got %s. The guard said:\n' "$label" "$want" "$got"
        printf '%s\n' "$output" | sed 's/^/      | /'
        failures=$((failures + 1))
        return
    fi
    for needle in "$@"; do
        if ! grep -F "$needle" >/dev/null <<<"$output"; then
            printf 'FAIL: %s - exit %s was right, but the report never mentions %s:\n' \
                "$label" "$got" "$needle"
            printf '%s\n' "$output" | sed 's/^/      | /'
            failures=$((failures + 1))
            return
        fi
    done
    printf 'ok:   %s (exit %s)\n' "$label" "$got"
}

deferred_dir() { printf '%s\n' "$1/parallel-consumer-proxy-clients/$DEFERRED_MODULE"; }
started_dir() { printf '%s\n' "$1/parallel-consumer-proxy-clients/$STARTED_MODULE"; }
fragment() { printf '%s\n' "$1/docs/data/module-maturity.d/$2.yaml"; }

# 1 - baseline
d="$(fixture baseline)"
expect 0 "baseline: a deferred skeleton beside a started module" "$d"

# 2 - the failure this check exists for
d="$(fixture source-in-deferred)"
mkdir -p "$(deferred_dir "$d")/Sources/ParallelConsumerProxyClient"
echo 'public struct Session {}' \
    >"$(deferred_dir "$d")/Sources/ParallelConsumerProxyClient/Session.swift"
expect 1 "source in a deferred module" "$d" \
    "$DEFERRED_MODULE" "Sources/ParallelConsumerProxyClient/Session.swift" "deferred"

# 3 - a lockfile is real dependency resolution, not a seed
d="$(fixture lockfile-in-deferred)"
echo '{"pins":[]}' >"$(deferred_dir "$d")/Package.resolved"
expect 1 "a lockfile in a deferred module" "$d" "Package.resolved"

# 4 - a manifest that declares dependencies is not the seeded target-less kind
d="$(fixture manifest-in-deferred)"
echo 'module example' >"$(deferred_dir "$d")/go.mod"
expect 1 "a dependency manifest in a deferred module" "$d" "go.mod"

# 5 - control: a new FILE is not the trigger; a new KIND of file is
d="$(fixture prose-in-deferred)"
echo '# Swift client' >"$(deferred_dir "$d")/README.md"
echo 'target/' >"$(deferred_dir "$d")/.gitignore"
expect 0 "prose and ignore rules added to a deferred module" "$d"

# 6 - the toolchain probe the seed really did write, nested source and all
d="$(fixture smoke-in-deferred)"
mkdir -p "$(deferred_dir "$d")/toolchain-smoke/Sources/Smoke"
echo 'print("smoke")' >"$(deferred_dir "$d")/toolchain-smoke/Sources/Smoke/main.swift"
echo '// swift-tools-version:5.9' >"$(deferred_dir "$d")/toolchain-smoke/Package.swift"
expect 0 "a seeded toolchain-smoke probe in a deferred module" "$d"

# 7 - CONTROL ARM: the same source, with the deferral lifted. Proves the fix is the fix.
d="$(fixture source-with-deferral-lifted)"
mkdir -p "$(deferred_dir "$d")/Sources/ParallelConsumerProxyClient"
echo 'public struct Session {}' \
    >"$(deferred_dir "$d")/Sources/ParallelConsumerProxyClient/Session.swift"
cat >"$(fragment "$d" "$DEFERRED_MODULE")" <<YAML
schema_version: 1
kind: module-maturity
modules:
  - artifact: $DEFERRED_MODULE
    maturity: alpha
YAML
expect 0 "the same source, once the deferral is lifted" "$d"

# 8 - CONTROL ARM: the same source in a module that was never deferred
d="$(fixture source-in-started)"
mkdir -p "$(started_dir "$d")/cmd/conformance-runner"
echo 'package main' >"$(started_dir "$d")/cmd/conformance-runner/main.go"
expect 0 "the same shape of source in a started module" "$d"

# 9 - the module-root allowance really is root-only
d="$(fixture nested-manifest)"
mkdir -p "$(deferred_dir "$d")/Sources/Client"
printf '// swift-tools-version:5.9\n' >"$(deferred_dir "$d")/Sources/Client/Package.swift"
expect 1 "a nested Package.swift is source, not the seeded root manifest" "$d" \
    "Sources/Client/Package.swift"

# 10 - git decides what counts, so build output never does
d="$(fixture ignored-output)"
mkdir -p "$(deferred_dir "$d")/target/classes"
echo 'binary' >"$(deferred_dir "$d")/target/classes/Session.class"
expect 0 "ignored build output under a deferred module" "$d"

# 11 - a missing fragment is a corpus defect, never a fall-through to 'started'
d="$(fixture fragment-missing)"
rm "$(fragment "$d" "$DEFERRED_MODULE")"
expect 1 "the fragment is missing" "$d" "$DEFERRED_MODULE" "missing"

# 12 - present but silent about this artifact
d="$(fixture fragment-no-entry)"
cat >"$(fragment "$d" "$DEFERRED_MODULE")" <<'YAML'
schema_version: 1
kind: module-maturity
modules:
  - artifact: some-other-module
    maturity: alpha
YAML
expect 1 "the fragment carries no entry for its artifact" "$d" "no modules entry"

# 13 - unparseable is a defect, not a licence to guess
d="$(fixture fragment-unparseable)"
printf 'modules:\n  - artifact: [unclosed\n' >"$(fragment "$d" "$DEFERRED_MODULE")"
expect 1 "the fragment does not parse" "$d" "does not parse"

# 14 - a row pointing at nothing would check nothing, quietly
d="$(fixture row-without-module)"
rm -rf "$(deferred_dir "$d")"
expect 1 "a matrix row naming a module with no directory" "$d" "no directory"

# 15 - GUARDS THE GUARD: the matrix moved
d="$(fixture matrix-moved)"
cat >"$d/.github/workflows/clients.yml" <<'YAML'
name: Clients
on:
  pull_request:
jobs:
  something-else:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6
YAML
expect 1 "the clients matrix moved out from under this check" "$d" "no jobs.client"

# 16 - GUARDS THE GUARD: the matrix emptied
d="$(fixture matrix-empty)"
cat >"$d/.github/workflows/clients.yml" <<'YAML'
name: Clients
on:
  pull_request:
jobs:
  client:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        include: []
    steps:
      - uses: actions/checkout@v6
YAML
expect 1 "the clients matrix is empty" "$d" "empty"

echo
if [ "$failures" -gt 0 ]; then
    echo "FAILED: $failures of $cases case(s) - bin/check-deferred-modules.sh does not behave as documented."
    exit 1
fi
echo "PASSED: all $cases case(s)."
