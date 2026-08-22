#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for bin/check-client-scanners.sh.
#
# Builds throwaway fixture trees in a temp dir - it never reads this checkout, which matters because
# the modules and the workflow the real check reads are being edited by other sessions while this
# runs, and a self-test whose baseline moves under it is a self-test nobody can trust.
#
#    1. a row with its own scanner-cmd, beside a delegating row      -> pass (0)
#    2. a row with an EMPTY scanner-cmd and no delegation            -> FAIL (1)
#    3. the delegating row's target file is missing                  -> FAIL (1)
#    4. the target exists but no longer runs the command             -> FAIL (1)
#    5. CONTROL: the same emptied scanner-cmd, on the language that
#       IS delegated, with its target intact                         -> pass (0)
#    6. a row that names no scanner at all                           -> FAIL (1)
#    7. a scanner-cmd naming a module script that does not exist     -> FAIL (1)
#    8. the same script, present                                     -> pass (0)
#    9. a row naming a module with no directory                      -> FAIL (1)
#   10. the matrix moved - no jobs.client.strategy.matrix.include    -> FAIL (1)
#   11. the matrix is present but empty                              -> FAIL (1)
#   12. the workflow is not there at all                             -> FAIL (1)
#
# Case 5 is the control arm, and it is the point. Case 2 proves the check can fail on an empty
# scanner-cmd; only 5 proves it fails on the emptiness being UNDECLARED rather than on emptiness
# itself, which is what makes Swift's deliberate carve-out legal and everyone else's accidental one
# red. Cases 3 and 4 are the other half: a declared delegation is a claim, and these are what turn
# it into a checked one.
#
# Cases 10-12 guard the guard. This check reads its scope from clients.yml's matrix, so a matrix
# that moved or emptied would leave it verifying nothing while still exiting 0 - the shape of the
# checks this repository has recorded reporting success without having run.
#
# Run: bin/test-check-client-scanners.sh   (CI runs it before the guard it protects)

set -uo pipefail

cd "$(dirname "$0")/.."

GUARD="$PWD/bin/check-client-scanners.sh"
WORK="$(mktemp -d)"
failures=0
cases=0

cleanup() { rm -rf "$WORK"; }
trap cleanup EXIT

# The language the guard's DELEGATED table knows about, and one that has to run its own step.
DELEGATED_LANGUAGE=swift
DELEGATED_MODULE=parallel-consumer-proxy-client-swift
STEP_LANGUAGE=ruby
STEP_MODULE=parallel-consumer-proxy-client-ruby

# workflow <dir> <ruby-scanner> <ruby-scanner-cmd> <swift-scanner> <swift-scanner-cmd>
#
# Written out per case rather than patched, because the field under test is the empty string and a
# patch that "removes" it is indistinguishable from a patch that missed.
workflow() {
    local dir=$1 ruby_scanner=$2 ruby_cmd=$3 swift_scanner=$4 swift_cmd=$5
    mkdir -p "$dir/.github/workflows"
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
          - language: $STEP_LANGUAGE
            module: $STEP_MODULE
            scanner: '$ruby_scanner'
            scanner-cmd: '$ruby_cmd'
          - language: $DELEGATED_LANGUAGE
            module: $DELEGATED_MODULE
            scanner: '$swift_scanner'
            scanner-cmd: '$swift_cmd'
    steps:
      - uses: actions/checkout@v6
YAML
}

# fixture <name> -- a baseline tree: one row running its own analyser, one delegating to its
# Dockerfile, and both modules present.
fixture() {
    local dir="$WORK/$1"
    mkdir -p "$dir/parallel-consumer-proxy-clients/$STEP_MODULE" \
             "$dir/parallel-consumer-proxy-clients/$DELEGATED_MODULE"
    workflow "$dir" "rubocop" "bundle exec rubocop" "swift-format, in the container" ""
    printf 'FROM swift:6.1\nRUN swift format lint --strict --recursive Sources\n' \
        >"$dir/parallel-consumer-proxy-clients/$DELEGATED_MODULE/Dockerfile"
    printf '%s\n' "$dir"
}

# expect <expected-exit> <label> <fixture-dir> [message-must-name...]
#
# Asserts the exit code AND, for a failing case, that the report names what the reader has to act
# on. A guard that fails with an unattributable message costs the next reader a bisect, and the exit
# code alone cannot tell the difference.
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

dockerfile() { printf '%s\n' "$1/parallel-consumer-proxy-clients/$DELEGATED_MODULE/Dockerfile"; }

# 1 - baseline
d="$(fixture baseline)"
expect 0 "baseline: one row with a command, one delegating to its container" "$d"

# 2 - the failure this check exists for: an empty command nobody declared
d="$(fixture undeclared-empty)"
workflow "$d" "rubocop" "" "swift-format, in the container" ""
expect 1 "an empty scanner-cmd on a language that does not delegate" "$d" \
    "$STEP_LANGUAGE" "SKIPS"

# 3 - the delegation names a file that is not there
d="$(fixture delegation-target-missing)"
rm "$(dockerfile "$d")"
expect 1 "the delegated file is missing" "$d" "Dockerfile" "$DELEGATED_LANGUAGE"

# 4 - the file is there, but the analysis has been taken out of it
d="$(fixture delegation-command-gone)"
printf 'FROM swift:6.1\nRUN swift build\n' >"$(dockerfile "$d")"
expect 1 "the delegated file no longer runs the analyser" "$d" \
    "swift format lint" "would stay green"

# 5 - CONTROL ARM: emptiness is legal exactly where it is declared and holds
d="$(fixture declared-empty)"
expect 0 "the delegated row's empty scanner-cmd, with its target intact" "$d"

# 6 - a row that names no analyser at all
d="$(fixture no-scanner-name)"
workflow "$d" "" "bundle exec rubocop" "swift-format, in the container" ""
expect 1 "a row with no scanner name" "$d" "names no"

# 7 - a scanner-cmd pointing at a module script that does not exist
d="$(fixture script-missing)"
workflow "$d" "analyse" "scripts/analyse.sh" "swift-format, in the container" ""
expect 1 "a scanner-cmd naming a script that is not in the module" "$d" "scripts/analyse.sh"

# 8 - control for 7: the same command, with the script present
d="$(fixture script-present)"
workflow "$d" "analyse" "scripts/analyse.sh" "swift-format, in the container" ""
mkdir -p "$d/parallel-consumer-proxy-clients/$STEP_MODULE/scripts"
printf '#!/usr/bin/env bash\n' >"$d/parallel-consumer-proxy-clients/$STEP_MODULE/scripts/analyse.sh"
expect 0 "a scanner-cmd naming a script the module really has" "$d"

# 9 - a row naming a module that is not there
d="$(fixture module-missing)"
rm -rf "$d/parallel-consumer-proxy-clients/$STEP_MODULE"
expect 1 "a row naming a module with no directory" "$d" "$STEP_MODULE"

# 10 - the matrix moved
d="$(fixture matrix-moved)"
cat >"$d/.github/workflows/clients.yml" <<'YAML'
name: Clients
on:
  pull_request:
jobs:
  client:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6
YAML
expect 1 "the clients matrix moved" "$d" "matrix moved"

# 11 - the matrix is there but empty
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
expect 1 "the clients matrix is empty" "$d" "verify nothing"

# 12 - no workflow at all
d="$(fixture workflow-missing)"
rm "$d/.github/workflows/clients.yml"
expect 1 "the clients workflow is missing" "$d" "not found"

printf '\n%s case(s), %s failure(s)\n' "$cases" "$failures"
[ "$failures" -eq 0 ] || exit 1
