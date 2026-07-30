#!/usr/bin/env bash
#
# Self-test for bin/check-copyright-headers.sh.
#
# Builds throwaway git repos that simulate the fork's provenance model (an "upstream"
# commit pinned as the fork point, then fork-original files layered on top) and asserts
# the scanner's verdict for every rule:
#   1. upstream-derived file keeping its Confluent header        -> pass
#   2. upstream-derived file that LOST its header                -> FAIL
#   3. fork-original file with the fork header                   -> pass
#   4. fork-original file claiming Confluent copyright           -> FAIL
#   5. fork-original file with no recognised header              -> FAIL
#   6. renamed upstream file registered as an exception          -> pass
#   7. clean repo                                                -> exit 0
#   8. fork-point commit missing from history (shallow clone)    -> exit 2
#
# Run: bin/test-check-copyright-headers.sh   (CI runs it before the real scan)

set -eu

SCANNER="$(cd "$(dirname "$0")" && pwd)/check-copyright-headers.sh"
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

failures=0

assert() { # <description> <expected> <actual>
    if [ "$2" = "$3" ]; then
        echo "ok:   $1"
    else
        echo "FAIL: $1 (expected '$2', got '$3')"
        failures=$((failures + 1))
    fi
}

assert_contains() { # <description> <needle> <haystack>
    case "$3" in
        *"$2"*) echo "ok:   $1" ;;
        *) echo "FAIL: $1 (output missing '$2')"; failures=$((failures + 1)) ;;
    esac
}

new_repo() { # <dir>
    git init -q "$1"
    git -C "$1" config user.email test@example.invalid
    git -C "$1" config user.name "scanner-test"
}

confluent_file() { # <path>
    printf '/*-\n * Copyright (C) 2020-2022 Confluent, Inc.\n */\nclass X {}\n' > "$1"
}

fork_file() { # <path>
    printf '/*-\n * Copyright (C) 2026 Antony Stubbs and contributors\n */\nclass X {}\n' > "$1"
}

headerless_file() { # <path>
    printf 'class X {}\n' > "$1"
}

# --- Fixture A: one repo exercising rules 1-6 ------------------------------------
repoA="$WORK/a"
new_repo "$repoA"
confluent_file "$repoA/Upstream.java"           # rule 1: stays good
confluent_file "$repoA/LosesHeader.java"        # rule 2: header removed post-fork
git -C "$repoA" add -A && git -C "$repoA" commit -qm upstream
fork_point_a=$(git -C "$repoA" rev-parse HEAD)

headerless_file "$repoA/LosesHeader.java"       # rule 2 violation
fork_file "$repoA/ForkGood.java"                # rule 3
confluent_file "$repoA/ForkClaimsConfluent.java" # rule 4 violation
headerless_file "$repoA/ForkNoHeader.java"      # rule 5 violation
confluent_file "$repoA/RenamedUpstream.java"    # rule 6: exception keeps Confluent
git -C "$repoA" add -A && git -C "$repoA" commit -qm fork

out=$( (cd "$repoA" && COPYRIGHT_CHECK_FORK_POINT="$fork_point_a" \
        COPYRIGHT_CHECK_EXTRA_EXCEPTIONS="RenamedUpstream.java" \
        bash "$SCANNER") 2>&1 ) && rc=0 || rc=$?
assert        "violating repo exits 1"                       1 "$rc"
assert_contains "detects upstream file losing its header"    "upstream-derived file has no copyright header): LosesHeader.java" "$out"
assert_contains "detects fork file claiming Confluent"       "fork-original file claims Confluent copyright): ForkClaimsConfluent.java" "$out"
assert_contains "detects fork file with no header"           "missing 'Antony Stubbs and contributors' header): ForkNoHeader.java" "$out"
assert_contains "reports exactly 3 violations"               "3 violation(s)" "$out"
case "$out" in
    *"ForkGood.java"*|*"Upstream.java"*|*"RenamedUpstream.java"*)
        echo "FAIL: conformant files were flagged"; failures=$((failures + 1)) ;;
    *) echo "ok:   conformant, upstream, and exception files not flagged" ;;
esac

# --- Fixture B: clean repo (rule 7) -----------------------------------------------
repoB="$WORK/b"
new_repo "$repoB"
confluent_file "$repoB/Upstream.java"
git -C "$repoB" add -A && git -C "$repoB" commit -qm upstream
fork_point_b=$(git -C "$repoB" rev-parse HEAD)
fork_file "$repoB/ForkGood.java"
git -C "$repoB" add -A && git -C "$repoB" commit -qm fork

out=$( (cd "$repoB" && COPYRIGHT_CHECK_FORK_POINT="$fork_point_b" bash "$SCANNER") 2>&1 ) && rc=0 || rc=$?
assert "clean repo exits 0" 0 "$rc"
assert_contains "clean repo reports zero violations" "0 violation(s)" "$out"

# --- Fixture C: fork point not in history (rule 8) ---------------------------------
out=$( (cd "$repoB" && COPYRIGHT_CHECK_FORK_POINT=0000000000000000000000000000000000000000 \
        bash "$SCANNER") 2>&1 ) && rc=0 || rc=$?
assert          "missing fork point exits 2"            2 "$rc"
assert_contains "missing fork point explains the fix"   "fetch-depth: 0" "$out"

echo
if [ "$failures" -eq 0 ]; then
    echo "All scanner self-tests passed."
else
    echo "$failures scanner self-test(s) FAILED."
    exit 1
fi
