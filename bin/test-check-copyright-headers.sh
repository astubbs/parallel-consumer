#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for bin/check-copyright-headers.sh.
#
# Builds throwaway git repos that simulate the fork's provenance model (an "upstream"
# commit pinned as the fork point, then fork-original work layered on top) and asserts
# the scanner's verdict for every rule:
#    1. upstream-derived file, unmodified, Confluent header        -> pass
#    2. upstream-derived file that LOST its header                 -> FAIL
#    3. upstream-derived file whose header was SWAPPED for fork's  -> FAIL
#    4. upstream-derived file MODIFIED, no modifications line      -> FAIL
#    5. upstream-derived file MODIFIED, holder named only in an
#       @author byline, no "Modifications Copyright" line          -> FAIL
#    6. upstream-derived file MODIFIED, dual header                -> pass
#    7. renamed upstream file, content unchanged, Confluent-only   -> pass
#    8. renamed upstream file, content changed, no mods line       -> FAIL
#    9. fork-original file whose path is a tail-substring of a
#       registered rename newpath (must NOT be misrouted)          -> pass
#   10. extraction of upstream code, no mods line                  -> FAIL
#   11. fork-original file with the fork header                    -> pass
#   12. fork-original file claiming Confluent copyright            -> FAIL
#   13. fork-original file with no recognised header               -> FAIL
#   14. clean repo                                                 -> exit 0
#   15. fork-point commit missing from history (shallow clone)     -> exit 0 (warn+skip);
#       exit 2 only with COPYRIGHT_CHECK_REQUIRE_FORK_POINT=1 (strict, as CI sets)
#
# Run: bin/test-check-copyright-headers.sh   (CI runs it before the real scan)

set -euo pipefail

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

confluent_file() { # <path> [body]
    printf '/*-\n * Copyright (C) 2020-2022 Confluent, Inc.\n */\nclass X { %s }\n' "${2:-}" > "$1"
}

dual_file() { # <path> [body]
    printf '/*-\n * Copyright (C) 2020-2022 Confluent, Inc.\n * Modifications Copyright (C) 2026 Antony Stubbs and contributors\n */\nclass X { %s }\n' "${2:-}" > "$1"
}

confluent_author_file() { # <path> [body] - holder named only in an @author byline, NO mods line
    printf '/*-\n * Copyright (C) 2020-2022 Confluent, Inc.\n *\n * @author Antony Stubbs and contributors\n */\nclass X { %s }\n' "${2:-}" > "$1"
}

fork_file() { # <path>
    printf '/*-\n * Copyright (C) 2026 Antony Stubbs and contributors\n */\nclass X {}\n' > "$1"
}

headerless_file() { # <path>
    printf 'class X {}\n' > "$1"
}

# --- Fixture A: one repo exercising rules 1-13 ------------------------------------
repoA="$WORK/a"
new_repo "$repoA"
confluent_file "$repoA/Upstream.java"                       # 1: stays untouched
confluent_file "$repoA/LosesHeader.java"                    # 2: header removed post-fork
confluent_file "$repoA/HeaderSwapped.java"                  # 3: header swapped for fork's post-fork
confluent_file "$repoA/ModifiedNoLine.java"                 # 4: modified post-fork, line forgotten
confluent_file "$repoA/ModifiedAuthorByline.java"           # 5: modified post-fork, @author only
confluent_file "$repoA/ModifiedDual.java"                   # 6: modified post-fork, line added
confluent_file "$repoA/RenamedSameOld.java"                 # 7: renamed verbatim post-fork
confluent_file "$repoA/RenamedChangedOld.java"              # 8: renamed WITH changes post-fork
mkdir "$repoA/sub"
confluent_file "$repoA/sub/Orig.java"                       # 9: renamed post-fork; newpath is a tail-substring trap
git -C "$repoA" add -A && git -C "$repoA" commit -qm upstream
fork_point_a=$(git -C "$repoA" rev-parse HEAD)

headerless_file "$repoA/LosesHeader.java"                   # rule 2 violation
fork_file "$repoA/HeaderSwapped.java"                       # rule 3 violation (mislabelled as fork-original)
confluent_file "$repoA/ModifiedNoLine.java" "int changed;"  # rule 4 violation
confluent_author_file "$repoA/ModifiedAuthorByline.java" "int changed;" # rule 5 violation (byline is no notice)
dual_file "$repoA/ModifiedDual.java" "int changed;"         # rule 6 conformant
git -C "$repoA" mv RenamedSameOld.java RenamedSame.java     # rule 7 conformant (verbatim)
git -C "$repoA" mv RenamedChangedOld.java RenamedChanged.java
confluent_file "$repoA/RenamedChanged.java" "int changed;"  # rule 8 violation
git -C "$repoA" mv sub/Orig.java sub/Renamed.java           # rule 9: verbatim rename, registered below
fork_file "$repoA/Renamed.java"                             # rule 9 conformant: fork-original whose path
                                                            #   tail-matches registered newpath sub/Renamed.java
confluent_file "$repoA/Extraction.java" "int extracted;"    # rule 10 violation
fork_file "$repoA/ForkGood.java"                            # rule 11 conformant
confluent_file "$repoA/ForkClaimsConfluent.java"            # rule 12 violation
headerless_file "$repoA/ForkNoHeader.java"                  # rule 13 violation
git -C "$repoA" add -A && git -C "$repoA" commit -qm fork

renames="RenamedSame.java|RenamedSameOld.java
RenamedChanged.java|RenamedChangedOld.java
sub/Renamed.java|sub/Orig.java"

out=$( (cd "$repoA" && COPYRIGHT_CHECK_FORK_POINT="$fork_point_a" \
        COPYRIGHT_CHECK_EXTRA_RENAMES="$renames" \
        COPYRIGHT_CHECK_EXTRA_EXTRACTIONS="Extraction.java" \
        bash "$SCANNER") 2>&1 ) && rc=0 || rc=$?
assert          "violating repo exits 1"                        1 "$rc"
assert_contains "detects upstream file losing its header"       "upstream-derived file has no copyright header): LosesHeader.java" "$out"
assert_contains "detects upstream file with header swapped"     "upstream-derived file lost its Confluent header): HeaderSwapped.java" "$out"
assert_contains "detects modified upstream file w/o mods line"  "upstream-derived file modified since the fork point but missing 'Modifications Copyright ... Antony Stubbs and contributors' line): ModifiedNoLine.java" "$out"
assert_contains "detects @author byline passed off as mods line" "upstream-derived file modified since the fork point but missing 'Modifications Copyright ... Antony Stubbs and contributors' line): ModifiedAuthorByline.java" "$out"
assert_contains "detects changed rename w/o mods line"          "renamed upstream file modified since the fork point but missing 'Modifications Copyright ... Antony Stubbs and contributors' line): RenamedChanged.java" "$out"
assert_contains "detects extraction w/o mods line"              "extraction of upstream-derived code but missing 'Modifications Copyright ... Antony Stubbs and contributors' line): Extraction.java" "$out"
assert_contains "detects fork file claiming Confluent"          "fork-original file claims Confluent copyright): ForkClaimsConfluent.java" "$out"
assert_contains "detects fork file with no header"              "missing 'Antony Stubbs and contributors' header): ForkNoHeader.java" "$out"
assert_contains "reports exactly 8 violations"                  "8 violation(s)" "$out"
case "$out" in
    *"ForkGood.java"*|*"Upstream.java"*|*"ModifiedDual.java"*|*"RenamedSame.java"*)
        echo "FAIL: conformant files were flagged"; failures=$((failures + 1)) ;;
    *) echo "ok:   conformant files (untouched, dual-header, verbatim rename, fork) not flagged" ;;
esac
case "$out" in
    *"sub/Renamed.java"*|*"): Renamed.java"*)
        echo "FAIL: rename newpath tail-substring misrouted (Renamed.java or sub/Renamed.java flagged)"; failures=$((failures + 1)) ;;
    *) echo "ok:   fork file tail-matching a rename newpath stays fork-original (not misrouted)" ;;
esac

# --- Fixture B: clean repo (rule 14) -----------------------------------------------
repoB="$WORK/b"
new_repo "$repoB"
confluent_file "$repoB/Upstream.java"
confluent_file "$repoB/Modified.java"
git -C "$repoB" add -A && git -C "$repoB" commit -qm upstream
fork_point_b=$(git -C "$repoB" rev-parse HEAD)
dual_file "$repoB/Modified.java" "int changed;"
fork_file "$repoB/ForkGood.java"
git -C "$repoB" add -A && git -C "$repoB" commit -qm fork

out=$( (cd "$repoB" && COPYRIGHT_CHECK_FORK_POINT="$fork_point_b" bash "$SCANNER") 2>&1 ) && rc=0 || rc=$?
assert "clean repo exits 0" 0 "$rc"
assert_contains "clean repo reports zero violations" "0 violation(s)" "$out"

# --- Fixture C: fork point not in history (rule 15) ---------------------------------
# Default: WARN and skip (exit 0) - can't determine provenance, so degrade gracefully (e.g. a
# shallow clone or a `mvn validate` build); the authoritative gate is copyright.yml (fetch-depth: 0).
out=$( (cd "$repoB" && COPYRIGHT_CHECK_FORK_POINT=0000000000000000000000000000000000000000 \
        bash "$SCANNER") 2>&1 ) && rc=0 || rc=$?
assert          "missing fork point skips (exit 0) by default" 0 "$rc"
assert_contains "missing fork point warns + explains the fix"  "fetch-depth: 0" "$out"
# Strict mode: COPYRIGHT_CHECK_REQUIRE_FORK_POINT=1 hard-fails (exit 2) - what CI sets.
out=$( (cd "$repoB" && COPYRIGHT_CHECK_FORK_POINT=0000000000000000000000000000000000000000 \
        COPYRIGHT_CHECK_REQUIRE_FORK_POINT=1 bash "$SCANNER") 2>&1 ) && rc=0 || rc=$?
assert          "missing fork point hard-fails (exit 2) in strict mode" 2 "$rc"
assert_contains "strict-mode missing fork point explains the fix"       "fetch-depth: 0" "$out"

# --- Structural guard: no SIGPIPE-prone pipes in the scanner ------------------------
# `printf | grep -q` (or piping into awk that `exit`s early) under `set -euo pipefail`
# randomly evaluates a MATCH as false when the reader exits before the writer finishes
# (SIGPIPE -> pipefail), misclassifying files. Seen live in CI: an upstream file flagged
# as fork-original. Membership tests must use herestrings (<<<), never pipes.
if grep -vE '^[[:space:]]*#' "$SCANNER" | grep -nE '\|[[:space:]]*grep -q|\|[[:space:]]*awk '; then
    echo "FAIL: scanner pipes into an early-exiting reader (SIGPIPE + pipefail misclassification risk) - use a herestring"
    failures=$((failures + 1))
else
    echo "ok:   scanner has no SIGPIPE-prone pipes into grep -q / awk"
fi

echo
if [ "$failures" -eq 0 ]; then
    echo "All scanner self-tests passed."
else
    echo "$failures scanner self-test(s) FAILED."
    exit 1
fi
