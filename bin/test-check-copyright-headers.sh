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
# Fixture D (cases 16-24) is the NEGATIVE CONTROL for the package move, and it is the reason the
# scanner resolves provenance through PACKAGE_MOVES instead of by path equality. Its files sit under
# bz/stub/... having been `git mv`d there from io/confluent/... after the fork point, exactly as
# bin/rename-packages.sh leaves them. Under the old path-equality model EVERY one of them missed the
# fork-point lookup, was judged fork-original, and its retained Confluent header was reported as a
# violation - the verdict INVERTS rather than degrading (measured on the real tree: 0 -> 197
# violations, and `./mvnw` dying in the validate phase before any goal ran). So cases 16, 18, 20 and
# 21 below FAIL against the old model, and case 17 fails with the WRONG message. Fixtures A-C keep
# the un-renamed spelling, so both tree states are covered - which is the actual requirement while
# the rename rolls out branch by branch.
#
#   16. upstream file MOVED verbatim by the rename, Confluent-only -> pass (still upstream-derived)
#   17. upstream file MOVED and edited, no modifications line      -> FAIL, as an upstream file
#   18. upstream file MOVED and edited, dual header                -> pass
#   19. upstream file MOVED with its header removed                -> FAIL (upstream, not fork-original)
#   20. io.confluent.csid.utils file MOVED and edited, dual header -> pass, through the NESTED rule
#       (bz/stub/parallelconsumer/internal/utils)
#   21. io.confluent.csid.testcontainers file MOVED, dual header   -> pass, through the SECOND
#       nested rule (bz/stub/parallelconsumer/internal/testcontainers). There is no general rule for
#       this prefix any more: the one that used to catch it mapped onto a same-named prefix under
#       bz/stub, carrying upstream's mark into the new namespace
#   22. registered rename whose newpath was RETARGETED to bz/stub  -> FAIL without the mods line
#   23. registered rename still written in the OLD spelling while
#       the file has already moved (the half-renamed tree that
#       exists between the rename's two commits)                   -> FAIL without the mods line
#   24. fork-original file under the NEW package claiming
#       Confluent                                                  -> FAIL (the rule must not
#       classify everything under bz/stub as upstream-derived)
#   25. the scanner's PACKAGE_MOVES agrees with bin/rename-packages.sh's PKG_MAP (drift guard) -
#       EVERY rule, not just the two-segment ones, so a nested rule cannot drift unwatched
#   26. CONTROL ARM: put the general rule ABOVE the nested ones and the moved files under them are
#       misjudged as fork-original, turning their required upstream headers into violations. The
#       ordering constraint is therefore measured, not asserted in a comment
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

# --- Fixture D: the package move (rules 16-24) --------------------------------------
# The paths here are the REAL package directories, because the scanner's PACKAGE_MOVES table is
# real and hard-coded - a fixture with invented package names would exercise nothing.
# bin/rename-packages.sh freezes this file for exactly that reason: rewriting io/confluent out of
# the fixture would not update this test, it would delete it, and the deletion would read as a pass.
repoD="$WORK/d"
new_repo "$repoD"
oldmain="$repoD/parallel-consumer-core/src/main/java/io/confluent/parallelconsumer"
newmain="$repoD/parallel-consumer-core/src/main/java/bz/stub/parallelconsumer"
# The NESTED rules: BOTH packages under the second upstream-owned prefix fold into the library's
# internals. There is no general rule for that prefix, so a file under it that no rule names does
# not quietly land somewhere - bin/rename-packages.sh refuses to run at all.
oldcsidutils="$repoD/parallel-consumer-core/src/main/java/io/confluent/csid/utils"
newcsidutils="$repoD/parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/utils"
oldcsidgeneral="$repoD/parallel-consumer-core/src/main/java/io/confluent/csid/testcontainers"
newcsidgeneral="$repoD/parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/testcontainers"
mkdir -p "$oldmain" "$oldcsidutils" "$oldcsidgeneral"
confluent_file "$oldmain/MovedVerbatim.java"           # 16
confluent_file "$oldmain/MovedEdited.java"             # 17
confluent_file "$oldmain/MovedEditedDual.java"         # 18
confluent_file "$oldmain/MovedLostHeader.java"         # 19
confluent_file "$oldcsidutils/MovedCsidUtils.java"     # 20: the first nested rule
confluent_file "$oldcsidgeneral/MovedCsidGeneral.java" # 21: the second nested rule
confluent_file "$oldmain/RetargetedOld.java"           # 22: renamed AND moved
confluent_file "$oldmain/StaleEntryOld.java"           # 23: renamed AND moved, entry not retargeted
git -C "$repoD" add -A && git -C "$repoD" commit -qm upstream
fork_point_d=$(git -C "$repoD" rev-parse HEAD)

# The rename: `git mv` of the package directories, then the content edits - the shape
# bin/rename-packages.sh produces.
mkdir -p "$newmain" "$newcsidutils" "$newcsidgeneral"
for n in MovedVerbatim MovedEdited MovedEditedDual MovedLostHeader; do
    git -C "$repoD" mv "parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/$n.java" \
                       "parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/$n.java"
done
git -C "$repoD" mv parallel-consumer-core/src/main/java/io/confluent/csid/utils/MovedCsidUtils.java \
                   parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/utils/MovedCsidUtils.java
git -C "$repoD" mv parallel-consumer-core/src/main/java/io/confluent/csid/testcontainers/MovedCsidGeneral.java \
                   parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/testcontainers/MovedCsidGeneral.java
git -C "$repoD" mv parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/RetargetedOld.java \
                   parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/Retargeted.java
git -C "$repoD" mv parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/StaleEntryOld.java \
                   parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/StaleEntry.java
confluent_file      "$newmain/MovedEdited.java" "int changed;"      # 17: no mods line -> violation
dual_file           "$newmain/MovedEditedDual.java" "int changed;"  # 18: conformant
headerless_file     "$newmain/MovedLostHeader.java"                 # 19: violation
dual_file           "$newcsidutils/MovedCsidUtils.java" "int changed;"     # 20: conformant, first nested rule
dual_file           "$newcsidgeneral/MovedCsidGeneral.java" "int changed;" # 21: conformant, second nested rule
confluent_file      "$newmain/Retargeted.java" "int changed;"       # 22: violation
confluent_file      "$newmain/StaleEntry.java" "int changed;"       # 23: violation
fork_file           "$newmain/ForkUnderNewPackage.java"             # conformant: fork-original, moved-package path
confluent_file      "$newmain/ForkClaimsConfluent.java"             # 24: violation
git -C "$repoD" add -A && git -C "$repoD" commit -qm rename

# One entry in the RETARGETED spelling (newpath already moved to bz/stub, as
# bin/rename-packages.sh leaves it) and one still in the OLD spelling (the half-renamed tree that
# exists between the rename's two commits). Both must resolve; the oldpath half of each names a
# path in the UPSTREAM tree and always keeps the io/confluent spelling.
renames_d="parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/Retargeted.java|parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/RetargetedOld.java
parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/StaleEntry.java|parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/StaleEntryOld.java"

out=$( (cd "$repoD" && COPYRIGHT_CHECK_FORK_POINT="$fork_point_d" \
        COPYRIGHT_CHECK_EXTRA_RENAMES="$renames_d" \
        bash "$SCANNER") 2>&1 ) && rc=0 || rc=$?
assert          "renamed-package repo with violations exits 1"  1 "$rc"
assert_contains "moved+edited upstream file is judged UPSTREAM, not fork-original" \
    "upstream-derived file modified since the fork point but missing 'Modifications Copyright ... Antony Stubbs and contributors' line): parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/MovedEdited.java" "$out"
assert_contains "moved upstream file that lost its header is still upstream-derived" \
    "upstream-derived file has no copyright header): parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/MovedLostHeader.java" "$out"
assert_contains "retargeted rename entry still resolves after the move" \
    "renamed upstream file modified since the fork point but missing 'Modifications Copyright ... Antony Stubbs and contributors' line): parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/Retargeted.java" "$out"
assert_contains "rename entry left in the OLD spelling still resolves (half-renamed tree)" \
    "renamed upstream file modified since the fork point but missing 'Modifications Copyright ... Antony Stubbs and contributors' line): parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/StaleEntry.java" "$out"
assert_contains "fork-original file under the NEW package still may not claim Confluent" \
    "fork-original file claims Confluent copyright): parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/ForkClaimsConfluent.java" "$out"
assert_contains "renamed-package repo reports exactly 5 violations" "5 violation(s)" "$out"
assert_contains "the move does not shrink the checked set" "Checked 10 java files" "$out"
# The conformant half. Under the OLD path-equality model every one of these was reported as a
# fork-original file claiming Confluent copyright, so this case is the negative control: it goes
# red against the model this replaced.
case "$out" in
    *"MovedVerbatim.java"*|*"MovedEditedDual.java"*|*"MovedCsidUtils.java"*|*"MovedCsidGeneral.java"*|*"ForkUnderNewPackage.java"*)
        echo "FAIL: moved-but-conformant files were flagged (provenance lost across the package move)"
        failures=$((failures + 1)) ;;
    *) echo "ok:   moved upstream files keep their provenance (verbatim, dual-header, both nested rules, fork-original)" ;;
esac

# --- Control arm for the ORDERING constraint (rule 26) ------------------------------
# One term changed, everything else identical: the general bz/stub/parallelconsumer rule is moved
# ABOVE the two nested ones in a copy of the scanner, and the SAME fixture is re-run.
#
# fork_point_path() returns on the FIRST prefix match, and both nested destinations sit inside the
# general prefix, so with the general rule first the two moved files resolve to fork-point paths
# under .../parallelconsumer/internal/... that hold no blob. They are then judged fork-original,
# and their retained upstream headers - which the licence requires - are reported as violations.
#
# PREDICTION, stated before the run: violations rise from 5 to 7, and the two new ones are exactly
# MovedCsidUtils.java and MovedCsidGeneral.java, reported as "fork-original file claims Confluent
# copyright". Without this arm, "the nested rules are listed first" is a comment nobody can falsify.
reordered="$WORK/scanner-reordered.sh"
# The closing quote travels with whichever rule ends up last, so strip it from every extracted rule
# and re-attach it once - otherwise moving the general rule up would move the quote with it and the
# "control arm" would be testing a syntax error.
all_rules=$(grep -E '^bz/stub/parallelconsumer' "$SCANNER" | tr -d '"')
general_rule=$(grep -E '^bz/stub/parallelconsumer\|' <<<"$all_rules")
nested_rules=$(grep -E '^bz/stub/parallelconsumer/internal/' <<<"$all_rules")
# Through a FILE rather than `awk -v`: BSD awk rejects a newline inside a -v assignment outright
# ("newline in string"), so the same command that works on a CI runner dies on a maintainer's Mac.
printf '%s\n%s"\n' "$general_rule" "$nested_rules" > "$WORK/moves-block.txt"
awk -v blockfile="$WORK/moves-block.txt" '
    /^PACKAGE_MOVES="$/ {
        print
        while ((getline line < blockfile) > 0) print line
        close(blockfile)
        next
    }
    /^bz\/stub\/parallelconsumer/  { next }
    { print }
' "$SCANNER" > "$reordered"
# A control arm that was not actually built is worse than none: it would pass by testing the
# original file. Demand that the general rule now comes FIRST among the rules in the rewritten copy.
first_rule=$(grep -E '^bz/stub/parallelconsumer' "$reordered" | sed -n '1p')
if [ "$first_rule" != "$general_rule" ] || ! bash -n "$reordered"; then
    echo "FAIL: the ordering control arm could not rebuild PACKAGE_MOVES (first rule: '$first_rule') - a control that cannot be built proves nothing"
    failures=$((failures + 1))
else
    out=$( (cd "$repoD" && COPYRIGHT_CHECK_FORK_POINT="$fork_point_d" \
            COPYRIGHT_CHECK_EXTRA_RENAMES="$renames_d" \
            bash "$reordered") 2>&1 ) && rc=0 || rc=$?
    assert          "CONTROL ARM: general-rule-first still exits 1"  1 "$rc"
    assert_contains "CONTROL ARM: general-rule-first adds exactly two violations" "7 violation(s)" "$out"
    assert_contains "CONTROL ARM: the first nested rule's file is misjudged as fork-original" \
        "fork-original file claims Confluent copyright): parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/utils/MovedCsidUtils.java" "$out"
    assert_contains "CONTROL ARM: the second nested rule's file is misjudged as fork-original" \
        "fork-original file claims Confluent copyright): parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/testcontainers/MovedCsidGeneral.java" "$out"
fi

# --- Drift guard: PACKAGE_MOVES vs bin/rename-packages.sh's PKG_MAP (rule 25) --------
# The two tables are deliberately NOT shared: the rename script is a migration tool and gets
# deleted once the rename has landed everywhere, while the scanner's table describes the fork
# point, which is permanent. This is what stops them disagreeing while both exist - if the target
# package is ever changed in one, this goes red rather than the copyright gate quietly inverting.
#
# EVERY rule, at any depth. This used to read only two-segment rules, which was fine while the
# nested ones were a lone special case carved out in prose - but once the general fallback was
# deleted and every package became an explicit rule, a depth-limited guard would have been checking
# a shrinking minority of the table while still reporting a pass. `{2,}` rather than a fixed count,
# so adding a rule at any depth is covered without touching this.
RENAME_SCRIPT="$(dirname "$SCANNER")/rename-packages.sh"
if [ ! -f "$RENAME_SCRIPT" ]; then
    echo "ok:   drift guard skipped - bin/rename-packages.sh is gone (the rename has landed)"
else
    # dot form `old|new` -> path form `new|old`, which is how the scanner writes it
    from_rename=$(grep -E '^io(\.[a-z0-9]+){2,}\|bz(\.[a-z0-9]+){2,}"?$' "$RENAME_SCRIPT" \
        | tr -d '"' | tr '.' '/' | awk -F'|' '{ print $2 "|" $1 }' | sort)
    from_scanner=$(grep -E '^bz(/[a-z0-9]+){2,}\|io(/[a-z0-9]+){2,}"?$' "$SCANNER" | tr -d '"' | sort)
    if [ -z "$from_rename" ] || [ -z "$from_scanner" ]; then
        echo "FAIL: drift guard could not read one of the tables (rename script: '$from_rename', scanner: '$from_scanner') - a guard that cannot see its subject passes for the wrong reason"
        failures=$((failures + 1))
    else
        assert "PACKAGE_MOVES matches the rename script's PKG_MAP" "$from_rename" "$from_scanner"
    fi
fi

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
