#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for bin/rename-packages.sh.
#
# Builds a throwaway git repository in a temp dir, shaped like this one in the ways that matter -
# package directories under io/confluent, an escaped-regex reference in a shell script, a misspelt
# reference in an IDE run configuration, Confluent copyright headers in four different comment
# syntaxes, a historical plan document, and the copyright provenance manifest with its
# `newpath|oldpath` pairs - then runs the real script against it and asserts the outcome.
#
# WHY A FIXTURE REPO AND NOT THE REAL TREE. The script moves 233 files and makes commits. Pointing it
# at the working checkout to see whether it works would BE the rename, which is not a thing to do by
# accident, and the negative controls below deliberately plant broken references.
#
# THE CASES, and what each one is actually protecting:
#
#    1. dry run changes nothing                       - the reporting path must not mutate
#    2. the two-commit default produces two commits   - the measured shape (see the script's header)
#    3. every move is recorded as a RENAME at R100    - the whole point; git DETECTS renames, so
#                                                       "we used git mv" proves nothing on its own
#    4. no delete/create pairs in the move commit     - a delete+create is a conflict at merge time
#    5. the content commit contains no moves          - otherwise it dilutes its parent's detection
#    6. the ESCAPED-REGEX form is rewritten           - io\.confluent\.parallelconsumer, which a
#                                                       find-and-replace on the dotted form misses
#                                                       and the habitual grep cannot even see
#    7. a MISSPELT reference is normalised            - parallalconsumer, from All_examples.xml
#   7a. io.confluent.csid.utils folds into the internal utils package, NOT the general bz.stub.csid
#                                                       a plain prefix rule would give it - and the
#                                                       general csid rule stays shadow-free elsewhere
#    8. java gets ` * Modifications Copyright ...`    - block-comment continuation
#    9. XML gets it INSIDE the <!-- --> block         - and NEVER a `//`
#   10. YAML gets a `#` prefix                        - and NEVER a `//`
#   11. .properties gets a `#` prefix                 - and NEVER a `//`
#   12. an UNKNOWN extension makes it REFUSE          - guessing a comment marker corrupts the file
#   13. historical docs/plans/ are left alone         - they said io.confluent because that was true
#   14. the copyright manifest's OLDPATH half survives- it names a path in the UPSTREAM tree, and
#                                                       rewriting it silently inverts the checker
#   15. a second run is a no-op                       - re-runnability on 29 branches depends on it
#   16. a file added under the OLD path afterwards is picked up - proves the script reads the TREE
#                                                       and not a manifest written today
#   17. NEGATIVE CONTROL: a planted stale reference makes the completeness check FAIL
#   18. NEGATIVE CONTROL: a planted MISSPELT stale reference fails it too - the permissive pattern
#                                                       stops at `conflu` precisely for this
#   19. NEGATIVE CONTROL: a planted ESCAPED-REGEX reference fails it - this is the one the habitual
#                                                       sweep `grep -rn "io\.confluent"` cannot see,
#                                                       so it is the case the check exists for
#   20. the prose guard BLOCKS by default             - a claim that becomes false must not be
#                                                       rewritten mechanically into a confident lie
#   21. --defer-prose proceeds and records it   - what you want on a PR branch
#   22. CONTROL ARM: two commits keep every pairing exact on near-identical sibling files
#   23. EXPERIMENT ARM: --single-commit mis-pairs them, and the verification CATCHES it - the
#                                                       measurement behind the default, kept runnable
#   24. every prose guard still MATCHES its sentence in the real tree - a guard whose prose was
#                                                       reworded or corrected matches nothing and
#                                                       reports "none found", which reads as a pass
#
# Cases 17-19 are the ones worth keeping honest about. A checker nobody has seen fail is decoration,
# and these three are the shapes that fail GREEN in production: the mutation lane exits 0 when its
# stale regex matches nothing, so nothing downstream would ever tell you.
#
# Run: bin/test-rename-packages.sh   (CI should run it before the script it protects)

set -uo pipefail

SCRIPT="$(cd "$(dirname "$0")" && pwd)/rename-packages.sh"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

failures=0

assert() { # <description> <expected> <actual>
    if [ "$2" = "$3" ]; then
        echo "ok:   $1"
    else
        echo "FAIL: $1 (expected '$2', got '$3')"
        failures=$((failures + 1))
    fi
}

assert_contains() { # <description> <file> <fixed-string>
    if [ -f "$2" ] && grep -qF -- "$3" "$2"; then
        echo "ok:   $1"
    else
        echo "FAIL: $1 (did not find '$3' in $2)"
        failures=$((failures + 1))
    fi
}

assert_absent() { # <description> <file> <fixed-string>
    if [ ! -f "$2" ]; then
        echo "FAIL: $1 (file $2 does not exist)"
        failures=$((failures + 1))
    elif grep -qF -- "$3" "$2"; then
        echo "FAIL: $1 (found '$3' in $2, and it should not be there)"
        failures=$((failures + 1))
    else
        echo "ok:   $1"
    fi
}

# --------------------------------------------------------------------------------------------------
# Fixture
# --------------------------------------------------------------------------------------------------

CONFLUENT_YEARS="2020-2022"

new_fixture() { # <dir>
    local d="$1"
    mkdir -p "$d"
    (
        cd "$d" || exit 1
        git init -q .
        git config user.name "self test"
        git config user.email "selftest@example.invalid"
        git config commit.gpgsign false

        mkdir -p bin
        cp "$SCRIPT" bin/

        # A shell script carrying the ESCAPED-REGEX form. This is the reference that is invisible
        # both to a find-and-replace on the dotted spelling and to `grep -rn "io\.confluent"`.
        cat > bin/ci-mutation-test.sh <<'SH'
#!/usr/bin/env bash
#
# Copyright (C) 2020-2022 Confluent, Inc.
#
set -euo pipefail
DECIDABLE="${PIT_DECIDABLE_PACKAGES:-^io\.confluent\.parallelconsumer\.offsets\.}"
TARGET_TESTS="${PIT_TARGET_TESTS:-io.confluent.parallelconsumer.*}"
SH

        # The provenance manifest. The `newpath|oldpath` shape is the trap: the oldpath names a path
        # in the UPSTREAM tree at the fork point and MUST keep saying io/confluent.
        cat > bin/check-copyright-headers.sh <<'SH'
#!/usr/bin/env bash
RENAMED_FROM_UPSTREAM="
parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/NewName.java|parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/OldName.java
"
EXTRACTED_FROM_UPSTREAM="
parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/Extracted.java
"
SH

        # Java, upstream-derived. Note the header sits BELOW the package line, as in the real tree.
        mkdir -p parallel-consumer-core/src/main/java/io/confluent/parallelconsumer
        cat > parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/Foo.java <<JAVA
package io.confluent.parallelconsumer;

/*-
 * Copyright (C) ${CONFLUENT_YEARS} Confluent, Inc.
 */

import io.confluent.csid.utils.Bar;

/** See {@link io.confluent.parallelconsumer.Foo} and io/confluent/parallelconsumer/Foo.java. */
public class Foo {
    static final String ARCH = "io.confluent.parallelconsumer";
    private Bar bar;
}
JAVA

        # The SECOND Confluent-owned prefix. Every automation scoped to io.confluent.parallelconsumer
        # misses io.confluent.csid, which is how the META-INF/services listener gets left behind.
        mkdir -p parallel-consumer-core/src/main/java/io/confluent/csid/utils
        cat > parallel-consumer-core/src/main/java/io/confluent/csid/utils/Bar.java <<JAVA
package io.confluent.csid.utils;

/*-
 * Copyright (C) ${CONFLUENT_YEARS} Confluent, Inc.
 */

public class Bar {
}
JAVA

        # A fork-original java file: no Confluent notice, so it must NOT be given a modifications
        # line - that would claim a derivation that does not exist.
        mkdir -p parallel-consumer-core/src/test/java/io/confluent/parallelconsumer
        cat > parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/ForkOnly.java <<JAVA
package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

public class ForkOnly {
}
JAVA

        # XML with the header inside an <!-- --> block: continuation lines carry no marker at all.
        mkdir -p parallel-consumer-core/src/test/resources
        cat > parallel-consumer-core/src/test/resources/logback-test.xml <<XML
<!--

    Copyright (C) ${CONFLUENT_YEARS} Confluent, Inc.

-->
<configuration>
    <logger name="io.confluent.parallelconsumer" level="info"/>
    <logger name="io.confluent.csid" level="info"/>
</configuration>
XML

        # .properties: '#' comments.
        cat > parallel-consumer-core/src/test/resources/junit-platform.properties <<PROPS
#
# Copyright (C) ${CONFLUENT_YEARS} Confluent, Inc.
#

#junit.jupiter.displayname.generator.default = io.confluent.csid.utils.ReplaceCamelCase
PROPS

        # YAML: '#' comments.
        mkdir -p .github/workflows
        cat > .github/workflows/mutation-full-sweep.yml <<YML
#
# Copyright (C) ${CONFLUENT_YEARS} Confluent, Inc.
#
on: workflow_dispatch
env:
  TARGET: 'io.confluent.parallelconsumer.offsets.*'
YML

        # The MISSPELLING, exactly as All_examples.xml carried it.
        mkdir -p .idea/runConfigurations
        cat > .idea/runConfigurations/All_examples.xml <<'XML'
<component>
    <option name="PATTERN" value="io.confluent.parallalconsumer.examples.core.*" />
    <option name="PACKAGE_NAME" value="io.confluent.parallelconsumer.examples" />
</component>
XML

        # A historical plan document: it must be left exactly as written.
        mkdir -p docs/plans
        cat > docs/plans/2026-01-01-001-history.md <<'MD'
# A dated record

At the time of writing the fix site was
`parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/Foo.java`.
MD

        # A file the prose guard is watching.
        mkdir -p src/docs
        cat > src/docs/README_TEMPLATE.adoc <<'ADOC'
= Readme
It is a *drop-in replacement*: the Java API and package (`io.confluent.parallelconsumer`) are unchanged from upstream 0.5.x.
See link:./parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/Foo.java[Foo].
ADOC

        git add -A
        git commit -qm "fixture"
    )
}

run_script() { # <dir> <args...> -> prints the exit code, output goes to $TMP/out.txt
    local d="$1"
    shift
    local ec=0
    (cd "$d" && bash bin/rename-packages.sh "$@") > "$TMP/out.txt" 2>&1 || ec=$?
    echo "$ec"
}

# --------------------------------------------------------------------------------------------------
# 1. Dry run changes nothing
# --------------------------------------------------------------------------------------------------

FIX="$TMP/dry"
new_fixture "$FIX"
before="$(cd "$FIX" && git rev-parse HEAD)"
ec="$(run_script "$FIX" --dry-run --defer-prose)"
after="$(cd "$FIX" && git rev-parse HEAD)"
assert "dry run exits 0" 0 "$ec"
assert "dry run makes no commit" "$before" "$after"
assert "dry run leaves the tree clean" "" "$(cd "$FIX" && git status --porcelain)"

# --------------------------------------------------------------------------------------------------
# 2-16. The real run
# --------------------------------------------------------------------------------------------------

FIX="$TMP/apply"
new_fixture "$FIX"
n_expected_moves="$(cd "$FIX" && git ls-files | grep -cE '(^|/)io/confluent/(parallelconsumer|csid)/')"
ec="$(run_script "$FIX" --defer-prose)"
assert "the default run exits 0" 0 "$ec"
[ "$ec" = 0 ] || { echo "--- script output ---"; cat "$TMP/out.txt"; }

assert "the default makes TWO commits (move, then content)" 2 \
    "$(cd "$FIX" && git rev-list --count HEAD ^HEAD~2 2>/dev/null || echo err)"

# --- 3, 4: rename tracking on the move commit ---
move_rev="$(cd "$FIX" && git rev-parse HEAD~1)"
raw="$(cd "$FIX" && git -c diff.renameLimit=65535 show --raw -M --no-color --format='' "$move_rev")"
n_r100="$(awk '/^:/ && $5 == "R100" { c++ } END { print c + 0 }' <<<"$raw")"
n_ad="$(awk '/^:/ && ($5 == "A" || $5 == "D") { c++ } END { print c + 0 }' <<<"$raw")"
assert "every moved file is recorded as an EXACT rename (R100)" "$n_expected_moves" "$n_r100"
assert "the move commit has no delete/create pairs" 0 "$n_ad"

# --- 5: the content commit carries no moves ---
raw2="$(cd "$FIX" && git -c diff.renameLimit=65535 show --raw -M --no-color --format='' HEAD)"
n_moves2="$(awk '/^:/ && $5 !~ /^M/ { c++ } END { print c + 0 }' <<<"$raw2")"
assert "the content commit contains no renames, adds or deletes" 0 "$n_moves2"

# --- 6: the escaped-regex form ---
assert_contains "escaped-regex form rewritten" \
    "$FIX/bin/ci-mutation-test.sh" 'bz\.stub\.parallelconsumer\.offsets\.'
assert_absent "no escaped-regex reference to the old package survives" \
    "$FIX/bin/ci-mutation-test.sh" 'io\.confluent'

# --- 7: the misspelling ---
assert_contains "misspelt parallalconsumer normalised AND renamed" \
    "$FIX/.idea/runConfigurations/All_examples.xml" 'bz.stub.parallelconsumer.examples.core.*'

# --- 8-11: the modifications line, in the right comment syntax for each file type ---
MODS="Modifications Copyright (C)"
javafile="$FIX/parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/Foo.java"
xmlfile="$FIX/parallel-consumer-core/src/test/resources/logback-test.xml"
ymlfile="$FIX/.github/workflows/mutation-full-sweep.yml"
propfile="$FIX/parallel-consumer-core/src/test/resources/junit-platform.properties"

assert_contains "java gets the modifications line as a block-comment continuation" \
    "$javafile" " * ${MODS}"
assert_contains "XML gets it inside the <!-- --> block, unmarked like its neighbours" \
    "$xmlfile" "    ${MODS}"
assert_absent "XML never gets a // comment" "$xmlfile" "//${MODS}"
assert_contains "YAML gets a # prefix" "$ymlfile" "# ${MODS}"
assert_absent "YAML never gets a // comment" "$ymlfile" "// ${MODS}"
assert_contains ".properties gets a # prefix" "$propfile" "# ${MODS}"
assert_absent ".properties never gets a // comment" "$propfile" "// ${MODS}"
assert_absent "a fork-original file gets NO modifications line" \
    "$FIX/parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/ForkOnly.java" "$MODS"

# --- 7a: the io.confluent.csid.utils special case: it folds into the internal utils package, NOT
# the general bz.stub.csid path a plain prefix rule would give it ---
utilsfile="$FIX/parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/utils/Bar.java"
assert_contains "io.confluent.csid.utils moves under the directory the special case names" \
    "$utilsfile" "class Bar"
assert_contains "its package declaration is rewritten to bz.stub.parallelconsumer.internal.utils" \
    "$utilsfile" "package bz.stub.parallelconsumer.internal.utils;"
if [ -f "$FIX/parallel-consumer-core/src/main/java/bz/stub/csid/utils/Bar.java" ]; then
    echo "FAIL: the general bz.stub.csid.utils path was used - the special case was shadowed"
    failures=$((failures + 1))
else
    echo "ok:   the general bz.stub.csid.utils path is NOT used (the special case is not shadowed)"
fi
assert_contains "the import of the folded class is rewritten to its new package" \
    "$javafile" "import bz.stub.parallelconsumer.internal.utils.Bar;"
assert_absent "no io.confluent.csid.utils spelling survives in the importing file" \
    "$javafile" "io.confluent.csid.utils"

# --- the general io.confluent.csid rule still applies OUTSIDE .utils, unaffected by the special
# case above (this is the ordering check: a bug that let the specific rule swallow the general
# prefix, or vice versa, would show up here) ---
assert_contains "io.confluent.csid (non-utils) still takes the general csid rule" \
    "$xmlfile" 'name="bz.stub.csid"'
assert_absent "the old io.confluent.csid spelling does not survive" "$xmlfile" "io.confluent.csid"

# --- 13: historical documents ---
assert_contains "historical docs/plans/ prose is left exactly as written" \
    "$FIX/docs/plans/2026-01-01-001-history.md" "io/confluent/parallelconsumer/Foo.java"

# --- 14: the provenance manifest's two halves ---
manifest="$FIX/bin/check-copyright-headers.sh"
assert_contains "the manifest's NEWPATH half moves to bz/stub" \
    "$manifest" "java/bz/stub/parallelconsumer/NewName.java|"
assert_contains "the manifest's OLDPATH half still names the upstream path" \
    "$manifest" "|parallel-consumer-core/src/test/java/io/confluent/parallelconsumer/OldName.java"
assert_contains "an EXTRACTED entry (no | separator) moves whole" \
    "$manifest" "main/java/bz/stub/parallelconsumer/Extracted.java"

# --- 15: idempotency ---
head_before="$(cd "$FIX" && git rev-parse HEAD)"
ec="$(run_script "$FIX" --defer-prose)"
head_after="$(cd "$FIX" && git rev-parse HEAD)"
assert "a second run exits 0" 0 "$ec"
assert "a second run makes no commit" "$head_before" "$head_after"
assert "a second run leaves the tree clean" "" "$(cd "$FIX" && git status --porcelain)"
if grep -q "already applied, nothing to do" "$TMP/out.txt"; then
    echo "ok:   a second run says 'already applied, nothing to do'"
else
    echo "FAIL: a second run did not report 'already applied, nothing to do'"
    failures=$((failures + 1))
fi

# --- 16: a file added under the OLD path afterwards, which is what a PR branch does ---
mkdir -p "$FIX/parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/latecomer"
cat > "$FIX/parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/latecomer/Late.java" <<'JAVA'
package io.confluent.parallelconsumer.latecomer;

public class Late {
}
JAVA
(cd "$FIX" && git add -A && git commit -qm "a PR adds a file under the old path")
ec="$(run_script "$FIX" --defer-prose)"
assert "a file added under the old path afterwards is picked up (tree, not manifest)" 0 "$ec"
if [ -f "$FIX/parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/latecomer/Late.java" ]; then
    echo "ok:   the late-arriving file was moved to the new path"
else
    echo "FAIL: the late-arriving file was not moved"
    failures=$((failures + 1))
fi

# --------------------------------------------------------------------------------------------------
# 17-19. NEGATIVE CONTROLS - the completeness check must be seen to FAIL
# --------------------------------------------------------------------------------------------------
#
# Each plants ONE stale reference into an already-renamed tree and asserts --verify-only goes red.
# Without these the check is decoration: all three of these shapes fail GREEN in production, so a
# check that silently stopped working would look exactly like a clean tree.

plant_and_verify() { # <description> <relative-file> <line to append>
    local desc="$1" rel="$2" line="$3"
    local d="$TMP/neg$RANDOM$RANDOM"
    new_fixture "$d"
    run_script "$d" --defer-prose > /dev/null
    printf '%s\n' "$line" >> "$d/$rel"
    (cd "$d" && git add -A && git commit -qm "plant a stale reference")
    local ec
    ec="$(run_script "$d" --verify-only)"
    assert "$desc" 1 "$ec"
}

plant_and_verify "NEGATIVE CONTROL: a plain stale reference fails the completeness check" \
    ".github/workflows/mutation-full-sweep.yml" "  STALE: 'io.confluent.parallelconsumer.state.*'"

plant_and_verify "NEGATIVE CONTROL: a MISSPELT stale reference fails it too" \
    ".github/workflows/mutation-full-sweep.yml" "  STALE: 'io.confluant.parallelconsumer.state.*'"

plant_and_verify "NEGATIVE CONTROL: an ESCAPED-REGEX stale reference fails it - the case the habitual sweep cannot see" \
    "bin/ci-mutation-test.sh" 'STALE="^io\.confluent\.parallelconsumer\.state\."'

# A positive control for the same three: with nothing planted, the check must PASS. Otherwise the
# assertions above would hold even if the checker simply always failed.
d="$TMP/positive"
new_fixture "$d"
run_script "$d" --defer-prose > /dev/null
assert "POSITIVE CONTROL: with nothing planted the completeness check passes" 0 \
    "$(run_script "$d" --verify-only)"

# --------------------------------------------------------------------------------------------------
# 20-21. Prose guards
# --------------------------------------------------------------------------------------------------

d="$TMP/prose"
new_fixture "$d"
ec="$(run_script "$d")"
assert "a claim that the packages are unchanged BLOCKS the run" 1 "$ec"
assert "and nothing was moved before it stopped" 0 \
    "$(cd "$d" && git status --porcelain | wc -l | tr -d ' ')"
if grep -q "drop-in claim stops being TRUE" "$TMP/out.txt"; then
    echo "ok:   the block names the replacement wording rather than just refusing"
else
    echo "FAIL: the prose guard did not print what to write instead"
    failures=$((failures + 1))
fi

ec="$(run_script "$d" --defer-prose)"
assert "--defer-prose proceeds" 0 "$ec"
if grep -q "MANUAL FOLLOW-UPS" "$TMP/out.txt" && grep -q "prose in src/docs/README_TEMPLATE.adoc" "$TMP/out.txt"; then
    echo "ok:   and carries the claim as a named manual follow-up"
else
    echo "FAIL: --defer-prose did not record the claim as a manual follow-up"
    failures=$((failures + 1))
fi

# --------------------------------------------------------------------------------------------------
# 22-23. The commit-shape measurement, encoded as a regression test
# --------------------------------------------------------------------------------------------------
#
# The same tree, one term changed - whether the content edits are folded into the move. This is the
# experiment from the script's header, kept runnable so the default can be re-justified rather than
# taken on trust.
#
# Five near-identical sibling files, one per module, whose whole body is a package name and an
# annotation. Fold the content edits into the move and each one resembles its four siblings about as
# much as it resembles its own former self: git pairs them into a cycle across modules and drops one
# out as an add/delete. Keep the move pure and every pairing is exact.
#
# This is not a synthetic worry - `TestConventionsArchTest.java` really does exist once per module in
# this repo, and the real tree reproduces it: 232 renames out of 233, four of them cross-module.

archunit_fixture() { # <dir>
    local d="$1" m
    mkdir -p "$d"
    (
        cd "$d" || exit 1
        git init -q .
        git config user.name "self test"
        git config user.email "selftest@example.invalid"
        git config commit.gpgsign false
        mkdir -p bin && cp "$SCRIPT" bin/
        for m in core vertx mutiny reactor streams; do
            mkdir -p "mod-$m/src/test/java/io/confluent/parallelconsumer/$m"
            cat > "mod-$m/src/test/java/io/confluent/parallelconsumer/$m/TestConventionsArchTest.java" <<JAVA
package io.confluent.parallelconsumer.$m;

import com.tngtech.archunit.junit.AnalyzeClasses;

@AnalyzeClasses(packages = "io.confluent.parallelconsumer.$m")
public class TestConventionsArchTest extends TestConventionRules {
}
JAVA
        done
        git add -A
        git commit -qm "fixture"
    )
}

d="$TMP/shape-two"
archunit_fixture "$d"
assert "CONTROL ARM: the two-commit default keeps every pairing exact on near-identical siblings" 0 \
    "$(run_script "$d")"

d="$TMP/shape-one"
archunit_fixture "$d"
ec="$(run_script "$d" --single-commit)"
assert "EXPERIMENT ARM: --single-commit is CAUGHT mis-pairing them, and refuses" 1 "$ec"
if grep -q "MIS-PAIRED RENAMES" "$TMP/out.txt"; then
    echo "ok:   and it names each file, its expected destination, and what git paired it with"
else
    echo "FAIL: --single-commit degraded without the mis-pairing report firing"
    failures=$((failures + 1))
    cat "$TMP/out.txt"
fi

# --------------------------------------------------------------------------------------------------
# 12. An unknown extension must make it REFUSE, not guess
# --------------------------------------------------------------------------------------------------
#
# A file carrying a Confluent notice and a reference to the old package, with an extension the script
# has never seen. Guessing `//` for a Lisp file (or `#` for one that wants `;;`) corrupts it silently,
# so the only safe answer is to stop and say so.

d="$TMP/unknown-ext"
new_fixture "$d"
cat > "$d/weird.zzz" <<'ZZZ'
;;
;; Copyright (C) 2020-2022 Confluent, Inc.
;;
(require "io.confluent.parallelconsumer")
ZZZ
(cd "$d" && git add -A && git commit -qm "add a file with an extension nobody has taught it")
ec="$(run_script "$d" --defer-prose)"
assert "an unrecognised extension makes it refuse rather than guess a comment marker" 1 "$ec"
if grep -q "refusing to guess a comment syntax" "$TMP/out.txt"; then
    echo "ok:   and it says which file and why"
else
    echo "FAIL: the refusal did not explain itself"
    failures=$((failures + 1))
fi

# --------------------------------------------------------------------------------------------------
# 24. Every prose guard still matches its sentence IN THE REAL TREE
# --------------------------------------------------------------------------------------------------
#
# The only case here that reads the working checkout, and the only one that can: a guard's whole job
# is to recognise one sentence in one real file, so a fixture cannot tell you whether it still does.
# Nothing else notices when it stops - `check_prose_guards` prints "none found" and exits 0, which is
# indistinguishable from a tree with no false claims left in it.
#
# Not hypothetical. astubbs/parallel-consumer#280 merged master's aa61238a, which rewrote the
# changelog entry the second guard was aimed at and deleted its wording; the guard survived the merge
# without a conflict, matching nothing.
#
# When this fails, the guard is spent one way or the other: the sentence was REWORDED (re-point the
# pattern) or it was CORRECTED (retire the guard). Both are edits to PROSE_GUARDS - neither is a
# reason to relax this test.

repo_root="$(cd "$(dirname "$SCRIPT")/.." && pwd)"
guards="$(awk '
    /^PROSE_GUARDS="/ { f = 1; next }
    f { line = $0; sub(/"$/, "", line); print line; if ($0 ~ /"$/) exit }
' "$SCRIPT")"

if [ -z "$guards" ]; then
    echo "FAIL: could not read PROSE_GUARDS out of $SCRIPT - the parser above has drifted from it"
    failures=$((failures + 1))
fi

while IFS='|' read -r gpath gere _; do
    [ -n "$gpath" ] || continue
    if [ ! -f "$repo_root/$gpath" ]; then
        echo "FAIL: prose guard names $gpath, which does not exist"
        failures=$((failures + 1))
    elif grep -qE "$gere" "$repo_root/$gpath"; then
        echo "ok:   prose guard still matches its sentence in $gpath"
    else
        echo "FAIL: prose guard matches NOTHING in $gpath (pattern: $gere) - the sentence was either"
        echo "      reworded (re-point the pattern) or corrected (retire the guard)"
        failures=$((failures + 1))
    fi
done <<EOF
$guards
EOF

# --------------------------------------------------------------------------------------------------

echo
if [ "$failures" -eq 0 ]; then
    echo "All bin/rename-packages.sh self-tests passed"
    exit 0
fi
echo "$failures self-test(s) FAILED"
exit 1
