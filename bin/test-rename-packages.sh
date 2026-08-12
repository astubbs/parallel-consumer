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
#   7a. the general-utilities package folds into the internal utils package
#   7b. the test-container package folds into the internal testcontainers package - it used to ride
#                                                       the DELETED general fallback, so this is the
#                                                       case that proves its own rule fires
#   7c. NOTHING in the renamed tree carries the legacy token into the new namespace, as a path or as
#                                                       a string. The deleted fallback minted exactly
#                                                       that by design, and a completeness check on
#                                                       the OLD spelling passes right through it
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
#   25. NEGATIVE CONTROL: an unmapped package DIRECTORY makes the run refuse, before it moves
#                                                       anything, and names the package
#   26. NEGATIVE CONTROL: an unmapped package REFERENCE in configuration does the same. There is no
#                                                       fallback rule to absorb it any more, and
#                                                       that refusal is what replaced the fallback
#   27. the run prints a residue report over EVERY tracked file, with its patterns proven live
#   28. NEGATIVE CONTROL: a residue pattern that matches nothing - a PCRE-ism in a POSIX ERE, the
#                                                       exact shape that published a false clean
#                                                       result - aborts the run instead of
#                                                       reporting clean
#   29. CONTROL ARM: reversing PKG_MAP produces a byte-identical tree. The rules are disjoint
#                                                       prefixes now, so order no longer decides the
#                                                       outcome; it did while the fallback existed
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

        # The other package under that prefix. It used to ride the general fallback rule and land at
        # a top-level bz.stub.<legacy-token>.testcontainers; it now has a rule of its own, and this
        # file is what proves the rule fires rather than the fallback that no longer exists.
        mkdir -p parallel-consumer-core/src/test/java/io/confluent/csid/testcontainers
        cat > parallel-consumer-core/src/test/java/io/confluent/csid/testcontainers/FilteredLog.java <<JAVA
package io.confluent.csid.testcontainers;

/*-
 * Copyright (C) ${CONFLUENT_YEARS} Confluent, Inc.
 */

public class FilteredLog {
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

import io.confluent.csid.testcontainers.FilteredLog;

public class ForkOnly {
    private FilteredLog log;
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
    <logger name="io.confluent.csid.testcontainers" level="info"/>
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

# --- 7a: the general-utilities package folds into the internal utils package ---
utilsfile="$FIX/parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/internal/utils/Bar.java"
assert_contains "the general-utilities package moves under the directory its rule names" \
    "$utilsfile" "class Bar"
assert_contains "its package declaration is rewritten to bz.stub.parallelconsumer.internal.utils" \
    "$utilsfile" "package bz.stub.parallelconsumer.internal.utils;"
assert_contains "the import of the folded class is rewritten to its new package" \
    "$javafile" "import bz.stub.parallelconsumer.internal.utils.Bar;"
assert_absent "no old spelling of the folded utils package survives in the importing file" \
    "$javafile" "io.confluent.csid.utils"

# --- 7b: the test-container package folds into the internal testcontainers package. It used to
# take the DELETED general fallback, so this is the case that proves the new explicit rule fires ---
tcfile="$FIX/parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/internal/testcontainers/FilteredLog.java"
assert_contains "the test-container package moves under the directory its rule names" \
    "$tcfile" "class FilteredLog"
assert_contains "its package declaration is rewritten to bz.stub.parallelconsumer.internal.testcontainers" \
    "$tcfile" "package bz.stub.parallelconsumer.internal.testcontainers;"
assert_contains "a logger name for it is rewritten to the same destination" \
    "$xmlfile" 'name="bz.stub.parallelconsumer.internal.testcontainers"'
assert_contains "an import of it is rewritten too" \
    "$FIX/parallel-consumer-core/src/test/java/bz/stub/parallelconsumer/ForkOnly.java" \
    "import bz.stub.parallelconsumer.internal.testcontainers.FilteredLog;"

# --- 7c: THE POINT OF DELETING THE FALLBACK. Nothing anywhere in the renamed tree may carry the
# legacy token into the new namespace - not as a directory, not as a string. The old fallback
# produced bz.stub.<legacy-token>.* by design, so a completeness check on the OLD spelling would
# have passed while the token lived on under the new root. Assert the new spelling's absence. ---
if [ -n "$(cd "$FIX" && git ls-files | grep -E '(^|/)bz/stub/csid/' || true)" ]; then
    echo "FAIL: a bz/stub/csid/... path exists - the legacy token survived into the new namespace"
    failures=$((failures + 1))
else
    echo "ok:   no bz/stub/csid/... path exists (the legacy token did not survive the rename)"
fi
if [ -n "$(cd "$FIX" && git grep -lF 'bz.stub.csid' -- . || true)" ]; then
    echo "FAIL: the string bz.stub.csid appears in the renamed tree - the legacy token survived"
    failures=$((failures + 1))
else
    echo "ok:   the string bz.stub.csid appears nowhere (no fallback minted a legacy-token package)"
fi

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
# 25-26. NEGATIVE CONTROLS - an UNMAPPED legacy package must stop the run and be named
# --------------------------------------------------------------------------------------------------
#
# These two are the control arm for deleting the general fallback rule. With the fallback in place
# both of these cases pass SILENTLY and produce a package under the new root still carrying the
# legacy token - which is the failure the deletion exists to prevent, and it is invisible to a
# completeness check because the check looks for the OLD spelling and the fallback has already
# rewritten it.
#
# Each asserts three things, and the third is the one that makes the refusal useful rather than
# merely loud: exit 1, nothing moved, and the offending PACKAGE named in the output. A refusal that
# says only "something is wrong" leaves the operator to find it.

assert_refuses_unmapped() { # <description> <fixture-mutation as a shell snippet> <package to name>
    local desc="$1" mutate="$2" pkg="$3"
    local d="$TMP/unmapped$RANDOM$RANDOM"
    new_fixture "$d"
    ( cd "$d" && eval "$mutate" && git add -A && git commit -qm "add an unmapped legacy package" )
    local ec
    ec="$(run_script "$d" --defer-prose)"
    assert "$desc" 1 "$ec"
    assert "  ... and nothing moved before it refused" "" "$(cd "$d" && git status --porcelain)"
    if grep -q "PACKAGES WITH NO RULE" "$TMP/out.txt" && grep -qF "$pkg" "$TMP/out.txt"; then
        echo "ok:     ... and the offending package is named ($pkg)"
    else
        echo "FAIL:   ... but the output did not name the offending package ($pkg)"
        failures=$((failures + 1))
        cat "$TMP/out.txt"
    fi
}

assert_refuses_unmapped \
    "NEGATIVE CONTROL: an unmapped package DIRECTORY makes the run refuse" \
    'mkdir -p parallel-consumer-core/src/main/java/io/confluent/csid/orphan
     printf "package io.confluent.csid.orphan;\n\npublic class Orphan {\n}\n" \
        > parallel-consumer-core/src/main/java/io/confluent/csid/orphan/Orphan.java' \
    "io/confluent/csid/orphan"

assert_refuses_unmapped \
    "NEGATIVE CONTROL: an unmapped package REFERENCE in configuration makes the run refuse" \
    'printf "%s\n" "    <logger name=\"io.confluent.csid\" level=\"info\"/>" \
        >> parallel-consumer-core/src/test/resources/logback-test.xml' \
    "io.confluent.csid"

# POSITIVE CONTROL for the pair above: the unmutated fixture has every package mapped, so the same
# preflight must PASS. Without this, both assertions would hold even if the guard simply always
# refused.
d="$TMP/mapped"
new_fixture "$d"
ec="$(run_script "$d" --defer-prose)"
assert "POSITIVE CONTROL: with every package mapped the preflight passes" 0 "$ec"
if grep -q "every legacy package in this tree is named by a rule" "$TMP/out.txt"; then
    echo "ok:     ... and says so explicitly"
else
    echo "FAIL:   ... but did not report the preflight verdict"
    failures=$((failures + 1))
fi

# --------------------------------------------------------------------------------------------------
# 27-28. The residue report, and the dead-pattern control
# --------------------------------------------------------------------------------------------------
#
# The report itself is advisory - it does not gate the run - so the thing worth testing is the one
# part of it that CAN fail: the liveness proof. A pattern that matches nothing produces a report
# indistinguishable from a clean tree, which is how a sibling branch published a false clean result.

for needle in "migration residue report" "pattern liveness" "TOTAL FINDINGS" "every tracked text file"; do
    if grep -qF "$needle" "$TMP/out.txt"; then
        echo "ok:   the run prints the residue report ($needle)"
    else
        echo "FAIL: the residue report is missing '$needle'"
        failures=$((failures + 1))
    fi
done

# The control. One term changed: a PCRE-ism is spliced into one residue pattern, which git grep
# accepts as a pattern and then matches nothing with.
#
# The construct is \d, NOT the \b that caused the original false clean result, and the difference
# matters because \b's deadness is PLATFORM-DEPENDENT. GNU regex - which is what git grep -E gets
# on a glibc box, and so what it gets on the ubuntu-latest runner this job runs on - implements \b
# as a word boundary, so `git grep -E '\bcsid\b'` MATCHES here and the "dead" pattern is alive.
# The control built on it therefore passed on the author's BSD machine and failed on CI. \d is in
# no POSIX class and is not a GNU regex extension either, so it degrades to a literal 'd' on every
# engine in play: `\dcsid` is "dcsid", which matches nothing, everywhere.
#
# What the control proves is unchanged - a pattern matching nothing must abort the run rather than
# read as a clean tree - and so is the reason the script proves patterns with git grep rather than
# grep: prove with the engine you sweep with, or you certify a pattern the sweep cannot run.
d="$TMP/deadpattern"
new_fixture "$d"
perl -i -pe "s/general-utils-token\\|csid\\|/general-utils-token|\\\\dcsid|/" "$d/bin/rename-packages.sh"
# The guard tests THE PATTERN LINE, not the file. Testing the file passes on a prose mention of the
# same construct in a comment - which is exactly how this control first reported itself as built
# while the splice had silently matched nothing.
if grep -qE "^RESIDUE_PATTERNS='general-utils-token\|\\\\dcsid\|" "$d/bin/rename-packages.sh"; then
    # Committed, or the script refuses to start on a dirty tree and the run exits 2 for a reason
    # that has nothing to do with the pattern under test.
    (cd "$d" && git add -A && git commit -qm "splice a dead residue pattern")
    ec="$(run_script "$d" --defer-prose)"
    assert "NEGATIVE CONTROL: a residue pattern that matches nothing aborts the run" 1 "$ec"
    if grep -q "DEAD - it does not even match its own sample" "$TMP/out.txt"; then
        echo "ok:     ... and names the dead pattern rather than reporting a clean sweep"
    else
        echo "FAIL:   ... but did not name the dead pattern"
        failures=$((failures + 1))
        cat "$TMP/out.txt"
    fi
else
    echo "FAIL: could not splice a dead pattern into the fixture's copy of the script - the control was never built"
    failures=$((failures + 1))
fi

# --------------------------------------------------------------------------------------------------
# 29. CONTROL ARM: PKG_MAP is order-independent now, and was not before
# --------------------------------------------------------------------------------------------------
#
# The rules are mutually disjoint prefixes once the general fallback is gone, so the table should
# produce the same tree whichever order it is applied in. Stated as a prediction before running:
# reversing PKG_MAP changes nothing, and `git diff` between the two renamed trees is empty.
#
# This is NOT a licence to stop writing specific rules first. It is a measurement of today's table,
# kept runnable so that adding a rule which DOES overlap another shows up here as a difference
# instead of as a package quietly landing in the wrong place.

reverse_pkg_map() { # <dir> - rewrite the fixture's copy of the script with PKG_MAP reversed
    # Two statements, NOT `local d="$1" s="$d/bin/..."`. Bash expands every argument to `local`
    # before the builtin runs, so the second `$d` there is the GLOBAL d - which this file happens to
    # have set from an earlier case, so the mis-scoped version silently rewrites a different
    # fixture's script and the control arm quietly stops being a control.
    local d="$1"
    local s="$d/bin/rename-packages.sh"
    awk '
        /^PKG_MAP="/ { print; inmap = 1; next }
        inmap {
            line = $0
            last = (line ~ /"$/)
            sub(/"$/, "", line)
            rules[++n] = line
            if (last) {
                for (i = n; i >= 1; i--) print rules[i] (i == 1 ? "\"" : "")
                inmap = 0
            }
            next
        }
        { print }
    ' "$s" > "$s.new" && mv "$s.new" "$s"
}

d_fwd="$TMP/order-forward"
d_rev="$TMP/order-reversed"
new_fixture "$d_fwd"
new_fixture "$d_rev"
reverse_pkg_map "$d_rev"
# Committed, or the script refuses to start on a dirty tree - which would look like the reversed
# order failing when in fact it was never run.
(cd "$d_rev" && git add -A && git commit -qm "reverse PKG_MAP")
if bash -n "$d_rev/bin/rename-packages.sh" 2>/dev/null &&
   [ "$(grep -c '^io\.confluent' "$d_rev/bin/rename-packages.sh")" = "$(grep -c '^io\.confluent' "$d_fwd/bin/rename-packages.sh")" ] &&
   [ "$(grep -m1 '^io\.confluent' "$d_rev/bin/rename-packages.sh")" != "$(grep -m1 '^io\.confluent' "$d_fwd/bin/rename-packages.sh")" ]; then
    ec_fwd="$(run_script "$d_fwd" --defer-prose)"
    ec_rev="$(run_script "$d_rev" --defer-prose)"
    assert "CONTROL ARM: the forward order applies cleanly"  0 "$ec_fwd"
    assert "CONTROL ARM: the reversed order applies cleanly" 0 "$ec_rev"
    # Compare the TREES, not the logs: the two runs differ only in the order of the rules, and the
    # question is whether any file landed anywhere different.
    fwd_tree="$(cd "$d_fwd" && git ls-files | sort)"
    rev_tree="$(cd "$d_rev" && git ls-files | sort)"
    assert "CONTROL ARM: reversing PKG_MAP moves every file to the same place" "$fwd_tree" "$rev_tree"
    fwd_hash="$(cd "$d_fwd" && git ls-files -s -- . | grep -v 'bin/rename-packages.sh' | sort)"
    rev_hash="$(cd "$d_rev" && git ls-files -s -- . | grep -v 'bin/rename-packages.sh' | sort)"
    assert "CONTROL ARM: and every file's CONTENT is byte-identical too" "$fwd_hash" "$rev_hash"
else
    echo "FAIL: could not reverse PKG_MAP in the fixture's copy - the control arm was never built"
    failures=$((failures + 1))
fi

# --------------------------------------------------------------------------------------------------
# 20-21. Prose guards
# --------------------------------------------------------------------------------------------------

# These test the MECHANISM, so they plant their own guard into the fixture's copy of the script rather
# than borrowing whatever the production PROSE_GUARDS happens to hold. The production list is empty
# once every guarded claim has been corrected, and coupling the mechanism's coverage to it meant that
# retiring the last guard silently deleted the only proof the machinery still worked. Same technique
# as the PKG_MAP control arm above: edit the fixture's script, commit, and verify the edit landed.
plant_prose_guard() { # <dir> - aim one guard at the claim new_fixture already writes into the fixture
    local s="$1/bin/rename-packages.sh"
    # No-op when the production list already carries a guard for that claim. This file has to work on
    # both bases: the tooling branch, where the three original guards are still live because the prose
    # they name has not been corrected yet, and the rename change-set, where they are retired and
    # PROSE_GUARDS is deliberately empty. Injecting over a populated list would be the drift.
    grep -q '^src/docs/README_TEMPLATE.adoc|drop-in replacement' "$s" && return 0
    awk '
        /^PROSE_GUARDS=""$/ {
            print "PROSE_GUARDS=\"\\"
            print "src/docs/README_TEMPLATE.adoc|drop-in replacement.*package.*are unchanged|The drop-in claim stops being TRUE and must not merely be qualified. Say the packages MOVE.\""
            next
        }
        { print }
    ' "$s" > "$s.new" && mv "$s.new" "$s"
    chmod +x "$s"
}

d="$TMP/prose"
new_fixture "$d"
plant_prose_guard "$d"
(cd "$d" && git add -A && git commit -qm "aim a prose guard at this fixture's own claim")

# Verify the injection, or an awk pattern that silently stopped matching would present as "the guard
# did not fire" - which is the same output as the guard being broken, and the opposite diagnosis.
if bash -n "$d/bin/rename-packages.sh" 2>/dev/null &&
   grep -q '^src/docs/README_TEMPLATE.adoc|drop-in replacement' "$d/bin/rename-packages.sh"; then
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
else
    echo "FAIL: could not plant a prose guard in the fixture's copy - the mechanism was never tested"
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
#
# PROSE_GUARDS IS ALLOWED TO BE EMPTY, once every guarded claim has been corrected - which is what the
# rename's own change-set does. But "empty" and "the parser drifted" must not look alike, and an empty
# list must not make this whole section pass by iterating zero times: that is the vacuous pass this
# suite exists to refuse. So the empty state is asserted explicitly, and a retired guard is replaced by

repo_root="$(cd "$(dirname "$SCRIPT")/.." && pwd)"

parse_pipe_list() { # <var-name> -> the heredoc lines of a `NAME="\` ... `"` block in the script
    awk -v want="^$1=\"" '
        $0 ~ want { f = 1; if ($0 ~ /^[A-Z_]+=""$/) exit; next }
        f { line = $0; sub(/"$/, "", line); print line; if ($0 ~ /"$/) exit }
    ' "$SCRIPT"
}

guards="$(parse_pipe_list PROSE_GUARDS)"

if [ -z "$guards" ]; then
    echo "FAIL: could not read PROSE_GUARDS out of $SCRIPT - the parser above has drifted from it"
    failures=$((failures + 1))
fi

# Three states per outlier, and the same assertion is correct on any branch: the claim is still live,
# or it has been corrected and its corrected form is present. Neither means the sentence was reworded
# into something nobody declared - the state that used to report "none found" and read clean.
while IFS='|' read -r gpath gere gfixed _; do
    [ -n "$gpath" ] || continue
    if [ ! -f "$repo_root/$gpath" ]; then
        echo "FAIL: prose guard names $gpath, which does not exist"
        failures=$((failures + 1))
    elif grep -qE "$gere" "$repo_root/$gpath"; then
        echo "ok:   guarded claim is still live in $gpath, so the guard will fire"
    elif grep -qE "$gfixed" "$repo_root/$gpath"; then
        echo "ok:   guarded claim is corrected in $gpath, and its corrected form is present"
    else
        echo "FAIL: $gpath has NEITHER the claim ($gere) nor its corrected form ($gfixed) - the"
        echo "      sentence was reworded rather than corrected, and the guard now matches nothing"
        failures=$((failures + 1))
    fi
done <<EOF
$guards
EOF

# --------------------------------------------------------------------------------------------------

# --------------------------------------------------------------------------------------------------
# The BRINGING AN OPEN BRANCH ACROSS procedure block
# --------------------------------------------------------------------------------------------------
#
# The block IS the delivery mechanism for the fan-out: a stale branch reads it out of origin/master
# with `git show`, because on that branch the file does not exist yet and `cat` cannot help. Three
# things are therefore tested rather than assumed - that it reaches --help, that the script's own
# rewrite cannot eat it, and that the staleness command in step 1 actually discriminates.

helptext="$TMP/helptext.txt"
bash "$SCRIPT" --help > "$helptext" 2>&1 || true

assert_contains "--help carries the condensed procedure" "$helptext" \
    "BRINGING AN OPEN PR BRANCH ACROSS"
assert_contains "--help gives step 1 as a COMMAND, not a description of a state" "$helptext" \
    'git diff --quiet origin/master -- bin/rename-packages.sh bin/check-copyright-headers.sh'
assert_contains "--help names BOTH tooling files in the checkout - one file is the 197-violation trap" \
    "$helptext" 'git checkout origin/master -- bin/rename-packages.sh bin/check-copyright-headers.sh'
assert_contains "--help states the scope boundary as a PROHIBITION, not an omission" "$helptext" \
    "Do NOT merge the PR"
assert_contains "--help says STOP and report rather than inviting judgement" "$helptext" \
    "STOP and report"

# The header is the long form. An agent pasting `git show origin/master:bin/rename-packages.sh` reads
# THIS, so the reasons that stop each step being "improved" have to be in it.
assert_contains "the header carries the full procedure section" "$SCRIPT" \
    "# BRINGING AN OPEN BRANCH ACROSS"
assert_contains "the header forecloses the merge variant" "$SCRIPT" "NOT a merge."
assert_contains "the header forecloses the cherry-pick variant" "$SCRIPT" "NOT a cherry-pick."
assert_contains "the header forecloses pinning a sha" "$SCRIPT" "A REF, NOT A SHA."
assert_contains "the header names the zero-conflict outcome as corruption, not luck" "$SCRIPT" \
    "That is not good luck."

# If a run rewrote the script, the procedure would decay silently across the 40 branches that take it
# from master - each one carrying instructions one generation staler. SELF_BASENAMES is what prevents
# it, matched on basename; this asserts the whole file is untouched, block included.
FIX="$TMP/selfblock"
new_fixture "$FIX"
ec="$(run_script "$FIX" --defer-prose)"

# The exit code is asserted FIRST and separately, and that is the whole point. `cmp` alone passes
# whenever the script was not rewritten - INCLUDING when the run refused before reaching the rewrite,
# which is exactly what happens if this script stops being excluded from its own processing: the
# preflight then reads the PKG_MAP and RESIDUE_PATTERNS below as unmapped packages and dies. A guard
# that cannot tell "excluded" from "aborted early" is decoration; observed, and this is the fix.
assert "a run with the procedure block present still applies cleanly" 0 "$ec"

if cmp -s "$SCRIPT" "$FIX/bin/rename-packages.sh"; then
    echo "ok:   the script is byte-identical after a run, so the procedure block survives it"
else
    echo "FAIL: the run REWROTE the script itself - the procedure block decays across branches"
    failures=$((failures + 1))
fi

# And prove the exclusion by its effect rather than by the file being equal: the old spelling has to
# still be sitting in the script as DATA after a successful run.
assert_contains "the script keeps the old spelling as data after a run" \
    "$FIX/bin/rename-packages.sh" "io.confluent.parallelconsumer"

# Step 1 has to answer "am I stale?" for a branch that has never seen the tooling AND for one holding
# an older copy. The second is the case `test -f` gets wrong, which is why step 1 diffs content.
staleness_ec() { # <dir> <ref> -> exit code of step 1's command
    local ec=0
    (cd "$1" && git diff --quiet "$2" -- bin/rename-packages.sh bin/check-copyright-headers.sh) || ec=$?
    echo "$ec"
}

SFIX="$TMP/staleness"
new_fixture "$SFIX"
(cd "$SFIX" && git branch -q pretend-master)
assert "step 1 reports CURRENT when the branch's tooling matches the ref" 0 \
    "$(staleness_ec "$SFIX" pretend-master)"

(cd "$SFIX" && git rm -q bin/rename-packages.sh bin/check-copyright-headers.sh \
    && git commit -qm "a branch cut before the tooling existed")
assert "step 1 reports STALE when the tooling is ABSENT" 1 \
    "$(staleness_ec "$SFIX" pretend-master)"

DFIX="$TMP/drifted"
new_fixture "$DFIX"
(cd "$DFIX" && git branch -q pretend-master \
    && printf '# a later change to the tooling\n' >> bin/rename-packages.sh \
    && git add -A && git commit -qm "the two copies have diverged")
assert "step 1 reports STALE when the tooling EXISTS but differs - the case 'test -f' misses" 1 \
    "$(staleness_ec "$DFIX" pretend-master)"

# --------------------------------------------------------------------------------------------------
# Freeze regions - the line-level exemption for text that must keep the OLD spelling
# --------------------------------------------------------------------------------------------------
#
# The case that forces this: upgrade instructions. "the packages move FROM io.confluent.parallelconsumer
# TO bz.stub.parallelconsumer" and the sed a reader runs are only useful while they still say the old
# name, and the bulk rewrite turns them into a package moving to itself and a sed that does nothing.
# MEASURED, and it passed every gate this script has - the sweep hunts old spellings that SHOULD have
# been rewritten, so one that must NOT be is invisible to it.
#
# Two directions matter equally. The region must hold, AND it must not leak: an exemption that swallowed
# the rest of the file would present as a clean sweep over text nobody checked.

FZ="$TMP/freeze"
new_fixture "$FZ"
mkdir -p "$FZ/docs"
cat > "$FZ/docs/upgrade-notes.adoc" <<'ADOC'
// rename-packages: freeze-begin(migration) - migration instructions must name the old package
Move from io.confluent.parallelconsumer to the new package.
Run: sed -i 's/io\.confluent\.parallelconsumer/NEW/g'
// rename-packages: freeze-end(migration)
See io.confluent.parallelconsumer.ParallelConsumer, an ordinary reference that MUST be rewritten.
ADOC
(cd "$FZ" && git add -A && git commit -qm "a doc with a freeze region")

ec="$(run_script "$FZ" --defer-prose)"
assert "a tree containing a freeze region applies cleanly" 0 "$ec"
assert_contains "the frozen dotted form keeps the OLD spelling" "$FZ/docs/upgrade-notes.adoc" \
    "from io.confluent.parallelconsumer to the new package"
assert_contains "the frozen ESCAPED-REGEX form is untouched too" "$FZ/docs/upgrade-notes.adoc" \
    's/io\.confluent\.parallelconsumer/NEW/g'
assert_contains "NEGATIVE CONTROL: a reference OUTSIDE the region is still rewritten" \
    "$FZ/docs/upgrade-notes.adoc" "bz.stub.parallelconsumer.ParallelConsumer"
assert_absent "so the exemption does not leak past freeze-end" \
    "$FZ/docs/upgrade-notes.adoc" "io.confluent.parallelconsumer.ParallelConsumer"

ec="$(run_script "$FZ" --verify-only)"
assert "the completeness check tolerates a frozen region" 0 "$ec"
if grep -q "FROZEN REGIONS" "$TMP/out.txt" && grep -q "docs/upgrade-notes.adoc" "$TMP/out.txt"; then
    echo "ok:   and PRINTS what the region held, so the exemption is auditable"
else
    echo "FAIL: the frozen region was tolerated SILENTLY - an exemption nobody can audit"
    failures=$((failures + 1))
fi

# An unbalanced marker is the quiet-hole case: everything after an unclosed freeze-begin stops being
# rewritten and stops being checked, and the check still says clean.
UB="$TMP/freeze-unclosed"
new_fixture "$UB"
mkdir -p "$UB/docs"
printf '%s\n%s\n' '// rename-packages: freeze-begin(unclosed) - deliberately never closed' \
    'io.confluent.parallelconsumer would be silently exempt from here down' > "$UB/docs/bad.adoc"
(cd "$UB" && git add -A && git commit -qm "unclosed freeze region")
ec="$(run_script "$UB" --defer-prose)"
assert "NEGATIVE CONTROL: an UNCLOSED freeze region makes the run REFUSE" 1 "$ec"
assert "  ... and nothing moved before it refused" 0 \
    "$(cd "$UB" && git status --porcelain | wc -l | tr -d ' ')"

# A SETTLED tree must re-run as a successful no-op. This fixture reproduces what the real tree has and
# the original fixture did not: a file that DESCRIBES the rename, so it matches the sweep pattern
# permanently while matching no rewrite rule. n_rewrites therefore never reaches zero, the
# "already applied, nothing to do" exit never fires however finished the rename is, and the run used to
# die at `git commit` with nothing to commit - exit 1 on a branch that was already correct, which the
# per-branch procedure turns into "any refusal, STOP and report".
SETTLED="$TMP/settled"
new_fixture "$SETTLED"
printf '%s\n' 'The fork is moving io.confluent.* to bz.stub.*, and quotes grep -rn "io\.confluent" as a trap.' \
    > "$SETTLED/AGENTS.md"
(cd "$SETTLED" && git add -A && git commit -qm "AGENTS.md describes the rename, as the real one does")
ec="$(run_script "$SETTLED" --defer-prose)"
assert "a tree whose AGENTS.md describes the rename applies cleanly" 0 "$ec"
ec="$(run_script "$SETTLED" --defer-prose)"
assert "and a SETTLED tree re-runs as a successful NO-OP, not a failed empty commit" 0 "$ec"
if grep -q "nothing to commit" "$TMP/out.txt"; then
    echo "ok:   and says so, rather than reporting a commit it did not make"
else
    echo "FAIL: the no-op run did not report that there was nothing to commit"
    failures=$((failures + 1))
fi

UB2="$TMP/freeze-stray-end"
new_fixture "$UB2"
mkdir -p "$UB2/docs"
printf '%s\n%s\n' 'io.confluent.parallelconsumer sits above a marker that closes nothing' \
    '// rename-packages: freeze-end(orphan)' > "$UB2/docs/bad2.adoc"
(cd "$UB2" && git add -A && git commit -qm "stray freeze-end")
ec="$(run_script "$UB2" --defer-prose)"
assert "NEGATIVE CONTROL: a STRAY freeze-end makes the run REFUSE" 1 "$ec"

# --------------------------------------------------------------------------------------------------
# Freeze regions, part 2: the guards a code review found missing
# --------------------------------------------------------------------------------------------------
#
# Every control below was observed FAILING with its guard removed. Three of them exist because the
# first version of this mechanism shipped without them and a review reproduced the consequences.

# has_rewritable_match's own effect. Re-running a settled tree whose ONLY remaining matches are frozen
# must reach the "already applied, nothing to do" exit. Without the guard the frozen-only file stays in
# the rewrite set, so the run proceeds and lands on the empty-commit no-op path instead - a different
# message, and the reason removing the guard used to leave this suite entirely green.
ec="$(run_script "$FZ" --defer-prose)"
assert "a settled tree whose only matches are FROZEN re-runs cleanly" 0 "$ec"
if grep -q "already applied, nothing to do" "$TMP/out.txt"; then
    echo "ok:   and a frozen-only file is not counted as rewritable work"
else
    echo "FAIL: a frozen-only file still counted as rewritable - has_rewritable_match did not filter it"
    failures=$((failures + 1))
fi

# The P0: two independent authoring mistakes in ONE file that cancel out under marker counting. A
# forgotten freeze-end plus an unrelated stray freeze-end read as begin/end/begin/end - balanced - and
# everything between the orphan and the stray joins the frozen set, so a live reference is neither
# rewritten nor reported while both VERDICTs print clean. Ids are what make this refusable.
MIS="$TMP/freeze-mispaired"
new_fixture "$MIS"
mkdir -p "$MIS/docs"
printf '%s\n' '// rename-packages: freeze-begin(real) - a legitimate, correctly closed region' \
    'Move from io.confluent.parallelconsumer to the new package.' \
    '// rename-packages: freeze-end(real)' \
    '' \
    '// rename-packages: freeze-begin(oops) - MISTAKE: this one is never closed' \
    'body text' \
    'An ORDINARY io.confluent.parallelconsumer.ParallelConsumer reference that MUST be rewritten.' \
    '' \
    '// rename-packages: freeze-end(real)' > "$MIS/docs/mispaired.adoc"
(cd "$MIS" && git add -A && git commit -qm "two independent marker mistakes in one file")
ec="$(run_script "$MIS" --defer-prose)"
assert "NEGATIVE CONTROL: a freeze-end that closes the WRONG region refuses" 1 "$ec"
assert_contains "  ... and names both ids rather than just saying unbalanced" "$TMP/out.txt" \
    "does not close freeze-begin(oops)"
assert_absent "  ... and the live reference was NOT silently frozen and left behind" \
    "$TMP/out.txt" "no stale references outside the excluded set"

# An unnamed region cannot be paired by identity at all, so it is refused rather than accepted.
NOID="$TMP/freeze-noid"
new_fixture "$NOID"
mkdir -p "$NOID/docs"
printf '%s\n' '// rename-packages: freeze-begin - no id, so nothing can close it by name' \
    'io.confluent.parallelconsumer stays here' \
    '// rename-packages: freeze-end' > "$NOID/docs/noid.adoc"
(cd "$NOID" && git add -A && git commit -qm "markers without ids")
ec="$(run_script "$NOID" --defer-prose)"
assert "NEGATIVE CONTROL: a freeze-begin with no id refuses" 1 "$ec"

# A reference riding on the marker line itself: the whole marker line is frozen by both the line-set
# builder and the rewrite, so this would be exempt with no region to audit it against.
SAME="$TMP/freeze-sameline"
new_fixture "$SAME"
mkdir -p "$SAME/docs"
printf '%s\n' '// rename-packages: freeze-begin(x) - fine' \
    'frozen body' \
    '// rename-packages: freeze-end(x) trailing io.confluent.parallelconsumer.Sneaky' > "$SAME/docs/sameline.adoc"
(cd "$SAME" && git add -A && git commit -qm "package reference on the marker line")
ec="$(run_script "$SAME" --defer-prose)"
assert "NEGATIVE CONTROL: a package reference ON the marker line refuses" 1 "$ec"

# The other P0: --verify-only HONOURS freeze regions, so it must also validate them. Its only call site
# used to sit after this entry point's early exit, so a malformed region reported a clean sweep.
ec="$(run_script "$MIS" --verify-only)"
assert "NEGATIVE CONTROL: --verify-only refuses a malformed region instead of reporting clean" 1 "$ec"
assert_absent "  ... and does not print a clean completeness verdict" "$TMP/out.txt" \
    "no stale references outside the excluded set"

# --------------------------------------------------------------------------------------------------

echo
if [ "$failures" -eq 0 ]; then
    echo "All bin/rename-packages.sh self-tests passed"
    exit 0
fi
echo "$failures self-test(s) FAILED"
exit 1
