#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Performs the fork's package rename - io.confluent.* -> bz.stub.* - on whatever branch it is run in.
#
# WHY THIS IS A SCRIPT AND NOT A ONE-OFF `sed`
#
# The rename has to land on master AND on every open PR branch. Done by hand, once, on master, the
# other branches each turn into a conflict storm at merge: their files still live under the old path
# and their diffs still say `io.confluent`. Done by a re-runnable script, each branch gets the SAME
# transformation, so both sides of every merge agree on where the files live and what they are
# called, and the merge is trivial. That is the whole design constraint - re-runnability on an
# arbitrary branch - and it is why nothing here is a hardcoded manifest: a PR that adds a new file
# under the old package path must be picked up by globbing the tree, not by a list written today.
#
# See docs/plans/2026-08-11-001-refactor-package-rename-plan.md for the full task inventory and the
# Apache 2.0 analysis. Two findings from it govern the code below.
#
#   1. THREE OF THE REFERENCES ARE INVISIBLE TO THE OBVIOUS SWEEP. `bin/ci-mutation-test.sh` and
#      `bin/lib/quarantine-common.sh` encode the package as an ESCAPED REGEX - the literal characters
#      io\.confluent\.parallelconsumer\. - which a find-and-replace on the dotted form does not
#      match, and which the habitual verification sweep `grep -rn "io\.confluent"` also cannot see:
#      it reports success while `bin/lib/quarantine-common.sh` is not even in its output. Stale, the
#      mutation lane matches nothing, prints "no core main-source classes changed - nothing to
#      mutate, skipping" and EXITS 0 - green forever while scoring zero mutants, indistinguishable in
#      the job summary from a real pass. So the rewrite has an explicit rule for the escaped shape,
#      and the completeness check at the end uses a PERMISSIVE pattern that tolerates it.
#
#   2. THE PACKAGE IS SPELT WRONG IN AT LEAST ONE PLACE. `.idea/runConfigurations/` has carried
#      io.confluent.parallalconsumer - an `a` where the second `e` belongs. Treat misspellings as a
#      category rather than a one-off: the rewrite normalises the known variant before renaming (so
#      the typo is fixed, not carried forward), and the completeness check's pattern stops at
#      `conflu` so that a variant nobody has seen still cannot hide from it.
#
# WHAT IT DOES, IN ORDER
#
#   preflight  - refuse to run on a dirty tree (the commit would sweep in unrelated work)
#   moves      - `git mv` every file under */io/confluent/{parallelconsumer,csid}/ to */bz/stub/...
#   rewrite    - every textual form, in every tracked text file, minus an explicit frozen list
#   headers    - Apache 2.0 s4(c): add the Modifications Copyright line to modified upstream files
#   regenerate - docs/TODO_INDEX.md and README.adoc, both of which must never be hand-edited
#   commit     - see COMMIT SHAPE below
#   verify     - assert git recorded the moves as RENAMES, then run the completeness check
#
# COMMIT SHAPE, AND WHY THE DEFAULT IS TWO COMMITS
#
# Measured, not assumed - both shapes have a plausible story and the cost of guessing wrong is a
# conflict storm across every open PR. The experiment, with a control arm: perform the rename as two
# commits (pure `git mv` first, content edits second) and verify rename detection; then squash the
# two and re-run the IDENTICAL verification on the result. Exactly one term changes - whether the
# content edits are folded into the move. Run against master at bd021cb2 on 2026-08-11, git 2.51.2,
# with an explicit diff.renameLimit of 65535 so no result is a limit artefact.
#
#   two commits  move commit     233 moved, 233 renames, ALL R100 (exact), 0 A, 0 D
#                content commit  0 renames, 0 A, 0 D, 253 modifications
#   squashed     one commit      233 moved, 232 renames, 1 A + 1 D, lowest similarity R061
#
# THE SQUASH LOST A RENAME, and the prediction about which file held. `TestConventionsArchTest.java`
# exists once per module - five near-identical files whose whole body is an `@AnalyzeClasses`
# annotation and an empty class - so once the package string is rewritten, each one resembles its
# four siblings about as much as it resembles its own former self. Git paired them into a cycle:
#
#   streams/TestConventionsArchTest.java  ->  metrics/...   R071
#   vertx/...                             ->  mutiny/...    R073
#   mutiny/...                            ->  reactor/...   R073
#   reactor/...                           ->  vertx/...     R073
#   metrics/...                           ->  DELETED, and streams/... reported as a new file
#
# Four renames recorded as CROSS-MODULE moves that never happened, and one file dropped out of the
# permutation as an add/delete pair. A bare count of R entries would have caught only the last of
# those, which is why the verification below also asserts that every rename's old path maps to its
# new path under the SAME transformation the script applied. A rename that is detected but wrong is
# worse than one that is missed.
#
# WHAT THE SPLIT DOES NOT BUY, ALSO MEASURED. `git merge` does rename detection over the merge base
# to tip TREE DELTA, and that delta is identical however many commits it is spread across. Merging
# each shape into the same PR branch produced byte-identical outcomes in every scenario tried. So the
# split does not rescue a merge; what it buys is an accurate HISTORY - `git log --follow`,
# `git blame -C`, and anything else reading the log get 233 true renames instead of four false ones
# and a fabricated deletion. That is worth the cost of a first commit that does not compile.
#
# DEFAULT: two commits. `--single-commit` is available for a branch where one atomic commit matters
# more than history fidelity - but the numbers above are the reason it is not the default, and a
# single-commit run is verified by the same assertions, so it will TELL you what it cost.
#
# RUNNING THIS ON EVERY OPEN PR BRANCH IS MANDATORY, NOT A CONVENIENCE. Merging a renamed master into
# a PR branch that has NOT been renamed reported ZERO conflicts and silently applied the PR's edit to
# the streams module's ArchUnit test INTO THE MUTINY MODULE'S FILE - the mis-pairing above, playing
# out as data loss with no warning. When both sides have run the script, the same case surfaces as an
# ordinary rename/rename conflict on the right file, with the PR's edit intact, for a human to
# resolve. Loud and correct beats silent and wrong: rename the branch, then merge.
#
# WHY THE VERIFICATION IS NOT OPTIONAL. Git does not RECORD renames, it DETECTS them at read time.
# "We used git mv" is therefore not evidence of anything; the question is whether `git show -M` finds
# them, and that depends on similarity scores and on `diff.renameLimit`, which git exceeds SILENTLY -
# reporting add+delete instead. So the script asserts the rename count equals the moved-file count,
# asserts every pairing is the one the script actually performed, asserts there are no stray
# add/delete pairs, prints the lowest similarity it saw, and prints the rename limits a future MERGE
# will use. And because a check nobody has watched fail is decoration, bin/test-rename-packages.sh
# includes a negative control where a planted stale reference is expected to make the completeness
# check go red.
#
# WHAT IT DELIBERATELY WILL NOT DO
#
# Two sentences in the tree are CLAIMS that become false the moment the rename lands - the README's
# "the Java API and package are unchanged from upstream", and the changelog's "Java package names are
# unchanged, so imports are unaffected". Rewriting them mechanically turns each into a confident
# false statement in a published artifact, which is worse than leaving it stale. So they are PROSE
# GUARDS: the script stops before touching anything and prints what to write instead.
#
# `--defer-prose` proceeds, and it is worth being exact about what that means rather than soothing:
# the bulk rewrite then TOUCHES those sentences, so the README's drop-in claim comes out saying
# "the Java API and package (`bz.stub.parallelconsumer`) are unchanged from upstream 0.5.x" - the
# same falsehood, in the new spelling. It is not left alone; it is deferred, and the script lists it
# by file and line under MANUAL FOLLOW-UPS. That is the right trade only where the prose will be
# corrected elsewhere: fix it once on master, and pass the flag on the PR branches, where the
# corrected sentence arrives from master at merge. (The changelog is frozen for the rewrite, so its
# claim is merely left stale.)
#
# Usage:
#   bin/rename-packages.sh                  # apply and commit (move commit + content commit)
#   bin/rename-packages.sh --dry-run        # report the work set, touch nothing
#   bin/rename-packages.sh --no-commit      # apply to the working tree, do not commit
#   bin/rename-packages.sh --single-commit  # one atomic commit instead of two (see above)
#   bin/rename-packages.sh --verify-only    # completeness check only, change nothing
#   bin/rename-packages.sh --defer-prose --skip-readme-regen
#
# Exit codes: 0 = applied (or already applied), 1 = a check failed, 2 = refused to start.
#
# Self-test: bin/test-rename-packages.sh

set -euo pipefail

# --------------------------------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------------------------------

# The rename, in DOT form. Everything else - the path form, the escaped-regex form, the directory
# moves, the scan patterns - is DERIVED from this table, so what is on disk and what is written in
# the files cannot drift apart.
#
# `io.confluent.csid` is in the table on purpose. It is a DIFFERENT Confluent-owned prefix from
# `io.confluent.parallelconsumer`, and every automation scoped to the latter misses it - which is how
# `META-INF/services/org.junit.platform.launcher.TestExecutionListener` gets left behind naming
# `io.confluent.csid.utils.MyRunListener`, a resource no compiler and no IDE refactor will ever
# touch. Leaving `csid` also defeats the reason for the rename, since it is still Confluent's mark in
# our namespace. So it moves, and the plan records that as the decision.
PKG_MAP="\
io.confluent.parallelconsumer|bz.stub.parallelconsumer
io.confluent.csid|bz.stub.csid"

MODS_HOLDER="Antony Stubbs and contributors"
MODS_YEAR="${RENAME_MODS_YEAR:-$(date +%Y)}"

# The permissive completeness pattern. It matches io.conflu, io/conflu, io\.conflu and ioconflu, and
# it stops at `conflu` so that a misspelling further along cannot hide from it. The habitual
# `io\.confluent` cannot see the backslash form at all, which is the trap this exists to avoid.
SWEEP_ERE='io[\\./]*conflu'

# TWO LISTS, AND THEY ARE NOT THE SAME LIST. Conflating them is how a generated file gets quietly
# excused from the check that would have caught it being stale.
#
# FROZEN_PREFIXES - never touched by the bulk rewrite. Some of these are still IN SCOPE for the
# completeness check, because something else is responsible for making them correct:
#
#   README.adoc         GENERATED from src/docs/README_TEMPLATE.adoc at process-sources, so it must
#                       never be hand-edited - but it must still come out CLEAN, which is what proves
#                       the regeneration actually ran. Checked.
#   docs/TODO_INDEX.md  GENERATED by bin/todo-index.sh, gated by `--check` in CI. Same: not rewritten,
#                       but regenerated and then checked.
#   NOTICE, LICENSE     Apache 2.0 s4(a) and s4(d). Untouched, always. They contain no `io.confluent`
#                       today, and if they ever do a human should decide, so they stay checked.
#   .semaphore/         legacy Confluent internal CI, retained but inactive on the fork. Also checked,
#                       for the same reason: it holds no reference today.
FROZEN_PREFIXES="\
CHANGELOG.adoc
README.adoc
NOTICE
LICENSE
docs/plans/
docs/solutions/
docs/inflight/
docs/TODO_INDEX.md
.semaphore/
bin/check-copyright-headers.sh
bin/test-check-copyright-headers.sh"

# SWEEP_EXCLUDE - the narrow set the completeness check is allowed to ignore, because `io.confluent`
# survives there LEGITIMATELY. The check prints every match it skipped in each of these, so the
# exclusion is auditable rather than a silent hole. Justify any addition, in writing, here:
#
#   CHANGELOG.adoc      release notes. The `=== Breaking` entry NAMES the old Maven coordinate as
#                       history and must keep saying so. AGENTS.md separately forbids a PR editing
#                       this file at all, bar correcting a claim that has become false - which is a
#                       PROSE_GUARD below, for a human, not something to sweep.
#   docs/plans/         dated plan, investigation and solution records, and the in-flight notes. They
#   docs/solutions/     said `io.confluent` because that is what was true when they were written, and
#   docs/inflight/      rewriting them makes them say something they did not say. The plan is
#                       explicit that the answer for everything under docs/plans/ is "leave it".
#   check-copyright-    its RENAMED_FROM_UPSTREAM entries are `newpath|oldpath-at-fork-point` pairs,
#   headers.sh          and the oldpath side names a path in the UPSTREAM tree. Rewrite it and
#                       `git cat-file blob $FORK_POINT:$oldpath` stops resolving - which fails by
#                       deciding the file is fork-original, turning a REQUIRED Confluent header into
#                       a violation. A targeted edit moves only the newpath half, so the oldpath
#                       halves legitimately still read io/confluent, and the check prints them all.
#                       Its PACKAGE_MOVES table names BOTH spellings for the same reason: it is the
#                       rule that maps a moved file back to its fork-point path.
#   test-check-         its fixtures move a file from io/confluent to bz/stub and assert the scanner
#   copyright-          still resolves provenance across the move. That fixture IS the negative
#   headers.sh          control for the rule above, so rewriting the old spelling out of it does not
#                       update a test, it deletes one - and the deletion reads as a pass.
#   AGENTS.md           both DESCRIBE the rename rather than reference the package: "the
#   repo-hygiene.yml    io.confluent.* -> bz.stub.* package-rename tool", and the sweep pattern
#                       `grep -rn "io\.confluent"` quoted as the trap it is. Neither matches any
#                       rewrite rule (they are not whole package names), and rewriting them would
#                       turn each sentence into nonsense - "the bz.stub.* -> bz.stub.* tool". They
#                       are NOT frozen, so a real package reference added to either is still
#                       rewritten; this only silences the residue, and the check prints it.
SWEEP_EXCLUDE="\
CHANGELOG.adoc
docs/plans/
docs/solutions/
docs/inflight/
bin/check-copyright-headers.sh
bin/test-check-copyright-headers.sh
AGENTS.md
.github/workflows/repo-hygiene.yml"

# Excluded from both the rewrite and the completeness check, because they must carry the old spelling
# as DATA. Matched on BASENAME, not on a hardcoded path, so moving or renaming this script cannot
# silently switch the exclusion off - the lesson bin/check-shell-sigpipe.sh already learned.
SELF_BASENAMES="\
rename-packages.sh
test-rename-packages.sh"

# Claims a mechanical rewrite would turn into confident falsehoods: `path|ERE|what to write instead`.
PROSE_GUARDS="\
src/docs/README_TEMPLATE.adoc|drop-in replacement.*package.*are unchanged|The drop-in claim stops being TRUE and must not merely be qualified. Plan s8 drafts the replacement: say the packages MOVE from io.confluent.parallelconsumer to bz.stub.parallelconsumer, that the API itself is unchanged, and give the one-line sed under == Upgrading.
CHANGELOG.adoc|Java package names.*are unchanged|This becomes a factual error the moment the rename lands, and AGENTS.md allows exactly one changelog edit in a PR: correcting an existing claim that is now false. Rewrite that bullet. Do NOT add a new entry - the 0.6.0.0 section is generated at release time from the commit log."

# Rename detection is a similarity matrix over unmatched paths, and git gives up above
# diff.renameLimit with a warning that is easy to miss, then reports add+delete. 264 files move
# today, so the default of 1000 is not truncating - but the assertion must not DEPEND on ambient
# config, so verification pins its own generous limit and separately reports the repo's.
VERIFY_RENAME_LIMIT=65535

HEADER_WINDOW=8 # lines from the top of a file searched for copyright notices, as in the checker

# --------------------------------------------------------------------------------------------------
# Options
# --------------------------------------------------------------------------------------------------

APPLY=true
DO_COMMIT=true
SPLIT_COMMITS=true # measured default - see COMMIT SHAPE in the header
DEFER_PROSE=false
REGEN_README=true
VERIFY_ONLY=false
SWEEP_EXCLUDE_EXTRA=""

usage() {
    cat <<'USAGE'
bin/rename-packages.sh - apply the io.confluent.* -> bz.stub.* package rename to this branch.

By default it makes TWO commits: a pure `git mv` move, then the content edits. Measured - squashing
them makes git mis-pair the five near-identical TestConventionsArchTest.java files across modules and
lose one rename outright.

  --dry-run             report the work set and exit; change nothing
  --no-commit           apply to the working tree, leave it uncommitted
  --single-commit       one atomic commit instead of the two-commit default
  --defer-prose         proceed past the prose guards. The guarded sentences are then REWRITTEN
                        mechanically (same false claim, new spelling) and listed as follow-ups
  --skip-readme-regen   do not run ./mvnw -N process-sources for README.adoc
  --verify-only         run the completeness check only
  -h, --help            this text

Read the header of this file for the measurement behind the default, for why running this on every
open PR branch is mandatory rather than convenient, and for the two references a naive
find-and-replace cannot see. Self-test: bin/test-rename-packages.sh
USAGE
}

while [ $# -gt 0 ]; do
    case "$1" in
        --dry-run)           APPLY=false ;;
        --no-commit)         DO_COMMIT=false ;;
        --single-commit)     SPLIT_COMMITS=false ;;
        --defer-prose) DEFER_PROSE=true ;;
        --skip-readme-regen) REGEN_README=false ;;
        --verify-only)       VERIFY_ONLY=true ;;
        -h|--help)           usage; exit 0 ;;
        *) echo "unknown option: $1 (try --help)" >&2; exit 2 ;;
    esac
    shift
done

cd "$(git rev-parse --show-toplevel)"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

MOVES="$TMP/moves.tsv"
REWRITES="$TMP/rewrites.txt"
MANUAL_FOLLOWUPS="$TMP/manual.txt"
: > "$MANUAL_FOLLOWUPS"

note_manual() { printf '  - %s\n' "$1" >> "$MANUAL_FOLLOWUPS"; }
section()     { echo; echo "== $*"; }
die()         { echo; echo "FAIL: $*" >&2; exit 1; }

count_lines() { # <file>
    local n
    n=$(wc -l < "$1")
    printf '%s' "${n//[[:space:]]/}"
}

# --------------------------------------------------------------------------------------------------
# The substitution program - built once from PKG_MAP, used for BOTH file paths and file contents
# --------------------------------------------------------------------------------------------------
#
# One program, two consumers, so a path and the string that names it can never be rewritten
# differently. Perl rather than sed because sed's -i differs between GNU and BSD and this has to run
# unchanged on a maintainer's Mac and on a CI runner.
build_perl_program() {
    local prog='' old new esc_re esc_rep dot_re path_re path_rep i n
    local ifs_save

    # Normalise the known misspelling FIRST, so the rules below stay clean and so the typo is fixed
    # rather than carried across the rename. `.idea/runConfigurations/All_examples.xml` shipped
    # `io.confluent.parallalconsumer.examples.core.*` and had been broken long before this rename.
    prog+='s{(confluent[\\]?[./])parallalconsumer}{$1parallelconsumer}g;'

    while IFS='|' read -r old new; do
        [ -n "$old" ] || continue

        ifs_save="$IFS"
        IFS='.'
        # shellcheck disable=SC2206 # deliberate word splitting on IFS='.'
        local oldseg=($old)
        # shellcheck disable=SC2206
        local newseg=($new)
        IFS="$ifs_save"
        n=${#oldseg[@]}

        # (1) The ESCAPED-REGEX form: the literal characters io\.confluent\.parallelconsumer, as they
        #     appear inside a grep/ERE pattern in a shell script. Invisible to a plain replace, and
        #     invisible to the sweep people habitually verify with. The backslash run is CAPTURED and
        #     replayed, so both `io\.x` and `io\\.x` survive with their own escaping intact; a
        #     mismatched pair falls through to the completeness check rather than being mangled.
        esc_re="${oldseg[0]}"
        esc_rep="${newseg[0]}"
        for ((i = 1; i < n; i++)); do
            if [ "$i" -eq 1 ]; then esc_re+='(\\+)\.'; else esc_re+='\1\.'; fi
            esc_re+="${oldseg[$i]}"
            esc_rep+='$1.'"${newseg[$i]}"
        done
        prog+="s{\\b${esc_re}}{${esc_rep}}g;"

        # (2) The plain dotted form: package declarations, imports, ArchUnit @AnalyzeClasses strings,
        #     the beAssignableTo FQCN, logback logger names, the truth-generator <class> list,
        #     entryPointClassPackage, PIT target packages, the META-INF/services listener.
        dot_re=${old//./\\.}
        prog+="s{\\b${dot_re}}{${new}}g;"

        # (3) The path form: javadoc links, README includes, workflow comments, the TODO index.
        path_re=${old//./\/}
        path_rep=${new//./\/}
        prog+="s{\\b${path_re}}{${path_rep}}g;"
    done <<EOF
$PKG_MAP
EOF

    printf '%s' "$prog"
}

PERL_PROG="$(build_perl_program)"

# Which tracked paths sit under an old package directory - derived from the same table.
build_path_scan_ere() {
    local ere='' old dir
    while IFS='|' read -r old _; do
        [ -n "$old" ] || continue
        # Not `${old//./\/}` here: that expansion sits inside double quotes below, where bash keeps
        # the backslash and emits `io\/confluent`, which is undefined in POSIX ERE even though GNU
        # and BSD grep happen to accept it.
        dir=$(tr '.' '/' <<<"$old")
        [ -n "$ere" ] && ere+='|'
        ere+="(^|/)${dir}/"
    done <<EOF
$PKG_MAP
EOF
    printf '%s' "$ere"
}

PATH_SCAN_ERE="$(build_path_scan_ere)"

is_self() { # <path> - matched on BASENAME so a move cannot disable the exclusion
    local b base
    b="$(basename "$1")"
    while IFS= read -r base; do
        [ -n "$base" ] || continue
        [ "$b" = "$base" ] && return 0
    done <<EOF
$SELF_BASENAMES
EOF
    return 1
}

has_prefix_in() { # <path> <newline-separated prefix list>
    local p="$1" prefix
    while IFS= read -r prefix; do
        [ -n "$prefix" ] || continue
        case "$p" in "$prefix"*) return 0 ;; esac
    done <<EOF
$2
EOF
    return 1
}

is_frozen() { # <path> - excluded from the bulk rewrite
    has_prefix_in "$1" "$FROZEN_PREFIXES" && return 0
    is_self "$1"
}

is_sweep_excluded() { # <path> - excluded from the COMPLETENESS CHECK. Deliberately a shorter list.
    has_prefix_in "$1" "$SWEEP_EXCLUDE" && return 0
    # Set at runtime by --skip-readme-regen: a README nobody regenerated is stale by construction, so
    # excusing it is a decision the operator made explicitly, and it is recorded as a follow-up.
    [ -n "${SWEEP_EXCLUDE_EXTRA:-}" ] && has_prefix_in "$1" "$SWEEP_EXCLUDE_EXTRA" && return 0
    is_self "$1"
}

# --------------------------------------------------------------------------------------------------
# Discovery - always from the TREE, never from a manifest, so a PR's new files are picked up too
# --------------------------------------------------------------------------------------------------

discover_moves() {
    : > "$MOVES"
    git ls-files | { grep -E "$PATH_SCAN_ERE" || true; } > "$TMP/old-paths.txt"
    [ -s "$TMP/old-paths.txt" ] || return 0
    perl -pe "$PERL_PROG" < "$TMP/old-paths.txt" > "$TMP/new-paths.txt"
    paste "$TMP/old-paths.txt" "$TMP/new-paths.txt" | awk -F'\t' '$1 != $2' > "$MOVES"
}

discover_rewrites() {
    : > "$REWRITES"
    local f
    while IFS= read -r f; do
        [ -n "$f" ] || continue
        is_frozen "$f" && continue
        printf '%s\n' "$f" >> "$REWRITES"
    done < <(git grep -lIE "$SWEEP_ERE" -- . || true)
}

# --------------------------------------------------------------------------------------------------
# Phase: move
# --------------------------------------------------------------------------------------------------

do_moves() {
    local old new moved=0 d
    while IFS=$'\t' read -r old new; do
        [ -n "$old" ] || continue
        mkdir -p "$(dirname "$new")"
        # `git mv`, never `mv` + `git add`. git mv stages the delete and the add as one operation
        # against the index; the hand-rolled version is where a half-staged move comes from, and a
        # half-staged move is exactly the thing that shows up as delete+create at merge time.
        # Per FILE, not per directory, so a branch where SOME of the tree has already moved (a PR
        # that added one new file under the old path after merging the rename) is handled by the
        # same code path as a clean branch.
        git mv "$old" "$new"
        moved=$((moved + 1))
    done < "$MOVES"

    # git tracks files, not directories, so each emptied io/confluent/... is left behind on disk.
    # Harmless to git, confusing to a human, and it makes a later "already applied" check ambiguous.
    # rmdir -p walks up and stops at the first non-empty parent, which is the source root.
    while IFS= read -r d; do
        [ -n "$d" ] || continue
        rmdir -p "$d" 2>/dev/null || true
    done < <(cut -f1 "$MOVES" | sed 's#/[^/]*$##' | sort -ru)

    echo "  moved ${moved} file(s) with git mv"
}

# --------------------------------------------------------------------------------------------------
# Phase: content rewrite
# --------------------------------------------------------------------------------------------------

do_rewrite() {
    local n
    n=$(count_lines "$REWRITES")
    if [ "$n" -gt 0 ]; then
        tr '\n' '\0' < "$REWRITES" | xargs -0 perl -i -pe "$PERL_PROG"
    fi
    echo "  rewrote references in ${n} file(s)"
}

# bin/check-copyright-headers.sh is frozen for the sweep because half of each RENAMED_FROM_UPSTREAM
# entry must NOT move: the format is `newpath|oldpath-at-fork-point`, and the oldpath names a path in
# the UPSTREAM tree, looked up with `git cat-file blob $FORK_POINT:$oldpath`. So: rewrite everything
# up to the first `|`, leave the rest exactly as it was. Lines with no `|` are the
# EXTRACTED_FROM_UPSTREAM block, which lists CURRENT paths, so those move whole. Idempotent: a second
# run finds nothing left to change on the head side.
retarget_copyright_manifest() {
    local f="bin/check-copyright-headers.sh" prog
    [ -f "$f" ] || return 0
    prog='if (m{^\s*parallel-consumer\S*}) { my $tail = q{}; if (index($_, q{|}) >= 0) { ($_, $tail) = split(/\|/, $_, 2); $tail = q{|} . $tail; } '
    prog+="$PERL_PROG"
    prog+=' $_ .= $tail; }'
    perl -i -pe "$prog" "$f"
    echo "  retargeted the newpath side of the copyright provenance manifest"
}

# --------------------------------------------------------------------------------------------------
# Phase: copyright headers (Apache 2.0 s4(c))
# --------------------------------------------------------------------------------------------------
#
# The rule the repo already enforces: an upstream-derived file MODIFIED on the fork keeps its
# Confluent notice and ADDS `Modifications Copyright (C) <year> Antony Stubbs and contributors`.
# This run has just modified a few hundred of them, so it owes them the line.
#
# "Upstream-derived" needs no fork-point lookup here, because the repo has a stronger invariant to
# lean on: bin/check-copyright-headers.sh FAILS any fork-original file that claims Confluent
# copyright. So a file carrying a Confluent notice IS upstream-derived, by construction - and that
# test also catches the renamed and extracted files a path lookup would miss.
#
# THE COMMENT SYNTAX IS COPIED FROM THE FILE'S OWN CONFLUENT LINE rather than chosen from a table.
# Taking the prefix that already sits in front of `Copyright (C)` makes it structurally impossible to
# emit `//` into a YAML file or `#` into an XML comment: the new line is, by construction, in
# whatever syntax the old one was in. The extension table is then a SECOND, INDEPENDENT check - it
# says what a file of this type is allowed to look like, and the script refuses rather than guesses
# when the two disagree, or when it meets an extension it has never seen.

comment_style_for() { # <path> -> java|hash|xml|adoc, non-zero for "I do not know"
    case "$1" in
        *.java|*.kt|*.groovy|*.js|*.ts) echo java ;;
        *.xml|*.html|*.md|*.markdown)   echo xml ;;
        *.yml|*.yaml|*.sh|*.bash|*.properties|*.conf|*.cfg|*.toml|*.ini) echo hash ;;
        *.adoc|*.asciidoc)              echo adoc ;;
        *) return 1 ;;
    esac
}

prefix_matches_style() { # <style> <prefix> - strict: an unrecognised shape is an error, not a default
    local re
    case "$1" in
        java|adoc) re='^[[:space:]]*(\*|//|/\*)' ;;
        # inside an <!-- --> block the continuation lines carry no marker at all, so whitespace-only
        # is the NORMAL shape for XML; `<!--` on the same line is the other legal one.
        xml)       re='^[[:space:]]*(<!--)?[[:space:]]*$' ;;
        hash)      re='^[[:space:]]*#' ;;
        *)         return 1 ;;
    esac
    [[ "$2" =~ $re ]]
}

MODS_LINE_ADDED=0
MODS_LINE_PRESENT=0

ensure_modifications_line() { # <path>
    local f="$1" header style prefix
    [ -f "$f" ] || return 0

    header="$(head -"$HEADER_WINDOW" "$f")"
    # Herestrings, never `printf | grep -q`: grep -q exits at its first match, the writer takes
    # SIGPIPE, and under pipefail the pipeline then reads as FALSE - so a file that DID match is
    # classified as one that did not. bin/check-shell-sigpipe.sh fails the build over this.
    grep -q "Copyright (C).*Confluent" <<<"$header" || return 0
    if grep -q "Modifications Copyright (C).*${MODS_HOLDER}" <<<"$header"; then
        MODS_LINE_PRESENT=$((MODS_LINE_PRESENT + 1))
        return 0
    fi

    if ! style="$(comment_style_for "$f")"; then
        die "refusing to guess a comment syntax for '${f}' (unrecognised extension).
     It carries a Confluent copyright notice and this run modified it, so Apache 2.0 s4(c) wants
     a Modifications Copyright line - but emitting the wrong comment marker would corrupt the
     file. Add the extension to comment_style_for() in bin/rename-packages.sh, or add the line
     by hand."
    fi

    prefix="$(head -"$HEADER_WINDOW" "$f" | sed -n 's/^\(.*\)Copyright (C).*Confluent.*$/\1/p' | sed -n '1p')"
    if ! prefix_matches_style "$style" "$prefix"; then
        die "the Confluent notice in '${f}' is prefixed with '${prefix}', which is not a ${style}
     comment opener. Refusing to insert a line I cannot place correctly."
    fi

    MODS_PREFIX="$prefix" \
    MODS_TEXT="Modifications Copyright (C) ${MODS_YEAR} ${MODS_HOLDER}" \
    HEADER_WINDOW="$HEADER_WINDOW" \
    perl -i -pe '
        BEGIN { $done = 0 }
        if (!$done && $. <= $ENV{HEADER_WINDOW} && /Copyright \(C\).*Confluent/) {
            $_ .= "$ENV{MODS_PREFIX}$ENV{MODS_TEXT}\n";
            $done = 1;
        }
    ' "$f"
    MODS_LINE_ADDED=$((MODS_LINE_ADDED + 1))
}

do_headers() {
    local f
    # Every file this run touched - the moved ones (their package declaration changed) and the
    # rewritten ones. Files the run did NOT modify get nothing: s4(c) is triggered by modifying a
    # file, and stamping untouched files would be an unrelated policy change riding along.
    {
        [ -s "$MOVES" ] && cut -f2 "$MOVES"
        cat "$REWRITES"
    } | sort -u > "$TMP/touched.txt"

    while IFS= read -r f; do
        [ -n "$f" ] || continue
        is_self "$f" && continue
        ensure_modifications_line "$f"
    done < "$TMP/touched.txt"

    echo "  modifications-copyright line: ${MODS_LINE_ADDED} added, ${MODS_LINE_PRESENT} already present"
}

# --------------------------------------------------------------------------------------------------
# Phase: regenerate what must never be hand-edited
# --------------------------------------------------------------------------------------------------

do_regenerate() {
    if [ -f bin/todo-index.sh ]; then
        bash bin/todo-index.sh > /dev/null
        echo "  regenerated docs/TODO_INDEX.md"
    fi

    [ -f README.adoc ] || return 0
    if [ "$REGEN_README" != true ]; then
        note_manual "README.adoc NOT regenerated (--skip-readme-regen), and therefore EXCUSED from the completeness check. Run: ./mvnw -N process-sources"
        SWEEP_EXCLUDE_EXTRA="README.adoc"
        echo "  skipped README.adoc regeneration (--skip-readme-regen); it is excused from the check"
        return 0
    fi
    if [ ! -x ./mvnw ]; then
        note_manual "README.adoc NOT regenerated (no ./mvnw in this tree), and therefore EXCUSED from the completeness check. Run: ./mvnw -N process-sources"
        SWEEP_EXCLUDE_EXTRA="README.adoc"
        return 0
    fi
    # -N: the asciidoc-template plugin is <inherited>false</inherited> and bound to the ROOT pom, so
    # a non-recursive run regenerates the README without building the reactor.
    #
    # NO -Dcopyright.skip. This used to be mandatory, and it was the sharp edge of the whole rename:
    # bin/check-copyright-headers.sh is bound to the `validate` phase via exec-maven-plugin, so it
    # runs before ANY goal, and it resolved provenance by exact path against the fork-point tree -
    # so the moment the move landed, every upstream-derived file missed the lookup, was judged
    # fork-original, and its REQUIRED Confluent header became a violation. 197 of them, and maven
    # died in validate before doing anything at all. That scanner now maps a path back through its
    # PACKAGE_MOVES table before every lookup, so the rename is invisible to it and the gate runs
    # here exactly as it does on any other build - which is the point: the run that changes ~200
    # copyright headers is the last run that should be skipping the copyright gate.
    echo "  regenerating README.adoc (./mvnw -N process-sources) ..."
    if ! ./mvnw -N -q process-sources; then
        note_manual "./mvnw -N process-sources FAILED - README.adoc is stale. The completeness check below will fail on it; that is correct. Fix the build, regenerate, commit. If it died in the validate phase, read the copyright violations it printed: the provenance model is what is broken, and -Dcopyright.skip=true would only hide it."
        echo "  WARNING: README regeneration failed; the completeness check will report it" >&2
    fi
}

# --------------------------------------------------------------------------------------------------
# Verification: did git actually record the moves as RENAMES?
# --------------------------------------------------------------------------------------------------

verify_renames() { # <rev> <expected-moves> <label>
    local rev="$1" expected="$2" label="$3"
    local statuses n_r n_a n_d n_m sorted lowest n_mispaired
    local raw="$TMP/raw-$$.txt"

    git -c diff.renameLimit="$VERIFY_RENAME_LIMIT" show --raw -M --no-color --format='' "$rev" > "$raw"
    statuses="$(awk '/^:/ { print $5 }' "$raw")"

    n_r=$(grep -c '^R' <<<"$statuses" || true)
    n_a=$(grep -c '^A' <<<"$statuses" || true)
    n_d=$(grep -c '^D' <<<"$statuses" || true)
    n_m=$(grep -c '^M' <<<"$statuses" || true)

    # sort reads its whole input, so no early-exiting reader and nothing for pipefail to promote.
    sorted="$(grep '^R' <<<"$statuses" | sed 's/^R//' | sort -n || true)"
    lowest="${sorted%%$'\n'*}"
    [ -n "$lowest" ] || lowest="n/a"

    echo "  rev                $(git rev-parse --short "$rev")   (${label})"
    echo "  files moved        ${expected}"
    echo "  renames (R)        ${n_r}"
    echo "  adds (A)           ${n_a}"
    echo "  deletes (D)        ${n_d}"
    echo "  modifies (M)       ${n_m}"
    echo "  lowest similarity  R${lowest}   (git's detection threshold is 50)"

    # THE COUNT IS NOT ENOUGH. Rename detection can pair the RIGHT NUMBER of files WRONGLY: five
    # near-identical TestConventionsArchTest.java files, one per module, were observed being paired
    # into a cross-module cycle (streams -> metrics, vertx -> mutiny, ...). A count of R entries sees
    # nothing amiss in that; it is only visible if you ask whether each pairing is the move the
    # script actually made. So: re-derive the expected new path from the old one with the SAME
    # substitution program, and demand it match.
    awk -F'\t' '/^:/ { split($1, a, " "); if (a[5] ~ /^R/) print $2 }' "$raw" > "$TMP/pair-old.txt"
    awk -F'\t' '/^:/ { split($1, a, " "); if (a[5] ~ /^R/) print $3 }' "$raw" > "$TMP/pair-new.txt"
    if [ -s "$TMP/pair-old.txt" ]; then
        perl -pe "$PERL_PROG" < "$TMP/pair-old.txt" > "$TMP/pair-expected.txt"
        paste "$TMP/pair-old.txt" "$TMP/pair-expected.txt" "$TMP/pair-new.txt" |
            awk -F'\t' '$2 != $3' > "$TMP/mispaired.txt"
    else
        : > "$TMP/mispaired.txt"
    fi
    n_mispaired=$(count_lines "$TMP/mispaired.txt")
    echo "  mis-paired         ${n_mispaired}   (renames git invented, pairing a file with one it never came from)"

    if [ "$n_mispaired" -gt 0 ]; then
        echo
        echo "  MIS-PAIRED RENAMES (old / expected new / what git actually paired it with):"
        sed 's/^/      /' "$TMP/mispaired.txt"
        die "git paired ${n_mispaired} file(s) with a destination this script never moved them to.
     The commit's history now asserts a move that did not happen, and a merge will apply a PR's
     edit to the WRONG FILE without conflicting. Re-run with the two-commit default (the pure
     move commit is 100% similar, so it cannot be mis-paired)."
    fi

    if [ "$n_r" -ne "$expected" ]; then
        die "git detected ${n_r} rename(s) but ${expected} file(s) were moved.
     The shortfall shows up as a delete+add pair, and every open PR touching one of those files
     conflicts instead of merging - which is the entire failure this script exists to prevent.
     Check the rename limits printed above. If this happened under --single-commit, drop the
     flag: the two-commit default has a move commit that is 100% similar by construction, so its
     detection cannot fail."
    fi
    if [ "$n_a" -ne 0 ] || [ "$n_d" -ne 0 ]; then
        die "expected no add/delete pairs, got A=${n_a} D=${n_d}.
     Inspect: git show --raw -M $(git rev-parse --short "$rev")"
    fi
    if [ "$expected" -eq 0 ]; then
        echo "  VERDICT            no moves here, and none detected - this commit cannot dilute its parent"
    else
        echo "  VERDICT            every move recorded as a rename, correctly paired, no delete/create pairs"
    fi
}

report_rename_limits() {
    local d m
    d="$(git config diff.renameLimit || true)"
    m="$(git config merge.renameLimit || true)"
    echo "  diff.renameLimit   ${d:-<unset - git default 1000>}"
    echo "  merge.renameLimit  ${m:-<unset - git default 7000>}"
    echo "  The assertion below pins its own limit of ${VERIFY_RENAME_LIMIT}, so it does not depend on ambient"
    echo "  config. The two values above are what a future MERGE will use: if either is below the"
    echo "  moved-file count, raise it before merging the open PR branches."
}

# --------------------------------------------------------------------------------------------------
# Verification: the completeness check
# --------------------------------------------------------------------------------------------------
#
# The one that has to be un-foolable. The two references it exists for - the escaped regex in the
# mutation lane and a misspelt package - both survive a naive sweep AND both fail GREEN afterwards,
# so nothing downstream will tell you they were missed.

completeness_check() {
    local f hits=0 skipped=0
    local live="$TMP/live-hits.txt" skipped_list="$TMP/skipped-hits.txt"
    : > "$live"
    : > "$skipped_list"

    while IFS= read -r f; do
        [ -n "$f" ] || continue
        if is_sweep_excluded "$f"; then
            {
                printf '      %s\n' "$f"
                git grep -nIE "$SWEEP_ERE" -- "$f" | sed 's/^/          /' || true
            } >> "$skipped_list"
            skipped=$((skipped + 1))
            continue
        fi
        git grep -nIE "$SWEEP_ERE" -- "$f" >> "$live" || true
        hits=$((hits + 1))
    done < <(git grep -lIE "$SWEEP_ERE" -- . || true)

    echo "  pattern            ${SWEEP_ERE}"
    echo "                     (tolerates io\\.conflu and io/conflu, and stops before the package"
    echo "                      name so a misspelling downstream of it cannot hide)"
    echo
    echo "  EXCLUDED, with everything each one hid - audit this, do not skim it:"
    if [ -s "$skipped_list" ]; then
        cat "$skipped_list"
    else
        echo "      (nothing matched inside the excluded set)"
    fi
    echo
    echo "  excluded files with matches      ${skipped}"
    echo "  NON-excluded files with matches  ${hits}"

    if [ "$hits" -gt 0 ]; then
        echo
        echo "  STALE REFERENCES:"
        sed 's/^/      /' "$live"
        die "the completeness check found matches in ${hits} file(s) outside the excluded set.
     Each is either a MISS - fix it, and say which sweep should have caught it - or a legitimate
     survivor, in which case add it to FROZEN_PREFIXES with a written justification."
    fi
    echo "  VERDICT            no stale references outside the excluded set"
}

# --------------------------------------------------------------------------------------------------
# Prose guards
# --------------------------------------------------------------------------------------------------

check_prose_guards() {
    local path ere advice found=0
    while IFS='|' read -r path ere advice; do
        [ -n "$path" ] || continue
        [ -f "$path" ] || continue
        if grep -nE "$ere" "$path" > "$TMP/prose.txt"; then
            found=$((found + 1))
            echo
            echo "  ${path}"
            sed 's/^/      /' "$TMP/prose.txt"
            echo "      -> ${advice}"
            note_manual "prose in ${path}:$(sed -n '1s/:.*//p' "$TMP/prose.txt") - ${advice}"
        fi
    done <<EOF
$PROSE_GUARDS
EOF

    if [ "$found" -eq 0 ]; then
        echo "  none found"
        return 0
    fi
    if [ "$DEFER_PROSE" = true ]; then
        echo
        echo "  --defer-prose: continuing. The ${found} claim(s) above are manual follow-ups."
        return 0
    fi
    echo
    die "${found} sentence(s) in the tree CLAIM the packages are unchanged. Rewriting them
     mechanically produces a confident false statement in a published artifact, which is worse
     than leaving them stale, so this script will not do it. Edit the prose and re-run - or pass
     --defer-prose to proceed and carry them as follow-ups, which is what you want on a PR
     branch, where the corrected prose arrives from master at merge."
}

# --------------------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------------------

echo "== package rename: io.confluent.* -> bz.stub.*"
echo "   branch $(git rev-parse --abbrev-ref HEAD)   tree $(pwd)"

if [ "$VERIFY_ONLY" = true ]; then
    section "verification: completeness"
    completeness_check
    echo
    echo "--verify-only: nothing was changed."
    exit 0
fi

discover_moves
discover_rewrites
n_moves=$(count_lines "$MOVES")
n_rewrites=$(count_lines "$REWRITES")

# --- idempotency ----------------------------------------------------------------------------------
# "Already applied" is decided by the TREE, not by a marker file or a commit message, so it is
# correct on any branch at any time - including a branch that merged the rename and THEN grew a new
# file under the old path, which must come out as "there is work to do", not as "already done".
if [ "$n_moves" -eq 0 ] && [ "$n_rewrites" -eq 0 ]; then
    section "already applied, nothing to do"
    echo "  no tracked path matches   ${PATH_SCAN_ERE}"
    echo "  no rewritable file matches  ${SWEEP_ERE}"
    echo
    echo "  Re-running is a no-op by design. This script is meant to be run on master and on every"
    echo "  open PR branch, and again on a branch that later adds a file under the old path."
    exit 0
fi

section "work set"
echo "  files to move      ${n_moves}"
echo "  files to rewrite   ${n_rewrites}"
echo "  frozen (never rewritten - generated files here are REGENERATED, not excused):"
sed 's/^/      /' <<EOF
$FROZEN_PREFIXES
EOF
echo "  excused from the completeness check (a shorter list, on purpose):"
sed 's/^/      /' <<EOF
$SWEEP_EXCLUDE
EOF

section "prose that a mechanical rewrite would falsify"
check_prose_guards

if [ "$APPLY" != true ]; then
    section "dry run"
    echo "  would move:"
    head -5 "$MOVES" | sed 's/^/      /'
    [ "$n_moves" -gt 5 ] && echo "      ... and $((n_moves - 5)) more"
    echo "  would rewrite:"
    head -10 "$REWRITES" | sed 's/^/      /'
    [ "$n_rewrites" -gt 10 ] && echo "      ... and $((n_rewrites - 10)) more"
    echo
    echo "--dry-run: nothing was changed."
    exit 0
fi

if [ "$DO_COMMIT" = true ] && [ -n "$(git status --porcelain)" ]; then
    {
        echo "ERROR: working tree is not clean."
        echo "  This script commits what it changes, and a dirty tree would sweep unrelated work into"
        echo "  the rename commit - which is exactly what makes a rename un-mergeable. Commit or"
        echo "  stash first, or pass --no-commit."
    } >&2
    exit 2
fi

MOVE_REV=""

section "phase 1: move"
do_moves
if [ "$SPLIT_COMMITS" = true ] && [ "$DO_COMMIT" = true ] && [ "$n_moves" -gt 0 ]; then
    git commit -q -m "refactor: move io.confluent.* to bz.stub.* (pure rename, no content changes)" \
        -m "Directory move only, so every path is 100% similar and git's exact-rename detection
cannot fail on it. The content edits follow in the next commit.

Generated by bin/rename-packages.sh."
    MOVE_REV="$(git rev-parse HEAD)"
    echo "  committed the moves on their own"
fi

section "phase 2: rewrite"
# Recomputed against the POST-MOVE tree: the moved files are at their new paths now, and the move
# itself changed nothing textual, so they are still in the rewrite set - under their new names.
discover_rewrites
n_rewrites=$(count_lines "$REWRITES")
do_rewrite
retarget_copyright_manifest
do_headers
do_regenerate

section "commit"
if [ "$DO_COMMIT" != true ]; then
    echo "  --no-commit: the changes are in the working tree and index, uncommitted."
    echo "  Rename verification is SKIPPED, because there is no commit to inspect. Commit the moves"
    echo "  and the edits TOGETHER (the measured default) and then run:"
    echo "      git show --raw -M HEAD"
    echo "  expecting ${n_moves} R-entries and no A/D pairs."
else
    git add -A
    if [ "$SPLIT_COMMITS" = true ]; then
        git commit -q -m "refactor: rename io.confluent.* references to bz.stub.* (content only)" \
            -m "Text edits only. No file moves in this commit, so it cannot dilute the rename
detection in its parent.

Generated by bin/rename-packages.sh."
    else
        git commit -q -m "refactor: rename packages io.confluent.* to bz.stub.*" \
            -m "Moves the package directories and rewrites every reference in one atomic commit, so
the tree compiles at every point in history and a PR branch has one commit to reconcile
rather than two rewrites of the same file. Both shapes were measured before choosing:
folding the content edits into the move left every rename detected, worst similarity 96%,
against git's 50% threshold.

Covers the forms a plain find-and-replace cannot see: the escaped-regex spelling in the
mutation and quarantine scripts, the io/confluent path form, ArchUnit @AnalyzeClasses
package strings, the truth-generator <class> list, the META-INF/services listener, the
logback logger names, and a misspelt variant.

Generated by bin/rename-packages.sh."
    fi
    echo "  committed $(git rev-parse --short HEAD)"
fi

section "verification: did git record the moves as RENAMES?"
report_rename_limits
echo
if [ "$DO_COMMIT" != true ]; then
    echo "  SKIPPED - nothing was committed (--no-commit). See the note above."
elif [ "$n_moves" -eq 0 ]; then
    echo "  no files moved on this branch, so there is nothing to detect."
elif [ -n "$MOVE_REV" ]; then
    echo "  [move commit]"
    verify_renames "$MOVE_REV" "$n_moves" "pure move"
    echo
    echo "  [content commit]"
    verify_renames HEAD 0 "content only"
else
    verify_renames HEAD "$n_moves" "single atomic commit"
fi

section "verification: completeness"
completeness_check

section "summary"
echo "  moved              ${n_moves} file(s)"
echo "  rewrote            ${n_rewrites} file(s)"
echo "  header lines       ${MODS_LINE_ADDED} added, ${MODS_LINE_PRESENT} already present"
if [ -s "$MANUAL_FOLLOWUPS" ]; then
    echo
    echo "  MANUAL FOLLOW-UPS - this script could not do these and will not pretend otherwise:"
    cat "$MANUAL_FOLLOWUPS"
fi
echo
echo "  NOT covered here, and NOT green merely because this exited 0:"
echo "   - The mutation lane EXITS 0 when it matches nothing. On the first PR after this lands,"
echo "     change a class under the decidable packages and read the job summary for a mutation score"
echo "     and a survivor list. A green tick carrying 'nothing to mutate, skipping' is the FAILURE"
echo "     mode, not the pass."
echo "   - ArchUnit rules pin package names as STRINGS and pass vacuously when they select nothing."
echo "     Break one on purpose and watch it go red before believing the suite still guards anything."
echo
echo "done."
