#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# EVERY GATE IN bin/, DISCOVERED RATHER THAN LISTED. Run this before you push.
#
# WHY THIS EXISTS. There are eighteen `check-*.sh` gates and twenty-one self-tests, and an agent
# preparing a push had to know which ones applied. In practice that means running a subset
# remembered from earlier in the session, which is how astubbs#356 pushed a branch that failed
# `check-branch-self-reference.sh` in CI: the sweep before that push ran seven gates chosen by hand,
# and that was not one of them. The gate was not new, not subtle, and not broken. It simply was not
# on somebody's list.
#
# So the list is gone. This script GLOBS, and a gate added tomorrow is picked up with no edit here -
# which also means nobody has to remember to register it. That property is the point, and it is why
# the discovery loop must never become an explicit array however tempting it looks.
#
# A SKIP IS NOT A PASS, the house rule this repo keeps relearning. Gates that exit 2 for "cannot
# run" - no `gh`, no credential, no network - get their own column and are never counted as passes,
# because a run that could not measure anything must not look like one that measured and found
# nothing. Same for exit 3, "nothing in scope".
#
# THE ONE EXCEPTION LIST, and the assertion that keeps it honest. A few `check-*.sh` are not tree
# gates: they take a required argument, or they report the state of a PULL REQUEST rather than the
# state of the tree, so running them in a pre-push sweep produces noise that trains people to ignore
# the output. They are skipped by default and run under `--pr`. Because a hand-maintained list is
# exactly what this script exists to abolish, every name in it is ASSERTED TO EXIST: rename one of
# these scripts and this runner fails loudly instead of quietly checking less than you think.
#
# Exit codes: 0 everything that could run passed, 1 at least one FAILED, 2 nothing ran at all.
#
# Usage:
#   bin/check-all.sh                 # self-tests, then tree gates - what to run before a push
#   bin/check-all.sh --pr            # also the PR-state reporters (merge prep)
#   bin/check-all.sh --gates-only    # skip the self-tests
#   bin/check-all.sh --tests-only    # only the self-tests

set -uo pipefail

cd "$(dirname "$0")/.."

MODE=all
WITH_PR=0
for arg in "$@"; do
    case "$arg" in
        --pr)         WITH_PR=1 ;;
        --gates-only) MODE=gates ;;
        --tests-only) MODE=tests ;;
        -h|--help)    sed -n '5,35p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "check-all: unknown argument '$arg'" >&2; exit 2 ;;
    esac
done

SELF="$(basename "$0")"

# NOT TREE GATES. Each line is a name and the reason it is not in the default sweep.
PR_SCOPED="check-pr-ready.sh check-human-lgtm.sh check-review-posted.sh check-pr-analysis-surfaces.sh"
NEEDS_ARGS="check-ossindex-audit.sh"

reason_for() {
    case "$1" in
        check-pr-ready.sh)             echo "reports PR readiness, not tree health" ;;
        check-human-lgtm.sh)           echo "asks GitHub whether a human approved" ;;
        check-review-posted.sh)        echo "asks GitHub whether a review was posted" ;;
        check-pr-analysis-surfaces.sh) echo "reads a PR's analysis annotations" ;;
        check-ossindex-audit.sh)       echo "requires a maven log argument" ;;
        *)                             echo "excluded" ;;
    esac
}

# THE ASSERTION THAT STOPS THIS LIST ROTTING, in both directions.
#
# Existence alone is the weaker half: it catches a rename, and is blind to a script whose REASON
# stopped being true. If `check-ossindex-audit.sh` were changed to stop requiring an argument it
# would sit in NEEDS_ARGS being skipped forever, which is "checking less than it claims" arriving by
# staleness rather than deletion. So each recorded reason is corroborated by an independent signal
# in the script itself, and a mismatch fails loudly: the list is five entries, a false positive
# costs a minute, and silent staleness is the defect this whole script exists to remove.
missing=""; uncorroborated=""
for n in $PR_SCOPED $NEEDS_ARGS; do
    if [ ! -f "bin/$n" ]; then missing="${missing} ${n}"; continue; fi
    case " $PR_SCOPED " in
        *" $n "*)  # claimed to report PR state - it must actually talk to GitHub
            grep -q "gh " "bin/$n" || uncorroborated="${uncorroborated} ${n}(no gh call)" ;;
    esac
    case " $NEEDS_ARGS " in
        *" $n "*)  # claimed to need an argument - it must actually say so
            grep -q "usage:" "bin/$n" || uncorroborated="${uncorroborated} ${n}(no usage line)" ;;
    esac
done
if [ -n "$missing" ]; then
    echo "check-all: the exception list names script(s) that do not exist:${missing}" >&2
    echo "check-all: renamed or deleted? Fix the list - until then this sweep is checking less than" >&2
    echo "check-all: it claims to, which is the exact failure this script was written to prevent." >&2
    exit 2
fi
if [ -n "$uncorroborated" ]; then
    echo "check-all: an exception's stated reason is no longer corroborated:${uncorroborated}" >&2
    echo "check-all: the script changed character. Re-read it: if it is now an ordinary tree gate," >&2
    echo "check-all: drop it from the list rather than leaving it skipped on a reason that expired." >&2
    exit 2
fi

pass=0; fail=0; cannot=0; nothing=0; ran=0; skipped=0
failed_names=""; cannot_names=""

run_one() {
    local script="$1" label="$2" start end rc out syntax
    start=$(date +%s)

    # A GATE BASH CANNOT PARSE ALSO EXITS 2, colliding with the exit-2 "cannot run" convention this
    # repo uses everywhere - leave an unresolved merge-conflict marker in a check script and `bash
    # the-script` returns 2 from the PARSER, before one line of its logic runs.
    #
    # THE SWEEP IS NOT USUALLY BLIND TO THAT, and the review that raised it overstated the case:
    # `check-shell-lint.sh` runs ShellCheck over all of `bin/` at severity=error, and an unterminated
    # `if` is SC1072/SC1073, so the sweep normally goes red via the linter. Measured, not assumed.
    #
    # The residual hole is narrow and real: `check-shell-lint.sh` itself exits 2 when ShellCheck is
    # not installed. On such a machine the broken gate exits 2 and the linter exits 2, both land in
    # `cannot`, and the sweep reports zero failures - a false green produced by two skips agreeing.
    # The second reason is plain attribution: "does not parse" names the broken gate, where "CANNOT
    # RUN (usually a missing tool or credential)" sends the reader looking for a missing credential.
    if ! syntax="$(bash -n "$script" 2>&1)"; then
        end=$(date +%s)
        ran=$((ran + 1)); fail=$((fail + 1)); failed_names="${failed_names} ${label}"
        printf '  FAIL    %-42s %ss  (does not parse - this is a broken gate, not a skip)\n' \
            "$label" "$((end - start))"
        printf '%s\n' "$syntax" | head -3 | sed 's/^/          | /'
        return
    fi

    out="$(PR_NUMBER="${PR_NUMBER:-}" bash "$script" 2>&1)"
    rc=$?
    end=$(date +%s)
    ran=$((ran + 1))
    case "$rc" in
        0) pass=$((pass + 1));     printf '  ok      %-42s %ss\n' "$label" "$((end - start))" ;;
        2) cannot=$((cannot + 1)); cannot_names="${cannot_names} ${label}"
           printf '  CANNOT  %-42s %ss  (exit 2 - not a pass)\n' "$label" "$((end - start))" ;;
        3) nothing=$((nothing + 1))
           printf '  none    %-42s %ss  (exit 3 - nothing in scope)\n' "$label" "$((end - start))" ;;
        *) fail=$((fail + 1)); failed_names="${failed_names} ${label}"
           printf '  FAIL    %-42s %ss  (exit %s)\n' "$label" "$((end - start))" "$rc"
           printf '%s\n' "$out" | tail -6 | sed 's/^/          | /' ;;
    esac
}

# Self-tests first, the order CI uses: bin/AGENTS.md requires a gate's self-test to run BEFORE the
# gate it protects, so a broken checker surfaces as a broken checker rather than as a mysterious
# failure in whatever it was checking.
if [ "$MODE" = "all" ] || [ "$MODE" = "tests" ]; then
    echo "=== self-tests ==="
    for t in bin/test-*.sh; do
        [ -f "$t" ] || continue
        run_one "$t" "$(basename "$t")"
    done
fi

if [ "$MODE" = "all" ] || [ "$MODE" = "gates" ]; then
    echo "=== gates ==="
    for g in bin/check-*.sh; do
        [ -f "$g" ] || continue
        n="$(basename "$g")"
        [ "$n" = "$SELF" ] && continue                       # never recurse
        if [ "$WITH_PR" -eq 0 ] && [[ " $PR_SCOPED " == *" $n "* ]]; then
            skipped=$((skipped + 1))
            printf '  skip    %-42s      (%s - use --pr)\n' "$n" "$(reason_for "$n")"
            continue
        fi
        if [[ " $NEEDS_ARGS " == *" $n "* ]]; then
            skipped=$((skipped + 1))
            printf '  skip    %-42s      (%s)\n' "$n" "$(reason_for "$n")"
            continue
        fi
        run_one "$g" "$n"
    done
fi

echo
if [ "$ran" -eq 0 ]; then
    echo "check-all: NOTHING RAN - no scripts matched. A broken checkout, not a clean tree." >&2
    exit 2
fi
printf 'check-all: %s ran - %s passed, %s failed, %s could not run, %s nothing-in-scope, %s skipped\n' \
    "$ran" "$pass" "$fail" "$cannot" "$nothing" "$skipped"

if [ "$cannot" -gt 0 ]; then
    printf 'check-all: COULD NOT RUN:%s\n' "$cannot_names"
    echo   "check-all:   these measured nothing. Usually a missing tool or credential - fix it, or"
    echo   "check-all:   know that CI is the first place that gate actually runs."
fi
if [ "$fail" -gt 0 ]; then
    printf 'check-all: FAILED:%s\n' "$failed_names" >&2
    exit 1
fi
echo "check-all: no gate failed."
exit 0
