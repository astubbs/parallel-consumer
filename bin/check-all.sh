#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# EVERY GATE IN bin/, DISCOVERED RATHER THAN LISTED. Run this before you push.
#
# WHY THIS EXISTS. There are dozens of `check-*.sh` gates and about as many self-tests - `ls bin/`
# is the count, and writing one here is how it goes stale - and an agent preparing a push had to
# know which ones applied. In practice that means running a subset
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
#   bin/check-all.sh                 # the tree gates - what to run before a push
#   bin/check-all.sh --pr            # also the PR-state reporters (merge prep)
#   bin/check-all.sh --with-tests    # also the self-tests (what CI runs)
#   bin/check-all.sh --tests-only    # only the self-tests
#
# THE DEFAULT IS GATES ONLY, AND THAT IS THE POINT. The gates answer "is my tree healthy" in about
# 25 seconds. The self-tests answer "do the gates themselves still work", take minutes - two of them
# build scratch repositories - and their answer changes only when somebody edits a gate. Bundling
# them made the routine command slow enough to skip, and a pre-push sweep that gets skipped protects
# nothing, which is the same failure `check-all` was written to fix.
#
# NOTHING IS LOST IN CI: .github/workflows/repo-hygiene.yml runs `--with-tests`, so the self-tests
# run there in one go. It no longer NAMES them - that workflow used to carry a job per self-test, and
# a self-test added to bin/ ran nowhere until somebody remembered to wire it. So a self-test added
# tomorrow is swept with no edit there and no edit here, which is the same discovery property the top
# of this file describes; do not reintroduce a list in either place.

set -uo pipefail

cd "$(dirname "$0")/.."

MODE=gates
WITH_PR=0
for arg in "$@"; do
    case "$arg" in
        --pr)         WITH_PR=1 ;;
        --with-tests) MODE=all ;;
        # Kept because scripts and habits name it; it is now what happens anyway.
        --gates-only) MODE=gates ;;
        --tests-only) MODE=tests ;;
        # To the end of the header, not to a hardcoded line. `5,35p` silently stopped short the
        # moment the header grew: it already omitted the `--tests-only` usage line, and the section
        # explaining that the default now EXCLUDES the self-tests - the biggest behavioural fact
        # about this script - landed below the cutoff and never reached anyone running `--help`.
        -h|--help)    sed -n '5,/^set -uo pipefail$/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'; exit 0 ;;
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

# RUN THE SWEEP CONCURRENTLY, ACCOUNT FOR IT IN ORDER. The gates are independent read-only scans of
# the tree, and run back to back they took about 25 seconds, with the slowest ones bunched within a
# few seconds of each other - no single dominator to optimise, so there was nothing to fix except the
# serialisation. Sequential was costing the wall-clock sum of an I/O-bound set for no reason.
#
# WHY A CAPTURE/REPLAY SPLIT rather than backgrounding run_one directly: every counter here lives in
# a shell variable, and a background job is a subshell, so increments would be discarded and the
# sweep would report zero of everything. Each gate now writes its exit code, timing and output to a
# file; the accounting and the printing happen afterwards, in the original order, from those files.
# Output stays deterministic and attributable, which matters more than the seconds saved.
#
# NO `wait -n` AND NO ASSOCIATIVE ARRAYS - macOS ships bash 3.2, where both are syntax errors. The
# set is small enough that launching all of it and waiting once is fine; a job cap would need exactly
# the bash 4 features that are unavailable on half the machines this runs on.
run_capture() { # <script>|SKIP:<reason> <label> <outfile-prefix>
    local script="$1" label="$2" pre="$3" start end rc out syntax
    case "$script" in
        SKIP:*)
            printf '%s\n%s\n%s\n' "SKIP" "0" "$label" > "$pre.meta"
            printf '%s\n' "${script#SKIP:}" > "$pre.out"
            return 0 ;;
    esac
    start=$(date +%s)
    # DISPATCH BY EXTENSION. Everything used to be shell, so this ran every gate through bash - which
    # gives a .mjs gate a bash syntax error and reports a clean rule as a broken gate. Found the same
    # hour the first Node gate appeared, by this sweep, which is the argument for having the sweep.
    case "$script" in
        *.mjs) checker="node --check"; runner="node" ;;
        *)     checker="bash -n";      runner="bash" ;;
    esac
    if ! syntax="$($checker "$script" 2>&1)"; then
        end=$(date +%s)
        printf '%s\n%s\n%s\n' "PARSE" "$((end - start))" "$label" > "$pre.meta"
        printf '%s\n' "$syntax" > "$pre.out"
        return 0
    fi
    out="$(PR_NUMBER="${PR_NUMBER:-}" $runner "$script" 2>&1)"; rc=$?
    end=$(date +%s)
    printf '%s\n%s\n%s\n' "$rc" "$((end - start))" "$label" > "$pre.meta"
    printf '%s\n' "$out" > "$pre.out"
    return 0
}

# Replays ONE captured result. Identical accounting and identical output to running it inline; the
# only difference is that the work already happened, in parallel, in run_capture.
run_one() { # <rc> <secs> <label> <capture-prefix>
    local rc="$1" secs="$2" label="$3" pre="$4" out
    out="$(cat "$pre.out" 2>/dev/null)"

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
    # A SKIP IS A RESULT, and it replays through here for one reason: printing it during the scatter
    # phase would put every skip above every gate, silently reordering a report whose order is
    # discovery order. It costs one branch to keep the output identical to the sequential sweep.
    if [ "$rc" = "SKIP" ]; then
        skipped=$((skipped + 1))
        printf '  skip    %-42s      (%s)\n' "$label" "$out"
        return
    fi
    ran=$((ran + 1))

    if [ "$rc" = "PARSE" ]; then
        fail=$((fail + 1)); failed_names="${failed_names} ${label}"
        printf '  FAIL    %-42s %ss  (does not parse - this is a broken gate, not a skip)\n' "$label" "$secs"
        printf '%s\n' "$out" | head -3 | sed 's/^/          | /'
        return
    fi

    case "$rc" in
        0) pass=$((pass + 1));     printf '  ok      %-42s %ss\n' "$label" "$secs" ;;
        2) cannot=$((cannot + 1)); cannot_names="${cannot_names} ${label}"
           printf '  CANNOT  %-42s %ss  (exit 2 - not a pass)\n' "$label" "$secs" ;;
        3) nothing=$((nothing + 1))
           printf '  none    %-42s %ss  (exit 3 - nothing in scope)\n' "$label" "$secs" ;;
        *) fail=$((fail + 1)); failed_names="${failed_names} ${label}"
           printf '  FAIL    %-42s %ss  (exit %s)\n' "$label" "$secs" "$rc"
           printf '%s\n' "$out" | tail -6 | sed 's/^/          | /' ;;
    esac
}

# Launches every member of a set at once, waits, then replays them in the order given - so the
# report reads exactly as it did when the sweep was sequential.
CAP_DIR="$(mktemp -d)"
trap 'rm -rf "$CAP_DIR"' EXIT
sweep() { # <script>|<label> pairs, script first
    local i=0 script label meta rc secs replayed=0 n
    local labels=()
    while [ "$#" -gt 0 ]; do
        script="$1"; label="$2"; shift 2
        i=$((i + 1))
        labels[i]="$label"
        run_capture "$script" "$label" "$CAP_DIR/$(printf '%03d' "$i")" &
    done
    wait
    # The three lines run_capture wrote, read once, in the order it wrote them. `read` rather than
    # three `sed -n Np` calls so the file is opened once and the field order lives in one place
    # instead of being split between this loop and run_one.
    for meta in "$CAP_DIR"/*.meta; do
        [ -f "$meta" ] || continue
        replayed=$((replayed + 1))
        { IFS= read -r rc; IFS= read -r secs; IFS= read -r label; } < "$meta"
        run_one "$rc" "$secs" "$label" "${meta%.meta}"
    done

    # A CAPTURE THAT NEVER WRITES ITS .meta IS NOT A SKIP AND NOT A PASS. `run_capture` writes that
    # file only on a normal return, so a gate killed mid-run - SIGKILL, OOM, a CI step timeout - never
    # writes one. The glob above silently steps over it, and without this block it is counted in NONE
    # of pass/fail/cannot/nothing/skipped: a gate that did not run would vanish from the sweep, which
    # still reports full success and exits 0. Compare launched (i) against replayed and account for
    # the gap explicitly, by label, using the launch order recorded above the `&`.
    #
    # ROUTED INTO fail/failed_names, NOT cannot: exit 2 ("cannot run") never flips the sweep's exit
    # code, and a vanished gate must, because there is strictly LESS evidence for it than for a gate
    # that ran to completion and reported "cannot run" for a missing tool or credential. Folding this
    # into `cannot` would let a killed gate slip out through the one bucket the exit code ignores -
    # exactly the false-green this fix exists to close. `ran` is also incremented so the printed
    # total (ran = pass+fail+cannot+nothing) still reconciles.
    if [ "$replayed" -lt "$i" ]; then
        for ((n = 1; n <= i; n++)); do
            [ -f "$CAP_DIR/$(printf '%03d' "$n").meta" ] && continue
            label="${labels[n]}"
            ran=$((ran + 1)); fail=$((fail + 1)); failed_names="${failed_names} ${label}"
            printf '  FAIL    %-42s  ?s  (no result - process vanished mid-run: killed, OOM, or\n' "$label"
            printf '                                              timed out. A gate that did not run is NOT a pass.)\n'
        done
    fi
    rm -f "$CAP_DIR"/*.meta "$CAP_DIR"/*.out 2>/dev/null || true
}

# Self-tests first, the order CI uses: bin/AGENTS.md requires a gate's self-test to run BEFORE the
# gate it protects, so a broken checker surfaces as a broken checker rather than as a mysterious
# failure in whatever it was checking.
if [ "$MODE" = "all" ] || [ "$MODE" = "tests" ]; then
    echo "=== self-tests ==="
    set --
    for t in bin/test-*.sh; do
        [ -f "$t" ] || continue
        set -- "$@" "$t" "$(basename "$t")"
    done
    [ "$#" -gt 0 ] && sweep "$@"
fi

if [ "$MODE" = "all" ] || [ "$MODE" = "gates" ]; then
    echo "=== gates ==="
    set --
    # BOTH SUFFIXES. Node is now the default language for new scripts here (bin/AGENTS.md, "Write it in
    # Node"), so a gate written the new way must be swept by the same run as the old ones. Globbing only
    # *.sh would have left every future gate discovered by nobody - a sweep that reports "no gate failed"
    # while never having looked, which is the failure this script exists to prevent, arriving through
    # the door a language change opened.
    for g in bin/check-*.sh bin/check-*.mjs; do
        [ -f "$g" ] || continue
        n="$(basename "$g")"
        [ "$n" = "$SELF" ] && continue                       # never recurse
        if [ "$WITH_PR" -eq 0 ] && [[ " $PR_SCOPED " == *" $n "* ]]; then
            set -- "$@" "SKIP:$(reason_for "$n") - use --pr" "$n"
            continue
        fi
        if [[ " $NEEDS_ARGS " == *" $n "* ]]; then
            set -- "$@" "SKIP:$(reason_for "$n")" "$n"
            continue
        fi
        set -- "$@" "$g" "$n"
    done
    [ "$#" -gt 0 ] && sweep "$@"
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
