#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Pulls per-class timings and throughput rates out of historical CI logs into a local history file.
#
# Usage: bin/perf-backfill.sh [max-runs]                  collect into the history file (default 40)
#        bin/perf-backfill.sh --suggest-baseline [n]      print a docs/perf-baseline.tsv block to paste
# Output: $PC_PERF_HISTORY, default ~/.parallel-consumer/perf-history.tsv
#
# WHY THIS EXISTS: THE SERIES CANNOT BE BUILT FORWARD FROM HERE
#
# Nothing in this repo stores throughput across runs - no data branch, no write grant, and run
# artifacts expire. So a threshold for bin/check-throughput-regression.sh has been deferred since
# August waiting for "a few runs of real spread", which waiting cannot supply.
#
# It does not have to be built forward. Every performance run this project has ever done, inside
# GitHub's log-retention window, has its per-class failsafe times sitting in the job log. That is
# enough to compute the same relative measure the check uses - did the subject slow down MORE than its
# neighbours - for the whole window, today, read-only.
#
# WHAT CAN AND CANNOT BE BACKFILLED, WHICH IS THE POINT WORTH READING
#
# RATES cannot. ThroughputReport landed on 2026-09-01; before that no run emitted a PC-THROUGHPUT line
# and no amount of log mining will produce one. Rates are collected here anyway, for runs that have
# them, so the history becomes richer going forward rather than starting empty.
#
# CLASS TIMES can, all the way back. They are a coarser instrument - a class time includes container
# pull, topic creation and teardown, and it SATURATES when a test strikes its deadline rather than
# continuing to grow - but the saturation is harmless here and arguably helpful: a run that hit the
# ceiling shows up as a class time pinned near the bound, which is exactly the shape being looked for.
#
# THE MEASURE: subject class time divided by the sum of its neighbours', in the SAME run. Machine
# speed cancels, so runs from different runners are comparable, which raw seconds are not - the same
# tree has been seen at a 1.54x spread. Lower is better. On master's own run at the time of writing:
# 51.29 / (51.69 + 38.85 + 39.63) = 0.394.
#
# READ-ONLY AND OFF-REPO. It writes one file outside the working tree and pushes nothing anywhere. The
# history is deliberately NOT in git: the decision not to keep a series in the repository stands, and
# this is the other way of honouring it - the data lives where the person analysing it is.
#
# IT USES THE RUN-LOGS ARCHIVE, NEVER `gh run view --log`, which silently truncates - see
# docs/solutions/workflow-issues/gh-run-view-log-truncation.md. That trap produced a confident wrong
# conclusion during the investigation that led to this script.

set -uo pipefail

REPO=astubbs/parallel-consumer

# --suggest-baseline answers "the product got FASTER, now what". A hand-updated baseline only ever
# ratchets when somebody remembers, so it silently loses sensitivity: the check keeps comparing against
# a floor the code left behind, and a later regression is measured from too low a bar with nothing
# going red to say so.
#
# IT PICKS THE MEDIAN RUN, NOT THE BEST ONE. Baselining on the fastest run seen would set the bar at an
# outlier and every ordinary run afterwards would read as a regression - the classic way a performance
# gate earns its reputation and gets switched off. The median of recent healthy runs is what "normal"
# means.
#
# IT PRINTS ONE RUN'S NUMBERS, NEVER A BLEND. docs/perf-baseline.tsv requires every row to come from a
# single run, because a rate from one run normalised by class times from another is not a comparison.
# So this selects the median RUN and emits that run verbatim, rather than averaging fields separately.
#
# IT PRINTS; IT DOES NOT WRITE. Re-baselining is a judgement - "the product got faster" and "the
# regression is now the baseline" produce identical numbers, and only a human knows which happened.
SUGGEST=false
if [ "${1:-}" = "--suggest-baseline" ]; then SUGGEST=true; shift; fi
MAX_RUNS="${1:-40}"
HISTORY="${PC_PERF_HISTORY:-$HOME/.parallel-consumer/perf-history.tsv}"
ROOT_BASELINE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/docs/perf-baseline.tsv"
SUBJECT=MultiInstanceHighVolumeTest
# A FIXED neighbour set, not "every other class that ran". The first version summed all of them and the
# sum swung from 130s to 1335s across branches - not because any machine was 10x slower, but because
# different branches run different performance classes, and a branch that adds one moves the
# denominator. That conflates "the subject slowed" with "the lane changed", which is the one thing this
# measure exists to keep apart. Naming the three makes a branch that adds a class comparable with one
# that does not, and a branch that DROPS one visible as a smaller denominator rather than silently
# rescaling every ratio.
NEIGHBOURS="VeryLargeMessageVolumeTest LargeVolumeInMemoryTests LoadTest"

mkdir -p "$(dirname "$HISTORY")" || exit 2
if [ ! -f "$HISTORY" ]; then
    printf '# run_id\tcreated\tbranch\tconclusion\tsubject_s\tneighbours_s\tratio\trate\tsha\n' > "$HISTORY"
fi

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# Runs already collected, so re-running is cheap and additive rather than duplicating rows.
seen="$work/seen"
awk -F'\t' '!/^#/ { print $1 }' "$HISTORY" | sort -u > "$seen"

echo "Collecting up to $MAX_RUNS runs into $HISTORY"
gh run list -R "$REPO" --workflow maven.yml --limit "$MAX_RUNS" \
    --json databaseId,createdAt,headBranch,conclusion,headSha \
    --jq '.[] | [.databaseId, .createdAt, .headBranch, (.conclusion // "running"), .headSha] | @tsv' \
    > "$work/runs.tsv" || { echo "could not list runs" >&2; exit 2; }

added=0; skipped=0; nodata=0
while IFS=$'\t' read -r id created branch conclusion sha; do
    if grep -qx "$id" "$seen" 2>/dev/null; then skipped=$((skipped + 1)); continue; fi

    if ! gh api "repos/$REPO/actions/runs/$id/logs" > "$work/logs.zip" 2>/dev/null; then
        # Logs expire before runs do, so an old run with no archive is ordinary, not an error.
        nodata=$((nodata + 1)); continue
    fi
    rm -rf "$work/x"; mkdir -p "$work/x"
    unzip -qq -o "$work/logs.zip" -d "$work/x" 2>/dev/null

    log=$(find "$work/x" -name '*Performance Tests*.txt' -size +1k 2>/dev/null | head -1)
    if [ -z "$log" ]; then nodata=$((nodata + 1)); continue; fi

    # One pass over the class lines: the subject's seconds, and the sum of every other class's.
    read -r subject neighbours < <(
        grep -ohE 'Time elapsed: [0-9.]+ s[^-]*-- in [A-Za-z0-9_.]+' "$log" \
        | sed 's/Time elapsed: //; s/ s.*-- in .*\./ /' \
        | awk -v s="$SUBJECT" -v n="$NEIGHBOURS" '
            BEGIN { split(n, keep, " "); for (i in keep) want[keep[i]] = 1 }
            { if ($2 == s) subj += $1; else if ($2 in want) other += $1 }
            END { printf "%.2f %.2f", subj, other }'
    )
    if [ "$(awk -v a="$subject" 'BEGIN { print (a > 0) }')" != "1" ] \
    || [ "$(awk -v a="$neighbours" 'BEGIN { print (a > 0) }')" != "1" ]; then
        # A run where the subject or every neighbour is missing yields no ratio. Counted, not invented.
        nodata=$((nodata + 1)); continue
    fi
    ratio=$(awk -v a="$subject" -v b="$neighbours" 'BEGIN { printf "%.4f", a / b }')
    rate=$(grep -ohE "PC-THROUGHPUT test=$SUBJECT .*recordsPerSecond=[0-9-]+" "$log" \
           | sed 's/.*recordsPerSecond=//; s/ .*//' | tail -1)

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$id" "${created:0:19}" "$branch" "$conclusion" "$subject" "$neighbours" "$ratio" \
        "${rate:-none}" "${sha:0:9}" >> "$HISTORY"
    added=$((added + 1))
done < "$work/runs.tsv"

printf '\n%d added, %d already present, %d with no usable performance log\n' "$added" "$skipped" "$nodata"
printf 'History: %s\n' "$HISTORY"

if [ "$SUGGEST" = true ]; then
    echo
    echo "=== suggested baseline, from the MEDIAN of the healthy runs collected ==="
    # MASTER RUNS ONLY. The first version took the median over every collected run, and its first real
    # use proposed 43,552 rec/s - the REGRESSED figure from an unmerged branch that dominated the
    # sample. It would have lowered the baseline from 77,960 and disarmed the check: exactly the failure
    # the closing advice warns about, arriving on the first invocation. A warning is not a guard.
    #
    # A baseline may only come from a tree that shipped, so PR branches are excluded and the branch
    # column is the filter. Until .github/workflows/perf-baseline.yml has run on master nothing is
    # eligible, and saying so is the right answer rather than falling back to PR data.
    median_id=$(awk -F'\t' '!/^#/ && $8 != "none" && $3 == "master" { print $8"\t"$1 }' "$HISTORY" \
                | sort -g | awk '{ a[NR] = $0 } END { if (NR) print a[int((NR + 1) / 2)] }' | cut -f2)
    if [ -z "$median_id" ]; then
        echo "No MASTER run in $HISTORY carries a rate." >&2
        echo "  A baseline may only come from a tree that shipped. PR-branch runs are excluded" >&2
        echo "  deliberately: an unmerged regression dominating the sample would otherwise be" >&2
        echo "  proposed as the new normal, lowering the bar and disarming the check." >&2
        exit 3
    fi
    echo "Median run: $median_id"
    rm -rf "$work/sug"; mkdir -p "$work/sug"
    if ! gh api "repos/$REPO/actions/runs/$median_id/logs" > "$work/sug.zip" 2>/dev/null; then
        echo "Its logs have expired - pick another run by hand from $HISTORY." >&2
        exit 2
    fi
    unzip -qq -o "$work/sug.zip" -d "$work/sug" 2>/dev/null
    slog=$(find "$work/sug" -iname '*Performance Tests*.txt' -size +1k 2>/dev/null | head -1)
    rate=$(grep -ohE "PC-THROUGHPUT test=$SUBJECT .*recordsPerSecond=[0-9-]+" "$slog" \
           | sed 's/.*recordsPerSecond=//; s/ .*//' | tail -1)
    echo
    echo "# Every row below is from run $median_id - do not mix runs."
    printf 'rate\t%s\t%s\trecords-per-second\n' "$SUBJECT" "$rate"
    for n in $NEIGHBOURS; do
        secs=$(grep -ohE "Time elapsed: [0-9.]+ s[^-]*-- in [A-Za-z0-9_.]*$n" "$slog" \
               | sed 's/Time elapsed: //; s/ s.*//' | head -1)
        [ -n "$secs" ] && printf 'class-seconds\t%s\t%s\tseconds\n' "$n" "$secs"
    done
    echo
    current=$(awk -F'\t' '$1 == "rate" { print $3; exit }' "$ROOT_BASELINE" 2>/dev/null)
    if [ -n "${current:-}" ] && [ -n "${rate:-}" ] \
       && [ "$(awk -v a="$rate" -v b="$current" 'BEGIN { print (a < b) }')" = "1" ]; then
        echo "REFUSING TO RECOMMEND: $rate is BELOW the current baseline of $current."
        echo "  Raising a baseline is routine. Lowering one is a claim that the product got slower and"
        echo "  that this is now acceptable, which is never a scripted decision. If it is genuinely"
        echo "  right, edit docs/perf-baseline.tsv by hand and say in the commit what got slower."
        exit 1
    fi
    echo "Paste into docs/perf-baseline.tsv ONLY if the product genuinely got faster."
    echo "A regression that has become the new normal produces exactly the same output."
fi
