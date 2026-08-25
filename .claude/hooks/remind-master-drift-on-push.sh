#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# PUSH-TIME report of what master has gained that this branch does not have - the commits, and
# whether any of them touch files this branch also changes.
#
# WHAT IT IS FOR, and it is deliberately not "merge master". `AGENTS.md`, "Read the commits you
# inherit", says a commit body can carry an instruction addressed to your branch, a decision that
# reshapes your work, or an argument against what you are about to do - and that a clean merge is
# evidence of none of them. The decision the agent actually faces is whether anything RELEVANT has
# landed: merge now if it has, batch it with the next few if it has not. That decision needs the
# subjects, not a divergence count, and nothing was putting the subjects in front of anyone.
#
# `docs/inflight/AGENTS.md` is why this is a hook and not a note: "never write down what a command
# can answer - branch divergence is `git rev-list --left-right --count`". Correct, and it is why
# the fact was never recorded anywhere; the gap was that nobody runs the command. So the hook runs
# it, at a moment it is worth reading.
#
# WHY PUSH. Same reasoning as remind-inflight-on-push.sh, whose header owns it: push is frequent
# enough to catch drift and rare enough not to be noise, and the agent is still IN the work, so the
# answer can still change what gets built. Per-commit would bury it; at merge prep it is too late to
# be cheap.
#
# NON-BLOCKING, and it must stay that way - `additionalContext`, never a `deny`. It reports a
# situation, not a violation, and there is no wrong answer to "should I merge master now".
#
# THROTTLED ON master's SHA, NOT ON A CLOCK. New commits are new information and stale ones are not,
# so the same master tip is reported once per branch however many times you push, and a master that
# moves reports again immediately. The stamp file's mtime carries a separate, cruder job: a floor on
# how often the network fetch below runs, so a push loop cannot turn this into a fetch loop.
#
# IT FETCHES, which is the one cost here worth stating. Reading a stale `origin/master` would
# under-report - the failure mode this exists to prevent - so it refreshes the ref, with
# GIT_TERMINAL_PROMPT=0 so a credential prompt can never hang the tool call. A fetch that fails for
# any other reason is ignored and whatever ref exists is used, because a degraded report beats none.
set -uo pipefail

payload="$(cat 2>/dev/null || true)"
[ -n "$payload" ] || exit 0

# Cheap bail before paying for python3 on EVERY Bash call.
case "$payload" in
    *push*) ;;
    *) exit 0 ;;
esac

# Resolved before it is sourced, and silent if it is missing - see the same bootstrap in
# remind-inflight-on-push.sh, and bin/lib/node-gate.sh's header for why the order matters.
hook_lib="${BASH_SOURCE[0]%/*}/lib/hook-common.sh"
[ -r "$hook_lib" ] || exit 0
# shellcheck source=.claude/hooks/lib/hook-common.sh
. "$hook_lib"

[ "$(hook_git_subcommand "$payload")" = "push" ] || exit 0

root="$(git rev-parse --show-toplevel 2>/dev/null || true)"
[ -n "$root" ] || exit 0
cd "$root" 2>/dev/null || exit 0

branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
[ -n "$branch" ] && [ "$branch" != "HEAD" ] || exit 0

base_ref="${MASTER_DRIFT_REF:-origin/master}"
# Pushing master itself has nothing to inherit. Compared against the ref BOTH ways, so a local
# `master` and a `MASTER_DRIFT_REF` of `origin/master` both match.
[ "$branch" != "$base_ref" ] && [ "$branch" != "${base_ref##*/}" ] || exit 0

stamp="${TMPDIR:-/tmp}/pc-master-drift-$(printf '%s' "$branch" | tr '/' '_')"

# FETCH FLOOR. Reported-SHA throttling cannot bound the network cost, because the SHA is only known
# after the fetch. This is the only thing the clock is used for.
now="$(date +%s)"
last="$(hook_file_mtime "$stamp")"
case "$last" in ''|*[!0-9]*) last=0 ;; esac
[ $(( now - last )) -lt "${MASTER_DRIFT_FETCH_FLOOR_SECONDS:-300}" ] && exit 0

case "$base_ref" in
    */*) GIT_TERMINAL_PROMPT=0 git fetch --quiet "${base_ref%%/*}" "${base_ref#*/}" 2>/dev/null || true ;;
esac

git rev-parse --verify --quiet "$base_ref" >/dev/null 2>&1 || exit 0
base_sha="$(git rev-parse "$base_ref" 2>/dev/null || true)"
[ -n "$base_sha" ] || exit 0

# Read the previously reported tip, then record the current one IMMEDIATELY - before any decision
# below - so both the SHA throttle and the fetch floor advance whether or not this run reports.
prev=""
[ -f "$stamp" ] && prev="$(head -n1 "$stamp" 2>/dev/null || true)"
printf '%s\n' "$base_sha" > "$stamp" 2>/dev/null || true

behind="$(git rev-list --count "HEAD..$base_sha" 2>/dev/null || echo 0)"
case "$behind" in ''|*[!0-9]*) behind=0 ;; esac
[ "$behind" -gt 0 ] || exit 0
[ "$prev" != "$base_sha" ] || exit 0

merge_base="$(git merge-base HEAD "$base_sha" 2>/dev/null || true)"
[ -n "$merge_base" ] || exit 0

# CAPPED, AND THE CAP IS STATED. A silent truncation reads as a complete list, which is the failure
# this repo has already published a wrong count from.
commit_cap="${MASTER_DRIFT_COMMIT_CAP:-25}"
commits="$(git log --format='  %h  %s' -n "$commit_cap" "HEAD..$base_sha" 2>/dev/null || true)"
commits_omitted=$(( behind > commit_cap ? behind - commit_cap : 0 ))

# OVERLAP is the question the report exists to answer: has master touched anything this branch
# touches? `git diff <merge-base>` with no second commit reads the WORKING TREE, so uncommitted work
# counts as this branch's - a file you are editing right now is exactly the one you want to know
# about.
mine="$(git diff --name-only "$merge_base" 2>/dev/null | sort -u)"
theirs="$(git diff --name-only "$merge_base" "$base_sha" 2>/dev/null | sort -u)"
overlap="$(comm -12 <(printf '%s\n' "$mine") <(printf '%s\n' "$theirs") 2>/dev/null | sed '/^$/d')"

overlap_count=0
[ -n "$overlap" ] && overlap_count="$(printf '%s\n' "$overlap" | wc -l | tr -d ' ')"
overlap_cap="${MASTER_DRIFT_OVERLAP_CAP:-15}"
overlap_report=""
if [ -n "$overlap" ]; then
    n=0
    while IFS= read -r f; do
        [ -n "$f" ] || continue
        n=$(( n + 1 ))
        [ "$n" -le "$overlap_cap" ] || break
        # Which master commit last touched it - the subject is what tells you whether it matters.
        touched="$(git log -1 --format='%h %s' "$merge_base..$base_sha" -- "$f" 2>/dev/null || true)"
        overlap_report="${overlap_report}  ${f}
      last touched by ${touched}
"
    done <<EOF
$overlap
EOF
    if [ "$overlap_count" -gt "$overlap_cap" ]; then
        overlap_report="${overlap_report}  ... and $(( overlap_count - overlap_cap )) more overlapping file(s), not listed
"
    fi
fi

base_age="$(git log -1 --format=%cr "$merge_base" 2>/dev/null || true)"

# DERIVED, NOT COPIED. AGENTS.md carries a live `## IN FLIGHT:` section when something must happen
# BEFORE any branch merges master - today, the package rename. Restating that rule here would be a
# second copy that keeps asserting itself after the section is deleted; echoing the heading back
# disappears on its own when the section does, and says nothing this hook has to keep true.
inflight_heading="$(grep -m1 '^## IN FLIGHT:' AGENTS.md 2>/dev/null || true)"

export DRIFT_BRANCH="$branch" DRIFT_REF="$base_ref" DRIFT_BEHIND="$behind" \
       DRIFT_COMMITS="$commits" DRIFT_OMITTED="$commits_omitted" \
       DRIFT_OVERLAP="$overlap_report" DRIFT_OVERLAP_COUNT="$overlap_count" \
       DRIFT_AGE="$base_age" DRIFT_INFLIGHT="$inflight_heading"
python3 -c '
import json, os

behind = os.environ["DRIFT_BEHIND"]
ref = os.environ["DRIFT_REF"]
branch = os.environ["DRIFT_BRANCH"]
age = os.environ["DRIFT_AGE"]
overlap_count = int(os.environ["DRIFT_OVERLAP_COUNT"] or 0)
omitted = int(os.environ["DRIFT_OMITTED"] or 0)

parts = []
parts.append(
    behind + " commit(s) on " + ref + " are not in " + branch + ". This branch is based on "
    + ref + " as it stood " + (age or "some time ago") + ".")
parts.append("")
parts.append(os.environ["DRIFT_COMMITS"])
if omitted:
    parts.append("  ... and " + str(omitted) + " older commit(s), not listed.")
parts.append("")

if overlap_count:
    parts.append(
        str(overlap_count) + " file(s) are changed on BOTH sides - master has moved under work this "
        "branch is doing:")
    parts.append("")
    parts.append(os.environ["DRIFT_OVERLAP"].rstrip("\n"))
    parts.append("")
    parts.append(
        "That is the case for merging now rather than later: the conflict is already there, it is "
        "smallest today, and a change to a file you are editing is the most likely place an "
        "inherited decision contradicts yours.")
else:
    parts.append(
        "None of them touch a file this branch changes, so nothing here forces a merge today - "
        "batching several master merges is a reasonable choice. File overlap is not the only kind "
        "of relevance, though: read the subjects.")

parts.append("")
parts.append(
    "THIS IS A REPORT, NOT AN INSTRUCTION. Merging master is not free and there is no wrong answer; "
    "what is wrong is deciding without looking. AGENTS.md, “Read the commits you inherit”, "
    "is the method: read the BODIES of anything touching your area, because an instruction addressed "
    "to your branch, a decision that reshapes your work, and an argument against it all hide there "
    "and none of them announce themselves. A clean merge is evidence of none of it.")

inflight = os.environ["DRIFT_INFLIGHT"].strip()
if inflight:
    parts.append("")
    parts.append(
        "AGENTS.md currently carries a section that binds any branch merging master - "
        + inflight.lstrip("# ").strip() + " - re-read it BEFORE the merge, not after.")

print(json.dumps({"hookSpecificOutput": {"hookEventName": "PreToolUse",
    "additionalContext": "\n".join(parts)}}))
' 2>/dev/null || true
exit 0
