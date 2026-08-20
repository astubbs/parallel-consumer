#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# ENUMERATES WHAT IS OUTSTANDING ON A PR. It never says "ready".
#
# WHY IT REFUSES TO CONCLUDE. An agent told the operator a PR was "MERGEABLE/CLEAN - waiting on your
# LGTM" while background work was still writing, no human had reviewed it, and the PR's OWN inflight
# note recorded an unresolved P1 that the same agent had written an hour earlier. `MERGEABLE/CLEAN`
# is a git fact - it means no merge conflicts - and it was reported as a verdict.
#
# A hook cannot catch that: `.claude/hooks/check-merge-outstanding-work.sh` fires on `gh pr merge`,
# which is far too late, and nothing intercepts a sentence. So instead of guarding the claim, this
# gives it a testable referent - run it and read the list, rather than forming an opinion.
#
# NOT FINDING A BLOCKER IS NOT READINESS, and this script must never imply otherwise. A reviewer
# part-way through a diff, a stale note, an agent about to push - none of those are visible here.
# Readiness is the operator's call; this only makes the measurable part measurable.
set -uo pipefail

pr="${1:-}"
if [ -z "$pr" ]; then
    branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
    pr="$(gh pr list --head "$branch" --json number --jq '.[0].number' 2>/dev/null || true)"
fi
[ -n "$pr" ] || { echo "usage: $(basename "$0") <pr-number>   (or run from a branch with an open PR)"; exit 2; }

blockers=0
say() { printf '  %s\n' "$1"; }
block() { printf '  BLOCKED  %s\n' "$1"; blockers=$((blockers + 1)); }

echo "astubbs/parallel-consumer#${pr} - what is outstanding"
echo

json="$(gh pr view "$pr" --json title,mergeable,mergeStateStatus,reviewDecision,isDraft,headRefName,statusCheckRollup 2>/dev/null || true)"
[ -n "$json" ] || { echo "  could not read the PR - gh unavailable or not authenticated"; exit 2; }

title=$(jq -r '.title' <<<"$json")
say "title: ${title}"

jq -e '.isDraft' <<<"$json" >/dev/null 2>&1 && block "it is a draft"

# A HUMAN LGTM. Automated review is not approval and neither is green CI - AGENTS.md is explicit.
decision=$(jq -r '.reviewDecision // ""' <<<"$json")
[ "$decision" = "APPROVED" ] || block "no human approval (reviewDecision: ${decision:-none}). Automated review is not approval."

# Git-mergeable is a GIT fact, reported as such and never as a verdict.
mergeable=$(jq -r '.mergeable' <<<"$json"); state=$(jq -r '.mergeStateStatus' <<<"$json")
[ "$mergeable" = "MERGEABLE" ] || block "git reports ${mergeable}/${state} - conflicts to resolve"
say "git mergeability: ${mergeable}/${state}   (a git fact: no conflicts. NOT a readiness verdict.)"

failing=$(jq -r '[.statusCheckRollup[]? | select(.conclusion=="FAILURE")] | length' <<<"$json")
[ "${failing:-0}" -gt 0 ] && block "${failing} check(s) failing"
pending=$(jq -r '[.statusCheckRollup[]? | select(.status=="IN_PROGRESS" or .status=="QUEUED")] | length' <<<"$json")
[ "${pending:-0}" -gt 0 ] && block "${pending} check(s) still running - the result is not known yet"

# THE PR'S OWN NOTE. Weak evidence on its own: a note is only as current as the last person to edit
# it, so its silence proves nothing. Its CONTENT, though, is a blocker whenever it has any.
# EVERY matching note, not the first. `pr-322-*` matches both the split plan and the outstanding
# note, and taking `head -1` read whichever sorted first - so the file actually describing what is
# open could be skipped entirely while the script reported confidently.
notes="$(find docs/inflight -maxdepth 1 -name "pr-${pr}-*.md" 2>/dev/null | sort)"
if [ -n "$notes" ]; then
    while IFS= read -r note; do
        [ -n "$note" ] || continue
        open_items="$(awk '/^## Already fixed/ {exit} /^- |^\*\*/ {n++} END {print n+0}' "$note")"
        if [ "${open_items:-0}" -gt 0 ]; then
            block "${note} lists ${open_items} item(s) above its 'Already fixed' heading"
        else
            say "${note} lists nothing outstanding - but a note is only as current as its last edit"
        fi
    done <<< "$notes"
else
    say "no docs/inflight/pr-${pr}-*.md note found - nothing to read, which is not the same as nothing outstanding"
fi

# BACKGROUND WORK IN THIS SESSION, the same window check-merge-outstanding-work.sh uses.
if [ -n "${CLAUDE_CODE_SESSION_ID:-}" ]; then
    now=$(date +%s); live=0
    while IFS= read -r f; do
        [ -f "$f" ] || continue
        m=$(stat -c %Y "$f" 2>/dev/null || echo 0)
        [ "$m" -eq 0 ] && continue
        [ $(( now - m )) -lt 120 ] && live=$((live + 1))
    done < <(find "/tmp/claude-$(id -u)" -maxdepth 4 -path "*/${CLAUDE_CODE_SESSION_ID}/tasks/*.output" 2>/dev/null || true)
    [ "$live" -gt 0 ] && block "${live} background task(s) wrote in the last 2 minutes - work is still in flight"
fi

echo
if [ "$blockers" -gt 0 ]; then
    echo "  ${blockers} blocker(s). Report these; do not call it ready."
    exit 1
fi
echo "  No blockers found in what this script can measure."
echo "  THAT IS NOT READINESS. A reviewer part-way through the diff, a note nobody updated, or an"
echo "  agent about to push are all invisible here. Readiness is the operator's call - ask."
exit 0
