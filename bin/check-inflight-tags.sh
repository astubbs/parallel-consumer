#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Validates the tags on every note in docs/inflight/ against the closed sets in
# docs/inflight/AGENTS.md, which owns their meanings.
#
# WHY A GATE AND NOT JUST THE INDEX. `.claude/hooks/inject-recorded-knowledge.sh` already lists any
# note that no group claimed, so a misspelt tag is visible - but only to whoever starts the NEXT
# session, which may not be the person who introduced it, and may be days later. A gate fails the
# commit that made the mistake, in front of the only person who can cheaply fix it. The index stays
# as the safety net for what this cannot see.
#
# THE SETS ARE CLOSED ON PURPOSE, the same reasoning docs/data/schema.yaml gives for the feature
# categories: an open set drifts into synonyms nobody can group by. If a note genuinely does not fit,
# add the value HERE and to docs/inflight/AGENTS.md in the same commit, and say why - do not invent
# one in a note and hope.
#
# WHAT IT DELIBERATELY DOES NOT CHECK: whether the chosen impact is the RIGHT one. That is a
# judgement about consequence, and no script can make it - the six corrections made when this scheme
# was introduced were all valid values applied to the wrong note.

set -uo pipefail

cd "$(git rev-parse --show-toplevel 2>/dev/null)" || exit 0
[ -d docs/inflight ] || exit 0

TYPES="bug feature task"
# Partitioned, not one flat set - the index groups bug-impacts and task-impacts separately, so a
# `bug` carrying `release-gate` passed this gate and then appeared under "unmatched" in the index.
# Two closed sets in two scripts with no shared source WILL drift; keeping the partition here is what
# makes the gate reject what the index cannot place.
BUG_IMPACTS="misdirection blind-spot data-loss stall security config-lie throughput"
TASK_IMPACTS="release-gate coordination stranded-work deps-debt"
IMPACTS="$BUG_IMPACTS $TASK_IMPACTS"

problems=0
note() { printf 'INFLIGHT: %s\n' "$1" >&2; problems=$((problems + 1)); }
in_set() { case " $2 " in *" $1 "*) return 0 ;; *) return 1 ;; esac; }

# A note in a subdirectory escapes EVERYTHING: this glob is non-recursive and the index's safety net
# uses -maxdepth 1, while its grouping grep recurses - so a mis-tagged note under docs/inflight/sub/
# is neither checked nor listed nor grouped. docs/inflight/AGENTS.md forbids subdirectories; this is
# the line that enforces it.
while IFS= read -r stray; do
    [ -n "$stray" ] && note "$stray: notes must be flat in docs/inflight/ - a subdirectory escapes both this gate and the session index"
done <<< "$(find docs/inflight -mindepth 2 -name '*.md' -type f 2>/dev/null)"

for f in docs/inflight/*.md; do
    base=$(basename "$f")
    case "$base" in AGENTS.md|CLAUDE.md) continue ;; esac

    # THE CLOSING --> IS REQUIRED, because the index's grep requires it. A bare `inflight-type: bug`
    # with no comment wrapper used to pass here and then land in the index's "unmatched" list -
    # defeating the whole point of a gate, which is to fail the commit rather than the next session.
    type=$(sed -n 's/.*inflight-type:[[:space:]]*\([a-z-]*\)[[:space:]]*-->.*/\1/p' "$f" | head -1)
    impact=$(sed -n 's/.*inflight-impact:[[:space:]]*\([a-z-]*\)[[:space:]]*-->.*/\1/p' "$f" | head -1)
    state=$(sed -n 's/.*inflight-state:[[:space:]]*\(.*\)-->.*/\1/p' "$f" | head -1 | sed 's/[[:space:]]*$//')

    if [ -z "$type" ]; then
        note "$f: no inflight-type. One of: $TYPES"
    elif ! in_set "$type" "$TYPES"; then
        note "$f: inflight-type '$type' is not one of: $TYPES"
    fi

    if [ -n "$impact" ] && ! in_set "$impact" "$IMPACTS"; then
        note "$f: inflight-impact '$impact' is not one of: $IMPACTS"
    elif [ -n "$impact" ] && [ "$type" = "bug" ] && ! in_set "$impact" "$BUG_IMPACTS"; then
        note "$f: impact '$impact' is a task impact, not a bug one. bug takes: $BUG_IMPACTS"
    elif [ -n "$impact" ] && [ "$type" = "task" ] && ! in_set "$impact" "$TASK_IMPACTS"; then
        note "$f: impact '$impact' is a bug impact, not a task one. task takes: $TASK_IMPACTS"
    fi

    # A bug or a task with no impact says what it IS without saying what it COSTS, which is the
    # question the index is ordered by - it would be filed but unrankable. A feature needs none:
    # proposed work has an opportunity, not a consequence.
    if [ -z "$impact" ] && { [ "$type" = "bug" ] || [ "$type" = "task" ]; }; then
        note "$f: type '$type' needs an inflight-impact (what it costs someone to not know)"
    fi
    if [ -n "$impact" ] && [ "$type" = "feature" ]; then
        note "$f: a feature carries no impact - it is proposed work, not a cost. Found '$impact'"
    fi

    # A state must say WHY, or a reader cannot tell a decision from an abandonment.
    if [ -n "$state" ] && ! grep -q ' - ' <<<"$state"; then
        note "$f: inflight-state '$state' has no reason. Use '<state> - <why>'"
    fi
done

if [ "$problems" -gt 0 ]; then
    printf 'check-inflight-tags: %d problem(s). docs/inflight/AGENTS.md owns the sets.\n' "$problems" >&2
    exit 1
fi
printf 'check-inflight-tags: %s note(s) valid\n' "$(ls docs/inflight/*.md | grep -vcE '(AGENTS|CLAUDE)\.md')"
