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
# add the value to bin/lib/inflight-tags.sh and to docs/inflight/AGENTS.md in the same commit, and
# say why - do not invent one in a note and hope.
#
# WHAT IT DELIBERATELY DOES NOT CHECK: whether the chosen impact is the RIGHT one. That is a
# judgement about consequence, and no script can make it - the six corrections made when this scheme
# was introduced were all valid values applied to the wrong note.

set -uo pipefail

# The sets live in bin/lib/inflight-tags.sh, shared with the session index
# (.claude/hooks/inject-recorded-knowledge.sh) so the gate can never accept a tag the index cannot
# place. The lib also explains why the bug/task partition exists. Resolve it from this script's own
# location BEFORE the cd below - the self-test runs this gate inside a fixture repo.
# shellcheck source=lib/inflight-tags.sh
. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/inflight-tags.sh" || exit 1

cd "$(git rev-parse --show-toplevel 2>/dev/null)" || exit 0
[ -d docs/inflight ] || exit 0

TYPES="$INFLIGHT_TYPES"
BUG_IMPACTS="$INFLIGHT_BUG_IMPACTS"
TASK_IMPACTS="$INFLIGHT_TASK_IMPACTS"
IMPACTS="$BUG_IMPACTS $TASK_IMPACTS"

problems=0
# NAME THE NOTE, NOT JUST THE PATH. A filename is an identifier; the title is what a reader
# recognises - and after the rename pass, paths are the part most likely to have just changed under
# whoever is reading the failure.
note_title() { sed -n 's/^# //p' "$1" 2>/dev/null | head -1; }

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

    # A SECOND copy of any tag is a merge artefact, and it is invisible to every other check here:
    # each one reads the first match, so a note can carry two contradictory states and be reported
    # valid. Introduced exactly that way - merging a branch whose notes predated a state reword
    # appended the stale block underneath the corrected one rather than replacing it, and the gate
    # said "83 note(s) valid" over a note that was both `deferred - parked` and `parked - deferred`.
    for tag in type impact state; do
        # No `|| echo 0`: grep -c PRINTS 0 and exits 1 on no match, so the fallback appends a
        # second line and the numeric test below errors on "0\n0".
        n=$(grep -c "<!-- inflight-$tag:" "$f" 2>/dev/null); n=${n:-0}
        if [ "${n:-0}" -gt 1 ]; then
            note "$f \"$(note_title "$f")\": $n inflight-$tag markers, expected one. Every check here reads the first, so the others are silently ignored - usually a merge that appended a stale tag block instead of replacing it"
        fi
    done

    # THE CLOSING --> IS REQUIRED, because the index's grep requires it. A bare `inflight-type: bug`
    # with no comment wrapper used to pass here and then land in the index's "unmatched" list -
    # defeating the whole point of a gate, which is to fail the commit rather than the next session.
    type=$(sed -n 's/.*inflight-type:[[:space:]]*\([a-z-]*\)[[:space:]]*-->.*/\1/p' "$f" | head -1)
    impact=$(sed -n 's/.*inflight-impact:[[:space:]]*\([a-z-]*\)[[:space:]]*-->.*/\1/p' "$f" | head -1)
    # [^>]* HERE IS THE CONTRACT WITH THE INDEX, whose is_open() greps 'inflight-state:[^>]*-->'.
    # A greedy .* parsed a reason containing '>' that the index cannot cross, so the same note was
    # gate-green yet listed as OPEN at every session start (astubbs#324 review). The strict
    # extraction plus the explicit rejection below keep the two readers in agreement.
    state=$(sed -n 's/.*inflight-state:[[:space:]]*\([^>]*\)-->.*/\1/p' "$f" | head -1 | sed 's/[[:space:]]*$//')

    if [ -z "$state" ] && grep -q 'inflight-state:.*-->' "$f"; then
        note "$f \"$(note_title "$f")\": inflight-state reason contains '>' - the session index (is_open in .claude/hooks/inject-recorded-knowledge.sh) cannot parse that marker and would list the note as OPEN. Reword without '>'"
    fi

    if [ -z "$type" ]; then
        note "$f \"$(note_title "$f")\": no inflight-type. One of: $TYPES"
    elif ! in_set "$type" "$TYPES"; then
        note "$f \"$(note_title "$f")\": inflight-type '$type' is not one of: $TYPES"
    fi

    if [ -n "$impact" ] && ! in_set "$impact" "$IMPACTS"; then
        note "$f \"$(note_title "$f")\": inflight-impact '$impact' is not one of: $IMPACTS"
    elif [ -n "$impact" ] && [ "$type" = "bug" ] && ! in_set "$impact" "$BUG_IMPACTS"; then
        note "$f \"$(note_title "$f")\": impact '$impact' is a task impact, not a bug one. bug takes: $BUG_IMPACTS"
    elif [ -n "$impact" ] && [ "$type" = "task" ] && ! in_set "$impact" "$TASK_IMPACTS"; then
        note "$f \"$(note_title "$f")\": impact '$impact' is a bug impact, not a task one. task takes: $TASK_IMPACTS"
    fi

    # A bug or a task with no impact says what it IS without saying what it COSTS, which is the
    # question the index is ordered by - it would be filed but unrankable. A feature MAY carry one and
    # should whenever it addresses a consequence, so that it sorts beside that consequence rather than
    # among cosmetic features; it is optional because a new capability with no problem behind it has an
    # opportunity rather than a cost.
    if [ -z "$impact" ] && { [ "$type" = "bug" ] || [ "$type" = "task" ]; }; then
        note "$f \"$(note_title "$f")\": type '$type' needs an inflight-impact (what it costs someone to not know)"
    fi
    if [ -n "$impact" ] && [ "$type" = "register" ] && ! in_set "$impact" "$INFLIGHT_REGISTER_IMPACTS"; then
        note "$f \"$(note_title "$f")\": inflight-impact '$impact' is not a known impact. register takes any of: $INFLIGHT_REGISTER_IMPACTS"
    fi
    if [ -n "$impact" ] && [ "$type" = "feature" ] && ! in_set "$impact" "$INFLIGHT_FEATURE_IMPACTS"; then
        note "$f \"$(note_title "$f")\": inflight-impact '$impact' is not a known impact. feature takes any of: $INFLIGHT_FEATURE_IMPACTS"
    fi

    # A state must say WHY, or a reader cannot tell a decision from an abandonment.
    if [ -n "$state" ] && ! grep -q ' - ' <<<"$state"; then
        note "$f \"$(note_title "$f")\": inflight-state '$state' has no reason. Use '<state> - <why>'"
    fi
done

# THE DOC AND THE LIB MUST LIST THE SAME VALUES, and until now nothing checked it. The sets live in
# bin/lib/inflight-tags.sh and are DESCRIBED in docs/inflight/AGENTS.md, which this script names as
# their owner five times over - so the two were stated twice with the agreement verified only by
# whoever happened to look. That is the shape this repo treats as a defect everywhere else: a rule
# documented rather than enforced drifts, and the drift is silent because both halves still parse.
#
# Checked in BOTH directions. A value in the lib the doc never explains is undocumented vocabulary
# nobody can use correctly; a value in the doc the lib rejects is an instruction that fails the gate.
OWNER_DOC="docs/inflight/AGENTS.md"
if [ -r "$OWNER_DOC" ]; then
    for v in $INFLIGHT_TYPES $INFLIGHT_BUG_IMPACTS $INFLIGHT_TASK_IMPACTS; do
        grep -q "\`${v}\`" "$OWNER_DOC" || note "vocabulary '$v' is in bin/lib/inflight-tags.sh but never described in $OWNER_DOC"
    done
    # every impact the doc's table declares must be one the gate accepts
    while IFS= read -r v; do
        [ -n "$v" ] || continue
        in_set "$v" "$INFLIGHT_BUG_IMPACTS $INFLIGHT_TASK_IMPACTS" \
            || note "$OWNER_DOC documents impact '$v', which bin/lib/inflight-tags.sh does not accept"
    done <<< "$(sed -n 's/^| `\([a-z-]\+\)` | \(bug\|task\|feature\|register\).*/\1/p' "$OWNER_DOC")"
fi

if [ "$problems" -gt 0 ]; then
    printf 'check-inflight-tags: %d problem(s). docs/inflight/AGENTS.md owns the sets.\n' "$problems" >&2
    exit 1
fi
printf 'check-inflight-tags: %s note(s) valid\n' "$(ls docs/inflight/*.md | grep -vcE '(AGENTS|CLAUDE)\.md')"
