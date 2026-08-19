#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# SessionStart hook: put the NAMES of what this repo has already solved into the agent's context,
# before it starts work.
#
# WHY THIS EXISTS
#
# AGENTS.md already tells every agent to grep `docs/solutions/`, `docs/plans/` and `docs/inflight/`
# before forming a hypothesis, and it is the check most often skipped - because skipping it is
# invisible. An agent that never learns a document exists cannot miss it; it just rediscovers the
# problem, and its work looks like progress the whole way.
#
# Worked case, and the reason this hook was written: astubbs/parallel-consumer#320 spent three
# rounds designing a fix for the duplication scanners' scope. The diagnosis and the prescribed fix
# had been written down a week earlier in
# `docs/solutions/workflow-issues/duplication-scanners-do-not-look-where-agents-duplicate-2026-08-12.md`,
# which names both CI jobs and the exact config line. One grep would have found it. The rule was
# read and not run.
#
# A RULE IS NOT A MECHANISM. `docs/agent-harness.md` makes the distinction this hook turns on:
# instructions in a file fire only if someone opens the file, while a hook fires whether or not
# anyone remembers it exists. Listing the titles is deliberately cheap - roughly one line per
# document, no bodies - because the failure being fixed is not knowing the document EXISTS. Once a
# title is in context, the agent's own grep does the rest.
#
# Tool-neutral where it can be: the knowledge lives in the documents, and this only enumerates them.
# Codex and anything else reading AGENTS.md gets the same rule, just without the reminder.
#
# Never fails a session: any error prints nothing and exits 0. A broken reminder must not be a
# broken session.

set -uo pipefail

root="${CLAUDE_PROJECT_DIR:-$(git rev-parse --show-toplevel 2>/dev/null)}" || exit 0
[ -n "$root" ] && [ -d "$root/docs" ] || exit 0
cd "$root" 2>/dev/null || exit 0

emit() { printf '%s\n' "$1"; }

solutions=$(find docs/solutions -name '*.md' -type f 2>/dev/null | sort)
[ -n "$solutions" ] || exit 0

emit "## Already solved here - read before you diagnose"
emit ""
emit "AGENTS.md asks you to check prior art before forming a hypothesis. These are the titles, so"
emit "you know what exists; grep the ones that look close. Skipping this is the check most often"
emit "skipped, and its failure is silent - you rediscover the problem and it feels like progress."
emit ""

current=""
while IFS= read -r f; do
    category=$(basename "$(dirname "$f")")
    if [ "$category" != "$current" ]; then
        emit ""
        emit "**${category}**"
        current="$category"
    fi
    # The frontmatter title if there is one, else the first heading, else the filename.
    # A frontmatter title is YAML, so it is quoted whenever it contains a colon or an apostrophe -
    # and this repo's titles routinely do. Strip the wrapping, or the index shows the quotes.
    title=$(sed -n 's/^title:[[:space:]]*//p' "$f" 2>/dev/null | head -1 \
              | sed 's/^"//;s/"$//;s/^'"'"'//;s/'"'"'$//')
    [ -n "$title" ] || title=$(sed -n 's/^# //p' "$f" 2>/dev/null | head -1)
    [ -n "$title" ] || title=$(basename "$f" .md)
    emit "- ${title}  \`${f}\`"
done <<< "$solutions"

emit ""

# A note carrying `<!-- inflight-priority: high - why -->` is lifted out of the flat list and shown
# with its reason. The flat list is a shape; these are things you are likely to COLLIDE with without
# knowing they exist - a live flake ledger being the case that prompted it, met twice on branches
# containing no Java at all. docs/inflight/AGENTS.md owns what qualifies, and the anti-inflation
# rule that keeps this block short enough to read.
priority=$(grep -rl 'inflight-priority:[[:space:]]*high' docs/inflight 2>/dev/null | sort)
if [ -n "$priority" ]; then
    emit "**Read these first:**"
    while IFS= read -r f; do
        why=$(sed -n 's/.*inflight-priority:[[:space:]]*high[[:space:]]*-[[:space:]]*//p' "$f" \
                | head -1 | sed 's/[[:space:]]*-->.*//')
        emit "- \`${f}\` - ${why:-no reason given}"
    done <<< "$priority"
    emit ""
fi

emit "**Everything else open** (\`docs/inflight/\`, one file per item):"
inflight=$(find docs/inflight -name '*.md' -not -name 'AGENTS.md' -not -name 'CLAUDE.md' \
             -type f 2>/dev/null | sort | sed 's|docs/inflight/||;s|\.md$||' | paste -sd', ')
emit "${inflight:-(none)}"
emit ""
emit "**Dated plans and investigations** (\`docs/plans/\`): $(find docs/plans -name '*.md' -type f 2>/dev/null | wc -l | tr -d ' ') documents - \`ls docs/plans/\` before investigating anything with a history."
