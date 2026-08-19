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

# INFLIGHT, GROUPED BY WHAT IT COSTS YOU TO NOT KNOW - not alphabetically, and not by filename.
#
# Three decisions are baked in here, all of them about signal rather than budget (measured: every
# path under docs/ is ~2.8k tokens, less than the solutions block above, so nothing here is a
# space saving).
#
#   1. TITLES, NOT SLUGS. Sampled across the whole corpus, 66 of 70 titles carry information the
#      filename does not - and crucially it is STATUS and NEXT ACTION, which a filename structurally
#      cannot hold: "done, no PR", "one result worth keeping, then delete", "may have a contaminated
#      control arm". Knowing a note exists is not the same as knowing whether it is still live. The
#      filename is dropped; grep the title when you want the file.
#
#   2. GROUPED BY CATEGORY, because the prefix IS the type field - docs/inflight/AGENTS.md says the
#      prefix is the point, so listing titles alone would throw that away. The heading restores it.
#
#   3. ORDERED BY COST OF NOT KNOWING, high priority first within each group. A known product defect
#      outranks a candidate idea; parked work is last because it is deferred BY DECISION. An
#      alphabetical list puts `branch-` above `bug-`, which is exactly backwards.
#
# A note marked `<!-- inflight-priority: high - why -->` is lifted to the top of its group with its
# reason. docs/inflight/AGENTS.md owns what qualifies and the anti-inflation rule that keeps this
# readable; a marker on everything is a marker on nothing.
inflight_title() { # <file> -> its heading, else its slug
    local t
    t=$(sed -n 's/^# //p' "$1" 2>/dev/null | head -1)
    [ -n "$t" ] || t=$(basename "$1" .md)
    printf '%s' "$t"
}

# The high-priority notes are ALSO lifted into a block of their own, above the groups. They stay
# marked in place with `!!` so a reader who scans by category still sees which ones they are, but a
# high-priority note sitting in a low-priority group (a `parked-` item that still binds everyone) is
# exactly the one a category-ordered list buries. The reason is printed once, here.
priority=$(grep -rl 'inflight-priority:[[:space:]]*high' docs/inflight --include='*.md' 2>/dev/null \
             | grep -v 'AGENTS.md' | sort)
if [ -n "$priority" ]; then
    emit "**Read these first** - they bind work that looks unrelated:"
    while IFS= read -r f; do
        why=$(sed -n 's/.*inflight-priority:[[:space:]]*high[[:space:]]*-[[:space:]]*//p' "$f" \
                | head -1 | sed 's/[[:space:]]*-->.*//')
        emit "- $(inflight_title "$f") - ${why:-no reason given}  \`${f}\`"
    done <<< "$priority"
    emit ""
fi

emit "**Open work** (\`docs/inflight/\`, one file per item) - grouped by what it costs you to not know:"
emit ""

# bug first (defects in shipped code), tooling next (fires on everyone), then tests, then
# cross-branch state, then the long tail, then candidates, and parked last.
for cat in bug ci test branch pr release deps perf static web next parked; do
    files=$(find docs/inflight -maxdepth 1 -name "${cat}-*.md" -type f 2>/dev/null | sort)
    [ -n "$files" ] || continue
    n=$(printf '%s\n' "$files" | grep -c .)
    emit "**${cat}** (${n})"
    # Within a group: high, then medium, then low. Group order already ranks CATEGORIES by what it
    # costs you to not know; this ranks the members against each other, which is the only place a
    # per-note level earns anything - 21 `next-` candidates in filename order tell you nothing about
    # which to read. An unmarked note sorts with `low`, so forgetting the marker is quiet rather
    # than promoting the note.
    for level in high medium low; do
        while IFS= read -r f; do
            noted=$(sed -n 's/.*inflight-priority:[[:space:]]*\([a-z]*\).*/\1/p' "$f" 2>/dev/null | head -1)
            [ -n "$noted" ] || noted=low
            [ "$noted" = "$level" ] || continue
            case "$level" in
                high) emit "- !! $(inflight_title "$f")" ;;
                *)    emit "- $(inflight_title "$f")" ;;
            esac
        done <<< "$files"
    done
    emit ""
done

emit "**Dated plans and investigations** (\`docs/plans/\`) - the method that settled a question of this shape before:"
plans=$(find docs/plans -name '*.md' -type f 2>/dev/null | sort | sed 's|docs/plans/||;s|\.md$||' | paste -sd, | sed 's/,/, /g')
emit "${plans:-(none)}"
emit ""

# Point-in-time audits of tests that do not run, do not assert, or were never written. Easy to miss
# precisely because nothing goes red to tell you - which is why AGENTS.md says to read the newest
# before re-enabling, deleting or rewriting a dark test.
hardening=$(find docs/test-hardening -name '*.md' -type f 2>/dev/null | sort | sed 's|docs/test-hardening/||;s|\.md$||' | paste -sd, | sed 's/,/, /g')
if [ -n "$hardening" ]; then
    emit "**Dated test-hardening audits** (\`docs/test-hardening/\`) - a test that never runs is not a passing test:"
    emit "${hardening}"
fi
