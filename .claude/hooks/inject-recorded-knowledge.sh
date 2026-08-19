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
# docs/inflight/AGENTS.md owns the tag vocabulary and is named by bin/check-inflight-tags.sh when it
# fails, so the sets here and there must never disagree.
inflight_title() { # <file> -> its heading, else its slug
    local t
    t=$(sed -n 's/^# //p' "$1" 2>/dev/null | head -1)
    [ -n "$t" ] || t=$(basename "$1" .md)
    printf '%s' "$t"
}

# bug first (defects in shipped code), tooling next (fires on everyone), then tests, then
# cross-branch state, then the long tail, then candidates, and parked last.
# THREE FIELDS, because one was doing three jobs. `inflight-type` says what KIND of item it is
# (bug/feature/task - this directory is a file-backed issue tracker, so use a tracker's vocabulary);
# `inflight-impact` says what it COSTS you to not know, which only bugs and tasks carry; and
# `inflight-state` is disposition, ABSENT meaning open. Collapsing those into one field produced
# classes like "candidate" and "decided-no", which are states wearing a class's clothes.
#
# ORDER IS NOT SEVERITY. Signal integrity first: you cannot judge the code through instruments that
# lie, so `misdirection` outranks `blind-spot` and both outrank any product defect. Bugs before
# tasks before features - what is broken before what is owed before what is wanted.
emit "**Open work** (\`docs/inflight/\`, one file per item) - what it costs you to not know:"
emit ""

emitted=""
# REQUIRE THE WHOLE MARKER, not the bare substring. A note that merely MENTIONS
# `inflight-state:` in its prose - one quoting this convention, or the gate's own output - was
# read as CLOSED and rolled into the "not shown" count. The note then appeared only as a
# mislabelled number telling you to delete it, which is exactly the omission this index claims
# its filters cannot make.
is_open()    { ! grep -q 'inflight-state:[^>]*-->' "$1" 2>/dev/null; }

emit_group() { # <type> <impact-or-empty> <heading>
    local files hits=""
    files=$(grep -rl "inflight-type:[[:space:]]*$1[[:space:]]*-->" docs/inflight --include='*.md' 2>/dev/null \
              | grep -v 'AGENTS.md' | sort)
    while IFS= read -r f; do
        [ -n "$f" ] || continue
        is_open "$f" || continue
        if [ -n "$2" ]; then grep -q "inflight-impact:[[:space:]]*$2[[:space:]]*-->" "$f" || continue; fi
        hits="${hits}- $(inflight_title "$f")"$'\n'
        emitted="${emitted}${f}"$'\n'
    done <<< "$files"
    [ -n "$hits" ] || return
    emit "**$3**"
    printf '%s' "$hits"
    emit ""
}

for imp in misdirection blind-spot data-loss stall security config-lie throughput; do
    emit_group bug "$imp" "bug / ${imp}"
done
for imp in release-gate coordination stranded-work deps-debt; do
    emit_group task "$imp" "task / ${imp}"
done
emit_group feature "" "feature - proposed, direction not chosen"

# ANY OPEN NOTE THAT MATCHED NO GROUP IS LISTED, LOUDLY. The loops above iterate the values this repo
# knows about, so a typo'd or invented inflight-type/inflight-impact matches nothing and the note
# would vanish from the index - silently, and precisely because the filter worked. Same failure as
# the exclusion count below, one level in: the grouping itself can hide things. Listed by name rather
# than counted, because an unmatched note is a bug in its tags and the fix needs to know which file.
unmatched=""
while IFS= read -r f; do
    [ -n "$f" ] || continue
    grep -q 'inflight-state:' "$f" 2>/dev/null && continue
    grep -qxF "$f" <<<"$emitted" && continue
    unmatched="${unmatched}- $(inflight_title "$f")  \`${f}\`"$'\n'
done <<< "$(find docs/inflight -maxdepth 1 -name '*.md' -type f 2>/dev/null | grep -vE '(AGENTS|CLAUDE)\.md' | sort)"
if [ -n "$unmatched" ]; then
    emit "**unmatched** - open, but no group claimed them. Their \`inflight-type\` or \`inflight-impact\` is missing or misspelt:"
    printf '%s' "$unmatched"
    emit ""
fi

# THE FILTER MUST ADMIT WHAT IT HID. Notes carrying a state are excluded - a closed item presented as
# open is misdirection by this repo's own taxonomy - but a view that silently shrinks is
# indistinguishable from one with nothing to hide. One line of count keeps the undone-cleanup backlog
# visible without letting it occupy the index. Same rule as "no silent caps" for a bounded workflow.
stated=$(grep -rl 'inflight-state:[^>]*-->' docs/inflight --include='*.md' 2>/dev/null | grep -v 'AGENTS.md' | grep -c . || true)
[ "${stated:-0}" -gt 0 ] && emit "_${stated} note(s) not shown: closed, parked or blocked. Delete or migrate them - \`grep -rln inflight-state: docs/inflight\`._" && emit ""

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
