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

# RESOLVED BEFORE ANYTHING cd's. This script cd's to the project root further down, after which a
# BASH_SOURCE-relative path no longer resolves - so `bash .claude/hooks/inject-recorded-knowledge.sh`
# from the repo root silently sourced nothing and exited 0, which is the exact invocation this
# index prints for agents to re-run.
HOOK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"


root="${CLAUDE_PROJECT_DIR:-$(git rev-parse --show-toplevel 2>/dev/null)}" || exit 0
[ -n "$root" ] && [ -d "$root/docs" ] || exit 0
cd "$root" 2>/dev/null || exit 0

# ALSO WRITTEN TO A FILE, so an agent can grep the index instead of re-reading it whole. The headings
# are real markdown (`#` sections, `##` groups) precisely so `grep '^##'` gives the shape and
# `sed -n '/^## crash/,/^#/p'` gives one group. Re-running this script is equally valid and always
# current - the file is the cheap path, not the only one.
INFLIGHT_INDEX_FILE="${TMPDIR:-/tmp}/pc-session-index.md"
: > "$INFLIGHT_INDEX_FILE" 2>/dev/null || INFLIGHT_INDEX_FILE=""

emit() {
    printf '%s\n' "$1"
    [ -n "$INFLIGHT_INDEX_FILE" ] && printf '%s\n' "$1" >> "$INFLIGHT_INDEX_FILE"
    return 0
}

solutions=$(find docs/solutions -name '*.md' -type f 2>/dev/null | sort)
[ -n "$solutions" ] || exit 0

emit "# Already solved here - read before you diagnose"
emit ""
[ -n "$INFLIGHT_INDEX_FILE" ] && emit "_This whole index is also at \`$INFLIGHT_INDEX_FILE\` - grep it by heading (\`^## \`) rather than re-reading it. Re-run \`.claude/hooks/inject-recorded-knowledge.sh\` to refresh._"
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
        emit "## ${category}"
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
# docs/inflight/AGENTS.md owns what each tag MEANS; the machine-readable sets are sourced from
# bin/lib/inflight-tags.sh, shared with the gate (bin/check-inflight-tags.sh) so this index and the
# gate cannot drift apart. Resolved from this hook's own location, not $CLAUDE_PROJECT_DIR - the
# vocabulary belongs to the code, the notes to the project. Missing lib = say nothing, exit 0,
# the same never-break-a-session contract as every other failure here.
. "${HOOK_DIR}/../../bin/lib/inflight-tags.sh" 2>/dev/null || exit 0
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

emitted=""
# REQUIRE THE WHOLE MARKER, not the bare substring. A note that merely MENTIONS
# `inflight-state:` in its prose - one quoting this convention, or the gate's own output - was
# read as CLOSED and rolled into the "not shown" count. The note then appeared only as a
# mislabelled number telling you to delete it, which is exactly the omission this index claims
# its filters cannot make.
is_open()    { ! grep -q 'inflight-state:[^>]*-->' "$1" 2>/dev/null; }

# DEFERRED IS NOT CLOSED. Any state beginning with the word `deferred` means "decided, not now" - the
# reason that follows says not-now-until-what (`deferred - after v6`), so it is greppable by version.
# Deferred work is listed at the BOTTOM, in its own section, using the same impact order as open work.
#
# The rule that makes this a schedule rather than a label: ALL non-deferred work happens before ANY
# deferred work, so running out of open work IS the trigger to re-read this section. It also means
# nothing needs re-tagging when a version ships - a note is only touched when the decision about it
# actually changes.
is_deferred() { grep -q 'inflight-state:[[:space:]]*deferred' "$1" 2>/dev/null; }
deferred_reason() { sed -n 's/.*inflight-state:[[:space:]]*\(deferred[^>]*\)-->.*/\1/p' "$1" 2>/dev/null | head -1 | sed 's/[[:space:]]*$//'; }

emit_group_impactless() { # <type> <full-heading-incl-hashes> - only notes of this type carrying NO impact
    local files hits=""
    files=$(grep -rl "inflight-type:[[:space:]]*$1[[:space:]]*-->" docs/inflight --include='*.md' 2>/dev/null \
              | grep -v 'AGENTS.md' | sort)
    while IFS= read -r f; do
        [ -n "$f" ] || continue
        is_open "$f" || continue
        grep -q 'inflight-impact:[[:space:]]*[a-z-]\+[[:space:]]*-->' "$f" && continue
        hits="${hits}- $(inflight_title "$f")"$'\n'
        emitted="${emitted}${f}"$'\n'
    done <<< "$files"
    [ -n "$hits" ] || return
    emit "$2"
    printf '%s' "$hits"
    emit ""
}

emit_registers() { # every open note typed `register`, whatever impact it carries
    local files hits=""
    # ORDERED BY THE SAME IMPACT SCALE as open work, so one ordering principle governs the whole
    # index. A register carrying `misdirection` - a ledger of flakes nobody tracked - is read before
    # one carrying `deps-debt`. Registers with no impact come last: they cost nothing to leave unread
    # until you need them. Without this the impact on a register was carried and ignored, which is
    # decorative state, and decorative state drifts.
    local ordered="" imp
    for imp in $INFLIGHT_IMPACT_ORDER; do
        ordered="${ordered}$(grep -rl "inflight-type:[[:space:]]*register[[:space:]]*-->" docs/inflight --include='*.md' 2>/dev/null \
                  | grep -v 'AGENTS.md' | xargs -r grep -l "inflight-impact:[[:space:]]*${imp}[[:space:]]*-->" 2>/dev/null | sort)"$'\n'
    done
    ordered="${ordered}$(grep -rl "inflight-type:[[:space:]]*register[[:space:]]*-->" docs/inflight --include='*.md' 2>/dev/null \
              | grep -v 'AGENTS.md' | xargs -r grep -L "inflight-impact:" 2>/dev/null | sort)"$'\n'
    files="$ordered"
    while IFS= read -r f; do
        [ -n "$f" ] || continue
        is_open "$f" || continue
        # PATH AS WELL AS TITLE. A register is something you go and OPEN - "Next candidates, ranked"
        # tells you it exists and not where it is, and these are the documents most likely to be
        # wanted immediately rather than recognised and deferred.
        hits="${hits}- $(inflight_title "$f")  \`${f}\`"$'\n'
        emitted="${emitted}${f}"$'\n'
    done <<< "$files"

    emit "# Registers - standing documents, consult before choosing work"
    emit ""
    emit "Consulted, never completed. Read these before picking up anything below."
    emit ""
    [ -n "$hits" ] && printf '%s' "$hits"

    # THE REPO-LEVEL REGISTERS, which are not inflight notes and so can carry no tag - but are the
    # four an agent most often needs and would otherwise never be told about.
    local extra=""
    [ -f docs/refactoring.md ]                        && extra="${extra}- The deferred-work backlog, and the TODO/FIXME triage  \`docs/refactoring.md\`"$'\n'
    [ -f docs/quarantined-tests.md ]                  && extra="${extra}- Tests currently held out of the gating suites  \`docs/quarantined-tests.md\`"$'\n'
    [ -f docs/todo-index.md ]                         && extra="${extra}- Generated inventory of every marker in the tree  \`docs/todo-index.md\`"$'\n'
    [ -f src/docs/development/upstream-map.yaml ]     && extra="${extra}- Fork-to-upstream mapping - a BACKLOG to chisel down, not just a reference  \`src/docs/development/upstream-map.yaml\`"$'\n'
    [ -n "$extra" ] && printf '%s' "$extra"
    emit ""
}

emit_impact_group() { # <impact> - every open note with this impact, whatever its type
    local files hits=""
    files=$(grep -rl "inflight-impact:[[:space:]]*$1[[:space:]]*-->" docs/inflight --include='*.md' 2>/dev/null \
              | grep -v 'AGENTS.md' | sort)
    while IFS= read -r f; do
        [ -n "$f" ] || continue
        is_open "$f" || continue
        local t
        t=$(sed -n 's/.*inflight-type:[[:space:]]*\([a-z]*\)[[:space:]]*-->.*/\1/p' "$f" | head -1)
        # Registers have their own section above; listing them here as well would double-count them
        # and put a document to consult among the work it ranks.
        [ "$t" = "register" ] && continue
        hits="${hits}- [${t}] $(inflight_title "$f")"$'\n'
        emitted="${emitted}${f}"$'\n'
    done <<< "$files"
    [ -n "$hits" ] || return
    emit "## $1"
    printf '%s' "$hits"
    emit ""
}

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
    emit "## $3"
    printf '%s' "$hits"
    emit ""
}

# GROUPED BY IMPACT, NOT BY TYPE. A feature that exists to prevent a crash has to appear beside the
# crashes; grouping by type first buried it under "proposed work" where nobody ranking the day's work
# would look. The type is still printed on each line, so nothing is lost by leading with the cost.
# REGISTERS FIRST. They are what you consult to decide what to work on - a ranked backlog, a
# collision list - so burying them among the work they rank is backwards.
emit_registers

emit "# Open work - what it costs you to not know"
    emit ""
    emit "One file per item under \`docs/inflight/\`, grouped by impact across every type."
emit ""

for imp in $INFLIGHT_IMPACT_ORDER; do
    emit_impact_group "$imp"
done
# ONLY the features that carried no impact - the rest already appeared under theirs. Without this
# guard a feature with an impact is listed TWICE, once beside its consequence and once here, which
# both inflates the index and undoes the point of grouping by cost.
emit_group_impactless feature "## feature - proposed, no consequence attached"

# ANY OPEN NOTE THAT MATCHED NO GROUP IS LISTED, LOUDLY. The loops above iterate the values this repo
# knows about, so a typo'd or invented inflight-type/inflight-impact matches nothing and the note
# would vanish from the index - silently, and precisely because the filter worked. Same failure as
# the exclusion count below, one level in: the grouping itself can hide things. Listed by name rather
# than counted, because an unmatched note is a bug in its tags and the fix needs to know which file.
unmatched=""
while IFS= read -r f; do
    [ -n "$f" ] || continue
    is_open "$f" || continue
    grep -qxF "$f" <<<"$emitted" && continue
    unmatched="${unmatched}- $(inflight_title "$f")  \`${f}\`"$'\n'
done <<< "$(find docs/inflight -maxdepth 1 -name '*.md' -type f 2>/dev/null | grep -vE '(AGENTS|CLAUDE)\.md' | sort)"
if [ -n "$unmatched" ]; then
    emit "## unmatched - no group claimed them"
    emit ""
    emit "Their \`inflight-type\` or \`inflight-impact\` is missing or misspelt:"
    printf '%s' "$unmatched"
    emit ""
fi

# THE FILTER MUST ADMIT WHAT IT HID. Notes carrying a state are excluded - a closed item presented as
# open is misdirection by this repo's own taxonomy - but a view that silently shrinks is
# indistinguishable from one with nothing to hide. One line of count keeps the undone-cleanup backlog
# visible without letting it occupy the index. Same rule as "no silent caps" for a bounded workflow.
# Deferred notes are NOT in this count - they have their own section below. Counting them here would
# tell you to delete work that was deliberately scheduled.
stated=$(grep -rl 'inflight-state:[^>]*-->' docs/inflight --include='*.md' 2>/dev/null | grep -v 'AGENTS.md' \
           | xargs -r grep -L 'inflight-state:[[:space:]]*deferred' 2>/dev/null | grep -c . || true)
[ "${stated:-0}" -gt 0 ] && emit "_${stated} note(s) not shown: closed, parked or blocked. Delete or migrate them - \`grep -rln inflight-state: docs/inflight\`._" && emit ""

# DEFERRED WORK, last, and never merely counted. It is decided work with a stated trigger, so hiding
# it behind a number would lose the schedule; putting it above open work would compete with it.
emit_deferred() {
    local hits="" imp f files
    for imp in $INFLIGHT_IMPACT_ORDER ""; do
        if [ -n "$imp" ]; then
            files=$(grep -rl "inflight-impact:[[:space:]]*${imp}[[:space:]]*-->" docs/inflight --include='*.md' 2>/dev/null | grep -v 'AGENTS.md' | sort)
        else
            files=$(grep -rL 'inflight-impact:' docs/inflight/*.md 2>/dev/null | grep -v 'AGENTS.md' | sort)
        fi
        while IFS= read -r f; do
            [ -n "$f" ] || continue
            is_deferred "$f" || continue
            hits="${hits}- [${imp:-no impact}] $(inflight_title "$f")  _$(deferred_reason "$f")_"$'\n'
        done <<< "$files"
    done
    [ -n "$hits" ] || return
    emit "# Deferred - decided, not now"
    emit ""
    emit "All non-deferred work happens first. Running out of open work above is the trigger to re-read this."
    emit ""
    printf '%s' "$hits"
    emit ""
}
emit_deferred

emit "# Dated plans and investigations"
    emit ""
    emit "\`docs/plans/\` - the method that settled a question of this shape before:"
plans=$(find docs/plans -name '*.md' -type f 2>/dev/null | sort | sed 's|docs/plans/||;s|\.md$||' | paste -sd, | sed 's/,/, /g')
emit "${plans:-(none)}"
emit ""

# Point-in-time audits of tests that do not run, do not assert, or were never written. Easy to miss
# precisely because nothing goes red to tell you - which is why AGENTS.md says to read the newest
# before re-enabling, deleting or rewriting a dark test.
hardening=$(find docs/test-hardening -name '*.md' -type f 2>/dev/null | sort | sed 's|docs/test-hardening/||;s|\.md$||' | paste -sd, | sed 's/,/, /g')
if [ -n "$hardening" ]; then
    emit "# Dated test-hardening audits"
    emit ""
    emit "\`docs/test-hardening/\` - a test that never runs is not a passing test:"
    emit "${hardening}"
fi
