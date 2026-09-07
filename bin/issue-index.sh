#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Regenerate docs/inflight/issue-index.md - one line per GitHub issue in this fork, so an agent can
# GREP for issues instead of remembering to query GitHub.
#
# Usage:
#   bin/issue-index.sh          # rewrite docs/inflight/issue-index.md
#
# WHY THIS EXISTS, given "never write down what a command can answer".
#
# It breaks that rule deliberately, and the reason is discovery rather than storage. The rule exists
# to stop a SECOND TRACKER forming - a copy that is believed, drifts, and is indistinguishable from
# the truth. This file is not believed: every row says which day it was read, and the header sends
# you to `gh issue view` before acting. What it buys is that `grep -ri 'offset encoding' docs/`
# now reaches the issue tracker, which no amount of discipline was achieving - `gh issue list
# --state all` is, by AGENTS.md's own admission, the most-skipped of the six prior-art checks,
# because an agent has to think of it first. Grep is what agents already do.
#
# The precedent is `.claude/hooks/inject-recorded-knowledge.sh`, which injects the TITLES of every
# `docs/solutions/` write-up at session start for exactly this reason: "so you know what exists;
# grep the ones that look close". Issues were the one knowledge surface with no such index.
#
# WHY NO `--check` GATE, unlike bin/todo-index.sh.
#
# That script regenerates from the tracked tree, so staleness is decidable offline and a gate is
# free. This one cannot be: deciding whether the index matches reality means asking GitHub, which
# needs the network and a token. A gate like that is skipped for fork PRs and dies on a token
# expiry - the exact failure `bin/check-cve-exclusions.sh`'s header describes, where an unwatched
# list rots precisely when the check that guards it cannot run. So staleness is handled by SAYING
# so, in the file, with a date - not by a check that reports "could not run" and gets read as a
# pass.
#
# THE EXEMPT-FILE MARKER IS EMITTED HERE TOO, and for the same reason as the tags below. Every row
# is a bare `#NN`, so `bin/check-issue-refs.sh` sees a file entirely composed of unqualified
# references - correctly, since qualifying 122 of them would be noise. The header qualifies them
# ONCE, by saying the numbers are this fork's. Note the gate only saw this after the file was first
# COMMITTED: it enumerates via `git diff --name-only`, so an untracked file is invisible to it and a
# local run reports a green that asserted nothing.
#
# THE INFLIGHT TAGS ARE EMITTED HERE, not added to the file by hand. This file is rewritten
# wholesale on every run, so a hand-added `inflight-type` would be silently dropped by the next
# regeneration and `bin/check-inflight-tags.sh` would fail on a file nobody remembered editing.
# `register` is the type because the index is consulted, never completed - it has no done state.
#
# NOT NAMED `check-*`, deliberately. That prefix grants a script to the review agent by pattern
# (bin/AGENTS.md), and the grant is meant for read-only tree gates. This one reaches the network,
# so it must stay outside it.

set -euo pipefail

cd "$(dirname "$0")/.."

OUT="docs/inflight/issue-index.md"
REPO="astubbs/parallel-consumer"

# Strict, for the reason bin/todo-index.sh's header records: a permissive parser silently accepted
# anything it did not recognise and fell through to the rewrite path. Exit 2 is a usage error.
if [ "$#" -ne 0 ]; then
    printf 'usage: %s   (no arguments)\n' "bin/issue-index.sh" >&2
    exit 2
fi

command -v gh >/dev/null 2>&1 || { echo "issue-index: gh is not on PATH" >&2; exit 2; }

# `-R` is not optional here. This fork's `gh` resolves to confluentinc when nothing says otherwise,
# and an index silently built from UPSTREAM's issues would be worse than no index at all - the
# numbers overlap, so every row would look plausible. AGENTS.md opens with this trap.
raw=$(gh issue list -R "$REPO" --state all --limit 1000 \
        --json number,title,state,labels,updatedAt) \
  || { echo "issue-index: could not read issues from $REPO" >&2; exit 2; }

count=$(printf '%s' "$raw" | jq 'length')
[ "$count" -gt 0 ] || { echo "issue-index: read zero issues - refusing to write an empty index" >&2; exit 2; }

# The date is when the data was READ, which is the only thing this file can honestly assert.
today=$(date -u +%Y-%m-%d)

{
cat <<HEADER
# Issue index - a discovery aid, NOT a source of truth

<!-- inflight-type: register -->
<!-- inflight-impact: blind-spot -->
<!-- issue-refs: exempt-file - every row IS a bare fork issue number; that is what the file is. The
     header states the numbers are this fork's, which is the qualification the gate wants, once. -->

**Data read from GitHub on ${today}.** Regenerate with \`bin/issue-index.sh\`.

**Every row here goes stale silently.** An issue can be closed, retitled or relabelled the minute
after this file is written, and nothing in the repository will notice. So: use this to FIND issues,
never to decide anything about one. \`gh issue view <n> -R ${REPO}\` before you act.

## Why this exists, and why it does not break "never write down what a command can answer"

It is here for **discovery**, not storage. That rule exists to stop a second tracker forming - a
copy that gets believed and drifts apart from the truth. This one is not believed: it is dated, it
says so twice, and it sends you elsewhere before acting.

What it buys is reach. \`gh issue list --state all\` is the most-skipped of the six prior-art checks
in [\`AGENTS.md\`](../../AGENTS.md), because an agent has to think of querying GitHub before it can
do it. Grep is what agents do anyway - so a keyword sweep of \`docs/\` now surfaces the tracker
too. The same argument already justifies the \`docs/solutions/\` title index that
\`.claude/hooks/inject-recorded-knowledge.sh\` injects at session start.

**Numbers here are this fork's.** Upstream's range overlaps ours, so a bare number is ambiguous
everywhere else - see [\`docs/issue-references.md\`](../issue-references.md). A row whose title
begins \`confluentinc#NN:\` is a mirror of that upstream issue; read the upstream original rather
than the mirror's summary.

| # | State | Title | Labels |
|---|---|---|---|
HEADER

printf '%s' "$raw" | jq -r '
  sort_by(.number) | .[] |
  # A pipe in a title would break the table row, so escape it rather than dropping the row.
  [ "#\(.number)",
    (.state | ascii_upcase),
    (.title | gsub("\\|"; "\\\\|")),
    ([.labels[].name] | join(", "))
  ] | "| " + join(" | ") + " |"'
} > "$OUT"

echo "issue-index: wrote $OUT ($count issue(s), read $today)"
