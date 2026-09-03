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
# A THIN WRAPPER OVER `bin/inflight.mjs docs index`, since the docs context query landed (the plan
# in docs/plans/2026-09-03-001-feat-inflight-docs-context-query-plan.md, KTD8). The three corpus
# areas - solutions, in-flight notes, plans - are rendered by that command, CORPUS-SCOPED: it reads
# every live ref, so a note that exists only on an unmerged branch is listed under the branch set
# carrying it, where this hook's own `find` and `grep` over the working tree could never see it. The
# grouping, the headings and the impact order are the ones this file used to compute in bash; they
# live in bin/lib/views.mjs (`formatDocsIndex`) and bin/lib/inflight-tags.mjs now, and this file
# keeps only what is NOT corpus: the framing, the repo-level registers, the ideation documents and
# the test-hardening audits, all still read from the working tree (R17). The hook no longer sources
# bin/lib/inflight-tags.sh - the vocabulary reaches it through the command, and the Node copy is
# held equal to the shell one by a parity self-test in bin/test-inflight.mjs.
#
# COST, MEASURED COLD, end to end, on the slowest developer host (an Apple Silicon laptop, 613
# refs), against the 8 s session-start budget (the plan's R19, KTD5). Three runs each, same
# window, the pre-migration hook materialised at its last commit beside this one:
#
#   quiet host    before 8.4 s to 12.6 s      after 6.9 s to 7.6 s
#   loaded host   before 7.5 s to  8.0 s      after 8.2 s to  9.1 s   (two self-test suites running)
#
# Before, most of the time was one `git grep` across every ref for a count this hook no longer
# prints. After, almost all of it was the index build - at that point one `git ls-tree` per ref,
# which bin/lib/notes.mjs owns and `bin/inflight.mjs --perf docs index` itemises (the ls-tree
# line carried most of the wall time). Bare `docs` costs the same build.
#
# The branch-facts block (R11) added its own call after the index, MEASURED 2026-09-03 on the same
# laptop, quiet host, 562 live refs: `docs for-branch` alone costs 1.0 s to 1.3 s cold, whether it
# finds documents or not - one `git grep` over the live refs plus a drift summary per hit shown -
# and the whole hook ran 9.2 s to 9.7 s in a window where `docs index` alone ran 6.5 s to 7.9 s:
# over budget, and the plan's stop condition is half again over it, which this did not reach.
#
# THE LEVER WAS PULLED the same day: the index now resolves every ref's `docs/` tree in one
# `cat-file --batch-check` and lists each DISTINCT tree once, because most tips never touch
# `docs/` and share the baseline's tree object. MEASURED cold, same laptop, quiet host, three runs
# each side of the change in one window: the hook went from a little over 7 s to under 5 s, and
# `--perf docs` shows the ls-tree line dropping from one call per ref to one per distinct tree - a
# small fraction of the refs - while the in-process and `rev-list` lines are unchanged. The exact
# figures are the command's to print; bin/lib/notes.mjs's `corpusIndex` header states the shape.
# The grep in the branch-facts block is now the largest single line, and it is fixed-string and
# single, per bin/lib/terms.mjs; on the baseline the call costs one `git rev-parse` and prints one
# line. Re-measure with `time CLAUDE_PROJECT_DIR=$PWD .claude/hooks/inject-recorded-knowledge.sh`.
#
# Tool-neutral where it can be: the knowledge lives in the documents, and this only enumerates them.
# Codex and anything else reading AGENTS.md gets the same rule, just without the reminder - and the
# same list, from `node bin/inflight.mjs docs index`.
#
# Never fails a session: any error prints nothing and exits 0. A broken reminder must not be a
# broken session. When the command cannot run - no `node` on PATH, or the corpus cannot be read -
# the framing is printed with ONE line saying the three areas are missing and how to get them, and
# the non-corpus sections follow; the old bash scan is never fallen back to, because a partial
# index that reads as complete is the failure this hook exists to end.

set -uo pipefail

# RESOLVED BEFORE ANYTHING cd's. This script cd's to the project root further down, after which a
# BASH_SOURCE-relative path no longer resolves - so `bash .claude/hooks/inject-recorded-knowledge.sh`
# from the repo root silently resolved nothing and exited 0, which is the exact invocation this
# index prints for agents to re-run. The tool is resolved from this hook's own location, not
# $CLAUDE_PROJECT_DIR - the code belongs to the checkout the hook ships in, the notes to the project.
HOOK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFLIGHT_TOOL="${HOOK_DIR}/../../bin/inflight.mjs"


root="${CLAUDE_PROJECT_DIR:-$(git rev-parse --show-toplevel 2>/dev/null)}" || exit 0
[ -n "$root" ] && [ -d "$root/docs" ] || exit 0
cd "$root" 2>/dev/null || exit 0
# A tree that does not look like this repository gets nothing, not a notice: the notice below is
# for a checkout that HAS the corpus and could not render it.
[ -d docs/solutions ] || exit 0

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

# A directory's own AGENTS.md/CLAUDE.md/README is not one of the documents these blocks list, and
# every scan over docs/ needs the same guard. Without it the rules doc lists as an ideation document
# or an audit, under a heading telling you to read it as one. Nothing goes red; the index just
# quietly says something false. Anchored on `/` so a real document named e.g. `writing-agents.md`
# is not swallowed. The three corpus areas carry the same guard inside bin/lib/docs-shape.mjs.
DIRECTORY_DOCS_RE='/(AGENTS|CLAUDE|README)\.'

emit "# Already solved here - read before you diagnose"
emit ""
[ -n "$INFLIGHT_INDEX_FILE" ] && emit "_This whole index is also at \`$INFLIGHT_INDEX_FILE\` - grep it by heading (\`^## \`) rather than re-reading it. Re-run \`.claude/hooks/inject-recorded-knowledge.sh\` to refresh._"
emit ""
emit "AGENTS.md asks you to check prior art before forming a hypothesis. These are the titles, so"
emit "you know what exists; grep the ones that look close. Skipping this is the check most often"
emit "skipped, and its failure is silent - you rediscover the problem and it feels like progress."
emit ""

# THIS INDEX IS CORPUS-SCOPED, AND SAYING SO IS STILL THE POINT.
#
# Its predecessor read the working tree, listed what the current branch carried, and confessed the
# gap with a count - because most of this repo's documentation has never landed on master: notes,
# plans and write-ups are authored on the branch that produced them and stay there until that
# branch merges, which many never do. On 2026-09-01 a session investigating
# astubbs/parallel-consumer#44 ran all six AGENTS.md checks from master, got a plausible-looking
# set of hits, and missed a decomposition plan, an architecture write-up on the exact seam under
# investigation, and a sibling-defect note - all of them branch-only, none of them findable by any
# command the table gave it.
#
# The command below closes that gap rather than confessing it: it lists every document on every
# live ref, and its own first lines state the ref set it searched and the one thing it cannot show
# (a version preserved only in an archival ref). So the framing here says what the list IS, and
# leaves the numbers to the command, which is the only thing that measured them.
emit "**This list is CORPUS-SCOPED: every live branch, not just the one checked out.** Documents that"
emit "exist only on unmerged branches are listed under the branch set carrying them, and no"
emit "working-tree grep can reach those - \`bin/inflight.mjs docs show <path>\` prints one from the"
emit "branch that holds it, and \`bin/inflight.mjs prior-art <mechanism>\` searches all of them."
emit ""

# THE THREE CORPUS AREAS, FROM THE TOOL. Stdout only, in one read, so a partial render is never
# emitted: the command prints its whole page or exits non-zero with the reason on stderr, and
# nothing here has an opinion about which. `node` absent is the same outcome as a failed run - the
# notice names both - and the outcome is never the old bash scan.
corpus_index=""
if command -v node >/dev/null 2>&1; then
    corpus_index=$(node "$INFLIGHT_TOOL" docs index 2>/dev/null) || corpus_index=""
fi
if [ -n "$corpus_index" ]; then
    emit "$corpus_index"
    emit ""
else
    emit "**DELIVERY FAILED: session index** - \`node bin/inflight.mjs docs index\` did not run here (no \`node\`"
    emit "on PATH, or the corpus could not be read), so the solutions, in-flight notes and plans are NOT"
    emit "listed below. Nothing else will tell you what exists: run \`node bin/inflight.mjs docs\` yourself,"
    emit "and if that fails too its exit reason is the fault to fix."
    emit ""
fi

# THE BRANCH'S OWN FACTS, FROM THE TOOL (the plan's R11): the documents across every live ref that
# name the checked-out branch, its PR or its issue numbers - the prior art most specific to this
# session and the least likely to be grepped for, because nobody searches for their own branch name.
# Same never-fail rule as the index; the difference is what a failure looks like. The index gets a
# notice because its absence is a hole in the map; this block's absence is the COMMON case - master,
# or a branch nothing names yet - so the hook prints nothing for it, and a real failure is recorded
# by the command itself under `branch facts`, where bare `inflight docs` reads it back. It never
# calls gh: PR facts come from the tool's cache or not at all, and the block says which it had.
branch_facts=""
if command -v node >/dev/null 2>&1; then
    branch_facts=$(node "$INFLIGHT_TOOL" docs for-branch 2>/dev/null) || branch_facts=""
fi
if [ -n "$branch_facts" ]; then
    emit "$branch_facts"
    emit ""
fi

# THE REPO-LEVEL REGISTERS, which are not inflight notes and so can carry no tag - but are the
# four an agent most often needs and would otherwise never be told about. Working tree, by
# design: they are not corpus documents (R17), and they exist on every branch.
registers=""
[ -f docs/refactoring.md ]                        && registers="${registers}- The deferred-work backlog, and the TODO/FIXME triage  \`docs/refactoring.md\`"$'\n'
[ -f docs/quarantined-tests.md ]                  && registers="${registers}- Tests currently held out of the gating suites  \`docs/quarantined-tests.md\`"$'\n'
[ -f docs/todo-index.md ]                         && registers="${registers}- Generated inventory of every marker in the tree  \`docs/todo-index.md\`"$'\n'
[ -f src/docs/development/upstream-map.yaml ]     && registers="${registers}- Fork-to-upstream mapping - a BACKLOG to chisel down, not just a reference  \`src/docs/development/upstream-map.yaml\`"$'\n'
if [ -n "$registers" ]; then
    emit "# Registers outside docs/inflight/ - standing documents, consult before choosing work"
    emit ""
    emit "Consulted, never completed, and untagged because they are not in-flight notes."
    emit ""
    printf '%s' "$registers"
    [ -n "$INFLIGHT_INDEX_FILE" ] && printf '%s' "$registers" >> "$INFLIGHT_INDEX_FILE"
    emit ""
fi

# Ideation documents - the ranked directions, the REJECTION TABLE and the prior-art autopsies behind
# a piece of work. They were invisible here until 2026-08-24 on two counts at once: this directory
# was never scanned, and every artifact in it is .html while the scans of the day matched only .md.
# The cost was not hypothetical - the adaptive-concurrency design was drafted, committed and offered
# for review having cited this directory's throttling document without opening it, and the document
# already held three API constraints that design violated and one decision it contradicted.
#
# The rejection table is the specific reason this block exists. Ideas rejected WITH REASONS are the
# cheapest prior art in the repo and the least likely to be found, because nothing links to a
# rejected idea and no symptom search returns one - you rediscover it by proposing it again.
#
# The explicit `-` stdin operand is master's BSD fix (astubbs#341) - a bare `paste -sd,` reads no
# input on macOS and the list silently comes out empty.
ideation=$(find docs/ideation -type f \( -name '*.html' -o -name '*.md' \) 2>/dev/null \
    | grep -vE "$DIRECTORY_DOCS_RE" \
    | sed -E 's#^docs/ideation/##; s#\.(html|md)$##' | sort -u | paste -sd, - | sed 's/,/, /g')
if [ -n "$ideation" ]; then
    emit "# Ideation: ranked directions, and what was already REJECTED and why"
    emit ""
    emit "\`docs/ideation/\` - read the one covering your area BEFORE designing, not after. These are"
    emit "\`.html\`; open the file, do not infer its contents from a note that summarises it:"
    emit "${ideation}"
    emit ""
fi

# Point-in-time audits of tests that do not run, do not assert, or were never written. Easy to miss
# precisely because nothing goes red to tell you - which is why AGENTS.md says to read the newest
# before re-enabling, deleting or rewriting a dark test.
hardening=$(find docs/test-hardening -name '*.md' -type f 2>/dev/null | grep -vE "$DIRECTORY_DOCS_RE" \
    | sort | sed 's|docs/test-hardening/||;s|\.md$||' | paste -sd, - | sed 's/,/, /g')
if [ -n "$hardening" ]; then
    emit "# Dated test-hardening audits"
    emit ""
    emit "\`docs/test-hardening/\` - a test that never runs is not a passing test:"
    emit "${hardening}"
fi
