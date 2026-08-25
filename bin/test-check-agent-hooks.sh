#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for the three hooks in `.claude/hooks/`. Feeds each one a crafted hook payload on stdin
# and asserts its verdict.
#
# WHY IT EXISTS. docs/agent-harness.md's own rule 3 is "give it a negative control - make it go red
# on purpose before you trust it". The harness shipped without applying that rule to itself, and a
# review found six defects in one 25-line parser, four of which let the exact mistake the hook is
# named after straight through while two blocked legitimate merges. Every one of those is a case
# below. A hook is unusually easy to get wrong this way: it runs inside another process, its output
# is consumed by a model rather than a person, and a broken one looks identical to a quiet one.
#
# WHY THE `test-check-` PREFIX, given there is no `bin/check-agent-hooks.sh`. bin/AGENTS.md pairs
# `test-check-X.sh` with `check-X.sh`, and this is a deliberate exception: the prefix is also what
# grants a script to the review agent by pattern, and a self-test for the harness is exactly the
# thing a reviewer should be able to run. It stays inside the grant's boundary - read-only, no
# network, no writes outside its own mktemp directory.
#
# Run: bin/test-check-agent-hooks.sh

set -uo pipefail

# Resolved from $0, never from the caller's cwd. inject-merge-checklist.sh finds its checklist via
# CLAUDE_PROJECT_DIR or `git rev-parse --show-toplevel`, so running this script from a DIFFERENT
# checkout of this repo - the primary one, a sibling worktree on another branch - silently pointed
# the hook at that tree's docs/ instead of this branch's. On a branch without the checklist the
# hook exits 0 and the assertions below read the absence as "not injected". That is the failure
# this file exists to catch, in this file: pinning the root makes the test measure the branch it
# ships with, wherever it is run from.
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HOOKS="$REPO_ROOT/.claude/hooks"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

failures=0

assert() { # <description> <expected> <actual>
    if [ "$2" = "$3" ]; then
        echo "ok:   $1"
    else
        echo "FAIL: $1 (expected '$2', got '$3')"
        failures=$((failures + 1))
    fi
}

# ---------------------------------------------------------------------------------------------
# check-squash-subject.sh
#
# The rule is now "no --subject on gh pr merge at all", so there is nothing to parse and nothing
# to get subtly wrong. The previous version policed CORRECT use of the flag - last-occurrence
# semantics, PR-number cross-check, shell quoting - in 343 lines that review found wrong in both
# directions. These cases exist to stop that creeping back.
# ---------------------------------------------------------------------------------------------

echo
echo "--- check-squash-subject.sh ---"


fails=${fails:-0}

# Which hook verdict()/expect() drive - each section points it at its own hook, so the harness
# below is written once (it was cloned per hook until review flagged the drift risk).
HOOK_UNDER_TEST="$HOOKS/check-squash-subject.sh"

verdict() { # <bash-command> -> ALLOW | DENY, from $HOOK_UNDER_TEST
    # The command goes to the JSON builder on STDIN, never argv: one case here is a 150 KB command,
    # and passing that as an argument hits the same E2BIG the case exists to detect - the harness
    # would die and the failure would read as the hook's.
    local out tmp
    tmp=$(mktemp)
    printf '%s' "$1" | python3 -c 'import json,sys; print(json.dumps({"tool_name":"Bash","tool_input":{"command":sys.stdin.read()}}))' > "$tmp"
    out=$("$HOOK_UNDER_TEST" < "$tmp" 2>/dev/null)
    rm -f "$tmp"
    case "$out" in
        *'"deny"'*) echo DENY ;;
        *)          echo ALLOW ;;
    esac
}

expect() { # <expected> <name> <command>
    local got; got=$(verdict "$3")
    if [ "$got" = "$1" ]; then echo "ok:   $2"; else echo "FAIL: $2 (expected $1, got $got)"; fails=$((fails + 1)); fi
}

expect DENY  "--subject with no (#N)"              'gh pr merge 2999 --squash --subject "foo"'
expect DENY  "-t, the documented SHORT form"       'gh pr merge 2999 --squash -t "foo"'
expect DENY  "--subject= equals form"              'gh pr merge 2999 --squash --subject=foo'
expect DENY  "-t= equals form"                     'gh pr merge 2999 --squash -t=foo'
expect DENY  "-tVALUE attached short form"         'gh pr merge 2999 --squash -tfoo'
expect ALLOW "-tVALUE attached, with (#N)"         'gh pr merge 2999 --squash -t"foo (#2999)"'
expect DENY  "(#N) present but not at the END"     'gh pr merge 2999 --squash --subject "port (#2999) to master"'

# Squash-body sign-off guard: a squash body override must carry the Co-authored-by trailer and
# must not carry a Claude-Session line (docs/merge-checklist.md owns both). Non-squash merges and
# merges without a body override are out of scope; an unreadable --body-file fails open because
# gh itself then fails loudly.
body_ok=$(mktemp); printf '%s\n%s\n' "Real explanation." "Co-authored-by: Claude Opus 5 (1M context) <noreply@anthropic.com>" > "$body_ok"
body_bare=$(mktemp); printf '%s\n' "Real explanation, no trailer." > "$body_bare"
body_sess=$(mktemp); printf '%s\n%s\n%s\n' "Real explanation." "Co-authored-by: Claude Opus 5 (1M context) <noreply@anthropic.com>" "Claude-Session: https://claude.ai/code/session_x" > "$body_sess"
expect DENY  "squash --body without co-authored trailer"    'gh pr merge 2999 --squash --body "no trailer here"'
expect ALLOW "squash --body with co-authored trailer"       'gh pr merge 2999 --squash --body "why.
Co-authored-by: Claude Opus 5 (1M context) <x@y>"'
expect DENY  "trailer mid-line is not a trailer"            'gh pr merge 2999 --squash --body "why. Co-authored-by: Claude Opus 5 (1M context) <x@y>"'
expect DENY  "squash body with Claude-Session line"         "gh pr merge 2999 --squash --body-file $body_sess"
expect ALLOW "squash --body-file with trailer"              "gh pr merge 2999 --squash --body-file $body_ok"
expect DENY  "squash --body-file without trailer"           "gh pr merge 2999 --squash --body-file $body_bare"
expect ALLOW "squash --body-file unreadable fails open"     'gh pr merge 2999 --squash --body-file /nonexistent/nope.txt'
expect ALLOW "non-squash merge body needs no trailer"       'gh pr merge 2999 --merge --body "no trailer needed"'
rm -f "$body_ok" "$body_bare" "$body_sess"
# The same global-flag gap the outstanding-work guard had, in the other implementation: this one
# detects the command with a regex, so `gh -R owner/repo pr merge` was not a merge as far as it was
# concerned and the subject went unchecked. Found by sweeping for the defect CLASS after fixing the
# sibling (AGENTS.md -> "look for other instances of the same defect"), astubbs#324.
expect DENY  "leading -R, --subject with no (#N)"  'gh -R astubbs/parallel-consumer pr merge 2999 --squash --subject "foo"'
expect DENY  "leading --repo, --subject no (#N)"   'gh --repo astubbs/parallel-consumer pr merge 2999 --squash --subject "foo"'
expect DENY  "attached --repo=, --subject no (#N)" 'gh --repo=astubbs/parallel-consumer pr merge 2999 --squash --subject "foo"'
expect DENY  "attached -RVALUE, squash body bare"  'gh -Rastubbs/parallel-consumer pr merge 2999 --squash --body "no trailer here"'
expect ALLOW "leading -R, subject ends with (#N)"  'gh -R astubbs/parallel-consumer pr merge 2999 --squash --subject "foo (#2999)"'
expect ALLOW "leading -R on a non-merge command"   'gh -R astubbs/parallel-consumer pr view 2999 --json title'
# gh also accepts the flag BETWEEN `pr` and `merge` (`gh pr -R owner/repo merge`), and the first
# global-flag fix only covered the leading position - found by the astubbs#324 review, live-proven
# against gh itself. Same defect class, third position.
expect DENY  "mid-position -R, --subject no (#N)"  'gh pr -R astubbs/parallel-consumer merge 2999 --squash --subject "foo"'
expect ALLOW "mid-position -R on a non-merge cmd"  'gh pr -R astubbs/parallel-consumer view 2999 --json title'

expect ALLOW "--subject ending with (#N)"          'gh pr merge 2999 --squash --subject "foo (#2999)"'
expect ALLOW "-t ending with (#N)"                 'gh pr merge 2999 --squash -t "foo (#2999)"'
expect ALLOW "--subject mentioned inside --body"   'gh pr merge 2999 --squash --body "why --subject matters
Co-authored-by: Claude Opus 5 (1M context) <noreply@anthropic.com>"'
expect ALLOW "escaped apostrophe, number present"  'gh pr merge 2999 --squash --subject '"'"'don'"'"'\'"'"''"'"'t drop it (#2999)'"'"''
expect ALLOW "--body-file does not touch subject"  'gh pr merge 2999 --squash --body-file b.txt'
expect ALLOW "no subject override at all"          'gh pr merge 2999 --squash'
expect ALLOW "not a merge command"                 'gh pr view 2999 --json title'
expect ALLOW "the word --subject in other text"    'echo "we discussed --subject and why"'

# A 150 KB command used to hit E2BIG and exit having printed nothing, which reads as ALLOW.
big=$(python3 -c "print('gh pr merge 2999 --squash --subject bad # ' + 'x'*150000)")
if [ "$(verdict "$big")" = DENY ]; then
    echo "ok:   a 150 KB command does not fail open"
else
    echo "FAIL: a 150 KB command fails open"; fails=$((fails + 1))
fi

# Negative control: prove it can deny. A guard that has never fired proves nothing (bin/AGENTS.md).
if [ "$(verdict 'gh pr merge 2999 --squash --subject "x"')" = DENY ] \
   && [ "$(verdict 'gh pr merge 2999 --squash')" = ALLOW ]; then
    echo "ok:   the guard distinguishes overridden from not"
else
    echo "FAIL: the guard does not distinguish overridden from not"; fails=$((fails + 1))
fi

# ---------------------------------------------------------------------------------------------
# pre-commit-gate.sh
#
# CLAUDE_PROJECT_DIR points at a fixture holding a stub `.githooks/pre-commit`, so the gate's
# pass/fail is controlled by the test rather than by the state of the real tree.
# ---------------------------------------------------------------------------------------------

echo
echo "--- pre-commit-gate.sh ---"

make_project() { # <gate-exit-code> -> project dir
    local dir="$TMP/proj$RANDOM$RANDOM"
    mkdir -p "$dir/.githooks"
    printf '#!/bin/sh\necho "STUB GATE SPOKE"\nexit %s\n' "$1" > "$dir/.githooks/pre-commit"
    chmod +x "$dir/.githooks/pre-commit"
    echo "$dir"
}

gate_rc() { # <project-dir> <bash-command>
    local payload
    payload=$(python3 -c 'import json,sys; print(json.dumps({"tool_name":"Bash","tool_input":{"command":sys.argv[1]}}))' "$2")
    printf '%s' "$payload" | CLAUDE_PROJECT_DIR="$1" "$HOOKS/pre-commit-gate.sh" >/dev/null 2>&1
    echo $?
}

red=$(make_project 1)
green=$(make_project 0)

assert "green gate lets the commit through" 0 \
    "$(gate_rc "$green" 'git commit -m "ordinary"')"

# The block is exit 2 - PreToolUse's documented deny - not 1, which is merely a non-blocking error.
assert "red gate blocks with exit 2" 2 \
    "$(gate_rc "$red" 'git commit -m "ordinary"')"

# THE POINT OF THE WHOLE SCRIPT. The first version was `pre-commit || exit 2` inline, which never
# read the payload, so --no-verify was ignored and a red gate locked the agent out of committing at
# all - including the commit that would have fixed the gate.
assert "--no-verify bypasses a red gate" 0 \
    "$(gate_rc "$red" 'git commit --no-verify -m "I have a reason"')"

# ...but only as a real argument. A commit MESSAGE that merely mentions the flag is not a bypass,
# or the escape hatch could be triggered by writing about it.
assert "--no-verify inside a quoted message is NOT a bypass" 2 \
    "$(gate_rc "$red" 'git commit -m "document the --no-verify escape hatch"')"

# The blocked call must carry the reason, or the agent sees "hook error: No stderr output" and has
# nothing to act on - the observed behaviour of the inline form.
stderr_text=$(python3 -c 'import json,sys; print(json.dumps({"tool_name":"Bash","tool_input":{"command":"git commit -m x"}}))' \
    | CLAUDE_PROJECT_DIR="$red" "$HOOKS/pre-commit-gate.sh" 2>&1 >/dev/null)
case "$stderr_text" in
    *"STUB GATE SPOKE"*) got=forwarded ;;
    *)                   got="swallowed: $stderr_text" ;;
esac
assert "the failing gate's own output reaches stderr" forwarded "$got"


# Round six. The bypass search scanned the WHOLE payload, so a later, unrelated command could
# request a bypass the commit never asked for - and the violation lands.
assert "--no-verify in a LATER command is not this commit's bypass" 2 \
    "$(gate_rc "$red" 'git commit -m x && echo --no-verify')"
assert "--no-verify in an EARLIER command is not this commit's bypass" 2 \
    "$(gate_rc "$red" 'echo --no-verify && git commit -m x')"
# TWO REAL COMMITS, one asking for a bypass and one not. The bypass used to be judged over the
# whole payload, so the second commit's flag exempted the first - which in a clone with no
# core.hooksPath meant that first commit landed with no gate run at all.
assert "a later commit's --no-verify does not exempt an earlier one" 2 \
    "$(gate_rc "$red" 'git commit -m first; git commit --no-verify -m second')"
assert "both commits asking for a bypass is honoured" 0 \
    "$(gate_rc "$red" 'git commit --no-verify -m a; git commit --no-verify -m b')"
# A quoted MULTILINE message containing the flag. Lexing line-by-line split this down the middle,
# the first line raised ValueError, and the fallback then found the flag in the message text.
assert "a multiline message mentioning the flag is not a bypass" 2 \
    "$(gate_rc "$red" 'git commit -m "line1
--no-verify
line3"')"
assert "git -C <path> commit --no-verify is still a bypass" 0 \
    "$(gate_rc "$red" 'git -C /some/path commit --no-verify -m x')"

# No python, no way to read the payload - so no way to see --no-verify. Running the gate anyway
# blocked the documented escape hatch on the one machine with no other way out. Python is not among
# the repo's build requirements (JDK 17, Docker, the Maven wrapper), so this is a real setup.
nopython="$TMP/nopython"; mkdir -p "$nopython"
for b in bash cat mktemp rm sh dirname env; do
    resolved=$(command -v "$b" 2>/dev/null) && ln -sf "$resolved" "$nopython/$b"
done
nopython_rc=$(printf '%s' "$(python3 -c 'import json; print(json.dumps({"tool_name":"Bash","tool_input":{"command":"git commit -m x"}}))')" \
    | PATH="$nopython" CLAUDE_PROJECT_DIR="$red" "$HOOKS/pre-commit-gate.sh" >/dev/null 2>&1; echo $?)
assert "a missing python3 fails OPEN rather than blocking the bypass" 0 "$nopython_rc"

# Fail open: no gate script in the project at all must not block every commit.
empty="$TMP/empty$RANDOM"; mkdir -p "$empty"
assert "absent gate script fails OPEN" 0 \
    "$(gate_rc "$empty" 'git commit -m "ordinary"')"

# NOT A COMMIT AT ALL: the hook must self-filter. The registration's `if: Bash(git commit *)` is
# supposed to scope it, but the script cannot rely on that - observed live (astubbs#324 babysit):
# with the gate red, a plain `ls` and a read-only `cat` of the gate itself were blocked with the
# gate's own error, because "no commit found" fell into "run the gate". Self-filtering in the
# script is the contract every other hook in this directory follows.
assert "a non-commit command is not gated" 0 \
    "$(gate_rc "$red" 'ls -la')"
assert "a read-only command naming the gate is not gated" 0 \
    "$(gate_rc "$red" 'cat .githooks/pre-commit')"

# ---------------------------------------------------------------------------------------------
# inject-merge-checklist.sh
# ---------------------------------------------------------------------------------------------

echo
echo "--- inject-merge-checklist.sh ---"

# The prompt reaches the JSON builder on STDIN, never as argv - the harness has the same ~128 KiB
# MAX_ARG_STRLEN ceiling as the hook it is testing, and a test that dies building its own fixture
# would report the hook broken (or fixed) for reasons that have nothing to do with the hook.
injected() { # <prompt> -> YES | NO
    local out
    out=$(printf '%s' "$1" \
        | python3 -c 'import json,sys; print(json.dumps({"prompt":sys.stdin.read()}))' \
        | CLAUDE_PROJECT_DIR="$REPO_ROOT" "$HOOKS/inject-merge-checklist.sh" 2>/dev/null)
    [ -n "$out" ] && echo YES || echo NO
}

assert "merge-prep prompt: 'ready to merge?'"          YES "$(injected 'is this ready to merge?')"
assert "merge-prep prompt: 'squash it'"                YES "$(injected 'squash it and land it')"
assert "merge-prep prompt: 'tidy up the commits'"      YES "$(injected 'can you tidy up the commits')"
assert "merge-prep prompt: 'rebase onto master'"       YES "$(injected 'rebase onto master please')"
assert "unrelated prompt: a test failure"              NO  "$(injected 'why is ShardTest failing under load')"
assert "unrelated prompt: reading code"                NO  "$(injected 'explain how offset encoding works')"
assert "unrelated prompt: emerge is not merge"         NO  "$(injected 'the pattern should emerge from the data')"

# NEUTRALITY, ENFORCED. docs/merge-checklist.md is the single source; the hook is a delivery
# mechanism. An earlier version also prepended its own summary of the checklist's two standing
# asks - a second copy, in the one place nobody would think to look for drift. This asserts the
# injected text is the file's own bytes plus a pointer, and nothing else.
body=$(python3 -c 'import json,sys; print(json.dumps({"prompt":"ready to merge?"}))' \
    | CLAUDE_PROJECT_DIR="$REPO_ROOT" "$HOOKS/inject-merge-checklist.sh" 2>/dev/null \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["hookSpecificOutput"]["additionalContext"])')
checklist_head=$(head -1 "$REPO_ROOT/docs/merge-checklist.md" 2>/dev/null)
case "$body" in
    *"$checklist_head"*) got=verbatim ;;
    *)                   got=missing ;;
esac
assert "the checklist is injected verbatim from its own file" verbatim "$got"

preamble=${body%%$'\n'*}
if [ "${#preamble}" -le 200 ]; then got=pointer_only; else got="carries its own advice (${#preamble} chars)"; fi
# A hook payload carries the whole prompt. Passed as argv it hit Linux's ~128 KiB MAX_ARG_STRLEN
# and died with "Argument list too long" BEFORE python started - so on exactly the long, pasted-in
# prompts a human is most likely to be mid-decision on, the checklist silently did not appear.
big_prompt="ready to merge $(head -c 150000 /dev/zero | tr '\0' 'x')"
assert "a 150 KB merge-prep prompt is still injected" YES "$(injected "$big_prompt")"

assert "the preamble points at the doc rather than restating it" pointer_only "$got"

# ---------------------------------------------------------------------------------------------
# inject-recorded-knowledge.sh
#
# It runs at session start with no input to parse, so the risks are different from the other three:
# it must never break a session, and it must actually name the documents. A reminder that silently
# emits nothing is worse than none - it looks installed.
# ---------------------------------------------------------------------------------------------

echo
echo "--- inject-recorded-knowledge.sh ---"

knowledge_out=$(CLAUDE_PROJECT_DIR="$REPO_ROOT" "$HOOKS/inject-recorded-knowledge.sh" 2>/dev/null)
knowledge_rc=$?

assert "exits 0" 0 "$knowledge_rc"

# The point of the hook: a path you can grep for, not a vague nudge to go and look.
case "$knowledge_out" in
    *"docs/solutions/"*) got=names_paths ;;
    *)                   got=no_paths ;;
esac
assert "names actual document paths" names_paths "$got"

# One line per document, or it is not an index. Compare against the real corpus rather than a
# hardcoded number, so adding a solution does not fail this test.
want_count=$(find "$REPO_ROOT/docs/solutions" -name '*.md' -type f 2>/dev/null | wc -l | tr -d ' ')
got_count=$(printf '%s\n' "$knowledge_out" | grep -c 'docs/solutions/.*\.md')
assert "lists every solution document" "$want_count" "$got_count"

# Titles, not slugs: the filename is already in the path, and the title is what makes an agent
# recognise the document as relevant to the thing in front of it.
case "$knowledge_out" in
    *"duplication scanners do not look where agents actually duplicate"*) got=titles ;;
    *) got=slugs_only ;;
esac
assert "uses frontmatter titles, not just filenames" titles "$got"

# A YAML-quoted title must render WITHOUT its quotes. The title-check above happens to use an
# unquoted one, so it passed while quoted titles rendered with literal quote marks - found in review
# on astubbs/parallel-consumer#320. Asserted against whichever document actually needs quoting, so
# the case survives that document being renamed.
quoted=$(grep -rl '^title:[[:space:]]*"' "$REPO_ROOT/docs/solutions" 2>/dev/null | head -1)
if [ -n "$quoted" ]; then
    bare=$(sed -n 's/^title:[[:space:]]*"//p' "$quoted" | head -1 | sed 's/"$//')
    case "$knowledge_out" in
        *"\"${bare}\""*) got=quotes_leaked ;;
        *"$bare"*)         got=quotes_stripped ;;
        *)                 got=title_missing ;;
    esac
    assert "strips YAML quoting from a quoted title" quotes_stripped "$got"
fi

# Open work must be grouped by CONSEQUENCE across every type, in the order docs/inflight/AGENTS.md defines - and
# signal-integrity classes must come FIRST: you cannot judge the code through instruments that lie,
# so `misdirection` before any product defect. Asserted against the FIXTURE corpus built below, not
# the real one - the real-corpus assertion required a live bug/misdirection note to exist on master
# forever, so the PR deleting the last one (deletion is the directory's prescribed lifecycle) would
# have turned Repo Hygiene red for doing its job (astubbs#324 review). The fixture carries one
# well-tagged bug and one feature, so grouping and ordering are asserted on controlled input.

# A note with no class must still appear. A marker someone forgot to add must be VISIBLE, never a
# way for a note to drop silently out of the index - that failure mode is the one the whole hook
# exists to prevent.
class_tmp=$(mktemp -d)
mkdir -p "$class_tmp/docs/inflight" "$class_tmp/docs/solutions/x"
printf -- '---\ntitle: "s"\n---\n' > "$class_tmp/docs/solutions/x/s.md"
printf '# An unclassified note\n' > "$class_tmp/docs/inflight/bug-no-class.md"
printf '# A closed note\n\n<!-- inflight-type: task -->\n<!-- inflight-state: closed - will not do -->\n' > "$class_tmp/docs/inflight/task-closed.md"
# Open + mistagged + prose that MENTIONS `inflight-state:` without carrying the marker. The safety
# net must judge open/closed by the whole `-->` marker exactly as the groups do: its first version
# used a bare substring grep, so this exact shape vanished - not grouped, not unmatched, not counted.
printf '# A prose mention of the marker\n\nThe gate greps for inflight-state: markers.\n\n<!-- inflight-type: bug -->\n<!-- inflight-impact: misdirekshun -->\n' > "$class_tmp/docs/inflight/bug-prose-mention.md"
printf '# A well-tagged bug\n\n<!-- inflight-type: bug -->\n<!-- inflight-impact: misdirection -->\n' > "$class_tmp/docs/inflight/bug-well-tagged.md"
printf '# A proposed feature\n\n<!-- inflight-type: feature -->\n' > "$class_tmp/docs/inflight/feature-idea.md"
unclassified_out=$(CLAUDE_PROJECT_DIR="$class_tmp" "$HOOKS/inject-recorded-knowledge.sh" 2>/dev/null)

# Grouped by IMPACT across every type, and rendered as REAL markdown headings. Both changed together:
# a feature that exists to prevent a crash has to sort beside the crashes rather than under "proposed
# work", and an agent has to be able to filter the index structurally instead of reading it whole.
# Asserted as ORDER and STRUCTURE, never as one literal heading string - the previous version matched
# `**bug / misdirection**` exactly and so went red the moment either changed, which is what a
# string-matching test does in place of catching a regression.
mis_at=$(grep -n '^## misdirection$' <<<"$unclassified_out" | head -1 | cut -d: -f1)
feat_at=$(grep -n '^## feature - proposed' <<<"$unclassified_out" | head -1 | cut -d: -f1)
if [ -n "$mis_at" ] && [ -n "$feat_at" ] && [ "$mis_at" -lt "$feat_at" ]; then
    got=signal_integrity_first
elif [ -z "$mis_at" ] || [ -z "$feat_at" ]; then
    got=group_missing
else
    got=proposed_work_first
fi
assert "groups open work by impact, signal integrity first" signal_integrity_first "$got"

case "$unclassified_out" in
    *$'\n## '*) got=real_headings ;;
    *)          got=bold_pseudo_headings ;;
esac
assert "groups are markdown headings, not bold text" real_headings "$got"

case "$unclassified_out" in
    *"An unclassified note"*) got=listed ;;
    *)                        got=dropped ;;
esac
assert "a note whose tags match no group is still listed" listed "$got"
case "$unclassified_out" in
    *"A prose mention of the marker"*) got=listed ;;
    *)                                 got=dropped ;;
esac
assert "an open note mentioning inflight-state: in prose is still listed" listed "$got"

# THE WORD ANYWHERE IN THE MARKER, not anchored to the front. `parked - deferred` is as deferred as
# `deferred - parked`; position carries no meaning, and requiring it invented a rule nobody agreed to.
pos_tmp=$(mktemp -d); mkdir -p "$pos_tmp/docs/inflight" "$pos_tmp/docs/solutions/x"
printf -- '---\ntitle: "s"\n---\n' > "$pos_tmp/docs/solutions/x/s.md"
printf '# Deferred with the word at the FRONT\n\n<!-- inflight-type: task -->\n<!-- inflight-impact: ci -->\n<!-- inflight-state: deferred - parked -->\n' > "$pos_tmp/docs/inflight/ci-front.md"
printf '# Deferred with the word in the MIDDLE\n\n<!-- inflight-type: task -->\n<!-- inflight-impact: ci -->\n<!-- inflight-state: parked - deferred, gated on a user base -->\n' > "$pos_tmp/docs/inflight/ci-middle.md"
pos_out=$(CLAUDE_PROJECT_DIR="$pos_tmp" "$HOOKS/inject-recorded-knowledge.sh" 2>/dev/null)
def_block=$(sed -n '/^# Deferred/,/^# /p' <<<"$pos_out")
case "$def_block" in *"word at the FRONT"*) got=found ;; *) got=missing ;; esac
assert "a state beginning 'deferred' is deferred" found "$got"
case "$def_block" in *"word in the MIDDLE"*) got=found ;; *) got=missing ;; esac
assert "a state with 'deferred' later is equally deferred" found "$got"

# PARKED IS DEFERRED - the two words name one disposition, so a note using either must land in the
# same section. Before this, `parked` alone matched neither is_open nor is_deferred and fell in with
# closed and blocked under "not shown", which is how notes went missing on astubbs#323.
printf '# Parked with no other keyword\n\n<!-- inflight-type: task -->\n<!-- inflight-impact: ci -->\n<!-- inflight-state: parked - nobody has argued for it -->\n' > "$pos_tmp/docs/inflight/ci-parked.md"
park_out=$(CLAUDE_PROJECT_DIR="$pos_tmp" "$HOOKS/inject-recorded-knowledge.sh" 2>/dev/null)
park_def=$(awk '/^# Deferred/{f=1;next} f&&/^# /{exit} f' <<<"$park_out")
case "$park_def" in *"Parked with no other keyword"*) got=deferred ;; *) got=stranded ;; esac
assert "a bare 'parked' state is deferred" deferred "$got"

# ...and its reason must survive. Anchoring the extractor to the word `deferred` printed an EMPTY
# reason for every note that said `parked` instead, which is a note in the right section saying nothing.
case "$park_def" in *"nobody has argued for it"*) got=reason_shown ;; *) got=reason_lost ;; esac
assert "a parked note still shows its reason" reason_shown "$got"
rm -rf "$pos_tmp"

# A stated note leaves OPEN WORK but must still be NAMED. Counting was the previous contract and it
# hid two notes tagged `parked - deferred`, which matched neither filter and lost their titles to a
# bare number - the filter making exactly the omission this index claims it cannot make.
open_block=$(sed -n '/^# Open work/,/^# /p' <<<"$unclassified_out")
case "$open_block" in
    *"A closed note"*) got=leaked ;;
    *)                 got=excluded ;;
esac
assert "a closed note is kept out of open work" excluded "$got"

case "$unclassified_out" in
    *"A closed note"*) got=named ;;
    *)                 got=hidden_silently ;;
esac
assert "an excluded note is still named, not just counted" named "$got"
rm -rf "$class_tmp"

# A session must survive a repo that does not look like this one. Two shapes: no docs at all, and
# a docs/ with no solutions - both are "say nothing, exit 0", never a stack trace into the session.
empty_dir=$(mktemp -d)
out_empty=$(CLAUDE_PROJECT_DIR="$empty_dir" "$HOOKS/inject-recorded-knowledge.sh" 2>/dev/null)
assert "a tree with no docs/ exits 0" 0 "$?"
assert "a tree with no docs/ emits nothing" "" "$out_empty"
mkdir -p "$empty_dir/docs"
out_nosol=$(CLAUDE_PROJECT_DIR="$empty_dir" "$HOOKS/inject-recorded-knowledge.sh" 2>/dev/null)
assert "a docs/ with no solutions exits 0" 0 "$?"
assert "a docs/ with no solutions emits nothing" "" "$out_nosol"
rm -rf "$empty_dir"

# check-merge-outstanding-work.sh
#
# The negative controls matter more than the positive one here. The guard's whole risk is that it
# either fires on ordinary commands (and gets routed around) or fails to fire when it counts. The
# substring-vs-token case below is not hypothetical: the first draft grepped for "gh pr merge" and
# blocked `gh pr comment --body "run gh pr merge later"`.
# ---------------------------------------------------------------------------------------------

echo
echo "--- check-merge-outstanding-work.sh ---"

HOOK_UNDER_TEST="$HOOKS/check-merge-outstanding-work.sh"

# Isolate every case from the caller's shell: an exported override there must not flip a DENY case.
unset MERGE_DESPITE_OUTSTANDING_WORK

# A session dir holding a task file written just now == background work in flight.
ow_session="ow-selftest-$$"
ow_tasks="/tmp/claude-$(id -u)/selftest/$ow_session/tasks"
mkdir -p "$ow_tasks"
printf 'still writing\n' > "$ow_tasks/agent-live.output"
export CLAUDE_CODE_SESSION_ID="$ow_session"

expect DENY  "a merge is refused while a task is still writing"            'gh pr merge 31 -R astubbs/parallel-consumer --rebase'
expect DENY  "a merge later in a compound command is still seen"           'echo hi && gh pr merge 31 --squash'
expect DENY  "a full-path gh is still a merge"                             '/usr/local/bin/gh pr merge 31 --rebase'
expect ALLOW "a binary merely ENDING in gh is not gh"                      '/usr/local/bin/sleigh pr merge 31'

# GLOBAL FLAGS BEFORE THE SUBCOMMAND. gh takes --repo/-R either side of `pr`, and only the trailing
# form keeps `gh pr merge` adjacent - so a three-token check passes the leading form straight
# through. It is the shape house style produces: AGENTS.md writes every prior-art command as
# `gh issue list -R astubbs/parallel-consumer`, and this repo's `gh` resolves to the WRONG repo
# without one, so an agent is actively encouraged to type it. Found babysitting astubbs#324; all
# four spellings were silently allowed before the fix.
expect DENY  "a -R before the subcommand is still a merge"                 'gh -R astubbs/parallel-consumer pr merge 31 --squash'
expect DENY  "a --repo before the subcommand is still a merge"             'gh --repo astubbs/parallel-consumer pr merge 31'
expect DENY  "an attached --repo=VALUE is still a merge"                   'gh --repo=astubbs/parallel-consumer pr merge 31'
expect DENY  "an attached -RVALUE is still a merge"                        'gh -Rastubbs/parallel-consumer pr merge 31'
expect ALLOW "a leading -R on a NON-merge subcommand still passes"         'gh -R astubbs/parallel-consumer pr view 31'
expect ALLOW "a leading -R on an unrelated subcommand still passes"        'gh -R astubbs/parallel-consumer issue list'
expect DENY  "a mid-position -R (between pr and merge) is still a merge"   'gh pr -R astubbs/parallel-consumer merge 31 --squash'
expect ALLOW "a mid-position -R on a non-merge subcommand still passes"    'gh pr -R astubbs/parallel-consumer view 31'
expect ALLOW "the words gh pr merge inside --body are not a merge"         'gh pr comment 5 --body "remember to run gh pr merge later"'
# QUOTE-SPLIT SPELLINGS. shlex joins mer""ge / mer'ge' / mer\ge back into the token `merge`, so
# the token scan sees a merge - but the cheap pre-filter used to test the RAW payload for the
# substring `merge` and exited first, making the pre-filter the decider its own comment says it
# must never be. Found by the astubbs#324 review; verified red before the quote-strip fix.
expect DENY  "a quote-split merge (mer\"\"ge) is still a merge"            'gh pr mer""ge 31 --squash'
expect DENY  "a single-quote-split merge is still a merge"                 "gh pr me'rge' 31 --squash"
expect DENY  "a backslash-split merge is still a merge"                    'gh pr mer\ge 31 --squash'
expect ALLOW "a non-merge gh command passes"                               'gh pr view 31'
expect ALLOW "an unrelated command passes"                                 'git status'

# THE DOCUMENTED OVERRIDE, delivered the only way an agent can deliver it: as an env-prefix on the
# merge command, which reaches the hook as command TOKENS - a hook only ever sees the HARNESS's
# process env. The first version of this suite asserted only the process-env form (kept at the
# bottom as the human-wrapping-the-harness path), which hid that the in-session route was dead.
expect ALLOW "env-prefix override on the command releases the guard"       'MERGE_DESPITE_OUTSTANDING_WORK=1 gh pr merge 31 --rebase'
expect DENY  "an env-prefix set to 0 is not an override"                   'MERGE_DESPITE_OUTSTANDING_WORK=0 gh pr merge 31 --rebase'
expect DENY  "the override token AFTER the command is not a prefix"        'gh pr merge 31 --body "MERGE_DESPITE_OUTSTANDING_WORK=1"'

# THE PR'S OWN INFLIGHT NOTE, surfaced at merge. A note recording what is still open is written so
# the items are not forgotten and is then read by nobody at the moment it could still change the
# outcome. These cases prove the arm fires, quotes the right part, and stays out of the way otherwise.
touch -t 200109090146 "$ow_tasks/agent-live.output"   # nothing in flight, so only the note arm can fire

# IN A SCRATCH REPOSITORY, NOT THE REAL WORKING TREE. The guard resolves the note from
# `git rev-parse --show-toplevel`, so this arm used to write `docs/inflight/pr-90001-selftest.md`
# into whatever checkout the suite was run from and delete it afterwards. Two things were wrong with
# that, and the second is why it is fixed here rather than noted: it MUTATES a tree the suite does
# not own, and the fixed path is shared, so two concurrent runs - ordinary on a box running several
# agent sessions - race between one run's write and the other's cleanup. Measured: five concurrent
# runs, two of them red on this arm, deterministically green once isolated.
ow_repo="$(mktemp -d)"
(
  cd "$ow_repo" || exit 1
  git init -q .
  git checkout -q -b selftest/outstanding-fixture 2>/dev/null || git branch -q -m selftest/outstanding-fixture
  : > .keep
  git add .keep
  git -c user.email=selftest@example.invalid -c user.name=selftest commit -qm "fixture"
)
ow_return="$PWD"
cd "$ow_repo" || exit 1
mkdir -p docs/inflight
cat > docs/inflight/pr-90001-selftest.md <<'NOTE'
# astubbs#90001 - self-test fixture
<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->
## Open
- SELFTEST_OPEN_ITEM
## Already fixed
- SELFTEST_RESOLVED_ITEM
NOTE
expect DENY  "a PR with an open inflight note is stopped at merge"          'gh pr merge 90001 --squash'
expect ALLOW "a PR with no inflight note is not stopped"                    'gh pr merge 90002 --squash'
expect ALLOW "the documented override releases the note arm too"            'MERGE_DESPITE_OUTSTANDING_WORK=1 gh pr merge 90001 --squash'

# The note arm must quote the OPEN section and stop at "Already fixed" - a note whose resolved
# section has grown must not bury the two lines that still matter.
note_out="$(printf '{"tool_name":"Bash","tool_input":{"command":"gh pr merge 90001 --squash"}}' \
    | CLAUDE_CODE_SESSION_ID="$ow_session" bash "$HOOK_UNDER_TEST" 2>/dev/null)"
case "$note_out" in *SELFTEST_OPEN_ITEM*) got=quoted ;; *) got=missing ;; esac
assert "the open item is quoted into the deny reason" quoted "$got"
case "$note_out" in *SELFTEST_RESOLVED_ITEM*) got=leaked ;; *) got=stopped ;; esac
assert "the Already-fixed section is NOT quoted" stopped "$got"
rm -f docs/inflight/pr-90001-selftest.md
cd "$ow_return" || exit 1
rm -rf "$ow_repo"

# Stale task file == nothing in flight. Proves the window is load-bearing rather than "any file".
touch -t 200109090146 "$ow_tasks/agent-live.output"
expect ALLOW "a task that stopped writing long ago does not block"         'gh pr merge 31 --rebase'

# BOTH ARMS OF THE MTIME READ, plus the two that cannot be reached with a working stat.
#
# CI is Linux, so the BSD arm shipped unexecuted, and the fail-closed arm is unreachable on any
# platform with a functioning stat - a file `find` has just listed can always be dated. A stub `stat`
# earlier on PATH forces each in turn: same file, same command throughout, only stat changing.
stub_bin="$TMP/stub-stat"; mkdir -p "$stub_bin"
saved_path="$PATH"
stub_stat() { cat > "$stub_bin/stat"; chmod +x "$stub_bin/stat"; PATH="$stub_bin:$saved_path"; }

# A BSD stat: rejects -c, answers `-f %m <file>`. MODELLED, not borrowed - the mtime read goes
# through python3 (which this suite already requires for every payload) rather than the host's stat,
# because on macOS the host stat IS BSD stat and would reject the `-c` a delegating stub hands it.
# That is how this case reddened on the very platform the branch exists for.
stub_stat <<'STUB'
#!/usr/bin/env bash
case "$1" in
    -c) exit 1 ;;                     # BSD stat has no -c
    -f) exec python3 -c 'import os,sys; print(int(os.stat(sys.argv[1]).st_mtime))' "$3" ;;
esac
exit 1
STUB
expect ALLOW "BSD stat: a long-stale task still does not block"            'gh pr merge 31 --rebase'
printf 'still writing\n' > "$ow_tasks/agent-live.output"
expect DENY  "BSD stat: a task still writing is still caught"              'gh pr merge 31 --rebase'

# A stat that answers nothing. The file matched the session's tasks glob, so something is there;
# being unable to date it is not evidence that nothing is running. Red against the pre-fix code,
# which skipped the file and allowed the merge - the macOS behaviour this branch exists to fix.
stub_stat <<'STUB'
#!/usr/bin/env bash
exit 1
STUB
touch -t 200109090146 "$ow_tasks/agent-live.output"
expect DENY  "an undateable task file is assumed LIVE, not stale"          'gh pr merge 31 --rebase'

# A stat that FAILS AND STILL PRINTS - which is what real GNU coreutils does when handed `-f %m
# FILE`: exit 1, six lines of filesystem prose on stdout (measured on 9.7). So the value reaching
# the guard is not absent, it is garbage, and a fail-closed arm testing only `-z` waves it through
# to `$(( now - mtime ))`, where the arithmetic evaluates `File` as a variable name and `set -u`
# aborts the hook - non-zero with no verdict, which PreToolUse ALLOWS. Hence the arm tests the
# value's SHAPE. Keep the task file LIVE so nothing but that arm can produce the refusal.
stub_stat <<'STUB'
#!/usr/bin/env bash
case "$1" in -c) exit 1 ;; esac
printf '  File: "x"\n    ID: 7f048f3f Namelen: 255     Type: tmpfs\nBlock size: 4096\n'
exit 1
STUB
printf 'still writing\n' > "$ow_tasks/agent-live.output"
expect DENY  "a stat that fails but still PRINTS cannot defeat the guard"  'gh pr merge 31 --rebase'

PATH="$saved_path"
rm -f "$stub_bin/stat"
touch -t 200109090146 "$ow_tasks/agent-live.output"
expect ALLOW "the same stale file, with stat working, does not block"      'gh pr merge 31 --rebase'

# Fail-open paths. A guard that blocks on its own bug jams the tool call shut.
printf 'still writing\n' > "$ow_tasks/agent-live.output"
export CLAUDE_CODE_SESSION_ID=""
expect ALLOW "no session id fails OPEN"                                    'gh pr merge 31 --rebase'
export CLAUDE_CODE_SESSION_ID="$ow_session"
# The payload must CONTAIN "merge", or the cheap pre-filter exits before python3 ever runs and
# this case tests the pre-filter instead of the parser's except branch it names (astubbs#324
# review: the original 'not json' payload never reached json.load).
got=$(printf 'not json but merge' | "$HOOK_UNDER_TEST" 2>/dev/null); \
    case "$got" in *'"deny"'*) got=DENY ;; *) got=ALLOW ;; esac
assert "unparseable payload fails OPEN" ALLOW "$got"

# The process-env form of the override is kept for a human whose own shell wraps the harness.
got=$(printf '%s' '{"tool_input":{"command":"gh pr merge 31 --rebase"}}' \
    | MERGE_DESPITE_OUTSTANDING_WORK=1 "$HOOK_UNDER_TEST" 2>/dev/null); \
    case "$got" in *'"deny"'*) got=DENY ;; *) got=ALLOW ;; esac
assert "the process-env override releases the guard" ALLOW "$got"

rm -rf "/tmp/claude-$(id -u)/selftest/$ow_session"

echo
# The squash-subject section counts into `fails` (its harness predates `assert`); fold it in so a
# failure there fails the script rather than printing FAIL and exiting 0.
failures=$((failures + fails))
# DEFERRED IS NOT CLOSED. A state beginning with `deferred` means decided-but-not-now: kept out of
# open work, listed in its own section at the BOTTOM with its reason, and NOT rolled into the
# "not shown" count - counting it there would tell you to delete work that was deliberately scheduled.
def_tmp=$(mktemp -d)
mkdir -p "$def_tmp/docs/inflight" "$def_tmp/docs/solutions/x"
printf -- '---\ntitle: "s"\n---\n' > "$def_tmp/docs/solutions/x/s.md"
printf '# An open bug\n\n<!-- inflight-type: bug -->\n<!-- inflight-impact: stall -->\n' > "$def_tmp/docs/inflight/bug-open.md"
printf '# A deferred bug\n\n<!-- inflight-type: bug -->\n<!-- inflight-impact: misdirection -->\n<!-- inflight-state: deferred - after v6 -->\n' > "$def_tmp/docs/inflight/bug-deferred.md"
printf '# A closed note\n\n<!-- inflight-type: task -->\n<!-- inflight-state: closed - will not do -->\n' > "$def_tmp/docs/inflight/task-closed.md"
def_out=$(CLAUDE_PROJECT_DIR="$def_tmp" "$HOOKS/inject-recorded-knowledge.sh" 2>/dev/null)

case "$(sed -n '/^# Open work/,/^# Deferred/p' <<<"$def_out")" in
    *"A deferred bug"*) got=leaked ;; *) got=excluded ;;
esac
assert "a deferred note is kept out of open work" excluded "$got"
case "$def_out" in *"# Deferred - decided, not now"*) got=has_section ;; *) got=no_section ;; esac
assert "deferred work gets its own section" has_section "$got"
case "$def_out" in *"_deferred - after v6_"*) got=reason_shown ;; *) got=reason_lost ;; esac
assert "the deferral reason is shown, greppable by version" reason_shown "$got"
# The not-shown section takes the CLOSED note and leaves the deferred one alone. This was a count
# ("1 note(s) not shown") until astubbs#323's review found the count hiding two notes that matched
# neither filter; the intent is unchanged - a deferred note must never be swept in with the abandoned
# ones - but it is now checked by name, which is what makes a miscategorised note visible at all.
# Bounded to its OWN section: "Not shown above" is emitted BEFORE "Deferred", so an open-ended range
# swallows the deferred section and every deferred note reads as swept in.
not_shown=$(awk '/^# Not shown above/{f=1;next} f&&/^# /{exit} f' <<<"$def_out")
case "$not_shown" in *"A closed note"*) got=listed ;; *) got=missing ;; esac
assert "the closed note is named in the not-shown section" listed "$got"
case "$not_shown" in *"A deferred bug"*) got=swept_in ;; *) got=left_alone ;; esac
assert "a deferred note is not swept into the not-shown section" left_alone "$got"
d_at=$(grep -n '^# Deferred' <<<"$def_out" | head -1 | cut -d: -f1)
o_at=$(grep -n '^# Open work' <<<"$def_out" | head -1 | cut -d: -f1)
{ [ -n "$d_at" ] && [ -n "$o_at" ] && [ "$o_at" -lt "$d_at" ]; } && got=below || got=above
assert "deferred is listed below open work" below "$got"
rm -rf "$def_tmp"

# INVOKED BY A RELATIVE PATH - the exact command the index prints for an agent to re-run. The script
# cd's to the project root, so anything resolved from BASH_SOURCE afterwards silently resolves to
# nothing and the whole index comes back empty.
rel_out=$( cd "$REPO_ROOT" && bash .claude/hooks/inject-recorded-knowledge.sh 2>/dev/null )
case "$rel_out" in *"# Open work"*) got=works ;; *) got=silently_empty ;; esac
assert "the hook works when invoked by a relative path" works "$got"

echo
echo "--- remind-inflight-on-push.sh ---"

# The PUSH-time complement to the merge guard. Informational (`additionalContext`), never a deny - a
# hook that blocked pushes would be routed around within a day. `gh` is stubbed so the case does not
# depend on a live PR, and the stub is first on PATH rather than mocked inside the hook.
PUSH_HOOK="$HOOKS/remind-inflight-on-push.sh"

# ITS OWN REPO, ON A REAL BRANCH. Both hooks resolve the PR from `git rev-parse --abbrev-ref HEAD`
# and exit silently when that returns "HEAD" - correct behaviour, since a detached checkout has no
# branch to look a PR up by. GitHub Actions checks PRs out DETACHED, so these cases passed on every
# developer machine and failed only in CI, which is the worst possible place to learn it. Running
# them inside a scratch repo on a named branch makes the ambient checkout irrelevant.
push_repo="$(mktemp -d)"
(
  cd "$push_repo" || exit 1
  git init -q .
  git checkout -q -b selftest/push-fixture 2>/dev/null || git branch -q -m selftest/push-fixture
  # An UNBORN branch reads as empty from `rev-parse --abbrev-ref HEAD`, which trips the same silent
  # exit as a detached one - so the fixture needs a commit, not just a branch name.
  : > .keep
  git add .keep
  git -c user.email=selftest@example.invalid -c user.name=selftest commit -qm "fixture"
)
cd "$push_repo" || exit 1
push_stub="$(mktemp -d)"
printf '#!/usr/bin/env bash\necho 90003\n' > "$push_stub/gh"
chmod +x "$push_stub/gh"
mkdir -p docs/inflight
cat > docs/inflight/pr-90003-selftest.md <<'NOTE'
# astubbs#90003 - self-test fixture
<!-- inflight-type: task -->
<!-- inflight-impact: coordination -->
## Open
- PUSH_OPEN_ITEM
## Already fixed
- PUSH_RESOLVED_ITEM
NOTE

# PRIVATE TMPDIR, for the reason the branch-context section states at length: the reminder's throttle
# stamp is named after the BRANCH, this fixture always uses the same branch name, and the stamp lives
# in shared /tmp - so a second concurrent run of this suite clears and rewrites the stamp between this
# function's `rm` and its fire, and the reminder that should have appeared is throttled away. Measured
# on a pristine master worktree: one run in three failed, on a different case each time, which is
# exactly how a shared-state flake presents.
PUSH_TMPDIR="$(mktemp -d)"

push_fire() { # <command> -> stdout of the hook
    rm -f "$PUSH_TMPDIR"/pc-push-reminder-* 2>/dev/null
    printf '{"tool_name":"Bash","tool_input":{"command":"%s"}}' "$1" \
        | PATH="$push_stub:$PATH" TMPDIR="$PUSH_TMPDIR" bash "$PUSH_HOOK" 2>/dev/null
}

out="$(push_fire 'git push')"
case "$out" in *PUSH_OPEN_ITEM*) got=reminded ;; *) got=silent ;; esac
assert "a push on a PR with an open note reminds" reminded "$got"

# GLOBAL FLAGS THAT TAKE A VALUE. Dropping the flag but not its value left the value where the
# subcommand should be, so `git -C <path> push` matched nothing and the hook was silently dead for
# the form an agent uses most. Review of astubbs#324 found it; the session that wrote the hook had
# been pushing with `git -C` all along and never saw a reminder.
for form in 'git -C /some/path push' 'git -c user.name=x push' 'git --git-dir=/d push' 'git -C /p -c a=b push'; do
    out="$(push_fire "$form")"
    case "$out" in *PUSH_OPEN_ITEM*) got=reminded ;; *) got=silent ;; esac
    assert "reminds on: $form" reminded "$got"
done

# The negative controls matter as much: a hook that fires on any mention of the word is noise, and
# noise is how a reminder gets ignored.
out="$(push_fire 'git commit -m "push later"')"
case "$out" in *PUSH_OPEN_ITEM*) got=fired ;; *) got=silent ;; esac
assert "a commit MESSAGE mentioning push does not fire" silent "$got"
case "$out" in *PUSH_RESOLVED_ITEM*) got=leaked ;; *) got=stopped ;; esac
assert "the Already-fixed section is not quoted" stopped "$got"
case "$out" in *'"deny"'*) got=blocked ;; *) got=advisory ;; esac
assert "the push reminder never denies" advisory "$got"

# TOKENS, NOT SUBSTRINGS - the rule the other two hooks state. A commit message mentioning a push,
# and a non-git binary, must both stay silent.
out="$(push_fire 'git commit -m "ready to push"')"
[ -z "$out" ] && got=silent || got=fired
assert "a commit message mentioning push does not fire" silent "$got"
out="$(push_fire 'npm push')"
[ -z "$out" ] && got=silent || got=fired
assert "a non-git binary does not fire" silent "$got"

# THROTTLED, or a push loop repeats the whole note and teaches the reader to skip it.
printf '{"tool_name":"Bash","tool_input":{"command":"git push"}}' | PATH="$push_stub:$PATH" TMPDIR="$PUSH_TMPDIR" bash "$PUSH_HOOK" >/dev/null 2>&1
out="$(printf '{"tool_name":"Bash","tool_input":{"command":"git push"}}' | PATH="$push_stub:$PATH" TMPDIR="$PUSH_TMPDIR" bash "$PUSH_HOOK" 2>/dev/null)"
[ -z "$out" ] && got=throttled || got=repeated
assert "an immediate second push is throttled" throttled "$got"

rm -f docs/inflight/pr-90003-selftest.md
rm -rf "$push_stub" "$PUSH_TMPDIR"

echo
echo "--- check-history-rewrite.sh ---"

# A force-push re-anchors every inline review comment and destroys the incremental diff a reviewer
# works from. This guard exists because docs/merge-checklist.md already said "re-cut at the end" and
# that did not prevent it happening twice in one session - once costing a reviewer their diff, once
# starting a re-cut with three reviews mid-flight.
HIST_HOOK="$HOOKS/check-history-rewrite.sh"
# STUBBED `gh`, because the enriched refusal only renders when a PR is found - and "found" depends on
# a branch, a PR and a network. Unstubbed, CI took the generic-refusal path and the quiet-PR case
# below read as reads_as_safe while every developer machine passed. The stub answers a PR with zero
# threads and zero running jobs, which is precisely the quiet PR that case exists to test.
hist_stub="$(mktemp -d)"
cat > "$hist_stub/gh" <<'GH'
#!/usr/bin/env bash
case "$*" in
    *"pr list"*)  echo 90003 ;;
    *"/comments"*) echo 0 ;;
    *"run list"*) echo 0 ;;
    *)            echo "" ;;
esac
GH
chmod +x "$hist_stub/gh"
hist() { printf '{"tool_name":"Bash","tool_input":{"command":"%s"}}' "$1" | PATH="$hist_stub:$PATH" bash "$HIST_HOOK" 2>/dev/null; }
hist_expect() { # <DENY|ALLOW> <desc> <command>
    local out got
    out="$(hist "$3")"
    [ -n "$out" ] && got=DENY || got=ALLOW
    assert "$2" "$1" "$got"
}

hist_expect DENY  "a force-push is refused"                      'git push --force origin main'
hist_expect DENY  "short -f at end of command is refused"        'git push -f'
hist_expect DENY  "--force-with-lease is still a rewrite"        'git push --force-with-lease origin main'
hist_expect DENY  "a rebase is refused"                          'git rebase --onto master abc branchname'
hist_expect DENY  "an amend is refused"                          'git commit --amend -m x'
hist_expect DENY  "filter-branch is refused"                     'git filter-branch --tree-filter x'

# Finishing a rebase already in progress is not starting one - blocking it would strand the tree
# mid-operation, which is worse than the rewrite.
hist_expect ALLOW "rebase --abort is not a rewrite"              'git rebase --abort'
hist_expect ALLOW "rebase --continue is not a rewrite"           'git rebase --continue'
hist_expect ALLOW "an ordinary push is untouched"                'git push origin main'
hist_expect ALLOW "reset --hard to a remote ref is untouched"    'git reset --hard origin/main'

# TOKENS, NOT SUBSTRINGS - prose about force-pushing must not fire, or the guard gets routed around.
hist_expect ALLOW "a commit message mentioning rebase"           'git commit -m "notes on rebase and force-push"'
hist_expect ALLOW "a gh comment mentioning force-push"           'gh pr comment 1 --body "we should force-push"'
hist_expect ALLOW "a non-git -f flag"                            'grep -f patterns.txt file'

# The override is delivered as an env PREFIX, which reaches a hook as command tokens - a hook only
# ever sees the harness process env.
hist_expect ALLOW "the documented override releases it"          'REWRITE_HISTORY_CONFIRMED=1 git push --force'
hist_expect DENY  "an override set to 0 is not an override"      'REWRITE_HISTORY_CONFIRMED=0 git push --force'

# EVERY OTHER WAY TO MOVE A REF. The first version caught four shapes and let seven through - a
# guard that reaches only what you thought of is a documented bypass, so each is pinned here.
hist_expect DENY  "reset backwards drops commits"                'git reset --hard HEAD~3'
hist_expect DENY  "reset to a bare SHA drops commits"            'git reset --hard 1a2b3c4d'
hist_expect DENY  "branch -f moves a ref"                        'git branch -f main abc1234'
hist_expect DENY  "checkout -B with a start point moves a ref"   'git checkout -B main abc1234'
hist_expect DENY  "switch -C with a start point moves a ref"     'git switch -C main abc1234'
hist_expect DENY  "update-ref writes a branch directly"          'git update-ref refs/heads/main abc1234'
hist_expect DENY  "deleting a remote branch"                     'git push origin --delete topic'
hist_expect DENY  "the colon form of remote deletion"            'git push origin :topic'

# Forward sync and ordinary branch creation are routine and must stay silent, or the guard becomes
# noise and gets waved through.
hist_expect ALLOW "reset --hard to a remote ref is a sync"       'git reset --hard origin/main'
hist_expect ALLOW "reset --hard HEAD discards local edits only"  'git reset --hard HEAD'
hist_expect ALLOW "checkout -B with no start point"              'git checkout -B tmp'
hist_expect ALLOW "creating a branch"                            'git branch newbranch'

# A QUIET PR MUST NOT READ AS SAFE. Zero threads and zero running jobs is what a reviewer part-way
# through the diff looks like - the case that loses most from a rewrite - so the refusal has to say
# that absence was measured rather than implying nothing is at risk.
quiet_out="$(hist 'git push --force origin main')"
case "$quiet_out" in
    *"NOT evidence that a rewrite is safe"*|*"inline review comment"*|*"IN PROGRESS"*) got=states_the_risk ;;
    *) got=reads_as_safe ;;
esac
assert "a PR with nothing outstanding still states the risk" states_the_risk "$got"

# The deny must NAME what would be lost, not just refuse.
hist_out="$(hist 'git push --force origin main')"
case "$hist_out" in *"LAST step before a merge"*) got=explains ;; *) got=bare_refusal ;; esac
assert "the refusal says when a rewrite IS allowed" explains "$got"

echo
echo "--- inject-branch-context.sh ---"

# WHY THESE CASES LOOK THE WAY THEY DO. An injection hook's CORRECT output on a boring branch is
# silence, and silence is byte-identical to being broken, unregistered or absent - so most cases
# below force the hook to speak, and every asserted-silent case is paired with a forced one proving
# the same call path can still talk. The lesson is astubbs/parallel-consumer#339's, applied here.
#
# EVERY CASE PINS ITS OWN FIXTURE. A self-test for a branch-context hook must not be a function of
# which branch it happens to be run on: the CI checkout is detached (which this hook deliberately
# stays silent for), and a developer's worktree has whatever commits it has. Each case builds a
# scratch repository with the commits, notes, marker and `gh` answers it wants to assert about.
BCTX_HOOK="$HOOKS/inject-branch-context.sh"

bctx_payload() { # <cwd> <event> [tool_name] [prompt] [agent_type] [agent_id] -> hook payload JSON
    # BUILT IN PYTHON, NOT BY printf. `%r` is a Python conversion and shell printf rejects it, which
    # produced an empty payload and turned every case here into a silent-looking pass-through -
    # exactly the "silence is indistinguishable from broken" failure these cases exist to catch,
    # committed inside the cases themselves.
    python3 -c '
import json, sys
cwd, event = sys.argv[1], sys.argv[2]
d = {"cwd": cwd, "hook_event_name": event}
if event == "SessionStart":
    d["source"] = "startup"
argv = sys.argv + [""] * 8
if argv[3]:
    d["tool_name"] = argv[3]
if argv[4]:
    d["tool_input"] = {"prompt": argv[4]}
if argv[5]:
    d["agent_type"] = argv[5]
if argv[6]:
    d["agent_id"] = argv[6]
print(json.dumps(d))
' "$@"
}

# A PRIVATE TMPDIR PER SUITE RUN, and it is not tidiness. The hook keys its throttle stamps and its
# PR cache off ${TMPDIR:-/tmp}, and these fixtures use FIXED names (`bctxfixture01`) - so two
# concurrent runs of this suite, which is ordinary on a box with several agent sessions, clear and
# recreate each other's stamps and the throttle assertions fail at random. Diagnosed from exactly
# that failure in the push-reminder cases below, which shared /tmp the same way.
BCTX_TMPDIR="$(mktemp -d)"

bctx_fire() { # <payload-json> [PATH-override] -> the hook's stdout
    local payload="$1" pathover="${2:-$PATH}" tmp out
    tmp=$(mktemp)
    printf '%s' "$payload" > "$tmp"
    out=$(PATH="$pathover" TMPDIR="$BCTX_TMPDIR" bash "$BCTX_HOOK" < "$tmp" 2>/dev/null)
    rm -f "$tmp"
    printf '%s' "$out"
}

bctx_context() { # <hook stdout> -> the additionalContext string, or the raw text for SessionStart
    printf '%s' "$1" | python3 -c '
import json,sys
raw=sys.stdin.read()
try:
    print(json.loads(raw)["hookSpecificOutput"]["additionalContext"])
except Exception:
    print(raw)
'
}

bctx_clean_stamps() { rm -f "$BCTX_TMPDIR"/pc-branch-context-* 2>/dev/null; return 0; }

bctx_repo() { # <dir> - master, then a feature branch with a bodied commit, a bodyless one, a note
    local d="$1"
    mkdir -p "$d/docs/inflight" "$d/docs/plans"
    (
      cd "$d" || exit 1
      git init -q .
      git checkout -q -b master 2>/dev/null || git branch -q -m master
      echo base > base.txt
      git add base.txt
      git -c user.email=selftest@example.invalid -c user.name=selftest commit -qm "base"
      git checkout -q -b feats/bctx-fixture
      echo one > one.txt
      git add one.txt
      git -c user.email=selftest@example.invalid -c user.name=selftest commit -qF - <<'MSG'
feat(fixture) astubbs#999: the decision this branch made on purpose

BCTX_BODY_ONLY_STRING. Three non-empty body lines, so the hook must report 3.

It argues by name against the most extractable thing in the diff.

Third line.
MSG
      printf '# handoff\nBCTX_NOTE_INNARDS\n' > docs/inflight/pr-999-handoff.md
      git add docs/inflight/pr-999-handoff.md
      git -c user.email=selftest@example.invalid -c user.name=selftest commit -qm "docs(inflight): the handoff note"
      printf 'owner: selftest\nstatus: BCTX_MARKER_LINE\n' > .worktree-owner
    )
}

bctx_gh_stub() { # <dir> <mode> - a `gh` first on PATH answering in one of five shapes
    cat > "$1/gh" <<STUB
#!/usr/bin/env bash
case "\$*" in
  *"pr view"*)
    case "$2" in
      pr)      cat <<'JSON'
{"number":9412,"title":"BCTX_PR_TITLE","body":"line1\nline2\nline3","url":"https://example.invalid/9412","state":"OPEN","isDraft":false,
 "comments":[{"author":{"login":"github-actions"}},{"author":{"login":"github-actions"}},{"author":{"login":"a-real-human"}}],
 "reviews":[{"author":{"login":"claude"},"state":"COMMENTED"},{"author":{"login":"claude"},"state":"COMMENTED"}]}
JSON
               ;;
      nopr)    echo "no pull requests found for branch \"whatever\"" >&2; exit 1 ;;
      broken)  echo "error connecting to github.com" >&2; exit 1 ;;
      slow)    sleep 30 ;;
      garbage) echo "not json at all" ;;
    esac
    ;;
esac
exit 0
STUB
    chmod +x "$1/gh"
}

bctx_tmp="$(mktemp -d)"
bctx_repo "$bctx_tmp/wt"
# `origin` exists so the repo slug resolves; nothing ever contacts it, because `gh` is stubbed.
git -C "$bctx_tmp/wt" remote add origin https://github.com/selftest-owner/selftest-repo.git
# The DISPATCHER sits in its own working tree on master - the coordinator's real position, and
# the one that makes "describe the tree you dispatched TO, not the one you are in" a real assertion.
bctx_dispatcher="$bctx_tmp/dispatcher"
bctx_repo "$bctx_dispatcher"
git -C "$bctx_dispatcher" checkout -q master
stub_pr="$(mktemp -d)";      bctx_gh_stub "$stub_pr" pr
stub_nopr="$(mktemp -d)";    bctx_gh_stub "$stub_nopr" nopr
stub_broken="$(mktemp -d)";  bctx_gh_stub "$stub_broken" broken
stub_slow="$(mktemp -d)";    bctx_gh_stub "$stub_slow" slow

sess_payload="$(bctx_payload "$bctx_tmp/wt" SessionStart)"

# --- SessionStart, the forced-to-speak baseline every silent case below is measured against ------
bctx_clean_stamps
out="$(bctx_fire "$sess_payload" "$stub_pr:$PATH")"
ctx="$(bctx_context "$out")"
case "$ctx" in *'Branch context: `feats/bctx-fixture`'*) got=named ;; *) got=silent_or_wrong ;; esac
assert "SessionStart on a feature branch names the branch" named "$got"
# SessionStart injects RAW STDOUT; only PreToolUse discards it and needs the JSON envelope. Emitting
# the envelope here would put a wall of JSON in front of the agent instead of the report.
case "$out" in *'hookSpecificOutput'*) got=envelope ;; *) got=plain ;; esac
assert "SessionStart emits plain text, not the PreToolUse envelope" plain "$got"

# --- the commits, which are the point of the hook ------------------------------------------------
case "$ctx" in *'the decision this branch made on purpose'*) got=listed ;; *) got=missing ;; esac
assert "the branch's commit subjects are listed" listed "$got"
case "$ctx" in *'[  3]'*) got=counted ;; *) got=uncounted ;; esac
assert "a commit body is reported by its non-empty line count" counted "$got"
case "$ctx" in *'[  -]'*) got=marked ;; *) got=unmarked ;; esac
assert "a bodyless commit is marked as having none" marked "$got"
# CHEAP: NAMES AND POINTERS, NEVER BODIES. The contract inject-recorded-knowledge.sh's header states
# and this hook inherits - the failure being fixed is not knowing the record EXISTS. If body text
# ever starts being inlined, this block's size stops being bounded by the number of commits.
case "$ctx" in *BCTX_BODY_ONLY_STRING*) got=inlined ;; *) got=pointer_only ;; esac
assert "commit BODIES are not inlined, only counted" pointer_only "$got"
case "$ctx" in *BCTX_NOTE_INNARDS*) got=inlined ;; *) got=pointer_only ;; esac
assert "note CONTENTS are not inlined, only named" pointer_only "$got"

# --- the handoff note and the marker --------------------------------------------------------------
case "$ctx" in *'docs/inflight/pr-999-handoff.md'*) got=listed ;; *) got=missing ;; esac
assert "a branch-only docs/inflight note is listed" listed "$got"
case "$ctx" in *BCTX_MARKER_LINE*) got=quoted ;; *) got=missing ;; esac
assert "the .worktree-owner marker is quoted" quoted "$got"

# --- the PR, including the comment authors that were the 2026-08-24 miss ---------------------------
case "$ctx" in *'selftest-owner/selftest-repo#9412'*) got=named ;; *) got=missing ;; esac
assert "the PR is named with the slug derived from origin" named "$got"
case "$ctx" in *'BCTX_PR_TITLE'*) got=titled ;; *) got=missing ;; esac
assert "the PR title is reported" titled "$got"
case "$ctx" in *'body: 3 lines'*) got=measured ;; *) got=missing ;; esac
assert "the PR body is measured, not fetched into context" measured "$got"
# The miss this hook was written for was a PR COMMENT, not the body - so a count that buries the one
# human comment under five bot ones is the same miss with extra steps.
case "$ctx" in *'a-real-human x1'*) got=attributed ;; *) got=anonymous ;; esac
assert "PR comments are counted per author" attributed "$got"
case "$ctx" in *'Read the ones from a-real-human'*) got=singled_out ;; *) got=buried ;; esac
assert "the non-bot commenter is singled out from the bots" singled_out "$got"
case "$ctx" in *'claude COMMENTED x2'*) got=aggregated ;; *) got=repeated ;; esac
assert "repeat reviews by one author are aggregated, not repeated" aggregated "$got"

# --- DEGRADED READS ARE LOUD, NEVER SHORT ---------------------------------------------------------
# Measured incident, not a hypothesis: inject-recorded-knowledge.sh's GNU-only `xargs -r` silently
# shortens its own index under a BSD xargs, and a truncated-but-plausible block is worse than none.
bctx_clean_stamps
ctx="$(bctx_context "$(bctx_fire "$sess_payload" "$stub_broken:$PATH")")"
case "$ctx" in *'Open PR - UNKNOWN'*) got=loud ;; *) got=quiet ;; esac
assert "a failing gh makes the PR section LOUDLY unknown" loud "$got"
case "$ctx" in *'Not "no PR", UNKNOWN'*) got=says_so ;; *) got=ambiguous ;; esac
assert "the unknown PR section refuses to be read as 'no PR'" says_so "$got"
# ...and the branch half must survive the PR half failing, or one dead network call costs the commits.
case "$ctx" in *'the decision this branch made on purpose'*) got=kept ;; *) got=lost ;; esac
assert "the commits still appear when gh fails" kept "$got"

# A CONFIRMED absence is a different answer from a failure, and must not read as an alarm - every
# fresh branch would otherwise print one, and an alarm that is always on gets scrolled past.
bctx_clean_stamps
ctx="$(bctx_context "$(bctx_fire "$sess_payload" "$stub_nopr:$PATH")")"
case "$ctx" in *'PR: none open for this branch'*) got=measured ;; *) got=alarmed ;; esac
assert "a confirmed absent PR is reported as a fact, not an alarm" measured "$got"
case "$ctx" in *'UNKNOWN'*) got=alarmed ;; *) got=measured ;; esac
assert "a confirmed absent PR does not also raise UNKNOWN" measured "$got"

# THE NETWORK CALL IS BOUNDED. `gh` sits on a path that fires per dispatch and per subagent, so a
# hung link must cost the timeout and not the session. GNU `timeout(1)` is deliberately NOT used -
# macOS does not ship it - so this case is what proves the python-side bound actually bounds.
bctx_clean_stamps
bctx_start=$(date +%s)
ctx="$(bctx_context "$(bctx_fire "$sess_payload" "$stub_slow:$PATH")")"
bctx_elapsed=$(( $(date +%s) - bctx_start ))
[ "$bctx_elapsed" -lt 20 ] && got=bounded || got=hung
assert "a hanging gh is bounded well under its 30s sleep (took ${bctx_elapsed}s)" bounded "$got"
case "$ctx" in *'UNKNOWN'*) got=loud ;; *) got=quiet ;; esac
assert "a timed-out gh is reported, not silently dropped" loud "$got"

# --- SILENCE, each paired with the forced case above proving the path can still talk ---------------
bctx_clean_stamps
mp="$sess_payload"
git -C "$bctx_tmp/wt" checkout -q master
out="$(bctx_fire "$mp" "$stub_pr:$PATH")"
[ -z "$out" ] && got=silent || got=spoke
assert "on master there is no inherited branch to describe, so it is silent" silent "$got"
git -C "$bctx_tmp/wt" checkout -q feats/bctx-fixture
bctx_clean_stamps
out="$(bctx_fire "$mp" "$stub_pr:$PATH")"
[ -n "$out" ] && got=spoke || got=silent
assert "...and the same payload speaks again once off master (the pairing)" spoke "$got"

# DETACHED HEAD. GitHub Actions checks PRs out detached, so a case that only passes on a developer
# machine is how remind-inflight-on-push.sh's fixtures went red in CI and nowhere else.
bctx_clean_stamps
git -C "$bctx_tmp/wt" checkout -q --detach HEAD
out="$(bctx_fire "$mp" "$stub_pr:$PATH")"
[ -z "$out" ] && got=silent || got=spoke
assert "a detached checkout has no branch to describe, so it is silent" silent "$got"
git -C "$bctx_tmp/wt" checkout -q feats/bctx-fixture

bctx_clean_stamps
nogit="$(mktemp -d)"
np="$(bctx_payload "$nogit" SessionStart)"
out="$(bctx_fire "$np" "$stub_pr:$PATH")"
[ -z "$out" ] && got=silent || got=spoke
assert "a directory outside any git repo is silent" silent "$got"
rmdir "$nogit" 2>/dev/null

# FAILING OPEN. Every error path prints nothing and exits 0 - a broken reminder must not be a broken
# session, which is the contract every hook in this directory shares.
bctx_clean_stamps
out="$(bctx_fire 'not json at all' "$stub_pr:$PATH")"
[ -z "$out" ] && got=silent || got=spoke
assert "an unparseable payload fails OPEN" silent "$got"
printf '{"tool_name":"Read","tool_input":{"file_path":"/etc/hosts"}}' > "$bctx_tmp/read.json"
out=$(PATH="$stub_pr:$PATH" bash "$BCTX_HOOK" < "$bctx_tmp/read.json" 2>/dev/null); rc=$?
assert "an ordinary tool call the hook has no business with exits 0" 0 "$rc"
[ -z "$out" ] && got=silent || got=spoke
assert "...and says nothing" silent "$got"

# A LOCAL-PATH ORIGIN IS NOT A REPOSITORY. `git clone /path/to/repo` - how a scratch or baseline
# checkout is made - would otherwise yield a slug from the last two path segments and send `gh` after
# a repo that does not exist, naming a plausible wrong one while failing. Paired with the PR case
# above, which proves the same path does resolve a real slug.
bctx_clean_stamps
local_origin="$(mktemp -d)"
bctx_repo "$local_origin/wt"
git -C "$local_origin/wt" remote add origin "$local_origin/some/local/path"
lp="$(bctx_payload "$local_origin/wt" SessionStart)"
ctx="$(bctx_context "$(bctx_fire "$lp" "$stub_pr:$PATH")")"
case "$ctx" in *'is not a remote URL'*) got=accurate ;; *) got=plausible_wrong_repo ;; esac
assert "a local-path origin is reported as having no repository to ask" accurate "$got"
case "$ctx" in *'/some/local/path'*|*'#9412'*) got=invented ;; *) got=clean ;; esac
assert "...and no slug is invented from the path segments" clean "$got"

# A `file://` ORIGIN IS THE SAME LOCAL CLONE, WEARING A SCHEME. The first version of the guard above
# asked only whether the URL contained `://`, so `git clone file:///path/to/repo` - the documented
# way to force a real transport against a local repository - walked through it and produced the very
# `git/parallel-consumer` slug the guard exists to prevent. Found in review of
# astubbs/parallel-consumer#350; the bare-path case above cannot catch it, which is why it is its own
# case rather than another spelling of the same one.
git -C "$local_origin/wt" remote set-url origin "file://$local_origin/some/local/path"
ctx="$(bctx_context "$(bctx_fire "$lp" "$stub_pr:$PATH")")"
case "$ctx" in *'is not a remote URL'*) got=accurate ;; *) got=plausible_wrong_repo ;; esac
assert "a file:// origin is a local clone, not a repository to ask about" accurate "$got"
case "$ctx" in *'/some/local/path'*|*'#9412'*) got=invented ;; *) got=clean ;; esac
assert "...and no slug is invented from its path segments either" clean "$got"
rm -rf "$local_origin"

# --- DISPATCH MODE: the shape of the 2026-08-24 incident -------------------------------------------
# The dispatcher's own cwd is the WRONG branch to describe when it is handing work to another
# worktree, and describing it would be noise - which is how a reminder gets scrolled past.
bctx_clean_stamps
dp="$(bctx_payload "$bctx_dispatcher" PreToolUse Agent "Run a simplify-then-review pass in $bctx_tmp/wt. Changed files: one.txt")"
out="$(bctx_fire "$dp" "$stub_pr:$PATH")"
ctx="$(bctx_context "$out")"
case "$ctx" in *'feats/bctx-fixture'*) got=dispatched_to ;; *) got=wrong_target ;; esac
assert "a dispatch describes the worktree named in its PROMPT" dispatched_to "$got"
case "$ctx" in *'named in the dispatch prompt'*) got=says_which ;; *) got=ambiguous ;; esac
assert "...and says which directory it is describing, so the two cannot be confused" says_which "$got"
# PreToolUse discards raw stdout; only the JSON envelope reaches the model.
case "$out" in *'"additionalContext"'*) got=enveloped ;; *) got=raw ;; esac
assert "a PreToolUse emission is wrapped in the JSON envelope" enveloped "$got"
case "$out" in *'"deny"'*) got=blocked ;; *) got=advisory ;; esac
assert "the branch-context hook never denies a tool call" advisory "$got"
# A PreToolUse hook cannot alter the call it fires on - the tool_use block was composed before the
# hook ran (measured against 2.1.231). Saying so is the difference between a useful late signal and
# a false promise that the dispatch was vetted.
case "$ctx" in *'not before it'*) got=honest ;; *) got=overclaims ;; esac
assert "the dispatch block states that it arrived AFTER the dispatch" honest "$got"

# A PATH AT THE END OF A SENTENCE. A worktree name may contain `.` and `-`, so the match swallows the
# full stop and the directory test then fails - which is the shape a dispatch prompt actually takes,
# and the first thing this resolver got wrong.
bctx_clean_stamps
dp2="$(bctx_payload "$bctx_dispatcher" PreToolUse Agent "Simplify pass in $bctx_tmp/wt.")"
ctx="$(bctx_context "$(bctx_fire "$dp2" "$stub_pr:$PATH")")"
case "$ctx" in *'feats/bctx-fixture'*) got=resolved ;; *) got=lost ;; esac
assert "a worktree path ending a sentence still resolves" resolved "$got"

# The real tool_name is `Agent`; `Task` is the name the documentation and every dispatcher uses.
# Both were measured to reach a matcher, and both must reach this hook.
bctx_clean_stamps
dp3="$(bctx_payload "$bctx_dispatcher" PreToolUse Task "Simplify pass in $bctx_tmp/wt.")"
ctx="$(bctx_context "$(bctx_fire "$dp3" "$stub_pr:$PATH")")"
case "$ctx" in *'feats/bctx-fixture'*) got=handled ;; *) got=missed ;; esac
assert "tool_name 'Task' is handled as well as 'Agent'" handled "$got"

# --- SUBAGENT MODE: the registration that actually closes the incident ------------------------------
# SessionStart does NOT fire for an agent spawned via the Task tool (measured against 2.1.231: the
# subagent shares the dispatcher's session_id and gets no session of its own), so without this the
# subagent could never receive branch context by any route.
bctx_clean_stamps
sub="$(bctx_payload "$bctx_tmp/wt" PreToolUse Bash "" general-purpose bctxfixture01)"
ctx="$(bctx_context "$(bctx_fire "$sub" "$stub_pr:$PATH")")"
case "$ctx" in *'feats/bctx-fixture'*) got=reached ;; *) got=missed ;; esac
assert "a subagent's own tool call carries it the branch context" reached "$got"
# THROTTLED PER agent_id, or it repeats on every Read the subagent makes for the rest of its life.
out="$(bctx_fire "$sub" "$stub_pr:$PATH")"
[ -z "$out" ] && got=throttled || got=repeated
assert "a second tool call from the same agent is throttled" throttled "$got"
# ...and a DIFFERENT agent is not - the pairing that stops the throttle from silently swallowing
# every subagent after the first.
sub2="$(bctx_payload "$bctx_tmp/wt" PreToolUse Bash "" general-purpose bctxfixture02)"
out="$(bctx_fire "$sub2" "$stub_pr:$PATH")"
[ -n "$out" ] && got=spoke || got=swallowed
assert "...but a different agent_id still gets told" spoke "$got"

# --- NO SILENT CAPS. A list that quietly stops is the same failure as a section that quietly vanishes.
bctx_clean_stamps
(
  cd "$bctx_tmp/wt" || exit 1
  i=0
  while [ "$i" -lt 45 ]; do
      echo "$i" > "bulk$i.txt"
      git add "bulk$i.txt"
      git -c user.email=selftest@example.invalid -c user.name=selftest commit -qm "chore(fixture): bulk commit $i"
      i=$((i + 1))
  done
)
ctx="$(bctx_context "$(bctx_fire "$sess_payload" "$stub_pr:$PATH")")"
case "$ctx" in *'NOT LISTED (capped at'*) got=announced ;; *) got=silent_cap ;; esac
assert "a capped commit list announces what it left out" announced "$got"
case "$ctx" in *'Commits on this branch (47)'*) got=true_total ;; *) got=cap_as_total ;; esac
assert "...and the heading counts every commit, not just the listed ones" true_total "$got"

bctx_clean_stamps
rm -rf "$bctx_tmp" "$stub_pr" "$stub_nopr" "$stub_broken" "$stub_slow" "$BCTX_TMPDIR"

if [ "$failures" -eq 0 ]; then
    echo "All .claude/hooks self-tests passed"
    exit 0
fi
echo "$failures self-test(s) FAILED"
exit 1
