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

if [ "$failures" -eq 0 ]; then
    echo "All .claude/hooks self-tests passed"
    exit 0
fi
echo "$failures self-test(s) FAILED"
exit 1
