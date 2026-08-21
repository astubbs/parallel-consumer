#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Self-test for the four hooks in `.claude/hooks/`. Feeds each one a crafted hook payload on stdin
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

verdict() { # <bash-command> -> ALLOW | DENY
    # The command goes to the JSON builder on STDIN, never argv: one case here is a 150 KB command,
    # and passing that as an argument hits the same E2BIG the case exists to detect - the harness
    # would die and the failure would read as the hook's.
    local out tmp
    tmp=$(mktemp)
    printf '%s' "$1" | python3 -c 'import json,sys; print(json.dumps({"tool_name":"Bash","tool_input":{"command":sys.stdin.read()}}))' > "$tmp"
    out=$("$HOOKS/check-squash-subject.sh" < "$tmp" 2>/dev/null)
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
# warn-low-disk.sh
#
# This hook has an unusual failure mode: its correct behaviour on a healthy machine is to print
# NOTHING, which is byte-identical to it being broken, misconfigured, or not running at all. So
# almost every case here forces it to speak, and the one case that asserts silence pairs with a
# case proving the same invocation can be made to talk.
#
# Every case pins PC_DISK_STATE_DIR into this test's own mktemp directory. Without that, the
# throttle state is shared with the live session running the test, and cases would pass or fail
# depending on whether a real warning had fired in the last ten minutes.
#
# The Docker Desktop cases pin PC_DISK_UNAME and a fake disk image, so they exercise the sparse-file
# branch on a Linux CI runner too. They deliberately do NOT fake `stat`: the hook resolves stat
# syntax from the real uname precisely because a forced platform would otherwise read filesystem
# blocks instead of file blocks and get a wrong number rather than an error.
# ---------------------------------------------------------------------------------------------

DISK_HOOK="$HOOKS/warn-low-disk.sh"

# EVERY case pins ALL FOUR thresholds, and each then raises exactly the one it is about. An earlier
# version left the host thresholds at their defaults, so the cases silently depended on how much free
# space the machine running them happened to have - and three of them flipped to failing mid-session
# when this host dropped below the 25 GiB default warn line. A self-test for a disk warner must not
# be a function of the disk. `env` applies assignments in order, so a caller's override wins.
DISK_ALL_QUIET="PC_DISK_HOST_WARN_GIB=0 PC_DISK_HOST_CRIT_GIB=0 PC_DISK_VM_WARN_GIB=0 PC_DISK_VM_CRIT_GIB=0"

disk_band() { # <VAR=value>... -> SILENT | WARN | CRITICAL | MALFORMED | BLOCKED:<rc>
    local out rc state
    state="$(mktemp -d "$TMP/disk.XXXXXX")"
    out="$(echo '{"tool_input":{"command":"echo hi"}}' |
        env PC_DISK_STATE_DIR="$state" $DISK_ALL_QUIET "$@" "$DISK_HOOK" 2>/dev/null)"
    rc=$?
    # A PreToolUse hook exiting non-zero can take the tool call away. This one never may.
    [ "$rc" -ne 0 ] && { echo "BLOCKED:$rc"; return; }
    [ -z "$out" ] && { echo SILENT; return; }
    printf '%s' "$out" | python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)["hookSpecificOutput"]
except Exception:
    print("MALFORMED"); raise SystemExit
if d.get("permissionDecision") != "allow":
    print("NOT_ALLOW:" + str(d.get("permissionDecision"))); raise SystemExit
ctx = d.get("additionalContext", "")
if ctx.startswith("DISK CRITICAL."):
    print("CRITICAL")
elif ctx.startswith("Disk running low."):
    print("WARN")
else:
    print("UNRECOGNISED:" + ctx[:40])
'
}

disk_context() { # <VAR=value>... -> the additionalContext string, or empty
    local state
    state="$(mktemp -d "$TMP/disk.XXXXXX")"
    echo '{"tool_input":{"command":"echo hi"}}' |
        env PC_DISK_STATE_DIR="$state" $DISK_ALL_QUIET "$@" "$DISK_HOOK" 2>/dev/null |
        python3 -c 'import json,sys
try: print(json.load(sys.stdin)["hookSpecificOutput"]["additionalContext"])
except Exception: pass'
}

# A fake Docker Desktop: an empty disk image (so allocated rounds to 0 GiB) plus a settings file
# naming the ceiling, which makes free-space arithmetic exact regardless of the host's real disk.
make_fake_docker() { # <ceiling-MiB> -> directory
    local dir; dir="$(mktemp -d "$TMP/dockerfake.XXXXXX")"
    : >"$dir/Docker.raw"
    printf '{"DiskSizeMiB": %s}\n' "$1" >"$dir/settings.json"
    echo "$dir"
}

# The default state of a working machine. Paired with the forced cases below, which prove the same
# call path can be made to speak - silence here is therefore a verdict, not an absence.
assert "a disk above every threshold says nothing at all" SILENT "$(disk_band)"

# Negative controls: the guard must be able to fire, in both bands.
assert "host below the warn threshold warns" \
    WARN "$(disk_band PC_DISK_HOST_WARN_GIB=99999999)"
assert "host below the critical threshold escalates" \
    CRITICAL "$(disk_band PC_DISK_HOST_CRIT_GIB=99999999)"

# The single most important property: a disk warner that blocked Bash would remove the commands
# needed to clear the disk. Asserted at the worst band, where the temptation to block would be
# greatest.
assert "even at critical it allows the call" \
    CRITICAL "$(disk_band PC_DISK_HOST_CRIT_GIB=99999999)"

# A bogus CLAUDE_PROJECT_DIR falls back to the working directory and still measures something real.
# Asserted so the fallback is a documented decision rather than an accident nobody noticed.
assert "a bogus project dir falls back to the working directory" \
    WARN "$(disk_band CLAUDE_PROJECT_DIR=/nonexistent/path/that/cannot/exist PC_DISK_HOST_WARN_GIB=99999999)"

# ...but when the disk CANNOT be read, silence is the only honest answer. The failure this rules
# out is the dangerous one: a hook that fails to measure and therefore says nothing is byte-identical
# to a hook reporting a healthy disk, so the guard must be the ABSENCE of a reading, never a default
# of "fine". Shadowing `df` on PATH asks exactly that question - an earlier version of this case
# emptied PATH entirely, which stopped `/usr/bin/env bash` finding an interpreter and tested only
# that a script which never ran printed nothing.
shadow_df() { # <body> -> a PATH prefix whose `df` behaves as given
    local dir; dir="$(mktemp -d "$TMP/shadow.XXXXXX")"
    { echo '#!/bin/sh'; echo "$1"; } >"$dir/df"
    chmod +x "$dir/df"
    echo "$dir"
}

for scenario in "exit 1::a df that fails" "echo garbage::a df that answers unparseably" "printf ''::a df that answers nothing"; do
    body="${scenario%%::*}"; label="${scenario##*::}"
    dir="$(shadow_df "$body")"
    out="$(echo '{}' | env PATH="$dir:$PATH" \
        PC_DISK_STATE_DIR="$(mktemp -d "$TMP/disk.XXXXXX")" $DISK_ALL_QUIET \
        PC_DISK_HOST_WARN_GIB=99999999 "$DISK_HOOK" 2>/dev/null; echo "rc=$?")"
    case "$out" in
        "rc=0") got=silent_and_clean ;;
        rc=*)   got="non-zero exit: $out" ;;
        *)      got="claimed a reading it never took: $out" ;;
    esac
    assert "$label leaves it silent, exiting 0" silent_and_clean "$got"
done

# The positive control for the three above: the SAME stub mechanism with a plausible answer must
# produce a warning. Without this, all three could be passing because the stub broke the invocation.
dir_ok="$(shadow_df 'echo "Filesystem 1024-blocks Used Available Capacity Mounted"; echo "/dev/fake 100000000 99000000 1048576 99% /"')"
out_ok="$(echo '{}' | env PATH="$dir_ok:$PATH" \
    PC_DISK_STATE_DIR="$(mktemp -d "$TMP/disk.XXXXXX")" $DISK_ALL_QUIET \
    PC_DISK_HOST_WARN_GIB=25 "$DISK_HOOK" 2>/dev/null)"
case "$out_ok" in *'Host volume: 1 GiB free'*) got=read_the_stub ;; *) got="did not read it: $out_ok" ;; esac
assert "a stubbed df reporting 1 GiB free does warn" read_the_stub "$got"

# THE HIGH-WATER-MARK CORRECTION. Docker Desktop's disk image never shrinks, so after a prune it
# still claims the space. Without this correction the hook nags for days about space that is free.
# The image claims 18 of a 20 GiB ceiling, leaving 2 GiB - comfortably under a realistic 12 GiB
# threshold, so the cheap signal trips. Docker then reports it is really holding 2 GiB, so there are
# 18 GiB actually free and the alarm is stale.
fake="$(make_fake_docker 20480)"                     # a 20 GiB ceiling
state_pruned="$(mktemp -d "$TMP/disk.XXXXXX")"
echo "2GB" >"$state_pruned/docker-df"                # ...of which docker really holds 2 GiB
out_pruned="$(echo '{}' | env \
    PC_DISK_STATE_DIR="$state_pruned" $DISK_ALL_QUIET PC_DISK_UNAME=Darwin \
    PC_DISK_DESKTOP_RAW="$fake/Docker.raw" PC_DISK_DESKTOP_SETTINGS="$fake/settings.json" \
    PC_DISK_VM_ALLOC_GIB=18 PC_DISK_VM_WARN_GIB=12 PC_DISK_VM_CRIT_GIB=5 "$DISK_HOOK" 2>/dev/null)"
[ -z "$out_pruned" ] && got=suppressed || got="warned anyway"
assert "a VM alarm that a prune already cleared is suppressed" suppressed "$got"

# ...and the other half, which is what stops the correction from becoming a blanket mute.
state_full="$(mktemp -d "$TMP/disk.XXXXXX")"
echo "19GB" >"$state_full/docker-df"                 # docker really is holding 19 of 20 GiB
out_full="$(echo '{}' | env \
    PC_DISK_STATE_DIR="$state_full" $DISK_ALL_QUIET PC_DISK_UNAME=Darwin \
    PC_DISK_DESKTOP_RAW="$fake/Docker.raw" PC_DISK_DESKTOP_SETTINGS="$fake/settings.json" \
    PC_DISK_VM_ALLOC_GIB=18 PC_DISK_VM_WARN_GIB=12 PC_DISK_VM_CRIT_GIB=0 "$DISK_HOOK" 2>/dev/null)"
case "$out_full" in *'~1 GiB headroom of 20 GiB'*) got=reported ;; *) got="not reported: $out_full" ;; esac
assert "a VM genuinely near full still warns, with the corrected figure" reported "$got"

# `docker system df` reports logical sizes that double-count shared layers, so corrected usage can
# exceed the ceiling. The user must never be shown a negative headroom.
state_over="$(mktemp -d "$TMP/disk.XXXXXX")"
echo "500GB" >"$state_over/docker-df"
out_over="$(echo '{}' | env \
    PC_DISK_STATE_DIR="$state_over" $DISK_ALL_QUIET PC_DISK_UNAME=Darwin \
    PC_DISK_DESKTOP_RAW="$fake/Docker.raw" PC_DISK_DESKTOP_SETTINGS="$fake/settings.json" \
    PC_DISK_VM_ALLOC_GIB=18 PC_DISK_VM_WARN_GIB=12 PC_DISK_VM_CRIT_GIB=5 "$DISK_HOOK" 2>/dev/null)"
case "$out_over" in *-[0-9]*GiB*) got="negative headroom shown" ;; *) got=clamped ;; esac
assert "over-reported docker usage cannot print negative headroom" clamped "$got"

# THROTTLE. Firing on every Bash call is what makes the warning cheap to ignore.
state_throttle="$(mktemp -d "$TMP/disk.XXXXXX")"
first="$(echo '{}' | env PC_DISK_STATE_DIR="$state_throttle" $DISK_ALL_QUIET PC_DISK_HOST_WARN_GIB=99999999 "$DISK_HOOK" 2>/dev/null)"
second="$(echo '{}' | env PC_DISK_STATE_DIR="$state_throttle" $DISK_ALL_QUIET PC_DISK_HOST_WARN_GIB=99999999 "$DISK_HOOK" 2>/dev/null)"
[ -n "$first" ] && [ -z "$second" ] && got=throttled || got="first=${#first} second=${#second}"
assert "the same band does not repeat on the next call" throttled "$got"

# ...but getting worse must beat the throttle, or the escalation nobody wants to miss is the one
# guaranteed to be swallowed.
third="$(echo '{}' | env PC_DISK_STATE_DIR="$state_throttle" $DISK_ALL_QUIET PC_DISK_HOST_CRIT_GIB=99999999 "$DISK_HOOK" 2>/dev/null)"
case "$third" in *'DISK CRITICAL.'*) got=escalated ;; *) got="swallowed" ;; esac
assert "worsening from warn to critical speaks through the throttle" escalated "$got"

# CROSS-PLATFORM. A platform whose Docker layout is unknown still reports the host volume, and must
# not invent or imply a Docker figure it never read.
ctx_unknown="$(disk_context PC_DISK_UNAME=Plan9 PC_DISK_HOST_WARN_GIB=99999999)"
case "$ctx_unknown" in
    *'Host volume:'*'Docker'*) got="claims a docker reading" ;;
    *'Host volume:'*)          got=host_only ;;
    *)                         got="no host reading: $ctx_unknown" ;;
esac
assert "an unknown platform reports the host volume and no Docker figure" host_only "$got"

# On Linux the engine writes into the host filesystem, so when that is the SAME mount the host
# check already covers it and a second figure would be noise.
ctx_linux_same="$(disk_context PC_DISK_UNAME=Linux PC_DISK_DOCKER_ROOT="$TMP" PC_DISK_HOST_WARN_GIB=99999999)"
case "$ctx_linux_same" in
    *'Docker data filesystem'*) got="reported twice" ;;
    *'Host volume:'*)           got=not_duplicated ;;
    *)                          got="no host reading" ;;
esac
assert "Linux docker root on the project's own mount is not reported twice" not_duplicated "$got"

echo
if [ "$failures" -eq 0 ]; then
    echo "All .claude/hooks self-tests passed"
    exit 0
fi
echo "$failures self-test(s) FAILED"
exit 1
