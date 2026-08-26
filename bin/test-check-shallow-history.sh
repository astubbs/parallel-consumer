#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Self-test for .claude/hooks/check-shallow-history.sh.
#
# The hook's whole value is PRECISION: it must fire on the queries a shallow clone answers wrongly
# and stay silent on the ones it answers correctly, because a hook that fires on `git status` gets
# switched off and then protects nothing. Most arms here are therefore negative.
#
# IT GUARDS TWO OPPOSITE THINGS, so there are two fixtures. A depth-dependent QUERY is wrong only in
# a clone that is already shallow; a SHALLOWING fetch is only worth stopping in one that is not. Get
# the fixture wrong and an arm passes for the wrong reason - `fire` uses the shallow clone, and
# `fire_full` the full one, and the last two arms below assert each kind stays silent in the other's
# fixture.
set -uo pipefail
# hazard-ok-file: every git command in here is a FIXTURE - a string handed to the hook to judge,
# never executed. Several are literally `git fetch --depth=1`, because the hook's job is to block
# that; check-shell-hazards.sh cannot tell a fixture from a use, and its own self-test carries the
# same exemption for the same reason.
HOOK="$(cd "$(dirname "$0")/.." && pwd)/.claude/hooks/check-shallow-history.sh"
problems=0

# The hook only consults git AFTER its token scan matches, so with a non-shallow clone every call
# exits silently. Force the decision half by pointing it at a scratch repo that IS shallow.
scratch="$(mktemp -d)"; trap 'rm -rf "$scratch"' EXIT
git -C "$scratch" init -q origin-repo 2>/dev/null
( cd "$scratch/origin-repo"
  git config user.email t@t; git config user.name t
  for i in 1 2 3; do echo "$i" > f; git add f; git commit -qm "c$i"; done ) >/dev/null 2>&1
git clone -q --depth=1 "file://$scratch/origin-repo" "$scratch/shallow" 2>/dev/null
# A LINKED WORKTREE OF THE FULL CLONE. Its `.git` is a different path that resolves to the SAME
# common directory - the exact reason this hook exists - so a redirect naming it is not a redirect.
git -C "$scratch/origin-repo" worktree add -q -b wt "$scratch/wt" 2>/dev/null

# ONE PAYLOAD BUILDER FOR ALL THREE HELPERS. python does the quoting because half the arms below are
# about quoting - a fixture carrying a quote, a `;` or a `$(` has to reach the hook exactly as typed.
payload_for() { python3 -c 'import json,sys;print(json.dumps({"tool_input":{"command":sys.argv[1]}}))' "$1"; }

fire() { # <name> <expect fire|silent> <command>
    local name="$1" expect="$2" cmd="$3" out
    out="$(payload_for "$cmd" | ( cd "$scratch/shallow" && bash "$HOOK" ) 2>/dev/null)"
    local got="silent"; [ -n "$out" ] && got="fire"
    if [ "$got" = "$expect" ]; then printf 'ok:   %s\n' "$name"
    else printf 'FAIL: %s (expected %s, got %s)\n' "$name" "$expect" "$got"; problems=$((problems + 1)); fi
}

# The same, in the FULL clone - the fixture the shallowing arms need.
fire_full() { # <name> <expect fire|silent> <command>
    local name="$1" expect="$2" cmd="$3" out
    out="$(payload_for "$cmd" | ( cd "$scratch/origin-repo" && bash "$HOOK" ) 2>/dev/null)"
    local got="silent"; [ -n "$out" ] && got="fire"
    if [ "$got" = "$expect" ]; then printf 'ok:   %s\n' "$name"
    else printf 'FAIL: %s (expected %s, got %s)\n' "$name" "$expect" "$got"; problems=$((problems + 1)); fi
}

# SOME ARMS CARE WHICH HAZARD THE DENY NAMES, not merely that it denied. A chained command that
# reports only its first git command still denies, so `fire` cannot tell the fixed scanner from the
# broken one - the deny text is the only place the second hazard becomes visible.
mentions() { # <name> <command> <needle>...
    local name="$1" cmd="$2"; shift 2
    local out missing="" needle
    out="$(payload_for "$cmd" | ( cd "$scratch/origin-repo" && bash "$HOOK" ) 2>/dev/null)"
    for needle in "$@"; do
        case "$out" in *"$needle"*) ;; *) missing="$missing [$needle]" ;; esac
    done
    if [ -z "$missing" ]; then printf 'ok:   %s\n' "$name"
    else printf 'FAIL: %s (deny did not name:%s)\n' "$name" "$missing"; problems=$((problems + 1)); fi
}

# MUST fire - every one of these answers wrongly from a truncated graft.
fire "rev-list --count is blocked"        fire   'git rev-list --left-right --count origin/master...HEAD'
fire "merge-base is blocked"              fire   'git merge-base HEAD origin/master'
fire "a log RANGE is blocked"             fire   'git log --oneline origin/master..HEAD'
fire "a diff RANGE is blocked"            fire   'git diff origin/master..HEAD --stat'
fire "blame is blocked"                   fire   'git blame README.adoc'
fire "describe is blocked"                fire   'git describe --tags'

# MUST NOT fire - all correct in a shallow clone, and blocking them would make this noise.
fire "git status is allowed"              silent 'git status --short'
fire "a bare git log is allowed"          silent 'git log --oneline -5'
fire "a working-tree diff is allowed"     silent 'git diff --stat'
fire "a staged diff is allowed"           silent 'git diff --staged'
fire "show HEAD is allowed"               silent 'git show HEAD --stat'
# TOKENS, NOT SUBSTRINGS: prose naming a subcommand must not trip it.
fire "prose mentioning rev-list allowed"  silent 'git commit -m "note about rev-list output"'
fire "an unrelated blame allowed"         silent 'echo "do not blame the tool"'
# The escape hatch has to work, or the hook is unbypassable and gets disabled wholesale.
fire "the override is honoured"           silent 'SHALLOW_HISTORY_ACCEPTED=1 git merge-base HEAD origin/master'

# EVERY ARM BELOW IS A BYPASS OR A MISFIRE THAT SHIPPED. A guard whose tokeniser can be walked past
# is worse than no guard: it reports safe. Found by review, not by these tests, which is why they
# exist now.
# A GLOBAL OPTION BEFORE THE SUBCOMMAND. `-C` and `-c` take a separate value, so the value was read
# as the subcommand and nothing matched - including the exact command this hook header cites as
# having reported 836 commits against a true 29.
fire "git -C DIR rev-list is caught"      fire   'git -C /tmp rev-list --left-right --count origin/master...HEAD'
fire "git -c k=v rev-list is caught"      fire   'git -c core.pager=cat rev-list --count a..b'
fire "git --no-pager log RANGE is caught" fire   'git --no-pager log origin/master..HEAD'
# COMMAND SUBSTITUTION. shlex glues `$(` to the next word, so no bare `git` token existed - and this
# is the idiom AGENTS.md itself prescribes for finding the merge base.
fire 'X=$(git merge-base ...) is caught'  fire   'BASE=$(git merge-base HEAD origin/master)'
fire 'echo $(git rev-list) is caught'     fire   'echo $(git rev-list --count origin/master..HEAD)'
# THE OVERRIDE IS A LEADING PREFIX, NOT ANY OCCURRENCE. Searching history for the name of the escape
# hatch used to disable the escape hatch.
fire "override in a -S pattern is caught" fire   'git log -S "SHALLOW_HISTORY_ACCEPTED=1" origin/master..HEAD'
# FALSE POSITIVES - the class ranked as the dominant risk, because a guard that fires on legitimate
# work gets switched off. Both of these were live.
fire "a heredoc mentioning a range ok"    silent 'cat <<EOF
see git log old..new for the story
EOF'
fire "a pathspec containing .. is ok"     silent 'git diff -- ../docs'
fire "prose in an echo is ok"             silent 'echo "run git log a..b next"'

# THE OTHER DIRECTION - the command that CREATES the truncation, in a clone that is still full.
# `bin/check-quarantine-owners.sh` did this on every sweep of bin/check-all.sh; hand-typed, there is
# no script to fix, which is what these arms are for.
fire_full "git fetch --depth=1 is blocked"     fire   'git fetch --quiet --depth=1 origin master'
fire_full "git fetch --depth 1 is blocked"     fire   'git fetch --depth 1 origin master'
fire_full "git pull --depth is blocked"        fire   'git pull --depth=1 origin master'
fire_full "--shallow-since is blocked"         fire   'git fetch --shallow-since=2026-01-01 origin master'
fire_full "a global option cannot hide it"     fire   'git -c core.pager=cat fetch --depth=1 origin master'
# NOT THE HAZARD. `--git-dir` names another repository - the sanctioned way to fetch a ref you only
# want to read; a clone owns its own depth; and an unrestricted fetch writes no `shallow` file.
fire_full "--git-dir elsewhere is allowed"     silent 'git --git-dir=/tmp/x fetch --depth=1 https://h/r ref'
# The same redirect as an assignment prefix - what bin/check-quarantine-owners.sh actually writes,
# so denying it would block the alternative this hook's own deny message recommends.
fire_full "GIT_DIR= prefix is allowed"         silent 'GIT_DIR=/tmp/x git fetch --depth=1 https://h/r ref'
fire_full "an unrelated env prefix is not"     fire   'FOO=bar git fetch --depth=1 origin master'
fire_full "git clone --depth=1 is allowed"     silent 'git clone -q --depth=1 https://h/r dir'
fire_full "an undepthed fetch is allowed"      silent 'git fetch --no-tags origin master'
fire_full "--unshallow is allowed"             silent 'git fetch --unshallow origin'
fire_full "the override is honoured"           silent 'SHALLOW_HISTORY_ACCEPTED=1 git fetch --depth=1 origin master'
fire_full "prose about it does not fire"       silent 'echo "never git fetch --depth=1 in a worktree"'
# THE TWO KINDS ARE GATED OPPOSITELY, and each must stay silent in the other's fixture - otherwise an
# arm above could be passing on the wrong condition entirely.
fire_full "a history query is fine when full"  silent 'git merge-base HEAD origin/master'
fire      "shallowing an already-shallow clone is a no-op" silent 'git fetch --depth=1 origin master'

# CHAINED GIT COMMANDS. The scanner reported the FIRST git call it matched and stopped, and the two
# directions want opposite answers from --is-shallow-repository - so the query's verdict was used to
# clear the fetch, and the chain re-shallowed the clone with the guard reporting safe.
fire_full "a fetch chained after a query" fire 'git merge-base HEAD origin/master; git fetch --depth=1 origin master'
fire      "a query chained after a fetch" fire 'git fetch --depth=1 origin master && git merge-base HEAD origin/master'
# ...and the deny has to NAME both, or a reader fixes the half it was told about and re-runs the rest.
mentions  "the chained deny names both" 'git fetch --depth=1 origin master && git merge-base HEAD origin/master' \
          'TWO SEPARATE HAZARDS' 'git fetch --depth=1' 'git merge-base'
# Chaining must not INVENT a hazard either: a redirected fetch truncates nothing, so the query after
# it is still answering from a full clone.
fire_full "a redirected fetch taints nothing" silent \
          'git --git-dir=/tmp/x fetch --depth=1 https://h/r ref && git merge-base HEAD origin/master'

# A REDIRECT IS TRUSTED ONLY AFTER IT IS RESOLVED. `--git-dir` was exempt on its spelling alone, so
# naming THIS clone's own git directory - which is precisely where the shared `shallow` file lives -
# was a one-flag bypass of the arm above.
fire_full "--git-dir=.git is not a redirect"         fire 'git --git-dir=.git fetch --depth=1 origin master'
fire_full "GIT_DIR=.git is not a redirect"           fire 'GIT_DIR=.git git fetch --depth=1 origin master'
fire_full "--git-dir naming this clone"    fire "git --git-dir=$scratch/origin-repo/.git fetch --depth=1 origin master"
fire_full "--git-dir with a separate value is caught" fire 'git --git-dir .git fetch --depth=1 origin master'
# A SIBLING WORKTREE'S `.git` IS A DIFFERENT PATH AND THE SAME REPOSITORY. Comparing the spelling, or
# even the git dir, would clear this one - it is the COMMON dir that holds the `shallow` file, and
# truncating through a sibling is precisely the incident this hook was written for.
fire_full "a sibling worktree's .git is caught"      fire "git --git-dir=$scratch/wt/.git fetch --depth=1 origin master"
# A target only the shell can expand is judged on its text, so the two spellings that name THIS
# clone through a substitution stay denied while an opaque one is allowed.
fire_full "a \$PWD/.git redirect is caught"           fire 'git --git-dir="$PWD/.git" fetch --depth=1 https://h/r ref'
fire_full "a rev-parse redirect is caught" fire \
          'git --git-dir="$(git rev-parse --git-common-dir)" fetch --depth=1 https://h/r ref'
# The genuine article still has to pass, or the hook denies the alternative its own message names -
# and bin/check-quarantine-owners.sh writes the second of these.
fire_full "a mktemp scratch dir is ok"  silent 'git --git-dir="$(mktemp -d)" fetch --depth=1 https://h/r ref'
fire_full "a \$scratch_dir GIT_DIR is ok" silent 'GIT_DIR="$scratch_dir/preview" git fetch --depth=1 https://h/r ref'

# COMMAND WRAPPERS. `git` was required to sit in command position, so one word in front of it -
# `command`, `env`, `time`, `nohup`, `stdbuf`, `xargs`, `sudo`, `nice`, `ionice`, `setsid` - skipped
# the whole hook, the pre-existing query arm included.
fire_full "command git fetch --depth is caught"      fire 'command git fetch --depth=1 origin master'
fire_full "env git fetch --depth is caught"          fire 'env git fetch --depth=1 origin master'
fire_full "nohup git fetch --depth is caught"        fire 'nohup git fetch --depth=1 origin master'
fire_full "sudo -u ci git fetch --depth is caught"   fire 'sudo -u ci git fetch --depth=1 origin master'
fire_full "nice -n 10 git fetch --depth is caught"   fire 'nice -n 10 git fetch --depth=1 origin master'
fire_full "stacked wrappers cannot hide it"          fire 'env command git fetch --depth=1 origin master'
fire      "time git merge-base is caught"            fire 'time git merge-base HEAD origin/master'
fire      "stdbuf -oL git rev-list is caught"        fire 'stdbuf -oL git rev-list --count a..b'
fire      "xargs -I {} git blame is caught"          fire 'xargs -I {} git blame README.adoc'
# `env -i` takes NO value: a shared option table would have it swallow the very `git` it looks for.
fire_full "timeout 60 git fetch is caught"  fire 'timeout 60 git fetch --depth=1 origin master'
# A SUBSHELL GLUES ITS `(` TO THE COMMAND WORD, which is the wrapper defect by another route.
fire_full "a subshell cannot hide it"       fire '(git fetch --depth=1 origin master)'
fire      "a subshell around a query too"   fire '(git merge-base HEAD origin/master)'
fire      "env -i git merge-base is caught"          fire 'env -i git merge-base HEAD origin/master'
# Walking wrappers must not widen what counts as a call: these are still prose and a non-git command.
fire      "a heredoc naming a wrapper is ok"         silent 'cat <<EOF
run command git blame README.adoc yourself
EOF'
fire      "a wrapper running echo is ok"              silent 'command echo git blame README.adoc'
# Unspaced control operators are padded before tokenising so `...master; git fetch --depth=1` is seen
# at all - a quoted one must still be text, or every commit message describing a command denies.
fire      "a ; inside a commit message is ok"        silent 'git commit -m "fix; then git blame f"'

if [ "$problems" -gt 0 ]; then
    echo "check-shallow-history self-test: $problems failure(s)" >&2; exit 1
fi
echo; echo "All check-shallow-history self-tests passed"
