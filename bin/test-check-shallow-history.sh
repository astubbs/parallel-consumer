#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# Self-test for .claude/hooks/check-shallow-history.sh.
#
# The hook's whole value is PRECISION: it must fire on the queries a shallow clone answers wrongly
# and stay silent on the ones it answers correctly, because a hook that fires on `git status` gets
# switched off and then protects nothing. Most arms here are therefore negative.
set -uo pipefail
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

fire() { # <name> <expect fire|silent> <command>
    local name="$1" expect="$2" cmd="$3" out
    out="$(printf '%s' "{\"tool_input\":{\"command\":$(python3 -c 'import json,sys;print(json.dumps(sys.argv[1]))' "$cmd")}}" \
        | ( cd "$scratch/shallow" && bash "$HOOK" ) 2>/dev/null)"
    local got="silent"; [ -n "$out" ] && got="fire"
    if [ "$got" = "$expect" ]; then printf 'ok:   %s\n' "$name"
    else printf 'FAIL: %s (expected %s, got %s)\n' "$name" "$expect" "$got"; problems=$((problems + 1)); fi
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
fire "X=$(git merge-base ...) is caught"  fire   'BASE=$(git merge-base HEAD origin/master)'
fire "echo $(git rev-list) is caught"     fire   'echo $(git rev-list --count origin/master..HEAD)'
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

if [ "$problems" -gt 0 ]; then
    echo "check-shallow-history self-test: $problems failure(s)" >&2; exit 1
fi
echo; echo "All check-shallow-history self-tests passed"
