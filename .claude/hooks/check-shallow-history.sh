#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# REFUSES A HISTORY QUERY WHILE THE CLONE IS SHALLOW.
#
# A shallow clone does not error on `git log`, `git merge-base` or `git rev-list` - it ANSWERS, from
# the truncated graft it has, and the answer is confidently wrong. Measured on one session:
# `git rev-list --left-right --count origin/master...HEAD` reported 836 and later 831 branch commits
# against a true 29, and `git merge-base` returned a commit that was not the merge base. Nothing goes
# red. That is the whole reason this is a hook and not a rule: a wrong number does not announce
# itself, and the reader has no way to tell it from a right one.
#
# WHY IT KEEPS HAPPENING HERE. The `shallow` file lives in the shared `--git-common-dir`, so it is
# common to EVERY worktree. One sibling agent doing a depth-limited fetch in any worktree re-shallows
# all of them, including one that unshallowed itself a minute earlier. Re-checking once at session
# start is therefore not enough; it has to be per-command.
#
# WHY DENY RATHER THAN WARN. A warning on stderr is read after the command has already produced its
# wrong answer, and by then the number is in the transcript being reasoned about. Denying costs one
# `git fetch --unshallow`; a wrong merge-base costs a `git reset` against the wrong commit, which is
# how master content gets silently reverted during a re-cut.
#
# SCOPED TO COMMANDS WHOSE ANSWER ACTUALLY DEPENDS ON DEPTH. `git status`, `git diff` of the working
# tree, `git show HEAD` and `git log -1` are all correct in a shallow clone, and blocking them would
# make this noise - which gets hooks disabled. Only ranges, ancestry and whole-history walks qualify.
set -uo pipefail

payload="$(cat 2>/dev/null || true)"
[ -n "$payload" ] || exit 0

# Cheap bail before paying for python3 on every Bash call - the pre-filter may only skip work, never
# decide. Any of these tokens MIGHT be a depth-dependent query; the scan below decides whether it is.
# Any mention of git at all goes to the scanner. The earlier filter looked for the CONTIGUOUS strings
# "git log" and "git diff", which `git -C DIR log` and `git --no-pager log` never contain - so the
# cheap pre-filter was silently DECIDING, which the rule below forbids. It may only skip work.
case "$payload" in
    *git*) ;;
    *) exit 0 ;;
esac

# Honours the variable when it really is exported into this hook's environment; the command-prefix
# form - which is what the deny message tells you to type - is handled in the token scan below,
# because a prefix on the inspected command never reaches this process.
[ "${SHALLOW_HISTORY_ACCEPTED:-}" = "1" ] && exit 0

# FAIL LOUDLY, NOT OPEN. `set -uo pipefail` without -e means a missing python3 leaves `verdict` empty
# and the guard allows - a guard that silently stops guarding when a dependency vanishes is the exact
# class it exists to police. The sibling pre-commit-gate.sh guards the same dependency explicitly.
command -v python3 >/dev/null 2>&1 || {
    echo "check-shallow-history: python3 not found - this guard CANNOT RUN and is not passing." >&2
    exit 0
}

# Only pay for the git call if the command really is a history query. TOKENS, NOT SUBSTRINGS - the
# rule the sibling hooks state - so `git commit -m "rev-list notes"` does not fire. An unbalanced
# quote makes shlex raise, and that fails open.
verdict="$(printf '%s' "$payload" | python3 -c '
import json, re, shlex, sys

DEPTH_DEPENDENT = {"rev-list", "merge-base", "blame", "describe", "bisect", "shortlog", "cherry"}

# git own options that sit BEFORE the subcommand. The two that take a separate value must consume it,
# or the value is mistaken for the subcommand - which is how `git -C DIR rev-list` read as subcommand
# "DIR" and sailed straight through.
GLOBAL_WITH_VALUE = {"-C", "-c", "--exec-path", "--git-dir", "--work-tree", "--namespace"}

# A token that ends a command, so the NEXT word is in command position. Anything else preceding
# `git` means the word is prose - a heredoc body, an echo argument, a commit message - not a call.
SEPARATORS = {";", "&&", "||", "|", "(", "{", "&", "then", "do", "else", "!"}

def report(v):
    print(v)
    sys.exit(0)

try:
    data = json.load(sys.stdin)
    cmd = data.get("tool_input", {}).get("command", "")
    # shlex glues `$(` onto the following word, so `X=$(git log a..b)` never yields a bare `git`
    # token. Pad the substitution openers so the command inside is tokenised as a command.
    cmd = cmd.replace("$(", "$( ").replace("`", " ` ")
    toks = shlex.split(cmd)
except Exception:
    sys.exit(0)

def in_command_position(i):
    if i == 0:
        return True
    prev = toks[i - 1]
    if prev in SEPARATORS:
        return True
    # `X=$( git ...` and a leading `VAR=value git ...` assignment prefix
    if prev.endswith("(") or re.match(r"^[A-Za-z_][A-Za-z_0-9]*=", prev):
        return True
    return False

# THE OVERRIDE IS A LEADING ASSIGNMENT PREFIX, not any occurrence. Matching it anywhere meant
# `git log -S "SHALLOW_HISTORY_ACCEPTED=1"` - searching history for the name of the escape hatch -
# disabled the guard.
for i, t in enumerate(toks):
    if t == "SHALLOW_HISTORY_ACCEPTED=1" and (i == 0 or toks[i - 1] in SEPARATORS or toks[i - 1].endswith("(")):
        sys.exit(0)

for i, t in enumerate(toks):
    if t != "git" or not in_command_position(i):
        continue
    rest = toks[i + 1:]
    # walk past git own global options to find the real subcommand
    j = 0
    while j < len(rest):
        r = rest[j]
        if r in GLOBAL_WITH_VALUE:
            j += 2
            continue
        if r.startswith("-"):
            j += 1
            continue
        break
    sub = rest[j] if j < len(rest) else None
    args = rest[j + 1:]
    # a pathspec after `--` is a FILE PATH: `git diff -- ../docs` is not a commit range
    if "--" in args:
        args = args[:args.index("--")]
    if sub in DEPTH_DEPENDENT:
        report("git " + sub)
    if sub in ("log", "diff") and any(".." in a and not a.startswith("-") for a in args):
        report("git " + sub + " over a commit range")
sys.exit(0)
')"

[ -n "$verdict" ] || exit 0

# Now, and only now, ask git. Cheap, but not free enough to run on every Bash call.
[ "$(git rev-parse --is-shallow-repository 2>/dev/null)" = "true" ] || exit 0

export VERDICT="$verdict"
python3 -c '
import json, os
print(json.dumps({"hookSpecificOutput": {"hookEventName": "PreToolUse",
    "permissionDecision": "deny",
    "permissionDecisionReason": (
        "THE CLONE IS SHALLOW, and `" + os.environ["VERDICT"] + "` will answer anyway - from the "
        "truncated graft, without erroring. Measured here: a branch reported 836 commits against a "
        "true 29, and merge-base returned a commit that was not the merge base.\n\n"
        "Run `git fetch --unshallow` first, then re-run. If you are mid-merge-prep this is not "
        "optional: `git reset --mixed $(git merge-base ...)` against a wrong base silently reverts "
        "whatever master gained.\n\n"
        "It re-shallows because the `shallow` file lives in the shared --git-common-dir, so any "
        "sibling worktree doing a depth-limited fetch re-shallows this one too. Expect to need it "
        "more than once in a session.\n\n"
        "If the truncated answer genuinely does not matter, re-run prefixed with "
        "SHALLOW_HISTORY_ACCEPTED=1.")}}))
'
exit 0
