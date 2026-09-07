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
#
# IT ALSO GUARDS THE OTHER DIRECTION: the command that MAKES the clone shallow. `git fetch --depth`
# and `git pull --depth` write that shared `shallow` file, and the shallowing is silent - the fetch
# succeeds, and the bill arrives later in some other worktree's merge-base. The script that did this
# on every gate sweep was bin/check-quarantine-owners.sh, fixed by fetching into a throwaway git dir;
# a hand-typed `--depth=1` is the same defect with no script to fix, which is what this arm is for.
# bin/check-shell-hazards.sh enforces the same rule on scripts in bin/ and .claude/hooks/.
#
# EVERY GIT CALL IN THE COMMAND IS JUDGED, NOT THE FIRST ONE THAT MATCHED. The scan used to print one
# verdict and stop, and the two directions want OPPOSITE answers from `--is-shallow-repository` - so
# `git merge-base HEAD origin/master; git fetch --depth=1 origin master` was classified as a QUERY,
# found the clone was not shallow, and ALLOWED the fetch that re-shallowed it. The mirror case hid
# the query behind a fetch. The state is still read once; each collected match is tested against it
# separately, and a fetch that will truncate the clone also arms every query that follows it in the
# same command, because by then the graft is already cut.
#
# A `--git-dir=` OR `GIT_DIR=` REDIRECT EXEMPTS IT ONLY ONCE THE TARGET IS RESOLVED AND SHOWN TO BE
# ANOTHER REPOSITORY - that idiom is the sanctioned way to fetch a ref you only want to read, and it
# is what this hook's own deny message recommends, so it has to keep working. Trusting the SPELLING
# made the option a one-flag bypass: `git --git-dir=.git fetch --depth=1` (and the `GIT_DIR=.git`
# form) names THIS clone, and writes THIS clone's shared `shallow` file. The target's common
# directory is now compared with ours, and an exemption that cannot be verified is not granted.
# `git clone --depth` is not the hazard either: a clone owns its own depth.
#
# WRAPPERS ARE WALKED THROUGH. `git` had to sit in command position, so one word in front of it -
# `command`, `env`, `time`, `timeout`, `nohup`, `stdbuf`, `xargs`, `sudo`, `nice`, `ionice`,
# `setsid` - skipped the entire hook, both directions, including the query arm that long predates
# the fetch arm. The same defect reached the tokeniser: shlex splits on whitespace only, so an
# unspaced `;`, `&` or `|` and a subshell's `(` glue to the neighbouring word and hid the call just
# as effectively. Both are handled where they arise, in the scan below.
#
# THREE KNOWN, DELIBERATE IMPRECISIONS, and the third is the only one that errs towards allowing:
#
#   - `-C <dir>` is NOT read as a redirect, so `git -C /some/other/repo fetch --depth=1` is denied
#     even though it is harmless. Treating it as one would be a bypass, since `-C .` names THIS
#     repository; over-blocking costs a prefixed re-run, under-blocking costs the incident above.
#   - Once the clone is already shallow this arm stands down entirely, so a fetch that CHANGES the
#     depth (`--depth=5` over a depth-1 clone, a different `--shallow-since`) still rewrites the
#     shared graft. That is accepted rather than missed: the residual is bounded, because in a clone
#     that is already shallow the QUERY arm above is armed, so no truncated answer reaches anyone
#     silently - and denying depth changes in an intentionally shallow CI clone is the noise that
#     gets hooks switched off.
#   - A redirect target only the shell can expand - `--git-dir="$(mktemp -d)"`, or the
#     `GIT_DIR="$scratch_dir/preview"` that bin/check-quarantine-owners.sh writes - cannot be
#     resolved without RUNNING it, which a guard inspecting an unapproved command must never do. It
#     is judged on its text instead: exempt unless the text could name a git directory of this clone.
#     So `--git-dir="$mystery"` is allowed while `--git-dir="$PWD/.git"` is not, and the residual is
#     a target that reaches this clone through a variable naming none of it. Failing closed here
#     instead would deny the alternative the deny message names, which is how a guard gets disabled.
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
# quote makes shlex raise, and that fails open. One record per match, in command order: KIND, the
# `--git-dir`/`GIT_DIR` target as written, and the text the deny message quotes. THE SEPARATOR IS
# US (\x1f), not a tab, because tab is an IFS *whitespace* character: `read` collapses a run of them,
# so a record with no redirect target silently shifted its description one field left and every
# QUERY verdict came back empty - a guard that allowed everything while reporting nothing.
verdicts="$(printf '%s' "$payload" | python3 -c '
import json, re, shlex, sys

DEPTH_DEPENDENT = {"rev-list", "merge-base", "blame", "describe", "bisect", "shortlog", "cherry"}

# The other direction: these do not READ a truncated history, they CREATE one, for every worktree.
SHALLOWING = {"fetch", "pull"}
DEPTH_FLAGS = ("--depth", "--shallow-since", "--shallow-exclude")

# git own options that sit BEFORE the subcommand. The two that take a separate value must consume it,
# or the value is mistaken for the subcommand - which is how `git -C DIR rev-list` read as subcommand
# "DIR" and sailed straight through.
GLOBAL_WITH_VALUE = {"-C", "-c", "--exec-path", "--git-dir", "--work-tree", "--namespace"}

# A token that ends a command, so the NEXT word is in command position. Anything else preceding
# `git` means the word is prose - a heredoc body, an echo argument, a commit message - not a call.
SEPARATORS = {";", "&&", "||", "|", "(", "{", "&", "then", "do", "else", "!"}

# A WRAPPER RUNS ITS TAIL AS A COMMAND, so `command git fetch --depth=1` is a git call with a word in
# front of it - and every name here used to put `git` out of command position and skip the whole
# hook. Each maps to ITS OWN options that take a SEPARATE value, which have to be stepped over or
# `nice -n 10 git ...` stops on "10" and the call is missed again. Per-wrapper on purpose: `env -i`
# takes no value while `stdbuf -i` does, so one shared list would have `env -i git fetch --depth=1`
# swallow the very `git` it is looking for.
WRAPPERS = {
    "command": (),
    "env": ("-u", "--unset", "-S", "--split-string", "-C", "--chdir"),
    "time": ("-o", "--output", "-f", "--format"),
    "nohup": (),
    "stdbuf": ("-i", "--input", "-o", "--output", "-e", "--error"),
    "xargs": ("-n", "--max-args", "-I", "--replace", "-P", "--max-procs", "-L", "-s",
              "--max-chars", "-d", "--delimiter", "-E", "-a", "--arg-file"),
    "sudo": ("-u", "--user", "-g", "--group", "-p", "--prompt", "-C", "--close-from",
             "-h", "--host", "-r", "--role", "-t", "--type"),
    "nice": ("-n", "--adjustment"),
    "ionice": ("-c", "--class", "-n", "--classdata", "-p", "--pid"),
    "setsid": (),
    "timeout": ("-s", "--signal", "-k", "--kill-after"),
}

# `timeout` is the one wrapper here that takes a MANDATORY POSITIONAL before the command it runs, so
# the option walk stops on "60" and `timeout 60 git fetch --depth=1 origin master` is missed exactly
# the way the bare wrappers were. Only a duration is stepped over, so nothing else is consumed.
POSITIONAL = {"timeout": re.compile(r"^[0-9]+(\.[0-9]+)?[smhd]?$")}

ASSIGNMENT = re.compile(r"^[A-Za-z_][A-Za-z_0-9]*=")

try:
    data = json.load(sys.stdin)
    cmd = data.get("tool_input", {}).get("command", "")
    # shlex glues `$(` onto the following word, so `X=$(git log a..b)` never yields a bare `git`
    # token. Pad the substitution openers so the command inside is tokenised as a command.
    cmd = cmd.replace("$(", "$( ").replace("`", " ` ")
    # AND SO DOES AN UNSPACED CONTROL OPERATOR. shlex splits on whitespace only, so
    # `git merge-base HEAD origin/master; git fetch --depth=1 origin master` tokenises as
    # `origin/master;` followed by `git` - which is not in command position, and the fetch was
    # therefore never seen. Padding is safe in a way the `(` glue is not: these three cannot appear
    # unquoted inside a path or an argument, and a quoted one stays inside its single token.
    cmd = cmd.replace(";", " ; ").replace("&", " & ").replace("|", " | ")
    toks = shlex.split(cmd)
except Exception:
    sys.exit(0)

def starts_a_command(i):
    if i == 0:
        return True
    prev = toks[i - 1]
    # `X=$( git ...` opens a command too; a leading `VAR=value` prefix is consumed by the walk below,
    # which is where wrappers and assignments are handled together.
    return prev in SEPARATORS or prev.endswith("(")

def walk_to_command(start):
    """Step over assignment prefixes and wrappers to the word actually executed."""
    j, assigns = start, []
    while j < len(toks):
        t = toks[j]
        if ASSIGNMENT.match(t):
            assigns.append(j)
            j += 1
            continue
        if t in WRAPPERS:
            with_value = WRAPPERS[t]
            positional = POSITIONAL.get(t)
            j += 1
            if positional is not None and j < len(toks) and positional.match(toks[j]):
                j += 1
            while j < len(toks):
                w = toks[j]
                if w == "--":
                    j += 1
                    break
                if w.startswith("-"):
                    j += 2 if w in with_value else 1
                    continue
                break
            continue
        break
    return j, assigns

def stitch(idx, value):
    """Re-join a `$(...)` target the opener padding split, so the bash half sees the whole text."""
    if not value.endswith("$("):
        return value
    parts, depth, k = [value], 1, idx + 1
    while k < len(toks) and depth > 0:
        t = toks[k]
        parts.append(t)
        if t.endswith("$("):
            depth += 1
        elif t.endswith(")"):
            depth -= 1
        k += 1
    return " ".join(parts)

def clean(s):
    # One record per line, US-separated, so a field may hold neither.
    return s.replace("\x1f", " ").replace("\n", " ").replace("\r", " ")

# THE OVERRIDE IS A LEADING ASSIGNMENT PREFIX, not any occurrence. Matching it anywhere meant
# `git log -S "SHALLOW_HISTORY_ACCEPTED=1"` - searching history for the name of the escape hatch -
# disabled the guard.
for i, t in enumerate(toks):
    if t == "SHALLOW_HISTORY_ACCEPTED=1" and (i == 0 or toks[i - 1] in SEPARATORS or toks[i - 1].endswith("(")):
        sys.exit(0)

seen = set()
for i in range(len(toks)):
    if not starts_a_command(i):
        continue
    g, assigns = walk_to_command(i)
    # `BASE=$( git ...` starts a command twice over - at the assignment and at the padded opener - so
    # the same call would otherwise be reported once per entry point.
    # A subshell glues its `(` to the command word - `(git fetch --depth=1 origin master)` - and the
    # padding trick used for `;` cannot be reused here without breaking the `$(` stitching below, so
    # the opener is stripped off the word instead. Same defect as the wrappers: the call is there and
    # the scanner cannot see it.
    if g >= len(toks) or toks[g].lstrip("(") != "git" or g in seen:
        continue
    seen.add(g)
    rest = toks[g + 1:]
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
    # Report the redirect target rather than deciding on it: whether it names another repository is a
    # question only git can answer, and this half runs no subprocess. `--git-dir` beats `GIT_DIR=`
    # here because it beats it in git.
    target = ""
    for idx in assigns:
        if toks[idx].startswith("GIT_DIR="):
            target = stitch(idx, toks[idx][len("GIT_DIR="):])
    for m in range(j):
        idx = g + 1 + m
        o = toks[idx]
        if o.startswith("--git-dir="):
            target = stitch(idx, o[len("--git-dir="):])
        elif o == "--git-dir" and idx + 1 < len(toks):
            target = stitch(idx + 1, toks[idx + 1])
    if sub in SHALLOWING:
        depth_flag = next((a for a in args if a.startswith(DEPTH_FLAGS)), None)
        if depth_flag:
            print("SHALLOWING\x1f" + clean(target) + "\x1f" + clean("git " + sub + " " + depth_flag))
    if sub in DEPTH_DEPENDENT:
        print("QUERY\x1f\x1fgit " + sub)
    if sub in ("log", "diff") and any(".." in a and not a.startswith("-") for a in args):
        print("QUERY\x1f\x1fgit " + sub + " over a commit range")
sys.exit(0)
')"

[ -n "$verdicts" ] || exit 0

# Absolute and physical, without `readlink -f` (GNU-only, and bin/check-shell-hazards.sh bans it),
# and TOLERANT OF A PATH THAT DOES NOT EXIST YET: a fetch into a directory git is about to create
# cannot truncate this clone, so failing to resolve one must not deny it.
abs_path() {
    local p="$1" dir base dir_abs
    [ -n "$p" ] || return 1
    case "$p" in /*) ;; *) p="$PWD/$p" ;; esac
    if [ -d "$p" ]; then
        ( cd "$p" 2>/dev/null && pwd -P )
        return 0
    fi
    dir="$(dirname "$p")"
    base="$(basename "$p")"
    if dir_abs="$( cd "$dir" 2>/dev/null && pwd -P )" && [ -n "$dir_abs" ]; then
        printf '%s/%s\n' "$dir_abs" "$base"
    else
        printf '%s\n' "$p"
    fi
}

# Does a `--git-dir=`/`GIT_DIR=` target really name a repository OTHER than this clone? Answering by
# spelling made the option a bypass - see the header - so the target is resolved and compared, and
# anything that cannot be shown to differ is not exempt.
redirect_is_elsewhere() {
    local t="$1" abs resolved
    [ -n "$t" ] || return 1
    [ -n "$self_common" ] || return 1
    case "$t" in
        *'$'*|*'`'*)
            # Only the shell can expand this, and expanding it means running it. Judged on its text -
            # the third imprecision in the header, and the one that errs towards allowing.
            case "$t" in
                .git|.git/*|*/.git|*/.git/*|*PWD*|*GIT_DIR*|*GIT_COMMON_DIR*|*git-common-dir*|*rev-parse*)
                    return 1 ;;
                *) return 0 ;;
            esac
            ;;
    esac
    abs="$(abs_path "$t")" || return 1
    [ -n "$abs" ] || return 1
    # `git --git-dir=X rev-parse --git-common-dir` follows a gitfile and a linked worktree back to
    # the SHARED directory - which is the file `--depth` writes - so a sibling worktree's `.git`
    # compares equal to ours, as it has to. A target that is not a repository at all resolves to
    # nothing and is compared as the plain path it is.
    resolved="$(git --git-dir="$abs" rev-parse --git-common-dir 2>/dev/null)"
    if [ -n "$resolved" ]; then
        abs="$(abs_path "$resolved")"
    fi
    [ "$abs" = "$self_common" ] && return 1
    return 0
}

# Now, and only now, ask git - once, however many matches there are. Cheap, but not free enough to
# run on every Bash call. THE TWO KINDS WANT OPPOSITE ANSWERS: a query is wrong only when the clone
# is ALREADY shallow, while a shallowing fetch is only worth stopping while it is not - once the
# clone is shallow, that fetch changes nothing, and denying it would block the CI-shaped case for no
# gain. That opposition is exactly why the state has to be tested per match rather than once for a
# single winning verdict.
shallow="$(git rev-parse --is-shallow-repository 2>/dev/null)"
self_common="$(git rev-parse --git-common-dir 2>/dev/null)"
if [ -n "$self_common" ]; then
    self_common="$(abs_path "$self_common")"
fi

query_verdict=""
shallowing_verdict=""
# A fetch that truncates the clone arms every query AFTER it in the same command, even though the
# clone is still full at the moment the hook runs.
truncated="$shallow"
while IFS=$'\x1f' read -r kind target desc; do
    [ -n "$kind" ] || continue
    if [ "$kind" = "SHALLOWING" ]; then
        [ "$truncated" = "true" ] && continue
        redirect_is_elsewhere "$target" && continue
        [ -n "$shallowing_verdict" ] || shallowing_verdict="$desc"
        truncated="true"
    else
        [ "$truncated" = "true" ] || continue
        [ -n "$query_verdict" ] || query_verdict="$desc"
    fi
done <<< "$verdicts"

[ -n "$query_verdict" ] || [ -n "$shallowing_verdict" ] || exit 0

export QUERY_VERDICT="$query_verdict"
export SHALLOWING_VERDICT="$shallowing_verdict"
python3 -c '
import json, os
query_reason = (
    "THE CLONE IS SHALLOW, and `" + os.environ["QUERY_VERDICT"] + "` will answer anyway - from the "
    "truncated graft, without erroring. Measured here: a branch reported 836 commits against a "
    "true 29, and merge-base returned a commit that was not the merge base.\n\n"
    "Run `git fetch --unshallow` first, then re-run. If you are mid-merge-prep this is not "
    "optional: `git reset --mixed $(git merge-base ...)` against a wrong base silently reverts "
    "whatever master gained.\n\n"
    "It re-shallows because the `shallow` file lives in the shared --git-common-dir, so any "
    "sibling worktree doing a depth-limited fetch re-shallows this one too. Expect to need it "
    "more than once in a session.\n\n"
    "If the truncated answer genuinely does not matter, re-run prefixed with "
    "SHALLOW_HISTORY_ACCEPTED=1.")
shallowing_reason = (
    "`" + os.environ["SHALLOWING_VERDICT"] + "` WOULD TRUNCATE THIS CLONE, and not just for you. The `shallow` "
    "file lives in the shared --git-common-dir, so one depth-limited fetch re-shallows EVERY "
    "worktree of this clone at once - including sessions that unshallowed a minute ago.\n\n"
    "Nothing goes red afterwards. `git merge-base` starts returning empty, ahead/behind counts read "
    "in the hundreds, and commits that plainly landed report as not ancestors of master - all of "
    "which read as a rewritten history rather than as missing objects.\n\n"
    "Drop `--depth` (an ordinary fetch is incremental and cheap here), or fetch into a throwaway "
    # hazard-ok: the guidance text this hook prints, naming the safe alternative - not a command.
    "repository if you only want to read a ref: `git --git-dir=\"$(mktemp -d)\" fetch --depth=1 "
    "<url> <ref>`, then read it back with `git --git-dir=... show FETCH_HEAD:<path>`.\n\n"
    "If you really do want this clone shallow, re-run prefixed with SHALLOW_HISTORY_ACCEPTED=1.")
# Chained commands hid one hazard behind the other, so when both are live both get named: fixing the
# half you were told about and re-running the rest is how the second one used to land.
both_reason = (
    "TWO SEPARATE HAZARDS in one command, and chaining them is what used to hide the second: `"
    + os.environ["SHALLOWING_VERDICT"] + "` truncates the clone, and `" + os.environ["QUERY_VERDICT"]
    + "` then reads the truncated graft it leaves behind. Both are below.\n\n"
    + shallowing_reason + "\n\n" + query_reason)
if os.environ["QUERY_VERDICT"] and os.environ["SHALLOWING_VERDICT"]:
    reason = both_reason
elif os.environ["SHALLOWING_VERDICT"]:
    reason = shallowing_reason
else:
    reason = query_reason
print(json.dumps({"hookSpecificOutput": {"hookEventName": "PreToolUse",
    "permissionDecision": "deny",
    "permissionDecisionReason": reason}}))
'
exit 0
