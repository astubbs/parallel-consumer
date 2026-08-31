#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# REFUSES A HISTORY REWRITE WHILE A REVIEW IS IN FLIGHT.
#
# A force-push re-anchors every inline review comment and destroys the incremental diff a reviewer
# works from - so it throws away the REVIEWER's effort, not git's. While a review is running it is
# worse: findings land against a SHA that no longer exists and threads are orphaned before anyone
# reads them.
#
# WHY A HOOK AND NOT A RULE. docs/merge-checklist.md already says re-cut "right at the end before
# merging". That did not stop it happening twice in one session - once losing the diff on a PR under
# review, once starting a re-cut with three reviews mid-flight. A rule fires when someone remembers
# it; this fires whether or not anyone does.
#
# IT CHECKS RATHER THAN ASKS. A guard that always asks "are you sure?" becomes noise and gets waved
# through reflexively, which is worse than no guard because it manufactures the habit of overriding.
# This one looks up the branch's PR and names what would actually be lost - open review threads, a
# review running right now. With nothing found it still stops, but says so honestly - and says WHICH
# nothing it found, because "this branch has no PR" and "the lookup never answered" are different
# facts that one message used to report identically.
#
# WHICH BRANCH, AND HOW IT WAS DECIDED. This hook does not run in the directory its guarded command
# runs in, and this repository keeps many worktrees checked out at once - so the original
# `git rev-parse --abbrev-ref HEAD` answered about whichever branch the SESSION sat on. Twice on
# 2026-08-31 that named a completely unrelated branch: a force-push of `feats/proxy-verdict-free-return`
# (open PR astubbs/parallel-consumer#295, with review history - the case this hook exists for) and a
# `git commit --amend` in the `feats/ks-streams-fork-machinery` worktree were BOTH reported against
# `docs/god-branch-decomposition-plan`, the plan worktree the session happened to occupy. The most
# confident sentence this hook can print - "the lookup ran and came back empty" - was describing the
# wrong target, which is the same silent-wrong-answer class as the wrong-REPOSITORY fix in
# astubbs/parallel-consumer#364, one field over.
#
# So the branch is taken from the strongest source available, and the message says which one:
#   1. the push refspec, when the command names one - the only authoritative answer, and free,
#      because the token scan below has already split the command;
#   2. otherwise the HEAD of a directory, chosen from a `cd <path> &&` prefix, then the tool call's
#      own `cwd` from the payload, then this hook process's directory as a last resort.
# Anything but (1) is a GUESS about where the command runs, and the refusal says so in as many
# words rather than presenting it as a measured fact.
set -uo pipefail

payload="$(cat 2>/dev/null || true)"
[ -n "$payload" ] || exit 0

# Cheap bail before paying for python3 on every Bash call.
case "$payload" in
    # `-f` matched with a LEADING space only: `git push -f` has nothing after it, and requiring a
    # trailing space let exactly that through - proven by the self-test below. Widening costs a
    # python3 call on commands like `grep -f`, which the token scan then rejects; the pre-filter may
    # only ever skip work, never decide.
    *rebase*|*--force*|*" -f"*|*--amend*|*filter-branch*|*filter-repo*|*reset*|*update-ref*|*" -B"*|*" -C"*|*--delete*|*" :"*) ;;
    *) exit 0 ;;
esac

[ "${REWRITE_HISTORY_CONFIRMED:-}" = "1" ] && exit 0

# TOKENS, NOT SUBSTRINGS - the rule the sibling hooks state. `git commit -m "rebase notes"` and
# `gh pr comment --body "we should force-push"` must not fire. An unbalanced quote makes shlex raise,
# and that fails open.
#
# FOUR LINES OUT, NOT ONE, and they are all read from the SAME token walk: the verdict, the branch
# the push refspec names, a leading `cd <path>` the command changes into, and the payload's own
# `cwd`. Deriving the last three anywhere else would mean tokenising the command a second time, which
# is how two copies of a scan start disagreeing. An empty line is a real answer - "the command did
# not say" - and the working-directory arm below is what turns each one into a message.
scan="$(printf '%s' "$payload" | python3 -c '
import json, shlex, sys
try:
    data = json.load(sys.stdin)
    cmd = data.get("tool_input", {}).get("command", "")
    payload_cwd = data.get("cwd") or ""
    toks = shlex.split(cmd)
except Exception:
    sys.exit(0)

FORCE = {"--force", "-f", "--force-with-lease", "--force-if-includes"}
# Push options that consume a SEPARATE value token. Dropping only the flag would leave its value
# where the repository or the refspec should be.
PUSH_VALUE_FLAGS = {"-o", "--push-option", "--receive-pack", "--exec", "--repo"}
OPERATORS = set("();<>|&")


def push_head_ref(rest, sub):
    """The REMOTE-side branch name this push publishes, or "" when the command names none.

    THE PR HEAD IS THE DESTINATION, NOT THE SOURCE: `git push origin src:dst` publishes `dst`, so
    `dst` is what a pull request has as its head branch. `+src` is the force spelling of `src`.
    A push with no refspec, `HEAD`, a bare `refs/`-something and `tag <name>` all name nothing this
    can read as a branch, and each returns "" so the caller falls back and SAYS it fell back.

    The same rule is spelled out a second time, in bash, as `hook_push_head_ref` in
    .claude/hooks/lib/hook-common.sh. This hook REFUSES tool calls, so it may not depend on a
    library it might fail to source (astubbs/parallel-consumer#341); the duplication is tracked with
    the rest of it in docs/inflight/ci-pr-lookup-is-copied-into-three-hooks.md.
    """
    try:
        args = rest[rest.index(sub) + 1:]
    except ValueError:
        return ""
    positional = []
    j = 0
    while j < len(args):
        t = args[j]
        # A shell operator ends this command; everything after it belongs to the next one.
        if t and all(c in OPERATORS for c in t):
            break
        if t in PUSH_VALUE_FLAGS:
            j += 2
            continue
        if t.startswith("-"):
            j += 1
            continue
        positional.append(t)
        j += 1
    # positional[0] is the repository, positional[1] the first refspec. A multi-refspec push is
    # answered with its first, which is incomplete rather than wrong - that branch really is one of
    # the branches being rewritten.
    if len(positional) < 2 or positional[1] == "tag":
        return ""
    src, sep, dst = positional[1].partition(":")
    name = (dst if sep else src).lstrip("+")
    if name.startswith("refs/heads/"):
        name = name[len("refs/heads/"):]
    if not name or name == "HEAD" or name.startswith("refs/"):
        return ""
    return name


# A LEADING `cd <path> &&` IS THE COMMAND SAYING WHERE IT RUNS, and it outranks the payload cwd for
# exactly that reason. Only a LEADING one: a `cd` later in a compound command may be undoing an
# earlier one, and guessing which is worse than not guessing. The trailing strip handles the
# unspaced `cd path&& git ...` that plain shlex fuses into one token.
cd_prefix = ""
if len(toks) > 1 and toks[0] == "cd" and not toks[1].startswith("-"):
    cd_prefix = toks[1].rstrip(";&|")

verdict = ""
ref = ""
for i, t in enumerate(toks):
    if t.rsplit("/", 1)[-1] != "git":
        continue
    rest = toks[i+1:]
    sub = next((x for x in rest if not x.startswith("-")), None)
    flags = set(rest)
    # An env-prefixed override reaches here as a token, not as process env.
    if any(x == "REWRITE_HISTORY_CONFIRMED=1" for x in toks):
        sys.exit(0)
    if sub == "push" and (flags & FORCE or any(x.startswith("--force-with-lease=") for x in rest)):
        verdict, ref = "force-push", push_head_ref(rest, sub); break
    if sub == "rebase" and "--abort" not in flags and "--continue" not in flags and "--skip" not in flags:
        verdict = "rebase"; break
    if sub == "commit" and "--amend" in flags:
        verdict = "amend"; break
    if sub in ("filter-branch", "filter-repo"):
        verdict = sub; break
    # EVERY OTHER WAY TO MOVE A REF AND LOSE COMMITS. Found by probing the first version, which
    # caught only the obvious four - a guard that reaches just the shapes you thought of is a
    # documented bypass.
    if sub == "reset":
        # Forward sync to a remote ref is routine. Going BACKWARDS - HEAD~n, a bare SHA - drops
        # commits and needs a force-push afterwards, which is the thing being guarded.
        tgt = next((x for x in rest[rest.index("reset")+1:] if not x.startswith("-")), None)
        if tgt and not tgt.startswith("origin/") and not tgt.startswith("upstream/"):
            import re as _re
            if _re.search(r"[~^]", tgt) or _re.fullmatch(r"[0-9a-f]{7,40}", tgt):
                verdict = "reset-backwards"; break
    if sub == "branch" and "-f" in flags or (sub == "branch" and "--force" in flags):
        verdict = "branch -f"; break
    if sub in ("checkout", "switch"):
        moved = "-B" in flags or "-C" in flags
        # `-B name` alone just points at HEAD; `-B name <start>` moves the ref somewhere else.
        if moved:
            after = rest[rest.index("-B") + 1:] if "-B" in rest else rest[rest.index("-C") + 1:]
            if len([x for x in after if not x.startswith("-")]) >= 2:
                verdict = "branch reset via " + sub; break
    if sub == "update-ref" and any(x.startswith("refs/heads/") for x in rest):
        verdict = "update-ref"; break
    if sub == "push" and ("--delete" in flags or "-d" in flags or any(x.startswith(":") and len(x) > 1 for x in rest)):
        verdict, ref = "remote branch deletion", push_head_ref(rest, sub); break

if verdict:
    print(verdict)
    print(ref)
    print(cd_prefix)
    print(payload_cwd)
' 2>/dev/null || true)"
# A HERESTRING, NOT A HEREDOC: an unquoted heredoc would expand `$` and `\` inside a path, and a
# quoted one would not expand `$scan` at all. `IFS=` keeps a path's own leading spaces.
{ IFS= read -r verdict; IFS= read -r pushed_ref; IFS= read -r cd_prefix; IFS= read -r payload_cwd; } <<<"$scan"
[ -n "$verdict" ] || exit 0

# WHERE THE COMMAND RUNS IS NOT WHERE THIS HOOK RUNS - see the header. Strongest source first, and
# whichever one answers is carried into the refusal, because the operator cannot check an answer
# whose provenance is hidden.
workdir=""
workdir_desc=""
if [ -n "$cd_prefix" ] && [ -d "$cd_prefix" ]; then
    workdir="$cd_prefix"
    workdir_desc="the directory the command changes into"
elif [ -n "$payload_cwd" ] && [ -d "$payload_cwd" ]; then
    workdir="$payload_cwd"
    workdir_desc="this tool call's own working directory"
else
    workdir="$PWD"
    workdir_desc="this hook process's directory, because the tool call did not say where it runs"
fi
if ! cd "$workdir" 2>/dev/null; then
    workdir_desc="this hook process's directory, because \`$workdir\` could not be entered"
    workdir="$PWD"
fi

root="$(git rev-parse --show-toplevel 2>/dev/null || true)"
[ -n "$root" ] || exit 0
cd "$root" 2>/dev/null || exit 0

# THE PROVENANCE SENTENCE IS NOT DECORATION. Everything below - the PR number, the thread count, the
# runs in progress - is about `$branch`, and when the command did not name a branch that is a GUESS
# about which worktree this call belongs to. Saying so is what turns a wrong answer into a checkable
# one; the two incidents in the header are both cases where it read as measured fact.
provenance=""
if [ -n "$pushed_ref" ]; then
    branch="$pushed_ref"
else
    branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
    provenance="THE COMMAND DOES NOT NAME A BRANCH, so this hook used \`${branch:-<none>}\` - the current HEAD of ${root}, reached from ${workdir_desc} (\`${workdir}\`). This repository normally has several worktrees checked out at once, so that may not be where your command runs: check it is the branch you are rewriting before reading anything below as being about it. "
fi

# WHAT WOULD ACTUALLY BE LOST. Best-effort: a missing PR, a missing gh or a dead network all fall
# through to the refusal rather than letting the rewrite past - the point is the pause, the detail is
# a bonus. But WHICH of those happened is not a detail, it is the difference between "there is
# nothing here to lose" and "I could not look", and the first version printed one sentence for both.
#
# THREE ANSWERS, NEVER ONE. `2>/dev/null || true` on the lookup discarded gh's exit status and its
# stderr together, so "this branch has no PR", "gh is not installed / not authenticated /
# rate-limited" and "the lookup answered for the WRONG REPOSITORY" all rendered as "No PR was found
# for this branch". Observed twice in one day on astubbs/parallel-consumer#356, from two different
# causes, with the operator told the same thing each time.
#
# THE REPO IS DERIVED FROM `origin`, NOT LEFT TO gh AND NOT HARDCODED - the same reasoning
# .claude/hooks/inject-branch-context.sh states at "THE REPO IS DERIVED FROM `origin`", a file that
# ARRIVES WITH astubbs#350 and is not in this tree yet, so grep it on that branch rather than here:
# a bare `gh` in this fork resolves to confluentinc/parallel-consumer, because gh prefers the
# `upstream` remote and the fix (`gh repo set-default`) writes `remote.origin.gh-resolved` into a
# LOCAL, uncommitted config that a CI runner or a fresh sandbox does not have. Hardcoding the slug
# would be wrong the moment someone works in their own fork. When `origin` cannot be read the
# lookup is NOT retried unqualified: a wrong answer that resolves is worse than no answer.
#
# BOUNDED, and bounded in python3 rather than with `timeout(1)`, which is GNU-only and absent on
# macOS - the portability rule this directory already follows. An unbounded lookup would hang the
# tool call it is guarding.
detail=""
if ! command -v python3 >/dev/null 2>&1; then
    detail="This hook could not look the branch up: python3 is not available, so nothing could be measured - which is not the same as nothing being at risk."
elif [ -z "$branch" ] || [ "$branch" = "HEAD" ]; then
    detail="HEAD is detached here, so there is no branch to look a pull request up by and nothing could be measured - which is not the same as nothing being at risk."
else
    detail="$(python3 - "$branch" <<'PY'
import concurrent.futures
import re
import subprocess
import sys

BRANCH = sys.argv[1] if len(sys.argv) > 1 else ""
GH_SECONDS = 10
GIT_SECONDS = 5


def run(args, secs):
    """(stdout, problem). stdout is None whenever the command did not answer."""
    try:
        p = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=secs)
    except FileNotFoundError:
        return None, "`%s` is not on PATH" % args[0]
    except subprocess.TimeoutExpired:
        return None, "`%s` did not answer within %ds" % (args[0], secs)
    except Exception as exc:
        return None, "`%s` could not be run (%s)" % (args[0], exc.__class__.__name__)
    if p.returncode != 0:
        why = " ".join(p.stderr.decode("utf-8", "replace").split())
        return None, (why[:200] or "`%s` exited %d without saying why" % (args[0], p.returncode))
    return p.stdout.decode("utf-8", "replace").strip(), None


def origin_slug():
    url, problem = run(["git", "remote", "get-url", "origin"], GIT_SECONDS)
    if url is None:
        return None, "the `origin` remote could not be read - %s" % problem
    # A HOSTED REMOTE, not any path and not any scheme: a clone whose origin is a local directory
    # otherwise yields a slug built from the last two path segments, and gh is then asked about a
    # repository that does not exist. inject-branch-context.sh, on astubbs#350, carries the worked
    # case, including why `file://` has to be excluded by allowlisting the schemes rather than
    # requiring one - that hook shipped the bug and fixed it there, so the allowlist here is its
    # conclusion rather than an independent one.
    hosted = re.match(r"^(?:https?|ssh|git)://", url) or re.match(r"^[^/]+@[^/:]+:", url)
    m = re.search(r"[:/]([^/:]+)/([^/]+?)(?:\.git)?/?$", url) if hosted else None
    if not m:
        return None, "`origin` is %s, which is not a hosted remote URL, so there is no repository to ask about" % (
            ("`%s`" % url) if url else "unset")
    return "%s/%s" % (m.group(1), m.group(2)), None


def emit(text):
    print(text)
    sys.exit(0)


try:
    slug, slug_problem = origin_slug()
    if slug is None:
        emit("The pull-request lookup DID NOT RUN: %s. It was not retried without `-R`, because gh "
             "prefers the `upstream` remote in this fork and an unqualified lookup answers for "
             "confluentinc/parallel-consumer instead - a wrong answer that resolves is worse than "
             "none. Nothing could be measured, which is NOT the same as nothing being at risk." % slug_problem)

    number, problem = run(["gh", "pr", "list", "-R", slug, "--head", BRANCH,
                           "--json", "number", "--jq", ".[0].number"], GH_SECONDS)
    if number is None:
        emit("The pull-request lookup FAILED against %s: %s. Nothing could be measured, and a "
             "lookup that never answered is NOT evidence that this branch has no PR - a rewrite "
             "here could still be destroying review work." % (slug, problem))
    if not number.isdigit():
        if number:
            emit("The pull-request lookup against %s answered `%s`, which is not a PR number, so "
                 "nothing could be measured - which is not the same as nothing being at risk."
                 % (slug, number[:80]))
        # gh exits 0 and prints nothing for a head branch with no open PR, so this - and only this -
        # is a measured absence. Say which repository was asked, or the reader cannot tell this
        # apart from the wrong-repo answer that used to be possible here.
        emit("The lookup ran against %s and came back empty: no open pull request has `%s` as its "
             "head branch. So nothing could be measured, which is not the same as nothing being at "
             "risk - an unpushed branch still carries commits a rewrite drops." % (slug, BRANCH))

    # TWO INDEPENDENT ROUND TRIPS, OVERLAPPED. Neither answer feeds the other, but run one after the
    # other their bounds ADD: this hook sits in front of the tool call it is guarding for
    # GIT_SECONDS + three times GH_SECONDS in the worst case, and the operator is waiting the whole
    # time for a message that ends in "ask the operator". Overlapping the pair costs one import and
    # takes the worst case for these two from 20s to 10s. `subprocess.run` is thread-safe and each
    # call keeps its own timeout, so the two results are the same two results, read back in the same
    # order - only the waiting is shared. Bounded at two workers because there are two calls; this
    # is not a pool to grow.
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        threads_call = pool.submit(run, ["gh", "api", "repos/%s/pulls/%s/comments" % (slug, number),
                                         "--jq", "length"], GH_SECONDS)
        runs_call = pool.submit(run, ["gh", "run", "list", "-R", slug, "--branch", BRANCH,
                                      "--json", "status,name", "--jq",
                                      '[.[] | select(.status=="in_progress" or .status=="queued")] | length'],
                                GH_SECONDS)
        threads, threads_problem = threads_call.result()
        runs, runs_problem = runs_call.result()

    # NAME THE BRANCH, not just the PR. Everything that follows is a measurement of one branch, and
    # a reader who cannot see which one cannot tell a correct answer from the wrong-worktree answer
    # this hook used to give (see WHICH BRANCH at the top of this file).
    parts = ["`%s` is %s#%s." % (BRANCH, slug, number)]
    found = False
    unmeasured = []
    if threads_problem:
        unmeasured.append("its inline review comments could not be counted (%s)" % threads_problem)
    elif threads not in ("", "0"):
        found = True
        parts.append("It has %s inline review comment(s), which a force-push re-anchors or orphans." % threads)
    if runs_problem:
        unmeasured.append("its runs in progress could not be counted (%s)" % runs_problem)
    elif runs not in ("", "0"):
        found = True
        parts.append("%s check/review run(s) are IN PROGRESS against the current head - their "
                     "findings would land on a SHA that no longer exists." % runs)

    # NOTHING FOUND IS NOT PERMISSION, and must not READ as permission. A quiet PR is the most
    # dangerous message this hook can send: a reviewer who has read the diff and not yet commented
    # has exactly zero threads and zero running jobs, and loses the most from a rewrite. What is
    # measurable here is a lower bound on the damage, never the absence of it. And a count that
    # FAILED is not a count of zero - saying "none were found" for a request that never answered is
    # the same defect one level down.
    if unmeasured:
        parts.append("However %s - so what a rewrite would cost here is UNMEASURED, not absent."
                     % " and ".join(unmeasured))
    elif not found:
        parts.append("No open review comments and no runs in progress were found - which is NOT "
                     "evidence that a rewrite is safe. A reviewer part-way through the diff, with "
                     "nothing posted yet, looks exactly like this and loses the most. Only the "
                     "operator saying now is evidence.")
    emit(" ".join(parts))
except Exception as exc:
    print("The pull-request lookup could not be completed (%s), so nothing could be measured - "
          "which is not the same as nothing being at risk." % exc.__class__.__name__)
PY
)"
fi
[ -n "$detail" ] || detail="The pull-request lookup produced no answer at all, so nothing could be measured - which is not the same as nothing being at risk."
detail="${provenance}${detail}"

export VERDICT="$verdict"
export DETAIL="$detail"
python3 -c '
import json, os
print(json.dumps({"hookSpecificOutput": {"hookEventName": "PreToolUse",
    "permissionDecision": "deny",
    "permissionDecisionReason": (
        "This is a history rewrite (" + os.environ["VERDICT"] + "). " + os.environ["DETAIL"] +
        "\n\nRe-cutting, rebasing and force-pushing are the LAST step before a merge - after the "
        "reviews are in and their fixes are made, never while one is running. Ask the operator "
        "whether now is the right time, and say what would be lost.\n\n"
        "If a merge cannot do the job - removing a commit from the ancestry of a branch, e.g. detaching a "
        "stacked PR - say so and re-run prefixed with REWRITE_HISTORY_CONFIRMED=1. Updating a moved "
        "base is a MERGE. Removing content from a branch is an ordinary revert commit. "
        "See docs/merge-checklist.md.")}}))
'
exit 0
