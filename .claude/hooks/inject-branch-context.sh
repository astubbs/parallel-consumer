#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# Put the branch's OWN record - its commits, its handoff notes, its PR body and its PR comments -
# into the context of whoever is about to work on it.
#
# WHY THIS EXISTS
#
# AGENTS.md has "Read the record you inherit", and until this hook that rule fired on ONE trigger:
# your base moved
# under you. Nothing covered the other trigger - you were HANDED a branch and never asked what it
# says about itself. The two are the same rule; only the trigger differs.
#
# Worked incident, 2026-08-24. Five agents were dispatched, one per open PR, to run a
# simplify-then-review pass. Each got the PR's changed-file list and NOT the PR's own commits,
# handoff note, body or comments. Those bodies pre-emptively defended decisions a simplify pass
# reverses on sight:
#
#   - astubbs/parallel-consumer#341's body argues BY NAME against extracting four repeated `stat`
#     lines into `bin/lib/`, because a guard that cannot source its helper fails OPEN - which is the
#     defect that PR exists to fix. It is the most extractable thing in the diff, and extracting it
#     is a regression.
#   - astubbs/parallel-consumer#338's copyright type table carries rows for languages the repo has
#     no files for. Deleting the unused rows reintroduces the silent-skip bug the PR fixes.
#   - astubbs/parallel-consumer#339 pins all four thresholds in every self-test case instead of
#     inheriting. Prime dedupe bait, and a recorded bug: an earlier version inherited defaults and
#     three cases flipped to failing mid-session when the machine dropped below the 25 GiB line.
#   - astubbs/parallel-consumer#347 carries a deliberately-broken RED control, which invites being
#     "fixed"; astubbs/parallel-consumer#348's volatile control arms look like duplicates of the
#     main probes and are the actual finding.
#
# Only one of the five had a handoff doc; the others did not need one, because the context that
# would have prevented every case was the branch's own commits and its PR body - and, in
# astubbs/parallel-consumer#341's case, a PR COMMENT posted after the body: a ride-along scope
# addition. So this hook counts comments and names who wrote them, rather than only noting that a
# body exists.
#
# WHERE IT IS REGISTERED, AND WHY EACH - all three measured against Claude Code 2.1.231, because
# docs/agent-harness.md's standing rule is that harness claims are tested rather than read off the
# documentation:
#
#   1. SessionStart - a session opened IN a worktree. The same event inject-recorded-knowledge.sh
#      uses. Fires once, `source=startup`.
#
#   2. PreToolUse matching `Task|Agent` - the DISPATCHER, at the moment it hands work to a subagent.
#      Two measured facts shape what this can promise. The tool's real `tool_name` is **`Agent`**,
#      not `Task`, and a matcher of either string fires (matchers are regexes; `Task|Agent` was
#      verified to fire). And a PreToolUse hook's `additionalContext` reaches the caller only
#      ALONGSIDE THE TOOL RESULT - the model had already composed that tool call before the hook
#      ran, so this cannot pre-empt the dispatch it fires on. What it does do is reach the
#      dispatcher before it reads the subagent's report and before dispatch N+1, which in the
#      incident above was four further agents.
#
#   3. PreToolUse with no matcher restriction, firing ONLY when the payload carries `agent_type` -
#      i.e. the tool call is being made BY a subagent. This is the registration that actually closes
#      the incident, and it exists because of a negative result: **SessionStart does NOT fire for an
#      agent spawned via the Task tool.** A subagent shares the dispatcher's `session_id` and gets no
#      session of its own, so without this registration a subagent could never receive branch context
#      by any route. Verified reachable: a hook keyed on `agent_type` injected a marker string that
#      the subagent itself then quoted back. It is throttled to once per `agent_id`, or it repeats on
#      every tool call the subagent makes.
#
# DEGRADED READS ARE LOUD, NEVER SHORT. An injection hook's correct output on a boring branch is
# silence, which is byte-identical to being broken - so a section that cannot be built says so by
# name instead of being omitted. That is not a hypothesis here: `inject-recorded-knowledge.sh` uses
# GNU-only `xargs -r`, and under a BSD `xargs` its Registers section silently drops from 13 entries
# to 4 while closed notes get relabelled as mis-tagged. A truncated-but-plausible index is worse
# than no index.
#
# BSD-CLEAN FROM LINE ONE. No `stat -c`, `mapfile`, `readarray`, `grep -P`, `date -d`,
# `readlink -f`, `touch -d`, `xargs -r`, no bare `mktemp`, and no `timeout(1)` - that last one is GNU
# coreutils and macOS does not ship it, which is the trap this class of bound usually falls into. The
# `gh` call is bounded by python3's `subprocess` timeout instead, and python3 also does the cache
# mtime check that would otherwise be `stat -c %Y`.
#
# `mktemp` is on that list because it was ON THIS SCRIPT, and reviewed past twice: the construct
# lists both this header and the review agent grepped for were inherited from
# astubbs/parallel-consumer#341's sweep, which did not include it. A list is only as good as the last
# defect that taught it something, so a construct joins it the run it is found, not the run it is
# theorised.
#
# CHEAP: names, counts and pointers - never bodies. The failure being fixed is not knowing the
# record EXISTS. Once a count and a command are in context, the agent's own `gh pr view --comments`
# does the rest.
#
# NEVER FAILS A SESSION: any error prints nothing and exits 0. A broken reminder must not be a
# broken session.

set -uo pipefail

# THE PAYLOAD ARRIVES BY FILE, NOT BY ARGV - the lesson inject-merge-checklist.sh's header records.
# Linux caps a single argv string at ~128 KiB (MAX_ARG_STRLEN) and a hook payload carries the whole
# tool input; a dispatch prompt clears that easily. Passing it as an argument fails with "Argument
# list too long" BEFORE python starts, and since these hooks fail open the failure is silent.
# A TEMPLATE, NOT A BARE `mktemp`. BSD/macOS `mktemp` requires a template operand (or `-t prefix`)
# and exits 1 on none, where GNU defaults one - so a bare call made this hook exit 0 having emitted
# ZERO BYTES on macOS. That is the exact failure this file's DEGRADED READS ARE LOUD rule exists to
# forbid, and it is worse here than anywhere else: an injection hook is silent when it is working,
# so total inertness is indistinguishable from a boring branch. Six X's satisfies both userlands.
payload_file=$(mktemp "${TMPDIR:-/tmp}/pc-branch-context-payload.XXXXXX" 2>/dev/null) || exit 0
trap 'rm -f "$payload_file"' EXIT
cat > "$payload_file" 2>/dev/null || exit 0
[ -s "$payload_file" ] || exit 0

# CHEAP BAIL BEFORE PAYING FOR python3, which fires on every tool call of every subagent. Every key
# tested here precedes `tool_input` in the payloads Claude Code emits, so 4 KB is enough to decide
# and a 150 KB dispatch prompt is never read. `head -c` on a FILE, not a pipe into grep - the
# pipefail/EPIPE trap bin/AGENTS.md documents, which bin/check-shell-sigpipe.sh scans this directory
# for.
head=$(head -c 4000 "$payload_file" 2>/dev/null) || exit 0
case "$head" in
    *'"hook_event_name":"SessionStart"'*|*'"hook_event_name": "SessionStart"'*) ;;
    *'"agent_type"'*) ;;
    *'"tool_name":"Agent"'*|*'"tool_name": "Agent"'*|*'"tool_name":"Task"'*|*'"tool_name": "Task"'*) ;;
    *) exit 0 ;;
esac

# SHELL-LEVEL THROTTLE FOR THE SUBAGENT CASE, measured rather than assumed. This hook is registered
# on every tool call, so a subagent that has already been told its branch context would otherwise pay
# a python3 spawn - 27ms measured - on every subsequent Read, Grep and Bash for the rest of its life,
# where the shell bail above costs 6ms. The stamp is named after the agent_id verbatim (Claude Code
# emits hex) rather than a hash of it, precisely so this test needs no `shasum` - whose name and
# output differ between GNU and BSD, which is the portability trap this whole script is avoiding.
# Anything unexpected in the id falls through to python rather than being sanitised here, because two
# spellings of "sanitised" is how the two throttles would silently stop agreeing on a filename.
agent_id=$(printf '%s' "$head" | sed -n 's/.*"agent_id":[[:space:]]*"\([^"]*\)".*/\1/p' | head -1)
case "$agent_id" in
    "" | *[!A-Za-z0-9._-]* ) ;;
    *) [ -f "${TMPDIR:-/tmp}/pc-branch-context-agent-${agent_id}.stamp" ] && exit 0 ;;
esac

command -v python3 >/dev/null 2>&1 || exit 0

python3 - "$payload_file" <<'PY' 2>/dev/null || exit 0
import hashlib
import json
import os
import re
import subprocess
import sys
import time

# Bounds. GH_SECONDS is the only network wait on a path that fires per dispatch and per subagent;
# a warm `gh pr view <branch> --json ...` against this repo measures 0.5-1.4s, so 5s absorbs a slow
# link while costing less than one model round-trip when the network is simply gone. GIT_SECONDS is
# generous for local plumbing and exists so a wedged index cannot hang a session either.
GH_SECONDS = 5
GIT_SECONDS = 10
# How long a PR answer is reused. Long enough that dispatching five agents is one network call;
# short enough that a comment posted mid-session is picked up on the next branch you touch.
PR_CACHE_SECONDS = 600
# How long the same block is suppressed for the same target, so a dispatch loop cannot repeat it.
REPEAT_SECONDS = 1800
COMMIT_CAP = 40
NOTES_CAP = 25

TMP = os.environ.get("TMPDIR") or "/tmp"


def read_payload(path):
    try:
        with open(path, encoding="utf-8", errors="replace") as fh:
            return json.load(fh)
    except Exception:
        return None


def run(args, cwd=None, seconds=GIT_SECONDS):
    """(ok, stdout, stderr). ok is False for a non-zero exit, a timeout, or a missing binary."""
    try:
        p = subprocess.run(args, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                           timeout=seconds)
    except Exception:
        return False, "", ""
    err = p.stderr.decode("utf-8", "replace")
    if p.returncode != 0:
        return False, "", err
    return True, p.stdout.decode("utf-8", "replace"), err


def git(target, *args, **kw):
    return run(["git", "-C", target] + list(args), **kw)


# --------------------------------------------------------------------------------------------
# Which directory are we describing, and on whose behalf
# --------------------------------------------------------------------------------------------

# A DISPATCH NAMES ITS WORKTREE IN THE PROMPT, and that is the directory worth describing. Emitting
# the DISPATCHER's own branch to a coordinator sitting on master is noise, and a hook that is noise
# gets scrolled past - the same failure as a check that is always red. So the dispatch case reads the
# prompt for a path, and the emitted block always NAMES the directory it describes so the two can
# never be mistaken for each other.
#
# THE RULE IS "A DIFFERENT WORKING TREE", NOT "A PATH UNDER .claude/worktrees/". Matching this repo's
# worktree convention was the first attempt and it is too narrow twice over: it cannot be self-tested
# without a fixture that fakes the layout, and it silently ignores a dispatch that names a plain
# clone. Requiring the candidate's git toplevel to DIFFER from the dispatcher's own is the property
# actually being tested, and it makes an ordinary in-repo file path - which every prompt is full of -
# resolve to the dispatcher and therefore lose, which is the right answer.
ABS_PATH_RE = re.compile(r"(/[^\s\"'`<>|]{2,})")
PATH_CANDIDATE_CAP = 25


def _toplevel(path):
    ok, out, _ = run(["git", "-C", path, "rev-parse", "--show-toplevel"])
    return out.strip() if ok else None


def target_from_prompt(payload, own_dir):
    ti = payload.get("tool_input") or {}
    blob = ""
    for key in ("prompt", "description"):
        v = ti.get(key)
        if isinstance(v, str):
            blob += "\n" + v
    if not blob:
        return None, None
    own_top = _toplevel(own_dir)
    seen, tried = set(), 0
    for m in ABS_PATH_RE.finditer(blob):
        cand = m.group(1)
        # Trailing punctuation, one character at a time. A path at the END OF A SENTENCE swallows the
        # full stop, and a directory name may legitimately contain `.` and `-`, so there is no
        # character class that separates the two - only trying is decisive. This is the shape a
        # dispatch prompt actually takes, and the first thing this resolver got wrong.
        while cand and not os.path.exists(cand) and cand[-1] in ".,;:'\"`)]}>":
            cand = cand[:-1]
        # A dispatch names files as often as directories; the file's directory is the useful half.
        while cand and cand != "/" and not os.path.isdir(cand):
            cand = os.path.dirname(cand)
        if not cand or cand == "/" or cand in seen:
            continue
        seen.add(cand)
        tried += 1
        if tried > PATH_CANDIDATE_CAP:
            break
        top = _toplevel(cand)
        if top and top != own_top:
            return top, "the working tree named in the dispatch prompt"
    return None, None


def decide(payload):
    """(mode, target_dir, why, throttle_key) or (None, ...) to stay silent."""
    event = payload.get("hook_event_name")
    cwd = payload.get("cwd") or os.getcwd()
    if event == "SessionStart":
        return "session", cwd, "this session's working directory", "session:" + cwd
    if payload.get("agent_type"):
        # Running INSIDE a subagent. Throttle on agent_id so it lands once, not per tool call.
        key = "agent:" + str(payload.get("agent_id") or payload.get("agent_type"))
        return "subagent", cwd, "this agent's working directory", key
    if payload.get("tool_name") in ("Agent", "Task"):
        tgt, why = target_from_prompt(payload, cwd)
        if tgt is None:
            tgt, why = cwd, ("the dispatching session's own working directory - the prompt named no "
                             "other working tree")
        return "dispatch", tgt, why, "dispatch:" + tgt
    return None, None, None, None


payload = read_payload(sys.argv[1])
if payload is None:
    sys.exit(0)
mode, target, why, throttle_key = decide(payload)
if mode is None or not target or not os.path.isdir(target):
    sys.exit(0)


def stamp_path(key):
    # The subagent key is spelt out rather than hashed so the shell fast path above can test for the
    # same file without hashing. Every other key is hashed - a directory path is not a filename.
    if key.startswith("agent:"):
        ident = key[len("agent:"):]
        if ident and not [c for c in ident if not (c.isalnum() or c in "._-")]:
            return os.path.join(TMP, "pc-branch-context-agent-%s.stamp" % ident)
    digest = hashlib.sha1(key.encode("utf-8", "replace")).hexdigest()[:16]
    return os.path.join(TMP, "pc-branch-context-%s.stamp" % digest)


def throttled(key, seconds):
    """True when this block was already emitted recently. Failing to read or write the stamp
    means NOT throttled - a hook that cannot remember must repeat itself rather than go quiet."""
    path = stamp_path(key)
    try:
        if os.path.exists(path) and (time.time() - os.path.getmtime(path)) < seconds:
            return True
    except Exception:
        return False
    try:
        with open(path, "w") as fh:
            fh.write("")
    except Exception:
        pass
    return False


# SessionStart fires once per session, so throttling it would only ever suppress a legitimate
# reminder after a resume.
if mode != "session" and throttled(throttle_key, REPEAT_SECONDS):
    sys.exit(0)

# --------------------------------------------------------------------------------------------
# Git side
# --------------------------------------------------------------------------------------------

ok, _, _ = git(target, "rev-parse", "--is-inside-work-tree")
if not ok:
    sys.exit(0)
ok, branch, _ = git(target, "rev-parse", "--abbrev-ref", "HEAD")
branch = branch.strip()
if not ok or not branch or branch == "HEAD":
    # A detached checkout has no branch to describe, and GitHub Actions checks PRs out detached.
    sys.exit(0)
if branch in ("master", "main"):
    # The base branch is not "a branch you were handed"; it has no own record to read.
    sys.exit(0)

# BASE, IN PREFERENCE ORDER. `origin/master` is what a worktree here is cut from; the local `master`
# is the fallback for a clone with no remote, and it is a WORSE answer (a stale local master invents
# work that already landed), so the block says which one was used.
base_ref, merge_base, base_problem = None, None, None
for cand in ("origin/master", "master", "origin/main", "main"):
    ok, out, _ = git(target, "merge-base", cand, "HEAD")
    if ok and out.strip():
        base_ref, merge_base = cand, out.strip()
        break
if merge_base is None:
    base_problem = ("no merge base against origin/master, master, origin/main or main - a shallow "
                    "clone is the usual cause (`git fetch --unshallow`)")

commit_lines = []
commits_problem = None
extra_commits = 0
if merge_base:
    # \x1e between records and \x00 between fields: a commit subject or body can contain anything
    # else, including newlines, and a line-based split silently merges two commits into one.
    ok, out, _ = git(target, "log", "--format=%h%x00%s%x00%b%x1e", merge_base + "..HEAD")
    if not ok:
        commits_problem = "`git log` failed against %s..HEAD" % base_ref
    else:
        records = [r for r in out.split("\x1e") if r.strip()]
        if len(records) > COMMIT_CAP:
            extra_commits = len(records) - COMMIT_CAP
            records = records[:COMMIT_CAP]
        for rec in records:
            parts = rec.lstrip("\n").split("\x00")
            if len(parts) < 3:
                continue
            sha, subject, body = parts[0], parts[1], parts[2]
            body_lines = len([ln for ln in body.splitlines() if ln.strip()])
            # THE COUNT IS THE SIGNAL, not a threshold someone has to agree with: commit bodies are
            # load-bearing here - release notes are generated from the log, so the most consequential
            # sentence in a commit is often nowhere near its subject, and "this one has 40 lines of
            # body" is what makes an agent open it. The `git show` command is printed ONCE above the
            # list rather than per line; repeating a 90-character path forty times was most of the
            # block's size and none of its information.
            mark = ("%3d" % body_lines) if body_lines else "  -"
            commit_lines.append("- %s [%s] %s" % (sha, mark, subject))

notes_lines = []
notes_problem = None
if merge_base:
    ok, out, _ = git(target, "diff", "--name-status", merge_base + "..HEAD",
                     "--", "docs/inflight", "docs/plans")
    if not ok:
        notes_problem = "`git diff --name-status` failed against %s..HEAD" % base_ref
    else:
        for ln in out.splitlines():
            bits = ln.split("\t")
            if len(bits) >= 2 and bits[0][:1] != "D":
                notes_lines.append("- %s  `%s`" % (bits[0].strip(), bits[-1]))
        if len(notes_lines) > NOTES_CAP:
            extra = len(notes_lines) - NOTES_CAP
            notes_lines = notes_lines[:NOTES_CAP]
            notes_lines.append("- ... and %d more, NOT LISTED (capped at %d) - "
                               "`git -C %s diff --name-status %s..HEAD -- docs/inflight docs/plans`"
                               % (extra, NOTES_CAP, target, merge_base[:9]))

marker_lines = []
marker_file = os.path.join(target, ".worktree-owner")
if os.path.isfile(marker_file):
    try:
        with open(marker_file, encoding="utf-8", errors="replace") as fh:
            marker_lines = [ln.rstrip() for ln in fh.read().splitlines() if ln.strip()][:12]
    except Exception:
        marker_lines = []

# --------------------------------------------------------------------------------------------
# PR side
# --------------------------------------------------------------------------------------------

# THE REPO IS DERIVED FROM `origin`, NOT LEFT TO gh AND NOT HARDCODED. A bare `gh` here resolves to
# confluentinc/parallel-consumer, because `gh` prefers the `upstream` remote and the fix
# (`gh repo set-default`) writes `remote.origin.gh-resolved` into a LOCAL, uncommitted config - so a
# fresh sandbox or a CI runner starts without it and the damaging case is the command that SUCCEEDS
# against the wrong repository. Hardcoding `astubbs/parallel-consumer` would be wrong the moment
# someone works in their own fork. `origin` is by definition the repo this branch pushes to.
def origin_slug(target):
    ok, url, _ = run(["git", "-C", target, "remote", "get-url", "origin"], seconds=GIT_SECONDS)
    if not ok:
        return None
    url = url.strip()
    # A HOSTED REMOTE, NOT ANY PATH AND NOT ANY SCHEME. A clone whose origin is a local directory -
    # `git clone /path/to/repo`, which is how a scratch or baseline checkout is usually made -
    # otherwise yields a slug built from the last two path segments (`git/parallel-consumer`), and
    # `gh` is then asked about a repository that does not exist. It fails, so the block stays honest,
    # but it names a plausible wrong repo while doing it.
    #
    # ALLOWLIST THE SCHEMES; DO NOT MERELY REQUIRE ONE. An earlier version asked only whether the URL
    # contained `://`, which `file:///home/astubbs/git/parallel-consumer` satisfies - so the very
    # local clone the guard exists to catch walked straight through it and produced the same
    # `git/parallel-consumer` slug, reached by a scheme instead of a bare path. `git clone file://...`
    # is the documented way to force a real transport against a local repo, so this is not a
    # hypothetical spelling. Naming the four schemes that can carry a hosting slug closes `file://`
    # and `ftp://` together, and an unknown scheme is the accurate answer either way: there is no
    # repository to ask about. Found in review of astubbs/parallel-consumer#350.
    if not re.match(r"^(?:https?|ssh|git)://", url) and not re.match(r"^[^/]+@[^/:]+:", url):
        return None
    m = re.search(r"[:/]([^/:]+)/([^/]+?)(?:\.git)?/?$", url)
    if not m:
        return None
    return "%s/%s" % (m.group(1), m.group(2))


BOT_LOGINS = ("github-actions", "dependabot", "codecov", "claude", "sonarcloud",
              "chatgpt-codex-connector", "coderabbitai")


# gh exits non-zero for "there is no PR for this branch" exactly as it does for offline,
# unauthenticated and timed out - and collapsing those into one UNKNOWN would make every fresh
# branch print an alarm, which is how a reminder becomes noise and gets scrolled past. Its stderr
# does distinguish them, so the confirmed-absent case is read off that and reported as a fact.
NO_PR_RE = re.compile(r"no (pull requests|pr)s? found|could not (find|resolve) any pull request",
                      re.IGNORECASE)


def pr_facts(slug, branch):
    """(facts_or_None, problem_or_None, confirmed_absent). Cached; a cache miss is not fatal."""
    cache = os.path.join(
        TMP, "pc-branch-context-pr-%s.json"
        % hashlib.sha1(("%s#%s" % (slug, branch)).encode("utf-8", "replace")).hexdigest()[:16])
    try:
        if os.path.exists(cache) and (time.time() - os.path.getmtime(cache)) < PR_CACHE_SECONDS:
            with open(cache, encoding="utf-8") as fh:
                blob = json.load(fh)
            return blob.get("facts"), blob.get("problem"), bool(blob.get("absent"))
    except Exception:
        pass

    absent = False
    if not _have_gh:
        return None, "`gh` is not on PATH", False
    ok, out, err = run(["gh", "pr", "view", branch, "-R", slug, "--json",
                        "number,title,body,url,comments,reviews,state,isDraft"], seconds=GH_SECONDS)
    if not ok:
        if NO_PR_RE.search(err or ""):
            facts, problem, absent = None, None, True
        else:
            facts, problem = None, ("`gh pr view %s -R %s` returned nothing within %ds - offline, "
                                    "unauthenticated, rate-limited or slow"
                                    % (branch, slug, GH_SECONDS))
    else:
        try:
            d = json.loads(out)
            authors = {}
            for c in d.get("comments") or []:
                login = ((c.get("author") or {}).get("login")) or "?"
                authors[login] = authors.get(login, 0) + 1
            # AGGREGATED BY (author, state). A PR here can carry twenty identical
            # "claude COMMENTED" entries, and printing them one per line is twenty lines carrying
            # one fact - the sort of bulk that teaches a reader to skip the whole block.
            rev_counts = {}
            for r in d.get("reviews") or []:
                key = ((((r.get("author") or {}).get("login")) or "?"), r.get("state") or "?")
                rev_counts[key] = rev_counts.get(key, 0) + 1
            reviews = [[a, st, n] for (a, st), n in sorted(rev_counts.items())]
            facts = {
                "number": d.get("number"),
                "title": d.get("title") or "",
                "url": d.get("url") or "",
                "state": d.get("state") or "",
                "draft": bool(d.get("isDraft")),
                "body_lines": len((d.get("body") or "").splitlines()),
                "comment_authors": authors,
                "reviews": reviews,
            }
            problem = None
        except Exception:
            facts, problem = None, "`gh` returned output this hook could not parse"
    try:
        with open(cache, "w") as fh:
            json.dump({"facts": facts, "problem": problem, "absent": absent}, fh)
    except Exception:
        pass
    return facts, problem, absent


_have_gh = False
for d in (os.environ.get("PATH") or "").split(os.pathsep):
    if d and os.path.isfile(os.path.join(d, "gh")) and os.access(os.path.join(d, "gh"), os.X_OK):
        _have_gh = True
        break

slug = origin_slug(target)
if slug is None:
    pr, pr_problem, pr_absent = (
        None, "`origin` is absent or is not a remote URL (a local-path clone has no repository to "
              "ask about), so `gh` was not called", False)
else:
    pr, pr_problem, pr_absent = pr_facts(slug, branch)

# --------------------------------------------------------------------------------------------
# Emit
# --------------------------------------------------------------------------------------------

# CORRECT SILENCE vs DEGRADED SILENCE. Nothing to say (no commits, no notes, no marker, and a
# CONFIRMED absence of a PR) is a real answer and stays quiet. Anything that could not be BUILT
# speaks up, because a shorter block that reads complete is how a broken hook passes for a healthy
# one - measured in this repo, in this repo's other injection hook.
degraded = [p for p in (base_problem, commits_problem, notes_problem, pr_problem) if p]
if not (commit_lines or notes_lines or marker_lines or pr or degraded):
    sys.exit(0)

L = []
L.append("# Branch context: `%s`" % branch)
L.append("")
L.append("Repository state for `%s` - %s. Produced by `.claude/hooks/inject-branch-context.sh`, "
         "registered in this repository's `.claude/settings.json`; it is a report, not an "
         "instruction." % (target, why))
L.append("")
L.append("**You were handed this branch. Read its own record before you change it.** AGENTS.md's "
         "\"Read the record you inherit\" covers the trigger where your BASE moved; this is the "
         "other trigger, same rule. A simplify, dedupe or review pass that has not read the "
         "branch's commits, its PR body and its PR comments reverses decisions the branch made on "
         "purpose - which is exactly what happened on 2026-08-24 across five PRs at once.")
L.append("")

if base_problem:
    L.append("## Commits - COULD NOT BE BUILT")
    L.append("")
    L.append("Not empty, MISSING: %s. Do not read this absence as \"the branch has no commits\"."
             % base_problem)
    L.append("")
elif commits_problem:
    L.append("## Commits - COULD NOT BE BUILT")
    L.append("")
    L.append("Not empty, MISSING: %s. Do not read this absence as \"the branch has no commits\"."
             % commits_problem)
    L.append("")
else:
    L.append("## Commits on this branch (%d), `%s..HEAD` via `%s`"
             % (len(commit_lines) + extra_commits, merge_base[:9], base_ref))
    L.append("")
    if commit_lines:
        L.append("`[n]` is the commit body's non-empty line count, `[-]` means no body. Read the "
                 "big ones - `git -C %s show -s <sha>`." % target)
        L.append("")
        L.extend(commit_lines)
        if extra_commits:
            L.append("- ... and %d older commits, NOT LISTED (capped at %d)." % (extra_commits, COMMIT_CAP))
    else:
        L.append("(none - the branch is level with `%s`)" % base_ref)
    L.append("")

if notes_problem:
    L.append("## Branch-only notes - COULD NOT BE BUILT")
    L.append("")
    L.append("Not empty, MISSING: %s." % notes_problem)
    L.append("")
elif notes_lines:
    L.append("## Notes this branch adds or changes under `docs/inflight/` and `docs/plans/`")
    L.append("")
    L.append("This is how a handoff document announces itself. Read it before the code.")
    L.append("")
    L.extend(notes_lines)
    L.append("")

if pr_problem:
    L.append("## Open PR - UNKNOWN")
    L.append("")
    L.append("Not \"no PR\", UNKNOWN: %s. Check it yourself before assuming this branch is "
             "unreviewed." % pr_problem)
    L.append("")
elif pr:
    num, slug_disp = pr.get("number"), slug
    L.append("## PR %s#%s - %s" % (slug_disp, num, pr.get("title") or ""))
    L.append("")
    L.append("- state: %s%s  %s" % (pr.get("state") or "?", " (draft)" if pr.get("draft") else "",
                                    pr.get("url") or ""))
    if pr.get("body_lines"):
        L.append("- body: %d lines. **Read it.** A PR body here routinely defends, by name, a "
                 "decision that a simplify pass would reverse on sight." % pr["body_lines"])
    else:
        L.append("- body: EMPTY.")
    authors = pr.get("comment_authors") or {}
    if authors:
        # LISTED BY AUTHOR, not classified into human and bot. This repo's PRs carry five or more
        # bot comments, so a bare "7 comments" hides the one that matters - and a login is a fact
        # while "is a human" is a guess this hook would get wrong for an outside contributor.
        who = ", ".join("%s x%d" % (k, v) for k, v in sorted(authors.items()))
        non_bot = [k for k in authors if not (k.endswith("[bot]") or k in BOT_LOGINS)]
        L.append("- %d comments: %s" % (sum(authors.values()), who))
        if non_bot:
            L.append("  **Read the ones from %s.** The 2026-08-24 miss was a PR COMMENT posted "
                     "after the body - a ride-along scope addition - which a hook that only "
                     "noticed the body would have missed too." % ", ".join(sorted(non_bot)))
    else:
        L.append("- comments: none")
    revs = pr.get("reviews") or []
    if revs:
        L.append("- %d reviews: %s"
                 % (sum(r[2] for r in revs),
                    ", ".join("%s %s x%d" % (r[0], r[1], r[2]) for r in revs)))
    else:
        L.append("- reviews: none")
    L.append("- read them: `gh pr view %s -R %s --comments`" % (num, slug_disp))
    L.append("")
elif pr_absent:
    L.append("## PR: none open for this branch")
    L.append("")
    L.append("Measured, not assumed - `gh` was asked and answered.")
    L.append("")

if marker_lines:
    L.append("## `.worktree-owner`")
    L.append("")
    L.extend("    " + ln for ln in marker_lines)
    L.append("")

if mode == "dispatch":
    L.append("_You are DISPATCHING. This block reached you alongside the subagent's result, not "
             "before it - a `PreToolUse` hook cannot alter the call it fires on. Use it to judge "
             "what came back, and to compose the next dispatch._")
    L.append("")

body = "\n".join(L).rstrip() + "\n"

if mode == "session":
    # A SessionStart hook's plain stdout is injected; the JSON envelope is only mandatory for
    # PreToolUse, whose raw stdout is discarded.
    sys.stdout.write(body)
else:
    print(json.dumps({"hookSpecificOutput": {
        "hookEventName": "PreToolUse",
        "permissionDecision": "allow",
        "additionalContext": body,
    }}))
PY
exit 0
