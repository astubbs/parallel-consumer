#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# PostToolUse hook: after a `git push` that actually moved a ref, tell the agent that CI is now
# running and what it has to go and read.
#
# WHY THIS EXISTS. On astubbs#267 the required `dups: clones` check found a real 31-line clone
# between MutinyProcessor and ReactorProcessor, failed the build for it, and could not post it - the
# inline annotation was rejected by GitHub ("pull_request_review_thread.line could not be resolved"),
# and nothing fell back to a summary comment. So the finding existed only in the job log. The agent
# pushed and moved on without waiting for CI; the next push happened to shrink the clone under the
# threshold, the check went green, and the duplication survived four more commits until a human read
# the code. Full write-up: docs/inflight/ci-duplication-report-can-fail-to-post.md.
#
# The gap is not detection and not thresholds - both worked. It is that a red check whose comment
# failed to post is indistinguishable from any other red tick, and that a *later* push can clear the
# red while the problem remains. A reminder at push time is the only layer that fires between "the
# agent pushed" and "the agent has stopped thinking about this push".
#
# WHAT IT CANNOT DO. It fires seconds after the push, minutes before check-runs exist - so it cannot
# check anything. It only injects the instruction to check, and names what to read. That is a real
# limit, not a placeholder: anything that waits for CI has to be a command the agent runs, not a hook.
#
# NO `if:` MATCHER, DELIBERATELY. docs/agent-harness.md, "...and `if` matches a PREFIX": an
# `if: "Bash(git push *)"` fires only when the command *starts* with `git push`, so `cd worktree &&
# git push` - the overwhelmingly common shape in this repo, which works in worktrees - would never
# reach it. This follows check-squash-subject.sh instead: registered on every Bash call, with a shell
# grep that rejects the vast majority before python starts.
#
# Self-tested by bin/test-check-agent-hooks.sh, including the negative controls (a push that changed
# nothing, a non-push command, a failed push).

set -euo pipefail

payload="$(cat)"

# Cheap reject first - most Bash calls are not a push at all.
case "$payload" in
    *'git push'*|*'git'*'push'*) ;;
    *) exit 0 ;;
esac

python3 - "$payload" <<'PY'
import json, re, sys

try:
    payload = json.loads(sys.argv[1])
except Exception:
    sys.exit(0)  # never break a tool call over a payload we cannot read

command = (payload.get("tool_input") or {}).get("command") or ""

# `git push` as a whole word, anywhere in the command, so `cd x && git push` and
# `/usr/local/bin/git push` both count. `git push --dry-run` deliberately does not.
if not re.search(r"\bgit\s+push\b", command):
    sys.exit(0)
if re.search(r"--dry-run\b|(^|\s)-n(\s|$)", command):
    sys.exit(0)

response = payload.get("tool_response") or {}
output = ""
if isinstance(response, dict):
    output = " ".join(
        str(response.get(k) or "") for k in ("stdout", "stderr", "output", "content")
    )
elif isinstance(response, str):
    output = response

# A push that moved nothing needs no CI reminder, and firing on it would train the reader to skim
# past this block. Git says so on stderr, in every porcelain and quiet mode.
if "Everything up-to-date" in output:
    sys.exit(0)

# A push that failed did not start CI either.
if re.search(r"\[rejected\]|! \[remote rejected\]|fatal:|error: failed to push", output):
    sys.exit(0)

print(json.dumps({
    "hookSpecificOutput": {
        "hookEventName": "PostToolUse",
        "additionalContext": (
            "You just pushed, so CI is starting on the new head. Two things about this repo's "
            "checks that have already cost it once, both recorded in "
            "docs/inflight/ci-duplication-report-can-fail-to-post.md:\n"
            "\n"
            "1. A RED CHECK CAN HAVE NO COMMENT. `dups: clones` is a required check, and it has "
            "failed on this repo with its finding posted nowhere - GitHub rejected the inline "
            "annotation and nothing fell back to a summary comment. If a check is red and you "
            "cannot find a comment explaining it, the finding is in the job log: "
            "`gh run view -R astubbs/parallel-consumer --job <id> --log`. Do not assume a red tick "
            "with no comment is infrastructure noise.\n"
            "\n"
            "2. A LATER PUSH CAN CLEAR A RED WITHOUT FIXING ANYTHING. Checks are per-head and the "
            "duplication gates are threshold-based, so partially removing a problem looks identical "
            "to removing it. Green on a later head is not evidence that an earlier red was "
            "addressed - if you saw one, close it out rather than letting the next push bury it.\n"
            "\n"
            "So: before you report on CI, or move on from this push, wait for the checks on THIS "
            "head and look at each non-green one individually. AGENTS.md's 'follow up on the "
            "duplication reports' is the rule this implements."
        ),
    }
}))
PY
