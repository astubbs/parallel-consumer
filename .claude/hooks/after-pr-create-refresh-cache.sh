#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#
# PostToolUse hook: after `gh pr create` succeeds, fold the new PR into the local cache and say so.
#
# WHY THIS EXISTS. `bin/inflight.mjs` caches the repository's PR set, and every command that names a
# branch reads it - `branch`, `note drift`, the tracking-gap detector. A PR created seconds ago is
# absent from that cache, so the tool reports the branch that PR is FOR as having no PR, and the
# detector then tells the agent to write a tracking note for work that is already tracked. Being
# wrong in that direction is the failure that gets a detector ignored.
#
# The cache used to expire after thirty minutes, which made the wrong answer temporary rather than
# absent. This is why it can now be held for twenty-four hours: the writers are the people working in
# this repository, and each write updates the cache as it happens, so the TTL became a backstop for
# changes made OUTSIDE this machine rather than the mechanism that makes the cache correct.
#
# COSTS ONE `gh pr view`. It folds a single PR in rather than refetching the set - which at 285 PRs
# is 56K, and 2.3MB if bodies were carried.
#
# NO `if` PREFIX MATCH. bin/AGENTS.md records that check-squash-subject.sh shipped with one and
# missed every command shape it existed for, so the cheap `case` below only skips obvious non-matches
# and the real decision is made on the parsed payload.
#
# FAILS SILENT, DELIBERATELY. A cache refresh is a convenience; it must never fail a tool call or
# emit noise when there is nothing to say. Every unreadable payload, missing interpreter and failed
# refresh exits 0 with no output.

set -o pipefail

payload="$(cat)"

case "$payload" in
    *'gh'*'pr'*'create'*) ;;
    *) exit 0 ;;
esac

command -v python3 >/dev/null 2>&1 || exit 0

python3 - "$payload" <<'PY'
import json, os, re, subprocess, sys

try:
    payload = json.loads(sys.argv[1])
except Exception:
    sys.exit(0)  # never break a tool call over a payload we cannot read

command = (payload.get("tool_input") or {}).get("command") or ""

# `gh pr create` as whole words, so `cd x && gh pr create ...` counts. A --dry-run creates nothing.
if not re.search(r"\bgh\b.*\bpr\b.*\bcreate\b", command):
    sys.exit(0)
if re.search(r"--dry-run\b", command):
    sys.exit(0)

response = payload.get("tool_response") or {}
if isinstance(response, dict):
    output = " ".join(str(response.get(k) or "") for k in ("stdout", "stderr", "output", "content"))
else:
    output = str(response or "")

# gh prints the new PR's URL on success. No URL means no PR was created - a validation failure, a
# missing base, an existing PR - and there is nothing to fold in.
match = re.search(r"https://[^\s]*/pull/(\d+)", output)
if not match:
    sys.exit(0)
number = match.group(1)

root = os.environ.get("CLAUDE_PROJECT_DIR") or os.getcwd()
tool = os.path.join(root, "bin", "inflight.mjs")
if not os.path.exists(tool):
    sys.exit(0)

try:
    run = subprocess.run(
        ["node", tool, "cache", "pr", number],
        capture_output=True, text=True, timeout=30, cwd=root,
    )
except Exception:
    sys.exit(0)

if run.returncode != 0:
    sys.exit(0)

print(json.dumps({
    "hookSpecificOutput": {
        "hookEventName": "PostToolUse",
        "additionalContext": (
            f"The in-flight tool's PR cache now includes astubbs/parallel-consumer#{number}, folded "
            "in automatically when you created it.\n"
            "\n"
            "This matters for what you do next: `bin/inflight.mjs branch <ref>` and its tracking-gap "
            "detector read that cache, and without the refresh they would have reported the branch "
            "you just opened a PR for as having no PR - and told you to write a tracking note for "
            "work that is already tracked.\n"
            "\n"
            "You do not need to run anything. `bin/inflight.mjs cache` shows what is held and how "
            "old it is."
        ),
    },
}))
PY
