#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# PreToolUse hook: refuse a `gh pr merge` while upstream-map.yaml still calls this PR `pr-open`.
#
# THE RULE IS "SET IT TO `merged` IN THE BRANCH, THEN MERGE." That reads like a lie and is not one:
# branch content is visible to nobody until it lands, and the moment it lands the statement is true.
# So the manifest is never wrong for a single observable instant, and there is nothing to clean up
# afterwards - which is the whole reason this is the fix rather than an after-the-fact audit.
#
# WHY A HOOK AND NOT A CHECK SCRIPT. The window this closes is one merge wide and only exists for an
# agent about to merge, so a CI job would have to run on master or on a schedule to see it at all,
# and would then report a mess that already happened. Refusing the merge is smaller, catches it at
# the only moment it is cheap to fix, and needs no scheduled job, no gh queries and no second copy
# of the manifest's schema. It also expires cleanly: when the last upstream link is closed out, this
# file is deleted and nothing else has to be unpicked.
#
# WHAT IT DOES NOT DO. It does not check `branches:`, `fork_issue`, or any other field, and it does
# not verify the status is *right* - only that a PR being merged is not still recorded as open. The
# manifest's schema is already validated by `scripts/upstream-map.py validate`.
#
# FAILS OPEN, deliberately, on every uncertainty: no python3, no PyYAML, unreadable or unparseable
# manifest, no PR number on the command line, or no entry naming that PR. A hook that blocks on its
# own bug jams the tool shut, and docs/upstream.md still carries the rule for anyone merging by hand.

set -euo pipefail

payload_file=$(mktemp)
trap 'rm -f "$payload_file"' EXIT
cat > "$payload_file"

# Cheap pre-filter before forking a Python interpreter: this hook is matcher-less, so without
# it every Bash call in every session pays for an interpreter start. Same guard the sibling
# hooks carry.
grep -q merge "$payload_file" || exit 0

python3 - "$payload_file" <<'PY'
import json, re, shlex, sys

try:
    with open(sys.argv[1], encoding="utf-8") as fh:
        tool = json.load(fh)
except Exception:
    sys.exit(0)

if tool.get("tool_name") != "Bash":
    sys.exit(0)

cmd = tool.get("tool_input", {}).get("command", "")

# `-R owner/repo` / `--repo=...` may sit between `gh` and `pr` OR between `pr` and `merge`, and
# AGENTS.md tells every agent in this repo to qualify gh commands - so a matcher that only accepts
# the bare form misses the shape the conventions actively train. Borrowed verbatim from
# check-squash-subject.sh, which grew this after the astubbs#324 review; a guard is only as good as
# the command forms it recognises.
_REPO_FLAG = r"(?:(?:-R|--repo)(?:\s+|=)?\S*\s+)*"
MERGE = re.compile(r"\bgh\s+" + _REPO_FLAG + r"pr\s+" + _REPO_FLAG + r"merge\b")

if not MERGE.search(cmd):
    sys.exit(0)

try:
    import yaml
    with open("src/docs/development/upstream-map.yaml", encoding="utf-8") as fh:
        manifest = yaml.safe_load(fh)
except Exception:
    sys.exit(0)                       # no manifest here, or no parser - nothing to assert

for m in MERGE.finditer(cmd):
    try:
        tokens = shlex.split(cmd[m.start():])
    except ValueError:
        continue
    def _pr_number(token):
        """A bare number, or the trailing number of a PR URL - gh accepts both interchangeably."""
        if token.isdigit():
            return token
        u = re.search(r"/pull/(\d+)(?:[/?#]|$)", token)
        return u.group(1) if u else None

    # Flags whose VALUE is a separate token. Without this, the first digit-shaped word after
    # `merge` wins - so `gh pr merge --body 2998 2999` reads the body's stray number as the PR and
    # then either denies the wrong merge or checks a PR nobody is merging. Both directions are
    # wrong, and the second is the quiet one.
    VALUE_FLAGS = {"--body", "-b", "--body-file", "-F", "--subject", "-t",
                   "--match-head-commit", "--repo", "-R", "--author-email"}

    try:
        after = tokens[tokens.index("merge") + 1:]
    except ValueError:
        continue

    pr, skip_next = None, False
    for tok in after:
        if skip_next:
            skip_next = False
            continue
        if tok in VALUE_FLAGS:        # `--body VALUE`: the value is the next token
            skip_next = True
            continue
        if tok.startswith("-"):       # `--body=VALUE` / `-tVALUE`: value is attached, not a token
            continue
        pr = _pr_number(tok)
        if pr:
            break
        break                         # first positional is the PR argument; if not a number, give up
    if pr is None:
        continue                      # merging the current branch's PR by name; number unknown

    for e in (manifest.get("entries") or []):
        fork = e.get("fork") or {}
        if int(pr) in (fork.get("prs") or []) and fork.get("status") == "pr-open":
            print(json.dumps({"hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "deny",
                "permissionDecisionReason": (
                    f"upstream-map.yaml entry '{e.get('id')}' still records astubbs#{pr} as "
                    "`status: pr-open`. Update this file to say merged, in the branch, and push "
                    "that before you merge - do not leave it to afterwards. Branch content is "
                    "visible to nobody until it lands, so it is correct the moment it does; "
                    "fixing it later means a commit straight to master. See docs/upstream.md."
                ),
            }}))
            sys.exit(0)
PY
