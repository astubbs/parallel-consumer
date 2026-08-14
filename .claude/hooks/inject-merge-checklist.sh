#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# UserPromptSubmit hook: when a prompt looks like merge preparation, put docs/merge-checklist.md in
# front of the agent - at the moment it is deciding, not in a document it might read afterwards.
#
# WHY THIS EVENT. PreToolUse cannot inject context (its stdout never reaches the model; it can only
# allow, deny or ask), so it cannot be used to remind an agent of anything. UserPromptSubmit can,
# via hookSpecificOutput.additionalContext. See docs/agent-harness.md for the full layer map.
#
# NEUTRALITY. This script holds no advice of its own - it prints docs/merge-checklist.md, which is
# tool-neutral and routed from AGENTS.md. Codex and anything else reading AGENTS.md gets the same
# words from the same file; only the delivery differs.
#
# It never blocks. The point is to inject a thought at the right time, not to gate anything.

set -euo pipefail

payload=$(cat)

project_dir="${CLAUDE_PROJECT_DIR:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"
checklist="$project_dir/docs/merge-checklist.md"
[ -r "$checklist" ] || exit 0

# Match merge-prep intent. Deliberately broad on the verbs and narrow on the nouns: a false positive
# costs a few hundred tokens of checklist, a false negative costs the thing this exists to prevent.
if ! python3 - "$payload" <<'PY'
import json, re, sys
try:
    prompt = json.loads(sys.argv[1]).get("prompt", "")
except Exception:
    sys.exit(1)
pattern = re.compile(
    r"\b(squash|rebase|merge|mergeable|land(ing)?\s+(it|this|the\s+pr)|ready\s+to\s+merge|"
    r"good\s+to\s+merge|ship\s+it|tidy\s+(up\s+)?the\s+commits|commit\s+history|"
    r"reorganis|reorganiz|fixup|autosquash)\b",
    re.IGNORECASE,
)
sys.exit(0 if pattern.search(prompt) else 1)
PY
then
    exit 0
fi

python3 - "$checklist" <<'PY'
import json, sys
body = open(sys.argv[1], encoding="utf-8").read()
print(json.dumps({
    "hookSpecificOutput": {
        "hookEventName": "UserPromptSubmit",
        "additionalContext": (
            "This prompt looks like merge preparation. The repo's merge checklist "
            "(docs/merge-checklist.md) follows. Its two standing asks are to OFFER to write the "
            "squash message, and to OFFER to reorganise the commits into cohesive units - offer "
            "both, do not silently do either.\n\n" + body
        ),
    }
}))
PY
