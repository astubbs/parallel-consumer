#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# UserPromptSubmit hook: when a prompt looks like merge preparation, put docs/merge-checklist.md in
# front of the agent - at the moment it is deciding, not in a document it might read afterwards.
#
# WHY THIS EVENT. Not because PreToolUse cannot inject context - it can, via
# hookSpecificOutput.additionalContext (verified live against 2.1.223; only its *stdout* is
# discarded). The reason is timing. PreToolUse fires per tool call, so the checklist would arrive
# attached to whichever command happened to run next, repeatedly, and never at the moment the
# strategy is actually being chosen. UserPromptSubmit fires when the human states the intent, which
# is the decision point. See docs/agent-harness.md for the full layer map.
#
# NEUTRALITY. This script holds no advice of its own - it prints docs/merge-checklist.md, which is
# tool-neutral and routed from AGENTS.md. Codex and anything else reading AGENTS.md gets the same
# words from the same file; only the delivery differs. That claim is load-bearing, so the injected
# preamble below says only where the text came from: an earlier version also summarised the
# checklist's two standing asks, which is precisely the second copy this design exists to avoid -
# the summary drifts from the doc, and the doc is what everything else reads.
#
# It never blocks. The point is to inject a thought at the right time, not to gate anything.

set -euo pipefail

# THE PAYLOAD ARRIVES BY FILE, NOT BY ARGV. Linux caps a single argv string at ~128 KiB
# (MAX_ARG_STRLEN), and a hook payload carries the whole prompt or command - a pasted diff or log
# clears that easily. Passing it as an argument then fails with "Argument list too long" BEFORE
# python starts, and since these hooks are built to fail open, the failure is silent: the hook
# simply stops doing its job on exactly the large inputs a human is most likely to be mid-decision
# on. A temp file has no such limit. mktemp is 0600 and the trap removes it.
payload_file=$(mktemp)
trap 'rm -f "$payload_file"' EXIT
cat > "$payload_file"

project_dir="${CLAUDE_PROJECT_DIR:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"
checklist="$project_dir/docs/merge-checklist.md"
[ -r "$checklist" ] || exit 0

# Match merge-prep intent. Deliberately broad on the verbs and narrow on the nouns: a false positive
# costs a few hundred tokens of checklist, a false negative costs the thing this exists to prevent.
if ! python3 - "$payload_file" <<'PY'
import json, re, sys
try:
    with open(sys.argv[1], encoding="utf-8") as fh:
        prompt = json.load(fh).get("prompt", "")
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
            "This prompt looks like merge preparation, so the repo's merge checklist follows "
            "verbatim from docs/merge-checklist.md, which owns it.\n\n" + body
        ),
    }
}))
PY
