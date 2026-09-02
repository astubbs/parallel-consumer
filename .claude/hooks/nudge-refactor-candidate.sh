#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# PreToolUse hook on the file-editing tools: when an agent edits a file this repo has decided to
# decompose, say what is worth extracting - and never stand in the way of the edit.
#
# WHY THIS EXISTS
#
# The other half of the refactor-window signal. remind-refactor-window.sh answers "is now a good
# time", which reaches whoever is choosing what to work on. This one reaches the moment that
# actually grew AbstractParallelEoSStreamProcessor from the 1533 lines docs/refactoring.md records
# to 2405: somebody with a change to make, for whom adding to the file is the locally correct move
# and extracting is a decision nobody prompted them to consider. docs/agent-harness.md calls this
# making code teach an agent how to use it at the moment of use.
#
# ADVISORY, NEVER A GUARD. It cannot block, ask, or delay - CONCEPTS.md's "Advisory reminder" sets
# the failure budget: silence is acceptable, blocking is not. A false positive here must not be able
# to cost anybody a turn, which is why this is not allowed to fail closed and why it never asks the
# agent to justify anything.
#
# NO SIGNAL, ONLY THE CONFIG. It runs in front of EVERY file edit, so the ~3.4s cross-branch
# measurement is unaffordable and irrelevant: the hint does not depend on how busy the file is.
# `refactor-window --hint-for` reads one small JSON file and exits in ~28ms.
#
# ONE python3 DOES ALL THE JSON, both directions. Reading `tool_input.file_path` out of the payload
# in bash and re-escaping a hint containing backticks and quotes back into JSON are both the class of
# fragility bin/AGENTS.md keeps a gate for; and doing the extraction in python3 while wrapping in
# another process would cost a third fork per edit.
#
# NOTEBOOKS CARRY A DIFFERENT FIELD. `NotebookEdit` uses `notebook_path` where `Edit` and `Write` use
# `file_path`, so reading only the latter would silently never fire for one of the three tools it is
# registered against.
#
# THE MATCHER IS THE RISK, and it is checked rather than assumed: this is the first hook here to
# match anything other than `Bash` or `*`, and .claude/hooks/inject-branch-context.sh records the
# precedent for the documented tool name and the payload name disagreeing - the Task tool's real
# `tool_name` is `Agent`. bin/test-check-agent-hooks.sh drives this script with real payload shapes.
#
# NOT REACHED BY A SHELL EDIT. `sed -i`, a heredoc, or any other edit made through Bash fires no
# file-editing tool matcher, so this cannot see it. That is a limit of where a hook can attach; the
# plan records it as a scope boundary rather than a defect.
#
# COST, measured rather than assumed, because this is the hottest hook here - it runs in front of
# every single edit: ~50ms end to end, of which ~28ms is node, ~9ms python3, and the rest bash and
# pipe overhead. Sourcing hook-common.sh for one shared helper adds ~0.3ms and was measured before
# being accepted. An earlier note here said 28ms, which was the node component rather than the hook.
#
# shell-justified: this is a hook, matching every other script in this directory; bin/AGENTS.md's
# Node-default rule is scoped to bin/ and does not reach here.
#
# Never blocks an edit: any error prints nothing and exits 0.

set -uo pipefail

payload="$(cat 2>/dev/null || true)"
[ -n "$payload" ] || exit 0

# Free bail before any fork. Every payload this can act on names one of the two path fields.
case "$payload" in
    *file_path*|*notebook_path*) ;;
    *) exit 0 ;;
esac

hook_lib="${BASH_SOURCE[0]%/*}/lib/hook-common.sh"
[ -r "$hook_lib" ] || exit 0
# shellcheck source=lib/hook-common.sh
. "$hook_lib"

# The payload's cwd wins over the session's, and the tool is invoked by ABSOLUTE path so nothing in
# the payload can choose which program runs. hook_project_root's header owns the ordering.
root="$(hook_project_root "$payload")"
[ -n "$root" ] || exit 0
inflight="$root/bin/inflight.mjs"
[ -f "$inflight" ] || exit 0
command -v node >/dev/null 2>&1 || exit 0
command -v python3 >/dev/null 2>&1 || exit 0

# ONE NUDGE PER CANDIDATE PER HOUR, not one per edit. CONCEPTS.md's own "Advisory reminder" entry
# says a reminder "that fires too often trains its reader to skip it, which is the same end state as
# one that never fires", and a full paragraph in front of every Edit to the same file is exactly
# that. The two sibling reminders here already throttle for this reason. This is NOT the no-throttle
# rule that governs remind-refactor-window.sh: that one must repeat until the work is done, because
# repeating IS its signal. Here the signal is the arrival of an edit, and it has already arrived.
: "${REFACTOR_NUDGE_SECONDS:=3600}"
export NUDGE_STAMP_PREFIX="$(hook_stamp_path pc-refactor-nudge '')"
export NUDGE_WINDOW="$REFACTOR_NUDGE_SECONDS"
export INFLIGHT_BIN="$inflight"

printf '%s' "$payload" | python3 -c '
import json, os, subprocess, sys, time

try:
    payload = json.load(sys.stdin)
except Exception:
    sys.exit(0)

edited = payload.get("tool_input", {})
path = edited.get("file_path") or edited.get("notebook_path") or ""
if not path:
    sys.exit(0)

try:
    proc = subprocess.run(
        ["node", os.environ["INFLIGHT_BIN"], "refactor-window", "--hint-for", path],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=15,
    )
except Exception:
    sys.exit(0)

# NAMED, NOT DISCARDED, per AGENTS.md: "a bare call reads identically whether ignoring the result was
# a decision or an oversight". A broken config makes this exit non-zero with its reason on stderr;
# staying silent here is deliberate, because remind-refactor-window.sh is the arm that says so
# loudly and an advisory nudge must not duplicate a shout.
if proc.returncode != 0:
    sys.exit(0)

hint = proc.stdout.decode("utf-8", "replace").strip()
if not hint:
    sys.exit(0)

# Throttled per candidate: the id is the part before the colon that --hint-for prints.
stamp = os.environ["NUDGE_STAMP_PREFIX"] + hint.split(":", 1)[0].replace("/", "_")
try:
    window = int(os.environ.get("NUDGE_WINDOW", "3600"))
except ValueError:
    window = 3600
try:
    if time.time() - os.path.getmtime(stamp) < window:
        sys.exit(0)
except OSError:
    pass
try:
    with open(stamp, "w") as fh:
        fh.write("")
except OSError:
    pass

print(json.dumps({"hookSpecificOutput": {"hookEventName": "PreToolUse", "additionalContext": (
    "You are editing a file this repo has already decided to decompose. Not a blocker, and not a "
    "reason to stop what you are doing - but if the piece you are touching can leave as its own "
    "unit, that IS the tracked work, and doing it here is cheaper than doing it later.\n\n"
    + hint + "\n\n"
    "Whichever you choose, say which in the commit - a file that grows for good reasons and a file "
    "nobody considered splitting look identical afterwards. "
    "`node bin/inflight.mjs refactor-window` says how expensive a full decomposition is right now."
)}}))
' 2>/dev/null || true
exit 0
