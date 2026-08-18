#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# PreToolUse hook: refuse a `gh pr merge` whose `--subject` does not end with the PR number.
#
# THE TRAP. `gh pr merge --squash` lands a subject ending `... (#265)` because GitHub appends the
# number - but only when the subject is NOT overridden. Override it and your text is used verbatim,
# the number silently never appears, and the commit lands out of step with every neighbour on
# master. That happened on astubbs#206 and needed a force-push to master to correct, which is why
# this is a hook and not a line in a document: the mistake is invisible at the point of failure.
#
# THE RULE, WHOLE: if the command overrides the subject, that subject must end with `(#N)`. Ending
# is the point - it is the slot GitHub itself uses, and the slot AGENTS.md reserves.
#
# BOTH SPELLINGS. `gh pr merge --help` documents `-t, --subject text`, and `-t` is the shorter one,
# so it is the one a hand-typed merge reaches for. A guard that knows only the long form is a guard
# with a documented way around it.
#
# TOKENS, NOT SUBSTRINGS. The value is read from the parsed argument list of the `gh pr merge`
# command itself, so `--body "explain why --subject matters"` is not mistaken for an override, and
# the flag is found wherever it sits.
#
# THE PAYLOAD GOES THROUGH A TEMP FILE, and only its NAME through argv. Passed as an argument the
# payload itself hits E2BIG on a large command - measured with a 150 KB one - and the hook then
# exits non-2 having printed nothing, which reads as ALLOW. A guard that fails open on big inputs
# is worse than no guard, because it looks present. Note the program cannot simply read stdin
# instead: stdin is where the heredoc delivers the program.
#
# THE BODY IS GUARDED TOO, for squash merges. A squash body override (--body/-b/--body-file/-F)
# must carry a `Co-authored-by:` trailer - any merge performed through this harness is
# agent-assisted by definition, and the sign-off is the convention master's squash commits carry -
# and must NOT carry a `Claude-Session:` line, which belongs on ordinary commits only
# (docs/merge-checklist.md owns both rules). A --body-file is read when it is a readable file;
# when it is not (process substitution not yet materialised, typo), the hook fails open because
# `gh` itself will then fail loudly on the missing file - a guard needs no teeth where the tool
# already bites.
#
# FAILS OPEN otherwise, deliberately: unparseable JSON, unbalanced quotes, no python3. A hook that
# blocks on its own bug jams the tool call shut, and docs/merge-checklist.md still carries the rule
# for anyone merging by hand.

set -euo pipefail

payload_file=$(mktemp)
trap 'rm -f "$payload_file"' EXIT
cat > "$payload_file"

python3 - "$payload_file" <<'PY'
import json, re, shlex, sys

try:
    with open(sys.argv[1], encoding="utf-8") as fh:
        tool = json.load(fh)
except Exception:
    sys.exit(0)                          # never block on our own parse failure

if tool.get("tool_name") != "Bash":
    sys.exit(0)

cmd = tool.get("tool_input", {}).get("command", "")

SUBJECT_FLAGS = ("--subject", "-t")
MERGE = re.compile(r"\bgh\s+pr\s+merge\b")

# One command line can hold more than one `gh pr merge`; each is judged on its own slice so a good
# merge cannot vouch for a bad one.
starts = [m.start() for m in MERGE.finditer(cmd)]
for start, end in zip(starts, starts[1:] + [len(cmd)]):
    try:
        tokens = shlex.split(cmd[start:end])
    except ValueError:
        continue                         # our own parse limit, not the author's mistake

    subject = None
    i = 0
    while i < len(tokens):
        t = tokens[i]
        if t in SUBJECT_FLAGS and i + 1 < len(tokens):
            subject, i = tokens[i + 1], i + 2
            continue
        for flag in SUBJECT_FLAGS:
            if t.startswith(flag + "="):
                subject = t[len(flag) + 1:]
                break
            # A SHORT flag may carry its value attached: -tbad == -t bad. Long flags do not.
            if len(flag) == 2 and t.startswith(flag) and len(t) > 2:
                subject = t[2:]
                break
        i += 1

    def deny(reason):
        print(json.dumps({
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "deny",
                "permissionDecisionReason": reason + " See docs/merge-checklist.md.",
            }
        }))

    pr = next((t for t in tokens[tokens.index("merge") + 1:] if t.isdigit()), None) \
        if "merge" in tokens else None
    hint = f"(#{pr})" if pr else "(#<pr>)"

    if subject is not None and not re.search(r"\(#\d+\)\s*$", subject):
        deny(
            f"This --subject does not end with {hint}. Overriding the subject suppresses the "
            "PR number GitHub would otherwise append, so the commit lands out of step with "
            "every neighbour on master - and that is not fixable afterwards without rewriting "
            f"a pushed commit (astubbs#206). End the subject with ' {hint}', or drop the flag "
            "and let the PR title be used. --body/--body-file do not affect the subject."
        )
        break

    # Squash-body sign-off: same token walk as the subject, for the body flags.
    if "--squash" not in tokens and "-s" not in tokens:
        continue
    body = None
    i = 0
    while i < len(tokens):
        t = tokens[i]
        if t in ("--body", "-b") and i + 1 < len(tokens):
            body, i = tokens[i + 1], i + 2
            continue
        if t in ("--body-file", "-F") and i + 1 < len(tokens):
            path = tokens[i + 1]
            i += 2
            try:
                with open(path, encoding="utf-8") as fh:
                    body = fh.read()
            except OSError:
                body = None              # gh will fail loudly on it; nothing to guard
            continue
        for flag in ("--body", "--body-file"):
            if t.startswith(flag + "="):
                val = t[len(flag) + 1:]
                if flag == "--body":
                    body = val
                else:
                    try:
                        with open(val, encoding="utf-8") as fh:
                            body = fh.read()
                    except OSError:
                        body = None
                break
        else:
            for flag in ("-b", "-F"):
                if t.startswith(flag) and len(t) > 2:
                    val = t[2:]
                    if flag == "-b":
                        body = val
                    else:
                        try:
                            with open(val, encoding="utf-8") as fh:
                                body = fh.read()
                        except OSError:
                            body = None
                    break
        i += 1

    if body is None:
        continue                         # no override, or unreadable file gh will reject itself
    if not re.search(r"^co-authored-by:", body, re.I | re.M):
        deny(
            "This squash --body carries no Co-authored-by trailer. A merge performed through "
            "this harness is agent-assisted, and master's squash commits carry exactly one "
            "sign-off line: 'Co-authored-by: Claude <model> (1M context) <noreply@anthropic.com>'. "
            "Add it as the body's final line."
        )
        break
    if re.search(r"^claude-session:", body, re.I | re.M):
        deny(
            "This squash --body carries a Claude-Session: line. Squash-merge commits on master "
            "carry only the Co-authored-by trailer; the session link belongs on ordinary "
            "branch commits, not the permanent squash record. Remove the Claude-Session line."
        )
        break
PY
