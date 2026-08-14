#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# PreToolUse hook: refuse a `gh pr merge --subject` whose subject does not carry the RIGHT PR number.
#
# THE TRAP. `gh pr merge --squash` normally lands a subject ending `... (#265)` - GitHub appends the
# PR number itself. It does that ONLY when the subject is not overridden. Pass `--subject "..."` and
# your text is used verbatim, so the number silently never appears. AGENTS.md reserves that trailing
# slot for exactly this, and every neighbouring commit on master has it.
#
# It is a good candidate for a hook rather than a rule because it is invisible at the point of
# failure: the merge succeeds, the message reads fine, and the omission only shows up later next to
# its neighbours in `git log`. That is what happened on astubbs#206, and rewriting a commit already
# on master is not a fix anyone should need.
#
# WHY THE PARSER IS SHLEX AND NOT A REGEX. The first version read the FIRST `--subject` with a
# regex over the whole command line. `gh` honours the LAST one, and the whole command line includes
# text that is not part of the merge at all. Both halves of that were wrong, in both directions:
#
#   - `--subject "ok (#299)" --subject "bad"` was ALLOWED - the bad subject is the one that lands.
#   - `echo --subject "x (#1)" ; gh pr merge 299 --subject "bad"` was ALLOWED - the decoy matched.
#   - `--subject "thing (#206)"` while merging 299 was ALLOWED - any `(#N)` satisfied it, so the
#     exact astubbs#206 shape this hook is named after walked straight through.
#   - `--subject 'don'\''t drop it (#299)'` was DENIED - the escaped apostrophe truncated the
#     capture before the suffix.
#   - `--body "we discussed --subject" --subject "thing (#299)"` was DENIED - the first `--subject`
#     is inside the body text.
#
# A PreToolUse deny is hard, so the last two are the more expensive class: a false positive stops a
# legitimate merge and the agent cannot argue with it. So: `shlex.split` the whole line, so quoting
# is handled by something that knows shell quoting; find each `gh pr merge` in the TOKEN stream and
# judge its slice on its own; take the LAST `--subject` the way `gh` does; and cross-check the
# number against the PR being merged.
#
# FAIL OPEN, ALWAYS. Any parse failure exits 0. A hook that blocks on its own bug is worse than no
# hook: the gate below it (docs/merge-checklist.md, and review) still catches a bad subject, but
# nothing catches a hook that has jammed the tool call shut. `bin/test-check-agent-hooks.sh` is the
# negative control - it asserts each shape above, in both directions.
#
# EVERY SPELLING OF THE FLAG, OR NONE. A third review round found `-t` - `gh`'s documented short
# form of `--subject` - was not read at all, so `-t "no number"` sailed through the "no override,
# GitHub appends it" branch while `gh` used the text verbatim. That is the astubbs#206 shape again,
# reached through an unhandled spelling: a parser that reads one spelling of a flag protects
# against one spelling of the mistake. `--subject`, `--subject=X`, `-t X`, `-tX`, `-t=X` and `-t`
# inside a shorthand group (`-st X`) are now all read, and every value-taking flag has its value
# consumed so it cannot be misread as the PR selector.
#
# BOUNDARY, STATED. The `(#N)` may sit anywhere in the subject, not only at the end, so
# `"port (#299) to master"` passes. Requiring the trailing slot would be truer to the convention
# but turns every unusual-but-fine subject into a hard block, and the cost of the two error
# directions is not symmetric.
#
# WHAT CANNOT BE CROSS-CHECKED, AND WHY THAT IS AN ALLOW. The number is compared against the PR
# selector when the selector carries one - a bare `299` or a `.../pull/299` URL. It cannot when the
# selector is a BRANCH NAME, or absent (`gh pr merge --squash`, resolving from the branch), because
# both need a network round trip this hook has no business making; any `(#N)` is accepted there.
# Same reasoning for a subject still holding `$VAR` or `$(...)`: shlex does not expand them, so the
# string is not the one gh will send, and judging it would be judging the wrong text.
#
# PreToolUse CAN feed text back to the model - a deny reason is delivered, and so is
# `hookSpecificOutput.additionalContext`. This hook uses the deny reason. See docs/agent-harness.md.

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

# The matcher in .claude/settings.json can only match a command PREFIX, which misses every shape
# this hook is built for - `/usr/local/bin/gh pr merge ...`, `echo x && gh pr merge ...`. So the
# hook is registered for every Bash call and does its own filtering, and this is the cheap first
# cut: no `merge` anywhere in the payload means no `gh pr merge`, decided without starting python.
if ! grep -q merge "$payload_file"; then
    exit 0
fi

python3 - "$payload_file" <<'PY'
import json, re, shlex, sys

try:
    with open(sys.argv[1], encoding="utf-8") as fh:
        tool = json.load(fh)
except Exception:
    sys.exit(0)                      # unparseable: never block on our own bug

if tool.get("tool_name") != "Bash":
    sys.exit(0)

cmd = tool.get("tool_input", {}).get("command", "")

# `gh pr merge` flags that CONSUME the next argument. The set matters twice over: `-t` is the
# short spelling of `--subject`, and reading only the long one let `-t "no number"` through the
# hook entirely; and a flag's VALUE must never be mistaken for the PR selector.
LONG_VALUE_FLAGS = {
    "--subject", "--body", "--body-file", "--author-email", "--repo", "--match-head-commit",
}
SHORT_VALUE_FLAGS = {"t": "--subject", "b": "--body", "F": "--body-file",
                     "A": "--author-email", "R": "--repo"}

# `gh pr merge [<number> | <url> | <branch>]`. Only the first two carry a number to cross-check.
# ANCHORED TO A REAL URL. Searching any non-numeric selector for `/pull/<digits>` also matched a
# perfectly legal BRANCH name - `git check-ref-format --branch fix/pull/1299` accepts it - so
# merging that branch was cross-checked against 1299 and a correct subject naming the real PR was
# DENIED. A false positive, in the class this hook weights hardest.
PR_URL = re.compile(r"^https?://\S+/pull/(\d+)(?:[/?#]|$)")

# A subject that still contains a shell variable or command substitution when we see it. `shlex`
# does not expand either, so the text we are holding is not the text `gh` will send.
UNEXPANDED = re.compile(r"[$`]")

# SEGMENT ON TOKENS, NEVER ON THE RAW STRING. One command line can carry more than one
# `gh pr merge` (`a && b`, `a ; b`), and each must be judged on its own slice so a good merge
# cannot vouch for a bad one. Finding those boundaries with a regex over the raw line also finds
# the phrase inside QUOTED BODY TEXT: `--body "text gh pr merge here"` cut the line into two
# slices that each had unbalanced quotes, so both fail-opened and the real `--subject` was never
# judged at all. Splitting first and matching on tokens makes the body a single token, so text
# inside it can no longer look like a command.
# `shlex.split` alone will not do: with default settings `a;b` is ONE token, so shell operators
# have to be lexed as tokens in their own right for the slice to end at them. `commenters` is
# cleared because a `#` in an unquoted subject is text here, not the start of a comment.
try:
    lexer = shlex.shlex(cmd, posix=True, punctuation_chars=True)
    lexer.whitespace_split = True
    lexer.commenters = ""
    tokens = list(lexer)
except ValueError:
    sys.exit(0)                      # unbalanced quotes: our own parse limit, never block on it

# Where one command ends and the next begins. A slice that ran to the next `gh pr merge` instead
# swallowed everything after `&&`/`;`/`|`, so an unrelated later command's `--subject` became "the
# last one" - a decoy could vouch for a bad merge, and a trailing `echo --subject "no number"`
# could condemn a good one. Both directions, from the same missing boundary.
OPERATORS = {"&&", "||", ";", ";;", "|", "&", "\n"}


def is_gh(token):
    return token == "gh" or token.endswith("/gh")


starts = [i for i in range(len(tokens) - 2)
          if is_gh(tokens[i]) and tokens[i + 1] == "pr" and tokens[i + 2] == "merge"]
if not starts:
    sys.exit(0)


def slice_end(start):
    """One `gh pr merge` runs until a shell operator or the next merge - whichever comes first."""
    for j in range(start + 3, len(tokens)):
        if tokens[j] in OPERATORS or j in starts:
            return j
    return len(tokens)


bounds = [(start, slice_end(start)) for start in starts]


def subjects_and_pr(slice_tokens):
    """The subject values and the PR number in one already-tokenised `gh pr merge ...` slice.

    Every spelling `gh` accepts for the subject counts, because the hook is worthless against the
    one it cannot read: `--subject X`, `--subject=X`, `-t X`, `-tX`, `-t=X`, and `-t` inside a
    combined shorthand group such as `-st X`. Flags that consume a value have theirs consumed here
    too, so a value is never mistaken for the PR selector.

    """
    values, pr = [], None
    seen_merge = False
    selector_seen = False
    i = 0
    while i < len(slice_tokens):
        token = slice_tokens[i]

        if token.startswith("--"):
            name, sep, inline = token.partition("=")
            if sep:
                if name == "--subject":
                    values.append(inline)
                i += 1
                continue
            if name in LONG_VALUE_FLAGS and i + 1 < len(slice_tokens):
                if name == "--subject":
                    values.append(slice_tokens[i + 1])
                i += 2
                continue
            i += 1
            continue

        # A shorthand group, per pflag: scan left to right, and the FIRST value-taking letter
        # takes the rest of the group as its value - or the next token when it ends the group.
        # Stopping at that letter is what keeps `-bt` from being read as a subject: there, `t` is
        # part of `-b`'s value, not a flag.
        if len(token) > 1 and token[0] == "-" and token[1] != "-":
            group = token[1:]
            took_next = False
            for j, ch in enumerate(group):
                if ch not in SHORT_VALUE_FLAGS:
                    continue
                rest = group[j + 1:]
                if rest.startswith("="):
                    value = rest[1:]
                elif rest:
                    value = rest
                elif i + 1 < len(slice_tokens):
                    value = slice_tokens[i + 1]
                    took_next = True
                else:
                    value = None
                if ch == "t" and value is not None:
                    values.append(value)
                break
            i += 2 if took_next else 1
            continue

        if token == "merge":
            seen_merge = True
        elif seen_merge and not selector_seen:
            # `gh pr merge [<number> | <url> | <branch>]` - the first bare argument is the
            # selector, whatever its form, so nothing after it can be re-read as one.
            selector_seen = True
            if token.isdigit():
                pr = token
            else:
                match = PR_URL.search(token)
                if match:
                    pr = match.group(1)
        i += 1
    return values, pr


for start, end in bounds:
    subjects, pr = subjects_and_pr(tokens[start:end])

    if not subjects:
        continue                     # no override, so GitHub appends the number itself - fine

    subject = subjects[-1]           # gh honours the LAST occurrence, so that is the one judged

    # `--subject "$SUBJECT"` / `--subject "$(cat msg.txt)"`: shlex does not expand either, so what
    # we are holding is not what gh will send, and its `(#N)` may well be in there. Denying on a
    # string we cannot resolve is the false-positive class this hook weights hardest against - and
    # the header promises to fail open on our own limits, which this is one of.
    if UNEXPANDED.search(subject):
        continue

    found = re.findall(r"\(#(\d+)\)", subject)
    pr_hint = f"(#{pr})" if pr else "(#<pr>)"

    if not found:
        reason = (
            "This --subject has no PR-number suffix, and passing --subject suppresses the "
            f"{pr_hint} that GitHub would otherwise append. "
        )
    elif pr is not None and pr not in found:
        reason = (
            f"This --subject carries (#{found[-1]}) but the merge is of PR #{pr}. A number that "
            "points at the wrong PR - or at an issue - is worse than none: it reads as correct and "
            "links a future reader somewhere else. "
        )
    else:
        continue                     # carries the right number - fine

    print(json.dumps({
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "permissionDecisionReason": (
                reason
                + "The commit would land out of step with every neighbour on master, and it is not "
                "fixable afterwards without rewriting a pushed commit (this happened on "
                f"astubbs#206). Either end the subject with ' {pr_hint}', or drop --subject "
                "entirely and let the PR title be used - --body-file alone does not affect the "
                "subject. See docs/merge-checklist.md."
            ),
        }
    }))
    break
PY
