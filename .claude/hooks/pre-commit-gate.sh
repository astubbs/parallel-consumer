#!/usr/bin/env bash
#
# Copyright (C) 2026 Antony Stubbs and contributors
#

# PreToolUse hook: run `.githooks/pre-commit` before the agent's `git commit`, and honour
# `--no-verify` the way git itself does.
#
# WHY THIS EXISTS AT ALL. `core.hooksPath` cannot be committed, so a fresh clone has no git hooks
# until someone runs the config command once. This covers Claude Code in that window. It is
# belt-and-braces, not the primary mechanism - the git hook binds every process that runs `git`,
# including humans and other agents; this binds one tool.
#
# WHY IT IS A SCRIPT AND NOT `... || exit 2` INLINE. The inline form never reads the hook payload,
# so it could not see the command it was gating - which meant `git commit --no-verify` ran the
# gates anyway and blocked. That directly contradicts the pre-commit hook's own header: "a gate
# people cannot skip when they have a reason is a gate they disable permanently". An agent facing a
# red gate it cannot bypass has exactly one move left, which is to stop working; a human in that
# spot deletes the hook. The escape hatch is the thing that keeps the gate installed.
#
# WHY EXIT 2 AND NOT A JSON DENY. Exit 2 is PreToolUse's documented block, and it forwards stderr to
# the model - so the failing gate's own output becomes the explanation. A bare `exit 2` with nothing
# on stderr produces "hook error: No stderr output", which tells the agent it was blocked and
# nothing about why; that was the observed behaviour of the inline form.
#
# WHICH REPOSITORY IT GATES - THE COMMAND'S, WHICH IS NOT ALWAYS THE SESSION'S. This used to resolve
# the gate from `$CLAUDE_PROJECT_DIR`, and to run it with this hook process's own working directory,
# so what got checked was the tree the SESSION is rooted in.
#
# The hazard was raised in review and dismissed with the wrong argument: that `if: Bash(git commit *)`
# matches the command as WRITTEN, so a `cd other-worktree && git commit` never reaches here, leaving
# only bare commits "in the session's own cwd, where session repo and commit repo are the same one".
# The premise is false for a SUBAGENT, which has its own working directory while `$CLAUDE_PROJECT_DIR`
# still names the session's most recent worktree - and a subagent's `git commit ...` is bare, so it
# matches the registration and arrives here.
#
# Observed 2026-08-31: a subagent committing in `.claude/worktrees/proxy-server-shell` was gated
# against `.claude/worktrees/bench-harness`. `check-file-refs.sh` failed on citations to `bench/`
# files that do not exist on the agent's branch, while the agent's own tree ran the same gate at
# exit 0, and five commits had to go through with `--no-verify`. The dangerous half is the mirror
# image and leaves no trace at all: a RED tree passes because the session's tree is green, so a
# violation lands with the gate reporting success - the silent-wrong-answer shape that
# astubbs/parallel-consumer#382 fixed in the two hooks either side of this one.
#
# So the working tree is now derived from the COMMAND, strongest source first, and the block message
# says which directory answered:
#
#   1. `git -C <path> commit` - the commit naming its own repository, and unambiguous only when
#      every commit in the payload names the same one;
#   2. a leading `cd <path> &&` - belt-and-braces, since the registration above does not currently
#      match that shape;
#   3. the payload's own `cwd`, which is what a subagent's directory arrives in;
#   4. `$CLAUDE_PROJECT_DIR`, then this process's directory - the labelled last resorts.
#
# The gate script and its working directory both come from that answer, so a gate added on one
# branch and absent on another behaves correctly in each. `.githooks/pre-commit` WOULD be the primary
# mechanism - git runs it inside the target repository by construction - but `core.hooksPath` is not
# set in this clone, so git never invokes it and this hook is the only gate that actually fires. That
# is why the wrong-tree case below REFUSES rather than failing open: there is nothing behind it.
#
# FAIL OPEN ON OUR OWN BUG. If the payload does not parse, or the gate script is missing, this exits
# 0. The git hook and CI both still gate the same commit.
#
# Negative control: bin/test-check-agent-hooks.sh.

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

# NO PYTHON, NO GATE. Neither the bypass below nor the working tree above can be read without
# parsing the payload, and the repo's documented build requirements are JDK 17, Docker and the Maven
# wrapper - python is not among them. Falling through on a missing interpreter meant
# `git commit --no-verify` ran the gates and was blocked at exit 2 with no way to argue: the escape
# hatch the header calls load-bearing, absent on exactly the machine that has no other way out. Fail
# open, like every other limit here; `.githooks/pre-commit` and CI still gate the same commit.
#
# THIS TEST MOVED ABOVE THE GATE-EXISTS TEST when the working tree stopped being read from the
# environment: which gate script to look for is now an answer python has to produce.
command -v python3 >/dev/null 2>&1 || exit 0

# Does THIS COMMIT carry a real `--no-verify` argument? Three things are load-bearing:
#
#   - `shlex`, so a commit MESSAGE mentioning the flag (`git commit -m "document --no-verify"`) is
#     not mistaken for the flag itself;
#   - the search is scoped to the `git commit` command, not the whole payload. It used to scan the
#     entire line, so `git commit -m x && echo --no-verify` bypassed a red gate for a commit that
#     never asked - the violation lands, and the later command is what "requested" it;
#   - a word-boundary search over the line only as the fallback when the line cannot be lexed,
#     because refusing to decide would mean gating a commit the author explicitly asked not to gate.
#
# Only the long spelling counts. `git commit -n` means the same thing to git, but `-n` is a common
# token in a command line that merely CONTAINS a commit (`echo -n`, an unquoted `$(...)`), and a
# bypass triggered by accident is a gate that silently stopped running. The long form is what the
# hook headers and docs tell people to type, and it is unambiguous.
#
# ONE SCAN, TWO ANSWERS: the exit code is the bypass decision, and stdout is the directory the
# commit runs in (empty when the command does not say). Reading the directory in a second python
# call would mean tokenising the same command twice, which is how two copies of a scan start
# disagreeing - the argument .claude/hooks/check-history-rewrite.sh makes for the same pairing.
scan_rc=0
commit_dir="$(python3 - "$payload_file" <<'PYGATE'
import json, os, re, shlex, subprocess, sys

OPERATORS = {"&&", "||", ";", ";;", "|", "&", "(", ")"}
# An unquoted NEWLINE separates statements exactly like `;`, but shlex's default whitespace
# swallows it - no token, no reset - so `at_command` carried over from wherever the previous line
# left it and `git add -A<newline>git commit -m wip` counted ZERO commits: an ordinary two-line
# payload silently exempted (found by review on this same PR). The lexer below therefore treats
# `\n` as a punctuation char instead, and this predicate recognises the runs shlex gloms together
# (a bare "\n", ";\n\n", "&&\n") which have no canonical spelling OPERATORS could enumerate.
SEPARATOR_CHARS = set(";&|()\n")


def is_separator(token):
    return token in OPERATORS or ("\n" in token and set(token) <= SEPARATOR_CHARS)



# Shell RESERVED WORDS that are FOLLOWED BY ANOTHER COMMAND, so command position survives them.
# Without these, `if ok; then git commit -m x; fi` counted zero commits: `then` is not an operator,
# so it consumed the command position and the commit behind it was invisible. That was harmless
# while zero commits meant "run the gate anyway", and became a hole the moment zero commits meant
# "skip" - see the exit-0 branch below. Recognised only when the word is ITSELF in command
# position, so `echo do git commit` stays text rather than becoming a commit.
COMMAND_INTRODUCERS = {"if", "then", "elif", "else", "while", "until", "do", "{", "!", "time"}
ASSIGNMENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=")
GIT_VALUE_FLAGS = {"-C", "-c", "--git-dir", "--work-tree"}

try:
    with open(sys.argv[1], encoding="utf-8") as fh:
        payload = json.load(fh)
    cmd = payload.get("tool_input", {}).get("command", "")
    # A SUBAGENT'S OWN DIRECTORY ARRIVES HERE and nowhere else. `$CLAUDE_PROJECT_DIR` names the
    # session's project root, which for a subagent working in another worktree is a different tree
    # entirely - see WHICH REPOSITORY IT GATES at the top of this file.
    payload_cwd = payload.get("cwd") or ""
except Exception:
    sys.exit(0)                      # unparseable payload: treat as bypass, never block on our bug


def commit_bypass_counts(line):
    """(commits, bypassing, dirs) for `git commit` in COMMAND POSITION on this line.

    Counted rather than short-circuited because ONE command line can hold SEVERAL commits, and a
    later one asking for a bypass must not exempt an earlier one that did not:
    `git commit -m first; git commit --no-verify -m second` used to exit 0 for both, so in a clone
    with no core.hooksPath the first commit landed with no gate run at all.

    `dirs` holds one entry per commit: the value of its `git -C <path>`, or "" when it has none and
    so runs where the tool call runs. Collected HERE rather than by a second walk because this loop
    already skips the git global flags that take a value, which is the whole difficulty.
    """
    # `\n` joins the default punctuation set and leaves `whitespace`, so a newline becomes a
    # TOKEN the loop can reset command position on - see SEPARATOR_CHARS above. Quoted newlines
    # are unaffected: quoting is resolved before either classification applies.
    lexer = shlex.shlex(line, posix=True, punctuation_chars="();<>|&;\n")
    lexer.whitespace = " \t\r"
    lexer.whitespace_split = True
    tokens = list(lexer)
    commits = bypassing = 0
    dirs = []
    i, at_command = 0, True
    while i < len(tokens):
        token = tokens[i]
        if is_separator(token):
            at_command = True
            i += 1
            continue
        if at_command and (token in COMMAND_INTRODUCERS or ASSIGNMENT.match(token)):
            i += 1
            continue             # a reserved word or a var assignment; the command is still ahead
        if at_command and token == "function":
            # `function NAME { body; }`: the token after the keyword is the function's NAME, never
            # a command, so skip both and leave command position OPEN for the `{` introducer (or
            # the `()` operators) that follows. Without this the name consumed the position and the
            # body's commits were invisible - the `foo() { ... }` spelling was only ever caught
            # because its bare `()` are operator tokens that happen to reopen position.
            i += 2
            continue
        if at_command and (token == "git" or token.endswith("/git")):
            j = i + 1
            repo_dir = ""
            while j < len(tokens) and tokens[j] in GIT_VALUE_FLAGS:
                # `-C <path>` is the only one of these that relocates the command. Recorded on the
                # way past rather than re-derived - and repeated -C values COMPOSE, each relative
                # path applied from the directory the previous one established, so `git -C sub -C ..
                # commit` runs in the ORIGINAL repository. Keeping only the last token resolved
                # `..` against the payload cwd instead, and a parent with no gate then passed the
                # commit unchecked (Codex review, astubbs/parallel-consumer#382).
                if tokens[j] == "-C" and j + 1 < len(tokens):
                    nxt = tokens[j + 1]
                    if os.path.isabs(nxt) or not repo_dir:
                        repo_dir = nxt
                    else:
                        repo_dir = os.path.join(repo_dir, nxt)
                j += 2               # git global flags that consume a value
            if j < len(tokens) and tokens[j] == "commit":
                end = j
                while end < len(tokens) and not is_separator(tokens[end]):
                    end += 1
                commits += 1
                dirs.append(repo_dir)
                if "--no-verify" in tokens[j:end]:
                    bypassing += 1
                i = end
                at_command = True
                continue
        at_command = False
        i += 1
    return commits, bypassing, dirs


def leading_cd(line):
    """The path of a LEADING `cd <path> &&`, or "".

    ONLY A LEADING ONE, AND ONLY WHEN IT IS THE ONLY ONE. This used to trust the first `cd`
    unconditionally, but `cd A && x && cd B && git commit` runs the commit in B, and gating A
    against that commit is a confident wrong answer - the class this whole hook exists to kill
    (reliability review, astubbs/parallel-consumer#382). More than one command-position `cd` is
    honestly ambiguous, so this returns "" and the caller falls through to the payload cwd.
    Belt-and-braces either way: the registration matches the command as written, so
    `cd X && git commit` does not currently fire this hook at all.
    NO APOSTROPHES IN THIS HEREDOC: the body sits inside a $( ) substitution, and an unbalanced
    quote stops bash finding the closing paren - proven the first time this docstring was written.
    """
    try:
        lexer = shlex.shlex(line, posix=True, punctuation_chars="();<>|&;\n")
        lexer.whitespace = " \t\r"
        lexer.whitespace_split = True
        toks = list(lexer)
    except ValueError:
        return ""
    cd_count = 0
    at_cmd = True
    for t in toks:
        if is_separator(t):
            at_cmd = True
            continue
        if at_cmd and t == "cd":
            cd_count += 1
        at_cmd = False
    # ...AND ONLY WHEN THE JOIN PRESERVES THE CWD. `cd /x & git commit` backgrounds the cd into a
    # subshell and `cd /x | cmd` pipes it into one - the commit stays in the payload cwd either
    # way, so trusting the prefix would run an unrelated green gate over a red tree (Codex review,
    # astubbs/parallel-consumer#382). Only `&&`, `;` and a newline hand the changed directory to
    # the next command in the same shell; operator runs can carry trailing newlines/semicolons.
    if cd_count == 1 and len(toks) > 2 and toks[0] == "cd" and not toks[1].startswith("-"):
        joiner = toks[2].strip("\n;")
        if joiner in ("", "&&"):
            return toks[1]
    return ""


def resolve_against(path, base):
    """A RELATIVE command-named path is relative to where the COMMAND runs - the payload cwd -
    never to this hook process, whose directory describes the session. Same-named subdirectories
    exist in every worktree of this repository, so resolving from the wrong directory SUCCEEDS on
    the wrong tree rather than failing. Unresolvable returns "", dropping to the next, labelled
    tier (astubbs/parallel-consumer#382, found cross-model; the history guard carries the same
    helper - change one, change the other)."""
    if not path:
        return ""
    if os.path.isabs(path):
        return path
    if base:
        return os.path.join(base, path)
    return ""


dirs = []
try:
    # The WHOLE command is lexed at once. Splitting into lines first breaks a quoted multiline
    # message down the middle, so the first line raises ValueError and the fallback below finds
    # `--no-verify` in the MESSAGE TEXT - which meant `git commit -m "...\n--no-verify\n..."`
    # skipped the gate entirely. shlex handles the newlines; the line split never needed to.
    commits, bypassing, dirs = commit_bypass_counts(cmd)
    # NO COMMIT AT ALL: skip. The registration's `if: Bash(git commit *)` is supposed to scope
    # this hook, but the script must not lean on it - observed live (astubbs#324 babysit): with
    # the gate red, a plain `ls` and a read-only `cat` of the gate itself were blocked with the
    # gate's own error, because "no commit found" fell into "run the gate". Every other hook in
    # this directory self-filters; this one does too.
    # OTHERWISE every commit in the payload must ask for the bypass. One that did not is gated.
    bypass = commits == 0 or commits == bypassing
except ValueError:
    # Genuinely unbalanced quoting. Fail OPEN so a hook bug cannot jam the tool call shut, but do
    # not try to read the flag out of text we could not lex.
    bypass = True

if not bypass:
    # WHICH TREE, printed only on the path that is about to gate something. `git -C` is used only
    # when it is UNAMBIGUOUS - every commit in the payload naming the same directory. A payload that
    # commits in two repositories has no single answer, and this hook can gate one tree; the mixed
    # case therefore falls through to the tool call's own directory, which gates the commits that
    # run there and leaves the relocated ones to `.githooks/pre-commit`, which git runs inside the
    # target repository. Incomplete, and never wrong about the tree it did read.
    named = set(dirs)
    cd_dir = resolve_against(leading_cd(cmd), payload_cwd)
    target = ""
    if len(named) == 1 and dirs and dirs[0]:
        # `git -C <rel>` is relative to where git runs - after any leading `cd`.
        target = resolve_against(dirs[0], cd_dir or payload_cwd)
    if not target:
        target = cd_dir
    if not target:
        target = payload_cwd
    # A COMMIT AGAINST A CLEAN TREE MEANS THIS IS THE WRONG TREE. Rule 3 above trusts the payload
    # `cwd` to be the directory a subagent runs in. It is not: it is the launch directory of the SESSION. On
    # 2026-09-02 three subagents, each committing in its own worktree after a `cd` in an EARLIER tool
    # call, were all gated against the main checkout - which had nothing changed - while their real
    # trees went unchecked. Nothing-to-commit is the signature: git would refuse this commit anyway,
    # so the only thing a gate can do here is read the wrong files and report their defects as yours.
    # `--allow-empty` is the one honest commit against a clean tree, and is let through to the gate.
    # (No apostrophes in these comments: Apple bash 3.2 quote-counts a heredoc inside $( ) naively.)
    if not bypass and target and "--allow-empty" not in cmd:
        try:
            st = subprocess.run(["git", "-C", target, "status", "--porcelain"],
                                capture_output=True, text=True, timeout=10)
            if st.returncode == 0 and not st.stdout.strip():
                print(target)
                sys.exit(3)              # wrong tree - bash prints the remedy and refuses
        except Exception:
            pass                         # cannot tell; fall through to gating as before
    print(target)

sys.exit(0 if bypass else 1)
PYGATE
)" || scan_rc=$?
if [ "$scan_rc" -eq 0 ]; then
    exit 0
fi
if [ "$scan_rc" -eq 3 ]; then
    printf 'pre-commit gate: this commit resolved to\n    %s\nand that tree has NO changes - nothing staged, unstaged or untracked - so it is not the tree\n' "$commit_dir" >&2
    printf 'you are committing to. The payload cwd is the SESSION root, not a subagent worktree, and a\n' >&2
    printf 'gate run there would report the defects of another tree as yours. Name the tree instead:\n\n' >&2
    printf '    git -C <your-worktree> commit ...\n\n' >&2
    printf 'Refusing rather than guessing, because .githooks/pre-commit is not wired (core.hooksPath\n' >&2
    printf 'is unset), so this hook is the only gate there is.\n' >&2
    exit 2
fi

# THE LAST RESORTS ARE LABELLED, and they are the pre-fix behaviour: `$CLAUDE_PROJECT_DIR` is the
# session's project root, which is the right answer only when the session and the command share a
# working tree. Reached when the payload carries no `cwd` and the command relocates nothing.
work_dir=""
work_dir_desc=""
if [ -n "$commit_dir" ] && [ -d "$commit_dir" ]; then
    work_dir="$commit_dir"
    work_dir_desc="the directory the commit runs in"
elif [ -n "${CLAUDE_PROJECT_DIR:-}" ] && [ -d "${CLAUDE_PROJECT_DIR:-}" ]; then
    work_dir="$CLAUDE_PROJECT_DIR"
    work_dir_desc="\$CLAUDE_PROJECT_DIR - the SESSION's project root, because the tool call did not say where it runs"
else
    work_dir="$PWD"
    work_dir_desc="this hook process's own directory, because nothing else said where the commit runs"
fi

# THE REPOSITORY ROOT, falling back to the directory itself. The gate lives at the root of a
# checkout, so a commit made from a subdirectory must climb; a directory that is not a git
# repository at all still gets asked directly, which is what keeps a plain fixture directory working.
if gate_dir="$(cd "$work_dir" 2>/dev/null && git rev-parse --show-toplevel 2>/dev/null)" && [ -n "$gate_dir" ]; then
    :
else
    gate_dir="$work_dir"
fi

gate="$gate_dir/.githooks/pre-commit"
[ -x "$gate" ] || exit 0

# RUN IT WHERE THE COMMIT RUNS. `.githooks/pre-commit` starts with its own
# `git rev-parse --show-toplevel`, so inheriting this hook process's directory was the second half
# of the same defect: even the right gate script read the wrong tree.
if ! output=$(cd "$gate_dir" && "$gate" 2>&1); then
    printf '%s\n' "$output" >&2
    printf '\nBlocked by the repo pre-commit gate (%s), run against %s\n' "$gate" "$gate_dir" >&2
    printf '(%s). Fix the gate(s) above, or commit with --no-verify if you\n' "$work_dir_desc" >&2
    printf 'have a reason - the bypass is deliberate, not an oversight.\n' >&2
    exit 2
fi

exit 0
