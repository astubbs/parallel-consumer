---
title: "A guard that lexes shell commands must lex like the shell: every divergence from real shell semantics is a silent steering vector"
date: 2026-09-01
category: best-practices
module: tooling
problem_type: best_practice
component: development_workflow
severity: high
applies_when:
  - "Writing or reviewing a Claude Code hook (or any guard) that parses a shell command string to decide whether to refuse, remind about, or steer it"
  - "The guard tokenises with a simplified splitter instead of posix shlex, or does not treat newline as operator punctuation the way a shell does"
  - "The guard reads a value-taking flag (-C, --branch, -m, -F) without consuming the value that follows it, letting it shift later positionals"
  - "The guard trusts a leading `cd` across a subshell or pipe operator (&, |) rather than only cwd-preserving joiners (&&, ;)"
  - "A review round on a guard-diagnostics PR keeps finding a new bypass or false-trigger of the same shape rather than a genuinely new class of bug"
tags:
  - shell-lexing
  - guard-bypass
  - posix-shlex
  - value-flags
  - cd-tracking
  - silent-steering
  - hook-security
  - adversarial-fixtures
---

# A guard that lexes shell commands must lex like the shell

> Every divergence between the guard's model of a command and the shell's actual semantics is a
> silent steering vector: an attacker - or an innocent compound command - lands on the wrong side of
> the divergence, and the guard confidently answers about a command nobody ran.

## Context

This repo's agent hooks read the Bash command an agent is about to run and answer questions about
it: is this a history rewrite (`.claude/hooks/check-history-rewrite.sh`), which tree does this
commit land in (`.claude/hooks/pre-commit-gate.sh`), which branch is being pushed
(`.claude/hooks/remind-inflight-on-push.sh`, `.claude/hooks/remind-master-drift-on-push.sh`). Two of
those refuse; two advise. One tokeniser configuration serves all four - owned by
`hook_git_invocations` and `hook_push_head_ref` in `.claude/hooks/lib/hook-common.sh`, sourced by
the two advisers, and deliberately MIRRORED rather than sourced by the two refusers, which may not
depend on a library they might fail to load (the duplication is tracked, with its reasoning, in
[ci-pr-lookup-is-copied-into-three-hooks.md](../../inflight/ci-pr-lookup-is-copied-into-three-hooks.md)).
All four are pinned by fixtures in `bin/test-check-agent-hooks.sh`.

The guards started life doing what every quick shell-inspecting script does: split the command on
whitespace, look for `git`, look for the subcommand, read the arguments. That model is not the
shell's model, and PR astubbs/parallel-consumer#382 is the record of how far apart they were. Three
successive independent review rounds each found real bypasses the previous round had missed, and
**every single one was a place where the guard's lexer and the shell disagreed about what a command
means**:

- Whole-string matching read `git push && git status` as not-a-push, because the trailing
  `git status` broke the pattern.
- Treating newline as whitespace fused separate commands: `git push -f` followed by a newline and
  `git log -1` handed `log` to the branch extractor as the pushed branch (the `NEWLINE IS AN OPERATOR`
  comment above the lexer in `hook-common.sh` owns the incident).
- Unspaced operators fused into their neighbours, so `feature&&git` parsed as a branch name.
- `git -C /path push --force` bypassed the history guard entirely - the `-C` value sat where the
  subcommand was expected.
- A second round (the Codex review on astubbs/parallel-consumer#382, each finding fixed and answered in its own resolved thread)
  found the *state* divergences: a `-C` recorded while scanning an earlier invocation bled into the
  later one carrying the verdict; repeated `-C` values compose in git (`git -C sub -C .. commit`
  runs in the original repo) but the guard kept only the last; a leading `cd` was trusted across `&`
  and `|`, which run the cd in a subshell the commit never inherits; and the python twin of the
  refspec parser was missing `--recurse-submodules` one review round after the bash copy gained it - the exact
  change-one-forget-the-other drift the code comments warn about, caught within one round of being
  written.
- The drift reminder measured the DESTINATION of a `src:dst` refspec, although git publishes the
  SOURCE - the `TWO SIDES OF ONE REFSPEC` comment in `remind-master-drift-on-push.sh` now owns that
  distinction.

The inverse failure showed up live too, and it is the same defect mirrored: the history guard's
lexer reads quoted heredoc *bodies* as command tokens, so a script that merely embeds text
resembling a force-push as data can false-trigger the refusal. It fired on its own test fixtures
while they were being written (recorded as the meta-finding in the Codex-round commit body on
astubbs/parallel-consumer#382).
Under-modelling the shell lets commands through; over-modelling data as commands blocks scripts
that did nothing.

## Guidance

**1. Use a real lexer, configured to the shell's actual token classes - not a regex, not a
whitespace split.** The shared tokeniser uses posix `shlex` with
`punctuation_chars="();<>|&;\n"` and `whitespace = " \t\r"` (both in
`hook_git_invocations` in `hook-common.sh`, mirrored in the python scanner inside
`check-history-rewrite.sh`). The two deliberate choices there are the whole lesson in miniature:
newline is an *operator* in shell grammar, not whitespace, and operators must become their own
tokens so `feature&&git` splits rather than fusing.

**2. Classify operators by character set, not by a hand-enumerated list of spellings.** shlex glues
adjacent punctuation into runs, so `&&` followed by a newline arrives as one token. The
`STOP AT ANY OPERATOR TOKEN` block in `hook-common.sh` classifies a token as an operator when every
character is in `OPERATORS = set("();<>|&;\n")` - merged runs still classify, and the comment
binds that set to the lexer's punctuation string so they cannot drift apart separately.

**3. Consume value-carrying flags WITH their values, from a named list.** A flag whose value can
look like a subcommand or a branch (`-C /path`, `--push-option x`) must swallow its value during
the scan, or the value lands in a positional slot and steers the answer. The lists are
`GIT_VALUE_FLAGS` and `PUSH_VALUE_FLAGS` in `check-history-rewrite.sh` and the matching skip list in
`hook_push_head_ref`. An incomplete list is a bypass per missing flag - which is how
`--recurse-submodules` became a finding.

**4. Model the state the shell models, with the shell's scoping.** `-C` is per-invocation state:
reset it at each new `git` token (the `git_c = ""` reset inside the invocation loop in
`check-history-rewrite.sh`). Repeated `-C` values compose the way git composes them
(`os.path.join`, absolute values restarting the chain). A leading `cd` changes the cwd of what
follows only across cwd-preserving joiners - `&&`, `;`, newline - never across `&` or `|`, which
run the cd in a subshell (the joiner check stripping `\n;` and accepting only `("", "&&")`, present
in both `check-history-rewrite.sh` and `pre-commit-gate.sh`).

**5. When the shell would do something your static scan cannot, degrade to a labelled guess - never
a confident answer.** A refspec containing `$` or a backtick would be expanded before git saw it,
so the token in the payload is not the branch; `hook_push_head_ref` drops it to the
`inferred-answer tier`, whose label says it is a guess. Two `cd`s in one command is honestly
ambiguous; `pre-commit-gate.sh` returns the empty answer and falls through to the payload cwd,
whose label already admits it describes the session. This provenance discipline is what made the
whole fix campaign safe: a divergence that still slips through degrades into a labelled guess
instead of a confidently wrong verdict.

**6. Pin every divergence with a fixture, and give the fixture a negative control that proves it
reaches the defect.** `bin/test-check-agent-hooks.sh` pins each fix, and for the sharp ones patches
the pre-fix code back in to show the fixture goes red against it - a fixture that passes against
both versions tests nothing. Where a parser exists twice (bash and python twins), the drift between
them is itself a fixture-worthy defect class.

**7. Expect the inverse defect and budget for it.** Every step toward modelling the shell more
faithfully raises the risk of reading *data* as commands - quoted heredoc bodies being the live
example. A refusing guard that false-fires on innocent scripts trains its users to bypass it, which
is the same end state as a guard that misses.

## Why This Matters

A guard that inspects commands is a parser with an adversary, and the adversary does not need to be
malicious - ordinary compound commands (`git push && git status`, a `cd` prefix, a multi-line
script) exercise the divergences for free. Each divergence fails *silently*: the guard still prints
an answer, the answer is about a command nobody ran, and nothing downstream can tell. For the
refusing guards that means a history rewrite sails through (`git -C /path push --force` did exactly
that); for the advisory ones it means the reminder names the wrong branch and the reader learns to
ignore reminders. The false-fire direction is as costly: a refusal triggered by fixture text as
data blocks legitimate work and erodes trust in the gate.

The fix campaign converged only because each round attacked the *model*, not the symptom: once
"the lexer must agree with the shell" was the named defect class, each reviewer could probe a fresh
semantic corner (operators, state scoping, subshells, expansion, refspec direction) instead of
patching individual command strings. The third review round ran its own adversarial probes against
head and found nothing left - which is what a class-level fix looks like, and what a
string-level fix never achieves.

## When to Apply

- Writing or reviewing anything that inspects a shell command it did not construct: agent hooks,
  CI gates, wrapper scripts, audit loggers, permission checkers.
- Adding a flag, subcommand, or operator to an existing command scanner - check the value-flag
  lists in *every* twin of the parser, not just the one you opened.
- A guard produced a wrong-but-plausible answer about a command: before patching the string that
  fooled it, ask which shell semantic its model lacks, and fix the model.
- A guard false-fired on data (heredocs, quoted strings, fixture text): the same model-divergence
  lens applies in reverse.
- Pairing a command-derived fact with an environment-derived one (branch from the command,
  repository from the process cwd): that is the WHERE half of this defect, owned by the sibling
  write-up
  [a-hook-processes-own-directory-describes-the-session-not-the-command-2026-08-31.md](../workflow-issues/a-hook-processes-own-directory-describes-the-session-not-the-command-2026-08-31.md) -
  this doc is the WHAT-the-command-says half.

## Examples

**Newline fusion.** The command `git push -f` + newline + `git log -1` is two commands to the
shell; a lexer treating `\n` as whitespace saw one token stream and handed `log` to the branch
extractor as the pushed ref. The shell's grammar makes newline a command *separator* - an operator.
Fix shape: remove `\n` from the lexer's whitespace, add it to `punctuation_chars`, and stop the
argument scan at any all-operator token (`hook_git_invocations` in `hook-common.sh`).

**The -C bypass.** `git -C /path push --force` is a force-push to git; a guard that looked for the
subcommand at a fixed position found `/path` there and concluded not-a-push, waving the rewrite
through. Git's semantics: `-C` takes a value, repeated `-C`s compose left to right, and the
subcommand is the first non-flag token after all value-flags are consumed. Fix shape: a named
value-flag list (`GIT_VALUE_FLAGS` in `check-history-rewrite.sh`) whose entries swallow their
values, `-C` recorded and composed like git composes it, and the state reset at each new
invocation.

**The subshell cd.** `cd /x & git commit` looks like "commit in /x" to a scanner that trusts any
leading cd - but `&` backgrounds the cd into a subshell, so the commit runs in the original cwd
while the guard gated `/x` (a tree that may have no gate at all). Fix shape: trust a leading cd
only when the joining operator preserves the cwd - `&&`, `;`, newline - and fall back to the
labelled payload-cwd tier otherwise (the joiner check in `pre-commit-gate.sh` and
`check-history-rewrite.sh`).
