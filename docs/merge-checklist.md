# Getting a PR ready to merge

**Owns the merge-strategy decision** and what to offer the author before a PR lands. `AGENTS.md`
keeps the one-line rule and points here; this doc holds the detail. Tool-neutral: Claude Code has it
injected when a prompt looks like merge prep (`docs/agent-harness.md`), everything else reaches it
from `AGENTS.md`. One source, so the two cannot drift.

## Recommend a merge strategy - and say why

A long-lived PR accumulates fix-ups nobody wants in the permanent log, but usually also two or three
genuinely separate pieces of work. **Do not default; look at the actual commits.** Release notes are
generated from the commit log, so this choice decides what a future changelog has to work with.

### Re-cut the commits

`git reset --mixed <merge-base>`, restage into a handful of atomic commits, rebase-merge. Right when
the branch holds distinct workstreams someone will later want to bisect to or revert independently.
The test for "atomic" is whether the message needs an "and also".

- **`git fetch origin master` first, every time**, and reset to the **merge-base**, not to
  `origin/master`. A stale ref or the wrong base silently reverts whatever master gained meanwhile,
  and the tell is files appearing in the staged set that the branch never touched.
- Verify with `git diff <old-tip> HEAD` - it must be **empty**, proving history changed and content
  did not.

### Squash-merge

When the branch is one idea and the intermediate commits are noise. GitHub's default message - the PR
title plus every commit subject concatenated - is a log of how the work happened rather than an
explanation of what changed and why, and the PR discussion is not in `git log`, so the squash message
is all a future reader gets. **Write a real one.**

**Write it where it is used, not into the conversation.** Put it in the merge when you perform it, or
in the PR body if the author is merging. Do not print it out in chat unless asked - it is long, the
author is being asked for a decision rather than a proofread, and pasting it makes them scroll past
the thing they actually have to answer. Say the strategy and why in a line or two; offer the message.

That is a delivery rule, not a licence to skip it: the message still gets written, and written
properly.

**Do not pass `--subject`.** `gh pr merge --squash` lands a subject ending `... (#265)` because
GitHub appends the PR number - but only when the subject is not overridden. Passing `--subject` uses
your text verbatim, so the number silently never appears and the commit lands out of step with every
neighbour on master; astubbs#206 needed a force-push to master to correct. Omit the flag and the PR
title is used. If the title is wrong, **fix the title** - it is what reviewers saw, and `AGENTS.md`
already asks for it to be kept in step. `--body-file` on its own does not affect the subject, so a
hand-written message still works. `.claude/hooks/check-squash-subject.sh` refuses the flag for Claude
Code; the rule binds anyone merging by hand.

### Rebase-merge as-is

Only when the existing commits are already clean and atomic.

## Offer, do not just do

Both of the above rewrite history the author may want kept, and both are easy to forget precisely
when the PR feels finished. **Offer them and say what you would write; do not silently rewrite.**
Rewriting history someone else may have pulled is not reversible from inside a PR.

## Also confirm before merge

- **Is the PR description still true?** Long-running branches drift; a description written before
  three rounds of review usually describes a PR that no longer exists.
- **Has a human reviewed it and said LGTM?** Automated review is not approval, and neither is green
  CI.
- **Do the commit messages explain WHY?** The diff already says what.
- **Is any scaffolding left?** Scratch tests, debug logging, commented-out experiments, a stray
  `.class`.
- **Did a rename or deletion leave a dangling reference?** Grep docs, scripts, workflows and the
  quarantine registry. A rename that compiles can still break a doc pointer or a gate that matches
  by name.
- **Other instances of the same defect** - `AGENTS.md`, "PR Discipline", owns that rule; it belongs
  at merge prep, once the defect class is understood.
