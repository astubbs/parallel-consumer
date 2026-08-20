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

**Write it to a scratch file and say where it is.** Put it in the merge when you perform it;
otherwise write it to a file under the session's scratchpad and give the author the path. Say the
strategy and why in a line or two.

**Never the PR body, and never chat.** The body is what reviewers read to understand the change - a
squash message there is a second description of the same PR, and the two drift the moment either is
touched. It is also not where any merge command reads from, so it buys nothing at the point of use.
Chat fails differently: the message is long, the author is being asked for a decision rather than a
proofread, and pasting it makes them scroll past the thing they have to answer.

This paragraph previously said "or in the PR body if the author is merging", and it is injected into
every merge-prep prompt, so it produced the same wrong move repeatedly - the last time on
astubbs/parallel-consumer#323, whose description carried a squash message until it was removed by
hand.

That is a delivery rule, not a licence to skip it: the message still gets written, and written
properly.

**If you override the subject, end it with `(#N)`.** `gh pr merge --squash` lands a subject ending
`... (#265)` because GitHub appends the PR number - but only when the subject is not overridden.
Pass `--subject` (or its short form `-t`) and your text is used verbatim, so the number silently
never appears and the commit lands out of step with every neighbour on master; astubbs#206 needed a
force-push to master to correct. Simplest is to omit the flag entirely and let the PR title be used
- and if the title is wrong, fix the title, since it is what reviewers saw.
`.claude/hooks/check-squash-subject.sh` refuses an override without the suffix for Claude Code; the
rule binds anyone merging by hand.

**Sign off the squash body, and nothing else.** An agent-assisted squash body ends with exactly one
trailer - `Co-authored-by: Claude <model> (1M context) <noreply@anthropic.com>` - and carries no
`Claude-Session:` line: the session link belongs on ordinary branch commits, not the permanent
squash record master keeps. The same hook enforces both halves for Claude Code when the body is
overridden (`--body`/`--body-file`); the rule binds anyone merging by hand.

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
- **Does this PR advance a roadmap entry?** Then its `stage`/`stage_delivery` in
  `docs/data/roadmap.yaml` move in the same change - the `stages` block there owns the rule. The
  roadmap-stage gate enforces it when the entry's `pull_request` names this PR; entries carried by
  a tracking issue only are on you.
