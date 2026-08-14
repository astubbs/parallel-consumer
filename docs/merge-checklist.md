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

When the branch is one idea and the intermediate commits are noise. If you recommend this, **write
the suggested squash message out in full.** It becomes the permanent record, and GitHub's default -
the PR title plus every commit subject concatenated - is a log of how the work happened rather than
an explanation of what changed and why. The PR discussion is not in `git log`; the squash message is
all a future reader gets.

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
