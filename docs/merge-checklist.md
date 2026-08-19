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

**Write it where it is used, not into the conversation.** That means the merge itself -
`gh pr merge --squash --body`, or the merge box. Do not print it out in chat unless asked: it is
long, the author is being asked for a decision rather than a proofread, and pasting it makes them
scroll past the thing they actually have to answer. Say the strategy and why in a line or two, and
offer the message.

**Never put a squash message in the PR description.** A description tells reviewers what the change
is and is read while the PR is open; a squash message is commit text consumed once, at merge. Parking
one in the other corrupts the description for every reader before the merge, and leaves it behind as
noise after. When the author is merging, hand the message over at merge time - post it as a PR
comment, or give it to them when they ask - and leave the description alone.

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

## Say everything you have to say BEFORE the merge

**Every follow-up, suggestion and learning goes to the author while the PR is still open** - as part
of answering "is this ready to merge?", not afterwards. That includes the ones it is most tempting
to defer: a refactor you noticed but did not do, a test you would add next, a rule the work suggests
changing, and the **compounding run** - the learnings worth capturing from what just happened
(`ce-compound`, `docs/solutions/`).

**After the merge they are worth a fraction as much, and some are worth nothing.** The branch is
gone, so anything that belonged *in* this PR now costs a new one. The reviewer's attention is gone.
The context that made the suggestion obvious is gone, and the person best placed to judge it has
moved on. A follow-up filed at merge+1 competes for attention with everything else in the backlog;
the same sentence said at merge-1 gets decided in seconds by someone already holding the problem.

The worked example is astubbs#204: the agent that merged it read the manifest entry during merge
prep, saw `status: pr-open`, **merged anyway**, and reported the staleness as a follow-up afterwards
- at which point fixing it needed a fresh branch, a commit straight to master, and a later session
to notice at all. Said one minute earlier it was a one-line edit to a branch that was still open.

So: when you think you are done, ask what you are holding back, and say it now. "Nothing outstanding"
is a fine answer and worth stating explicitly - it is the difference between having checked and
having not thought about it.
