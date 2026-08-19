# Open obligation: every upstream issue, PR and discussion must be accounted for

<!-- inflight-type: bug -->
<!-- inflight-impact: blind-spot -->


**This is not done.** The 2023 administrative sweeps are handled - 28 issues mirrored as
astubbs#227-254, the PR sweep mapped - but that was one cohort, found because it happened to be a
*bulk* event. The general obligation is wider and still open:

> Work through **all** upstream issues, **all** upstream PRs and **all** upstream discussions, and
> confirm each one has been addressed - carried into the fork, deliberately declined, or genuinely
> resolved upstream. Not sampled. Not "the interesting ones".

Upstream is unmaintained and the README points here, so anything left unaddressed there is simply
lost. That is the whole argument for doing this exhaustively rather than opportunistically.

## Why the tooling does not already answer this

`--audit` finds *bulk* closures and *zero-reply* discussions - both proxies. Its blind spots are
documented durably in [`docs/upstream.md`](../upstream.md), "`--audit` - closures the window cannot
see". The audit narrows the field; it does not discharge the obligation. Only reading does. (An
issue closed `COMPLETED` with no linked merged PR is indistinguishable from a real fix without
reading it - see
[`docs/solutions/workflow-issues/closed-as-completed-is-not-completed-2026-08-12.md`](../solutions/workflow-issues/closed-as-completed-is-not-completed-2026-08-12.md).)

## State of each surface

Run `scripts/upstream-sweep.sh --audit` for the lists and the totals - they move, and a copy here
would be wrong within a day. What the audit cannot tell you is how much of each surface a human has
actually *read*, which is the only thing recorded below:

- **Issues - closed, neither tracked nor mirrored.** Listed by `--audit`, **none read**.
- **Issues - open upstream.** Mirrored (see [`docs/upstream.md`](../upstream.md)), but no
  completeness check has ever been run against the live list.
- **PRs - closed unmerged, human-authored.** Only the 2023-06-15 sweep has been examined; the rest
  are unread.
- **Discussions.** Only the zero-reply ones surfaced by `--audit` have been read; the remainder are
  unread.

## Also unchecked

- **Project boards.** `has_projects: true` upstream, but our token lacks the `read:project` scope, so
  `projectsV2` returns INSUFFICIENT_SCOPES. Never inspected. Needs a scoped token or a manual look.
- **169 forks.** Other people's downstream fixes, entirely unexamined. Probably low yield per unit
  effort, but it is a genuine surface and worth one pass.

## Ruled out - do not re-investigate

Wiki, security advisories, open milestones, orphan branches and the misleading `pushed_at`
timestamp are all accounted for. Those facts are permanent, so they live in
[`docs/upstream.md`](../upstream.md), "Surfaces checked and ruled out" - not here.
