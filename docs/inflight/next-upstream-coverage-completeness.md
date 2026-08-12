# Open obligation: every upstream issue, PR and discussion must be accounted for

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

| Surface | Count | Status |
|---|---|---|
| Issues - closed, neither tracked nor mirrored | ~139 | **unread**, listed by `--audit` |
| Issues - open upstream | ~100 | partially mirrored; no completeness check has ever been run |
| PRs - closed unmerged, human-authored | see `--audit` | only the 2023-06-15 sweep examined |
| Discussions | 74 | 6 zero-reply read; the other 68 unread |

Do not re-derive these lists here - run the audit. Recorded here is only the fact that nobody has
read them.

## Also unchecked

- **Project boards.** `has_projects: true` upstream, but our token lacks the `read:project` scope, so
  `projectsV2` returns INSUFFICIENT_SCOPES. Never inspected. Needs a scoped token or a manual look.
- **169 forks.** Other people's downstream fixes, entirely unexamined. Probably low yield per unit
  effort, but it is a genuine surface and worth one pass.

## Ruled out - do not re-investigate

Wiki, security advisories, open milestones, orphan branches and the misleading `pushed_at`
timestamp are all accounted for. Those facts are permanent, so they live in
[`docs/upstream.md`](../upstream.md), "Surfaces checked and ruled out" - not here.
