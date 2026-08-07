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

`scripts/upstream-sweep.sh --audit` finds *bulk* closures and *zero-reply* discussions. Both are
proxies, and each one misses a whole shape of problem:

- A PR closed alone on a quiet day looks like nothing. confluentinc#508 (our own docs work) and
  confluentinc#650 were only found because they happened to sit inside a dependabot batch.
- A discussion with one dismissive reply is not "zero reply", so it never appears.
- An issue closed with `COMPLETED` and no linked merged PR is indistinguishable from a real fix
  without reading it.

So the audit narrows the field; it does not discharge the obligation. Only reading does.

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

- **Wiki** - disabled upstream (`has_wiki: false`).
- **Security advisories** - none published.
- **Milestones** - three still open (0.3.1, 0.5.1, 0.6); their only open issues are confluentinc#27,
  confluentinc#192 and confluentinc#78, and all three are already mirrored.
- **Orphan branches never attached to a PR** - `v0.6.x`, `v0.6.x-dev`, `0.5.3.x`, `v0.5.2.x-dev`,
  `DP-12547`. `v0.6.x-dev` is 78 non-release commits of the lambda-actor-bus work already captured
  via the swept PRs confluentinc#325 and confluentinc#524. `0.5.3.x`'s regression fix
  (confluentinc#362, state truncation vs commit order) **is** on master as `a908e1663` - verified,
  not a lost fix. `DP-12547` shares no ancestor with master; it is Confluent-internal service config.
- **"Upstream pushed today"** - misleading. `pushed_at` moves on branch and tag activity; the newest
  actual commit is 2026-05-28 (`rmoff`, "Add link to fork"). No new upstream code activity.
