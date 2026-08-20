# Adopt an external harness-engineering solution instead of building our own

<!-- inflight-type: task -->
<!-- inflight-impact: process -->
<!-- inflight-state: deferred - after v6; adopting mid-release-run would churn the tooling every agent depends on -->

**The decision is whether to keep hand-building the agent harness at all.** It began as "should we
extract ours as a FOSS project", which prior-art research answered *no* - and in answering it, made
the better question obvious. If the space is occupied by products with real distribution, the
interesting move is adopting one, not competing with it.

## What we have, and what it cost

Hooks in `.claude/hooks/` covering session start, pre-tool-use and prompt submission; a family of
`bin/check-*.sh` gates, each paired one-to-one with a `bin/test-check-*.sh` self-test; and the
`docs/inflight/` tracker with its vocabulary, gate and session index. Deliberately not counted - the
totals change most weeks, and a number in a note is wrong the moment one lands.

Almost none of it is parallel-consumer-specific: each hook mentions Kafka or this project at most
once, in a comment. It was all written by hand, by agents, in this repo.

## What exists already (researched 2026-08-19)

"Harness engineering" became a named category in February 2026, with an awesome-list, a compatibility
matrix and published pieces from Anthropic and Martin Fowler.

- **Hooks**: [`sd0xdev/sd0x-dev-flow`](https://github.com/sd0xdev/sd0x-dev-flow) (188 stars) - "the
  harness layer for Claude Code", five-layer chain, gates that survive context compaction.
  [`karanb192/claude-code-hooks`](https://github.com/karanb192/claude-code-hooks) (480 stars) - twenty
  marketplace-installable plugins, including a `dead-rules-audit` that promotes ignored rules into
  deterministic hooks. That is our central thesis, already shipping.
- **Tracker**: [Beads](https://github.com/steveyegge/beads) at 26.5k stars - dependency graphs,
  context injection, memory decay. Backlog.md, git-issues, tkr and ai-trackdown cover the
  markdown-file variant.
- All MIT, so adoption is legally cheap.

## What adoption would cost us, honestly

- **The conventions are not in the code.** The tag vocabulary, the weight-axis boundary between
  `docs/inflight/` and `docs/refactoring.md`, "a filename carries no status", "deferred is a schedule
  and all non-deferred work happens first" - none of that ships with anyone's hooks. Adopting the
  tooling without them gets gates enforcing rules nobody agreed to, which is worse than no gates.
- **The self-test density is unmatched** - thirteen self-tests paired one-to-one with their gates,
  each verified red against a deliberately broken copy. The research found the *idea* advocated
  elsewhere and one project genuinely eval-gated, but nobody requires it per gate. That discipline is
  the thing worth keeping whatever the tooling underneath.
- **Migration is not free**, and the harness is what every agent session depends on. Doing it during
  a release run would churn the one thing that has to stay reliable.

## Cheap now, regardless of the decision

`karanb192`'s `config-guard` / `instructions-audit` stop an agent editing its own hooks and settings.
**We have no such guard, and every hook here was written by an agent** - that is a real hole, and one
hook closes it. Worth doing before any adoption decision, because it is useful either way.

## Why deferred rather than open

Adopting mid-release-run would destabilise the tooling every session depends on, for a benefit that
is entirely about future effort. The prerequisite is that our own harness stops changing weekly -
three of the seven hooks were written or substantially fixed in a single session, and two gates gained
bypass fixes in the same window. Re-evaluate once v6 has shipped and the harness has been still for a
while.

[`process-quarantine-lane-foss.md`](process-quarantine-lane-foss.md) is the same question about one
gate and should be decided together with this, not separately.
