# A Node query client over branch-distributed state - and the half of it that is already built

<!-- inflight-type: feature -->
<!-- inflight-impact: ci -->

**Proposal (2026-09-01):** keep the data model - markdown notes in the tree, three tag axes - and put
a Node client in front of it that (a) answers "what is open" by reading notes across *every* branch
rather than the checked-out one, (b) is the tunnel for reading GitHub, forcibly caching and expanding
links to related issues and PRs, and (c) consolidates the gates onto one runtime. The aim is a
harness that is easier to program and that makes good practice the path of least resistance.

## Half of this ships today, at 6.6k stars

**Backlog.md already does the tracker half, including the cross-branch part** - markdown task files
committed to the repo and travelling with branches, plus `checkActiveBranches`, `remoteOperations`
and `activeBranchDays`, which reconcile task state across branches and optionally check remote
branches for newer states. TypeScript, MIT, actively maintained.
[`docs/plans/2026-09-01-001-investigate-beads-comparison.md`](../plans/2026-09-01-001-investigate-beads-comparison.md)
owns the survey; §10 there records how this was missed on the first pass.

So "read the notes on every branch and report drift" is **not** the differentiator it looked like.
Design accordingly: either adopt that capability or justify rebuilding it, and do not spend the
budget proving something a `npm i -g` already does.

## The half nobody ships: the GitHub tunnel

No tracker surveyed integrates with GitHub Issues - not Backlog.md, not Beads, not Claude Code
Tasks, not OpenClaw. The two that bridge (`git-bug`, `dspinellis/git-issue`) keep their own store
outside the working tree, so they import GitHub *into a parallel tracker* rather than making
GitHub's own graph queryable from the repo.

That is the gap our GitHub surface already half-fills - `bin/issue-index.sh`, the `upstream-mirror`
pairing, `.claude/hooks/inject-branch-context.sh` - and the part of this proposal worth building.
The narrower predecessor is `ci-issue-index-has-no-edges.md`, on branch
`docs/inflight-gh-link-graph`, which proposes a hook that reacts to `gh` reads; this proposes the
client that makes the expansion unavoidable rather than reactive. **They must not both be built** -
whichever lands first owns the edges, and the other becomes its consumer.

## The decisions

- **A CLI cannot force anything, and the proposal's word is "forcibly".** An agent can always call
  `gh` directly. The forcing function is a `PreToolUse` hook that denies or redirects the raw call;
  the client is only the good path it redirects *to*. `docs/agent-harness.md` owns the distinction -
  a rule is not a mechanism - and designing the client as if it enforces is how the enforcement
  never gets written.
- **What "drift" means has to be defined before it can be detected.** Candidates, in rising order of
  value: the same note differing across branches; a note still open on a branch whose work landed on
  master; a note whose issue closed underneath it; and **two branches claiming the same work**, which
  is the collision `pr-blockers-and-collisions.md` currently tracks by hand.
- **Cache on ref SHA, not on time.** A ref that has not moved cannot have changed its notes, so the
  SHA is an exact cache key rather than a heuristic one. This matters at the number of refs this fork
  carries; `git for-each-ref` gives the count.
- **The GitHub cache must inherit `issue-index.md`'s discipline or it becomes the second tracker
  that "never write down what a command can answer" forbids.** Cache *structure* - which things link
  to which, and their titles. Never *status*, which moves. That file's header owns the argument.
- **No daemon, no socket, no lockfile.** Plain `.js` run by `node`, no bundler and no npm
  dependencies, so it works in a fresh clone with nothing built - the property that rules out a
  compiled binary rules out a package with an install step just as firmly. `.github/scripts/*.js` is
  the existing proof this is workable.
- **Runtime consolidation is a separate decision with its own evidence**, and should not ride along
  silently: the real gate logic is already Node with co-located tests, while the one substantial
  Python gate is the one whose missing dependency the pre-commit hook has to special-case.

## Delete when

The tunnel is built, or the decision is taken to adopt Backlog.md's cross-branch capability instead
of rebuilding it.
