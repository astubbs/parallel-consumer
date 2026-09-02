# The commands `inflight` should grow next

<!-- inflight-type: feature -->
<!-- inflight-impact: stranded-work -->

Queued capabilities for `bin/inflight.mjs`, each measured on this repository rather than estimated.
Sibling note: [`ci-inflight-absorbs-the-query-half.md`](ci-inflight-absorbs-the-query-half.md) owns
the *migration* of query logic out of the bash scripts; this owns what the tool does not do yet.
<!-- post-merge: checked-begin -->
`bin/inflight.mjs` shipped in astubbs/parallel-consumer#400 with `prior-art`, `note find`,
`note drift` and `stranded`.
<!-- post-merge: checked-end -->

**Direction, not a constraint: build it as though it will be extracted as a FOSS project one day.**
Antony's steer. It is a vision guide for where the seams go rather than a rule to satisfy now - but
it already argues for one concrete thing: repository-specific facts belong in one place. `REPO` and
`NOTES_DIR` are single constants today; the `docs/plans|solutions|inflight` section list is
hard-coded inside `bin/lib/prior-art.mjs`, and that is the one to lift before it spreads.

## `inflight branch` - the per-branch view

`corpusIndex` already computes both sides of the map - path to refs, and ref to what it carries - and
`stranded` reads only the first. The second side answers a different question entirely, for no extra
git work. Per branch:

- its PR and state, from the map `prsByBranch` already builds and caches
- whether it is pushed anywhere at all
- how many notes it carries that the baseline has never held
- whether it is fully contained in the baseline, via `git merge-base --is-ancestor` and **never**
  `git branch -d` or `git cherry`, both of which answer a different question than they appear to
  (see the sibling note's git-traps section)
- **the Claude session that owns it** - see below

**Measured 2026-09-02: 144 local branches, 68 of them pushed nowhere at all, and 10 in-flight notes
that exist only on one disk.** Three branches are fully contained in master and could be deleted.
There are 131 worktrees.

## The Claude session that owns a branch

**Antony's ask, and the data already exists** - this needs no new plumbing, only a reader. Measured
2026-09-02: **1035 commits carry a `Claude-Session:` trailer, across 63 distinct sessions**, and 72
worktrees carry a `.worktree-owner` marker.

Two sources answering different questions, and the output must say which is which:

- the **commit trailer** is durable, travels with the branch, and works from any clone - it says
  which session *produced* this work
- the **`.worktree-owner` marker** is local and uncommitted - it says who is holding that worktree
  *right now*

"Which session owns this?" is currently answered by asking an agent to go hunting.

## The tracking-gap detector, and the remedy it must emit

**This is how work gets lost, and it is the reason the tool exists.** A branch with no PR, no
`docs/inflight/branch-*.md`, and no mention anywhere in `docs/inflight/` is invisible to every check
this repository runs: `gh` cannot see it, CI cannot see it, another clone cannot see it.

Worked case, from the day the tool was built: `bin/prior-art.mjs` - 226 lines of tooling - sat on an
unpushed local branch with no PR and no note. It was found by a hand-written `for-each-ref` sweep,
not by any gate.

**It must emit the remedy, not a report.** A report gets skimmed; an instruction gets acted on. For
each gap: *push it*, or *write `docs/inflight/branch-<slug>.md` saying what this is*. The `branch-`
prefix already exists in [`AGENTS.md`](AGENTS.md) for exactly this - "work sitting on a branch with
no PR" - and nothing enforces it.

**Baselining is a TIMESTAMP LOOKUP, not stored state** - Antony's design, and better than the two it
replaced (a committed marker per orphan, or a generated snapshot the tool diffs against; both store
something that goes stale). The moment `bin/inflight.mjs` first appears on the baseline is the moment
tracking became expected, and it is recoverable from git at any time:

```
git log <baseline> --diff-filter=A --format=%ct -- bin/inflight.mjs | tail -1
```

A branch whose own history predates that was cut when nothing asked, and is reported as backlog
rather than as a new gap. One cut afterwards has no excuse. The grandfathered set shrinks on its own
as those branches land or die, and there is no snapshot file to rot. **An unknown moment never
grandfathers** - if the tool has not reached the baseline yet, every gap still reports loudly, since
silencing everything is the worst possible default for a detector.

**The remaining day-one concern.** Roughly sixty branches would report at
once, and a check that always says the same sixty things is a check nobody reads. The answer is not a
separate baseline file: **backfill the branch index first**, giving every existing orphan a
`branch-*.md` even where it says only "not yet triaged". An orphan is then *by definition* a branch
with no note, new ones stand out against a recorded set, and there is no generated snapshot to go
stale - which is the failure [`docs/todo-index.md`](../todo-index.md) is this repository's cautionary
tale for.

## Related branches, from the commit graph

**Antony's ask.** Looking at one branch, show what else relates to it - what it integrates, and what
integrates it. Exact from containment, no heuristic: branch A is a parent of B when A's tip is among
B's commits off the baseline.

One `rev-list <ref> ^origin/master` per ref builds the whole map - **measured at 1.9s across 436 refs
and 27,775 commits** - after which every relationship is a set-membership test rather than a fork.

Demonstrated on `origin/feats/ks-streams-reconciled`, which has no PR and looked orphaned:

```
  PARENTS - branches fully contained in it (it integrates them): 8
      origin/feats/ks-streams-error-surfacing
      origin/feats/ks-streams-example
      origin/feats/ks-streams-execution-seam
      ... and five more
  CHILDREN - branches that contain it: 1
      origin/feats/ks-on-pc-spike
```

The name says "reconciled" and the graph proves it: it is an integration branch for eight siblings.
**That is also the answer to why it has no PR**, which the tracking-gap detector would otherwise
report as a bare orphan - so these two features are worth building together, and the detector should
say "integration branch for N others" rather than "tracked nowhere".

## Asking GitHub: local first, one name on a miss, and the fix is the cache

**The bulk PR fetch must stay cheap.** Carrying every PR body took that response from **56K to
2.3MB** - Antony caught it - to answer a question about the rare branch that looks untracked. A PR's
`baseRefName` is a few bytes and settles the common case exactly, because a base ref IS a branch.

So the shape is: answer from the tree and the cheap bulk fields; only when a branch would otherwise
be reported as untracked, ask GitHub's **search API about that one name** - measured at 0.94s - and
cache it locally.

**The real cache is the fix, not the cache.** The agent writes the tracking note, the note merges,
and every later run answers from the tree without asking GitHub at all. A query that eliminates
itself beats a warm one.

### Where this goes, once there is a GitHub graph

Antony, thinking out loud, and worth keeping because the bootstrap problem is the hard part of
[`ci-issue-index-has-no-edges.md`](ci-issue-index-has-no-edges.md):

- the per-branch search results are exactly the edges that graph would hold, so **this is a cheap
  bootstrap for it** - the cache is populated by ordinary use rather than by a migration
- **a shared bootstrap could ride on GitHub Actions artifacts**, which would make GitHub itself the
  store - and one that **TTLs naturally when nobody is working on the project**, which is the right
  behaviour for a cache of a repository's live state rather than a defect in it

  **The mechanics check out, probed 2026-09-02 rather than read off documentation.** An artifact
  belongs to a workflow RUN, and the run carries the commit it was built at, so **the artifact's
  identity is a master SHA** - which is exactly the comparison key a client needs. Listing artifact
  metadata costs **one API call, 0.42s, and no download**: `repos/{owner}/{repo}/actions/artifacts`
  returns `workflow_run.head_sha`, `size_in_bytes` and `expires_at` per entry. This repository
  already holds 4601 of them at mixed retentions.

  So the sync is Antony's: compare the newest artifact's SHA with the one your cache was built at;
  equal means there is nothing to do and nothing was transferred. Different means **ask git locally
  whether the intervening range touched anything the cache covers** - free, offline - and only then
  download. The 90-day retention is the expiry, so a project nobody is working on loses its cache
  without anyone deciding to.

  **What must be tested before it is built, not assumed** (`docs/agent-harness.md`'s standing rule):
  whether a fork PR can read base-repo artifacts at all, what auth a download needs outside Actions,
  and whether the per-artifact size cap bites once the graph is more than edges.

Neither is a decision. Both are recorded because the alternative was losing them.

## Prior art to mine: Antony's own Gerrit fork

**Antony built distributed issue tracking on a Gerrit fork around 2008**, as a company that never
went public, and still has the code. He has asked that it be read for ideas before this goes much
further, and it is worth more than a courtesy look: it is the same problem, attempted by the person
now specifying this one, with the benefit of knowing which parts did not survive contact.

The timing is the interesting part. Everything in that generation - ditz, Bugs Everywhere, ticgit,
git-issue - shared one failure: the tooling assumed a human would maintain the state by hand, and
humans did not. What has changed is not the idea but who maintains it. An agent will, and there is
now a reason to read the state that did not exist then.

**And the field is still open**, which the 2026-09-01 survey establishes rather than assumes:
[`../plans/2026-09-01-001-investigate-beads-comparison.md`](../plans/2026-09-01-001-investigate-beads-comparison.md)
found exactly one surveyed tracker that keeps state in the working tree, and nothing at all that
makes GitHub's own graph queryable from the repository.

Read it before the GitHub-graph work starts, not after - a design already half-built is the worst
moment to discover someone solved a piece of it seventeen years ago.

## Before this merges

**Re-run the comparison against Backlog.md and the rest of the field** - Antony's instruction, and
deliberately timed for just before merge rather than now. The survey in
[`../plans/2026-09-01-001-investigate-beads-comparison.md`](../plans/2026-09-01-001-investigate-beads-comparison.md)
was written before this tool existed, and the thing being compared has changed underneath it: what
was a proposal is now a working CLI with commands, measurements and a self-test. The question "what
does adopting Backlog.md replace" therefore has a different answer than it did, and the honest
version of it can only be asked once the tool has stopped moving.

## Delete when

Each command above has shipped or been ruled out in writing, and the branch index has been
backfilled so the tracking-gap detector reports only new orphans.
