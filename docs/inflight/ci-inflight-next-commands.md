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

**Baselining, so the detector is not noise on day one.** Roughly sixty branches would report at
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

## Delete when

Each command above has shipped or been ruled out in writing, and the branch index has been
backfilled so the tracking-gap detector reports only new orphans.
