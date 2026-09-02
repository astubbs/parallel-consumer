# The commands `inflight` should grow next

<!-- inflight-type: feature -->
<!-- inflight-impact: blind-spot -->

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

  **DECIDED NOT NOW, 2026-09-02.** The whole GitHub node set is 123 issues and 285 PRs - about 2MB
  with bodies, and 56K without - so a client can simply fetch it. Sharing a bootstrap is a fix for a
  cost that does not exist yet, and it is one person on this repository today. Revisit if that
  changes; the mechanics above are recorded so nobody has to re-derive them.

  **What would need testing then, and was not** (`docs/agent-harness.md`'s standing rule): whether a
  fork PR can read base-repo artifacts at all, what auth a download needs outside Actions, and
  whether the per-artifact size cap bites once the graph is more than edges.

Neither is a decision. Both are recorded because the alternative was losing them.

## The graph store: JSON in memory, and no database

**Decided 2026-09-02, from measurements rather than taste.** The graph is 123 issues and 285 PRs -
**408 nodes**. With edges that is about **48KB of JSON, 1ms to build, and a full two-hop traversal in
under a millisecond**. This repository already runs the same shape at sixty times the scale:
`commitGraph` holds 27,775 commits across 436 Sets, `corpusIndex` holds 26,539 rows.

- **JSON, not YAML.** `gh` already emits JSON, so there is no conversion step at all, and `JSON.parse`
  is built in. Node has no YAML parser and this repository has **no `package.json` anywhere** - zero
  npm dependencies, deliberately. YAML's advantage is human editing, which does not apply to a
  machine-written throwaway cache.
- **Not Neo4j embedded**, and not for taste: it takes an **exclusive store lock, one process per
  store**. There are 131 worktrees here and agent sessions run concurrently - they would block each
  other on a cache. It is also JVM, against a Node tool, for 48KB of data.
- **Not Titan** - it became JanusGraph in 2017 and needs Cassandra, HBase or BerkeleyDB behind it.
- **`node:sqlite` is the escape hatch, named so nobody re-derives it.** Node 24 ships it, so it stays
  dependency-free. It earns its place when the graph stops fitting comfortably in memory or a query
  should not load all of it - think 100k nodes, three orders of magnitude from here. Its cost is a
  binary file that does not diff, does not merge, and cannot be reviewed in a PR: fine for a cache,
  wrong for anything committed.

**The layering that falls out:** what is committed and human-facing stays markdown, as
`docs/inflight/issue-index.md` already is; the graph is a throwaway JSON cache loaded into Maps. No
new storage technology is involved.

## Prior art, read: Antony's 2010 Gerrit-based issue tracker

**Read 2026-09-02. It changes nothing at this stage** - Antony's call, and the right one - so this is
a record rather than an input to any decision. Kept because the next person to reach for a graph
store should see it first.

**Where it is:** `project-gittybits`, a private repository on Antony's own git server. It is the only
copy: nothing Gittybits-shaped exists on GitHub under that account, and nothing public anywhere. Ask
him for access; how a non-Mac host reaches that server is his homelab repository's business, not
this one's.

**What it is.** Five Maven modules under `com.sharca.*`, 208 commits, May to November 2010, built
against Gerrit over SSH. A domain model of `Issue`, `Comment`, `Project`, `User`; a GWT web UI; a
Grails server; a Griffon desktop client; and `gitfocus-jdits-engine` - the store.

**The design decision worth having seen.** The store was **pluggable, with two implementations
bound by Guice annotations**: `@DirectFileIO` and `@OrientDBIssueStore`. Issues serialised as
**YAML**, with **OrientDB** holding the per-project "nice ID" counter - the human-readable issue
number. That is the same axis - *where issue state lives relative to the code* - that the
[2026-09-01 survey](../plans/2026-09-01-001-investigate-beads-comparison.md) found still unoccupied
sixteen years later.

### Three things it settles, none of which change today's plan

- **The human-readable ID is the hard part, and it needs a coordination point.** The engine's README
  documents recovering a desynchronised counter by hand: connect to OrientDB, `update
  projectissuecounter set issueNo = …`. A sequential per-project number is exactly what a
  distributed store cannot give cheaply, which is why Backlog.md and Beads both use hash IDs, and
  why this tool sidesteps the problem entirely by making the **filename** the identity.
- **The database is what made it unreadable, not the format.** OrientDB is effectively gone, the
  same arc as Titan into JanusGraph. A 2010 design is stranded by its datastore while its YAML
  issues would still parse today - which is a better argument for the JSON-in-memory decision above
  than anything reasoned from first principles. Corroborating: Antony forked OrientDB on 2010-07-29,
  mid-project, and the README references building private `0.9.22-SNAPSHOT` artifacts.
- **YAML and JSON are not competing here, they are different layers.** That store was human-facing
  and committed, where YAML earns its readability. The decision above is about a machine-written
  throwaway cache, where it buys nothing and costs a parser Node does not have.

### What is NOT there, checked across all 33 refs

**The engine was never implemented.** `gitfocus-jdits-engine` holds two Guice binding annotations
and two SnakeYAML workarounds - four Java files - and never exceeds that on any ref. The project
itself did get substantial: 139 Java files on `master`. The work went into the UI, the Grails
server and the Gerrit connector; the storage layer stayed at its interfaces.

So the *decision* survives and the *implementation* does not. That is the whole of what this repo can
teach us, and it is worth exactly one read - which has now happened.

## The comparison was re-run, and it moved the answer

Antony asked for the Backlog.md comparison to be re-run once the tool had stopped moving, because
the 2026-09-01 survey was written while this was still a proposal.
[`../plans/2026-09-02-001-investigate-adopt-or-build-re-run.md`](../plans/2026-09-02-001-investigate-adopt-or-build-re-run.md)
**owns those findings** - it drove Backlog.md 1.50.1 rather than reading about it, and the headline
is that its cross-branch layer RECONCILES to one winner where `note drift` REPORTS the
disagreement. Read it there; this note does not summarise it.

The verdict still belongs to
[`process-adopt-external-harness.md`](process-adopt-external-harness.md), which owns the
adopt-or-build decision and keeps it deferred until after v6.

## The principle the comparison exposed: flow with git, do not suppress it

Antony's framing, and it is the design axis for everything below rather than a comment on
Backlog.md: **state in git is statistical, not binary.** "25% of branches show this as fixed" is the
true answer to a question about a note; one status is a summary that has thrown away the shape of
the disagreement. A tool that reconciles is answering a question nobody asked here.

- **It is already how this tool is built**, not a new direction: `note drift` clusters by blob and
  reports every version, and `stranded` reports a ref-set. Neither has a notion of a winner. What
  the re-run supplied is the *reason* - a comparison against a tool that took the opposite position
  and made the difference visible.
- **The gap is in the OUTPUT, not the model.** The corpus is already a distribution; nothing reports
  it as one. There is no command that answers "of the refs carrying this note, what proportion say
  what", and the shape it would print - proportions, with the outliers named - is the natural next
  view given the data already collected.
- **Antony's precedent** is Hasten - another of his projects, not in this tree, so nothing here
  verifies it - where features with novel advantages *fell out of* the architecture rather than
  being designed in. The bet is the same: commit to git's model all the way down, and the
  distinguishing features arrive as consequences.
- **Backlog.md remains worth reading for ideas**, translated rather than adopted. Its board and its
  branch-scan loader solve real presentation problems; only its reconciliation step is wrong here.

## The corpus is not every ref, and the tool says it is

Measured 2026-09-02 in the development clone. `refTips()` enumerates `refs/heads` and
`refs/remotes/origin`, which is 436 refs - and the help text calls that "every branch tip", which is
true and is read as "everything".

| Ref space | Count | In the corpus? |
|---|---|---|
| `refs/heads`, `refs/remotes/origin` | 436 | yes |
| `refs/tags` | 64 | **no** |
| `refs/backup` | 44 | **no** |
| `refs/remotes/upstream` and three stray remotes | 3 | no, and correctly so |

**What that costs, measured rather than assumed** - both findings came from checking, and most of
the excluded refs turned out to be duplicates:

- **41 of 44 `refs/backup` tips are already commits inside the visible history**, so excluding them
  costs nothing. Of the remaining three, one - `pre-rename-merge/heads/264-rename` - holds a version
  of `branch-package-rename.md` that exists at no visible ref. One note, but `stranded` exists
  precisely to find that class, and it cannot see it.
- **12 of 64 tags point at commits outside the visible set**, named `backup/pre-recut-324`,
  `recut-baseline-342`, `archive/presentation` and so on. Tags are how this repo preserves work
  before a re-cut, which makes them a *likely* home for stranded knowledge rather than an unlikely
  one. Antony named tags explicitly.

The fix is not simply widening the glob: `refs/backup` and `refs/tags` are archival, so a version
found only there is a different finding from one on a live branch - preserved, not in flight - and
the output has to say which. Scope first, then widen.

## Fetch completeness, not just fetch age

Antony works across machines, so the corpus is only as complete as the last fetch on *this* one. Two
<!-- post-merge: checked-begin -->
defects in the freshness check were found and fixed in astubbs/parallel-consumer#400; the remaining
work is the part that acts rather than warns.
<!-- post-merge: checked-end -->

**Fixed** (`freshnessWarnings` in `bin/lib/git.mjs`, both negative-controlled in
`bin/test-inflight.mjs`):

- **A narrow fetch silenced the staleness warning.** FETCH_HEAD's mtime dates a fetch of any width,
  so `git fetch origin master` - one ref of 292 - reset the clock over a corpus exactly as stale as
  before. Measured: mtime forced to 2020, one single-ref fetch, mtime now. The file lists what the
  fetch brought, one line per ref, and a full fetch lists every ref it covered even when none moved
  - so width is readable rather than guessable. `narrow-fetch` now fires below a quarter of the
  corpus.
- **A fresh clone was told it may never have fetched.** `git clone` writes no FETCH_HEAD at all, so
  keying "never fetched" on that file's absence fired hardest on the newest corpus obtainable.
  `packed-refs` is written by the clone, so its mtime dates the refs actually held.

**Still queued:**

- **Fetch in the background rather than telling the user to.** Antony: accept that the odd action
  takes a few seconds longer. The shape is a fetch kicked off when the corpus is older than a
  threshold, with the current run answering from what it has and *saying so* - never a silent wait,
  because a command that sometimes blocks on the network is a command agents learn to distrust.
- **Prune and tags are not configured.** `fetch.prune` and `remote.origin.tagOpt` are both unset in
  the development clone, so deleted remote branches linger as remote-tracking refs - the opposite
  error to staleness, and one that makes `stranded` report work on a branch that no longer exists.
  Nothing has measured how many of the 292 origin refs are already dead this way.
- **Completeness is a claim the tool should test, not assume.** The clone-level facts it already
  reads - shallow, single-ref, baseline missing - are the same class as "this remote has refs you do
  not have", and that one needs the network to answer.

## Delete when

Each command above has shipped or been ruled out in writing, and the branch index has been
backfilled so the tracking-gap detector reports only new orphans.
