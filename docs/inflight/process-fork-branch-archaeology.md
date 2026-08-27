# Nothing has ever audited the fork's own branches

<!-- inflight-type: task -->
<!-- inflight-impact: stranded-work -->

Opened 2026-08-20 by the owner's question, on finding that the code behind the project's
most-linked artifact sits on a branch named in no ledger: *"How can a branch be invisible to every
inventory we keep? How the hell did we not find this in our archaeology hunts, and what else have we
missed?"*

The trigger is `origin/presentation`, found while scoping the per-language comparison demo on
`feats/classic-vertx-demo` (its own entry, `branch-classic-comparison-demo.md`, lands with that
branch). It carries `Demo.java`,
the code that produced the asciinema cast embedded in `src/docs/README_TEMPLATE.adoc`. (The branch
itself was archived as the tag `archive/presentation` and deleted on 2026-08-26; the account of it
lives in `branch_accounting`, which is the point of that section.) It was never
merged, is 3 ahead / 631 behind, and has been untouched since 2021.

## How it was missed: every audit was keyed on the tracker, never on the artifact store

Four inventories exist. Each was scoped to something that structurally excluded this branch, and
three of the four were correctly scoped - the gap is that nothing covers what falls between them.

1. **[`upstream-map.yaml`](../../src/docs/development/upstream-map.yaml)** maps fork↔**upstream
   issues and PRs**. `presentation` has neither. Never a candidate, and its header says so honestly.

2. **[`docs/upstream.md`](../upstream.md), "Surfaces checked and ruled out"** carries an entry
   literally titled **"Orphan branches never attached to a PR"**, naming five: `v0.6.x`, `v0.6.x-dev`,
   `0.5.3.x`, `v0.5.2.x-dev`, `DP-12547`. That is exactly the right *shape* of check - and it was run
   against **`upstream`** (confluentinc), not **`origin`** (astubbs). `presentation` does not exist on
   upstream at all.

   **This is the miss, and it is worse than a gap**, because the entry reads as "orphan branches:
   ruled out, do not re-investigate". A reader takes it as covering orphan branches generally. It
   covers five branches on one of two remotes.

3. **[`branch-package-rename-sweep.md`](branch-package-rename-sweep.md)**, the 38-branch sweep,
   enumerated from `gh pr list` - branches with **open PRs**. Thirty-eight PRs, not 198 branches. A
   2021 branch with no PR was correctly out of scope: that sweep's job was keeping open PRs mergeable
   across the rename.

4. **[`branch-stale-and-diagnostic.md`](branch-stale-and-diagnostic.md)** is a hand-written
   salvage-or-delete list of branches someone had already looked at. Not an enumeration.

**No inventory has ever run `git ls-remote origin` and demanded an account for each ref.** Every
audit started from a list of *interesting things* - PRs, upstream issues - and worked outward. None
started from the *complete set of refs* and worked inward. That is the defect class, and it is the
same shape as the recorded rule about searching only `--state open`: a search seeded from the tracker
cannot find what the tracker never knew about.

## What else is missing: the measured scale

Counts as of 2026-08-20 (re-derive rather than trusting these; the commands are given so they can be):

| | |
|---|---|
| Branches on `origin` | 198 |
| Never had a PR of any state | 154 |
| No PR **and** not merged into `master` | 150 |
| ...of those, last commit predates 2026 | **112** |
| ...of those, named in **no** document in the repo | **109** |

`presentation` is one of 109, not a one-off. The pre-2026 tail is the 2020-2023 upstream-era
development history that came across with the fork: `demo`, `features/*`, `improvements/*`,
`refactor/*`, `continuous-encode`, `massive-refactor`, `predictive-offset-payloads` and so on.

```sh
git ls-remote --heads origin | awk '{print $2}' | sed 's|refs/heads/||' | sort
gh pr list --repo astubbs/parallel-consumer --state all --limit 500 --json headRefName --jq '.[].headRefName' | sort -u
git branch -r --merged origin/master
```

### The content check: all 109 carry code that exists nowhere on master

"Not merged into master" is computed by ancestry, and squash-merges break ancestry - so the 109
could in principle be branches whose content landed years ago under a different commit. It was
checked rather than assumed: for each branch, which **file basenames** in its tree have no
counterpart anywhere in master's tree.

Nine files are common to more than half the branches - `Jenkinsfile`, `RELEASE.adoc`,
`EnumCartesianProductTestSets.java`, `JavaEnvTest.java`, `StringTestUtils.java` and four more. That
is the shared baseline master has since deleted or renamed, and it is subtracted as noise.

**After subtracting it, all 109 still carry branch-specific classes that exist nowhere on master.**
The largest: `features/consumer-interface` (27), `refactor/gpt3-central-queue-direct-pull` (26),
`refactor/gpt3-queue-management-with-msg-push` (23), `massive-refactor` (23), `partition-state` (22),
`continuous-encode` (18). The names are real design explorations, not scaffolding - `CentralQueue`,
`ControlLoop`, `PCWorkerPool`, `ChaosBroker`, `AutoScalingProcessor`, `HealthCheck`,
`ConnectExampleApp`, `ParallelJoin`, `BitSetFragment`, an `Actor`/`ActorRef` family across a dozen
branches.

**Read this as "109 branches nobody has an account for", not "109 lost treasures."** Two honest
caveats: basename matching over-counts, because a class that was renamed on master reads as absent;
and much of this is work that was deliberately tried and abandoned, which is a perfectly good account
- it is just an account nobody has written down. The finding is the absence of the account, not a
claim about the value of the code.

## Already surfaced by the first pass

- **`upstream/pyallel-consumer` is a prior Python client**, from `confluentinc#443` (Robbie-Palmer,
  "feature: Python Support", CLOSED unmerged), plus `confluentinc#539` ("Automatically Publish Python
  Package to PyPi", MERGED). `confluentinc#443` **is** tracked in `upstream-map.yaml` under `sweep-2023-long-tail`,
  characterised in one line as having "attacked the same goal from the client side" - but the branch
  itself has not been read, while a Python client is being written right now on
  `feats/proxy-requirements` (astubbs#293, astubbs#242). This is prior art in the strongest sense:
  someone already did this, and their code is sitting there. `confluentinc#539` appears in no
  document.
- **The ruled-out entry accounted for only a handful of upstream's branches as orphans**, implying
  that nearly all the rest each had a PR. That implication has not been verified.
  `git ls-remote --heads upstream` is the current total; the section below measures the gap and
  dates it.

## The other remote has the same hole, and it is not ours to lose

Checked 2026-08-20, after this note was written. **34 of upstream's 42 branches exist on no branch of
this fork.** The tempting reading is that we had them and deleted some. Tested and refuted: for 33 of
the 34, *every commit on the branch* postdates this fork's creation, so they cannot have been ours to
delete. The one exception, `features/dynamic-concurrency-control`, we do have - our copy diverged
under our own work.

The mechanism is worth writing down because it is invisible and permanent: **a fork receives
branches once, when it is created, and never again.** This fork dates from 2020-11-11; upstream went
on creating branches for years afterwards, and not one of them arrives. The older the fork, the wider
the gap, and nothing reports it.

**Closed 2026-08-20: all 42 upstream branches are now mirrored on this fork under `upstream/*`.**
Tips verified identical to upstream's. The namespace is load-bearing rather than tidy - eight
upstream branches share a name with one of ours carrying divergent work, `master` among them, so
pushing them under their own names would have overwritten our history. Nothing of ours was touched.
Re-derive rather than trusting a list: compare `git ls-remote --heads origin` against
`--heads upstream` by name and tip.

**Cleaned up the same day:** the 17 bot dependency branches among them were deleted again, and the
deletion is recorded in `branch_accounting` rather than left as an absence. Sixteen were single-commit
pom bumps against upstream's diverged tree and would not apply here. The seventeenth was not a bot
branch at all despite its name - a maintainer had pushed CI, licence-header and gitignore commits onto
`dependabot/maven/org.threeten-threeten-extra-1.8.0` - and it was read before being deleted, then
recorded separately. **That is the argument for checking contents rather than prefixes**, and it is
the same failure this note is about: a prefix sweep would have taken human commits with no trace that
anyone had looked.

What was in the gap is not all noise. `pyallel-consumer` and `python-cd-pipeline` are a prior Python
client - prior art in the strongest sense while a Python client is being written on
`feats/proxy-requirements` (astubbs#293, astubbs#242). `features/batching`, `docs/back-pressure`,
`improvements/vertx-vertical` and the `v0.6.x` / `v0.5.2.x-dev` / `0.5.3.x` release lines are also
absent. Upstream is unmaintained, so if that repository is ever archived or removed, none of it is
preserved anywhere we control.

## What the accounting must record, and why deletion is the case that matters

The check this note asks for was framed as answering "is every branch accounted for". That is not
sufficient on its own. **The record has to be permanent and per-branch, and it has to survive the
branch.** The failure it prevents is specific: a branch is reviewed, judged worthless, deleted - and
because the only trace was the branch itself, a later audit re-finds its absence, cannot tell a
deliberate deletion from an accident, and re-does the work. That is the same shape as the mirror
bodies this branch's sibling notes are about: the judgement existed, and nothing outlived the thing
it was about.

So the disposition belongs in
[`upstream-map.yaml`](../../src/docs/development/upstream-map.yaml) - the file that already exists to
cache judgements that were being re-derived every session - and it must record, per branch: which
remote it came from, what was decided, why, and when. A deleted branch keeps its entry, with its tip
SHA, so "we deleted this deliberately on this date for this reason" is a fact the next audit reads
rather than a hole it rediscovers.

**Settled 2026-08-20:** the manifest now carries a `branch_accounting` section, keyed by ref rather
than by work item, seeded with the mirrored upstream branches and the two of ours that carry a
decision the branch itself does not. `state: deleted` keeps its entry, with tip, date and reason -
that case is the whole point.

It is also now the ONLY branch record. Two others existed - `preserved_branch_tips` and, inside the
2023 sweep entry, `preserved_heads` - and they recorded the same commits under different framings:
every `preserved_heads` PR head is the tip of an upstream branch, so `pr: confluentinc#443` and the
`pyallel-consumer` tip were two names for one SHA. Both are folded in, with `tag` and `pr` as fields
on the branch entry.

The rule that keeps it useful: **nothing a command answers.** Open PRs are `gh pr list`; merged-ness
is `git branch --merged`. This records the judgement, not the state.

## What this wants

Not a sweep-by-hand. A **check**, so the answer stays true:

- Enumerate `origin` and `upstream` refs, and require every one to be accounted for by exactly one
  of: merged, has a PR (any state), named in a document, or tagged as archived.
- Report the unaccounted set. Absence must fail loudly rather than read as coverage - the same
  epistemology the conformance suite already applies to the wire.
- Fix the `docs/upstream.md` "Orphan branches" entry, which currently over-claims: say which remote
  it covered.

## Related

- `branch-classic-comparison-demo.md`, landing on `feats/classic-vertx-demo` - where this was
  found, and the branch that rescues the artifact. This entry is deliberately **not** stacked on that
  branch: the audit is a repo-wide concern with nothing to do with the language proxy, and stacking
  it would gate a master-level finding behind an unrelated feature PR.
- [`upstream-coverage-completeness.md`](upstream-coverage-completeness.md) - the sibling
  obligation, pointed at upstream issues/PRs/discussions. Same "not sampled, not the interesting
  ones" standard; different surface. That one is explicit that it is unfinished; this one nobody knew
  was open.
