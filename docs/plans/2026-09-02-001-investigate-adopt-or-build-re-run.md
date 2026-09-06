# Adopt-or-build, re-run now that the tool exists

**Status:** Re-run of the 2026-09-01 comparison, with the thing being compared no longer a proposal.
**Written:** 2026-09-02
**Does not supersede the record:**
[`docs/plans/2026-09-01-001-investigate-beads-comparison.md`](2026-09-01-001-investigate-beads-comparison.md)
says what was known that day and is left alone. This document supersedes its *conclusions* where
they are named below, per [`docs/citations.md`](../citations.md).
**Feeds, does not overrule:**
[`docs/inflight/process-adopt-external-harness.md`](../inflight/process-adopt-external-harness.md)
owns the adopt-or-build decision and defers it until after v6. This is evidence for that note.
**Executes:** the "Before this merges" instruction in
[`docs/inflight/ci-inflight-next-commands.md`](../inflight/ci-inflight-next-commands.md) - Antony's,
deliberately timed for just before merge.

---

## 0. What was RUN this time, and what was not

The 2026-09-01 pass opened by declaring itself documentary and breaking
[`docs/agent-harness.md`](../agent-harness.md)'s standing rule on purpose. **This one does not, and
the decisive finding is one no amount of reading would have produced** - Backlog.md's documentation
and its README describe the cross-branch feature accurately, and still leave you with the wrong
model of it.

**Run, on this machine, 2026-09-02:**

- **Backlog.md 1.50.1 driven end to end** in a throwaway git repository: `init`, a task on `main`,
  two divergent branches editing it, `task list --plain`, `task <id> --plain`, `board export`,
  `--help` for the command surface. Installed transiently with `npx --yes backlog.md@1.50.1` - the
  same discipline the earlier §6 chose for `bd`: nothing global, nothing on `PATH`, deleted with the
  scratchpad.
- **Its source read** at a pinned commit, `876523c` (2026-09-02), rather than its docs.
- `npm view backlog.md version` -> **1.50.1**; `gh api` for repository metadata and for the current
  state of the three issues the earlier pass cited.
- `node bin/inflight.mjs --perf` on `stranded`, `note find`, `note drift`, and
  `node bin/test-inflight.mjs` in full.
- Measurements over this repository's refs, reported below with the command that produced them.

**Not run, and therefore not claimed:**

- **Beads.** Probe 1 of the earlier §6 - *does `bd` follow the git branch* - is still unrun. What is
  said about Beads here is its README re-read 2026-09-02 and carried forward, not re-established.
- Backlog.md's browser, MCP server and TUI. The board was exercised through `board export`, which is
  the same loader without the UI.
- No migration was attempted. The file-by-file cost in §4 is arithmetic over this tree, not a spike.

---

## 1. What changed underneath the question

The earlier survey compared Backlog.md against a *proposal* -
[`ci-node-query-client.md`](../inflight/ci-node-query-client.md), a Node client that would read
notes across every branch. Against a proposal, "Backlog.md already ships that" is a strong argument,
and it is the argument that note now carries: *"Backlog.md already does the tracker half, including
the cross-branch part."*

The thing being compared is now `bin/inflight.mjs`: six commands, a corpus index over 436 refs, and
`bin/test-inflight.mjs`. **So the comparison can be made against behaviour instead of intent, on
both sides** - which is the only reason this re-run finds anything.

---

## 2. The decisive finding: their cross-branch layer RECONCILES, ours REPORTS

**These are opposite operations on the same data, and the earlier survey read one as the other.**

### The probe, and what it returned

A throwaway repository, `checkActiveBranches` at its default of `true`:

| Ref | What it holds |
|---|---|
| `main` | `TASK-1` "Shared item", status **To Do**, description "MAIN version" |
| `feat/a` | `TASK-1` edited; plus `TASK-2` and `TASK-3`, which exist **only** there |
| `feat/b` | `TASK-1` edited, status **Done**, committed 80 seconds after `feat/a` |

Standing on `main`:

- `backlog task list --plain` shows **one task**: `TASK-1`, To Do. Not `TASK-2`, not `TASK-3`.
- `backlog task task-1 --plain` shows main's description and To Do.
- Asking for a task that is not in the working copy answers, in its own words:
  **"Task lookups read only the local working copy; use 'backlog browser' to see tasks from other
  branches."**
- `backlog board export` *does* cross branches - it prints `Indexing 2 other local branches` and
  `Applying latest task states from branch scans`, and its output carries **three** tasks, so
  `TASK-2` and `TASK-3` are discovered from `feat/a`.
- **And `TASK-1` is in the To Do column**, though the newest version of it anywhere says Done.

Nothing in any of that output says the branches disagree.

### Why, from their source rather than from the behaviour

Same-path versions resolve in favour of the working copy, deliberately: their own task
`back-557` states the rule as *"Versions at the same path resolve as one identity with working-copy
authority, while live identities at distinct paths remain ambiguous and fail closed."* The selection
itself is `chooseWinners` in `src/core/task-loader.ts` - the variable is literally named `winners` -
which takes the newest remote when there is no local version, and otherwise defers; ties break
through `selectPreferredIdentityEntry` on commit time, then lifecycle rank, then branch name. The
older `getLatestTaskStatesForIds` in `src/core/cross-branch-tasks.ts` opens with the same intent -
*"Determines the latest state of tasks across all git branches"* - and its consumer is
`filterTasksByLatestState`, whose job is to **hide** a task whose newest version elsewhere is
completed, archived or a draft.
<!-- file-refs: N/A - paths and identifiers in this paragraph are in MrLesk/Backlog.md at 876523c, read via a shallow clone, not files in this repository -->

**So the feature exists to make one board correct. It is drift *suppression*, and it is the right
design for what it is for**: a task has one true state, and a stale local copy should not contradict
it. `docs/inflight/AGENTS.md` chose the opposite premise in as many words - *"two branches can
disagree about what is open"* - and built the tag vocabulary, the branch-travel property and
`note drift` on top of it.

### The scale of what that discards, on this repository

`node bin/inflight.mjs note drift docs/inflight/bug-857-family.md`, run 2026-09-02: **275 of 436
refs carry that note, and 25 distinct versions carry content `origin/master` has never held, across
80 refs.** A reconciler answers that question with one version and no indication that twenty-four
others exist. That is not a missing feature in Backlog.md; it is the model working.

---

## 3. Command by command - is the overlap real?

| `bin/inflight.mjs` | Its question | Backlog.md's nearest | Same capability? |
|---|---|---|---|
| `note find` | which note paths exist on **any** ref, and which refs carry each | the board's branch scan | **Partly.** It does discover branch-only items - proven above. It reports the winner, never the ref set, so it cannot say *which* branches carry a thing. |
| `note drift` | versions carrying content the baseline has never held, clustered by blob | none | **No** - and contrary to the model, per §2. |
| `stranded` | notes on a branch that never reached the baseline, after subtracting renames and landed-then-deleted | none | **No.** There is no baseline in their model; active branches are symmetric, and a version at a *different* path is "ambiguous" rather than a rename. |
| `prior-art` | plans, solutions, notes, commit messages and GitHub, across every ref | `backlog search` | **No.** Different corpus - its own tasks, docs and decisions through fuse.js - and it searches the loaded set, not every ref. |
| `branch <ref>` | a branch's PR, its session, what it integrates and what integrates it, and whether anything tracks it | none | **No.** |
| `cache pr` | the GitHub PR set, folded in one PR at a time | none | **No.** There is no GitHub integration at all - see §6. |

**One of six overlaps, and even that one answers a different question.** The earlier survey's §10
sentence - *"Backlog.md ships `checkActiveBranches`, `remoteOperations` and `activeBranchDays` …
that is cross-branch drift detection, in a shipping tool"* - is accurate about the configuration and
wrong about the capability.

---

## 4. What adoption would replace TODAY, file by file

`wc -l`, this tree, 2026-09-02.

**Replaced in kind, if and only if the schema survives §5:**

| File | Lines | What is left over |
|---|---|---|
| `bin/lib/notes.mjs` | 461 | its `find` half is approximated; `drift` and the rename subtraction have no counterpart |
| `bin/check-inflight-tags.sh` | 169 | a schema makes a validator redundant - but only for the axes the schema has |
| `bin/lib/inflight-tags.sh` | 66 | same, and the type-partitioned impact sets have nowhere to go |
| `docs/inflight/AGENTS.md` | 325 | the note contract; most of it is the argument for the axes, which does not migrate |

**Not addressed, and kept regardless - this is the larger half:**

| File | Lines | Why it survives |
|---|---|---|
| `.claude/hooks/inject-branch-context.sh` | 654 | its inputs are git and GitHub, not a tracker |
| `.claude/hooks/inject-recorded-knowledge.sh` | 437 | the session index, ordered by consequence; there is no injection layer to adopt |
| `.claude/hooks/check-merge-outstanding-work.sh` | 364 | a merge-time gate over notes and `gh` |
| `bin/check-branch-self-reference.sh` | 360 | a rule about what a note may say about its own branch |
| `.claude/hooks/remind-inflight-on-push.sh` | 239 | fires on a git event |
| `bin/issue-index.sh` | 127 | GitHub to tree - the half nobody ships |
| `bin/lib/prior-art.mjs`, `branches.mjs`, `git.mjs`, `views.mjs`, `cache.mjs`, `perf.mjs` | 1,243 | none of these questions is a tracker question |
| `bin/inflight.mjs` | 369 | the front door for all of the above |
| `bin/test-inflight.mjs` | 886 | the self-test, including its negative controls |

**And a migration cost that is not optional.** Backlog.md's identity is a canonical ID plus a
normalised path, and its filenames carry that ID (`task-1 - Shared-item.md`, observed). This
repository's notes have **no ID** - `docs/inflight/AGENTS.md` makes the filename the identity and
forbids it carrying state. Adopting means renaming every note into `backlog/tasks/`. The working
tree carries **461** fully-qualified `docs/inflight/*.md` citations
(`grep -rEoh 'docs/inflight/[A-Za-z0-9._/-]+\.md' . | wc -l`), plus relative ones, and
`bin/check-file-refs.sh` fails on each broken one. That is the cost before any behaviour changes.

**Partial adoption is the dangerous outcome, not the safe middle** - the earlier survey's §3 said
so and it is still right. Adopting the tracker core while keeping the hooks, the gates and the
GitHub surface means two places to look for what is open.

---

## 5. The schema round-trip, tested against their source

The earlier §4 argued the axes cannot survive, on the grounds that *"`P1` is a position in a queue;
`misdirection` is a statement about what is wrong with your instruments"*. **That is too strong, and
the real loss is somewhere else.**

- **`priorities` is a configurable, ordered, validated vocabulary.** `resolvePriorityValue` rejects
  a value outside the configured list and `getPriorityRank` orders by its position, so you could
  literally configure `priorities: ["misdirection", "blind-spot", "data-loss", …]` and keep both the
  names and the signal-integrity-first ordering.
- **What it cannot express is the partition.** `bin/lib/inflight-tags.sh` keeps
  `INFLIGHT_BUG_IMPACTS` and `INFLIGHT_TASK_IMPACTS` as separate sets *because the index groups them
  separately*, and its header records the failure that forced it: a `bug` carrying `release-gate`
  passed a flat-set gate and then appeared under "unmatched". Backlog.md's priority list is flat and
  global, so that gate is not reconstructible.
- **`labels` is an OPEN set, which is the one property our third axis exists to have.**
  `collectAvailableLabels` unions the configured list with whatever labels tasks actually carry, so
  config is a suggestion list. `docs/inflight/AGENTS.md`: *"A free-text field becomes tag soup within
  a month and then partitions nothing, which is the failure this whole scheme exists to prevent."*
- **`inflight-state: deferred - <reason>` has no home.** `statuses` is a list of names with no
  free-text reason field, and the reason is what makes `grep 'deferred - after v6'` a release's
  worth of work.
<!-- file-refs: N/A - resolvePriorityValue, getPriorityRank and collectAvailableLabels are in MrLesk/Backlog.md at 876523c, not this repository -->

**Round-trip verdict: two axes survive, one lossily, one constraint and one field do not.** Narrower
than the earlier claim, and located differently.

---

## 6. What has moved since 2026-09-01

Checked 2026-09-02, by query rather than by re-reading the earlier text.

| Fact | Then | Now |
|---|---|---|
| Backlog.md version | not recorded | **1.50.1** (`npm view backlog.md version`), published 2026-08-10 |
| Stars / licence | ~6.6k, MIT | **6,603**, MIT, not archived, 35 open issues |
| Activity | "actively maintained" | last push 2026-09-01, head commit `876523c` dated 2026-09-02 |
| GitHub sync, their [#231](https://github.com/MrLesk/Backlog.md/issues/231) | closed NOT_PLANNED | **unchanged**, closed 2026-07-01, last touched 2026-07-02 |
| Lifecycle hooks, their [#456](https://github.com/MrLesk/Backlog.md/issues/456) | closed NOT_PLANNED | **unchanged**, closed 2026-07-01 |
| Beads integration, their [#588](https://github.com/MrLesk/Backlog.md/issues/588) | closed as too open-ended | **unchanged**, closed 2026-07-01 |
| Any new GitHub-integration issue | - | **none**. `gh api search/issues` for `github in:title` on that repo returns 8, all closed, newest 2026-07-02 |
| Beads | generation 2, Dolt in `refs/dolt/data` | **unchanged.** README re-read 2026-09-02: `.beads/embeddeddolt/`, `bd dolt push`/`pull` against `refs/dolt/data`, JSONL still *"an export"*. 26,800 stars |
| `sd0xdev/sd0x-dev-flow` | 188 stars | **renamed to `sd0xdev/sd0x-harness`**; the old URL still redirects, so the earlier survey's links resolve and need no repair |
| `karanb192/claude-code-hooks` | ~492 stars | 494, last push 2026-08-23 |
| `git-bug` | ~10k stars | 10,018, last push 2026-07-06 |

**Two things shipped that the earlier survey did not name**, both found by driving the CLI:

- **`onStatusChange`** - a shell command run on a task's status change, overridable per task in
  frontmatter. Their [#456](https://github.com/MrLesk/Backlog.md/issues/456) asked for a
  comprehensive lifecycle hook system and was declined; a
  single-event callback exists anyway. So *"Backlog.md is not going to grow the injection layer"*
  is right about injection and slightly wrong about hooks: there is exactly one seam.
- **`backlog cleanup`** - moves terminal-status tasks into a `completed/` folder by age. That is a
  compaction analogue, and the earlier §2 called compaction *"the strongest single thing Beads has
  that we do not"*. It is not only Beads.

---

## 7. What the earlier survey got wrong

It retracted three of its own claims; these are four more. None of them is a reason to think the
next pass will not find a fifth.

1. **"Backlog.md already does the cross-branch part" is withdrawn.** Reconciliation is not drift
   detection - §2. The consequence lands on
   [`ci-node-query-client.md`](../inflight/ci-node-query-client.md), which says *"Do not spend the
   budget proving something an `npm i -g` already does"*: the budget was spent, and what was built
   is not what `npm i -g` does.
2. **"With the default now on adopt, because it ships" is withdrawn** for the cross-branch reader
   specifically. It ships a different capability. The default on adopt is untouched for anything
   that genuinely ships - the hooks question in
   [`process-adopt-external-harness.md`](../inflight/process-adopt-external-harness.md) is not
   affected by this document at all.
3. **§10 over-corrected §8.** §8 claimed the distinctive axis is *state beside the code rather than
   beside the agent*; §10 retracted it because Backlog.md is in the tree too. Both are half right.
   The axis that actually separates them is not **location** but **authority**: whether a branch is
   allowed to disagree. Backlog.md is in-tree *and* single-truth. Beads, Tasks and OpenClaw are
   out-of-tree *and* single-truth. Nothing surveyed is in-tree and multi-truth.
4. **§4's priority-versus-consequence argument is overstated** - §5. Their priority vocabulary is
   configurable and ordered; the loss is the type partition, the closed label set and the state
   reason.

**And one earlier retraction is now testable, and survives in a sharper form.**
`process-adopt-external-harness.md` retracted *"the self-test density is unmatched"*, correctly.
Measured 2026-09-02: Backlog.md carries **245 test files and 63,413 lines of test** against 64,778
lines of source - near 1:1, far more than this repository. But `bin/test-inflight.mjs` runs **64
self-tests of which 32 are negative controls**, each mutating the unit under test and asserting its
own check goes red; a grep of their tree for mutation or negative-control tooling finds nothing.
So **volume is theirs and the admission rule is ours**, which is two of the three gates
[GENESIS](https://arxiv.org/pdf/2605.27360) asks for, not one. That is worth keeping and is still
not a reason to build a tracker.

---

## 8. Worth stealing regardless of the decision

- **`--modified-file`, and the `modified_files` frontmatter behind it.** Their tasks carry the files
  they touched, and search filters on it. That is a note-to-code edge, which is exactly the shape
  [`ci-issue-index-has-no-edges.md`](../inflight/ci-issue-index-has-no-edges.md) complains is
  missing here - and it is derivable from the commit that wrote the note, so it needs no store.
- **Compaction, now with two implementations to copy from.** `docs/inflight/AGENTS.md` still rests
  the anti-inflation duty on a person noticing the index has got long.
- **`--json`, versioned.** `bin/inflight.mjs` prints for a terminal only. Every consumer named in
  [`ci-inflight-absorbs-the-query-half.md`](../inflight/ci-inflight-absorbs-the-query-half.md) is a
  hook that would rather have structure than scrape a page.
- **A hazard to record before anyone runs it here**: `backlog init` writes `AGENTS.md`, `CLAUDE.md`,
  `GEMINI.md` and `.github/copilot-instructions.md` by default. Observed - it created `AGENTS.md` in
  the probe repository. The content is marker-delimited and therefore reversible, which makes it
  gentler than `bd init`, but this repository's `AGENTS.md` is the hand-curated router. Throwaway
  clone only, or `backlog agents` to manage the nudge deliberately.
  <!-- file-refs: N/A - the copilot path names a file Backlog.md would CREATE, in the throwaway probe repository; this tree deliberately has no such file -->

---

## 9. Recommendation

**Build. Do not adopt Backlog.md for the query layer, and do not treat it as having pre-empted
`bin/inflight.mjs`.** Confidence **high**, and the reason is not a feature count: the two tools take
opposite positions on whether a branch may disagree with the baseline, and this repository's whole
note contract is built on the position Backlog.md does not take.

**Confidence is high on this question only.** It says nothing about the hooks half - `sd0x-harness`,
`karanb192/claude-code-hooks` - which
[`process-adopt-external-harness.md`](../inflight/process-adopt-external-harness.md) owns and which
this pass did not re-examine. Nor does it un-defer anything: that note keeps the decision until
after v6, and this is evidence arriving early, exactly as
[`process-beads-evaluation.md`](../inflight/process-beads-evaluation.md) intends.

**What would change it, in falsifiable form:**

- **They ship a command that names the disagreeing versions instead of picking one.** Watch their
  `back-601` - its third item already concedes that cross-branch dependencies "fail closed as
  unknown" - and `doctor`, which is the only surface that currently reports a cross-branch condition
  at all.
- **Their [#231](https://github.com/MrLesk/Backlog.md/issues/231) reopens and a GitHub tunnel ships.** That is the half nobody occupies, and it is the
  larger part of what is left to build here.
- **This repository stops working branch-per-task.** If notes stopped diverging across refs, a
  reconciler would be adequate and the 461-citation migration would buy something. The measurement
  that would say so is `note drift` returning few divergent versions; today it returns 25 on one
  note.
- **Someone demonstrates the schema round-trip working**, including the type-partitioned impacts.
  §5 says it cannot; §5 is derived from their source, not from a migration, so it is the weakest
  claim here.

---

## 10. What could not be verified

- **Beads was not run.** Probe 1 of the earlier §6 is still open, and
  [`process-beads-evaluation.md`](../inflight/process-beads-evaluation.md) still owns it. Its
  architecture is re-confirmed from the README only.
- **The probe used local branches with no remote.** `remoteOperations` warned and skipped, so the
  remote-branch path is untested. It reads tasks the same way; the fetch is the difference.
- **Three tasks in a fresh repository is not this repository.** Whether their loader stays usable at
  436 refs and 575 note paths is unmeasured, and their own docs warn that cross-branch checking
  "may impact performance on large repositories".
- **The 30-day `activeBranchDays` window looked like a disqualifier and is not.** 152 of this
  repository's 436 refs have no commit in 30 days, but of 594 documents under `docs/` absent from
  `origin/master`, exactly **one** lives only on such a ref, and of 410 absent in-flight notes,
  **none** do. Recorded because it was the expected finding and the measurement refused it.
- **No estimate is offered for the migration beyond the citation count.** §4 counts what breaks, not
  what it costs to fix.
