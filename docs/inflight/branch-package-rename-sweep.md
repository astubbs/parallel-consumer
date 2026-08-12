# Package-rename grand sweep — plan and handoff

**Status: planned, not started.** Two rehearsals are done and green; this is the execution plan for the
real thing. Written to be picked up by a session with no memory of the rehearsals.

Read `docs/inflight/branch-package-rename.md` first — it is the project's ledger and records findings
this plan depends on. Read the `BRINGING AN OPEN BRANCH ACROSS` section in `bin/rename-packages.sh`
second; it is the per-branch procedure and it is authoritative over any summary here.

---

## HIGH PRIORITY — NEEDS A HUMAN: astubbs#51 is excluded from the sweep

**astubbs#51 (`features/enable-virtual-threads`) cannot be brought across by this procedure and has
been dropped from it.** It is the only cross-repository PR of the 38: its head lives on
`devingryu`'s fork, so `origin/features/enable-virtual-threads` does not exist and step 0 of the
per-branch recipe is unrunnable. Verified with
`gh pr list --json number,isCrossRepository` — every other PR in the sweep is `false`.

The commit is reachable as `refs/pull/51/head` (`b4d5c2df`), so the rename *could* be performed
locally. That is not the problem. The problem is the other end: **there is nowhere to put the
result.** Phase D fast-forwards each `<branch>-rename` into its parent and the parent here is a
branch in someone else's repository. A renamed local branch with no push destination is not a
delivered result, it is a branch that quietly rots while the tree moves underneath it.

Whoever picks this up is choosing between:

- **Push to the contributor's fork**, if they enabled maintainer edits. Check with
  `gh pr view 51 --json maintainerCanModify`. This is the only option that keeps the PR mergeable
  without the contributor doing anything.
- **Ask the contributor to run the procedure** on their fork, pointing them at
  `bin/rename-packages.sh` and the `BRINGING AN OPEN BRANCH ACROSS` block. Correct, and slow, and it
  is a 2021-era PR whose author may not still be reachable.
- **Close it**, and re-open the work as a fork-owned branch if the feature is still wanted.

Doing nothing has a cost and a deadline: once the rename lands on master, astubbs#51's diff is
against paths that no longer exist. Every day it stays open, the eventual merge gets worse — and it
is the one branch in this operation where the fork cannot fix that unilaterally.

`refs/pull/51/merge` is the wrong ref to use for any of this: it is GitHub's speculative merge with
the base, not the contributor's work.

### Also waiting on a human, lower priority

- **`dups: similarity` is a required check and fails on any renamed branch, as a false positive.**
  The five near-identical `TestConventionsArchTest.java` files score 89-91% against each other and
  always have — on astubbs#293 they score 89.57-91.04 and the job passes. The same numbers fail on
  astubbs#294 because the check compares against base *by file path*, and after a 234-file rename no
  path has a base counterpart, so pre-existing duplication reads as newly introduced. Nothing in the
  diff became more duplicated. This will fire on all 38 branches at Phase D. Options: make the
  action's base comparison rename-aware, add a documented temporary exemption, or override at merge.
- **The third refusal case: `src/docs/README_TEMPLATE.adoc`'s `confluent-accelerators` link.** The
  path-form scan matches `io/confluent` inside the URL `www.confluent.io/confluent-accelerators/` —
  a live external link, not a package reference. Master deleted the line in `04cb92de` as part of a
  wholesale intro rewrite, so "delete what master deleted" does **not** inherit the conflict-free
  property that makes the astubbs#289 deletion safe, and the line sits next to the guarded prose.
  **2 of 37 branches** are affected. Probe: `git merge-base --is-ancestor 04cb92de origin/<headRef>`.

---

## What exists right now

| Thing | Where | State |
|---|---|---|
| The tooling | `tooling/package-rename-script` (astubbs#280), pushed | The script, its 114-assertion self-test, the procedure block |
| Renamed master, ready | `rename-final-rehearsal`, **local only** | master + astubbs#280, renamed, prose corrected, full build + tests **green** |
| A renamed PR branch | `260-rename-final` + tag `260-final-preupdate`, local | astubbs#260 brought across, both merge directions verified |
| Earlier rehearsal | `rename-stage`, `260-rename`, `264-rename`, local | **Throwaway.** Superseded by `rename-final-rehearsal`. Delete when convenient |

`rename-final-rehearsal` is the candidate for master's rename. It was built off `origin/master` with the
real astubbs#280 merged — no stand-ins.

**Closed: astubbs#280's 8 CI failures were a real portability bug, not flake and not base drift.**
`FREEZE_ID_ERE` reached `awk` through `-v`, which runs C-string escape processing over the value before
`match()` ever sees it. `\(` is not a defined escape and POSIX leaves it implementation-defined: local
mawk keeps the backslash, so the parens stay literal; the CI awk drops it, so they become groups, the
id of every region reads as empty, and a correctly written freeze region is refused as unnamed. All
eight failures follow from that one substitution — reproduced locally, exactly and in order, by
stripping the backslashes from that one variable. The regex now uses `[(]`/`[)]`, which carry no
backslash for `-v` to eat, and the suite asserts the value survives the mangling as well as the raw
form, so the next machine with a different awk fails the assertion instead of the feature.

The general rule, worth more than the fix: **a regex crossing into awk through `-v` must contain no
backslash escapes.** `SWEEP_ERE` (`io[\\./]*conflu`) still does — inside a bracket expression, which is
where awks disagree about whether the backslash is a member of the class or an escape. It is not
currently failing, because both spellings still match what the sweep needs, but it is the same class of
hazard and it is now written down.

---

## The shape

Four phases. A and B run **in parallel**. C is a human gate. D is separate and later.

```
A. Stage master's rename as a PR  ─┐
                                   ├─> C. review together ──> D. FF each -rename into its parent
B. Sweep: 38 <branch>-rename      ─┘
```

- **A** produces the only thing visible on GitHub: a PR for master's rename.
- **B** produces 38 local branches. **They are not pushed.**
- **C** reviews both together. The sweep branches are inspected locally; the master rename is reviewed
  as a normal PR.
- **D** fast-forwards each `<branch>-rename` into `<branch>`. Separate step, after review, not part of B.

### Why this order

Master carries the rename first, so each branch takes the tooling from real `origin/master` and the
procedure block needs no substitution. Branch-first also works — two independently renamed sides met at
2 conflicts in rehearsal — but it does the rename work twice per branch. Master-first is one rename and
one merge each.

---

## Phase A — stage master's rename as a PR

First subagent task. There is no PR for master's rename yet.

1. `rename-final-rehearsal` already holds it, green. Verify rather than trust: `bash
   bin/test-rename-packages.sh` (114/0), `bash bin/rename-packages.sh --verify-only` (exit 0), `bash
   bin/check-copyright-headers.sh` (0 violations), `bash bin/check-docs-data.sh` (39 valid), and
   `./mvnw -DskipITs install` (BUILD SUCCESS, 387 tests / 0 failures). JDK:
   `~/.local/share/mise/installs/java/temurin-17.0.20+8`.
2. Push it and open the PR against `master`.
3. **THE PR MUST NOT BE SQUASH-MERGED.** Say so in the PR body, in the first paragraph. Squashing pairs
   the five near-identical `TestConventionsArchTest.java` files into a cross-module cycle —
   `streams→metrics`, `vertx→mutiny`, `mutiny→reactor`, `reactor→vertx` — with `metrics` reported deleted
   and `streams` reported new. Four renames that never happened, permanent in history, unfixable in
   place. Merge method is per-PR in this repo and squash is the habitual choice, so agree the method with
   whoever merges **before** the PR opens.
4. The commits on that branch, and what each is for:
   - the `astubbs#280` merge — the tooling
   - `fix(test): re-point the stale asyncconsumer javadoc reference` — **prerequisite**, the script
     refuses at preflight without it, and it carries the `Modifications Copyright` line that editing an
     upstream-derived file requires
   - the move commit (pure `git mv`, 234 files) and the content commit (282 files) — the script's own
     two-commit shape, which must stay two commits
   - `docs(rename): correct the three falsified claims, after the rewrite`
   - `test(arch): exempt the Testcontainers support package`
5. Confirm it is 0 behind master immediately before merging. Four open PRs touch the mis-pairing files
   (astubbs#266, astubbs#268, astubbs#269, astubbs#271); if one lands while this PR is open, the
   landing merge has a renamed side meeting edits at old paths.

---

## Phase B — the sweep, 38 branches

One subagent per branch, or batched; the harness caps concurrency. Each agent owns exactly one branch.

### The recipe, per branch

Work from the `BRINGING AN OPEN BRANCH ACROSS` block, not from this summary. In outline:

```sh
git worktree add <wt> -b <headRef>-rename origin/<headRef>
cd <wt> && git branch --unset-upstream

# step 1 - staleness, run it, do not reason about it
git diff --quiet origin/master -- bin/rename-packages.sh bin/check-copyright-headers.sh \
  && echo "CURRENT" || echo "STALE"

# step 2 - BOTH files, a file checkout, NOT a merge and NOT a cherry-pick
git checkout origin/master -- bin/rename-packages.sh bin/check-copyright-headers.sh
git commit -m "chore: take the rename tooling from master"

# step 3 - dry run, then apply
bin/rename-packages.sh --dry-run --defer-prose
bin/rename-packages.sh --defer-prose
```

**Expect the run to refuse** on `io.confluent.csid.asyncconsumer.BrokerPollSystem` having no rule. Every
branch hits it: the fix lands on master as part of the rename, and the branch must rename *before*
merging master, so the one operation that would deliver it must come afterwards. The block gives the
exact replacement text. Apply it **verbatim** and commit, then re-run.

### Hard constraints, in order of how badly they bite

1. **One pinned tooling source for all 38.** Not "whatever the script is now" per agent — one ref,
   `origin/master` after Phase A merges. Two branches renaming to different `PKG_MAP` destinations are
   stranded: re-running is a no-op (`already applied, nothing to do`), so neither side can be brought
   into agreement, and there is no recovery procedure for that class.
2. **The `asyncconsumer` fix must be byte-identical on every branch.** Same bytes → the later master
   merge raises no conflict on that file. 38 slightly different wordings → 38 conflicts.
3. **No agent merges master.** That is a separate step and getting it wrong is the silent cross-module
   corruption this whole procedure exists to prevent.
4. **`--defer-prose` on every branch.** Prose is corrected only on master.
5. **No agent pushes.** Phase B produces local branches only.

### Gates, per branch, before it is eligible for Phase D

| Gate | Pass condition |
|---|---|
| script exit | 0 |
| renames detected | equals files moved, all `R100` |
| `mis-paired` | **0** on both commits |
| add/delete pairs | none on the move commit |
| completeness | exit 0, `NON-excluded files with matches 0` |
| copyright | 0 violations, checked count = baseline + that branch's added java files |
| commit shape | exactly two commits from the script, move then content |

**Any refusal, any non-zero `mis-paired`, or any conflict whose resolution is not obvious: the agent
stops and reports.** It does not improvise. That is step 8 of the block and it is not advice.

### What each agent reports

Four numbers and a verdict: script exit, `mis-paired`, completeness exit, copyright violations — plus
files moved, and anything it had to do that the block did not tell it to do. **An improvisation is a
procedure defect, not a footnote**; it means the block is wrong and needs fixing for the branches that
come after.

### The 38 branches

Open PRs, excluding astubbs#1 (`codeql`, 2021) and astubbs#8 (`features/retry-dlq`, 2022) as too old, astubbs#277 (the rename
plan itself) and astubbs#280 (the tooling).

| PR | head ref | PR | head ref |
|---|---|---|---|
| 29 | `bugs/857-paused-consumption-multi-consumers-bug` | 226 | `feats/health-check-api` |
| 31 | `fix/909-stale-container-replacement` | 256 | `ci/issue-first-response` |
| 38 | `dependabot/maven/junit.version-6.0.3` | 257 | `fix/produce-lock-double-release` |
| 51 | `features/enable-virtual-threads` | 260 | `test/commit-assertion-repeat-tolerance` |
| 53 | `feat/java-17-baseline` | 261 | `fix/transactional-produce-callback-abort` |
| 57 | `fix/859-metrics-leak-plus-cherrypicks` | 262 | `test/transactional-mode-battle-test` |
| 105 | `optimize/unit-gate` | 263 | `chore/audit-inactive-tests` |
| 106 | `perf/sparse-offset-encoding` | 264 | `test/inactive-test-remediation` |
| 116 | `bugs/912-vertx-stream-memory-leak` | 265 | `test/suite-config-isolation-and-event-based-waits` |
| 199 | `ci/release-notes-body` | 266 | `feats/industry-grounded-examples` |
| 200 | `docs/180-build-recipe` | 267 | `fix/concurrent-listener-registration` |
| 201 | `fix/155-load-factor-noise` | 268 | `feats/web-gui` |
| 202 | `refactor/159-longpolling-to-main` | 269 | `feats/connect-on-pc-spike` |
| 203 | `fix/log-verbosity-batch` | 271 | `feats/ks-on-pc-spike` |
| 204 | `test/177-commit-response-timeout` | 274 | `ci/review-gate-comment-boundary` |
| 205 | `feat/mdc-context-propagation` | 275 | `ci/review-gate-rejects-unfinished-review` |
| 206 | `refactor/40-mockconsumer-test-dedup` | 286 | `ci/claude-yml-script-grant` |
| 207 | `fix/offset-encoding-policy-bypass` | 290 | `test/apache-kafka-broker-image` |
| | | 292 | `fix/chaos-harness-double-start-race` |
| | | 293 | `feats/proxy-requirements` |

**astubbs#266, astubbs#268, astubbs#269 and astubbs#271 touch the five near-identical
`TestConventionsArchTest.java` files.** Neither rehearsal exercised a branch that genuinely edits them
with divergent content, so these four are the only
untested shape left. Do them with attention, not last.

Two are worth knowing about in advance: **astubbs#38 is a dependabot branch** (it may be force-pushed under
you), and **astubbs#263/#264 are a stacked pair** — astubbs#264 builds on astubbs#263's work.

---

## Phase C — review

Both halves together. The master rename is a normal PR review. The 38 sweep branches are inspected
locally; nothing is pushed, so there is nothing to fetch. Agents stay alive so a message can be pasted
into each shell.

Per branch, the review needs only the four gate numbers plus the conflict count against master. Anything
that stopped and reported is triaged here, not by the agent.

---

## Phase D — fast-forward, separate step

After review. For each approved branch:

```sh
git checkout <headRef> && git merge --ff-only <headRef>-rename
```

`<headRef>-rename` is a strict descendant, so this is an append, not a force-push. Pushing each parent
branch updates 38 open PRs, each gaining 234 moved files — that is expected and is the point, but it is
the outward-facing step, so it is the owner's call per branch.

---

## Wrong paths — do not retry these

Each was tried and refuted; the reasoning is in the ledger and the script header.

- **"Branches pick the script up by updating from master."** Once the rename is on master a stale branch
  needs the script *before* it can merge master, and merging master is the operation the script exists to
  prevent. Acquisition must not be a merge. It also cannot be a cherry-pick: the tooling arrived over
  eleven non-merge commits.
- **Taking only `bin/rename-packages.sh`.** Without the copyright checker's provenance normalisation
  every moved upstream file loses its fork-point lookup and its retained Confluent header becomes a
  violation — 197 of them, measured. **Both files, always.**
- **"Re-running brings a drifted branch current."** True only for old-side `PKG_MAP` additions and
  exclusion-list *removals*. A destination change or an exclusion *addition* makes re-running a silent
  no-op, and the branch is stranded. A destination change is a second migration, not a rule edit.
- **Pinning a sha in the fan-out instruction.** Freezes the instructions, which must evolve, while doing
  nothing about the transformation rules, which must not drift.
- **Correcting the guarded prose before the rewrite.** Every correct replacement must NAME the old
  package, so the bulk rewrite eats it — producing a sentence saying a package moves to itself and a
  `sed` that does nothing, and it passes every gate. Correct prose **after** the rewrite. Plan §8 says so
  itself: ready to paste when the rename lands, not before.
- **Assuming the two-commit shape helps the merge.** It does not, and this is measured: `git merge`
  detects renames over the merge-base-to-tip delta, which is identical however many commits it spans.
  The split buys accurate *history* — `git log --follow` traverses the rename — not clean merges.

---

## What the rehearsals measured, so it is not re-derived

Two full rehearsals, both merge directions, all gates green.

| | astubbs#260 (4 files) | astubbs#264 (26 files, deletes some) |
|---|---|---|
| files moved | 235 | 233 |
| `mis-paired` | 0 | 0 |
| conflicts per merge | **2** | **9** |
| of which need a decision | 0 | **3** (deleted-vs-renamed) |
| content check | 4/4 byte-identical | tree-wide gate exact |

**Per-branch cost is driven by PR size and whether it deletes files**, not by whether it touches the arch
tests. The mechanical conflicts are the prose pair (master's wording wins) and additive one-line unions.
The decisions are where a PR deleted a file that master renamed — deletion wins, that being the PR's
intent.

`diff.renameLimit` (default 1000) and `merge.renameLimit` (default 7000) are both comfortably above 234.

Origin §6's deferred verification is **done**: compile and the full unit suite pass on the renamed tree —
387 tests, 0 failures, every module. Still not run: the mutation lane observed scoring mutants, and the
Docker integration suite.

---

## The six things that actually blocked branches

The plan predicted one blocker per branch (the `asyncconsumer` javadoc). The sweep found six classes.
Every one was found by an agent **stopping and reporting instead of improvising**, which is the single
highest-value rule in the procedure: five agents diagnosed class B independently and none of them
invented a fix, so it cost one investigation rather than 33 divergent resolutions.

Counts are out of the 37 branches attempted.

| | What | Branches | Resolution |
|---|---|---|---|
| A | `asyncconsumer` javadoc has no rule | all | verbatim replacement, already in the procedure |
| B | dead `io.confluent.csid` logback loggers, deleted on master by astubbs#289 | 33 | delete the same lines; verified on astubbs#31 |
| C | the tooling checkout wipes the branch's own copyright-manifest entry | 3 | restore the entry; verified on astubbs#202 |
| D | the README regen inlines the frozen CHANGELOG | 4 | apply astubbs#113's replacement; verified on astubbs#105 |
| E | the accelerators URL reads as a package path | 2 | delete the line |
| F | a prose guard whose sentence the branch never had | 2 attempted, ~121 repo-wide | **fixed in the tooling** |

A fifth instance of the same shape turned up in the tail, and it is worth naming because it is the one
that does **not** announce itself as a refusal. On the four oldest branches — the same four as class D —
`pom.xml` still carries `license-maven-plugin` and its `license-maven-plugin-git` extension, which
master has since removed entirely. That extension's jgit cannot read a worktree's `.git` **file**, so
`./mvnw -N process-sources` reports failure inside a sweep worktree even though the asciidoc regen goal
has already run and produced a correct `README.adoc`. **Judge the regen by its output, not by the exit
code**: `--verify-only` passing afterwards is the real check, and it did pass on every affected branch.
On master's current `pom.xml` the command exits 0 cleanly, which is why 30-odd agents never saw it.

**B, D, E and the license plugin share one shape, and it is the lesson worth carrying.** Each is a master commit that made
the rename mechanical — deleting dead config, removing an include, dropping a stale link — which older
branches simply do not have. The procedure was written against master's tip and silently assumed every
branch was rebased onto it. **When a cleanup lands "ahead of the rename so the change stays mechanical",
it makes the rename mechanical only for branches that contain it.** A future sweep should screen for
each such prerequisite commit up front with `git merge-base --is-ancestor`, rather than discovering
them one refusal at a time.

**A copyright count that DROPS after the rename is not a check that stopped running.** astubbs#38 went
from 15 violations to 0. The 15 were upstream-derived files modified since the fork point without a
`Modifications Copyright` line — the branch's own pre-existing debt. The rename's content commit touches
those same files and stamps the line in, so it healed the debt rather than hiding it; verified by
finding the content commit as the one that added the line. Expect this on any branch carrying that debt,
and check which commit added the line before believing either story.

**C is the one that will bite again outside this project.** `git checkout <ref> -- <file>` is a
whole-file overwrite, so a branch that had registered its own file provenance in
`bin/check-copyright-headers.sh` lost it. The failure then surfaces four steps downstream — copyright
fails, so `mvnw` fails, so the README regen is skipped, so the README keeps its old spelling, so the
completeness check fails — and reads as "the rename broke something". astubbs#202 traced the whole
cascade back to one deleted line. The procedure warned about taking too few files; it did not warn that
taking them is destructive to what the branch had added.

**F was a genuine tool defect and was fixed rather than worked around**, because it does not scale: the
orphan refusal sat before the `--defer-prose` check, so the flag meaning "prose is master's problem"
could not reach it, and replicating the guard's logic across the repo showed 121 of 193 branches would
fail the same way. On master it still refuses — a guard matching nothing is still not a passing check —
and a negative control asserts that widening the branch case did not soften the master case.

Cost of that fix, measured rather than assumed: branches carry a copy of the tooling, so master's copy
now differs from theirs and each branch gains **one** extra conflict on `bin/rename-packages.sh`,
add/add, resolved by taking master's side. One mechanical conflict per branch, against a tool that
would otherwise block ~121.

## What the real sweep measured, superseding the rehearsal numbers

Two swept branches were merge-tested against renamed master in throwaway worktrees — nothing
committed, no branch touched. **This is not Phase D**, which is a fast-forward; it is the later
per-branch master merge, measured early.

| | astubbs#263 (small) | astubbs#268 (`feats/web-gui`, 311 files moved) |
|---|---|---|
| conflicted paths | **3** | **22** |
| of which prose (`README.adoc`, `README_TEMPLATE.adoc`) | 2 | 2 |
| of which `TestConventionRules.java` | 1 | 1 |
| of which logback | 0 | 9 |
| of which branch content | 0 | 10 |

Every conflict is mechanical. The prose pair resolves to master's wording, as the procedure already
says. `TestConventionRules.java` conflicts because master's rename added the Testcontainers exemption
to it. The logback ones are the interesting class, and they are **avoidable noise this sweep chose to
accept**: master's astubbs#289 deleted the dead logger lines AND collapsed the blank runs around them
AND removed one further commented `ParallelConsumerTestBase` logger. The sweep's prescribed deletion
removes only the logger lines, so the two sides differ by whitespace and one comment, and git reports
a conflict on ~8-9 logback files per pre-289 branch. Both sides are commented-out dead configuration,
so resolution is trivial in every case.

A future sweep of this shape should transplant the upstream commit's hunks whole rather than deleting
the offending lines, and would save roughly 8 spurious conflicts on each of 33 branches. Re-running
the 30 branches already green to buy that was not judged worth it — the conflicts are loud, mechanical,
and land at a step a human is doing anyway.

**The mis-pairing fear did not materialise, at any pool size.** The plan was written around five
near-identical `TestConventionsArchTest.java` files. Branches carry more — astubbs#268 seven,
astubbs#269 eight, astubbs#266 ten — and in every case all of them paired within their own module at
R100, with no cross-module invention and nothing dropped to an add/delete pair. The reason is
structural, not luck: the move commit changes only paths, so each file scores exactly 100% against its
own former path and git never has a tie to break. The documented cross-module cycle is a property of
the **squashed** arm alone, and pool size does not touch the supported shape. It does mean a squash on
astubbs#266 would now damage ten files rather than the five the plan describes.

## Housekeeping

Delete when this is finished, folding anything durable into
`docs/inflight/branch-package-rename.md`. Leaving half-renamed branches lying around is how a future
session mistakes a rehearsal for the real thing.

The raw rehearsal log — exact commands and every number — was written to session scratch and is **not**
durable. What matters from it is in the ledger.
