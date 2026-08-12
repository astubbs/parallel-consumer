# Package-rename grand sweep — plan and handoff

**Status: planned, not started.** Two rehearsals are done and green; this is the execution plan for the
real thing. Written to be picked up by a session with no memory of the rehearsals.

Read `docs/inflight/branch-package-rename.md` first — it is the project's ledger and records findings
this plan depends on. Read the `BRINGING AN OPEN BRANCH ACROSS` section in `bin/rename-packages.sh`
second; it is the per-branch procedure and it is authoritative over any summary here.

---

## What exists right now

| Thing | Where | State |
|---|---|---|
| The tooling | `tooling/package-rename-script` (#280), pushed | The script, its 112-assertion self-test, the procedure block |
| Renamed master, ready | `rename-final-rehearsal`, **local only** | master + #280, renamed, prose corrected, full build + tests **green** |
| A renamed PR branch | `260-rename-final` + tag `260-final-preupdate`, local | #260 brought across, both merge directions verified |
| Earlier rehearsal | `rename-stage`, `260-rename`, `264-rename`, local | **Throwaway.** Superseded by `rename-final-rehearsal`. Delete when convenient |

`rename-final-rehearsal` is the candidate for master's rename. It was built off `origin/master` with the
real #280 merged — no stand-ins.

**Known open item on #280:** its `tooling: package rename` CI check fails, 8 of 120 assertions, all in
the freeze-region code, on that feature's first CI run. The suite passes 112/112 locally, twelve runs
including under `env -i`, and `refs/pull/280/merge` is byte-identical to the branch — so it is not base
drift. Suspect runner tool-version skew (CI `git 2.54.0` and mawk vs local `git 2.47.3`). The check is
**not** in master's required-status-checks, so it does not block merge, but it is the safety net for the
tool this whole plan runs. `gh run rerun <id> --failed` separates flake from environment skew in one
shot. **Resolve this before Phase A merges.**

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
   bin/test-rename-packages.sh` (112/0), `bash bin/rename-packages.sh --verify-only` (exit 0), `bash
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
   - the `#280` merge — the tooling
   - `fix(test): re-point the stale asyncconsumer javadoc reference` — **prerequisite**, the script
     refuses at preflight without it, and it carries the `Modifications Copyright` line that editing an
     upstream-derived file requires
   - the move commit (pure `git mv`, 234 files) and the content commit (282 files) — the script's own
     two-commit shape, which must stay two commits
   - `docs(rename): correct the three falsified claims, after the rewrite`
   - `test(arch): exempt the Testcontainers support package`
5. Confirm it is 0 behind master immediately before merging. Four open PRs touch the mis-pairing files
   (#266, #268, #269, #271); if one lands while this PR is open, the landing merge has a renamed side
   meeting edits at old paths.

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

Open PRs, excluding #1 (`codeql`, 2021) and #8 (`features/retry-dlq`, 2022) as too old, #277 (the rename
plan itself) and #280 (the tooling).

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

**#266, #268, #269 and #271 touch the five near-identical `TestConventionsArchTest.java` files.** Neither
rehearsal exercised a branch that genuinely edits them with divergent content, so these four are the only
untested shape left. Do them with attention, not last.

Two are worth knowing about in advance: **#38 is a dependabot branch** (it may be force-pushed under
you), and **#263/#264 are a stacked pair** — #264 builds on #263's work.

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

| | #260 (4 files) | #264 (26 files, deletes some) |
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

## Housekeeping

Delete when this is finished, folding anything durable into
`docs/inflight/branch-package-rename.md`. Leaving half-renamed branches lying around is how a future
session mistakes a rehearsal for the real thing.

The raw rehearsal log — exact commands and every number — was written to session scratch and is **not**
durable. What matters from it is in the ledger.
