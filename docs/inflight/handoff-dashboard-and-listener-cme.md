---
artifact_contract: "ce-handoff/v1"
created_at: "2026-08-13T00:00:00Z"
title: "Dashboard PR astubbs#268 and listener-CME PR astubbs#267"
summary: "Two stacked PRs are open: astubbs#267 (concurrent listener registration, blocked on spotbugs plus 12 unresolved review threads) and astubbs#268 (embedded web dashboard, correctly blocked until astubbs#267 merges)."
keywords: ["parallel-consumer", "dashboard", "web-gui", "ConcurrentModificationException", "spotbugs", "pr-267", "pr-268", "issue-215"]
cwd: "/Users/astubbs/github/parallel-consumer/.claude/worktrees/web-gui"
resume_focus: "Unblock astubbs#267 - decide the spotbugs EI_EXPOSE_REP response and clear the 12 unresolved review threads - then rebase astubbs#268 onto it."
repository: "astubbs/parallel-consumer"
repo_root_sha: "713c7468d5ecda55a2190d38e698cda017e91259"
branch: "feats/web-gui"
head: "cdb99d53d50a20095f3d3e4018841dee6b70ba2a"
worktree_path: "/Users/astubbs/github/parallel-consumer/.claude/worktrees/web-gui"
---

# Handoff: dashboard (astubbs#268) and listener CME (astubbs#267)

Two stacked PRs are open. **astubbs#267 must merge first**; astubbs#268 carries a duplicate of one
line from it and drops that line on rebase.

> Read this file as *status*, not as instructions. Nothing here is authorised to be pushed, posted or
> merged without the user saying so in the new session. The user gates **every** push and **every**
> outbound comment separately - see "Standing constraints".

## The two PRs

### astubbs/parallel-consumer#267 - listener/callback registration race (`fix/concurrent-listener-registration`)

Worktree `.claude/worktrees/fix-loop-hook-cme`, tip `c79f27b63`, **one commit unpushed**.

Registering a callback or listener on a *running* consumer could silently stop it. Two fields were
plain `ArrayList`s iterated by the control loop while a public registration path mutated them from
another thread; the resulting `ConcurrentModificationException` escaped the control loop and killed
the consumer with no error the caller could catch.

- `AbstractParallelEoSStreamProcessor.controlLoopHooks` - registered via public `addLoopEndCallBack`
- `WorkManager.successfulWorkListeners` - was registered via a Lombok `@Getter(PUBLIC)` that handed
  out the live list

Both are now `CopyOnWriteArrayList`, and the second field's getter was replaced by a real
`WorkManager.addSuccessfulWorkListener(..)`.

Commits (oldest first): `0eb5fbf5` hooks fix · `9d639123` listeners fix · `b60ba77a` encapsulation ·
`b2ce7aa7` test dedup · `34410fcc` breaking-change record · `c79f27b63` link-text fix (**unpushed**).

### astubbs/parallel-consumer#268 - embedded web dashboard (`feats/web-gui`)

Worktree `.claude/worktrees/web-gui`, tip `cdb99d53`, pushed and in sync. Tracks astubbs#215.

New opt-in, experimental, read-only module `parallel-consumer-dashboard`. Phase 1 only. Centrepiece
is the offset ribbon showing head-of-line blocking *being solved* - work completed beyond the base
commit offset, encoded into commit metadata, that a single-threaded consumer would have replayed.

`Check PR Dependencies` is **failing on purpose** - it blocks on astubbs#267 being open. That is the
gate working, not a defect.

## Blockers on astubbs#267 - the live problem

`mergeStateStatus` is `BLOCKED`. There are **two** causes; an earlier subagent report named only the
first.

### 1. `spotbugs` - required, `neutral`, 1 violation

```
EI_EXPOSE_REP  PCModule.workManager() may expose internal representation
parallel-consumer-core/src/main/java/io/confluent/parallelconsumer/internal/PCModule.java:85
```

**`gh pr checks` renders `neutral` as `skipping`**, so it reads as a non-event and was wrongly waved
through twice. Query the check-run API instead:

```bash
gh api repos/astubbs/parallel-consumer/commits/<sha>/check-runs --paginate \
  --jq '.check_runs[] | select(.name|test("spotbugs";"i")) | "\(.name): \(.conclusion) | \(.output.summary)"'
```

Reproduced locally with a control arm (`./mvnw -Pci compile spotbugs:spotbugs -pl parallel-consumer-core -am`):
master flags `EI_EXPOSE_REP` on `PCModule.options` and `pcMetrics` but **not** `workManager`; the
branch flags all three. `PCModule.java` is untouched by this PR.

**Proposed mechanism, NOT verified - do not repeat it as fact.** SpotBugs infers mutability partly
from public mutators. `WorkManager` previously exposed registration through a Lombok *getter* and now
has an explicit public *mutator*, so returning it from `PCModule.workManager()` became an exposure.
If you need this settled, run the single-term experiment: add back only the mutator, or only remove
the getter, and see which flips it.

Context for the decision: the repo has **no `@SuppressFBWarnings` anywhere**, so a suppression sets a
new precedent. The baseline is *cached from master pushes*, not checked in
(`.github/workflows/maven.yml`, the `static: spotbugs baseline` job), so this self-heals once merged
but blocks now. `options` and `pcMetrics` on the same class are already flagged-and-baselined, so
treating `workManager` as a false positive is consistent.

### 2. Twelve unresolved review threads

The `master` ruleset (id `15055005`) sets `required_review_thread_resolution: true` and
`required_approving_review_count: 0`. So unresolved threads block merge and **no approval is needed**.
This is invisible in `gh pr checks`. List them with the GraphQL query in
`reviewThreads(first: 40) { nodes { id isResolved isOutdated path comments } }`.

Most are already fixed in code and need only a reply plus resolve: two duplicate-code warnings (now
`isOutdated`), three `claude` comments about the duplicated latch choreography (fixed by `b2ce7aa7`),
the undocumented copy-on-write behaviour change (documented on `addSuccessfulWorkListener`), and a
codex P2 asking for listener-contract docs (same commit). The convention finding about bare `#252`
link text is fixed in `c79f27b63`.

**Two need a human decision:**

- **codex P1, `docs/refactoring.md:63`** - "Keep landed changes out of the refactoring backlog." It
  argues the file is reserved for deferred work and that release notes generate from commit history,
  so a shipped ledger creates conflicting sources. **The user explicitly asked for v6 breaking
  changes to be tracked**, which the bot does not know. Unresolved.
- **codex P1 on removing `getSuccessfulWorkListeners()`** - already **decided by the user: it stays
  removed.** v6 is the major bump, so the change sits inside the release gate. The deprecated-getter
  suggestion was rejected on merit: returning the live list would leave
  `getSuccessfulWorkListeners().add(..)` compiling, reopening the discoverability hole the refactor
  closes. The thread still needs a reply and resolve.

## Decisions already made - do not relitigate

- **Removing `getSuccessfulWorkListeners()` is sanctioned.** v6 *is* the major bump; nothing has ever
  shipped under the `bz.stub.parallelconsumer` coordinate (`gh release list` is empty; the `0.5.x`
  tags are inherited upstream history under `io.confluent`); the only callers were this repo's tests.
- **The exposure tests assert the observable outcome** (consumer keeps running, closes cleanly), not
  the exception type. A change that swapped one exception for another would still be the bug.
- **Both fixes are needed, they are not alternatives.** Encapsulation fixes *discoverability*;
  copy-on-write fixes *correctness*. `addSuccessfulWorkListener` is still callable from any thread
  while `onSuccessResult` iterates.
- **astubbs#268 is Phase 1 only.** Phases 2-5 are scoped in the plan and deliberately excluded.
- **No TLS in the dashboard MVP**, and no auth. Loopback plus a `Host` allowlist is the posture.

## Verification performed

Each fix was confirmed as a controlled experiment, one term changed:

| Field | `ArrayList` | `CopyOnWriteArrayList` |
|---|---|---|
| `controlLoopHooks` | FAIL - CME, loop stops, 30s timeout | PASS 1.6s |
| `successfulWorkListeners` | FAIL - CME, consumer stops, 30s timeout | PASS 1.3s |

A subagent independently re-ran the control arm against the *deduplicated* test form and confirmed it
still fails without the fix - so the dedup did not quietly stop exposing the bug.

CI on `34410fcc`: 17 of 18 required contexts green. `dups: clones` returned exactly to base
(2.47%, +0.00%). Unit Tests 76/0 in the dashboard-relevant classes, matching local.

**Known-noisy, do not misattribute:**

- `ProducerManagerTest.producedRecordsCantBeInTransactionWithoutItsOffsetDirect` - failed locally once
  at `PT19.98S` against an `at least PT20S` assertion under parallel load; passes isolated 4/4.
  Load-sensitive, pre-existing.
- `ParallelEoSStreamProcessorTest.queuedMessagesNotProcessedOrCommittedIfSubmittedDuringShutdown` and
  `PCMetricsTest.metricsRegisterBinding` flaked once in CI, auto-retried green. Both documented
  pre-existing - see `docs/solutions/test-flakiness/` and
  `docs/plans/2026-08-03-001-investigate-transactional-commit-flake.md`.
- `Mutation Tests (PIT, PR-scoped)` passes **vacuously** here - its package filter excludes both
  changed classes. The tick carries no signal for this PR.
- `Chaos Pain Suite` and `Performance (optional)` sit `queued`, never started - they need a
  self-hosted `highcpu` runner. Neither is in the required set.

## Traps this session actually hit - you will hit them too

- **Gate scripts that mask their exit code.** A wrapper ending in `grep`/`tail` exits with the
  *filter's* status, not the tool's. This hid a real Maven failure and let a real issue-ref violation
  through. Capture `$?` immediately and gate on it before committing.
- **`bin/check-issue-refs.sh` locally only scans the diff; CI also scans the PR body.** A bare `#NN`
  in a PR body passes locally and fails CI. Check bodies by hand.
- **Grep the mechanism, not the field name.** `successfulWorkListeners` looked like dead code because
  its only mutation was spelled `getSuccessfulWorkListeners().add(..)` through a generated accessor.
  A Lombok getter hides writes from any search for the field.
- **Never give a subagent write access to a worktree you are editing.** One did (correctly) revert
  the fix to run a control arm, in the tree being edited. Every agent gets its own
  `.claude/worktrees/<slug>` - `/tmp` and the scratchpad are both wrong.
- **`git cherry-pick -q` is not a valid option** and can look like a silent no-op inside `set -e` with
  `&&`.
- **`git rm` needs `-f`** when the index deliberately differs from HEAD, which it always does
  mid-rebuild.

## Authoritative references

- Plan: `docs/plans/2026-08-07-002-feat-embedded-web-dashboard-plan.md` - R1-R56, KTD1-KTD23, U1-U15,
  Phased Delivery, Promotional Potential. The authority for astubbs#268's scope.
- `docs/inflight/branch-web-gui-dashboard.md` - what astubbs#268 touches outside its module, the
  seed-stability invariant, and the completed re-cut with the corrected method.
- `docs/inflight/bug-control-loop-hooks-cme.md` - the defect write-up and the class sweep, including
  every field checked and cleared.
- `docs/refactoring.md` - the section at issue in the open codex P1, plus the queued breaking changes.
- `AGENTS.md` - PR title and template rules (CI-enforced), issue-reference convention, merge strategy.

## The re-cut, already done

`feats/web-gui` was re-cut to fix two misattributed commits (a `git add -A` swallowed a parallel
agent's work). Verified: `git diff backup/web-gui-pre-recut HEAD` is **empty** outside
`docs/inflight/` - history changed, content did not.

**`backup/web-gui-pre-recut` (`5ad663c3`) is a LOCAL TAG ONLY** and the sole remaining copy of the
pre-re-cut tip. Machine-local and fragile. Content is verified identical, so it is safe to lose, but
do not delete it casually.

The recorded method in the inflight note was corrected because following it literally would have
damaged the history: `git reset --mixed <merge-base>` flattens the branch so every rebuilt commit
gets each file's *final* state, and the net-diff check still passes - the damage is invisible. Use
`git read-tree -u --reset <commit>` + `git commit -C <commit>` instead.

## Not started

- **README end-user documentation and promotional material** for the dashboard. `README.adoc` is
  **generated** - edit `src/docs/README_TEMPLATE.adoc`. Raw material is in the plan's
  `## Promotional Potential`; both its claims carry caveats that make them wrong if copied carelessly,
  and KIP-489's status needs re-checking at publication time.
- **Rebase astubbs#268 onto astubbs#267** and drop its copy of the `controlLoopHooks` line. Waits on
  astubbs#267 merging.
- **`ce-compound`** has not been run and the haul is large - see Traps above.
- Phases 2-5 of the dashboard plan.

## Plausible next steps

1. **Unblock astubbs#267** - decide the spotbugs response, then reply to and resolve the 12 threads,
   then push `c79f27b63`. These are one sequence, not alternatives.
2. **Run `ce-compound`** before either branch disappears.
3. **Write the README and promotional material** for the dashboard.

## Standing constraints

- **Never push without explicit approval.** Commit freely; every push is gated separately because it
  triggers a review round.
- **Never post a comment, review reply, issue or PR action without being told.** "Draft it" is not
  "post it".
- Never weaken, skip, `@Disabled` or delete a test to go green. Never N/A a checklist item to beat a
  gate.
- Never work in the main checkout - always a dedicated `.claude/worktrees/<slug>`.
- Never hand-edit `README.adoc`. Never add a `CHANGELOG.adoc` entry in a PR.
- `gh` needs `-R astubbs/parallel-consumer` - the repo is a fork and defaults to upstream.
- The user's shell is fish: write commit messages to a file and use `git commit -F`.
